//! Asynchronous Bybit UTA V5 Command Gateway, Private WS Parser & P0-A Emergency Flatten Handler.

#![allow(
    clippy::manual_clamp,
    clippy::manual_range_contains,
    clippy::too_many_arguments
)]

use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread;
use std::time::Duration;

use crate::bybit_client::BybitV5LiveClient;
use crate::execution_fsm::{
    compute_amend_total_lots, format_bijective_order_link_id, parse_verified_avg_price_ticks,
    OrderLinkRole, WsMutationOp, WsOrderLatticeState, WsOrderRank, WsTerminalKind,
};
use crate::http_ws_transport::{
    boot_session_hash32, global_ws_trackers, OnlineIndexerLagEstimator,
};
use crate::types::{slot_from_symbol_name, LotQty, PriceTicks, Side, APPROVED_SPECS};

pub const GATEWAY_CMD_CAP: usize = 64;
pub const GATEWAY_EVT_CAP: usize = 256;

#[derive(Copy, Clone, Debug)]
pub enum GatewayCommand {
    SyncWallet,
    PlaceEntry {
        slot_idx: usize,
        side: Side,
        entry_price: PriceTicks,
        qty: LotQty,
        sl_price: PriceTicks,
    },
    RepegEntry {
        slot_idx: usize,
        side: Side,
        new_price: PriceTicks,
        qty: LotQty,
        new_sl_price: PriceTicks,
    },
    AbortEntry {
        slot_idx: usize,
        side: Side,
        aborted_price: PriceTicks,
        sl_hint: PriceTicks,
    },
    CancelResidualEntries {
        slot_idx: usize,
    },
    PollEntryFill {
        slot_idx: usize,
        sl_hint: PriceTicks,
    },
    EnforceStopLoss {
        slot_idx: usize,
        sl_price: PriceTicks,
    },
    EmergencyFlattenSlot {
        slot_idx: usize,
        open_side: Side,
        open_lots: LotQty,
        backup_sl: PriceTicks,
    },
    SyncOpenSlot {
        slot_idx: usize,
        side: Side,
        target_px: PriceTicks,
        target_lots: LotQty,
        defensive_lock: bool,
        cur_gross_bps: i64,
        vac_level: usize,
        opened_ts_ns: i64,
        tranche1_harvested: bool,
    },
}

#[derive(Clone, Debug)]
pub enum GatewayEvent {
    WalletUpdated {
        wallet_1e8: i64,
    },
    EntryPlaced {
        slot_idx: usize,
        accepted: bool,
        entry_price: PriceTicks,
        sl_price: PriceTicks,
    },
    EntryRepegged {
        slot_idx: usize,
        accepted: bool,
        new_price: PriceTicks,
        new_sl_price: PriceTicks,
    },
    EntryAborted {
        slot_idx: usize,
        side: Side,
        aborted_price: PriceTicks,
    },
    EntryOrderVanished {
        slot_idx: usize,
    },
    EntryFilledLive {
        slot_idx: usize,
        real_side: Side,
        verified_avg: Option<PriceTicks>,
        real_lots: LotQty,
        sl_verified: bool,
        tag: &'static str,
    },
    EntryStillResting {
        slot_idx: usize,
    },
    StopLossSynced {
        slot_idx: usize,
        sl_price: PriceTicks,
        verified: bool,
    },
    SlotStillOpenSynced {
        slot_idx: usize,
        real_side: Side,
        verified_avg: Option<PriceTicks>,
        real_lots: LotQty,
        active_resting_tp: PriceTicks,
    },
    SlotClosedDetected {
        slot_idx: usize,
        vac_level: usize,
        closed_pnl_payload: Option<serde_json::Value>,
    },
    SlotSyncSkipped {
        slot_idx: usize,
    },
    WsPositionUpdate {
        slot_idx: usize,
        is_open: bool,
        real_side: Side,
        verified_avg: Option<PriceTicks>,
        real_lots: LotQty,
    },
    WsEntryOrderTerminated {
        slot_idx: usize,
    },
}

/// Parses Bybit V5 private WS frames (wallet, position & order PostOnly cancellations).
pub fn parse_ws_private_frame(txt: &str, evt_tx: &SyncSender<GatewayEvent>) {
    let Ok(val) = serde_json::from_str::<serde_json::Value>(txt) else {
        return;
    };
    let Some(topic) = val.get("topic").and_then(|t| t.as_str()) else {
        return;
    };
    if topic == "wallet" {
        if let Some(arr) = val.get("data").and_then(|d| d.as_array()) {
            if let Some(first) = arr.first() {
                let w_f = first
                    .get("totalWalletBalance")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                if w_f > 0.0 {
                    let wallet_1e8 = (w_f * 100_000_000.0).round() as i64;
                    let _ = evt_tx.try_send(GatewayEvent::WalletUpdated { wallet_1e8 });
                }
            }
        }
    } else if topic == "position" {
        let Some(arr) = val.get("data").and_then(|d| d.as_array()) else {
            return;
        };
        for item in arr {
            let sym = item.get("symbol").and_then(|v| v.as_str()).unwrap_or("");
            let Some(slot_idx) = slot_from_symbol_name(sym) else {
                continue;
            };
            let spec = APPROVED_SPECS[slot_idx];
            let sz_s = item.get("size").and_then(|v| v.as_str()).unwrap_or("0");
            let Ok(sz_f) = sz_s.parse::<f64>() else {
                continue;
            };
            let side_s = item.get("side").and_then(|v| v.as_str()).unwrap_or("Buy");
            let real_side = if side_s == "Sell" { Side::Ask } else { Side::Bid };
            if sz_f > 0.0 {
                let avg_s = item.get("avgPrice").and_then(|v| v.as_str()).unwrap_or("0");
                let verified_avg = parse_verified_avg_price_ticks(spec, real_side, avg_s).ok();
                let real_lots = spec.parse_lot_qty(sz_s).max(LotQty(1));
                let _ = evt_tx.try_send(GatewayEvent::WsPositionUpdate {
                    slot_idx,
                    is_open: true,
                    real_side,
                    verified_avg,
                    real_lots,
                });
            } else {
                let _ = evt_tx.try_send(GatewayEvent::WsPositionUpdate {
                    slot_idx,
                    is_open: false,
                    real_side,
                    verified_avg: None,
                    real_lots: LotQty(0),
                });
            }
        }
    } else if topic == "order" {
        let Some(arr) = val.get("data").and_then(|d| d.as_array()) else {
            return;
        };
        for item in arr {
            let sym = item.get("symbol").and_then(|v| v.as_str()).unwrap_or("");
            let Some(slot_idx) = slot_from_symbol_name(sym) else {
                continue;
            };
            let reduce_only = item
                .get("reduceOnly")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let status = item.get("orderStatus").and_then(|v| v.as_str()).unwrap_or("");
            let cum_exec_q = APPROVED_SPECS[slot_idx].parse_lot_qty(
                item.get("cumExecQty")
                    .and_then(|v| v.as_str())
                    .unwrap_or("0"),
            );
            let link_s = item.get("orderLinkId").and_then(|v| v.as_str()).unwrap_or("");
            if !link_s.is_empty() {
                let (rk, tk) = match status {
                    "Filled" => (WsOrderRank::Terminal, WsTerminalKind::Filled),
                    "Cancelled" | "Deactivated" => {
                        (WsOrderRank::Terminal, WsTerminalKind::Cancelled)
                    }
                    "Rejected" => (WsOrderRank::Terminal, WsTerminalKind::PostOnlyRejected),
                    "PartiallyFilled" => (WsOrderRank::PartialFill, WsTerminalKind::None),
                    _ => (WsOrderRank::RestingBook, WsTerminalKind::None),
                };
                let inc = WsOrderLatticeState {
                    rank: rk,
                    price_ticks: PriceTicks(0),
                    total_lots: LotQty(0),
                    cum_exec_lots: cum_exec_q,
                    terminal_kind: tk,
                };
                if let Ok(mut trks) = global_ws_trackers().lock() {
                    let tr = &mut trks[slot_idx];
                    if tr.entry_link_id == link_s {
                        tr.entry_lattice = tr.entry_lattice.join(inc);
                    } else if tr.exit_link_id == link_s {
                        tr.exit_lattice = tr.exit_lattice.join(inc);
                    }
                }
            }
            if reduce_only {
                continue;
            }
            if matches!(status, "Cancelled" | "Rejected" | "Deactivated") && cum_exec_q.0 <= 0 {
                let _ = evt_tx.try_send(GatewayEvent::WsEntryOrderTerminated { slot_idx });
            }
        }
    }
}

pub fn process_gateway_command(
    client: &mut BybitV5LiveClient,
    cmd: GatewayCommand,
    evt_tx: &SyncSender<GatewayEvent>,
) {
    match cmd {
        GatewayCommand::SyncWallet => {
            if let Some((w_f, _, _, _)) = client.fetch_wallet_and_equity_usdt() {
                if w_f > 0.0 {
                    let wallet_1e8 = (w_f * 100_000_000.0).round() as i64;
                    let _ = evt_tx.try_send(GatewayEvent::WalletUpdated { wallet_1e8 });
                }
            }
        }
        GatewayCommand::PlaceEntry {
            slot_idx,
            side,
            entry_price,
            qty,
            sl_price,
        } => {
            if !client.authorizes_ws_mutation(WsMutationOp::Create, false) {
                let _ = evt_tx.try_send(GatewayEvent::EntryPlaced {
                    slot_idx,
                    accepted: false,
                    entry_price,
                    sl_price,
                });
                return;
            }
            let spec = APPROVED_SPECS[slot_idx];
            let link_id = if let Ok(mut trks) = global_ws_trackers().lock() {
                let tr = &mut trks[slot_idx];
                if tr.entry_lattice.rank == WsOrderRank::Terminal {
                    tr.entry_sub_idx = tr.entry_sub_idx.wrapping_add(1);
                }
                let lid = format_bijective_order_link_id(
                    boot_session_hash32(),
                    slot_idx,
                    tr.cycle_id,
                    OrderLinkRole::Entry,
                    tr.entry_sub_idx,
                );
                tr.entry_link_id.clone_from(&lid);
                tr.entry_lattice = WsOrderLatticeState::new_dispatched(entry_price, qty);
                lid
            } else {
                String::new()
            };
            let rc = client.place_order_ws_or_rest(
                spec,
                side,
                entry_price,
                qty,
                Some(sl_price),
                false,
                &link_id,
                true,
            );
            let mut accepted = rc == 0;
            if let Ok(mut trks) = global_ws_trackers().lock() {
                let tr = &mut trks[slot_idx];
                let ack_st = if accepted {
                    WsOrderLatticeState {
                        rank: WsOrderRank::GatewayAccepted,
                        price_ticks: entry_price,
                        total_lots: qty,
                        cum_exec_lots: LotQty(0),
                        terminal_kind: WsTerminalKind::None,
                    }
                } else {
                    WsOrderLatticeState {
                        rank: WsOrderRank::Terminal,
                        price_ticks: entry_price,
                        total_lots: qty,
                        cum_exec_lots: LotQty(0),
                        terminal_kind: WsTerminalKind::ExchangeRejected,
                    }
                };
                tr.entry_lattice = tr.entry_lattice.join(ack_st);
                if tr.entry_lattice.rank == WsOrderRank::Terminal
                    && tr.entry_lattice.cum_exec_lots.0 <= 0
                    && tr.entry_lattice.terminal_kind != WsTerminalKind::Filled
                {
                    accepted = false;
                }
            }
            let _ = evt_tx.try_send(GatewayEvent::EntryPlaced {
                slot_idx,
                accepted,
                entry_price,
                sl_price,
            });
        }
        GatewayCommand::RepegEntry {
            slot_idx,
            side,
            new_price,
            qty,
            new_sl_price,
        } => {
            let spec = APPROVED_SPECS[slot_idx];
            let sym = spec.symbol_id.name();
            let (cur_link, cur_exec, cur_rank) = if let Ok(trks) = global_ws_trackers().lock() {
                let tr = &trks[slot_idx];
                (
                    tr.entry_link_id.clone(),
                    tr.entry_lattice.cum_exec_lots,
                    tr.entry_lattice.rank,
                )
            } else {
                (String::new(), LotQty(0), WsOrderRank::Terminal)
            };
            if !cur_link.is_empty()
                && cur_rank != WsOrderRank::Terminal
                && client.authorizes_ws_mutation(WsMutationOp::Amend, false)
            {
                let am_lots = compute_amend_total_lots(cur_exec, qty);
                if client.amend_order_ws_or_rest(
                    spec,
                    &cur_link,
                    new_price,
                    am_lots,
                    Some(new_sl_price),
                ) == 0
                {
                    if let Ok(mut trks) = global_ws_trackers().lock() {
                        trks[slot_idx].entry_lattice.price_ticks = new_price;
                        trks[slot_idx].entry_lattice.total_lots = am_lots;
                    }
                    let _ = evt_tx.try_send(GatewayEvent::EntryRepegged {
                        slot_idx,
                        accepted: true,
                        new_price,
                        new_sl_price,
                    });
                    return;
                }
            }
            let _ = client.cancel_non_reduce_only_orders(spec);
            if let Some((true, sz_s, r_side, avg_s)) = client.query_open_position_verified(sym) {
                let ver_avg = parse_verified_avg_price_ticks(spec, r_side, &avg_s).ok();
                let r_lots = spec.parse_lot_qty(&sz_s).max(LotQty(1));
                let sl_ok = client.set_position_trading_stop(spec, new_sl_price);
                let _ = evt_tx.try_send(GatewayEvent::EntryFilledLive {
                    slot_idx,
                    real_side: r_side,
                    verified_avg: ver_avg,
                    real_lots: r_lots,
                    sl_verified: sl_ok,
                    tag: "MAKER FILL REPEG",
                });
                return;
            }
            let new_link = if let Ok(mut trks) = global_ws_trackers().lock() {
                let tr = &mut trks[slot_idx];
                tr.entry_sub_idx = tr.entry_sub_idx.wrapping_add(1);
                let lid = format_bijective_order_link_id(
                    boot_session_hash32(),
                    slot_idx,
                    tr.cycle_id,
                    OrderLinkRole::Entry,
                    tr.entry_sub_idx,
                );
                tr.entry_link_id.clone_from(&lid);
                tr.entry_lattice = WsOrderLatticeState::new_dispatched(new_price, qty);
                lid
            } else {
                String::new()
            };
            let rc = client.place_order_ws_or_rest(
                spec,
                side,
                new_price,
                qty,
                Some(new_sl_price),
                false,
                &new_link,
                true,
            );
            let _ = evt_tx.try_send(GatewayEvent::EntryRepegged {
                slot_idx,
                accepted: rc == 0,
                new_price,
                new_sl_price,
            });
        }
        GatewayCommand::AbortEntry {
            slot_idx,
            side,
            aborted_price,
            sl_hint,
        } => {
            let spec = APPROVED_SPECS[slot_idx];
            let sym = spec.symbol_id.name();
            let cur_link = if let Ok(trks) = global_ws_trackers().lock() {
                trks[slot_idx].entry_link_id.clone()
            } else {
                String::new()
            };
            let canceled_targeted =
                !cur_link.is_empty() && client.cancel_order_ws_or_rest(spec, &cur_link) == 0;
            if !canceled_targeted {
                let _ = client.cancel_non_reduce_only_orders(spec);
            }
            match client.query_open_position_verified(sym) {
                Some((true, sz_s, r_side, avg_s)) => {
                    let ver_avg = parse_verified_avg_price_ticks(spec, r_side, &avg_s).ok();
                    let r_lots = spec.parse_lot_qty(&sz_s).max(LotQty(1));
                    let sl_ok = client.set_position_trading_stop(spec, sl_hint);
                    let _ = evt_tx.try_send(GatewayEvent::EntryFilledLive {
                        slot_idx,
                        real_side: r_side,
                        verified_avg: ver_avg,
                        real_lots: r_lots,
                        sl_verified: sl_ok,
                        tag: "MAKER FILL ABORT",
                    });
                }
                Some((false, ..)) => {
                    if let Ok(mut trks) = global_ws_trackers().lock() {
                        trks[slot_idx].advance_cycle();
                    }
                    let _ = evt_tx.try_send(GatewayEvent::EntryAborted {
                        slot_idx,
                        side,
                        aborted_price,
                    });
                }
                None => {
                    let _ = evt_tx.try_send(GatewayEvent::EntryOrderVanished { slot_idx });
                }
            }
        }
        GatewayCommand::CancelResidualEntries { slot_idx } => {
            let _ = client.cancel_non_reduce_only_orders(APPROVED_SPECS[slot_idx]);
        }
        GatewayCommand::PollEntryFill { slot_idx, sl_hint } => {
            let spec = APPROVED_SPECS[slot_idx];
            let sym = spec.symbol_id.name();
            match client.query_open_position_verified(sym) {
                Some((true, sz_s, r_side, avg_s)) => {
                    let _ = client.cancel_non_reduce_only_orders(spec);
                    let ver_avg = parse_verified_avg_price_ticks(spec, r_side, &avg_s).ok();
                    let r_lots = spec.parse_lot_qty(&sz_s).max(LotQty(1));
                    let sl_ok = client.set_position_trading_stop(spec, sl_hint);
                    let _ = evt_tx.try_send(GatewayEvent::EntryFilledLive {
                        slot_idx,
                        real_side: r_side,
                        verified_avg: ver_avg,
                        real_lots: r_lots,
                        sl_verified: sl_ok,
                        tag: "MAKER FILL",
                    });
                }
                Some((false, _, _, _)) if client.has_resting_entry_order(spec) == Some(false) => {
                    if let Some((true, sz2, rs2, avg2)) = client.query_open_position_verified(sym)
                    {
                        let v_avg2 = parse_verified_avg_price_ticks(spec, rs2, &avg2).ok();
                        let r_lots2 = spec.parse_lot_qty(&sz2).max(LotQty(1));
                        let sl_ok2 = client.set_position_trading_stop(spec, sl_hint);
                        let _ = evt_tx.try_send(GatewayEvent::EntryFilledLive {
                            slot_idx,
                            real_side: rs2,
                            verified_avg: v_avg2,
                            real_lots: r_lots2,
                            sl_verified: sl_ok2,
                            tag: "MAKER FILL",
                        });
                    } else {
                        let _ = evt_tx.try_send(GatewayEvent::EntryOrderVanished { slot_idx });
                    }
                }
                Some((false, _, _, _)) | None => {
                    let _ = evt_tx.try_send(GatewayEvent::EntryStillResting { slot_idx });
                }
            }
        }
        GatewayCommand::EnforceStopLoss { slot_idx, sl_price } => {
            let verified = client.set_position_trading_stop(APPROVED_SPECS[slot_idx], sl_price);
            let _ = evt_tx.try_send(GatewayEvent::StopLossSynced {
                slot_idx,
                sl_price,
                verified,
            });
        }
        GatewayCommand::EmergencyFlattenSlot {
            slot_idx,
            open_side,
            open_lots,
            backup_sl,
        } => {
            let spec = APPROVED_SPECS[slot_idx];
            let sym = spec.symbol_id.name();
            if backup_sl.0 > 0 {
                let _ = client.set_position_trading_stop(spec, backup_sl);
            }
            let _ = client.cancel_all_symbol_orders(sym);
            let (eff_side, eff_lots) = match client.query_open_position_verified(sym) {
                Some((false, ..)) => {
                    if let Ok(mut trks) = global_ws_trackers().lock() {
                        trks[slot_idx].advance_cycle();
                    }
                    let closed_pnl_payload = client.query_closed_pnl_raw(sym, 0);
                    let _ = evt_tx.try_send(GatewayEvent::SlotClosedDetected {
                        slot_idx,
                        vac_level: 1,
                        closed_pnl_payload,
                    });
                    return;
                }
                Some((true, sz_s, r_side, _)) => {
                    (r_side, spec.parse_lot_qty(&sz_s).max(LotQty(1)))
                }
                None => (open_side, LotQty(open_lots.0.max(1))),
            };
            let flat_link = if let Ok(mut trks) = global_ws_trackers().lock() {
                let tr = &mut trks[slot_idx];
                tr.exit_sub_idx = tr.exit_sub_idx.wrapping_add(1);
                format_bijective_order_link_id(
                    boot_session_hash32(),
                    slot_idx,
                    tr.cycle_id,
                    OrderLinkRole::Runner2,
                    tr.exit_sub_idx,
                )
            } else {
                String::new()
            };
            if client.authorizes_ws_mutation(WsMutationOp::Create, true) {
                let _ = client.flatten_position_market_ioc(spec, eff_side, eff_lots, &flat_link);
            }
            if let Some((false, ..)) = client.query_open_position_verified(sym) {
                if let Ok(mut trks) = global_ws_trackers().lock() {
                    trks[slot_idx].advance_cycle();
                }
                let closed_pnl_payload = client.query_closed_pnl_raw(sym, 0);
                let _ = evt_tx.try_send(GatewayEvent::SlotClosedDetected {
                    slot_idx,
                    vac_level: 1,
                    closed_pnl_payload,
                });
            } else {
                let _ = evt_tx.try_send(GatewayEvent::StopLossSynced {
                    slot_idx,
                    sl_price: backup_sl,
                    verified: false,
                });
            }
        }
        GatewayCommand::SyncOpenSlot {
            slot_idx,
            side: _,
            target_px,
            target_lots,
            defensive_lock,
            cur_gross_bps,
            vac_level,
            opened_ts_ns,
            tranche1_harvested,
        } => {
            let spec = APPROVED_SPECS[slot_idx];
            let sym = spec.symbol_id.name();
            let Some((mut still_open, mut sz_s, mut r_side, mut avg_s)) =
                client.query_open_position_verified(sym)
            else {
                let _ = evt_tx.try_send(GatewayEvent::SlotSyncSkipped { slot_idx });
                return;
            };
            if !still_open && cur_gross_bps < 0 {
                match client.query_open_position_verified(sym) {
                    Some((open2, sz2, s2, a2)) => {
                        still_open = open2;
                        sz_s = sz2;
                        r_side = s2;
                        avg_s = a2;
                    }
                    None => {
                        let _ = evt_tx.try_send(GatewayEvent::SlotSyncSkipped { slot_idx });
                        return;
                    }
                }
            }
            if !still_open {
                if let Ok(mut trks) = global_ws_trackers().lock() {
                    trks[slot_idx].advance_cycle();
                }
                let _ = client.cancel_all_symbol_orders(sym);
                let opened_ms = (opened_ts_ns / 1_000_000).max(0);
                let start_ms = if tranche1_harvested {
                    opened_ms.saturating_sub(86_400_000)
                } else {
                    opened_ms.saturating_sub(5_000)
                };
                let closed_pnl_payload = client.query_closed_pnl_raw(sym, start_ms);
                let _ = evt_tx.try_send(GatewayEvent::SlotClosedDetected {
                    slot_idx,
                    vac_level,
                    closed_pnl_payload,
                });
                return;
            }
            let verified_avg = parse_verified_avg_price_ticks(spec, r_side, &avg_s).ok();
            let real_lots = spec.parse_lot_qty(&sz_s).max(LotQty(1));
            let Some((resting_tp, resting_qty)) =
                client.inspect_exit_and_purge_residual_entries(spec)
            else {
                let _ = evt_tx.try_send(GatewayEvent::SlotSyncSkipped { slot_idx });
                return;
            };
            let eff_lots = LotQty(target_lots.0.min(real_lots.0).max(1));
            let tp_diff = (target_px.0 - resting_tp.0).abs();
            let moving_closer = match r_side {
                Side::Bid => resting_tp.0 == 0 || target_px.0 < resting_tp.0,
                Side::Ask => resting_tp.0 == 0 || target_px.0 > resting_tp.0,
            };
            let should_repeg = resting_tp.0 == 0
                || resting_qty != eff_lots
                || (moving_closer && tp_diff >= 1 && (defensive_lock || tp_diff >= 2))
                || (!moving_closer && cur_gross_bps < 200 && tp_diff >= 4);
            let desired_role = if !tranche1_harvested && eff_lots.0 < real_lots.0 {
                OrderLinkRole::Tranche1
            } else {
                OrderLinkRole::Runner2
            };
            if resting_tp.0 == 0 {
                if let Ok(mut trks) = global_ws_trackers().lock() {
                    trks[slot_idx].exit_link_id.clear();
                }
            }
            let active_resting_tp = if should_repeg {
                let (cur_ex_link, cur_ex_exec) = if let Ok(trks) = global_ws_trackers().lock() {
                    let tr = &trks[slot_idx];
                    (tr.exit_link_id.clone(), tr.exit_lattice.cum_exec_lots)
                } else {
                    (String::new(), LotQty(0))
                };
                let mut amended_ok = false;
                if resting_tp.0 > 0
                    && !cur_ex_link.is_empty()
                    && client.authorizes_ws_mutation(WsMutationOp::Amend, defensive_lock)
                {
                    let am_tot = compute_amend_total_lots(cur_ex_exec, eff_lots);
                    if client.amend_order_ws_or_rest(spec, &cur_ex_link, target_px, am_tot, None)
                        == 0
                    {
                        amended_ok = true;
                        if let Ok(mut trks) = global_ws_trackers().lock() {
                            trks[slot_idx].exit_lattice.price_ticks = target_px;
                            trks[slot_idx].exit_lattice.total_lots = am_tot;
                        }
                    }
                }
                if amended_ok {
                    target_px
                } else if client.authorizes_ws_mutation(WsMutationOp::Create, true) {
                    if resting_tp.0 > 0 || resting_qty.0 > 0 {
                        if !cur_ex_link.is_empty() {
                            let _ = client.cancel_order_ws_or_rest(spec, &cur_ex_link);
                        } else {
                            let _ = client.cancel_all_symbol_orders(sym);
                        }
                    }
                    let new_ex_link = if let Ok(mut trks) = global_ws_trackers().lock() {
                        let tr = &mut trks[slot_idx];
                        tr.exit_sub_idx = tr.exit_sub_idx.wrapping_add(1);
                        let lid = format_bijective_order_link_id(
                            boot_session_hash32(),
                            slot_idx,
                            tr.cycle_id,
                            desired_role,
                            tr.exit_sub_idx,
                        );
                        tr.exit_link_id.clone_from(&lid);
                        tr.exit_lattice = WsOrderLatticeState::new_dispatched(target_px, eff_lots);
                        lid
                    } else {
                        String::new()
                    };
                    let rc = client.place_order_ws_or_rest(
                        spec,
                        r_side.opposite(),
                        target_px,
                        eff_lots,
                        None,
                        true,
                        &new_ex_link,
                        false,
                    );
                    if rc == 0 { target_px } else { PriceTicks(0) }
                } else {
                    resting_tp
                }
            } else {
                resting_tp
            };
            let _ = evt_tx.try_send(GatewayEvent::SlotStillOpenSynced {
                slot_idx,
                real_side: r_side,
                verified_avg,
                real_lots,
                active_resting_tp,
            });
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn spawn_private_ws_listener(_evt_tx: SyncSender<GatewayEvent>) {}

#[cfg(target_os = "linux")]
fn spawn_private_ws_listener(evt_tx: SyncSender<GatewayEvent>) {
    use crate::execution_fsm::sign_bybit_v5_hmac_sha256;
    use crate::http_ws_transport::load_dotenv_map;
    use crate::types::now_epoch_ns;

    let env_map = load_dotenv_map();
    let api_key = env_map.get("BYBIT_API_KEY").cloned().unwrap_or_default();
    let api_secret = env_map.get("BYBIT_API_SECRET").cloned().unwrap_or_default();
    if api_key.is_empty() || api_secret.is_empty() {
        return;
    }
    let ws_url = env_map
        .get("BYBIT_PRIVATE_WS_URL")
        .cloned()
        .unwrap_or_else(|| "wss://stream.bybit.com/v5/private".to_string());
    thread::spawn(move || loop {
        if let Ok((mut socket, _)) = tungstenite::connect(ws_url.as_str()) {
            let tcp_opt = match socket.get_mut() {
                tungstenite::stream::MaybeTlsStream::Plain(s) => Some(s),
                tungstenite::stream::MaybeTlsStream::NativeTls(tls) => Some(tls.get_mut()),
                _ => None,
            };
            if let Some(tcp) = tcp_opt {
                let _ = tcp.set_nodelay(true);
                let _ = tcp.set_read_timeout(Some(Duration::from_secs(10)));
                let _ = tcp.set_write_timeout(Some(Duration::from_secs(5)));
            }
            let expires_ms = (now_epoch_ns().0 / 1_000_000).max(1) + 10_000;
            let sig =
                sign_bybit_v5_hmac_sha256(&api_secret, &format!("GET/realtime{}", expires_ms));
            let auth_msg = serde_json::json!({
                "op": "auth",
                "args": [api_key, expires_ms, sig],
            })
            .to_string();
            let sub_msg =
                "{\"op\":\"subscribe\",\"args\":[\"position\",\"wallet\",\"order\"]}".to_string();
            if socket.send(tungstenite::Message::Text(auth_msg)).is_err()
                || socket.send(tungstenite::Message::Text(sub_msg)).is_err()
            {
                thread::sleep(Duration::from_millis(500));
                continue;
            }
            let mut last_ping_ns = now_epoch_ns().0;
            while let Ok(msg) = socket.read() {
                let now_ns = now_epoch_ns().0;
                if now_ns.saturating_sub(last_ping_ns) >= 15_000_000_000 {
                    if socket
                        .send(tungstenite::Message::Text("{\"op\":\"ping\"}".to_string()))
                        .is_err()
                    {
                        break;
                    }
                    last_ping_ns = now_ns;
                }
                match msg {
                    tungstenite::Message::Text(t) => parse_ws_private_frame(&t, &evt_tx),
                    tungstenite::Message::Ping(p) => {
                        let _ = socket.send(tungstenite::Message::Pong(p));
                    }
                    tungstenite::Message::Close(_) => break,
                    _ => {}
                }
            }
        }
        thread::sleep(Duration::from_millis(500));
    });
}

#[derive(Debug)]
pub struct BybitAsyncGateway {
    cmd_tx: SyncSender<GatewayCommand>,
    evt_rx: Receiver<GatewayEvent>,
}

impl BybitAsyncGateway {
    #[must_use]
    pub fn spawn_if_enabled(enabled: bool) -> Option<Self> {
        let mut client = BybitV5LiveClient::from_env_if_enabled(enabled)?;
        let (cmd_tx, cmd_rx) = mpsc::sync_channel::<GatewayCommand>(GATEWAY_CMD_CAP);
        let (evt_tx, evt_rx) = mpsc::sync_channel::<GatewayEvent>(GATEWAY_EVT_CAP);
        let evt_ws = evt_tx.clone();
        thread::spawn(move || {
            client.enable_ws_trade_from_env();
            loop {
                match cmd_rx.recv_timeout(Duration::from_secs(12)) {
                    Ok(cmd) => process_gateway_command(&mut client, cmd, &evt_tx),
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        client.maintain_ws_trade_heartbeat();
                    }
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
        });
        spawn_private_ws_listener(evt_ws);
        Some(Self { cmd_tx, evt_rx })
    }

    #[inline]
    #[must_use]
    pub fn try_send(&self, cmd: GatewayCommand) -> bool {
        self.cmd_tx.try_send(cmd).is_ok()
    }

    #[inline]
    #[must_use]
    pub fn try_recv(&self) -> Option<GatewayEvent> {
        self.evt_rx.try_recv().ok()
    }
}

#[must_use]
pub fn verify_ws_private_parser_self_check() -> bool {
    let (tx, rx) = mpsc::sync_channel::<GatewayEvent>(8);
    let w_frame = "{\"topic\":\"wallet\",\"data\":[{\"totalWalletBalance\":\"66.2808\"}]}";
    let p_frame = concat!(
        "{\"topic\":\"position\",\"data\":[{\"symbol\":\"AVAXUSDT\",",
        "\"side\":\"Buy\",\"size\":\"2.6\",\"avgPrice\":\"8.204\"}]}"
    );
    let o_frame = concat!(
        "{\"topic\":\"order\",\"data\":[{\"symbol\":\"NEARUSDT\",",
        "\"orderStatus\":\"Cancelled\",\"reduceOnly\":false,\"cumExecQty\":\"0\"}]}"
    );
    parse_ws_private_frame(w_frame, &tx);
    parse_ws_private_frame(p_frame, &tx);
    parse_ws_private_frame(o_frame, &tx);
    let ok_w = matches!(
        rx.try_recv(),
        Ok(GatewayEvent::WalletUpdated { wallet_1e8 }) if wallet_1e8 == 6_628_080_000
    );
    let ok_p = matches!(
        rx.try_recv(),
        Ok(GatewayEvent::WsPositionUpdate {
            slot_idx: 3,
            is_open: true,
            real_side: Side::Bid,
            verified_avg: Some(PriceTicks(8_204)),
            real_lots: LotQty(26),
        })
    );
    let ok_o = matches!(
        rx.try_recv(),
        Ok(GatewayEvent::WsEntryOrderTerminated { slot_idx: 10 })
    );
    let mut lag_est = OnlineIndexerLagEstimator::new();
    let sample_p = serde_json::json!({
        "result": { "list": [{ "updatedTime": "1700000000000" }] }
    });
    lag_est.observe_closed_pnl_payload(&sample_p, 1_700_000_000_600_000_000);
    lag_est.observe_empty_retry();
    let ok_lag =
        lag_est.current_lag_ns() >= 350_000_000 && lag_est.adaptive_poll_delay_ns(2) >= 700_000_000;
    ok_w && ok_p && ok_o && ok_lag
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::{TcpListener, TcpStream};

    fn drain_req(rdr: &mut BufReader<TcpStream>) -> bool {
        let mut clen = 0_usize;
        loop {
            let mut line = String::new();
            match rdr.read_line(&mut line) {
                Ok(0) | Err(_) => return false,
                Ok(_) => {
                    if line == "\r\n" || line == "\n" {
                        break;
                    }
                    if let Some((k, v)) = line.split_once(':') {
                        if k.trim().eq_ignore_ascii_case("content-length") {
                            clen = v.trim().parse::<usize>().unwrap_or(0);
                        }
                    }
                }
            }
        }
        if clen > 0 {
            let mut body = vec![0_u8; clen];
            if rdr.read_exact(&mut body).is_err() {
                return false;
            }
        }
        true
    }

    fn write_json_ok(writer: &mut TcpStream, body: &str, close: bool) {
        let conn = if close { "close" } else { "keep-alive" };
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: {}\r\n\r\n{}",
            body.len(),
            conn,
            body
        );
        let _ = writer.write_all(resp.as_bytes());
        let _ = writer.flush();
    }

    #[test]
    fn test_gateway_ws_parser_and_p0a_emergency_flatten_slot_flow() {
        assert!(verify_ws_private_parser_self_check());

        let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
            return;
        };
        let Ok(addr) = listener.local_addr() else {
            return;
        };
        let srv = thread::spawn(move || {
            let Ok((mut writer, _)) = listener.accept() else {
                return;
            };
            let Ok(reader_sock) = writer.try_clone() else {
                return;
            };
            let mut rdr = BufReader::new(reader_sock);
            // 1. set_position_trading_stop
            if drain_req(&mut rdr) {
                write_json_ok(&mut writer, "{\"retCode\":0}", false);
            }
            // 2. cancel_all_symbol_orders
            if drain_req(&mut rdr) {
                write_json_ok(&mut writer, "{\"retCode\":0}", false);
            }
            // 3. query_open_position_verified (open = 2.6 AVAX)
            if drain_req(&mut rdr) {
                write_json_ok(
                    &mut writer,
                    concat!(
                        "{\"retCode\":0,\"result\":{\"list\":[",
                        "{\"symbol\":\"AVAXUSDT\",\"side\":\"Sell\",\"size\":\"2.6\",\"avgPrice\":\"8.030\"}",
                        "]}}"
                    ),
                    false,
                );
            }
            // 4. flatten_position_market_ioc (POST /v5/order/create Market IOC)
            if drain_req(&mut rdr) {
                write_json_ok(&mut writer, "{\"retCode\":0}", false);
            }
            // 5. query_open_position_verified (now size == 0)
            if drain_req(&mut rdr) {
                write_json_ok(
                    &mut writer,
                    "{\"retCode\":0,\"result\":{\"list\":[{\"size\":\"0\",\"side\":\"Sell\",\"avgPrice\":\"0\"}]}}",
                    false,
                );
            }
            // 6. query_closed_pnl_raw
            if drain_req(&mut rdr) {
                write_json_ok(
                    &mut writer,
                    "{\"retCode\":0,\"result\":{\"list\":[{\"closedPnl\":\"-0.2400\"}]}}",
                    true,
                );
            }
        });

        let base = format!("http://127.0.0.1:{}", addr.port());
        let (evt_tx, evt_rx) = mpsc::sync_channel::<GatewayEvent>(8);
        if let Some(mut client) =
            BybitV5LiveClient::with_base_url("k_test".to_string(), "s_test".to_string(), &base)
        {
            process_gateway_command(
                &mut client,
                GatewayCommand::EmergencyFlattenSlot {
                    slot_idx: 3,
                    open_side: Side::Ask,
                    open_lots: LotQty(26),
                    backup_sl: PriceTicks(9_011),
                },
                &evt_tx,
            );
            assert!(matches!(
                evt_rx.try_recv(),
                Ok(GatewayEvent::SlotClosedDetected { slot_idx: 3, .. })
            ));
        }
        let _ = srv.join();
    }
}
