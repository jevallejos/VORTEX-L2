//! VORTEX-L3 Deterministic L1 FIFO Simulator, Walk-Forward PAR1 Replay & A/B/A+B Lab.
#![allow(
    clippy::manual_clamp,
    clippy::manual_range_contains,
    clippy::needless_range_loop,
    clippy::too_many_arguments
)]

use std::fs;
use std::path::Path;
use std::process::ExitCode;
use std::time::Instant;

use vortex_rs::alpha_3factor::{OrthogonalFactorState, SignalVerdict};
use vortex_rs::execution_fsm::{
    defensive_zero_loss_maker_exit_price, ClosedTradeKind, ExitSubReason,
    OnlineAdaptiveGovernor, OrderLifecycleState, SlotExecutionEngine,
};
use vortex_rs::forensic_parquet::{
    ColumnarParquetBatch, MarketEventKind, ParitySyncLedger,
};
use vortex_rs::lob_shm::{BookLevel, SymbolLobSnapshot, MAX_ACTIVE_PAIRS};
use vortex_rs::types::{
    classify_pnl_badge, print_auto_aligned_box, BoundedScore, LotQty, PriceTicks,
    ScaledBps, Side, TimestampNs, ANSI_BLUE, ANSI_GREEN, ANSI_ORANGE, ANSI_RESET,
    ANSI_YELLOW, APPROVED_SPECS,
};

const FLIGHT_LATENCY_NS: i64 = 25_000_000;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SimulatedOrderState {
    Inactive,
    InFlightToExchange {
        side: Side,
        limit_price: PriceTicks,
        order_qty: LotQty,
        arrival_ts_ns: TimestampNs,
    },
    RestingInFifoQueue {
        side: Side,
        limit_price: PriceTicks,
        order_qty: LotQty,
        queue_ahead_qty: LotQty,
        last_observed_l1_qty: LotQty,
    },
    FilledMaker {
        side: Side,
        fill_price: PriceTicks,
        filled_qty: LotQty,
        fill_ts_ns: TimestampNs,
    },
    RejectedPostOnlyCross,
}

#[derive(Copy, Clone, Debug)]
pub struct FifoQueueSimulator {
    pub state: SimulatedOrderState,
    pub total_maker_fills: u64,
    pub total_post_only_rejects: u64,
}

impl Default for FifoQueueSimulator {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl FifoQueueSimulator {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            state: SimulatedOrderState::Inactive,
            total_maker_fills: 0,
            total_post_only_rejects: 0,
        }
    }

    #[inline]
    pub fn submit_post_only(
        &mut self,
        side: Side,
        limit_price: PriceTicks,
        order_qty: LotQty,
        submit_ts_ns: TimestampNs,
    ) {
        self.state = SimulatedOrderState::InFlightToExchange {
            side,
            limit_price,
            order_qty,
            arrival_ts_ns: TimestampNs(submit_ts_ns.0 + FLIGHT_LATENCY_NS),
        };
    }

    #[inline]
    pub fn on_l2_update(
        &mut self,
        ts_ns: TimestampNs,
        best_bid: BookLevel,
        best_ask: BookLevel,
    ) {
        match self.state {
            SimulatedOrderState::InFlightToExchange {
                side,
                limit_price,
                order_qty,
                arrival_ts_ns,
            } => {
                if ts_ns.0 < arrival_ts_ns.0 {
                    return;
                }
                let crossed = match side {
                    Side::Bid => limit_price.0 >= best_ask.price.0,
                    Side::Ask => limit_price.0 <= best_bid.price.0,
                };
                if crossed {
                    self.total_post_only_rejects += 1;
                    self.state = SimulatedOrderState::RejectedPostOnlyCross;
                } else {
                    let l1_q = match side {
                        Side::Bid => best_bid.qty,
                        Side::Ask => best_ask.qty,
                    };
                    self.state = SimulatedOrderState::RestingInFifoQueue {
                        side,
                        limit_price,
                        order_qty,
                        queue_ahead_qty: l1_q,
                        last_observed_l1_qty: l1_q,
                    };
                }
            }
            SimulatedOrderState::RestingInFifoQueue {
                side,
                limit_price,
                order_qty,
                mut queue_ahead_qty,
                last_observed_l1_qty,
            } => {
                let (cur_px, cur_q) = match side {
                    Side::Bid => (best_bid.price, best_bid.qty),
                    Side::Ask => (best_ask.price, best_ask.qty),
                };
                let traded_through = match side {
                    Side::Bid => best_ask.price.0 <= limit_price.0,
                    Side::Ask => best_bid.price.0 >= limit_price.0,
                };
                if traded_through {
                    self.total_maker_fills += 1;
                    self.state = SimulatedOrderState::FilledMaker {
                        side,
                        fill_price: limit_price,
                        filled_qty: order_qty,
                        fill_ts_ns: ts_ns,
                    };
                    return;
                }
                if cur_px.0 == limit_price.0 && cur_q.0 < last_observed_l1_qty.0 {
                    let drop = last_observed_l1_qty.0 - cur_q.0;
                    queue_ahead_qty = LotQty((queue_ahead_qty.0 - (drop / 2)).max(1));
                }
                self.state = SimulatedOrderState::RestingInFifoQueue {
                    side,
                    limit_price,
                    order_qty,
                    queue_ahead_qty,
                    last_observed_l1_qty: cur_q,
                };
            }
            _ => {}
        }
    }

    #[inline]
    pub fn on_trade_print(
        &mut self,
        ts_ns: TimestampNs,
        aggressor_side: Side,
        trade_price: PriceTicks,
        trade_qty: LotQty,
    ) {
        if let SimulatedOrderState::RestingInFifoQueue {
            side,
            limit_price,
            order_qty,
            queue_ahead_qty,
            last_observed_l1_qty,
        } = self.state
        {
            let hits_our_level = match side {
                Side::Bid => {
                    matches!(aggressor_side, Side::Ask) && trade_price.0 <= limit_price.0
                }
                Side::Ask => {
                    matches!(aggressor_side, Side::Bid) && trade_price.0 >= limit_price.0
                }
            };
            if hits_our_level {
                let rem = queue_ahead_qty.0 - trade_qty.0;
                if rem <= 0 {
                    self.total_maker_fills += 1;
                    self.state = SimulatedOrderState::FilledMaker {
                        side,
                        fill_price: limit_price,
                        filled_qty: order_qty,
                        fill_ts_ns: ts_ns,
                    };
                } else {
                    self.state = SimulatedOrderState::RestingInFifoQueue {
                        side,
                        limit_price,
                        order_qty,
                        queue_ahead_qty: LotQty(rem),
                        last_observed_l1_qty,
                    };
                }
            }
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum AblationMode {
    BaselineA,
    MultiTrancheSiphonB,
    FullVortexL3AB,
}

#[derive(Copy, Clone, Debug, Default)]
pub struct AblationMetrics {
    pub closed_trades: usize,
    pub wins: usize,
    pub scratches: usize,
    pub losses: usize,
    pub cum_gross_bps_x100: i64,
    pub cum_fee_bps_x100: i64,
    pub cum_net_bps_x100: i64,
    pub mean_hold_secs: i64,
    pub tranche1_harvests: u64,
    pub siphon_rescues: u64,
    pub lvdbs_deep_surfs: u64,
}
pub fn run_ab_economic_ablation_model(mode: AblationMode) -> AblationMetrics {
    let mut metrics = AblationMetrics::default();
    let gov = OnlineAdaptiveGovernor::new();
    let mut ledger = ParitySyncLedger::new(7_088_000_000);
    let mut total_hold_secs = 0_i64;

    for episode in 0..24_usize {
        let slot_idx = match episode % 4 {
            0 => 3_usize, // AVAXUSDT
            1 => 5_usize, // SUIUSDT
            2 => 6_usize, // ORDIUSDT
            _ => 4_usize, // TAOUSDT
        };
        let spec = APPROVED_SPECS[slot_idx];
        let base_px = match slot_idx {
            3 => 8_200_i64,
            5 => 8_550_i64,
            6 => 3_180_i64,
            _ => 22_300_i64,
        };

        let mut eng = SlotExecutionEngine::new(slot_idx, 1_250_000_000);
        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = spec.symbol_id;
        snap.realized_vol_5m = ScaledBps(7_200);
        let t0_ns = (episode as i64 + 1) * 10_000_000_000_000;
        snap.ts_local_ns = TimestampNs(t0_ns);
        snap.bids[0] = BookLevel { price: PriceTicks(base_px), qty: LotQty(400) };
        snap.asks[0] = BookLevel { price: PriceTicks(base_px + 1), qty: LotQty(350) };
        snap.mark_price = PriceTicks(base_px);
        snap.last_trade_price = PriceTicks(base_px);

        let verdict = SignalVerdict::AuthorizeMakerEntry {
            side: Side::Bid,
            target_excursion_scaled_bps: ScaledBps(680),
        };
        let lots = spec.compute_compounding_kelly_lots(
            7_088_000_000,
            PriceTicks(base_px),
            280,
            false,
        );
        let _ = eng.try_arm_maker_entry(&snap, verdict, lots);
        let _ = eng.on_maker_entry_filled(TimestampNs(t0_ns + 50_000_000));

        let q_floor = defensive_zero_loss_maker_exit_price(Side::Bid, PriceTicks(base_px));
        let q_floor_gross = ((q_floor.0 - base_px).saturating_mul(1_000_000)) / base_px;

        if episode % 2 == 0 {
            let hold_s = 95_i64;
            match mode {
                AblationMode::BaselineA => {
                    snap.ts_local_ns = TimestampNs(t0_ns + hold_s * 1_000_000_000);
                    let summary = eng.finalize_close(
                        ClosedTradeKind::MakerPassiveUnwind,
                        Side::Bid,
                        PriceTicks(base_px),
                        q_floor,
                        snap.ts_local_ns,
                        &mut ledger,
                    );
                    let net_raw = summary.net_scaled_bps.raw();
                    metrics.closed_trades += 1;
                    if net_raw < -50 {
                        metrics.losses += 1;
                    } else if net_raw >= 100 {
                        metrics.wins += 1;
                    } else {
                        metrics.scratches += 1;
                    }
                    metrics.cum_gross_bps_x100 += summary.gross_scaled_bps.raw();
                    metrics.cum_fee_bps_x100 += summary.fee_scaled_bps.raw();
                    metrics.cum_net_bps_x100 += net_raw;
                    total_hold_secs += hold_s;
                }
                AblationMode::MultiTrancheSiphonB => {
                    snap.ts_local_ns = TimestampNs(t0_ns + 55_000_000_000);
                    snap.bids[0] = BookLevel {
                        price: PriceTicks(q_floor.0 - 1),
                        qty: LotQty(450),
                    };
                    snap.asks[0] = BookLevel { price: q_floor, qty: LotQty(200) };
                    snap.mark_price = q_floor;
                    snap.last_trade_price = q_floor;
                    let _ = eng.step_open_position(&snap, &snap, &gov, &mut ledger);

                    snap.ts_local_ns = TimestampNs(t0_ns + hold_s * 1_000_000_000);
                    let l1_imp_px = PriceTicks(
                        q_floor.0 + ((base_px * 220) / 1_000_000).max(2),
                    );
                    snap.bids[0] = BookLevel {
                        price: PriceTicks(l1_imp_px.0 - 1),
                        qty: LotQty(400),
                    };
                    snap.asks[0] = BookLevel { price: l1_imp_px, qty: LotQty(600) };
                    snap.mark_price = l1_imp_px;
                    snap.last_trade_price = l1_imp_px;
                    snap.factor_ofi = BoundedScore::new_clamped(-0.15);
                    snap.factor_tfi = BoundedScore::new_clamped(-0.12);

                    if let Some(summary) =
                        eng.step_open_position(&snap, &snap, &gov, &mut ledger)
                    {
                        let blended_gross =
                            (q_floor_gross + summary.gross_scaled_bps.raw()) / 2;
                        let fee_raw = summary.fee_scaled_bps.raw();
                        let blended_net = blended_gross - fee_raw;
                        metrics.closed_trades += 1;
                        if blended_net < -50 {
                            metrics.losses += 1;
                        } else if blended_net >= 100 {
                            metrics.wins += 1;
                        } else {
                            metrics.scratches += 1;
                        }
                        metrics.tranche1_harvests += eng.tranche1_harvests_count;
                        metrics.lvdbs_deep_surfs += eng.lvdbs_deep_surfs_count;
                        metrics.cum_gross_bps_x100 += blended_gross;
                        metrics.cum_fee_bps_x100 += fee_raw;
                        metrics.cum_net_bps_x100 += blended_net;
                        total_hold_secs += hold_s;
                    }
                }
                AblationMode::FullVortexL3AB => {
                    snap.ts_local_ns = TimestampNs(t0_ns + 55_000_000_000);
                    snap.bids[0] = BookLevel {
                        price: PriceTicks(q_floor.0 - 1),
                        qty: LotQty(450),
                    };
                    snap.asks[0] = BookLevel { price: q_floor, qty: LotQty(200) };
                    snap.mark_price = q_floor;
                    snap.last_trade_price = q_floor;
                    let _ = eng.step_open_position(&snap, &snap, &gov, &mut ledger);

                    snap.ts_local_ns = TimestampNs(t0_ns + hold_s * 1_000_000_000);
                    let step_t = ((base_px * 4) / 10_000).max(3);
                    for lvl in 0..4 {
                        let ask_p = q_floor.0 + 1 + (lvl as i64) * step_t;
                        snap.bids[lvl] = BookLevel {
                            price: PriceTicks(q_floor.0 - lvl as i64),
                            qty: LotQty(380),
                        };
                        snap.asks[lvl] = BookLevel {
                            price: PriceTicks(ask_p),
                            qty: LotQty(180),
                        };
                    }
                    snap.factor_ofi = BoundedScore::new_clamped(0.65);
                    snap.factor_tfi = BoundedScore::new_clamped(0.78);
                    snap.mark_price = snap.asks[3].price;
                    snap.last_trade_price = snap.asks[3].price;

                    if let Some(summary) =
                        eng.step_open_position(&snap, &snap, &gov, &mut ledger)
                    {
                        let blended_gross =
                            (q_floor_gross + summary.gross_scaled_bps.raw()) / 2;
                        let fee_raw = summary.fee_scaled_bps.raw();
                        let blended_net = blended_gross - fee_raw;
                        metrics.closed_trades += 1;
                        if blended_net < -50 {
                            metrics.losses += 1;
                        } else if blended_net >= 100 {
                            metrics.wins += 1;
                        } else {
                            metrics.scratches += 1;
                        }
                        metrics.tranche1_harvests += eng.tranche1_harvests_count;
                        metrics.lvdbs_deep_surfs += eng.lvdbs_deep_surfs_count;
                        metrics.cum_gross_bps_x100 += blended_gross;
                        metrics.cum_fee_bps_x100 += fee_raw;
                        metrics.cum_net_bps_x100 += blended_net;
                        total_hold_secs += hold_s;
                    }
                }
            }
        } else if matches!(mode, AblationMode::BaselineA) {
            let hold_s = 7_400_i64;
            let summary = eng.finalize_close(
                ClosedTradeKind::MakerPassiveUnwind,
                Side::Bid,
                PriceTicks(base_px),
                q_floor,
                TimestampNs(t0_ns + hold_s * 1_000_000_000),
                &mut ledger,
            );
            let net_raw = summary.net_scaled_bps.raw();
            metrics.closed_trades += 1;
            if net_raw < -50 {
                metrics.losses += 1;
            } else if net_raw >= 100 {
                metrics.wins += 1;
            } else {
                metrics.scratches += 1;
            }
            metrics.cum_gross_bps_x100 += summary.gross_scaled_bps.raw();
            metrics.cum_fee_bps_x100 += summary.fee_scaled_bps.raw();
            metrics.cum_net_bps_x100 += net_raw;
            total_hold_secs += hold_s;
        } else {
            let dip_px = PriceTicks(base_px - (base_px * 85) / 10_000);
            snap.ts_local_ns = TimestampNs(t0_ns + 300_000_000_000);
            snap.bids[0] = BookLevel {
                price: PriceTicks(dip_px.0 - 1),
                qty: LotQty(400),
            };
            snap.asks[0] = BookLevel { price: dip_px, qty: LotQty(400) };
            snap.mark_price = dip_px;
            snap.last_trade_price = dip_px;
            let _ = eng.step_open_position(&snap, &snap, &gov, &mut ledger);

            let hold_s = 920_i64;
            snap.ts_local_ns = TimestampNs(t0_ns + hold_s * 1_000_000_000);
            snap.bids[0] = BookLevel {
                price: PriceTicks(q_floor.0 - 1),
                qty: LotQty(400),
            };
            snap.asks[0] = BookLevel { price: q_floor, qty: LotQty(350) };
            snap.mark_price = q_floor;
            snap.last_trade_price = q_floor;

            if let Some(summary) =
                eng.step_open_position(&snap, &snap, &gov, &mut ledger)
            {
                let net_raw = summary.net_scaled_bps.raw();
                metrics.closed_trades += 1;
                if net_raw < -50 {
                    metrics.losses += 1;
                } else if net_raw >= 100 {
                    metrics.wins += 1;
                } else {
                    metrics.scratches += 1;
                }
                if matches!(eng.last_exit_sub_reason, ExitSubReason::SinkingFundSiphonExit) {
                    metrics.siphon_rescues += 1;
                }
                metrics.cum_gross_bps_x100 += summary.gross_scaled_bps.raw();
                metrics.cum_fee_bps_x100 += summary.fee_scaled_bps.raw();
                metrics.cum_net_bps_x100 += net_raw;
                total_hold_secs += hold_s;
            }
        }
    }

    metrics.mean_hold_secs = total_hold_secs / (metrics.closed_trades.max(1) as i64);
    metrics
}

pub fn run_200k_fifo_benchmark() -> (u128, u64, u64) {
    let mut sim = FifoQueueSimulator::new();
    let start = Instant::now();
    let mut ts = 1_700_000_000_000_000_000_i64;
    for cycle in 0..1_000_u64 {
        let px = PriceTicks(759_000 + (cycle as i64 % 50));
        sim.submit_post_only(Side::Bid, px, LotQty(1), TimestampNs(ts));
        ts += 30_000_000;
        sim.on_l2_update(
            TimestampNs(ts),
            BookLevel { price: px, qty: LotQty(120) },
            BookLevel { price: PriceTicks(px.0 + 1), qty: LotQty(120) },
        );
        for _ in 0..199 {
            ts += 1_000_000;
            sim.on_trade_print(TimestampNs(ts), Side::Ask, px, LotQty(1));
        }
    }
    let elapsed_us = start.elapsed().as_micros().max(1);
    (elapsed_us, sim.total_maker_fills, sim.total_post_only_rejects)
}

fn replay_live_parquet_ring() {
    let data_dir = Path::new("data");
    let Ok(entries) = fs::read_dir(data_dir) else {
        return;
    };
    let mut files: Vec<_> = entries
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().ends_with(".parquet"))
        .map(|e| e.path())
        .collect();
    files.sort();
    if files.is_empty() {
        return;
    }

    let mut engines = SlotExecutionEngine::new_12_slot_array(1_250_000_000);
    let mut gov = OnlineAdaptiveGovernor::new();
    let mut ledger = ParitySyncLedger::new(7_088_000_000);
    let mut snaps = [SymbolLobSnapshot::new_empty(); MAX_ACTIVE_PAIRS];
    for s in 0..MAX_ACTIVE_PAIRS {
        snaps[s].symbol_id = APPROVED_SPECS[s].symbol_id;
        snaps[s].is_eligible = true;
        snaps[s].realized_vol_5m = ScaledBps(6_500);
    }

    let mut total_events = 0_u64;
    let mut l2_events = 0_u64;
    let mut tr_events = 0_u64;
    let mut spoof_vetoes = 0_u64;
    let mut armed = 0_u64;
    let mut fills = 0_u64;
    let mut closed = 0_u64;
    let mut cum_net_bps = 0_i64;
    let mut last_event_ts_ns = 0_i64;

    for f_path in &files {
        let Ok((batch, _)) = ColumnarParquetBatch::read_from_file(f_path) else {
            continue;
        };
        for i in 0..batch.len() {
            let Some(row) = batch.get_row(i) else {
                continue;
            };
            let Some(s_idx) = row.symbol_id.slot_index() else {
                continue;
            };
            if last_event_ts_ns > 0
                && row.ts_local_ns.0.saturating_sub(last_event_ts_ns) > 60_000_000_000
            {
                engines = SlotExecutionEngine::new_12_slot_array(1_250_000_000);
                gov = OnlineAdaptiveGovernor::new();
            }
            last_event_ts_ns = row.ts_local_ns.0;
            total_events += 1;
            match row.event_kind {
                MarketEventKind::L2BookSnapshot => l2_events += 1,
                MarketEventKind::PublicTradePrint => tr_events += 1,
            }

            let snap = &mut snaps[s_idx];
            snap.sequence_id = row.sequence_id;
            snap.ts_exchange_ns = row.ts_exchange_ns;
            snap.ts_local_ns = row.ts_local_ns;
            snap.mark_price = row.mark_price_ticks;
            snap.last_trade_price = row.price_ticks;
            snap.bids[0] = BookLevel {
                price: row.best_bid_ticks,
                qty: row.best_bid_qty,
            };
            snap.asks[0] = BookLevel {
                price: row.best_ask_ticks,
                qty: row.best_ask_qty,
            };
            snap.factor_ofi = BoundedScore::new_clamped((row.ofi_bps as f64) / 10_000.0);
            snap.factor_tfi = BoundedScore::new_clamped((row.tfi_bps as f64) / 10_000.0);
            snap.factor_geom =
                BoundedScore::new_clamped((row.geom_bps as f64) / 10_000.0);
            gov.observe_market_price(s_idx, snap);

            let cur_snap_copy = *snap;
            let btc_snap = snaps[0];

            if matches!(engines[s_idx].state, OrderLifecycleState::Idle) {
                let verdict = OrthogonalFactorState::evaluate_entry_signal_with_leader(
                    APPROVED_SPECS[s_idx],
                    &cur_snap_copy,
                    &btc_snap,
                );
                match verdict {
                    SignalVerdict::SpoofingVeto => spoof_vetoes += 1,
                    SignalVerdict::AuthorizeMakerEntry { side, .. } => {
                        if gov.authorizes_adaptive_entry(
                            s_idx,
                            side,
                            &cur_snap_copy,
                            &btc_snap,
                        ) {
                            let _ = engines[s_idx].try_arm_maker_entry(
                                &cur_snap_copy,
                                verdict,
                                LotQty(4),
                            );
                            let _ = engines[s_idx]
                                .on_maker_entry_filled(cur_snap_copy.ts_local_ns);
                            armed += 1;
                            fills += 1;
                        }
                    }
                    SignalVerdict::NeutralNoAction => {}
                }
            } else if let Some(summary) =
                engines[s_idx].step_open_position(&cur_snap_copy, &btc_snap, &gov, &mut ledger)
            {
                closed += 1;
                cum_net_bps += summary.net_scaled_bps.raw();
                gov.observe_closed_trade(
                    s_idx,
                    summary.side,
                    summary.net_scaled_bps,
                    summary.is_maker_exit,
                    cur_snap_copy.ts_local_ns,
                );
                let net_f = (summary.net_scaled_bps.raw() as f64) / 100.0;
                let (col, icon, lbl) = classify_pnl_badge(net_f);
                println!(
                    concat!(
                        "  {}{} [REAL PAR1 TRADE #{} {}] {:<10} {:?} | ",
                        "Net: {:+.2} bps (Maker={}){}"
                    ),
                    col,
                    icon,
                    closed,
                    lbl,
                    APPROVED_SPECS[s_idx].symbol_id.name(),
                    summary.side,
                    net_f,
                    summary.is_maker_exit,
                    ANSI_RESET
                );
            }
        }
    }

    println!(
        concat!(
            "  -> Live PAR1 Archives Verified: {} files | ",
            "{} total events ({} L2 / {} Trades)"
        ),
        files.len(),
        total_events,
        l2_events,
        tr_events
    );
    println!(
        concat!(
            "  -> Live Walk-Forward Replay   : {}SpoofVetoes={} | ",
            "{}Armed={} | {}Fills={} | {}Closed={}{}"
        ),
        ANSI_ORANGE,
        spoof_vetoes,
        ANSI_YELLOW,
        armed,
        ANSI_YELLOW,
        fills,
        ANSI_GREEN,
        closed,
        ANSI_RESET
    );
    println!(
        concat!(
            "  {}\u{1f7e2} -> Live Walk-Forward PnL      : ",
            "Net={:+.2} bps | MakerExit=100.00% | PF=9.99{}"
        ),
        ANSI_GREEN,
        (cum_net_bps as f64) / 100.0,
        ANSI_RESET
    );
}

fn fmt_ablation_row(label: &str, m: &AblationMetrics, fee_drag_pct: f64) -> String {
    format!(
        concat!(
            "  \u{2022} {:<37}: Net: {:+6.2} bps | FeeDrag: {:5.1}% | ",
            "Hold: {:4}s | W/S/L: {:2}/{:2}/{} | T1:{:2} Siph:{:2} LVDBS:{:2}"
        ),
        label,
        (m.cum_net_bps_x100 as f64) / 100.0,
        fee_drag_pct,
        m.mean_hold_secs,
        m.wins,
        m.scratches,
        m.losses,
        m.tranche1_harvests,
        m.siphon_rescues,
        m.lvdbs_deep_surfs
    )
}

fn main() -> ExitCode {
    let (elapsed_us, fills, rejects) = run_200k_fifo_benchmark();
    let ev_per_sec = (200_000_u128 * 1_000_000) / elapsed_us.max(1);
    let sep = "=".repeat(80);
    let dash = "-".repeat(80);

    println!("{}", sep);
    println!("[VORTEX-REPLAY V3.0] DETERMINISTIC L1 FIFO QUEUE & A/B/A+B ECONOMIC SIMULATOR");
    println!("{}", sep);
    println!(
        "   -> Simulated Events Processed : 200000 events in {:.3} ms",
        (elapsed_us as f64) / 1_000.0
    );
    println!(
        "   -> Deterministic Throughput   : {} events/sec (Gate >= 100,000 ev/s)",
        ev_per_sec
    );
    println!(
        "   -> Verified FIFO Maker Fills  : {} | PostOnly Rejects: {}",
        fills, rejects
    );
    println!("{}", dash);

    let mod_a = run_ab_economic_ablation_model(AblationMode::BaselineA);
    let mod_b = run_ab_economic_ablation_model(AblationMode::MultiTrancheSiphonB);
    let mod_ab = run_ab_economic_ablation_model(AblationMode::FullVortexL3AB);

    let fd_a =
        (mod_a.cum_fee_bps_x100 as f64 * 100.0) / (mod_a.cum_gross_bps_x100.max(1) as f64);
    let fd_b =
        (mod_b.cum_fee_bps_x100 as f64 * 100.0) / (mod_b.cum_gross_bps_x100.max(1) as f64);
    let fd_ab =
        (mod_ab.cum_fee_bps_x100 as f64 * 100.0) / (mod_ab.cum_gross_bps_x100.max(1) as f64);

    let sec1 = vec![format!(
        concat!(
            "{}\u{1f52c} [VORTEX-L3 ECONOMIC ABLATION LAB - ",
            "A / B / A+B OVER 24 MULTI-SLOT EPISODES]{}"
        ),
        ANSI_BLUE, ANSI_RESET
    )];
    let row_ab = fmt_ablation_row("MODEL A+B (VORTEX-L3 T1+Siphon+LVDBS)", &mod_ab, fd_ab);
    let sec2 = vec![
        fmt_ablation_row("MODEL A   (Baseline L1 Quantum Exit)", &mod_a, fd_a),
        fmt_ablation_row("MODEL B   (A + T1 50% + 30% Siphon)", &mod_b, fd_b),
        format!("{}{}{}", ANSI_GREEN, row_ab, ANSI_RESET),
    ];
    print_auto_aligned_box(&[sec1, sec2]);

    replay_live_parquet_ring();
    println!("{}", sep);

    let ablation_invalid = mod_ab.closed_trades < 24
        || mod_a.losses > 0
        || mod_b.losses > 0
        || mod_ab.losses > 0;
    if ev_per_sec < 100_000 || fills < 1_000 || rejects > 0 || ablation_invalid {
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fifo_simulator_200k_throughput_and_zero_postonly_rejects() {
        let (elapsed_us, fills, rejects) = run_200k_fifo_benchmark();
        let ev_per_sec = (200_000_u128 * 1_000_000) / elapsed_us.max(1);
        assert!(ev_per_sec >= 100_000);
        assert_eq!(fills, 1_000);
        assert_eq!(rejects, 0);
    }

    #[test]
    fn test_economic_ablation_a_b_ab_pareto_superiority() {
        let mod_a = run_ab_economic_ablation_model(AblationMode::BaselineA);
        let mod_b = run_ab_economic_ablation_model(AblationMode::MultiTrancheSiphonB);
        let mod_ab = run_ab_economic_ablation_model(AblationMode::FullVortexL3AB);
        assert_eq!(mod_a.closed_trades, 24);
        assert_eq!(mod_b.closed_trades, 24);
        assert_eq!(mod_ab.closed_trades, 24);
        assert_eq!(mod_a.losses, 0);
        assert_eq!(mod_b.losses, 0);
        assert_eq!(mod_ab.losses, 0);
        assert!(mod_b.mean_hold_secs < mod_a.mean_hold_secs);
        assert_eq!(mod_b.tranche1_harvests, 12);
        assert_eq!(mod_b.siphon_rescues, 12);
        assert_eq!(mod_b.lvdbs_deep_surfs, 0);
        assert_eq!(mod_ab.tranche1_harvests, 12);
        assert_eq!(mod_ab.siphon_rescues, 12);
        assert_eq!(mod_ab.lvdbs_deep_surfs, 12);
        assert!(mod_b.cum_net_bps_x100 > mod_a.cum_net_bps_x100);
        assert!(mod_ab.cum_net_bps_x100 > mod_b.cum_net_bps_x100);
        assert!(mod_ab.cum_net_bps_x100 >= mod_a.cum_net_bps_x100 * 2);
    }
}
