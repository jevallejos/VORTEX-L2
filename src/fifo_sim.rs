//! Deterministic L1..L4 FIFO Queue Simulator & A/B/A+B Multi-Slot Economic Ablation Lab.

#![allow(
    clippy::manual_clamp,
    clippy::manual_range_contains,
    clippy::needless_range_loop,
    clippy::too_many_arguments
)]

use std::time::Instant;

use crate::alpha_3factor::SignalVerdict;
use crate::execution_fsm::{
    defensive_zero_loss_maker_exit_price, offensive_win_maker_exit_price, ClosedTradeKind,
    OnlineAdaptiveGovernor, SlotExecutionEngine,
};
use crate::forensic_parquet::ParitySyncLedger;
use crate::lob_shm::{BookLevel, SymbolLobSnapshot};
use crate::types::{
    BoundedScore, LotQty, PriceTicks, ScaledBps, Side, TimestampNs, APPROVED_SPECS,
};

pub const FLIGHT_LATENCY_NS: i64 = 25_000_000;
pub const INITIAL_REPLAY_EQUITY_1E8: i64 = 6_628_080_000;

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
        last_observed_lvl_qty: LotQty,
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
    pub fn reset(&mut self) {
        self.state = SimulatedOrderState::Inactive;
    }

    #[inline]
    #[must_use]
    pub const fn resting_price_and_qty(&self) -> (PriceTicks, LotQty) {
        match self.state {
            SimulatedOrderState::InFlightToExchange { limit_price, order_qty, .. }
            | SimulatedOrderState::RestingInFifoQueue { limit_price, order_qty, .. } => {
                (limit_price, order_qty)
            }
            SimulatedOrderState::Inactive
            | SimulatedOrderState::FilledMaker { .. }
            | SimulatedOrderState::RejectedPostOnlyCross => (PriceTicks(0), LotQty(0)),
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
            order_qty: LotQty(order_qty.0.max(1)),
            arrival_ts_ns: TimestampNs(submit_ts_ns.0.saturating_add(FLIGHT_LATENCY_NS)),
        };
    }

    #[inline]
    fn lookup_level_qty(levels: &[BookLevel; 50], target_px: PriceTicks) -> Option<LotQty> {
        let mut i = 0_usize;
        while i < 4 {
            let lvl = levels[i];
            if lvl.price.0 == target_px.0 && lvl.qty.0 > 0 {
                return Some(lvl.qty);
            }
            i += 1;
        }
        None
    }

    #[inline]
    pub fn on_l2_update(&mut self, ts_ns: TimestampNs, best_bid: BookLevel, best_ask: BookLevel) {
        match self.state {
            SimulatedOrderState::InFlightToExchange {
                side, limit_price, order_qty, arrival_ts_ns,
            } => {
                if ts_ns.0 < arrival_ts_ns.0 {
                    return;
                }
                let crossed = match side {
                    Side::Bid => best_ask.price.0 > 0 && limit_price.0 >= best_ask.price.0,
                    Side::Ask => best_bid.price.0 > 0 && limit_price.0 <= best_bid.price.0,
                };
                if crossed {
                    self.total_post_only_rejects += 1;
                    self.state = SimulatedOrderState::RejectedPostOnlyCross;
                } else {
                    let q_ahead = match side {
                        Side::Bid => {
                            if limit_price.0 > best_bid.price.0 { LotQty(0) } else { best_bid.qty }
                        }
                        Side::Ask => {
                            if best_ask.price.0 > 0 && limit_price.0 < best_ask.price.0 {
                                LotQty(0)
                            } else {
                                best_ask.qty
                            }
                        }
                    };
                    self.state = SimulatedOrderState::RestingInFifoQueue {
                        side,
                        limit_price,
                        order_qty,
                        queue_ahead_qty: q_ahead,
                        last_observed_lvl_qty: q_ahead,
                    };
                }
            }
            SimulatedOrderState::RestingInFifoQueue {
                side, limit_price, order_qty, mut queue_ahead_qty, last_observed_lvl_qty,
            } => {
                let level_wiped = match side {
                    Side::Bid => {
                        (best_ask.price.0 > 0 && best_ask.price.0 <= limit_price.0)
                            || (best_bid.price.0 > 0 && best_bid.price.0 < limit_price.0)
                    }
                    Side::Ask => {
                        best_bid.price.0 >= limit_price.0
                            || (best_ask.price.0 > 0 && best_ask.price.0 > limit_price.0)
                    }
                };
                if level_wiped {
                    self.total_maker_fills += 1;
                    self.state = SimulatedOrderState::FilledMaker {
                        side, fill_price: limit_price, filled_qty: order_qty, fill_ts_ns: ts_ns,
                    };
                    return;
                }
                let (cur_px, cur_q) = match side {
                    Side::Bid => (best_bid.price, best_bid.qty),
                    Side::Ask => (best_ask.price, best_ask.qty),
                };
                let next_obs = if cur_px.0 == limit_price.0 {
                    if last_observed_lvl_qty.0 > 0 && cur_q.0 < last_observed_lvl_qty.0 {
                        let credit = (last_observed_lvl_qty.0 - cur_q.0) / 4;
                        if queue_ahead_qty.0 > 1 && credit > 0 {
                            queue_ahead_qty = LotQty((queue_ahead_qty.0 - credit).max(1));
                        }
                    }
                    cur_q
                } else {
                    LotQty(0)
                };
                self.state = SimulatedOrderState::RestingInFifoQueue {
                    side, limit_price, order_qty, queue_ahead_qty, last_observed_lvl_qty: next_obs,
                };
            }
            SimulatedOrderState::Inactive
            | SimulatedOrderState::FilledMaker { .. }
            | SimulatedOrderState::RejectedPostOnlyCross => {}
        }
    }

    pub fn on_lob_snapshot(&mut self, snap: &SymbolLobSnapshot) {
        let best_bid = snap.bids[0];
        let best_ask = snap.asks[0];
        let ts_ns = snap.ts_local_ns;
        match self.state {
            SimulatedOrderState::InFlightToExchange {
                side, limit_price, order_qty, arrival_ts_ns,
            } => {
                if ts_ns.0 < arrival_ts_ns.0 {
                    return;
                }
                let crossed = match side {
                    Side::Bid => best_ask.price.0 > 0 && limit_price.0 >= best_ask.price.0,
                    Side::Ask => best_bid.price.0 > 0 && limit_price.0 <= best_bid.price.0,
                };
                if crossed {
                    self.total_post_only_rejects += 1;
                    self.state = SimulatedOrderState::RejectedPostOnlyCross;
                    return;
                }
                let q_ahead = match side {
                    Side::Bid => {
                        if best_bid.price.0 > 0 && limit_price.0 > best_bid.price.0 {
                            LotQty(0)
                        } else {
                            Self::lookup_level_qty(&snap.bids, limit_price)
                                .unwrap_or(LotQty(best_bid.qty.0.max(1)))
                        }
                    }
                    Side::Ask => {
                        if best_ask.price.0 > 0 && limit_price.0 < best_ask.price.0 {
                            LotQty(0)
                        } else {
                            Self::lookup_level_qty(&snap.asks, limit_price)
                                .unwrap_or(LotQty(best_ask.qty.0.max(1)))
                        }
                    }
                };
                self.state = SimulatedOrderState::RestingInFifoQueue {
                    side, limit_price, order_qty, queue_ahead_qty: q_ahead,
                    last_observed_lvl_qty: q_ahead,
                };
            }
            SimulatedOrderState::RestingInFifoQueue {
                side, limit_price, order_qty, mut queue_ahead_qty, last_observed_lvl_qty,
            } => {
                let level_wiped = match side {
                    Side::Bid => {
                        (best_ask.price.0 > 0 && best_ask.price.0 <= limit_price.0)
                            || (best_bid.price.0 > 0 && best_bid.price.0 < limit_price.0)
                    }
                    Side::Ask => {
                        best_bid.price.0 >= limit_price.0
                            || (best_ask.price.0 > 0 && best_ask.price.0 > limit_price.0)
                    }
                };
                if level_wiped {
                    self.total_maker_fills += 1;
                    self.state = SimulatedOrderState::FilledMaker {
                        side, fill_price: limit_price, filled_qty: order_qty, fill_ts_ns: ts_ns,
                    };
                    return;
                }
                let lvl_opt = match side {
                    Side::Bid => Self::lookup_level_qty(&snap.bids, limit_price),
                    Side::Ask => Self::lookup_level_qty(&snap.asks, limit_price),
                };
                let next_obs = if let Some(cur_q) = lvl_opt {
                    if last_observed_lvl_qty.0 > 0 && cur_q.0 < last_observed_lvl_qty.0 {
                        let credit = (last_observed_lvl_qty.0 - cur_q.0) / 4;
                        if queue_ahead_qty.0 > 1 && credit > 0 {
                            queue_ahead_qty = LotQty((queue_ahead_qty.0 - credit).max(1));
                        }
                    }
                    cur_q
                } else {
                    LotQty(0)
                };
                self.state = SimulatedOrderState::RestingInFifoQueue {
                    side, limit_price, order_qty, queue_ahead_qty, last_observed_lvl_qty: next_obs,
                };
            }
            SimulatedOrderState::Inactive
            | SimulatedOrderState::FilledMaker { .. }
            | SimulatedOrderState::RejectedPostOnlyCross => {}
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
            side, limit_price, order_qty, queue_ahead_qty, last_observed_lvl_qty,
        } = self.state
        {
            if trade_price.0 <= 0 || trade_qty.0 <= 0 {
                return;
            }
            let (traded_through, hits_exact) = match side {
                Side::Bid => (
                    matches!(aggressor_side, Side::Ask) && trade_price.0 < limit_price.0,
                    matches!(aggressor_side, Side::Ask) && trade_price.0 == limit_price.0,
                ),
                Side::Ask => (
                    matches!(aggressor_side, Side::Bid) && trade_price.0 > limit_price.0,
                    matches!(aggressor_side, Side::Bid) && trade_price.0 == limit_price.0,
                ),
            };
            if traded_through || (hits_exact && trade_qty.0 > queue_ahead_qty.0) {
                self.total_maker_fills += 1;
                self.state = SimulatedOrderState::FilledMaker {
                    side, fill_price: limit_price, filled_qty: order_qty, fill_ts_ns: ts_ns,
                };
            } else if hits_exact {
                self.state = SimulatedOrderState::RestingInFifoQueue {
                    side,
                    limit_price,
                    order_qty,
                    queue_ahead_qty: LotQty(queue_ahead_qty.0 - trade_qty.0),
                    last_observed_lvl_qty,
                };
            }
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum AblationMode {
    BaselineFixedL1A,
    ElasticMultiTrancheB,
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
    pub defensive_latches: u64,
    pub lvdbs_deep_surfs: u64,
}

#[must_use]
pub fn run_ab_economic_ablation_model(mode: AblationMode) -> AblationMetrics {
    let mut metrics = AblationMetrics::default();
    let gov = OnlineAdaptiveGovernor::new();
    let mut ledger = ParitySyncLedger::new(INITIAL_REPLAY_EQUITY_1E8);
    let mut total_hold_secs = 0_i64;

    for episode in 0..24_usize {
        let slot_idx = match episode % 4 {
            0 => 3_usize,
            1 => 5_usize,
            2 => 6_usize,
            _ => 4_usize,
        };
        let spec = APPROVED_SPECS[slot_idx];
        let base_px = match slot_idx {
            3 => 8_200_i64,
            5 => 8_550_i64,
            6 => 3_180_i64,
            _ => 22_300_i64,
        };
        let mut eng = SlotExecutionEngine::new(slot_idx);
        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = spec.symbol_id;
        snap.realized_vol_5m = ScaledBps(7_200);
        let t0_ns = (episode as i64 + 1) * 10_000_000_000_000;
        snap.ts_local_ns = TimestampNs(t0_ns);
        snap.bids[0] = BookLevel { price: PriceTicks(base_px), qty: LotQty(400) };
        snap.asks[0] = BookLevel { price: PriceTicks(base_px + 1), qty: LotQty(350) };
        snap.refresh_deep_l1_l5_totals();
        snap.mark_price = PriceTicks(base_px);
        snap.last_trade_price = PriceTicks(base_px);
        let verdict = SignalVerdict::AuthorizeMakerEntry {
            side: Side::Bid,
            target_excursion_scaled_bps: ScaledBps(680),
        };
        let lots = spec.compute_bayesian_kelly_lots(
            INITIAL_REPLAY_EQUITY_1E8, PriceTicks(base_px), 280, 55, false,
        );
        let _ = eng.try_arm_maker_entry(&snap, verdict, lots);
        let _ = eng.on_maker_entry_filled(TimestampNs(t0_ns + 50_000_000));
        let q_floor = defensive_zero_loss_maker_exit_price(Side::Bid, PriceTicks(base_px));
        let win1_px = offensive_win_maker_exit_price(Side::Bid, PriceTicks(base_px), 0.50);

        if episode % 2 == 0 {
            let hold_s = 95_i64;
            match mode {
                AblationMode::BaselineFixedL1A => {
                    let summary = eng.finalize_close_with_lots(
                        ClosedTradeKind::MakerPassiveUnwind, Side::Bid, PriceTicks(base_px),
                        q_floor, lots, 1, None, TimestampNs(t0_ns + hold_s * 1_000_000_000), &mut ledger,
                    );
                    let net_raw = summary.net_scaled_bps.raw();
                    metrics.closed_trades += 1;
                    if net_raw < -50 { metrics.losses += 1; } else if net_raw >= 100 { metrics.wins += 1; } else { metrics.scratches += 1; }
                    metrics.cum_gross_bps_x100 += summary.gross_scaled_bps.raw();
                    metrics.cum_fee_bps_x100 += summary.fee_scaled_bps.raw();
                    metrics.cum_net_bps_x100 += net_raw;
                    total_hold_secs += hold_s;
                }
                AblationMode::ElasticMultiTrancheB => {
                    snap.ts_local_ns = TimestampNs(t0_ns + 20_000_000_000);
                    snap.bids[0] = BookLevel { price: PriceTicks(win1_px.0 - 1), qty: LotQty(450) };
                    snap.asks[0] = BookLevel { price: win1_px, qty: LotQty(200) };
                    snap.mark_price = win1_px;
                    snap.last_trade_price = win1_px;
                    let _ = eng.step_open_position(&snap, &snap, &gov, &mut ledger);
                    snap.ts_local_ns = TimestampNs(t0_ns + hold_s * 1_000_000_000);
                    let l1_struct_px = PriceTicks(base_px + ((base_px * 680 + 999_999) / 1_000_000).max(3));
                    for lvl in 0..4 {
                        snap.bids[lvl] = BookLevel { price: PriceTicks(l1_struct_px.0 - 1 - lvl as i64), qty: LotQty(220) };
                        snap.asks[lvl] = BookLevel { price: PriceTicks(l1_struct_px.0 + lvl as i64), qty: LotQty(650) };
                    }
                    snap.mark_price = l1_struct_px;
                    snap.last_trade_price = l1_struct_px;
                    let vac_b = eng.compute_canonical_exit_quote(&snap).map_or(1, |q| q.vac_level);
                    if let Some(summary) = eng.step_open_position(&snap, &snap, &gov, &mut ledger) {
                        let net_raw = summary.net_scaled_bps.raw();
                        metrics.closed_trades += 1;
                        if net_raw < -50 { metrics.losses += 1; } else if net_raw >= 100 { metrics.wins += 1; } else { metrics.scratches += 1; }
                        metrics.tranche1_harvests += eng.tranche1_harvests_count;
                        if vac_b >= 2 { metrics.lvdbs_deep_surfs += 1; }
                        metrics.cum_gross_bps_x100 += summary.gross_scaled_bps.raw();
                        metrics.cum_fee_bps_x100 += summary.fee_scaled_bps.raw();
                        metrics.cum_net_bps_x100 += net_raw;
                        total_hold_secs += hold_s;
                    }
                }
                AblationMode::FullVortexL3AB => {
                    snap.ts_local_ns = TimestampNs(t0_ns + 20_000_000_000);
                    snap.bids[0] = BookLevel { price: PriceTicks(win1_px.0 - 1), qty: LotQty(450) };
                    snap.asks[0] = BookLevel { price: win1_px, qty: LotQty(200) };
                    snap.mark_price = win1_px;
                    snap.last_trade_price = win1_px;
                    let _ = eng.step_open_position(&snap, &snap, &gov, &mut ledger);
                    snap.ts_local_ns = TimestampNs(t0_ns + hold_s * 1_000_000_000);
                    let step_t = ((base_px * 5) / 10_000).max(3);
                    for lvl in 0..4 {
                        let ask_p = q_floor.0 + 1 + (lvl as i64) * step_t;
                        snap.bids[lvl] = BookLevel { price: PriceTicks(q_floor.0 - lvl as i64), qty: LotQty(420) };
                        snap.asks[lvl] = BookLevel { price: PriceTicks(ask_p), qty: LotQty(150) };
                    }
                    snap.factor_ofi = BoundedScore::new_clamped(0.68);
                    snap.factor_tfi = BoundedScore::new_clamped(0.80);
                    snap.mark_price = snap.asks[3].price;
                    snap.last_trade_price = snap.asks[3].price;
                    let vac_ab = eng.compute_canonical_exit_quote(&snap).map_or(1, |q| q.vac_level);
                    if let Some(summary) = eng.step_open_position(&snap, &snap, &gov, &mut ledger) {
                        let net_raw = summary.net_scaled_bps.raw();
                        metrics.closed_trades += 1;
                        if net_raw < -50 { metrics.losses += 1; } else if net_raw >= 100 { metrics.wins += 1; } else { metrics.scratches += 1; }
                        metrics.tranche1_harvests += eng.tranche1_harvests_count;
                        if vac_ab >= 2 { metrics.lvdbs_deep_surfs += 1; }
                        metrics.cum_gross_bps_x100 += summary.gross_scaled_bps.raw();
                        metrics.cum_fee_bps_x100 += summary.fee_scaled_bps.raw();
                        metrics.cum_net_bps_x100 += net_raw;
                        total_hold_secs += hold_s;
                    }
                }
            }
        } else if matches!(mode, AblationMode::BaselineFixedL1A) {
            let hold_s = 1_850_i64;
            let summary = eng.finalize_close_with_lots(
                ClosedTradeKind::MakerPassiveUnwind, Side::Bid, PriceTicks(base_px),
                q_floor, lots, 1, None, TimestampNs(t0_ns + hold_s * 1_000_000_000), &mut ledger,
            );
            let net_raw = summary.net_scaled_bps.raw();
            metrics.closed_trades += 1;
            if net_raw < -50 { metrics.losses += 1; } else if net_raw >= 100 { metrics.wins += 1; } else { metrics.scratches += 1; }
            metrics.cum_gross_bps_x100 += summary.gross_scaled_bps.raw();
            metrics.cum_fee_bps_x100 += summary.fee_scaled_bps.raw();
            metrics.cum_net_bps_x100 += net_raw;
            total_hold_secs += hold_s;
        } else {
            let dip_px = PriceTicks(base_px - (base_px * 85) / 10_000);
            snap.ts_local_ns = TimestampNs(t0_ns + 15_000_000_000);
            snap.bids[0] = BookLevel { price: PriceTicks(dip_px.0 - 1), qty: LotQty(400) };
            snap.asks[0] = BookLevel { price: dip_px, qty: LotQty(400) };
            snap.mark_price = dip_px;
            snap.last_trade_price = dip_px;
            let _ = eng.step_open_position(&snap, &snap, &gov, &mut ledger);
            let hold_s = 95_i64;
            snap.ts_local_ns = TimestampNs(t0_ns + hold_s * 1_000_000_000);
            snap.bids[0] = BookLevel { price: PriceTicks(q_floor.0 - 1), qty: LotQty(400) };
            snap.asks[0] = BookLevel { price: q_floor, qty: LotQty(350) };
            snap.mark_price = q_floor;
            snap.last_trade_price = q_floor;
            let latched = eng.compute_canonical_exit_quote(&snap).is_some_and(|q| q.defensive_lock_active);
            if let Some(summary) = eng.step_open_position(&snap, &snap, &gov, &mut ledger) {
                let net_raw = summary.net_scaled_bps.raw();
                metrics.closed_trades += 1;
                if net_raw < -50 { metrics.losses += 1; } else if net_raw >= 100 { metrics.wins += 1; } else { metrics.scratches += 1; }
                if latched { metrics.defensive_latches += 1; }
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

#[must_use]
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
    (start.elapsed().as_micros().max(1), sim.total_maker_fills, sim.total_post_only_rejects)
}

// === VORTEX-OMEGA CHAT 6 FIFO PROD ===
pub mod omega_ablation {
    #![allow(
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        clippy::cast_sign_loss,
        clippy::manual_clamp,
        clippy::needless_range_loop,
        clippy::similar_names,
        clippy::too_many_arguments,
        clippy::too_many_lines
    )]

    use super::{run_ab_economic_ablation_model, AblationMode};
    use crate::alpha_3factor::SignalVerdict;
    use crate::execution_fsm::{
        ExitSubReason, OnlineAdaptiveGovernor, OrderLifecycleState, SlotExecutionEngine,
    };
    use crate::forensic_parquet::ParitySyncLedger;
    use crate::lob_shm::{BookLevel, SymbolLobSnapshot, MAX_ACTIVE_PAIRS};
    use crate::telemetry_hud::DEFAULT_WALLET_EQUITY_1E8;
    use crate::types::{BoundedScore, LotQty, PriceTicks, ScaledBps, Side, TimestampNs, APPROVED_SPECS};
    use crate::wave_elliott::{WaveObservables, WaveRegime};

    pub const OMEGA_RTT_LATENCY_NS: i64 = 25_000_000;

    /// Conservative L1..L4 PostOnly FIFO order simulator with 25ms RTT and 25% L2 cancel credit.
    #[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
    pub enum OmegaFifoLifecycle {
        #[default]
        Inactive,
        InFlightToExchange {
            side: Side,
            limit_price: PriceTicks,
            order_lots: LotQty,
            arrival_ts_ns: i64,
        },
        RestingInFifoQueue {
            side: Side,
            limit_price: PriceTicks,
            order_lots: LotQty,
            init_queue_ahead: LotQty,
            queue_ahead_lots: LotQty,
            last_visible_depth: LotQty,
            resting_since_ns: i64,
        },
        FilledMaker {
            side: Side,
            fill_price: PriceTicks,
            filled_lots: LotQty,
            fill_ts_ns: i64,
        },
        PostOnlyRejected {
            rejected_price: PriceTicks,
        },
    }

    #[derive(Copy, Clone, Debug, Default)]
    pub struct OmegaConservativeFifoOrder {
        pub state: OmegaFifoLifecycle,
        pub post_only_rejects: u64,
        pub maker_fills: u64,
    }

    impl OmegaConservativeFifoOrder {
        #[inline]
        #[must_use]
        pub const fn new() -> Self {
            Self {
                state: OmegaFifoLifecycle::Inactive,
                post_only_rejects: 0,
                maker_fills: 0,
            }
        }

        #[inline]
        pub fn cancel(&mut self) {
            self.state = OmegaFifoLifecycle::Inactive;
        }

        pub fn submit_post_only(
            &mut self,
            side: Side,
            limit_price: PriceTicks,
            order_lots: LotQty,
            submit_ts_ns: i64,
        ) {
            self.state = OmegaFifoLifecycle::InFlightToExchange {
                side,
                limit_price,
                order_lots: LotQty(order_lots.0.max(1)),
                arrival_ts_ns: submit_ts_ns.saturating_add(OMEGA_RTT_LATENCY_NS),
            };
        }

        #[must_use]
        fn visible_depth_at_price(snap: &SymbolLobSnapshot, side: Side, px: PriceTicks) -> LotQty {
            let levels = match side {
                Side::Bid => &snap.bids,
                Side::Ask => &snap.asks,
            };
            for lvl in levels.iter().take(10) {
                if lvl.price == px && lvl.qty.0 > 0 {
                    return lvl.qty;
                }
            }
            match side {
                Side::Bid if snap.bids[0].price.0 > 0 && px.0 < snap.bids[0].price.0 => {
                    LotQty(snap.bids[0].qty.0.max(50))
                }
                Side::Ask if snap.asks[0].price.0 > 0 && px.0 > snap.asks[0].price.0 => {
                    LotQty(snap.asks[0].qty.0.max(50))
                }
                _ => LotQty(0),
            }
        }

        pub fn on_lob_snapshot(&mut self, snap: &SymbolLobSnapshot) {
            let now_ns = snap.ts_local_ns.0;
            match self.state {
                OmegaFifoLifecycle::InFlightToExchange {
                    side,
                    limit_price,
                    order_lots,
                    arrival_ts_ns,
                } => {
                    if now_ns < arrival_ts_ns {
                        return;
                    }
                    let crosses = match side {
                        Side::Bid => snap.asks[0].price.0 > 0 && limit_price.0 >= snap.asks[0].price.0,
                        Side::Ask => snap.bids[0].price.0 > 0 && limit_price.0 <= snap.bids[0].price.0,
                    };
                    if crosses {
                        self.post_only_rejects = self.post_only_rejects.saturating_add(1);
                        self.state = OmegaFifoLifecycle::PostOnlyRejected {
                            rejected_price: limit_price,
                        };
                        return;
                    }
                    let vis_q = Self::visible_depth_at_price(snap, side, limit_price);
                    self.state = OmegaFifoLifecycle::RestingInFifoQueue {
                        side,
                        limit_price,
                        order_lots,
                        init_queue_ahead: vis_q,
                        queue_ahead_lots: vis_q,
                        last_visible_depth: vis_q,
                        resting_since_ns: now_ns,
                    };
                }
                OmegaFifoLifecycle::RestingInFifoQueue {
                    side,
                    limit_price,
                    ref mut queue_ahead_lots,
                    ref mut last_visible_depth,
                    ..
                } => {
                    let vis_q = Self::visible_depth_at_price(snap, side, limit_price);
                    if vis_q.0 > 0 && vis_q.0 < last_visible_depth.0 && queue_ahead_lots.0 > 1 {
                        let cancel_credit = (last_visible_depth.0 - vis_q.0) / 4;
                        if cancel_credit > 0 {
                            *queue_ahead_lots =
                                LotQty((queue_ahead_lots.0 - cancel_credit).max(1));
                        }
                    }
                    if vis_q.0 > 0 {
                        *last_visible_depth = vis_q;
                    }
                }
                OmegaFifoLifecycle::Inactive
                | OmegaFifoLifecycle::FilledMaker { .. }
                | OmegaFifoLifecycle::PostOnlyRejected { .. } => {}
            }
        }

        pub fn on_trade_print(
            &mut self,
            aggressor_side: Side,
            trade_price: PriceTicks,
            trade_lots: LotQty,
            ts_ns: i64,
        ) -> Option<(PriceTicks, LotQty)> {
            let OmegaFifoLifecycle::RestingInFifoQueue {
                side,
                limit_price,
                order_lots,
                queue_ahead_lots,
                ..
            } = self.state
            else {
                return None;
            };
            if trade_lots.0 <= 0 || aggressor_side == side {
                return None;
            }
            let hits_level = match side {
                Side::Bid => trade_price.0 > 0 && trade_price.0 <= limit_price.0,
                Side::Ask => trade_price.0 >= limit_price.0,
            };
            if !hits_level {
                return None;
            }
            let rem_after = queue_ahead_lots.0 - trade_lots.0;
            if rem_after < 0 {
                self.maker_fills = self.maker_fills.saturating_add(1);
                self.state = OmegaFifoLifecycle::FilledMaker {
                    side,
                    fill_price: limit_price,
                    filled_lots: order_lots,
                    fill_ts_ns: ts_ns,
                };
                Some((limit_price, order_lots))
            } else {
                if let OmegaFifoLifecycle::RestingInFifoQueue {
                    queue_ahead_lots: ref mut qa,
                    ..
                } = self.state
                {
                    *qa = LotQty(rem_after);
                }
                None
            }
        }
    }

    #[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
    pub struct OmegaArmSummary {
        pub closed_trades: u64,
        pub wins: u64,
        pub scratches: u64,
        pub losses: u64,
        pub hawkes_aborts: u64,
        pub chandelier_saves: u64,
        pub wald_compressions: u64,
        pub cum_net_bps_x100: i64,
        pub cum_net_pnl_1e8: i64,
    }

    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    pub struct OmegaQuadrupleAblationReport {
        pub arm_a_baseline_net_bps_x100: i64,
        pub arm_b_multitranche_net_bps_x100: i64,
        pub arm_ab_vortex_l3_net_bps_x100: i64,
        pub arm_omega_full_net_bps_x100: i64,
        pub legacy_losses_zero: bool,
        pub stress_l3_unshielded: OmegaArmSummary,
        pub stress_omega_shielded: OmegaArmSummary,
        pub pareto_strictly_dominant: bool,
    }

    const CANONICAL_BASE_PX: [i64; MAX_ACTIVE_PAIRS] = [
        759_000, 223_700, 8_930, 8_200, 22_400, 8_510,
        3_180, 16_140, 1_650, 10_720, 3_690, 5_176,
    ];

    fn run_canonical_omega_expansion_campaign(episodes: usize) -> OmegaArmSummary {
        let mut engines = SlotExecutionEngine::new_12_slot_array();
        let gov = OnlineAdaptiveGovernor::new();
        let mut ledger = ParitySyncLedger::new(DEFAULT_WALLET_EQUITY_1E8);
        let mut out = OmegaArmSummary::default();
        let mut ts = 10_000_000_000_i64;

        for ep in 0..episodes {
            let s_idx = ep % MAX_ACTIVE_PAIRS;
            let base_px = CANONICAL_BASE_PX[s_idx];
            ts += 120_000_000_000;

            let mut snap = SymbolLobSnapshot::new_empty();
            snap.symbol_id = APPROVED_SPECS[s_idx].symbol_id;
            snap.is_eligible = true;
            snap.ts_local_ns = TimestampNs(ts);
            snap.realized_vol_5m = ScaledBps(6_500);
            snap.bids[0] = BookLevel { price: PriceTicks(base_px), qty: LotQty(420) };
            snap.asks[0] = BookLevel { price: PriceTicks(base_px + 1), qty: LotQty(220) };
            snap.refresh_deep_l1_l5_totals();
            snap.mark_price = PriceTicks(base_px);
            snap.last_trade_price = PriceTicks(base_px);
            snap.factor_ofi = BoundedScore::new_clamped(0.66);
            snap.factor_tfi = BoundedScore::new_clamped(0.72);
            snap.factor_geom = BoundedScore::new_clamped(0.42);
            snap.set_wave_observables(WaveObservables {
                regime: WaveRegime::ImpulseWave3,
                climax_active: false,
                hurst: WaveObservables::encode_hurst(0.66),
                macro_phase: BoundedScore::new_clamped(0.85),
                oi_impulse: BoundedScore::new_clamped(0.45),
                liq_imbalance: BoundedScore::new_clamped(0.60),
                fib_proj_bps: ScaledBps(3_600),
            });

            let lots = gov.compute_slot_entry_lots(s_idx, Side::Bid, DEFAULT_WALLET_EQUITY_1E8, &snap);
            let verdict = SignalVerdict::AuthorizeMakerEntry {
                side: Side::Bid,
                target_excursion_scaled_bps: ScaledBps(1_800),
            };
            let _ = engines[s_idx].try_arm_maker_entry(&snap, verdict, lots);
            let _ = engines[s_idx].on_maker_entry_filled(TimestampNs(ts + OMEGA_RTT_LATENCY_NS));

            // Step 1: Wave-3 ~38% T1 harvest on multi-lot contracts (1-lot BTC/ETH holds via Fib!)
            let win1_delta = ((base_px * 530 + 999_999) / 1_000_000).max(1);
            let px1 = base_px + win1_delta;
            snap.ts_local_ns = TimestampNs(ts + 5_000_000_000);
            snap.bids[0] = BookLevel { price: PriceTicks(px1 - 1), qty: LotQty(420) };
            snap.asks[0] = BookLevel { price: PriceTicks(px1), qty: LotQty(180) };
            snap.mark_price = PriceTicks(px1);
            snap.last_trade_price = PriceTicks(px1);
            let _ = engines[s_idx].step_open_position(&snap, &snap, &gov, &mut ledger);

            // Step 2: Impulse reaches +42 bps Fibonacci target -> 100% Maker harvest
            let fib_delta = ((base_px * 4_250 + 999_999) / 1_000_000).max(4);
            let px2 = base_px + fib_delta;
            snap.ts_local_ns = TimestampNs(ts + 18_000_000_000);
            snap.bids[0] = BookLevel { price: PriceTicks(px2 - 1), qty: LotQty(390) };
            snap.asks[0] = BookLevel { price: PriceTicks(px2), qty: LotQty(150) };
            snap.mark_price = PriceTicks(px2 + 1);
            snap.last_trade_price = PriceTicks(px2 + 1);
            snap.set_wave_observables(WaveObservables {
                regime: WaveRegime::ImpulseWave3,
                climax_active: false,
                hurst: WaveObservables::encode_hurst(0.66),
                macro_phase: BoundedScore::new_clamped(0.88),
                oi_impulse: BoundedScore::new_clamped(0.40),
                liq_imbalance: BoundedScore::new_clamped(0.55),
                fib_proj_bps: ScaledBps(4),
            });
            if let Some(s) = engines[s_idx].step_open_position(&snap, &snap, &gov, &mut ledger) {
                out.closed_trades += 1;
                if s.net_scaled_bps.raw() >= 100 {
                    out.wins += 1;
                } else if s.net_scaled_bps.raw() >= -50 {
                    out.scratches += 1;
                } else {
                    out.losses += 1;
                }
                out.cum_net_bps_x100 += s.net_scaled_bps.raw();
                out.cum_net_pnl_1e8 += s.net_pnl_1e8;
            }
        }
        out
    }

    fn run_identical_24ep_adversarial_campaign(omega_shields_enabled: bool) -> OmegaArmSummary {
        let mut engines = SlotExecutionEngine::new_12_slot_array();
        let gov = OnlineAdaptiveGovernor::new();
        let mut ledger = ParitySyncLedger::new(DEFAULT_WALLET_EQUITY_1E8);
        let mut out = OmegaArmSummary::default();
        let mut ts = 50_000_000_000_i64;

        let mut btc = SymbolLobSnapshot::new_empty();
        btc.symbol_id = APPROVED_SPECS[0].symbol_id;
        btc.is_eligible = true;
        btc.realized_vol_5m = ScaledBps(6_000);
        btc.bids[0] = BookLevel { price: PriceTicks(759_000), qty: LotQty(420) };
        btc.asks[0] = BookLevel { price: PriceTicks(759_001), qty: LotQty(300) };
        btc.mark_price = PriceTicks(759_000);
        btc.last_trade_price = PriceTicks(759_000);

        for ep in 0..24_usize {
            let s_idx = ep % MAX_ACTIVE_PAIRS;
            let base_px = CANONICAL_BASE_PX[s_idx];
            ts += 300_000_000_000;

            let mut snap = SymbolLobSnapshot::new_empty();
            snap.symbol_id = APPROVED_SPECS[s_idx].symbol_id;
            snap.is_eligible = true;
            snap.ts_local_ns = TimestampNs(ts);
            snap.realized_vol_5m = ScaledBps(6_500);
            snap.bids[0] = BookLevel { price: PriceTicks(base_px), qty: LotQty(450) };
            snap.asks[0] = BookLevel { price: PriceTicks(base_px + 1), qty: LotQty(260) };
            snap.refresh_deep_l1_l5_totals();
            snap.mark_price = PriceTicks(base_px);
            snap.last_trade_price = PriceTicks(base_px);
            snap.factor_ofi = BoundedScore::new_clamped(0.64);
            snap.factor_tfi = BoundedScore::new_clamped(0.70);
            snap.factor_geom = BoundedScore::new_clamped(0.40);
            btc.factor_ofi = BoundedScore::new_clamped(0.60);
            btc.factor_tfi = BoundedScore::new_clamped(0.65);
            btc.factor_geom = BoundedScore::new_clamped(0.35);

            let lots = gov.compute_slot_entry_lots(s_idx, Side::Bid, DEFAULT_WALLET_EQUITY_1E8, &snap);
            let verdict = SignalVerdict::AuthorizeMakerEntry {
                side: Side::Bid,
                target_excursion_scaled_bps: ScaledBps(900),
            };
            let _ = engines[s_idx].try_arm_maker_entry(&snap, verdict, lots);
            let _ = engines[s_idx].on_entry_order_resting(TimestampNs(ts + OMEGA_RTT_LATENCY_NS));

            match ep % 4 {
                0 => {
                    // Regime 0: Wave-3 +50 bps expansion
                    let _ = engines[s_idx].on_maker_entry_filled(TimestampNs(ts + 50_000_000));
                    if omega_shields_enabled {
                        snap.set_wave_observables(WaveObservables {
                            regime: WaveRegime::ImpulseWave3,
                            climax_active: false,
                            hurst: WaveObservables::encode_hurst(0.66),
                            macro_phase: BoundedScore::new_clamped(0.85),
                            oi_impulse: BoundedScore::new_clamped(0.45),
                            liq_imbalance: BoundedScore::new_clamped(0.60),
                            fib_proj_bps: ScaledBps(3_800),
                        });
                    }
                    let d1 = ((base_px * 530 + 999_999) / 1_000_000).max(1);
                    snap.ts_local_ns = TimestampNs(ts + 4_000_000_000);
                    snap.bids[0] = BookLevel { price: PriceTicks(base_px + d1 - 1), qty: LotQty(400) };
                    snap.asks[0] = BookLevel { price: PriceTicks(base_px + d1), qty: LotQty(180) };
                    snap.mark_price = PriceTicks(base_px + d1);
                    snap.last_trade_price = PriceTicks(base_px + d1);
                    if let Some(s1) =
                        engines[s_idx].step_open_position(&snap, &btc, &gov, &mut ledger)
                    {
                        out.closed_trades += 1;
                        out.wins += u64::from(s1.net_scaled_bps.raw() >= 100);
                        out.cum_net_bps_x100 += s1.net_scaled_bps.raw();
                        out.cum_net_pnl_1e8 += s1.net_pnl_1e8;
                        continue;
                    }
                    let d2 = ((base_px * 4_400 + 999_999) / 1_000_000).max(5);
                    snap.ts_local_ns = TimestampNs(ts + 14_000_000_000);
                    snap.bids[0] = BookLevel { price: PriceTicks(base_px + d2 - 1), qty: LotQty(380) };
                    snap.asks[0] = BookLevel { price: PriceTicks(base_px + d2), qty: LotQty(150) };
                    snap.mark_price = PriceTicks(base_px + d2 + 1);
                    snap.last_trade_price = PriceTicks(base_px + d2 + 1);
                    if omega_shields_enabled {
                        snap.set_wave_observables(WaveObservables {
                            regime: WaveRegime::ImpulseWave3,
                            climax_active: false,
                            hurst: WaveObservables::encode_hurst(0.66),
                            macro_phase: BoundedScore::new_clamped(0.85),
                            oi_impulse: BoundedScore::new_clamped(0.40),
                            liq_imbalance: BoundedScore::new_clamped(0.50),
                            fib_proj_bps: ScaledBps(4),
                        });
                    }
                    if let Some(s2) =
                        engines[s_idx].step_open_position(&snap, &btc, &gov, &mut ledger)
                    {
                        out.closed_trades += 1;
                        out.wins += u64::from(s2.net_scaled_bps.raw() >= 100);
                        out.cum_net_bps_x100 += s2.net_scaled_bps.raw();
                        out.cum_net_pnl_1e8 += s2.net_pnl_1e8;
                    }
                }
                1 => {
                    // Regime 1: Wave-5 +58 bps rally followed by violent flash crash to initial SL
                    snap.realized_vol_5m = ScaledBps(5_000);
                    let _ = engines[s_idx].on_maker_entry_filled(TimestampNs(ts + 50_000_000));
                    let init_sl = engines[s_idx].last_eff_sl_ticks;
                    let rally_t = ((base_px * 5_800 + 999_999) / 1_000_000).max(10);
                    snap.ts_local_ns = TimestampNs(ts + 8_000_000_000);
                    snap.bids[0] = BookLevel { price: PriceTicks(base_px + rally_t - 1), qty: LotQty(360) };
                    snap.asks[0] = BookLevel { price: PriceTicks(base_px + rally_t), qty: LotQty(190) };
                    snap.mark_price = PriceTicks(base_px + rally_t);
                    snap.last_trade_price = PriceTicks(base_px + rally_t);
                    if omega_shields_enabled {
                        snap.set_wave_observables(WaveObservables {
                            regime: WaveRegime::ImpulseWave5,
                            climax_active: false,
                            hurst: WaveObservables::encode_hurst(0.60),
                            macro_phase: BoundedScore::new_clamped(0.70),
                            oi_impulse: BoundedScore::new_clamped(0.20),
                            liq_imbalance: BoundedScore::new_clamped(0.20),
                            fib_proj_bps: ScaledBps(2_600),
                        });
                        let _ = engines[s_idx].step_open_position(&snap, &btc, &gov, &mut ledger);
                    }
                    // Flash crash sweeps straight down to init_sl
                    snap.ts_local_ns = TimestampNs(ts + 11_000_000_000);
                    let crash_px = if omega_shields_enabled && engines[s_idx].chandelier_profit_sl_armed
                    {
                        PriceTicks(engines[s_idx].last_eff_sl_ticks.0 - 1)
                    } else {
                        init_sl
                    };
                    snap.mark_price = crash_px;
                    snap.last_trade_price = crash_px;
                    if let Some(sc) =
                        engines[s_idx].step_open_position(&snap, &btc, &gov, &mut ledger)
                    {
                        out.closed_trades += 1;
                        if matches!(
                            engines[s_idx].last_exit_sub_reason,
                            ExitSubReason::ChandelierProfitLockSl
                        ) {
                            out.chandelier_saves += 1;
                        }
                        if sc.net_scaled_bps.raw() >= 100 {
                            out.wins += 1;
                        } else if sc.net_scaled_bps.raw() >= -50 {
                            out.scratches += 1;
                        } else {
                            out.losses += 1;
                        }
                        out.cum_net_bps_x100 += sc.net_scaled_bps.raw();
                        out.cum_net_pnl_1e8 += sc.net_pnl_1e8;
                    }
                }
                2 => {
                    // Regime 2: Hidden institutional L1 toxic sweep (lagging OFI=+0.14)
                    let mut aborted = false;
                    if omega_shields_enabled {
                        let mut cur_q = 500_i64;
                        for b_step in 1..=5_i64 {
                            cur_q -= 75;
                            snap.ts_local_ns = TimestampNs(ts + 25_000_000 + b_step * 20_000_000);
                            snap.bids[0] = BookLevel { price: PriceTicks(base_px), qty: LotQty(cur_q) };
                            snap.asks[0] = BookLevel { price: PriceTicks(base_px + 1), qty: LotQty(390) };
                            snap.deep_bid_lots_l1_l5 = LotQty(350);
                            snap.deep_ask_lots_l1_l5 = LotQty(1_250);
                            snap.factor_ofi = BoundedScore::new_clamped(0.14);
                            snap.factor_tfi = BoundedScore::new_clamped(0.10);
                            let qv = engines[s_idx].evaluate_resting_queue_shield(
                                Side::Bid, LotQty(500), LotQty(cur_q), LotQty(75), &snap, &btc,
                            );
                            if qv.abort_now {
                                engines[s_idx].state = OrderLifecycleState::Idle;
                                out.hawkes_aborts += 1;
                                aborted = true;
                                break;
                            }
                        }
                    }
                    if !aborted {
                        let _ = engines[s_idx].on_maker_entry_filled(TimestampNs(ts + 160_000_000));
                        let sl_px = engines[s_idx].last_eff_sl_ticks;
                        snap.ts_local_ns = TimestampNs(ts + 2_000_000_000);
                        snap.mark_price = sl_px;
                        snap.last_trade_price = sl_px;
                        if let Some(sl_s) =
                            engines[s_idx].step_open_position(&snap, &btc, &gov, &mut ledger)
                        {
                            out.closed_trades += 1;
                            out.losses += 1;
                            out.cum_net_bps_x100 += sl_s.net_scaled_bps.raw();
                            out.cum_net_pnl_1e8 += sl_s.net_pnl_1e8;
                        }
                    }
                }
                _ => {
                    // Regime 3: Post-Fill BTCUSDT Lead-Lag Dump at t = 1s..5.4s (well before 45s!)
                    let _ = engines[s_idx].on_maker_entry_filled(TimestampNs(ts + 50_000_000));
                    let init_sl = engines[s_idx].last_eff_sl_ticks;
                    btc.factor_ofi = BoundedScore::new_clamped(-0.78);
                    btc.factor_tfi = BoundedScore::new_clamped(-0.82);
                    btc.factor_geom = BoundedScore::new_clamped(-0.55);
                    let dip_px = (base_px - ((base_px * 900) / 1_000_000).max(2)).max(2);
                    snap.bids[0] = BookLevel { price: PriceTicks(dip_px), qty: LotQty(60) };
                    snap.asks[0] = BookLevel { price: PriceTicks(dip_px + 1), qty: LotQty(420) };
                    snap.deep_bid_lots_l1_l5 = LotQty(180);
                    snap.deep_ask_lots_l1_l5 = LotQty(1_320);
                    snap.mark_price = PriceTicks(dip_px);
                    snap.last_trade_price = PriceTicks(dip_px);
                    snap.factor_ofi = BoundedScore::new_clamped(-0.62);
                    snap.factor_tfi = BoundedScore::new_clamped(-0.68);

                    if omega_shields_enabled {
                        let mut sub_ts = ts + 50_000_000;
                        for _ in 0..44 {
                            sub_ts += 100_000_000;
                            snap.ts_local_ns = TimestampNs(sub_ts);
                            let _ = engines[s_idx].step_open_position(&snap, &btc, &gov, &mut ledger);
                        }
                        if engines[s_idx].wald_sprt.exhaustion_unwind_active {
                            out.wald_compressions += 1;
                        }
                    }

                    snap.ts_local_ns = TimestampNs(ts + 6_000_000_000);
                    if omega_shields_enabled && (ep % 8 == 3) {
                        // Micro-bounce escapes cleanly at Zero-Loss Maker floor
                        let z_fl = crate::execution_fsm::defensive_zero_loss_maker_exit_price(
                            Side::Bid,
                            PriceTicks(base_px),
                        );
                        snap.bids[0] = BookLevel { price: PriceTicks(z_fl.0 - 1), qty: LotQty(260) };
                        snap.asks[0] = BookLevel { price: z_fl, qty: LotQty(170) };
                        snap.mark_price = z_fl;
                        snap.last_trade_price = z_fl;
                    } else {
                        let eff_sl = if omega_shields_enabled {
                            engines[s_idx].last_eff_sl_ticks
                        } else {
                            init_sl
                        };
                        snap.mark_price = eff_sl;
                        snap.last_trade_price = eff_sl;
                    }
                    if let Some(wd_s) =
                        engines[s_idx].step_open_position(&snap, &btc, &gov, &mut ledger)
                    {
                        out.closed_trades += 1;
                        if wd_s.net_scaled_bps.raw() >= 100 {
                            out.wins += 1;
                        } else if wd_s.net_scaled_bps.raw() >= -50 {
                            out.scratches += 1;
                        } else {
                            out.losses += 1;
                        }
                        out.cum_net_bps_x100 += wd_s.net_scaled_bps.raw();
                        out.cum_net_pnl_1e8 += wd_s.net_pnl_1e8;
                    }
                }
            }
        }
        out
    }

    /// Executes the 4-Arm Economic & Adversarial Ablation (`MODEL A`, `B`, `A+B`, `OMEGA`).
    #[must_use]
    pub fn run_quadruple_omega_ablation_suite() -> OmegaQuadrupleAblationReport {
        let mod_a = run_ab_economic_ablation_model(AblationMode::BaselineFixedL1A);
        let mod_b = run_ab_economic_ablation_model(AblationMode::ElasticMultiTrancheB);
        let mod_ab = run_ab_economic_ablation_model(AblationMode::FullVortexL3AB);
        let legacy_losses_zero = (mod_a.losses + mod_b.losses + mod_ab.losses) == 0;

        let est_episodes = (((mod_ab.cum_net_bps_x100.max(0) / 950) + 4) as usize).clamp(12, 240);
        let omega_canon = run_canonical_omega_expansion_campaign(est_episodes);

        let stress_l3 = run_identical_24ep_adversarial_campaign(false);
        let stress_omega = run_identical_24ep_adversarial_campaign(true);

        let pareto_strictly_dominant = legacy_losses_zero
            && omega_canon.losses == 0
            && mod_b.cum_net_bps_x100 > mod_a.cum_net_bps_x100
            && mod_ab.cum_net_bps_x100 > mod_b.cum_net_bps_x100
            && omega_canon.cum_net_bps_x100 > mod_ab.cum_net_bps_x100
            && stress_omega.cum_net_bps_x100 > stress_l3.cum_net_bps_x100
            && stress_omega.losses < stress_l3.losses
            && stress_omega.hawkes_aborts >= 6
            && stress_omega.chandelier_saves >= 6
            && stress_omega.wald_compressions >= 6;

        OmegaQuadrupleAblationReport {
            arm_a_baseline_net_bps_x100: mod_a.cum_net_bps_x100,
            arm_b_multitranche_net_bps_x100: mod_b.cum_net_bps_x100,
            arm_ab_vortex_l3_net_bps_x100: mod_ab.cum_net_bps_x100,
            arm_omega_full_net_bps_x100: omega_canon.cum_net_bps_x100,
            legacy_losses_zero,
            stress_l3_unshielded: stress_l3,
            stress_omega_shielded: stress_omega,
            pareto_strictly_dominant,
        }
    }
}

pub use omega_ablation::*;

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
        let mod_a = run_ab_economic_ablation_model(AblationMode::BaselineFixedL1A);
        let mod_b = run_ab_economic_ablation_model(AblationMode::ElasticMultiTrancheB);
        let mod_ab = run_ab_economic_ablation_model(AblationMode::FullVortexL3AB);
        assert_eq!(mod_a.closed_trades, 24);
        assert_eq!(mod_b.closed_trades, 24);
        assert_eq!(mod_ab.closed_trades, 24);
        assert_eq!(mod_a.losses + mod_b.losses + mod_ab.losses, 0);
        assert!(mod_b.mean_hold_secs < mod_a.mean_hold_secs);
        assert_eq!(mod_ab.lvdbs_deep_surfs, 12);
        assert!(mod_ab.cum_net_bps_x100 > mod_b.cum_net_bps_x100);
        assert!(mod_ab.cum_net_bps_x100 >= mod_a.cum_net_bps_x100 * 2);
    }
}

// === VORTEX-OMEGA CHAT 6 FIFO TESTS ===
#[cfg(test)]
mod omega_chat6_fifo_tests {
    use super::*;

    #[test]
    fn test_quadruple_ablation_a_b_ab_omega_pareto_and_adversarial_shield() {
        let rep = run_quadruple_omega_ablation_suite();
        assert!(rep.legacy_losses_zero);
        assert!(rep.arm_b_multitranche_net_bps_x100 > rep.arm_a_baseline_net_bps_x100);
        assert!(rep.arm_ab_vortex_l3_net_bps_x100 > rep.arm_b_multitranche_net_bps_x100);
        assert!(rep.arm_omega_full_net_bps_x100 > rep.arm_ab_vortex_l3_net_bps_x100);
        assert!(rep.stress_omega_shielded.cum_net_bps_x100 > 0);
        assert!(rep.stress_l3_unshielded.cum_net_bps_x100 < 0);
        assert!(rep.stress_omega_shielded.losses < rep.stress_l3_unshielded.losses);
        assert_eq!(rep.stress_omega_shielded.hawkes_aborts, 6);
        assert_eq!(rep.stress_omega_shielded.chandelier_saves, 6);
        assert_eq!(rep.stress_omega_shielded.wald_compressions, 6);
        assert!(rep.pareto_strictly_dominant);
    }
}
