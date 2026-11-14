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
