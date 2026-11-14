//! Autonomous 12-Slot Execution State Machine, Fibonacci Ceiling, Wald SL & Tear-Free SHM.

#![allow(clippy::manual_clamp, clippy::too_many_arguments)]

use crate::alpha_3factor::{OrthogonalFactorState, SignalVerdict};
use crate::forensic_parquet::ParitySyncLedger;
use crate::lob_shm::{
    SlotTelemetrySnapshot, SymbolLobSnapshot, VortexShmArena, MAX_ACTIVE_PAIRS,
};
use crate::types::{
    LotQty, PairSpec, PriceTicks, ScaledBps, Side, TimestampNs, VortexError, APPROVED_SPECS,
};

pub use crate::bayesian_governor::OnlineAdaptiveGovernor;
pub use crate::exit_quoter::{
    classify_maker_exit_sub_reason, compute_canonical_exit_quote_core,
    compute_tranche1_split_lots_core, compute_validated_sl_ticks,
    defensive_zero_loss_maker_exit_price, dynamic_maker_floor_price,
    evaluate_sl_or_emergency_flatten_core, offensive_win_maker_exit_price,
    reconcile_closed_pnl_core, resolve_fallback_unverified_close_core, CanonicalExitQuote,
    ClosedPnlQueryOutcome, ClosedTradeKind, ClosedTradeSummary, ExitOrderRole, ExitSubReason,
    OpenExitContext, ServerSlDecision, VerifiedCloseResolution, MAX_CLOSE_RECONCILE_RETRIES,
    OFFENSIVE_WIN_FLOOR_GROSS_BPS,
};
pub use crate::kinematic_surface::{
    check_queue_depletion_velocity_abort, ScarEventKey, ScarEventPayload, UNIFIED_LOSS_LAB_COUNT,
};
pub use crate::ws_protocol::{
    amend_fifo_queue_priority, asymmetric_recv_window_ms, compute_amend_total_lots,
    format_bijective_order_link_id, format_ws_trade_req_id, parse_signed_decimal_1e8,
    parse_verified_avg_price_ticks, parse_verified_exit_price_ticks, sign_bybit_v5_hmac_sha256,
    OrderLinkRole, Vip0WsRateGovernor, WsMutationOp, WsOrderLatticeState, WsOrderRank,
    WsTerminalKind,
};

const VOL_5M_HORIZON_NS: i64 = 300_000_000_000;

/// Per-Slot Autonomous Lifecycle State Machine (Monotonic Tranche, Wald SL & Fibonacci Native).
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum OrderLifecycleState {
    #[default]
    Idle,
    PlacingEntryMaker {
        side: Side,
        entry_price: PriceTicks,
        qty: LotQty,
        target_excursion: ScaledBps,
        sl_trigger_price: PriceTicks,
        submitted_ts: TimestampNs,
    },
    RestingEntryQueue {
        side: Side,
        entry_price: PriceTicks,
        qty: LotQty,
        target_excursion: ScaledBps,
        sl_trigger_price: PriceTicks,
        resting_since_ts: TimestampNs,
    },
    PositionOpenMakerExit {
        side: Side,
        entry_price: PriceTicks,
        avg_price_ticks: PriceTicks,
        peak_favorable_price: PriceTicks,
        worst_adverse_price: PriceTicks,
        initial_lots: LotQty,
        open_lots: LotQty,
        tranche1_harvested: bool,
        tranche1_lots: LotQty,
        tranche1_exit_price: PriceTicks,
        defensive_floor_locked: bool,
        active_exit_role: ExitOrderRole,
        active_exit_price: PriceTicks,
        active_exit_lots: LotQty,
        structural_tp_price: PriceTicks,
        resting_exit_price: PriceTicks,
        sl_trigger_price: PriceTicks,
        opened_ts: TimestampNs,
    },
}

/// Autonomous Per-Slot Execution Engine (1 Instance per Slot in [SlotExecutionEngine; 12]).
#[derive(Clone, Debug)]
pub struct SlotExecutionEngine {
    pub slot_idx: usize,
    pub state: OrderLifecycleState,
    pub cooldown_until_ns: TimestampNs,
    pub tranche1_harvests_count: u64,
    pub lvdbs_deep_surfs_count: u64,
    pub learned_mfe_bps: i64,
    pub learned_t1_permille: i64,
    pub learned_ir_x100: i64,
    pub learned_ret_std_bps: i64,
    pub intrinsic_tau_mkt_s: i64,
    pub last_eff_sl_ticks: PriceTicks,
    pub open_vol_5m_bps: ScaledBps,
    pub ratchet_locked_gross_bps: i64,
    pub last_closed_mfe_bps: i64,
    pub last_closed_mae_bps: i64,
    pub last_closed_peak_ticks: PriceTicks,
    pub last_closed_worst_ticks: PriceTicks,
    pub last_closed_wald_pct: i64,
    pub last_exit_sub_reason: ExitSubReason,
    pub last_opened_ts: TimestampNs,
    pub next_peg_allowed_ts_ns: i64,
    pub exit_peg_attempts: u32,
    pub close_reconcile_retries: u8,
}

impl SlotExecutionEngine {
    #[inline]
    #[must_use]
    pub const fn new(slot_idx: usize) -> Self {
        let init_mfe = (ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 17) / 5;
        let init_tau_s = VOL_5M_HORIZON_NS / 1_000_000_000;
        Self {
            slot_idx,
            state: OrderLifecycleState::Idle,
            cooldown_until_ns: TimestampNs(0),
            tranche1_harvests_count: 0,
            lvdbs_deep_surfs_count: 0,
            learned_mfe_bps: init_mfe,
            learned_t1_permille: 500,
            learned_ir_x100: 33,
            learned_ret_std_bps: 200,
            intrinsic_tau_mkt_s: init_tau_s,
            last_eff_sl_ticks: PriceTicks(0),
            open_vol_5m_bps: ScaledBps(6_000),
            ratchet_locked_gross_bps: 0,
            last_closed_mfe_bps: 0,
            last_closed_mae_bps: 0,
            last_closed_peak_ticks: PriceTicks(0),
            last_closed_worst_ticks: PriceTicks(0),
            last_closed_wald_pct: 0,
            last_exit_sub_reason: ExitSubReason::AdverseScratchEscape,
            last_opened_ts: TimestampNs(0),
            next_peg_allowed_ts_ns: 0,
            exit_peg_attempts: 0,
            close_reconcile_retries: 0,
        }
    }

    #[inline]
    #[must_use]
    pub const fn new_12_slot_array() -> [Self; MAX_ACTIVE_PAIRS] {
        [
            Self::new(0), Self::new(1), Self::new(2), Self::new(3),
            Self::new(4), Self::new(5), Self::new(6), Self::new(7),
            Self::new(8), Self::new(9), Self::new(10), Self::new(11),
        ]
    }

    #[inline]
    #[must_use]
    pub fn compute_tranche1_split_lots(
        slot_idx: usize,
        open_lots: LotQty,
        exit_price: PriceTicks,
        t1_permille: i64,
    ) -> Option<LotQty> {
        compute_tranche1_split_lots_core(slot_idx, open_lots, exit_price, t1_permille)
    }

    #[inline]
    pub fn sync_governor_for_side(&mut self, gov: &OnlineAdaptiveGovernor, side: Side) {
        self.learned_t1_permille =
            gov.learned_tranche1_fraction_permille(self.slot_idx, side, self.learned_mfe_bps);
        self.learned_ir_x100 = gov.slot_information_ratio_x100(self.slot_idx, side);
        self.learned_ret_std_bps = gov.slot_return_std_bps(self.slot_idx, side);
        self.intrinsic_tau_mkt_s =
            (gov.intrinsic_diffusion_clock_ns(self.slot_idx) / 1_000_000_000).max(90);
    }

    #[inline]
    pub fn sync_governor_dynamics(&mut self, gov: &OnlineAdaptiveGovernor) {
        if let Some(side) = self.active_side() {
            self.sync_governor_for_side(gov, side);
        }
    }

    #[inline]
    #[must_use]
    pub const fn active_side(&self) -> Option<Side> {
        match self.state {
            OrderLifecycleState::Idle => None,
            OrderLifecycleState::PlacingEntryMaker { side, .. }
            | OrderLifecycleState::RestingEntryQueue { side, .. }
            | OrderLifecycleState::PositionOpenMakerExit { side, .. } => Some(side),
        }
    }

    #[inline]
    #[must_use]
    pub fn elastic_preflight_window_ns(&self, realized_vol_5m: ScaledBps) -> i64 {
        let min_v = ScaledBps::MIN_REQUIRED_VOL_5M_BPS.raw();
        let eq_v = min_v * 4;
        let vol_c = realized_vol_5m.raw().clamp(min_v, eq_v * 3);
        let fee_rt = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
        let init_mfe = (fee_rt * 17) / 10;
        let mfe_c = self.learned_mfe_bps.clamp((fee_rt * 7) / 5, fee_rt * 3);
        let tau_ns = self.intrinsic_tau_mkt_s.max(90) * 1_000_000_000;
        let base_flight_ns = tau_ns / 12_000;
        let raw_ns = ((base_flight_ns * eq_v) / vol_c * mfe_c) / init_mfe;
        raw_ns.clamp((base_flight_ns * 3) / 5, (base_flight_ns * 7) / 5)
    }

    /// Plastic MFE memory up to 12x round-trip friction (48.00 bps) for Elliott impulse legs.
    #[inline]
    pub fn observe_mfe_sample(&mut self, gross_mfe_scaled_bps: i64) {
        let fee_rt = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
        if gross_mfe_scaled_bps >= fee_rt + 100 {
            let clamped = gross_mfe_scaled_bps.clamp((fee_rt * 7) / 5, fee_rt * 12);
            self.learned_mfe_bps = (self.learned_mfe_bps * 80 + clamped * 20) / 100;
        }
    }

    #[inline]
    pub fn compute_fill_or_adopt_sl_ticks(
        side: Side,
        verified_avg_price: PriceTicks,
        snap: &SymbolLobSnapshot,
    ) -> Result<PriceTicks, VortexError> {
        compute_validated_sl_ticks(side, verified_avg_price, snap)
    }

    /// P0-A Law V: Evaluates server SL vs 1.5x breach boundary for emergency Market IOC flatten.
    #[inline]
    pub fn evaluate_sl_or_emergency_flatten(
        side: Side,
        verified_avg_price: PriceTicks,
        snap: &SymbolLobSnapshot,
    ) -> Result<ServerSlDecision, VortexError> {
        evaluate_sl_or_emergency_flatten_core(side, verified_avg_price, snap)
    }

    pub fn maybe_ratchet_wald_stop_loss(
        &mut self,
        snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
    ) -> Option<PriceTicks> {
        let OrderLifecycleState::PositionOpenMakerExit {
            side,
            avg_price_ticks,
            sl_trigger_price: cur_sl,
            opened_ts,
            ..
        } = self.state
        else {
            return None;
        };
        if avg_price_ticks.0 <= 0 || self.last_closed_wald_pct < 12 {
            return None;
        }
        let fee_rt = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
        if self.last_closed_mfe_bps >= (fee_rt * 13) / 10 {
            return None;
        }
        let hold_s = snap.ts_local_ns.0.saturating_sub(opened_ts.0).max(0) / 1_000_000_000;
        if hold_s < (self.intrinsic_tau_mkt_s.max(90) / 5).max(45) {
            return None;
        }
        let dir_s = side.sign_f64();
        let own_flow = (0.55 * snap.factor_ofi.get() + 0.45 * snap.factor_tfi.get()) * dir_s;
        let btc_snr = OrthogonalFactorState::snr_composite_score(btc_leader_snap) * dir_s;
        if own_flow >= -0.05 && btc_snr >= -0.20 {
            return None;
        }
        let base_sl_bps = PairSpec::elastic_server_sl_distance_bps(snap.realized_vol_5m).raw();
        let btc_headwind = if btc_snr < -0.25 {
            ((-btc_snr - 0.25) * 20.0).round() as i64
        } else {
            0
        };
        let eff_wald_pct = (self.last_closed_wald_pct + btc_headwind).clamp(12, 42);
        let contracted_bps = ((base_sl_bps * (100 - eff_wald_pct)) / 100).max(fee_rt * 12);
        let sl_delta = ((avg_price_ticks.0.saturating_mul(contracted_bps)) / 1_000_000).max(4);
        let ref_mark = if snap.mark_price.0 > 0 { snap.mark_price } else { avg_price_ticks };
        let ref_last = if snap.last_trade_price.0 > 0 { snap.last_trade_price } else { ref_mark };
        let cand_sl = match side {
            Side::Bid => {
                let nominal = avg_price_ticks.0 - sl_delta;
                PriceTicks(nominal.min(ref_mark.0.min(ref_last.0) - 4).max(1))
            }
            Side::Ask => {
                let nominal = avg_price_ticks.0.saturating_add(sl_delta);
                PriceTicks(nominal.max(ref_mark.0.max(ref_last.0).saturating_add(4)))
            }
        };
        let valid_sl = PairSpec::validate_stop_loss_geometry(side, cand_sl, ref_last, ref_mark).ok()?;
        let prev_sl = if self.last_eff_sl_ticks.0 > 0 { self.last_eff_sl_ticks } else { cur_sl };
        let tighter_by_3_ticks = match side {
            Side::Bid => valid_sl.0 >= prev_sl.0.saturating_add(3) && valid_sl.0 < avg_price_ticks.0,
            Side::Ask => valid_sl.0 <= prev_sl.0.saturating_sub(3) && valid_sl.0 > avg_price_ticks.0,
        };
        if !tighter_by_3_ticks {
            return None;
        }
        self.last_eff_sl_ticks = valid_sl;
        if let OrderLifecycleState::PositionOpenMakerExit {
            sl_trigger_price: ref mut sl_p,
            ..
        } = self.state
        {
            *sl_p = valid_sl;
        }
        Some(valid_sl)
    }

    pub fn sync_to_shm_telemetry(&self, arena: &VortexShmArena) {
        let Ok(tel) = arena.telemetry(self.slot_idx) else {
            return;
        };
        let snap = match self.state {
            OrderLifecycleState::Idle => SlotTelemetrySnapshot {
                state_code: 0,
                lvdbs_surfs: self.lvdbs_deep_surfs_count,
                ..SlotTelemetrySnapshot::default()
            },
            OrderLifecycleState::PlacingEntryMaker {
                side, entry_price, qty, sl_trigger_price, submitted_ts, ..
            } => SlotTelemetrySnapshot {
                state_code: 1,
                side_code: side as u64,
                entry_ticks: entry_price.0,
                avg_price_ticks: entry_price.0,
                sl_ticks: sl_trigger_price.0,
                initial_lots: qty.0,
                lvdbs_surfs: self.lvdbs_deep_surfs_count,
                resting_since_ns: submitted_ts.0.max(0) as u64,
                queue_ahead_lots: tel.queue_ahead_lots_relaxed(),
                ..SlotTelemetrySnapshot::default()
            },
            OrderLifecycleState::RestingEntryQueue {
                side, entry_price, qty, sl_trigger_price, resting_since_ts, ..
            } => SlotTelemetrySnapshot {
                state_code: 2,
                side_code: side as u64,
                entry_ticks: entry_price.0,
                avg_price_ticks: entry_price.0,
                sl_ticks: sl_trigger_price.0,
                initial_lots: qty.0,
                lvdbs_surfs: self.lvdbs_deep_surfs_count,
                resting_since_ns: resting_since_ts.0.max(0) as u64,
                queue_ahead_lots: tel.queue_ahead_lots_relaxed(),
                ..SlotTelemetrySnapshot::default()
            },
            OrderLifecycleState::PositionOpenMakerExit {
                side, entry_price, avg_price_ticks, peak_favorable_price, initial_lots,
                open_lots, tranche1_harvested, active_exit_price, resting_exit_price,
                sl_trigger_price, opened_ts, ..
            } => {
                let state_code = if tranche1_harvested {
                    4_u8
                } else if open_lots.0 < initial_lots.0 {
                    5_u8
                } else {
                    3_u8
                };
                SlotTelemetrySnapshot {
                    state_code,
                    side_code: side as u64,
                    entry_ticks: entry_price.0,
                    avg_price_ticks: avg_price_ticks.0,
                    peak_ticks: peak_favorable_price.0,
                    tp_ticks: if resting_exit_price.0 > 0 { resting_exit_price.0 } else { active_exit_price.0 },
                    sl_ticks: if self.last_eff_sl_ticks.0 > 0 { self.last_eff_sl_ticks.0 } else { sl_trigger_price.0 },
                    open_lots: open_lots.0,
                    initial_lots: initial_lots.0,
                    lvdbs_surfs: self.lvdbs_deep_surfs_count,
                    tranche1_harvested,
                    resting_since_ns: opened_ts.0.max(0) as u64,
                    queue_ahead_lots: 0,
                }
            }
        };
        tel.write_consistent(&snap);
    }

    pub fn try_arm_maker_entry(
        &mut self,
        snap: &SymbolLobSnapshot,
        verdict: SignalVerdict,
        qty: LotQty,
    ) -> Result<OrderLifecycleState, VortexError> {
        if !matches!(self.state, OrderLifecycleState::Idle)
            || snap.ts_local_ns.0 < self.cooldown_until_ns.0
        {
            return Ok(self.state);
        }
        let SignalVerdict::AuthorizeMakerEntry { side, target_excursion_scaled_bps } = verdict else {
            return Ok(self.state);
        };
        let Some((best_bid, best_ask, _)) = snap.bbo_and_spread_ticks() else {
            return Ok(self.state);
        };
        let entry_price = side.select_entry_bbo_price(best_bid.price, best_ask.price);
        let sl_trigger_price = Self::compute_fill_or_adopt_sl_ticks(side, entry_price, snap)?;
        self.open_vol_5m_bps = snap.realized_vol_5m;
        self.state = OrderLifecycleState::PlacingEntryMaker {
            side,
            entry_price,
            qty: LotQty(qty.0.max(1)),
            target_excursion: target_excursion_scaled_bps,
            sl_trigger_price,
            submitted_ts: snap.ts_local_ns,
        };
        Ok(self.state)
    }

    pub fn sync_preflight_l1_entry(
        &mut self,
        snap: &SymbolLobSnapshot,
        max_drift_ticks: i64,
    ) -> Result<(PriceTicks, PriceTicks), VortexError> {
        let OrderLifecycleState::PlacingEntryMaker { side, entry_price: armed_px, .. } = self.state else {
            return Err(VortexError::InvalidPriceOrTickSpec);
        };
        let Some((best_bid, best_ask, _)) = snap.bbo_and_spread_ticks() else {
            return Err(VortexError::InvalidPriceOrTickSpec);
        };
        let cur_l1_px = side.select_entry_bbo_price(best_bid.price, best_ask.price);
        if cur_l1_px.0 <= 0 || (cur_l1_px.0 - armed_px.0).abs() > max_drift_ticks {
            return Err(VortexError::InvalidPriceOrTickSpec);
        }
        let new_sl = Self::compute_fill_or_adopt_sl_ticks(side, cur_l1_px, snap)?;
        if let OrderLifecycleState::PlacingEntryMaker {
            ref mut entry_price,
            ref mut sl_trigger_price,
            ..
        } = self.state
        {
            *entry_price = cur_l1_px;
            *sl_trigger_price = new_sl;
        }
        Ok((cur_l1_px, new_sl))
    }

    pub fn compute_repeg_sl_trigger(
        &self,
        snap: &SymbolLobSnapshot,
        new_l1_price: PriceTicks,
    ) -> Result<PriceTicks, VortexError> {
        let OrderLifecycleState::RestingEntryQueue { side, .. } = self.state else {
            return Err(VortexError::InvalidPriceOrTickSpec);
        };
        Self::compute_fill_or_adopt_sl_ticks(side, new_l1_price, snap)
    }

    #[inline]
    pub fn apply_resting_repeg(
        &mut self,
        new_l1_price: PriceTicks,
        new_sl_price: PriceTicks,
        now_ts: TimestampNs,
    ) {
        if let OrderLifecycleState::RestingEntryQueue {
            ref mut entry_price,
            ref mut sl_trigger_price,
            ref mut resting_since_ts,
            ..
        } = self.state
        {
            *entry_price = new_l1_price;
            *sl_trigger_price = new_sl_price;
            *resting_since_ts = now_ts;
        }
    }

    pub fn on_entry_order_resting(
        &mut self,
        resting_ts: TimestampNs,
    ) -> Result<OrderLifecycleState, VortexError> {
        if let OrderLifecycleState::PlacingEntryMaker {
            side, entry_price, qty, target_excursion, sl_trigger_price, ..
        } = self.state
        {
            self.state = OrderLifecycleState::RestingEntryQueue {
                side, entry_price, qty, target_excursion, sl_trigger_price,
                resting_since_ts: resting_ts,
            };
        }
        Ok(self.state)
    }

    pub fn on_maker_entry_filled_verified(
        &mut self,
        fill_ts: TimestampNs,
        verified_side: Option<Side>,
        verified_avg_price: Option<PriceTicks>,
        verified_open_lots: Option<LotQty>,
    ) -> Result<OrderLifecycleState, VortexError> {
        let (state_side, state_entry_px, state_qty, target_excursion, state_sl_px) = match self.state {
            OrderLifecycleState::RestingEntryQueue {
                side, entry_price, qty, target_excursion, sl_trigger_price, ..
            }
            | OrderLifecycleState::PlacingEntryMaker {
                side, entry_price, qty, target_excursion, sl_trigger_price, ..
            } => (side, entry_price, qty, target_excursion, sl_trigger_price),
            OrderLifecycleState::Idle | OrderLifecycleState::PositionOpenMakerExit { .. } => {
                return Ok(self.state);
            }
        };
        let side = verified_side.unwrap_or(state_side);
        let eff_entry_px = match verified_avg_price {
            Some(px) if px.0 > 0 => px,
            _ => state_entry_px,
        };
        let eff_lots = match verified_open_lots {
            Some(lq) if lq.0 > 0 => lq,
            _ => LotQty(state_qty.0.max(1)),
        };
        let eff_sl_px = if verified_avg_price.is_some()
            && (eff_entry_px != state_entry_px || side != state_side)
        {
            let sl_dist = (state_entry_px.0 - state_sl_px.0).abs().max(4);
            match side {
                Side::Bid => PriceTicks((eff_entry_px.0 - sl_dist).max(1)),
                Side::Ask => PriceTicks(eff_entry_px.0.saturating_add(sl_dist)),
            }
        } else {
            state_sl_px
        };

        let fee_rt = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
        let min_win_gross = (fee_rt * 13) / 10;
        let win1_px = offensive_win_maker_exit_price(side, eff_entry_px, 0.50);
        let split_t1_opt = Self::compute_tranche1_split_lots(
            self.slot_idx, eff_lots, win1_px, self.learned_t1_permille,
        );
        let tp_bps = if split_t1_opt.is_some() {
            target_excursion.raw().max(min_win_gross + 40)
        } else {
            let vol_raw = self.open_vol_5m_bps.raw();
            let max_single_cap = fee_rt * 12;
            let vol_unit_cap = if vol_raw > 0 {
                ((vol_raw * 62) / 100).clamp(min_win_gross + 15, max_single_cap)
            } else {
                (fee_rt * 9) / 4
            };
            let mfe_cap = self.learned_mfe_bps.clamp(min_win_gross + 15, max_single_cap);
            target_excursion.raw().min(mfe_cap.min(vol_unit_cap)).max(min_win_gross)
        };
        let structural_tp_price = dynamic_maker_floor_price(side, eff_entry_px, ScaledBps(tp_bps));
        let (init_role, init_exit_px, init_exit_lots) = match split_t1_opt {
            Some(t1_lots) => (ExitOrderRole::Tranche1Harvest50, win1_px, t1_lots),
            None => (ExitOrderRole::StandardMakerExit100, structural_tp_price, eff_lots),
        };

        self.last_opened_ts = fill_ts;
        self.last_eff_sl_ticks = eff_sl_px;
        self.ratchet_locked_gross_bps = 0;
        self.tranche1_harvests_count = 0;
        self.lvdbs_deep_surfs_count = 0;
        self.last_closed_mfe_bps = 0;
        self.last_closed_mae_bps = 0;
        self.last_closed_peak_ticks = eff_entry_px;
        self.last_closed_worst_ticks = eff_entry_px;
        self.last_closed_wald_pct = 0;
        self.exit_peg_attempts = 0;
        self.close_reconcile_retries = 0;
        self.next_peg_allowed_ts_ns = fill_ts.0;

        self.state = OrderLifecycleState::PositionOpenMakerExit {
            side,
            entry_price: eff_entry_px,
            avg_price_ticks: eff_entry_px,
            peak_favorable_price: eff_entry_px,
            worst_adverse_price: eff_entry_px,
            initial_lots: eff_lots,
            open_lots: eff_lots,
            tranche1_harvested: false,
            tranche1_lots: LotQty(0),
            tranche1_exit_price: PriceTicks(0),
            defensive_floor_locked: false,
            active_exit_role: init_role,
            active_exit_price: init_exit_px,
            active_exit_lots: init_exit_lots,
            structural_tp_price,
            resting_exit_price: PriceTicks(0),
            sl_trigger_price: eff_sl_px,
            opened_ts: fill_ts,
        };
        Ok(self.state)
    }

    pub fn on_maker_entry_filled(
        &mut self,
        fill_ts: TimestampNs,
    ) -> Result<OrderLifecycleState, VortexError> {
        self.on_maker_entry_filled_verified(fill_ts, None, None, None)
    }

    pub fn adopt_live_open_position(
        &mut self,
        side: Side,
        verified_avg_price: PriceTicks,
        initial_lots: LotQty,
        open_lots: LotQty,
        tranche1_harvested: bool,
        peak_price: PriceTicks,
        worst_price: PriceTicks,
        opened_ts: TimestampNs,
        snap: &SymbolLobSnapshot,
    ) -> Result<PriceTicks, VortexError> {
        let sl_t = Self::compute_fill_or_adopt_sl_ticks(side, verified_avg_price, snap)?;
        let eff_open = LotQty(open_lots.0.max(1));
        let eff_init = LotQty(initial_lots.0.max(eff_open.0));
        let q_fl = defensive_zero_loss_maker_exit_price(side, verified_avg_price);
        let fee_rt = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
        let struct_tp = dynamic_maker_floor_price(
            side,
            verified_avg_price,
            ScaledBps(self.learned_mfe_bps.max((fee_rt * 27) / 20)),
        );
        let harvested_lots = LotQty((eff_init.0 - eff_open.0).max(0));
        let t1_done = tranche1_harvested || harvested_lots.0 > 0;
        let split_opt = if !t1_done {
            Self::compute_tranche1_split_lots(self.slot_idx, eff_open, q_fl, self.learned_t1_permille)
        } else {
            None
        };
        let (role, init_px, init_lots) = match split_opt {
            Some(t1_lots) => (ExitOrderRole::Tranche1Harvest50, q_fl, t1_lots),
            None if t1_done => (ExitOrderRole::LvdbsDeepSurf50, struct_tp, eff_open),
            None => (ExitOrderRole::StandardMakerExit100, struct_tp, eff_open),
        };
        let pk_init = if peak_price.0 > 0 { peak_price } else { verified_avg_price };
        let wr_init = if worst_price.0 > 0 { worst_price } else { verified_avg_price };

        self.last_opened_ts = opened_ts;
        self.last_eff_sl_ticks = sl_t;
        self.open_vol_5m_bps = snap.realized_vol_5m;
        self.ratchet_locked_gross_bps = 0;
        self.last_closed_peak_ticks = pk_init;
        self.last_closed_worst_ticks = wr_init;
        self.tranche1_harvests_count = u64::from(t1_done);
        self.lvdbs_deep_surfs_count = 0;
        self.exit_peg_attempts = 0;
        self.close_reconcile_retries = 0;
        self.next_peg_allowed_ts_ns = opened_ts.0;

        self.state = OrderLifecycleState::PositionOpenMakerExit {
            side,
            entry_price: verified_avg_price,
            avg_price_ticks: verified_avg_price,
            peak_favorable_price: pk_init,
            worst_adverse_price: wr_init,
            initial_lots: eff_init,
            open_lots: eff_open,
            tranche1_harvested: t1_done,
            tranche1_lots: harvested_lots,
            tranche1_exit_price: if t1_done { q_fl } else { PriceTicks(0) },
            defensive_floor_locked: false,
            active_exit_role: role,
            active_exit_price: init_px,
            active_exit_lots: init_lots,
            structural_tp_price: struct_tp,
            resting_exit_price: PriceTicks(0),
            sl_trigger_price: sl_t,
            opened_ts,
        };
        Ok(sl_t)
    }

    #[inline]
    pub fn sync_open_avg_price_ticks(&mut self, verified_avg_price: PriceTicks) {
        if verified_avg_price.0 <= 0 {
            return;
        }
        let fee_rt = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
        let min_win_gross = (fee_rt * 13) / 10;
        if let OrderLifecycleState::PositionOpenMakerExit {
            side,
            ref mut entry_price,
            ref mut avg_price_ticks,
            ref mut peak_favorable_price,
            ref mut worst_adverse_price,
            ref mut structural_tp_price,
            ..
        } = self.state
        {
            if *avg_price_ticks != verified_avg_price {
                let prev_avg = avg_price_ticks.0.max(1);
                let prev_diff = (structural_tp_price.0 - prev_avg).abs();
                let prev_bps = ((prev_diff.saturating_mul(1_000_000)) / prev_avg).max(min_win_gross);
                *entry_price = verified_avg_price;
                *avg_price_ticks = verified_avg_price;
                *structural_tp_price =
                    dynamic_maker_floor_price(side, verified_avg_price, ScaledBps(prev_bps));
                match side {
                    Side::Bid => {
                        if peak_favorable_price.0 < verified_avg_price.0 {
                            *peak_favorable_price = verified_avg_price;
                        }
                        if worst_adverse_price.0 > verified_avg_price.0 {
                            *worst_adverse_price = verified_avg_price;
                        }
                    }
                    Side::Ask => {
                        if peak_favorable_price.0 > verified_avg_price.0 {
                            *peak_favorable_price = verified_avg_price;
                        }
                        if worst_adverse_price.0 < verified_avg_price.0 {
                            *worst_adverse_price = verified_avg_price;
                        }
                    }
                }
            }
        }
    }

    #[inline]
    pub fn latch_defensive_floor_if_needed(&mut self, lock_active: bool) {
        let fee_rt = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
        let state_mfe = if let OrderLifecycleState::PositionOpenMakerExit {
            side, avg_price_ticks, peak_favorable_price, ..
        } = self.state
        {
            let diff = (peak_favorable_price.0 - avg_price_ticks.0).saturating_mul(side.sign_i64());
            (diff.saturating_mul(1_000_000)) / avg_price_ticks.0.max(1)
        } else {
            0
        };
        let best_mfe = self.last_closed_mfe_bps.max(state_mfe);
        if best_mfe >= (fee_rt * 139) / 100 {
            let win_fl = fee_rt + (fee_rt * 3) / 10;
            let cand = (fee_rt + ((best_mfe - fee_rt) * 72) / 100).max(win_fl);
            if cand > self.ratchet_locked_gross_bps {
                self.ratchet_locked_gross_bps = cand;
            }
        }
        if lock_active {
            if let OrderLifecycleState::PositionOpenMakerExit {
                ref mut defensive_floor_locked, ..
            } = self.state
            {
                *defensive_floor_locked = true;
            }
        }
    }

    pub fn record_tranche1_harvest(&mut self, fill_price: PriceTicks) -> bool {
        let s_idx = self.slot_idx;
        let phi = self.learned_t1_permille;
        let OrderLifecycleState::PositionOpenMakerExit {
            ref mut open_lots,
            ref mut tranche1_harvested,
            ref mut tranche1_lots,
            ref mut tranche1_exit_price,
            ref mut active_exit_role,
            ref mut active_exit_lots,
            ..
        } = self.state
        else {
            return false;
        };
        if *tranche1_harvested {
            return false;
        }
        let Some(harvested) = Self::compute_tranche1_split_lots(s_idx, *open_lots, fill_price, phi)
        else {
            return false;
        };
        *open_lots = LotQty(open_lots.0 - harvested.0);
        *tranche1_harvested = true;
        *tranche1_lots = harvested;
        *tranche1_exit_price = fill_price;
        *active_exit_role = ExitOrderRole::LvdbsDeepSurf50;
        *active_exit_lots = *open_lots;
        self.tranche1_harvests_count = self.tranche1_harvests_count.saturating_add(1);
        true
    }

    #[inline]
    fn open_exit_context(&self) -> Option<OpenExitContext> {
        let OrderLifecycleState::PositionOpenMakerExit {
            side, avg_price_ticks, peak_favorable_price, worst_adverse_price, initial_lots,
            open_lots, tranche1_harvested, tranche1_lots, tranche1_exit_price,
            defensive_floor_locked, active_exit_price, structural_tp_price, resting_exit_price,
            sl_trigger_price, opened_ts, ..
        } = self.state
        else {
            return None;
        };
        Some(OpenExitContext {
            slot_idx: self.slot_idx,
            side,
            avg_price_ticks,
            peak_favorable_price,
            worst_adverse_price,
            initial_lots,
            open_lots,
            tranche1_harvested,
            tranche1_lots,
            tranche1_exit_price,
            defensive_floor_locked,
            active_exit_price,
            structural_tp_price,
            resting_exit_price,
            sl_trigger_price,
            opened_ts,
            learned_mfe_bps: self.learned_mfe_bps,
            learned_t1_permille: self.learned_t1_permille,
            learned_ir_x100: self.learned_ir_x100,
            learned_ret_std_bps: self.learned_ret_std_bps,
            intrinsic_tau_mkt_s: self.intrinsic_tau_mkt_s,
            last_eff_sl_ticks: self.last_eff_sl_ticks,
            ratchet_locked_gross_bps: self.ratchet_locked_gross_bps,
            last_closed_mfe_bps: self.last_closed_mfe_bps,
            last_closed_mae_bps: self.last_closed_mae_bps,
        })
    }

    #[inline]
    fn fold_price_into_excursions(&mut self, side: Side, avg_px: PriceTicks, obs_px: PriceTicks) {
        if avg_px.0 <= 0 || obs_px.0 <= 0 {
            return;
        }
        if self.last_closed_peak_ticks.0 <= 0 {
            self.last_closed_peak_ticks = avg_px;
        }
        if self.last_closed_worst_ticks.0 <= 0 {
            self.last_closed_worst_ticks = avg_px;
        }
        match side {
            Side::Bid => {
                if obs_px.0 > self.last_closed_peak_ticks.0 {
                    self.last_closed_peak_ticks = obs_px;
                }
                if obs_px.0 < self.last_closed_worst_ticks.0 {
                    self.last_closed_worst_ticks = obs_px;
                }
            }
            Side::Ask => {
                if obs_px.0 < self.last_closed_peak_ticks.0 {
                    self.last_closed_peak_ticks = obs_px;
                }
                if obs_px.0 > self.last_closed_worst_ticks.0 {
                    self.last_closed_worst_ticks = obs_px;
                }
            }
        }
        let d_sign = side.sign_i64();
        let pk_diff = (self.last_closed_peak_ticks.0 - avg_px.0).saturating_mul(d_sign);
        let wr_diff = (self.last_closed_worst_ticks.0 - avg_px.0).saturating_mul(d_sign);
        let denom = avg_px.0.max(1);
        self.last_closed_mfe_bps =
            self.last_closed_mfe_bps.max((pk_diff.saturating_mul(1_000_000)) / denom);
        self.last_closed_mae_bps =
            self.last_closed_mae_bps.min((wr_diff.saturating_mul(1_000_000)) / denom);
    }

    #[must_use]
    pub fn reconcile_closed_pnl_payload(
        &self,
        payload: &serde_json::Value,
        vac_level_hint: usize,
        allow_partial_lot_coverage: bool,
    ) -> ClosedPnlQueryOutcome {
        let Some(ctx) = self.open_exit_context() else {
            return ClosedPnlQueryOutcome::TransportOrApiError;
        };
        reconcile_closed_pnl_core(&ctx, payload, vac_level_hint, allow_partial_lot_coverage)
    }

    #[must_use]
    pub fn resolve_fallback_unverified_close(
        &self,
        snap: &SymbolLobSnapshot,
        vac_level_hint: usize,
    ) -> Option<VerifiedCloseResolution> {
        let ctx = self.open_exit_context()?;
        Some(resolve_fallback_unverified_close_core(&ctx, snap, vac_level_hint))
    }

    pub fn apply_verified_close_resolution(
        &mut self,
        resolution: VerifiedCloseResolution,
        closed_ts: TimestampNs,
        ledger: &mut ParitySyncLedger,
    ) -> Option<ClosedTradeSummary> {
        let OrderLifecycleState::PositionOpenMakerExit { side, .. } = self.state else {
            return None;
        };
        self.last_exit_sub_reason = resolution.sub_reason;
        if matches!(resolution.kind, ClosedTradeKind::MakerPassiveUnwind)
            && matches!(
                resolution.sub_reason,
                ExitSubReason::LvdbsDeepVacuumSurf | ExitSubReason::FibonacciImpulseSurf
            )
        {
            self.lvdbs_deep_surfs_count = self.lvdbs_deep_surfs_count.saturating_add(1);
        }
        Some(self.finalize_close_with_lots(
            resolution.kind,
            side,
            resolution.verified_entry_price,
            resolution.verified_exit_price,
            resolution.closed_lots,
            resolution.legs_count,
            resolution.exchange_closed_pnl_1e8,
            closed_ts,
            ledger,
        ))
    }

    #[must_use]
    pub fn compute_canonical_exit_quote(
        &self,
        snap: &SymbolLobSnapshot,
    ) -> Option<CanonicalExitQuote> {
        let ctx = self.open_exit_context()?;
        compute_canonical_exit_quote_core(&ctx, snap)
    }

    pub fn finalize_close_with_lots(
        &mut self,
        kind: ClosedTradeKind,
        side: Side,
        entry_price: PriceTicks,
        exit_price: PriceTicks,
        closed_lots: LotQty,
        legs_consolidated: u32,
        exchange_closed_pnl_1e8: Option<i64>,
        closed_ts: TimestampNs,
        ledger: &mut ParitySyncLedger,
    ) -> ClosedTradeSummary {
        let is_maker_exit = !matches!(kind, ClosedTradeKind::ServerMarkPriceStopLoss);
        let fee_scaled_bps = if is_maker_exit {
            ScaledBps(ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2)
        } else {
            ScaledBps(
                ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw()
                    + ScaledBps::BYBIT_VIP0_TAKER_FEE_BPS.raw(),
            )
        };
        let eff_lots = LotQty(closed_lots.0.max(1));
        let spec = APPROVED_SPECS[self.slot_idx.min(MAX_ACTIVE_PAIRS - 1)];
        let (calc_pnl_1e8, fee_1e8, calc_gross_bps, calc_net_bps) =
            spec.compute_leg_pnl_and_fee_1e8(side, eff_lots, entry_price, exit_price, fee_scaled_bps);
        let net_pnl_1e8 = exchange_closed_pnl_1e8.unwrap_or(calc_pnl_1e8);
        let entry_notional_1e8 = spec.lot_notional_1e8(eff_lots, entry_price);
        let (gross_scaled_bps, net_scaled_bps) =
            if exchange_closed_pnl_1e8.is_some() && entry_notional_1e8 > 0 {
                let exact_net =
                    ((net_pnl_1e8 as i128 * 1_000_000) / (entry_notional_1e8 as i128)) as i64;
                let exact_gross = exact_net.saturating_add(fee_scaled_bps.raw());
                if exact_net >= 100
                    && matches!(self.last_exit_sub_reason, ExitSubReason::Tranche1Harvest50)
                {
                    self.last_exit_sub_reason = ExitSubReason::LvdbsDeepVacuumSurf;
                }
                (ScaledBps(exact_gross), ScaledBps(exact_net))
            } else {
                (calc_gross_bps, calc_net_bps)
            };

        self.fold_price_into_excursions(side, entry_price, exit_price);
        ledger.record_closed_trade_with_sinking_split(net_pnl_1e8, fee_1e8, net_scaled_bps);
        self.observe_mfe_sample(self.last_closed_mfe_bps.max(gross_scaled_bps.raw()));
        let post_close_breather_ns = (self.intrinsic_tau_mkt_s.max(90) * 1_000_000_000) / 100;
        self.cooldown_until_ns = TimestampNs(closed_ts.0 + post_close_breather_ns);
        self.close_reconcile_retries = 0;
        self.state = OrderLifecycleState::Idle;
        ClosedTradeSummary {
            kind,
            side,
            entry_price,
            exit_price,
            closed_lots: eff_lots,
            legs_consolidated: legs_consolidated.max(1),
            gross_scaled_bps,
            fee_scaled_bps,
            net_scaled_bps,
            net_pnl_1e8,
            fee_1e8,
            is_maker_exit,
        }
    }

    pub fn finalize_open_position_close(
        &mut self,
        leg2_kind: ClosedTradeKind,
        leg2_exit_price: PriceTicks,
        closed_ts: TimestampNs,
        ledger: &mut ParitySyncLedger,
    ) -> Option<ClosedTradeSummary> {
        let OrderLifecycleState::PositionOpenMakerExit {
            side, avg_price_ticks, open_lots, tranche1_harvested, tranche1_lots,
            tranche1_exit_price, ..
        } = self.state
        else {
            return None;
        };
        let q2 = LotQty(open_lots.0.max(1));
        if !tranche1_harvested || tranche1_lots.0 <= 0 || tranche1_exit_price.0 <= 0 {
            return Some(self.finalize_close_with_lots(
                leg2_kind, side, avg_price_ticks, leg2_exit_price, q2, 1, None, closed_ts, ledger,
            ));
        }
        let spec = APPROVED_SPECS[self.slot_idx.min(MAX_ACTIVE_PAIRS - 1)];
        let q1 = tranche1_lots;
        let fee1_bps = ScaledBps(ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2);
        let is_maker_exit = !matches!(leg2_kind, ClosedTradeKind::ServerMarkPriceStopLoss);
        let fee2_bps = if is_maker_exit {
            fee1_bps
        } else {
            ScaledBps(
                ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw()
                    + ScaledBps::BYBIT_VIP0_TAKER_FEE_BPS.raw(),
            )
        };
        let (pnl1, fee1, g1, _) =
            spec.compute_leg_pnl_and_fee_1e8(side, q1, avg_price_ticks, tranche1_exit_price, fee1_bps);
        let (pnl2, fee2, g2, _) =
            spec.compute_leg_pnl_and_fee_1e8(side, q2, avg_price_ticks, leg2_exit_price, fee2_bps);

        let tot_q = (q1.0 + q2.0).max(1);
        let tot_q_128 = tot_q as i128;
        let w_exit = (q1.0 as i128) * (tranche1_exit_price.0 as i128)
            + (q2.0 as i128) * (leg2_exit_price.0 as i128);
        let avg_exit_ticks = match side {
            Side::Bid => (w_exit / tot_q_128).max(1) as i64,
            Side::Ask => ((w_exit + tot_q_128 - 1) / tot_q_128) as i64,
        };
        let gross_bps = (q1.0 * g1.raw() + q2.0 * g2.raw()) / tot_q;
        let fee_bps = (q1.0 * fee1_bps.raw() + q2.0 * fee2_bps.raw()) / tot_q;
        let net_bps = gross_bps - fee_bps;
        let net_pnl_1e8 = pnl1.saturating_add(pnl2);
        let fee_1e8 = fee1.saturating_add(fee2);

        self.fold_price_into_excursions(side, avg_price_ticks, tranche1_exit_price);
        self.fold_price_into_excursions(side, avg_price_ticks, leg2_exit_price);
        ledger.record_closed_trade_with_sinking_split(net_pnl_1e8, fee_1e8, ScaledBps(net_bps));
        self.observe_mfe_sample(self.last_closed_mfe_bps.max(gross_bps));
        let post_close_breather_ns = (self.intrinsic_tau_mkt_s.max(90) * 1_000_000_000) / 100;
        self.cooldown_until_ns = TimestampNs(closed_ts.0 + post_close_breather_ns);
        self.close_reconcile_retries = 0;
        self.state = OrderLifecycleState::Idle;
        Some(ClosedTradeSummary {
            kind: leg2_kind,
            side,
            entry_price: avg_price_ticks,
            exit_price: PriceTicks(avg_exit_ticks),
            closed_lots: LotQty(tot_q),
            legs_consolidated: 2,
            gross_scaled_bps: ScaledBps(gross_bps),
            fee_scaled_bps: ScaledBps(fee_bps),
            net_scaled_bps: ScaledBps(net_bps),
            net_pnl_1e8,
            fee_1e8,
            is_maker_exit,
        })
    }

    pub fn step_open_position(
        &mut self,
        snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
        gov: &OnlineAdaptiveGovernor,
        ledger: &mut ParitySyncLedger,
    ) -> Option<ClosedTradeSummary> {
        self.sync_governor_dynamics(gov);
        let OrderLifecycleState::PositionOpenMakerExit {
            side, avg_price_ticks, mut peak_favorable_price, mut worst_adverse_price,
            sl_trigger_price, ..
        } = self.state
        else {
            return None;
        };
        let cur_bbo_ref = match side {
            Side::Bid => snap.asks[0].price,
            Side::Ask => snap.bids[0].price,
        };
        let ref_trade = if snap.last_trade_price.0 > 0 { snap.last_trade_price } else { cur_bbo_ref };
        match side {
            Side::Bid => {
                if ref_trade.0 > peak_favorable_price.0 {
                    peak_favorable_price = ref_trade;
                }
                if ref_trade.0 > 0 && ref_trade.0 < worst_adverse_price.0 {
                    worst_adverse_price = ref_trade;
                }
            }
            Side::Ask => {
                if ref_trade.0 > 0 && ref_trade.0 < peak_favorable_price.0 {
                    peak_favorable_price = ref_trade;
                }
                if ref_trade.0 > worst_adverse_price.0 {
                    worst_adverse_price = ref_trade;
                }
            }
        }
        if let OrderLifecycleState::PositionOpenMakerExit {
            peak_favorable_price: ref mut pk,
            worst_adverse_price: ref mut wr,
            ..
        } = self.state
        {
            *pk = peak_favorable_price;
            *wr = worst_adverse_price;
        }
        let d_sign = side.sign_i64();
        let avg_denom = avg_price_ticks.0.max(1);
        self.last_closed_mfe_bps =
            ((peak_favorable_price.0 - avg_price_ticks.0).saturating_mul(d_sign).saturating_mul(1_000_000))
                / avg_denom;
        self.last_closed_mae_bps =
            ((worst_adverse_price.0 - avg_price_ticks.0).saturating_mul(d_sign).saturating_mul(1_000_000))
                / avg_denom;
        self.last_closed_peak_ticks = peak_favorable_price;
        self.last_closed_worst_ticks = worst_adverse_price;

        if self.last_eff_sl_ticks.0 <= 0 {
            self.last_eff_sl_ticks = sl_trigger_price;
        }
        let eff_sl_now = self.last_eff_sl_ticks;
        let sl_hit = match side {
            Side::Bid => snap.mark_price.0 > 0 && snap.mark_price.0 <= eff_sl_now.0,
            Side::Ask => snap.mark_price.0 > 0 && snap.mark_price.0 >= eff_sl_now.0,
        };
        if sl_hit {
            self.last_exit_sub_reason = ExitSubReason::WaldBaseStopLoss;
            return self.finalize_open_position_close(
                ClosedTradeKind::ServerMarkPriceStopLoss, eff_sl_now, snap.ts_local_ns, ledger,
            );
        }
        let quote = self.compute_canonical_exit_quote(snap)?;
        self.last_closed_wald_pct = quote.wald_pct;
        self.latch_defensive_floor_if_needed(quote.defensive_lock_active);
        let _ = self.maybe_ratchet_wald_stop_loss(snap, btc_leader_snap);

        let reached_target = match side {
            Side::Bid => ref_trade.0 >= quote.target_price.0,
            Side::Ask => ref_trade.0 > 0 && ref_trade.0 <= quote.target_price.0,
        };
        if !reached_target {
            return None;
        }
        if matches!(quote.role, ExitOrderRole::Tranche1Harvest50) {
            let _ = self.record_tranche1_harvest(quote.target_price);
            return None;
        }
        self.exit_peg_attempts = self.exit_peg_attempts.saturating_add(1);
        let q_fl = defensive_zero_loss_maker_exit_price(side, avg_price_ticks);
        let above_qf = match side {
            Side::Bid => quote.target_price.0 > q_fl.0,
            Side::Ask => quote.target_price.0 < q_fl.0,
        };
        if above_qf
            && matches!(
                quote.sub_reason,
                ExitSubReason::FibonacciImpulseSurf | ExitSubReason::LvdbsDeepVacuumSurf
            )
        {
            self.lvdbs_deep_surfs_count = self.lvdbs_deep_surfs_count.saturating_add(1);
            self.last_exit_sub_reason = quote.sub_reason;
        } else if above_qf && quote.vac_level >= 2 {
            self.lvdbs_deep_surfs_count = self.lvdbs_deep_surfs_count.saturating_add(1);
            self.last_exit_sub_reason = ExitSubReason::LvdbsDeepVacuumSurf;
        } else {
            self.last_exit_sub_reason = quote.sub_reason;
        }
        self.finalize_open_position_close(
            ClosedTradeKind::MakerPassiveUnwind, quote.target_price, snap.ts_local_ns, ledger,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lob_shm::{BookLevel, MappedShmRegion};
    use crate::types::BoundedScore;
    use crate::wave_elliott::{WaveObservables, WaveRegime};

    #[test]
    fn test_fsm_p0a_breach_wald_ratchet_tranche_fusion_and_tear_free_shm() {
        let gov = OnlineAdaptiveGovernor::new();
        let mut ledger = ParitySyncLedger::new(6_628_080_000);
        let shm_path = MappedShmRegion::ephemeral_self_test_path("fsm_33b_f09");
        let reg = MappedShmRegion::open_or_create(&shm_path, true);
        assert!(reg.is_ok());

        let mut eng = SlotExecutionEngine::new(3);
        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = APPROVED_SPECS[3].symbol_id;
        snap.realized_vol_5m = ScaledBps(7_000);
        snap.ts_local_ns = TimestampNs(1_000_000_000);
        snap.bids[0] = BookLevel { price: PriceTicks(8_198), qty: LotQty(300) };
        snap.asks[0] = BookLevel { price: PriceTicks(8_199), qty: LotQty(250) };
        snap.mark_price = PriceTicks(8_198);
        snap.last_trade_price = PriceTicks(8_198);
        let verdict = SignalVerdict::AuthorizeMakerEntry {
            side: Side::Bid,
            target_excursion_scaled_bps: ScaledBps(650),
        };
        assert!(eng.try_arm_maker_entry(&snap, verdict, LotQty(26)).is_ok());
        assert!(eng.on_maker_entry_filled(TimestampNs(1_500_000_000)).is_ok());

        snap.ts_local_ns = TimestampNs(10_000_000_000);
        snap.bids[0] = BookLevel { price: PriceTicks(8_202), qty: LotQty(400) };
        snap.asks[0] = BookLevel { price: PriceTicks(8_203), qty: LotQty(200) };
        snap.mark_price = PriceTicks(8_203);
        snap.last_trade_price = PriceTicks(8_203);
        assert!(eng.step_open_position(&snap, &snap, &gov, &mut ledger).is_none());
        assert_eq!(eng.tranche1_harvests_count, 1);

        if let Ok(ref r) = reg {
            eng.sync_to_shm_telemetry(r.arena());
            if let Ok(tel) = r.arena().telemetry(3) {
                let ts = tel.read_consistent_or_default(4);
                assert_eq!(ts.state_code, 4);
                assert!(ts.tranche1_harvested);
            }
        }

        let ask_px = [8_203, 8_205, 8_208, 8_211];
        for (i, &p) in ask_px.iter().enumerate() {
            snap.bids[i] = BookLevel { price: PriceTicks(8_202 - i as i64), qty: LotQty(350) };
            snap.asks[i] = BookLevel { price: PriceTicks(p), qty: LotQty(200) };
        }
        snap.factor_ofi = BoundedScore::new_clamped(0.65);
        snap.factor_tfi = BoundedScore::new_clamped(0.75);
        snap.mark_price = PriceTicks(8_211);
        snap.last_trade_price = PriceTicks(8_211);
        let closed = eng.step_open_position(&snap, &snap, &gov, &mut ledger);
        assert!(closed.is_some());
        if let Some(summary) = closed {
            assert_eq!(summary.closed_lots, LotQty(26));
            assert_eq!(summary.legs_consolidated, 2);
            assert!(summary.net_pnl_1e8 > 1_250_000);
        }
        let _ = std::fs::remove_file(&shm_path);

        let mut gap_snap = snap;
        gap_snap.mark_price = PriceTicks(9_007);
        gap_snap.last_trade_price = PriceTicks(9_008);
        assert!(matches!(
            SlotExecutionEngine::evaluate_sl_or_emergency_flatten(
                Side::Ask,
                PriceTicks(8_030),
                &gap_snap
            ),
            Ok(ServerSlDecision::BreachEmergencyFlatten { .. })
        ));
    }

    #[test]
    fn test_fsm_fibonacci_1lot_btc_runner_and_mfe_plasticity_12x() {
        let gov = OnlineAdaptiveGovernor::new();
        let mut ledger = ParitySyncLedger::new(6_628_080_000);
        let mut eng = SlotExecutionEngine::new(0);
        for _ in 0..12 {
            eng.observe_mfe_sample(4_200);
        }
        assert!(eng.learned_mfe_bps > 3_500);
        assert!(eng.learned_mfe_bps <= 4_800);

        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = APPROVED_SPECS[0].symbol_id;
        snap.realized_vol_5m = ScaledBps(6_500);
        snap.ts_local_ns = TimestampNs(1_000_000_000);
        snap.bids[0] = BookLevel { price: PriceTicks(759_000), qty: LotQty(380) };
        snap.asks[0] = BookLevel { price: PriceTicks(759_001), qty: LotQty(260) };
        snap.mark_price = PriceTicks(759_000);
        snap.last_trade_price = PriceTicks(759_000);
        let verdict = SignalVerdict::AuthorizeMakerEntry {
            side: Side::Bid,
            target_excursion_scaled_bps: ScaledBps(2_800),
        };
        assert!(eng.try_arm_maker_entry(&snap, verdict, LotQty(1)).is_ok());
        assert!(eng.on_maker_entry_filled(TimestampNs(1_500_000_000)).is_ok());
        if let OrderLifecycleState::PositionOpenMakerExit { structural_tp_price, .. } = eng.state {
            assert!(structural_tp_price.0 >= 761_100);
        }

        // At +5 bps, ImpulseWave3 projects +45 bps remaining -> runner holds instead of choking
        snap.ts_local_ns = TimestampNs(15_000_000_000);
        snap.bids[0] = BookLevel { price: PriceTicks(759_380), qty: LotQty(360) };
        snap.asks[0] = BookLevel { price: PriceTicks(759_381), qty: LotQty(220) };
        snap.mark_price = PriceTicks(759_380);
        snap.last_trade_price = PriceTicks(759_380);
        snap.factor_ofi = BoundedScore::new_clamped(0.60);
        snap.factor_tfi = BoundedScore::new_clamped(0.65);
        snap.set_wave_observables(WaveObservables {
            regime: WaveRegime::ImpulseWave3,
            climax_active: false,
            hurst: WaveObservables::encode_hurst(0.64),
            macro_phase: BoundedScore::new_clamped(0.85),
            oi_impulse: BoundedScore::new_clamped(0.42),
            liq_imbalance: BoundedScore::new_clamped(0.55),
            fib_proj_bps: ScaledBps(4_500),
        });
        assert!(eng.step_open_position(&snap, &snap, &gov, &mut ledger).is_none());

        // Price reaches the +50 bps Fibonacci projection -> harvests full 100% Maker impulse
        snap.ts_local_ns = TimestampNs(35_000_000_000);
        snap.bids[0] = BookLevel { price: PriceTicks(762_798), qty: LotQty(340) };
        snap.asks[0] = BookLevel { price: PriceTicks(762_799), qty: LotQty(180) };
        snap.mark_price = PriceTicks(762_803);
        snap.last_trade_price = PriceTicks(762_805);
        snap.set_wave_observables(WaveObservables {
            regime: WaveRegime::ImpulseWave3,
            climax_active: false,
            hurst: WaveObservables::encode_hurst(0.65),
            macro_phase: BoundedScore::new_clamped(0.88),
            oi_impulse: BoundedScore::new_clamped(0.40),
            liq_imbalance: BoundedScore::new_clamped(0.50),
            fib_proj_bps: ScaledBps(5),
        });
        let closed = eng.step_open_position(&snap, &snap, &gov, &mut ledger);
        assert!(closed.is_some());
        if let Some(summary) = closed {
            assert_eq!(eng.last_exit_sub_reason, ExitSubReason::FibonacciImpulseSurf);
            assert_eq!(eng.lvdbs_deep_surfs_count, 1);
            assert!(summary.is_maker_exit);
            assert!(summary.net_scaled_bps.raw() >= 4_500);
        }
    }
}
