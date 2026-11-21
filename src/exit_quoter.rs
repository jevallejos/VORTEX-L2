//! Quantum Maker Exit Quoter, Live VIP Fee Floors, Shadow-Runner & Chandelier SL (V4.8).
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::manual_clamp,
    clippy::manual_range_contains,
    clippy::needless_range_loop,
    clippy::suboptimal_flops,
    clippy::too_many_arguments
)]

use crate::fee_cadence_beta::FeeSchedule;
use crate::lob_shm::{SymbolLobSnapshot, MAX_ACTIVE_PAIRS};
use crate::types::{
    BoundedScore, LotQty, PairSpec, PriceTicks, ScaledBps, Side, TimestampNs, VortexError,
    APPROVED_SPECS, SPLIT_LEG_MIN_NOTIONAL_1E8,
};
use crate::wave_elliott::{CausalElliottWaveState, WaveObservables, WaveRegime};
use crate::ws_protocol::parse_verified_avg_price_ticks;

pub const OFFENSIVE_WIN_FLOOR_GROSS_BPS: i64 = 520;
pub const MAX_CLOSE_RECONCILE_RETRIES: u8 = 4;

const PHI: f64 = 1.618_033_988_749_895;
const PHI_SQ: f64 = 2.618_033_988_749_895;
const INV_PHI: f64 = 0.618_033_988_749_895;
const INV_PHI_SQ: f64 = 0.381_966_011_250_105;
const INV_PHI_CUBE: f64 = 0.236_067_977_499_79;
const INV_PHI_FIFTH: f64 = 0.090_169_943_749_47;
const PHI_CONJ_DELTA_PM: f64 = 118.0;
const SWING_RATIO_300S_15S_SQRT: f64 = 4.472_135_954_999_58;
const MIN_VALID_UTC_EPOCH_NS: i64 = 1_577_836_800_000_000_000;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ClosedTradeKind {
    MakerPassiveUnwind,
    ServerMarkPriceStopLoss,
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum ExitOrderRole {
    #[default]
    None,
    Tranche1Harvest50,
    Tranche2HarvestPhi,
    LvdbsDeepSurf50,
    StandardMakerExit100,
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum ExitSubReason {
    #[default]
    AdverseScratchEscape,
    Tranche1Harvest50,
    Tranche2HarvestPhi,
    StandardStructuralTp,
    LvdbsDeepVacuumSurf,
    FibonacciImpulseSurf,
    ShadowRunnerCrestHarvest,
    ConvexMfeRatchetLock,
    ClimaxExhaustionHarvest,
    ChandelierProfitLockSl,
    WaldBaseStopLoss,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct ClosedTradeSummary {
    pub kind: ClosedTradeKind,
    pub side: Side,
    pub entry_price: PriceTicks,
    pub exit_price: PriceTicks,
    pub closed_lots: LotQty,
    pub legs_consolidated: u32,
    pub gross_scaled_bps: ScaledBps,
    pub fee_scaled_bps: ScaledBps,
    pub net_scaled_bps: ScaledBps,
    pub net_pnl_1e8: i64,
    pub fee_1e8: i64,
    pub is_maker_exit: bool,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ServerSlDecision {
    ArmServerSl(PriceTicks),
    BreachEmergencyFlatten {
        backup_sl: PriceTicks,
        observed_loss_bps: ScaledBps,
        max_allowed_bps: ScaledBps,
    },
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct ShadowRunnerKinematics {
    pub flow_ema_fast: f64,
    pub flow_ema_slow: f64,
    pub in_scar_debt: bool,
    pub chandelier_armed: bool,
}

impl Default for ShadowRunnerKinematics {
    #[inline]
    fn default() -> Self { Self::neutral() }
}

impl ShadowRunnerKinematics {
    #[inline]
    #[must_use]
    pub const fn neutral() -> Self {
        Self { flow_ema_fast: 0.0, flow_ema_slow: 0.0, in_scar_debt: false, chandelier_armed: false }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct OpenExitContext {
    pub slot_idx: usize,
    pub side: Side,
    pub avg_price_ticks: PriceTicks,
    pub peak_favorable_price: PriceTicks,
    pub worst_adverse_price: PriceTicks,
    pub initial_lots: LotQty,
    pub open_lots: LotQty,
    pub tranche1_harvested: bool,
    pub tranche1_lots: LotQty,
    pub tranche1_exit_price: PriceTicks,
    pub defensive_floor_locked: bool,
    pub active_exit_price: PriceTicks,
    pub structural_tp_price: PriceTicks,
    pub resting_exit_price: PriceTicks,
    pub sl_trigger_price: PriceTicks,
    pub opened_ts: TimestampNs,
    pub learned_mfe_bps: i64,
    pub learned_t1_permille: i64,
    pub learned_ir_x100: i64,
    pub learned_ret_std_bps: i64,
    pub intrinsic_tau_mkt_s: i64,
    pub last_eff_sl_ticks: PriceTicks,
    pub ratchet_locked_gross_bps: i64,
    pub last_closed_mfe_bps: i64,
    pub last_closed_mae_bps: i64,
    pub active_fees: FeeSchedule,
    pub learned_taker_prob_pm: i64,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct CanonicalExitQuote {
    pub role: ExitOrderRole,
    pub target_price: PriceTicks,
    pub order_lots: LotQty,
    pub sub_reason: ExitSubReason,
    pub vac_level: usize,
    pub defensive_lock_active: bool,
    pub urgent_close_repeg: bool,
    pub cur_gross_bps: i64,
    pub wald_pct: i64,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct VerifiedCloseResolution {
    pub kind: ClosedTradeKind,
    pub sub_reason: ExitSubReason,
    pub verified_entry_price: PriceTicks,
    pub verified_exit_price: PriceTicks,
    pub closed_lots: LotQty,
    pub legs_count: u32,
    pub exchange_closed_pnl_1e8: Option<i64>,
    pub exact_fee_1e8: Option<i64>,
    pub maker_closed_lots: LotQty,
    pub taker_closed_lots: LotQty,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ClosedPnlQueryOutcome {
    Verified(VerifiedCloseResolution),
    PendingIndexerRetry,
    TransportOrApiError,
}

#[inline]
#[must_use]
pub const fn dynamic_maker_floor_price(side: Side, avg_price_ticks: PriceTicks, target_gross_bps: ScaledBps) -> PriceTicks {
    let avg = if avg_price_ticks.0 > 1 { avg_price_ticks.0 } else { 1 };
    let bps = if target_gross_bps.0 > 0 { target_gross_bps.0 } else { 0 };
    let raw_delta = (((avg as i128) * (bps as i128) + 999_999) / 1_000_000) as i64;
    let delta = if raw_delta > 1 { raw_delta } else { 1 };
    match side {
        Side::Bid => PriceTicks(avg.saturating_add(delta)),
        Side::Ask => {
            let sub = avg - delta;
            PriceTicks(if sub > 1 { sub } else { 1 })
        }
    }
}

/// Exact closed-form quantization to the first integer tick with strictly positive net Maker PnL (`PnL_net > 0`).
/// E.g., on `SUIUSDT Bid @ 7138` with VIP0 `4.00 bps` RT Maker fee, yields `7141t` (`+4.20 bps` gross, `+0.20 bps` net), never `7142t`.
#[inline]
#[must_use]
pub const fn defensive_zero_loss_maker_exit_price_with_fees(
    side: Side,
    avg_price_ticks: PriceTicks,
    fees: FeeSchedule,
) -> PriceTicks {
    let avg = if avg_price_ticks.0 > 1 { avg_price_ticks.0 } else { 1 };
    let m = fees.maker_fee_bps.0;
    let rt = m.saturating_mul(2);
    let delta = if rt <= 0 {
        1_i64
    } else {
        let denom = match side {
            Side::Bid => 1_000_000_i128 - (m as i128),
            Side::Ask => 1_000_000_i128 + (m as i128),
        };
        let safe_denom = if denom > 1 { denom } else { 1 };
        (((avg as i128) * (rt as i128)) / safe_denom) as i64 + 1
    };
    let eff_delta = if delta > 1 { delta } else { 1 };
    match side {
        Side::Bid => PriceTicks(avg.saturating_add(eff_delta)),
        Side::Ask => {
            let sub = avg - eff_delta;
            PriceTicks(if sub > 1 { sub } else { 1 })
        }
    }
}

#[inline]
#[must_use]
pub const fn defensive_zero_loss_maker_exit_price(side: Side, avg_price_ticks: PriceTicks) -> PriceTicks {
    defensive_zero_loss_maker_exit_price_with_fees(side, avg_price_ticks, FeeSchedule::VIP0_DEFAULT)
}

#[inline]
#[must_use]
pub const fn compute_elastic_offensive_win_floor_bps(fee_rt_bps: i64, vol_5m: ScaledBps) -> ScaledBps {
    let eff_rt = if fee_rt_bps > 100 { fee_rt_bps } else { 100 };
    let base_13_10 = (eff_rt.saturating_mul(13) + 9) / 10;
    let vol_pos = if vol_5m.0 > 0 { vol_5m.0 } else { 0 };
    let vol_term = (vol_pos.saturating_mul(72)) / 1_000;
    let raw = if base_13_10 > vol_term { base_13_10 } else { vol_term };
    let lo = eff_rt.saturating_add(60);
    let hi = (eff_rt.saturating_mul(3)) / 2;
    let eff_hi = if hi > lo { hi } else { lo };
    ScaledBps(if raw < lo { lo } else if raw > eff_hi { eff_hi } else { raw })
}

#[inline]
#[must_use]
pub fn elastic_offensive_win_maker_exit_price(
    side: Side,
    avg_price_ticks: PriceTicks,
    fee_rt_bps: i64,
    vol_5m: ScaledBps,
    urgency_0_to_1: f64,
) -> PriceTicks {
    let u = if urgency_0_to_1.is_nan() { 0.50 } else { urgency_0_to_1.clamp(0.0, 1.0) };
    let floor_bps = compute_elastic_offensive_win_floor_bps(fee_rt_bps, vol_5m).raw();
    let clamped = ((floor_bps as f64 + 60.0 * (0.50 - u)).round() as i64).clamp(floor_bps, floor_bps + 60);
    dynamic_maker_floor_price(side, avg_price_ticks, ScaledBps(clamped))
}

#[inline]
#[must_use]
pub fn offensive_win_maker_exit_price(side: Side, avg_price_ticks: PriceTicks, urgency_0_to_1: f64) -> PriceTicks {
    elastic_offensive_win_maker_exit_price(
        side, avg_price_ticks, ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2, ScaledBps(0), urgency_0_to_1,
    )
}

#[inline]
#[must_use]
pub fn evaluate_shadow_runner_propulsion(side: Side, snap: &SymbolLobSnapshot, flow_ema_fast: f64, flow_ema_slow: f64) -> (f64, f64) {
    let dir_s = side.sign_f64();
    let phi_t = (0.50 * snap.factor_ofi.get() + 0.38 * snap.factor_tfi.get() + 0.12 * snap.factor_geom.get()) * dir_s;
    let flow_accel = (flow_ema_fast - flow_ema_slow) * dir_s;
    let h_term = ((snap.wave_observables().hurst_exponent() - 0.50) / 0.35).max(0.0);
    (0.58 * phi_t + 0.27 * (3.0 * flow_accel).tanh() + 0.15 * h_term, flow_accel)
}

#[inline]
#[must_use]
pub fn compute_wave_dynamic_t1_permille(base_learned_permille: i64, side: Side, snap: &SymbolLobSnapshot, in_scar_debt: bool) -> i64 {
    if in_scar_debt { return (base_learned_permille + 100).clamp(580, 650); }
    let w_obs = snap.wave_observables();
    let dir_s = side.sign_f64();
    let signed_phase = w_obs.macro_phase.get() * dir_s;
    let hurst = w_obs.hurst_exponent();
    let squeeze_fuel = (0.60 * (w_obs.liq_imbalance.get() * dir_s).max(0.0) + 0.40 * w_obs.oi_impulse.get().max(0.0)).clamp(0.0, 1.0);
    let h_excess = ((hurst - 0.52) / 0.20).max(0.0);
    let delta = if matches!(w_obs.regime, WaveRegime::ImpulseWave3) && signed_phase > 0.15 {
        let gp_res = w_obs.golden_pocket_resonance(side);
        let sp = crate::types::spec_from_symbol_id(snap.symbol_id).unwrap_or(APPROVED_SPECS[0]);
        let m_px = if snap.bids[0].price.0 > 0 && snap.asks[0].price.0 > 0 { PriceTicks(i64::midpoint(snap.bids[0].price.0, snap.asks[0].price.0)) } else { snap.mark_price };
        let mut al_ob = snap.order_block_observables();
        let al_pm = match side { Side::Bid => snap.ob_bull_efficacy_pm, Side::Ask => snap.ob_bear_efficacy_pm };
        if al_pm > 0 { al_ob.learned_efficacy_pm = al_pm; }
        let ob_res = al_ob.aligned_confluence_resonance(side, sp, m_px, snap.realized_vol_5m);
        let conv = (0.50 + 0.28 * h_excess + 0.22 * squeeze_fuel + 0.18 * gp_res).min(1.0);
        let gp_boost = if gp_res >= 0.85 { (32.0 * gp_res * gp_res).round() as i64 } else { 0 };
        -((PHI_CONJ_DELTA_PM * conv).round() as i64) - gp_boost - ((24.0 * ob_res * (0.50 + 0.50 * gp_res)).round() as i64)
    } else if matches!(w_obs.regime, WaveRegime::ImpulseWave5) && signed_phase > 0.15 {
        (PHI_CONJ_DELTA_PM * (0.75 - 0.25 * squeeze_fuel)).round() as i64
    } else if matches!(w_obs.regime, WaveRegime::CorrectiveWave4 | WaveRegime::CorrectiveAbc | WaveRegime::ClimaxExhaustion) || signed_phase < -0.15 {
        PHI_CONJ_DELTA_PM.round() as i64
    } else {
        -(40.0 * (hurst - 0.50) / 0.20).round() as i64
    };
    let eff_base = if delta <= -60 {
        base_learned_permille.min(500)
    } else if delta >= 60 {
        base_learned_permille.max(500)
    } else {
        base_learned_permille
    };
    (eff_base + delta).clamp(350, 650)
}

#[inline]
#[must_use]
pub const fn is_chandelier_profit_locked_sl_with_fees(
    side: Side,
    avg_price_ticks: PriceTicks,
    sl_ticks: PriceTicks,
    fees: FeeSchedule,
) -> bool {
    if avg_price_ticks.0 <= 0 || sl_ticks.0 <= 0 { return false; }
    let sum_mt = fees.round_trip_taker_exit_bps().raw();
    let fee_sl_rt = if sum_mt > 50 { sum_mt } else { 50 };
    let diff = match side { Side::Bid => sl_ticks.0 - avg_price_ticks.0, Side::Ask => avg_price_ticks.0 - sl_ticks.0 };
    if diff <= 0 { return false; }
    (diff.saturating_mul(1_000_000)) / avg_price_ticks.0 > fee_sl_rt
}

#[inline]
#[must_use]
pub const fn is_chandelier_profit_locked_sl(side: Side, avg_price_ticks: PriceTicks, sl_ticks: PriceTicks) -> bool {
    is_chandelier_profit_locked_sl_with_fees(side, avg_price_ticks, sl_ticks, FeeSchedule::VIP0_DEFAULT)
}

#[must_use]
pub fn compute_chandelier_profit_lock_sl(ctx: &OpenExitContext, snap: &SymbolLobSnapshot) -> Option<PriceTicks> {
    if ctx.avg_price_ticks.0 <= 0 || ctx.open_lots.0 <= 0 { return None; }
    let spec = APPROVED_SPECS[ctx.slot_idx.min(MAX_ACTIVE_PAIRS - 1)];
    let tick_bps = spec.tick_to_price_scaled_bps(ctx.avg_price_ticks).unwrap_or(ScaledBps(15)).raw();
    let fee_sl_rt = ctx.active_fees.round_trip_taker_exit_bps().raw().max(50);
    let green_floor_bps = fee_sl_rt + 2 * tick_bps + 100;
    let state_mfe = ((ctx.peak_favorable_price.0 - ctx.avg_price_ticks.0).saturating_mul(ctx.side.sign_i64()).saturating_mul(1_000_000))
        / ctx.avg_price_ticks.0.max(1);
    let best_mfe_bps = ctx.last_closed_mfe_bps.max(state_mfe);
    if best_mfe_bps <= green_floor_bps { return None; }
    let w_obs = snap.wave_observables();
    let k_reg = if w_obs.climax_active || w_obs.regime == WaveRegime::ClimaxExhaustion {
        0.45_f64
    } else if matches!(w_obs.regime, WaveRegime::ImpulseWave5 | WaveRegime::CorrectiveWave4 | WaveRegime::CorrectiveAbc) {
        0.72_f64
    } else {
        1.00_f64
    };
    let vol_eff = snap.realized_vol_5m.raw().max(1_200);
    let d_vol = (((vol_eff as f64) / SWING_RATIO_300S_15S_SQRT * k_reg).round() as i64).clamp(400, 2_800);
    let d_ell = (INV_PHI_SQ * (best_mfe_bps as f64) * k_reg).round() as i64;
    let g_lock_bps = best_mfe_bps - d_vol.max(d_ell);
    if g_lock_bps < green_floor_bps { return None; }
    let lock_delta = (((ctx.avg_price_ticks.0 as i128) * (g_lock_bps as i128)) / 1_000_000).max(1) as i64;
    let c_ticks = ((((ctx.avg_price_ticks.0 as i128) * (vol_eff as i128) * 8) / 100_000_000) as i64).max(4);
    let ref_mark = if snap.mark_price.0 > 0 { snap.mark_price } else { ctx.avg_price_ticks };
    let ref_last = if snap.last_trade_price.0 > 0 { snap.last_trade_price } else { ref_mark };
    let prev_sl = if ctx.last_eff_sl_ticks.0 > 0 { ctx.last_eff_sl_ticks } else { ctx.sl_trigger_price };
    let min_step_ticks = ((((ctx.avg_price_ticks.0 as i128) * ((4 * tick_bps).max(120) as i128)) + 999_999) / 1_000_000).max(3) as i64;
    let green_bound_px = dynamic_maker_floor_price(ctx.side, ctx.avg_price_ticks, ScaledBps(green_floor_bps));
    match ctx.side {
        Side::Bid => {
            let mut guard_lo = ref_mark.0.min(ref_last.0);
            if snap.bids[0].price.0 > 0 { guard_lo = guard_lo.min(snap.bids[0].price.0); }
            let eff_px = PriceTicks(ctx.avg_price_ticks.0.saturating_add(lock_delta).min(guard_lo - c_ticks).max(1));
            if eff_px.0 < green_bound_px.0 || (prev_sl.0 > 0 && eff_px.0 < prev_sl.0.saturating_add(min_step_ticks)) { return None; }
            PairSpec::validate_stop_loss_geometry(Side::Bid, eff_px, ref_last, ref_mark).ok()
        }
        Side::Ask => {
            let mut guard_hi = ref_mark.0.max(ref_last.0);
            if snap.asks[0].price.0 > 0 { guard_hi = guard_hi.max(snap.asks[0].price.0); }
            let eff_px = PriceTicks((ctx.avg_price_ticks.0 - lock_delta).max(1).max(guard_hi.saturating_add(c_ticks)));
            if eff_px.0 > green_bound_px.0 || (prev_sl.0 > 0 && eff_px.0 > prev_sl.0.saturating_sub(min_step_ticks)) { return None; }
            PairSpec::validate_stop_loss_geometry(Side::Ask, eff_px, ref_last, ref_mark).ok()
        }
    }
}

#[inline]
#[must_use]
pub fn compute_tranche1_split_lots_core(slot_idx: usize, open_lots: LotQty, exit_price: PriceTicks, t1_permille: i64) -> Option<LotQty> {
    if open_lots.0 < 2 || exit_price.0 <= 0 { return None; }
    let spec = APPROVED_SPECS[slot_idx.min(MAX_ACTIVE_PAIRS - 1)];
    if spec.lot_notional_1e8(open_lots, exit_price) < SPLIT_LEG_MIN_NOTIONAL_1E8 {
        return None;
    }
    let one_lot_1e8 = spec.lot_notional_1e8(LotQty(1), exit_price);
    if one_lot_1e8 <= 0 { return None; }
    let full_leg_lots = ((SPLIT_LEG_MIN_NOTIONAL_1E8 + one_lot_1e8 - 1) / one_lot_1e8).max(1);
    let min_leg_lots = if open_lots.0 >= full_leg_lots.saturating_mul(2) {
        full_leg_lots
    } else {
        1
    };
    let raw_t1 = (((open_lots.0.saturating_mul(t1_permille.clamp(350, 650))) + 500) / 1_000).clamp(1, open_lots.0 - 1);
    let t1_lots = LotQty(raw_t1.clamp(min_leg_lots, open_lots.0 - min_leg_lots));
    let rem_lots = LotQty(open_lots.0 - t1_lots.0);
    if t1_lots.0 < 1 || rem_lots.0 < 1 {
        None
    } else {
        Some(t1_lots)
    }
}

#[inline]
#[must_use]
pub fn can_split_runner_legs(
    slot_idx: usize,
    open_lots: LotQty,
    exit_price: PriceTicks,
    t1_permille: i64,
) -> bool {
    if open_lots.0 < 2 || exit_price.0 <= 0 {
        return false;
    }
    let spec = APPROVED_SPECS[slot_idx.min(MAX_ACTIVE_PAIRS - 1)];
    let one_lot_1e8 = spec.lot_notional_1e8(LotQty(1), exit_price);
    if one_lot_1e8 <= 0 {
        return false;
    }
    let full_leg_lots = ((SPLIT_LEG_MIN_NOTIONAL_1E8 + one_lot_1e8 - 1) / one_lot_1e8).max(1);
    open_lots.0 >= full_leg_lots.saturating_mul(3)
        && compute_tranche1_split_lots_core(slot_idx, open_lots, exit_price, t1_permille).is_some()
}

/// Exact consolidated House-Money Stop-Loss floor `PriceTicks` where residual net risk across
/// remaining `T2 + T3` (`rem_lots`) against realized `T1` net profit (`realized_partial_net_1e8`) is `<= 0`.
#[inline]
#[must_use]
pub fn compute_house_money_sl_floor_ticks(
    slot_idx: usize,
    side: Side,
    avg_price_ticks: PriceTicks,
    rem_lots: LotQty,
    realized_partial_net_1e8: i64,
    fees: FeeSchedule,
) -> PriceTicks {
    let avg = avg_price_ticks.0.max(1);
    let rem_q = LotQty(rem_lots.0.max(1));
    let spec = APPROVED_SPECS[slot_idx.min(MAX_ACTIVE_PAIRS - 1)];
    let realized_gain = realized_partial_net_1e8.max(0);
    let target_rem_pnl = -realized_gain;
    let one_tick_1e8 = spec.lot_notional_1e8(rem_q, PriceTicks(1)).max(1) as i128;
    let (pnl_at_avg, _, _, _) = spec.compute_leg_pnl_and_fee_1e8_with_schedule(
        side,
        rem_q,
        PriceTicks(avg),
        PriceTicks(avg),
        false,
        fees,
    );
    let needed_1e8 = (target_rem_pnl as i128) - (pnl_at_avg as i128);
    let approx_ticks = (needed_1e8 / one_tick_1e8) as i64;
    let mut cand = match side {
        Side::Bid => avg.saturating_add(approx_ticks).max(1),
        Side::Ask => (avg - approx_ticks).max(1),
    };
    for _ in 0..32 {
        let (pnl_c, _, _, _) = spec.compute_leg_pnl_and_fee_1e8_with_schedule(
            side,
            rem_q,
            PriceTicks(avg),
            PriceTicks(cand),
            false,
            fees,
        );
        if pnl_c >= target_rem_pnl {
            let looser = match side {
                Side::Bid => (cand - 1).max(1),
                Side::Ask => cand.saturating_add(1),
            };
            if looser == cand {
                break;
            }
            let (pnl_l, _, _, _) = spec.compute_leg_pnl_and_fee_1e8_with_schedule(
                side,
                rem_q,
                PriceTicks(avg),
                PriceTicks(looser),
                false,
                fees,
            );
            if pnl_l >= target_rem_pnl {
                cand = looser;
            } else {
                break;
            }
        } else {
            match side {
                Side::Bid => cand = cand.saturating_add(1),
                Side::Ask => {
                    if cand <= 1 {
                        break;
                    }
                    cand -= 1;
                }
            }
        }
    }
    PriceTicks(cand.max(1))
}

/// Validates `SPLIT_LEG_MIN_NOTIONAL_1E8` on `T2` and `T3 = open_lots - t2_lots` without creating `1-lot` sub-notional dust orphans (`[F-10]`).
#[inline]
#[must_use]
pub fn compute_tranche2_split_lots_core(
    slot_idx: usize,
    open_lots: LotQty,
    exit_price: PriceTicks,
) -> Option<LotQty> {
    if open_lots.0 < 2 || exit_price.0 <= 0 {
        return None;
    }
    let spec = APPROVED_SPECS[slot_idx.min(MAX_ACTIVE_PAIRS - 1)];
    let one_lot_1e8 = spec.lot_notional_1e8(LotQty(1), exit_price);
    if one_lot_1e8 <= 0 {
        return None;
    }
    let full_leg_lots = ((SPLIT_LEG_MIN_NOTIONAL_1E8 + one_lot_1e8 - 1) / one_lot_1e8).max(1);
    let min_leg_lots = if open_lots.0 >= full_leg_lots.saturating_mul(2) {
        full_leg_lots
    } else if one_lot_1e8 >= SPLIT_LEG_MIN_NOTIONAL_1E8 {
        1
    } else {
        2
    };
    if open_lots.0 < min_leg_lots.saturating_mul(2) {
        return None;
    }
    let raw_t2 = ((INV_PHI * (open_lots.0 as f64)).round() as i64)
        .clamp(min_leg_lots, open_lots.0 - min_leg_lots);
    let t2_lots = LotQty(raw_t2);
    let t3_lots = LotQty(open_lots.0 - t2_lots.0);
    if t2_lots.0 < min_leg_lots || t3_lots.0 < min_leg_lots {
        None
    } else {
        Some(t2_lots)
    }
}

#[inline]
pub fn compute_validated_sl_ticks_with_fees(
    side: Side,
    verified_avg_price: PriceTicks,
    snap: &SymbolLobSnapshot,
    fees: FeeSchedule,
) -> Result<PriceTicks, VortexError> {
    if verified_avg_price.0 <= 0 { return Err(VortexError::InvalidPriceOrTickSpec); }
    let sl_bps = PairSpec::elastic_server_sl_distance_bps_with_fees(snap.realized_vol_5m, fees).raw();
    let sl_delta = (((verified_avg_price.0 as i128 * sl_bps as i128) + 999_999) / 1_000_000).max(4) as i64;
    let ref_mark = if snap.mark_price.0 > 0 { snap.mark_price } else { verified_avg_price };
    let ref_last = if snap.last_trade_price.0 > 0 { snap.last_trade_price } else { ref_mark };
    let cand_sl = match side {
        Side::Bid => PriceTicks((verified_avg_price.0 - sl_delta).min(ref_mark.0.min(ref_last.0) - 4).max(1)),
        Side::Ask => PriceTicks(verified_avg_price.0.saturating_add(sl_delta).max(ref_mark.0.max(ref_last.0).saturating_add(4))),
    };
    PairSpec::validate_stop_loss_geometry(side, cand_sl, ref_last, ref_mark)
}

#[inline]
pub fn compute_validated_sl_ticks(side: Side, verified_avg_price: PriceTicks, snap: &SymbolLobSnapshot) -> Result<PriceTicks, VortexError> {
    compute_validated_sl_ticks_with_fees(side, verified_avg_price, snap, FeeSchedule::VIP0_DEFAULT)
}

#[inline]
pub fn evaluate_sl_or_emergency_flatten_with_fees(
    side: Side,
    verified_avg_price: PriceTicks,
    snap: &SymbolLobSnapshot,
    fees: FeeSchedule,
) -> Result<ServerSlDecision, VortexError> {
    let valid_sl = compute_validated_sl_ticks_with_fees(side, verified_avg_price, snap, fees)?;
    let max_allowed_bps = (PairSpec::elastic_server_sl_distance_bps_with_fees(snap.realized_vol_5m, fees).raw() * 3) / 2;
    let ref_mark = if snap.mark_price.0 > 0 { snap.mark_price } else { verified_avg_price };
    let ref_last = if snap.last_trade_price.0 > 0 { snap.last_trade_price } else { ref_mark };
    let adv_ticks = match side {
        Side::Bid => verified_avg_price.0 - ref_mark.0.min(ref_last.0),
        Side::Ask => ref_mark.0.max(ref_last.0) - verified_avg_price.0,
    };
    let observed_loss_bps = (adv_ticks.max(0).saturating_mul(1_000_000)) / verified_avg_price.0.max(1);
    if observed_loss_bps > max_allowed_bps {
        Ok(ServerSlDecision::BreachEmergencyFlatten {
            backup_sl: valid_sl,
            observed_loss_bps: ScaledBps(observed_loss_bps),
            max_allowed_bps: ScaledBps(max_allowed_bps),
        })
    } else {
        Ok(ServerSlDecision::ArmServerSl(valid_sl))
    }
}

#[inline]
pub fn evaluate_sl_or_emergency_flatten_core(side: Side, verified_avg_price: PriceTicks, snap: &SymbolLobSnapshot) -> Result<ServerSlDecision, VortexError> {
    evaluate_sl_or_emergency_flatten_with_fees(side, verified_avg_price, snap, FeeSchedule::VIP0_DEFAULT)
}

#[inline]
#[must_use]
pub const fn classify_maker_exit_sub_reason_with_fees(
    gross_scaled_bps: i64,
    vac_level_hint: usize,
    tranche1_harvested: bool,
    climax_active: bool,
    fib_projected: bool,
    fees: FeeSchedule,
) -> ExitSubReason {
    let rt_raw = fees.round_trip_maker_exit_bps().raw();
    let fee_rt = if rt_raw > 100 { rt_raw } else { 100 };
    let win_fl = compute_elastic_offensive_win_floor_bps(fee_rt, ScaledBps(0)).raw();
    if climax_active && gross_scaled_bps >= fee_rt {
        ExitSubReason::ClimaxExhaustionHarvest
    } else if fib_projected && gross_scaled_bps >= win_fl + 200 {
        ExitSubReason::FibonacciImpulseSurf
    } else if vac_level_hint >= 2 && gross_scaled_bps > fee_rt + 40 {
        ExitSubReason::LvdbsDeepVacuumSurf
    } else if gross_scaled_bps < win_fl {
        if tranche1_harvested { ExitSubReason::Tranche1Harvest50 } else { ExitSubReason::AdverseScratchEscape }
    } else {
        ExitSubReason::StandardStructuralTp
    }
}

#[inline]
#[must_use]
pub const fn classify_maker_exit_sub_reason(
    gross_scaled_bps: i64,
    vac_level_hint: usize,
    tranche1_harvested: bool,
    climax_active: bool,
    fib_projected: bool,
) -> ExitSubReason {
    classify_maker_exit_sub_reason_with_fees(
        gross_scaled_bps, vac_level_hint, tranche1_harvested, climax_active, fib_projected, FeeSchedule::VIP0_DEFAULT,
    )
}

fn parse_pnl_decimal_1e8(raw: &str) -> Option<i64> {
    let s = raw.trim();
    if s.is_empty() { return None; }
    let (neg, body) = if let Some(r) = s.strip_prefix('-') { (true, r) } else if let Some(r) = s.strip_prefix('+') { (false, r) } else { (false, s) };
    if body.is_empty() { return None; }
    let (mut whole, mut frac, mut frac_digits, mut in_frac) = (0_i128, 0_i128, 0_u32, false);
    for b in body.bytes() {
        if b == b'.' { if in_frac { return None; } in_frac = true; continue; }
        if !b.is_ascii_digit() { return None; }
        let d = i128::from(b - b'0');
        if !in_frac { whole = whole.saturating_mul(10).saturating_add(d); }
        else if frac_digits < 8 { frac = frac.saturating_mul(10).saturating_add(d); frac_digits += 1; }
    }
    while frac_digits < 8 { frac = frac.saturating_mul(10); frac_digits += 1; }
    let mag = whole.saturating_mul(100_000_000).saturating_add(frac);
    i64::try_from(if neg { -mag } else { mag }).ok()
}

#[inline]
#[must_use]
pub fn reconcile_closed_pnl_core(
    ctx: &OpenExitContext,
    payload: &serde_json::Value,
    vac_level_hint: usize,
    allow_partial_lot_coverage: bool,
) -> ClosedPnlQueryOutcome {
    reconcile_closed_pnl_core_v553(ctx, payload, vac_level_hint, allow_partial_lot_coverage, false)
}

#[must_use]
pub fn reconcile_closed_pnl_core_v553(
    ctx: &OpenExitContext,
    payload: &serde_json::Value,
    vac_level_hint: usize,
    allow_partial_lot_coverage: bool,
    chandelier_profit_sl_armed: bool,
) -> ClosedPnlQueryOutcome {
    if payload.get("retCode").and_then(serde_json::Value::as_i64) != Some(0) {
        return ClosedPnlQueryOutcome::TransportOrApiError;
    }
    let Some(list) = payload.get("result").and_then(|r| r.get("list")).and_then(serde_json::Value::as_array) else {
        return ClosedPnlQueryOutcome::TransportOrApiError;
    };
    if list.is_empty() { return ClosedPnlQueryOutcome::PendingIndexerRetry; }
    let spec = APPROVED_SPECS[ctx.slot_idx.min(MAX_ACTIVE_PAIRS - 1)];
    let expected_sym = spec.symbol_id.name();
    let expected_lots = if ctx.tranche1_harvested || ctx.tranche1_lots.0 > 0 {
        ctx.initial_lots.0.max(ctx.open_lots.0).max(1)
    } else {
        ctx.open_lots.0.max(1)
    };
    let min_ms = if ctx.opened_ts.0 >= MIN_VALID_UTC_EPOCH_NS { (ctx.opened_ts.0 / 1_000_000).saturating_sub(1_500) } else { 0 };
    let (mut acc_lots, mut w_en, mut w_ex, mut w_lots, mut sum_pnl, mut has_pnl, mut legs_count, mut saw_mkt_sl, mut saw_limit_exit) =
        (0_i64, 0_i128, 0_i128, 0_i128, 0_i64, false, 0_u32, false, false);
    let (mut exact_fee_sum_1e8, mut typed_fee_lots, mut maker_lots_acc, mut taker_lots_acc) =
        (0_i64, 0_i64, 0_i64, 0_i64);
    for row in list {
        if acc_lots >= expected_lots { break; }
        let r_sym = row.get("symbol").and_then(serde_json::Value::as_str).unwrap_or("");
        if !r_sym.is_empty() && r_sym != expected_sym { continue; }
        let upd_ms = row.get("updatedTime").or_else(|| row.get("createdTime"))
            .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.trim().parse::<i64>().ok()))).unwrap_or(0);
        if min_ms > 0 && upd_ms > 0 && upd_ms < min_ms { continue; }
        let q_lots = row.get("closedSize").or_else(|| row.get("qty")).and_then(serde_json::Value::as_str).map_or(LotQty(0), |qs| spec.parse_lot_qty(qs));
        let en_opt = row.get("avgEntryPrice").and_then(serde_json::Value::as_str).and_then(|s| parse_verified_avg_price_ticks(spec, ctx.side, s).ok());
        let ex_opt = row.get("avgExitPrice").and_then(serde_json::Value::as_str).and_then(|s| spec.parse_price_ticks(s).ok());
        let pnl_opt = row.get("closedPnl").and_then(|v| v.as_str().and_then(parse_pnl_decimal_1e8).or_else(|| v.as_f64().map(|f| (f * 100_000_000.0).round() as i64)));
        if q_lots.0 <= 0 && pnl_opt.is_none() && ex_opt.is_none() { continue; }
        legs_count = legs_count.saturating_add(1);
        if let Some(p1e8) = pnl_opt { sum_pnl = sum_pnl.saturating_add(p1e8); has_pnl = true; }
        let ord_t = row.get("orderType").and_then(serde_json::Value::as_str).unwrap_or("");
        let exec_t = row.get("execType").and_then(serde_json::Value::as_str).unwrap_or("");
        let row_is_mkt = ord_t.eq_ignore_ascii_case("Market")
            || exec_t.eq_ignore_ascii_case("BustTrade")
            || exec_t.eq_ignore_ascii_case("AdlTrade");
        let row_is_limit = ord_t.eq_ignore_ascii_case("Limit");
        if row_is_mkt {
            saw_mkt_sl = true;
        }
        if row_is_limit {
            saw_limit_exit = true;
        }
        let rem_cap = (expected_lots - acc_lots).max(1);
        let eff_q = if q_lots.0 > 0 { q_lots.0.min(rem_cap) } else { 1 };
        if q_lots.0 > 0 { acc_lots = acc_lots.saturating_add(eff_q); }
        if row_is_mkt || row_is_limit {
            let leg_en = en_opt.unwrap_or_else(|| ctx.avg_price_ticks.max(PriceTicks(1)));
            let leg_ex = ex_opt.unwrap_or_else(|| {
                if row_is_mkt {
                    if ctx.last_eff_sl_ticks.0 > 0 { ctx.last_eff_sl_ticks } else { ctx.sl_trigger_price.max(leg_en) }
                } else if ctx.resting_exit_price.0 > 0 {
                    ctx.resting_exit_price
                } else {
                    defensive_zero_loss_maker_exit_price_with_fees(ctx.side, leg_en, ctx.active_fees)
                }
            });
            let (_, leg_fee_1e8, _, _) = spec.compute_leg_pnl_and_fee_1e8_with_schedule(
                ctx.side, LotQty(eff_q), leg_en, leg_ex, !row_is_mkt, ctx.active_fees,
            );
            exact_fee_sum_1e8 = exact_fee_sum_1e8.saturating_add(leg_fee_1e8);
            typed_fee_lots = typed_fee_lots.saturating_add(eff_q);
            if row_is_mkt {
                taker_lots_acc = taker_lots_acc.saturating_add(eff_q);
            } else {
                maker_lots_acc = maker_lots_acc.saturating_add(eff_q);
            }
        }
        if let (Some(en_px), Some(ex_px)) = (en_opt, ex_opt) {
            w_en += (eff_q as i128) * (en_px.0 as i128);
            w_ex += (eff_q as i128) * (ex_px.0 as i128);
            w_lots += eff_q as i128;
        }
    }
    if legs_count == 0 { return ClosedPnlQueryOutcome::PendingIndexerRetry; }
    if !allow_partial_lot_coverage && acc_lots > 0 && acc_lots * 100 < expected_lots * 95 {
        return ClosedPnlQueryOutcome::PendingIndexerRetry;
    }
    let final_lots = LotQty(if acc_lots > 0 { acc_lots } else { expected_lots });
    let verified_entry_price = if w_lots > 0 { PriceTicks(((w_en + w_lots / 2) / w_lots).max(1) as i64) } else { ctx.avg_price_ticks.max(PriceTicks(1)) };
    let verified_exit_price = if w_lots > 0 {
        PriceTicks(((w_ex + w_lots / 2) / w_lots).max(1) as i64)
    } else if ctx.resting_exit_price.0 > 0 {
        ctx.resting_exit_price
    } else if ctx.active_exit_price.0 > 0 {
        ctx.active_exit_price
    } else {
        defensive_zero_loss_maker_exit_price_with_fees(ctx.side, verified_entry_price, ctx.active_fees)
    };
    let rt_mkr = ctx.active_fees.round_trip_maker_exit_bps().raw();
    let rt_tkr = ctx.active_fees.round_trip_taker_exit_bps().raw();
    let eff_rt_bps = if typed_fee_lots > 0 {
        (((maker_lots_acc as i128) * (rt_mkr as i128) + (taker_lots_acc as i128) * (rt_tkr as i128))
            / (typed_fee_lots as i128)) as i64
    } else if saw_mkt_sl {
        rt_tkr
    } else {
        rt_mkr
    };
    let notional_1e8 = spec.lot_notional_1e8(final_lots, verified_entry_price).max(1);
    let est_net_bps = if has_pnl {
        ((sum_pnl as i128 * 1_000_000) / (notional_1e8 as i128)) as i64
    } else {
        ((verified_exit_price.0 - verified_entry_price.0).saturating_mul(ctx.side.sign_i64()).saturating_mul(1_000_000))
            / verified_entry_price.0.max(1) - eff_rt_bps
    };
    let kind = if saw_mkt_sl {
        ClosedTradeKind::ServerMarkPriceStopLoss
    } else if saw_limit_exit {
        ClosedTradeKind::MakerPassiveUnwind
    } else {
        let sl_ref = PairSpec::elastic_server_sl_distance_bps_with_fees(
            ScaledBps(ctx.learned_ret_std_bps.max(1_200)),
            ctx.active_fees,
        )
        .raw() as f64;
        let endo_tkr_cut = -((PHI * (rt_tkr.max(50) as f64))
            .max(INV_PHI_SQ * sl_ref)
            .round() as i64);
        if est_net_bps <= endo_tkr_cut {
            ClosedTradeKind::ServerMarkPriceStopLoss
        } else {
            ClosedTradeKind::MakerPassiveUnwind
        }
    };
    let sub_reason = match kind {
        ClosedTradeKind::ServerMarkPriceStopLoss => {
            if chandelier_profit_sl_armed {
                ExitSubReason::ChandelierProfitLockSl
            } else {
                ExitSubReason::WaldBaseStopLoss
            }
        }
        ClosedTradeKind::MakerPassiveUnwind => classify_maker_exit_sub_reason_with_fees(
            est_net_bps.saturating_add(eff_rt_bps), vac_level_hint, ctx.tranche1_harvested, false, false, ctx.active_fees,
        ),
    };
    let exact_fee_1e8 = if typed_fee_lots >= final_lots.0 && final_lots.0 > 0 {
        Some(exact_fee_sum_1e8)
    } else {
        None
    };
    ClosedPnlQueryOutcome::Verified(VerifiedCloseResolution {
        kind, sub_reason, verified_entry_price, verified_exit_price,
        closed_lots: final_lots, legs_count, exchange_closed_pnl_1e8: if has_pnl { Some(sum_pnl) } else { None },
        exact_fee_1e8, maker_closed_lots: LotQty(maker_lots_acc), taker_closed_lots: LotQty(taker_lots_acc),
    })
}

#[inline]
#[must_use]
pub fn resolve_fallback_unverified_close_core(
    ctx: &OpenExitContext,
    snap: &SymbolLobSnapshot,
    vac_level_hint: usize,
) -> VerifiedCloseResolution {
    resolve_fallback_unverified_close_core_v553(ctx, snap, vac_level_hint, false)
}

#[must_use]
pub fn resolve_fallback_unverified_close_core_v553(
    ctx: &OpenExitContext,
    snap: &SymbolLobSnapshot,
    vac_level_hint: usize,
    chandelier_profit_sl_armed: bool,
) -> VerifiedCloseResolution {
    let en_px = ctx.avg_price_ticks.max(PriceTicks(1));
    let eff_sl = if ctx.last_eff_sl_ticks.0 > 0 { ctx.last_eff_sl_ticks } else { ctx.sl_trigger_price };
    let ref_mk = if snap.mark_price.0 > 0 { snap.mark_price } else { en_px };
    let sl_crossed = eff_sl.0 > 0 && match ctx.side { Side::Bid => ref_mk.0 <= eff_sl.0, Side::Ask => ref_mk.0 >= eff_sl.0 };
    let tot_lots = LotQty(if ctx.tranche1_harvested || ctx.tranche1_lots.0 > 0 {
        ctx.initial_lots.0.max(ctx.open_lots.0).max(1)
    } else {
        ctx.open_lots.0.max(1)
    });
    if sl_crossed {
        let sl_reason = if chandelier_profit_sl_armed {
            ExitSubReason::ChandelierProfitLockSl
        } else {
            ExitSubReason::WaldBaseStopLoss
        };
        let mkr_l = if ctx.tranche1_harvested || ctx.tranche1_lots.0 > 0 {
            (tot_lots.0 - ctx.open_lots.0.max(1)).max(0)
        } else {
            0
        };
        return VerifiedCloseResolution {
            kind: ClosedTradeKind::ServerMarkPriceStopLoss, sub_reason: sl_reason,
            verified_entry_price: en_px, verified_exit_price: eff_sl, closed_lots: tot_lots,
            legs_count: if mkr_l > 0 { 2 } else { 1 }, exchange_closed_pnl_1e8: None,
            exact_fee_1e8: None, maker_closed_lots: LotQty(mkr_l), taker_closed_lots: LotQty((tot_lots.0 - mkr_l).max(1)),
        };
    }
    let fb_exit = if ctx.resting_exit_price.0 > 0 {
        ctx.resting_exit_price
    } else if ctx.active_exit_price.0 > 0 {
        ctx.active_exit_price
    } else {
        defensive_zero_loss_maker_exit_price_with_fees(ctx.side, en_px, ctx.active_fees)
    };
    let gross_bps = ((fb_exit.0 - en_px.0).saturating_mul(ctx.side.sign_i64()).saturating_mul(1_000_000)) / en_px.0.max(1);
    let w_obs = snap.wave_observables();
    let aligned_fib = w_obs.fib_proj_bps.raw().saturating_mul(ctx.side.sign_i64()) > 0;
    VerifiedCloseResolution {
        kind: ClosedTradeKind::MakerPassiveUnwind,
        sub_reason: classify_maker_exit_sub_reason_with_fees(gross_bps, vac_level_hint, ctx.tranche1_harvested, w_obs.climax_active, aligned_fib, ctx.active_fees),
        verified_entry_price: en_px, verified_exit_price: fb_exit, closed_lots: tot_lots,
        legs_count: if ctx.tranche1_harvested { 2 } else { 1 }, exchange_closed_pnl_1e8: None,
        exact_fee_1e8: None, maker_closed_lots: tot_lots, taker_closed_lots: LotQty(0),
    }
}

#[inline]
const fn enforce_maker_bbo_bound(pos_side: Side, candidate_exit_px: PriceTicks, snap: &SymbolLobSnapshot) -> PriceTicks {
    match pos_side {
        Side::Bid => {
            let b0 = snap.bids[0].price.0;
            if b0 > 0 && candidate_exit_px.0 <= b0 { PriceTicks(b0.saturating_add(1)) } else { candidate_exit_px }
        }
        Side::Ask => {
            let a0 = snap.asks[0].price.0;
            if a0 > 1 && candidate_exit_px.0 >= a0 { PriceTicks(a0 - 1) } else { PriceTicks(if candidate_exit_px.0 > 1 { candidate_exit_px.0 } else { 1 }) }
        }
    }
}

#[inline]
const fn pick_more_ambitious_tp(pos_side: Side, a: PriceTicks, b: PriceTicks) -> PriceTicks {
    if b.0 <= 0 { return a; }
    if a.0 <= 0 { return b; }
    match pos_side {
        Side::Bid => if b.0 > a.0 { b } else { a },
        Side::Ask => if b.0 < a.0 { b } else { a },
    }
}

#[inline]
#[must_use]
fn hermite_s3_unit(x: f64, lo: f64, hi: f64) -> f64 {
    if hi <= lo { return if x >= hi { 1.0 } else { 0.0 }; }
    let u = ((x - lo) / (hi - lo)).clamp(0.0, 1.0);
    u * u * (3.0 - 2.0 * u)
}

#[inline]
#[must_use]
pub fn compute_reversible_defensive_state_c1(
    cur_gross_bps: ScaledBps,
    win_floor_bps: ScaledBps,
    signed_flow_slow: BoundedScore,
    taker_prob_pm: i64,
    ir_x100: i64,
    wave: WaveObservables,
    side: Side,
    prev_locked: bool,
) -> (bool, bool) {
    let aligned_phase = wave.macro_phase.get() * side.sign_f64();
    let opposing_wave3_escape = matches!(wave.regime, WaveRegime::ImpulseWave3) && aligned_phase <= -INV_PHI;
    if opposing_wave3_escape { return (true, true); }
    if !prev_locked { return (false, false); }
    let p_taker = (taker_prob_pm.clamp(5, 950) as f64) * 1e-3;
    let ir_pos = (ir_x100.clamp(0, 100) as f64) * 1e-2;
    let phi_unlock = (INV_PHI * (1.0 + INV_PHI_SQ * p_taker) / (1.0 + INV_PHI_CUBE * ir_pos)).clamp(INV_PHI_SQ, PHI);
    let recov_s3 = hermite_s3_unit(cur_gross_bps.raw() as f64, 0.0, (win_floor_bps.raw().max(100)) as f64);
    let eff_unlock = phi_unlock * (1.0 - INV_PHI_CUBE * recov_s3);
    (signed_flow_slow.get() < eff_unlock, false)
}

#[must_use]
pub fn compute_canonical_exit_quote_omega(
    ctx: &OpenExitContext,
    kin: ShadowRunnerKinematics,
    snap: &SymbolLobSnapshot,
) -> Option<CanonicalExitQuote> {
    let (ws_rtt_us, _) = crate::execution_fsm::read_live_ws_pulse();
    compute_canonical_exit_quote_omega_v52(ctx, kin, snap, None, 0, ws_rtt_us)
}

#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn compute_canonical_exit_quote_omega_v52(
    ctx: &OpenExitContext,
    kin: ShadowRunnerKinematics,
    snap: &SymbolLobSnapshot,
    wald_log_likelihood: Option<f64>,
    funding_adv_sbps: i64,
    ws_rtt_us: u32,
) -> Option<CanonicalExitQuote> {
    compute_canonical_exit_quote_omega_v54(
        ctx,
        kin,
        snap,
        snap.realized_vol_5m,
        wald_log_likelihood,
        funding_adv_sbps,
        ws_rtt_us,
    )
}

#[derive(Copy, Clone, Debug)]
pub struct CascadeAndAgingContextV55 {
    pub tranche1_harvests_count: u64,
    pub tranche2_harvested: bool,
    pub harvested_legs: u32,
    pub psi_health: f64,
    pub cont_ratio_x1000: i64,
    pub last_peak_favorable_ts_ns: i64,
    pub zeta_tox: f64,
    pub house_money_runner_unlocked: bool,
}

impl CascadeAndAgingContextV55 {
    #[inline]
    #[must_use]
    pub const fn default_from_ctx(ctx: &OpenExitContext) -> Self {
        Self {
            tranche1_harvests_count: ctx.tranche1_harvested as u64,
            tranche2_harvested: false,
            harvested_legs: ctx.tranche1_harvested as u32,
            psi_health: 1.0,
            cont_ratio_x1000: 1_000,
            last_peak_favorable_ts_ns: ctx.opened_ts.0,
            zeta_tox: 0.0,
            house_money_runner_unlocked: false,
        }
    }
}

/// Rebound continuation probability `pi_reb` under golden `C1` Hermite modulation (V5.5.4):
/// `S_3(INV_PHI*ofi_al + INV_PHI_SQ*tfi_al + INV_PHI_CUBE*geom_al; INV_PHI_CUBE, INV_PHI) * (1 - S_3(zeta_tox; INV_PHI, PHI))`.
#[inline]
#[must_use]
pub fn compute_rebound_probability_pi_c1(
    side: Side,
    snap: &SymbolLobSnapshot,
    zeta_tox: f64,
) -> f64 {
    let dir_s = side.sign_f64();
    let ofi_al = snap.factor_ofi.get() * dir_s;
    let tfi_al = snap.factor_tfi.get() * dir_s;
    let geom_al = snap.factor_geom.get() * dir_s;
    let composite_al = INV_PHI * ofi_al + INV_PHI_SQ * tfi_al + INV_PHI_CUBE * geom_al;
    let z_clean = if zeta_tox.is_finite() { zeta_tox.max(0.0) } else { 0.0 };
    crate::wave_elliott::hermite_smoothstep_c1(composite_al, INV_PHI_CUBE, INV_PHI)
        * (1.0 - crate::wave_elliott::hermite_smoothstep_c1(z_clean, INV_PHI, PHI))
}

/// Intrinsic diffusive signal lifetime `\tau_{horizon} = \varphi^3 \cdot \tau_{mkt,i} \cdot (1 + \varphi^{-1} S_3(harvested\_legs; 0, 1))` (`~6.5..20 min`, V5.5 O7).
#[inline]
#[must_use]
pub fn compute_anti_hostage_horizon_s(intrinsic_tau_mkt_s: i64, harvested_legs: u32) -> f64 {
    const PHI_CUBE_AH: f64 = 4.236_067_977_499_79;
    let tau_mkt = (intrinsic_tau_mkt_s.max(90)) as f64;
    let s_harv = hermite_s3_unit(f64::from(harvested_legs), 0.0, 1.0);
    PHI_CUBE_AH * tau_mkt * (1.0 + INV_PHI * s_harv)
}

/// Continuous `C1` diffusive anti-hostage aging ramp `chi_aging = S_3(hold_s / tau_horizon; INV_PHI, PHI)`
/// strictly `0.0` during initial minutes (`hold_s <= INV_PHI * tau_horizon`) and while new `peak_favorable_price`
/// highs are being marked (V5.5 O7).
#[inline]
#[must_use]
pub fn compute_anti_hostage_aging_c1(
    hold_s: f64,
    stagnant_since_peak_s: f64,
    tau_horizon_s: f64,
) -> f64 {
    let tau_h = tau_horizon_s.max(90.0);
    if hold_s <= INV_PHI * tau_h {
        return 0.0;
    }
    let s_hold = hermite_s3_unit(hold_s / tau_h, INV_PHI, PHI);
    let s_stag = hermite_s3_unit(stagnant_since_peak_s / tau_h, INV_PHI_SQ, INV_PHI);
    (s_hold * s_stag).clamp(0.0, 1.0)
}

#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn compute_canonical_exit_quote_omega_v54(
    ctx: &OpenExitContext,
    kin: ShadowRunnerKinematics,
    snap: &SymbolLobSnapshot,
    open_vol_5m_bps: ScaledBps,
    wald_log_likelihood: Option<f64>,
    funding_adv_sbps: i64,
    ws_rtt_us: u32,
) -> Option<CanonicalExitQuote> {
    compute_canonical_exit_quote_omega_v55(
        ctx,
        kin,
        CascadeAndAgingContextV55::default_from_ctx(ctx),
        snap,
        open_vol_5m_bps,
        wald_log_likelihood,
        funding_adv_sbps,
        ws_rtt_us,
    )
}

#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn compute_canonical_exit_quote_omega_v55(
    ctx: &OpenExitContext,
    kin: ShadowRunnerKinematics,
    v55: CascadeAndAgingContextV55,
    snap: &SymbolLobSnapshot,
    open_vol_5m_bps: ScaledBps,
    wald_log_likelihood: Option<f64>,
    funding_adv_sbps: i64,
    ws_rtt_us: u32,
) -> Option<CanonicalExitQuote> {
    const INV_PHI_FOURTH: f64 = 0.145_898_033_750_315;
    const SQRT_5: f64 = PHI + INV_PHI;
    if ctx.avg_price_ticks.0 <= 0 || ctx.open_lots.0 <= 0 { return None; }
    let rt_raw = ctx.active_fees.round_trip_maker_exit_bps().raw();
    let fee_rt = rt_raw.max(100);
    let win_floor_bps = compute_elastic_offensive_win_floor_bps(fee_rt, snap.realized_vol_5m).raw();
    let eq_vol_mfe = ScaledBps(ScaledBps::MIN_REQUIRED_VOL_5M_BPS.raw().saturating_mul(2));
    let projected_mfe_bps = if snap.realized_vol_5m.raw() > 0 {
        crate::types::modulate_learned_mfe_by_diffusive_regime_sbps(
            ctx.learned_mfe_bps,
            snap.realized_vol_5m,
            eq_vol_mfe,
        )
    } else {
        ctx.learned_mfe_bps
    };
    let dir_s = ctx.side.sign_f64();
    let d_sign = ctx.side.sign_i64();
    let avg_denom = ctx.avg_price_ticks.0.max(1);
    let zero_loss_px = defensive_zero_loss_maker_exit_price_with_fees(ctx.side, ctx.avg_price_ticks, ctx.active_fees);
    let zero_loss_bps = (((zero_loss_px.0 - ctx.avg_price_ticks.0)
        .saturating_mul(d_sign)
        .saturating_mul(1_000_000))
        / avg_denom)
        .max(rt_raw.max(0) + 1);
    let own_flow = (0.55 * snap.factor_ofi.get() + 0.45 * snap.factor_tfi.get()) * dir_s;
    let signed_flow_slow = BoundedScore::new_clamped(kin.flow_ema_slow * dir_s);
    let has_live_kin = kin.flow_ema_slow.abs() > 1e-9 || kin.flow_ema_fast.abs() > 1e-9;
    let eff_flow_slow = if has_live_kin { signed_flow_slow.get() } else { own_flow };
    let (s_prop, flow_accel) = evaluate_shadow_runner_propulsion(ctx.side, snap, kin.flow_ema_fast, kin.flow_ema_slow);
    let bbo_exit_px = match ctx.side { Side::Bid => snap.asks[0].price, Side::Ask => snap.bids[0].price };
    let ref_px = if bbo_exit_px.0 > 0 { bbo_exit_px } else if snap.mark_price.0 > 0 { snap.mark_price } else { snap.last_trade_price };
    let cur_gross_bps = if ref_px.0 > 0 { ((ref_px.0 - ctx.avg_price_ticks.0).saturating_mul(d_sign).saturating_mul(1_000_000)) / avg_denom } else { 0 };
    let state_mfe = ((ctx.peak_favorable_price.0 - ctx.avg_price_ticks.0).saturating_mul(d_sign).saturating_mul(1_000_000)) / avg_denom;
    let best_mfe_bps = ctx.last_closed_mfe_bps.max(state_mfe).max(cur_gross_bps);
    let w_obs = snap.wave_observables();
    let wave_def_mult = CausalElliottWaveState::evaluate_wave_entry_modulation(w_obs, ctx.side, eff_flow_slow).map_or(1_000, |m| m.def_horizon_permille_mult);
    let aligned_fib_rem_bps = w_obs.fib_proj_bps.raw().saturating_mul(d_sign).max(0);
    let aligned_phase = w_obs.macro_phase.get() * dir_s;
    let hold_ns = snap.ts_local_ns.0.saturating_sub(ctx.opened_ts.0).max(0);
    let hold_s_f = (hold_ns as f64) * 1e-9;
    let hold_s = hold_ns / 1_000_000_000;
    let tau_s = ctx.intrinsic_tau_mkt_s.max(90);
    let tau_s_f = tau_s as f64;
    let def_horizon_s = (INV_PHI_CUBE * tau_s_f * ((wave_def_mult as f64) * 1e-3))
        .clamp(INV_PHI_FOURTH * tau_s_f, PHI * tau_s_f);
    let rtt_s = (f64::from(ws_rtt_us) * 1e-6).clamp(0.0, PHI);
    let c_mkr = (rt_raw.max(0)) as f64;
    let c_mkr_scale = (fee_rt.max(100)) as f64;
    let c_tkr = (ctx.active_fees.round_trip_taker_exit_bps().raw().max(50)) as f64;
    let f_adv = (funding_adv_sbps.max(0)) as f64;
    let climax_harvest = (w_obs.climax_active || w_obs.regime == WaveRegime::ClimaxExhaustion)
        && aligned_phase > 0.0 && (cur_gross_bps >= zero_loss_bps || best_mfe_bps >= win_floor_bps);
    let (still_locked, mut opposing_wave3_escape) = compute_reversible_defensive_state_c1(
        ScaledBps(cur_gross_bps), ScaledBps(win_floor_bps), signed_flow_slow,
        ctx.learned_taker_prob_pm, ctx.learned_ir_x100, w_obs, ctx.side, ctx.defensive_floor_locked,
    );
    if crate::alpha_3factor::is_opposing_impulse_wave3_hazard(ctx.side, snap) {
        opposing_wave3_escape = true;
    }
    let flow_deficit = (INV_PHI_FOURTH - eff_flow_slow).max(0.0);
    let mfe_drought = hermite_s3_unit(c_mkr_scale - (best_mfe_bps.max(0) as f64), 0.0, c_mkr_scale);
    let wave3_shock = if opposing_wave3_escape { PHI * (-aligned_phase).max(INV_PHI) } else { 0.0 };
    let tau_wave3_s = (INV_PHI_FOURTH * def_horizon_s + PHI_SQ * rtt_s).max(1.0);
    let synth_lambda = -(PHI * flow_deficit * (hold_s_f / def_horizon_s.max(1.0)) * (1.0 + INV_PHI * mfe_drought)
        + wave3_shock * (hold_s_f / tau_wave3_s));
    let lambda_t = match wald_log_likelihood {
        Some(ll) if ll.is_finite() => ll.min(synth_lambda),
        _ => synth_lambda,
    };
    let p_cont = (1.0 / (1.0 + (-lambda_t).exp())).clamp(0.0, 1.0);
    let wald_pct = ((100.0 * INV_PHI * hermite_s3_unit(INV_PHI - p_cont, 0.0, INV_PHI_SQ)).round() as i64).clamp(0, 62);
    let sl_bps = PairSpec::elastic_server_sl_distance_bps_with_fees(snap.realized_vol_5m, ctx.active_fees).raw();
    let gamma_pm = (220 + ctx.learned_ir_x100.clamp(-40, 80)).clamp(180, 300);
    let mae_trig_bps = -((sl_bps * gamma_pm) / 1_000)
        .max(ctx.learned_ret_std_bps / 6)
        .max((PHI * c_mkr_scale).round() as i64);
    let ir_norm = (ctx.learned_ir_x100.clamp(0, 100) as f64) * 1e-2;
    let stag_flow_ceil = INV_PHI_CUBE * (1.0 - INV_PHI_SQ * ir_norm);
    let live_deep_underwater = cur_gross_bps <= mae_trig_bps && eff_flow_slow < 0.0;
    let time_stagnation = !v55.house_money_runner_unlocked
        && hold_s_f >= def_horizon_s
        && cur_gross_bps < win_floor_bps
        && eff_flow_slow < stag_flow_ceil;
    let mut defensive_lock_active = still_locked || opposing_wave3_escape || live_deep_underwater || time_stagnation || climax_harvest;
    let eff_ratchet_bps = if best_mfe_bps >= (fee_rt * 139) / 100 {
        ctx.ratchet_locked_gross_bps.max(fee_rt + ((best_mfe_bps - fee_rt) * 72) / 100).max(win_floor_bps)
    } else { ctx.ratchet_locked_gross_bps };
    let ratchet_pullback = eff_ratchet_bps >= win_floor_bps
        && ((own_flow <= -0.5 * INV_PHI_CUBE) || (cur_gross_bps <= eff_ratchet_bps + 40 && own_flow < INV_PHI_CUBE));
    let shadow_crest_snap = best_mfe_bps >= fee_rt * 2 && cur_gross_bps >= zero_loss_bps && s_prop < 0.18 && flow_accel < -0.005;
    let q_floor_px = enforce_maker_bbo_bound(ctx.side, zero_loss_px, snap);
    let eff_t1_pm_def = compute_wave_dynamic_t1_permille(ctx.learned_t1_permille, ctx.side, snap, kin.in_scar_debt);
    let def_runner_split_t1 = if !ctx.tranche1_harvested
        && ctx.defensive_floor_locked
        && eff_flow_slow >= -INV_PHI_FIFTH
    {
        let opt_t1 = compute_tranche1_split_lots_core(ctx.slot_idx, ctx.open_lots, q_floor_px, eff_t1_pm_def);
        if opt_t1.is_some() {
            defensive_lock_active = true;
        }
        opt_t1
    } else {
        None
    };
    if def_runner_split_t1.is_none()
        && !ctx.tranche1_harvested
        && !ratchet_pullback
        && !shadow_crest_snap
        && !opposing_wave3_escape
        && !climax_harvest
        && (!defensive_lock_active || (!still_locked && signed_flow_slow.get() >= INV_PHI_FIFTH))
    {
        let sigma_1m_cur = (snap.realized_vol_5m.raw().max(0) as f64) / SQRT_5;
        let raw_t1_px = if defensive_lock_active {
            zero_loss_px
        } else {
            let urgency = (INV_PHI - INV_PHI_FIFTH * own_flow.clamp(0.0, 1.0) * (1.0 + ir_norm)).clamp(0.50, INV_PHI);
            let base_t1_px = elastic_offensive_win_maker_exit_price(ctx.side, ctx.avg_price_ticks, fee_rt, snap.realized_vol_5m, urgency);
            let t1_drive = flow_accel.max(INV_PHI * own_flow);
            if s_prop > 0.0 && sigma_1m_cur > c_mkr_scale && t1_drive > INV_PHI_FOURTH {
                let s_t1_accel = hermite_s3_unit(t1_drive, INV_PHI_FOURTH, INV_PHI);
                let s_t1_prop = hermite_s3_unit(s_prop, INV_PHI_CUBE, INV_PHI);
                let s_t1_vol = hermite_s3_unit(sigma_1m_cur, c_mkr_scale, PHI_SQ * c_mkr_scale);
                let s_t1_mfe = hermite_s3_unit(projected_mfe_bps as f64, PHI * c_mkr_scale, PHI_SQ * c_mkr_scale);
                let t1_base_f = win_floor_bps as f64;
                let t1_ceil = (INV_PHI * sigma_1m_cur)
                    .min(INV_PHI * (projected_mfe_bps as f64) + INV_PHI_CUBE * sigma_1m_cur)
                    .max(t1_base_f);
                let t1_exp_bps = (t1_base_f
                    + s_t1_accel * s_t1_prop * s_t1_vol * s_t1_mfe * (t1_ceil - t1_base_f))
                    .round() as i64;
                let exp_px = dynamic_maker_floor_price(ctx.side, ctx.avg_price_ticks, ScaledBps(t1_exp_bps));
                pick_more_ambitious_tp(ctx.side, base_t1_px, exp_px)
            } else {
                base_t1_px
            }
        };
        let win1_px = enforce_maker_bbo_bound(ctx.side, raw_t1_px, snap);
        let eff_t1_pm = compute_wave_dynamic_t1_permille(ctx.learned_t1_permille, ctx.side, snap, kin.in_scar_debt);
        if let Some(t1_lots) = compute_tranche1_split_lots_core(ctx.slot_idx, ctx.open_lots, win1_px, eff_t1_pm) {
            return Some(CanonicalExitQuote {
                role: ExitOrderRole::Tranche1Harvest50, target_price: win1_px, order_lots: t1_lots,
                sub_reason: ExitSubReason::Tranche1Harvest50, vac_level: 1,
                defensive_lock_active, urgent_close_repeg: defensive_lock_active, cur_gross_bps, wald_pct,
            });
        }
    }
    let role = if ctx.tranche1_harvested { ExitOrderRole::LvdbsDeepSurf50 } else { ExitOrderRole::StandardMakerExit100 };
    let tau_horizon_s = compute_anti_hostage_horizon_s(tau_s, v55.harvested_legs);
    let peak_ts = if v55.last_peak_favorable_ts_ns > 0 {
        v55.last_peak_favorable_ts_ns
    } else {
        ctx.opened_ts.0
    };
    let stagnant_s = ((snap.ts_local_ns.0.saturating_sub(peak_ts).max(0)) as f64) * 1e-9;
    let chi_aging = compute_anti_hostage_aging_c1(hold_s_f, stagnant_s, tau_horizon_s);
    let aging_min_gross_sbps =
        zero_loss_bps - (((zero_loss_bps + fee_rt) as f64 * chi_aging).round() as i64);
    let z_tox_clean = if v55.zeta_tox.is_finite() { v55.zeta_tox.max(0.0) } else { 0.0 };
    let s_live_recov = hermite_s3_unit(own_flow, INV_PHI_CUBE, INV_PHI)
        * hermite_s3_unit(eff_flow_slow, -INV_PHI_FIFTH, INV_PHI_CUBE)
        * (1.0 - hermite_s3_unit(z_tox_clean, INV_PHI, PHI));
    let aging_opportunistic_maker = chi_aging > 0.0
        && !climax_harvest
        && !shadow_crest_snap
        && !ratchet_pullback
        && eff_ratchet_bps < win_floor_bps
        && ref_px.0 > 0
        && cur_gross_bps >= aging_min_gross_sbps
        && (cur_gross_bps >= zero_loss_bps || s_live_recov < INV_PHI_SQ)
        && (cur_gross_bps <= zero_loss_bps
            || (chi_aging >= INV_PHI_SQ && cur_gross_bps < win_floor_bps));
    if defensive_lock_active || ratchet_pullback || shadow_crest_snap || aging_opportunistic_maker {
        let p_taker = (ctx.learned_taker_prob_pm.clamp(5, 950) as f64) * 1e-3;
        let s_adv_flow = hermite_s3_unit(-eff_flow_slow, INV_PHI_FOURTH, INV_PHI);
        let p_eff_cont = (p_cont * (1.0 - p_taker) * (1.0 - INV_PHI * s_adv_flow)).clamp(0.0, 1.0);
        let g_recov = (projected_mfe_bps.max(win_floor_bps) as f64) - c_mkr - f_adv;
        let live_sl_sbps = if ctx.last_eff_sl_ticks.0 > 0 {
            (((ctx.avg_price_ticks.0 - ctx.last_eff_sl_ticks.0)
                .saturating_mul(d_sign)
                .max(0)
                .saturating_mul(1_000_000))
                / avg_denom) as f64
        } else {
            (INV_PHI * (sl_bps as f64)).max(PHI_SQ * c_mkr_scale)
        };
        let cur_g = cur_gross_bps as f64;
        let sl_loss_gross = live_sl_sbps.max((-cur_g).max(0.0));
        let v_sl = -(sl_loss_gross + c_tkr + f_adv);
        let e_hold = p_eff_cont * g_recov + (1.0 - p_eff_cont) * v_sl;
        let v_bailout_maker = cur_g - c_mkr - f_adv;
        let s_wave3_stress = if opposing_wave3_escape {
            hermite_s3_unit(hold_s_f, INV_PHI * tau_wave3_s, tau_wave3_s)
                * hermite_s3_unit(-eff_flow_slow, INV_PHI_FIFTH, INV_PHI_CUBE)
        } else {
            0.0
        };
        let s_stag_stress = hermite_s3_unit(hold_s_f, INV_PHI * def_horizon_s, def_horizon_s)
            * hermite_s3_unit(-eff_flow_slow, INV_PHI_CUBE, INV_PHI_SQ)
            * hermite_s3_unit(INV_PHI_SQ - p_cont, 0.0, INV_PHI_FOURTH)
            * hermite_s3_unit(
                INV_PHI * c_mkr_scale - (best_mfe_bps.max(0) as f64),
                0.0,
                INV_PHI_SQ * c_mkr_scale,
            );
        let s_wald_deep_stress = if live_deep_underwater {
            hermite_s3_unit(hold_s_f, INV_PHI * tau_wave3_s, tau_wave3_s)
                * hermite_s3_unit(-eff_flow_slow, INV_PHI_SQ, INV_PHI)
                * hermite_s3_unit(INV_PHI_CUBE - p_cont, 0.0, INV_PHI_FOURTH)
        } else {
            0.0
        };
        let state_mae = ((ctx.worst_adverse_price.0 - ctx.avg_price_ticks.0)
            .saturating_mul(d_sign)
            .saturating_mul(1_000_000))
            / avg_denom;
        let worst_mae_bps = ctx.last_closed_mae_bps.min(state_mae).min(cur_gross_bps);
        let s_bounce_bailout = if worst_mae_bps <= mae_trig_bps && cur_gross_bps > worst_mae_bps {
            hermite_s3_unit(hold_s_f, INV_PHI * tau_wave3_s, def_horizon_s)
                * hermite_s3_unit(
                    (cur_gross_bps - worst_mae_bps) as f64,
                    INV_PHI_SQ * c_mkr_scale,
                    PHI * c_mkr_scale,
                )
                * hermite_s3_unit(INV_PHI_SQ - p_cont, 0.0, INV_PHI_FOURTH)
                * (1.0 - s_live_recov)
        } else {
            0.0
        };
        let eff_open_vol_sbps = if open_vol_5m_bps.raw() > 0 {
            open_vol_5m_bps.raw()
        } else {
            snap.realized_vol_5m.raw().max(0)
        };
        let sigma_1m_open_sbps = (eff_open_vol_sbps as f64) / SQRT_5;
        let deep_bailout_floor = -((live_sl_sbps - c_tkr).max(PHI * c_mkr_scale));
        let b_floor_sbps = if worst_mae_bps <= mae_trig_bps && cur_gross_bps < mae_trig_bps && p_cont < INV_PHI_SQ && eff_flow_slow > -INV_PHI_CUBE {
            deep_bailout_floor
        } else {
            -((PHI * c_mkr_scale).max(INV_PHI_SQ * sigma_1m_open_sbps))
        };
        let base_bailout_band = hermite_s3_unit(cur_g, PHI * b_floor_sbps, b_floor_sbps);
        let aligned_fast = if has_live_kin {
            BoundedScore::new_clamped(kin.flow_ema_fast * dir_s).get()
        } else {
            own_flow
        };
        let opp_w3_confirmed = crate::alpha_3factor::is_opposing_impulse_wave3_hazard_dynamic(
            ctx.side,
            snap,
            eff_open_vol_sbps,
            ctx.learned_taker_prob_pm,
            ctx.active_fees,
        );
        let s_w3_pause_evac = if opp_w3_confirmed
            && aligned_fast > eff_flow_slow
            && aligned_fast >= -INV_PHI_CUBE
        {
            let s_hold_w3 = hermite_s3_unit(hold_s_f, INV_PHI_SQ * tau_wave3_s, tau_wave3_s);
            let s_decel = hermite_s3_unit(aligned_fast - eff_flow_slow, 0.0, INV_PHI_FIFTH);
            let s_breather = hermite_s3_unit(aligned_fast, -INV_PHI_CUBE, -INV_PHI_FOURTH);
            let s_sl_headroom = hermite_s3_unit(
                cur_g + live_sl_sbps,
                INV_PHI_SQ * c_mkr_scale,
                PHI * c_mkr_scale,
            );
            s_hold_w3 * s_decel * s_breather * s_sl_headroom
        } else {
            0.0
        };
        let chi_stress = (s_wave3_stress
            .max(s_stag_stress)
            .max(s_wald_deep_stress)
            .max(s_bounce_bailout)
            * base_bailout_band)
            .max(s_w3_pause_evac);
        let trapped_or_underwater = cur_gross_bps < 0 || worst_mae_bps < 0;
        let maker_bailout_active = aging_opportunistic_maker
            || (trapped_or_underwater
                && cur_gross_bps < zero_loss_bps
                && eff_ratchet_bps < win_floor_bps
                && ref_px.0 > 0
                && chi_stress >= INV_PHI
                && v_bailout_maker > e_hold + INV_PHI_CUBE * c_mkr_scale);
        let target_price = if aging_opportunistic_maker {
            let aging_px = if cur_gross_bps > zero_loss_bps {
                pick_more_ambitious_tp(ctx.side, zero_loss_px, ref_px)
            } else {
                ref_px
            };
            enforce_maker_bbo_bound(ctx.side, aging_px, snap)
        } else if maker_bailout_active {
            enforce_maker_bbo_bound(ctx.side, ref_px, snap)
        } else if let Some(t1_lots) = def_runner_split_t1 {
            let opp_w3 = opposing_wave3_escape || opp_w3_confirmed;
            let pi_reb = compute_rebound_probability_pi_c1(ctx.side, snap, v55.zeta_tox);
            let s_slow_reb = hermite_s3_unit(eff_flow_slow, -INV_PHI_FIFTH, INV_PHI_SQ);
            let pi_split = pi_reb.max(s_slow_reb) * (1.0 - hermite_s3_unit(z_tox_clean, INV_PHI_SQ, INV_PHI));
            if pi_split >= INV_PHI_CUBE && !opp_w3 && !climax_harvest {
                return Some(CanonicalExitQuote {
                    role: ExitOrderRole::Tranche1Harvest50,
                    target_price: q_floor_px,
                    order_lots: t1_lots,
                    sub_reason: ExitSubReason::Tranche1Harvest50,
                    vac_level: 1,
                    defensive_lock_active: true,
                    urgent_close_repeg: true,
                    cur_gross_bps,
                    wald_pct,
                });
            }
            q_floor_px
        } else {
            let healthy_stag_win_hold = !live_deep_underwater
                && !opposing_wave3_escape
                && !opp_w3_confirmed
                && worst_mae_bps > mae_trig_bps
                && z_tox_clean < INV_PHI
                && eff_flow_slow > -INV_PHI_FOURTH;
            let stag_win_floor_bps = if healthy_stag_win_hold {
                let u_decay = hermite_s3_unit(hold_s_f / def_horizon_s.max(1.0), PHI, PHI_SQ)
                    * (1.0 - INV_PHI * hermite_s3_unit(own_flow, 0.0, INV_PHI));
                let span = (win_floor_bps - zero_loss_bps).max(0) as f64;
                win_floor_bps - ((u_decay * span).round() as i64)
            } else {
                zero_loss_bps
            };
            let surf_lift_bps = if worst_mae_bps > mae_trig_bps
                && cur_gross_bps >= fee_rt / 2
                && z_tox_clean < INV_PHI_SQ
                && own_flow >= INV_PHI
            {
                let s_ow = hermite_s3_unit(own_flow, INV_PHI, 1.0 - INV_PHI_FIFTH);
                let s_cg = hermite_s3_unit(cur_gross_bps as f64, 0.5 * c_mkr_scale, c_mkr_scale);
                let mfe_room = (projected_mfe_bps.min((PHI_SQ * c_mkr_scale).round() as i64) - win_floor_bps).max(0) as f64;
                let top_bps = (win_floor_bps as f64) + INV_PHI_SQ * mfe_room;
                (s_ow * s_cg * (top_bps - (zero_loss_bps as f64)).max(0.0)).round() as i64
            } else {
                0
            };
            let base_else_floor = stag_win_floor_bps.max(zero_loss_bps.saturating_add(surf_lift_bps));
            let floor_bps = if climax_harvest || shadow_crest_snap || opposing_wave3_escape {
                cur_gross_bps.max(eff_ratchet_bps).max(zero_loss_bps)
            } else if ratchet_pullback || eff_ratchet_bps >= win_floor_bps {
                eff_ratchet_bps
            } else {
                base_else_floor
            };
            let raw_def_px = if floor_bps <= zero_loss_bps {
                zero_loss_px
            } else {
                dynamic_maker_floor_price(ctx.side, ctx.avg_price_ticks, ScaledBps(floor_bps))
            };
            enforce_maker_bbo_bound(ctx.side, raw_def_px, snap)
        };
        let is_win_floor_quote = !aging_opportunistic_maker
            && !maker_bailout_active
            && ((target_price.0 - ctx.avg_price_ticks.0).saturating_mul(d_sign).saturating_mul(1_000_000) / avg_denom) >= win_floor_bps;
        let sub_reason = if climax_harvest { ExitSubReason::ClimaxExhaustionHarvest }
            else if shadow_crest_snap { ExitSubReason::ShadowRunnerCrestHarvest }
            else if eff_ratchet_bps >= win_floor_bps { ExitSubReason::ConvexMfeRatchetLock }
            else if is_win_floor_quote { ExitSubReason::StandardStructuralTp }
            else { ExitSubReason::AdverseScratchEscape };
        return Some(CanonicalExitQuote {
            role, target_price, order_lots: ctx.open_lots, sub_reason, vac_level: 1,
            defensive_lock_active: defensive_lock_active || aging_opportunistic_maker,
            urgent_close_repeg: true, cur_gross_bps, wald_pct,
        });
    }
    let min_win_px = dynamic_maker_floor_price(ctx.side, ctx.avg_price_ticks, ScaledBps(eff_ratchet_bps.max(win_floor_bps)));
    let eff_struct_tp = if !v55.tranche2_harvested
        && snap.realized_vol_5m.raw() > 0
        && snap.realized_vol_5m.raw() < eq_vol_mfe.raw()
        && !matches!(w_obs.regime, WaveRegime::ImpulseWave3 | WaveRegime::ImpulseWave5)
    {
        let calm_cap_px = dynamic_maker_floor_price(
            ctx.side,
            ctx.avg_price_ticks,
            ScaledBps(projected_mfe_bps.max(win_floor_bps + fee_rt / 2)),
        );
        match ctx.side {
            Side::Bid => PriceTicks(ctx.structural_tp_price.0.min(calm_cap_px.0)),
            Side::Ask => PriceTicks(if ctx.structural_tp_price.0 > 0 {
                ctx.structural_tp_price.0.max(calm_cap_px.0)
            } else {
                calm_cap_px.0
            }),
        }
    } else {
        ctx.structural_tp_price
    };
    let base_tp = pick_more_ambitious_tp(ctx.side, min_win_px, eff_struct_tp);
    let (mut lvdbs_vac_level, mut lvdbs_px) = (1_usize, base_tp);
    if own_flow >= 0.35 {
        let (own_lvls, opp_lvls) = match ctx.side { Side::Bid => (&snap.bids, &snap.asks), Side::Ask => (&snap.asks, &snap.bids) };
        let own_l1_q = own_lvls[0].qty.0;
        let opp_l1_q = opp_lvls[0].qty.0.max(1);
        if own_l1_q > 0 && own_l1_q.saturating_mul(100) >= opp_l1_q.saturating_mul(115) {
            for lvl in 1..4 {
                let cand = opp_lvls[lvl];
                if cand.price.0 > 0 && cand.qty.0 > 0 && own_l1_q.saturating_mul(100) >= cand.qty.0.saturating_mul(120) {
                    lvdbs_vac_level = lvl + 1;
                    lvdbs_px = pick_more_ambitious_tp(ctx.side, lvdbs_px, cand.price);
                }
            }
        }
    }
    let vol_f = snap.realized_vol_5m.raw().max(1_600) as f64;
    let max_fib_cap = ((vol_f * PHI_SQ).round() as i64).clamp((fee_rt * 4).max(projected_mfe_bps.min(fee_rt * 12)), fee_rt * 45);
    let s_run = if ctx.tranche1_harvested {
        hermite_s3_unit(own_flow.max(s_prop), INV_PHI_FIFTH, INV_PHI_SQ)
    } else {
        let p_tkr_r = (ctx.learned_taker_prob_pm.clamp(5, 950) as f64) * 1e-3;
        let run_lo = (INV_PHI_SQ * (1.0 + PHI * p_tkr_r - INV_PHI_CUBE * ir_norm))
            .clamp(INV_PHI_CUBE, INV_PHI);
        hermite_s3_unit(own_flow.max(s_prop), run_lo, 1.0 - INV_PHI_FOURTH)
    };
    let runner2_px = if lvdbs_vac_level < 2 && s_run > 1e-6 {
        let sigma_1m = vol_f / SQRT_5;
        let base_r = (projected_mfe_bps as f64).min(PHI * sigma_1m).max(win_floor_bps as f64);
        let opt_coeff = if v55.tranche2_harvested {
            1.0 + INV_PHI * s_run
        } else if ctx.tranche1_harvested {
            INV_PHI + INV_PHI_SQ * s_run
        } else {
            1.0 + INV_PHI_SQ * s_run
        };
        let s_vol_mkr = hermite_s3_unit(sigma_1m, c_mkr_scale, PHI * c_mkr_scale);
        let mfe_runner_r = (base_r * (1.0 + INV_PHI_SQ * s_run * s_vol_mkr))
            .min(PHI * sigma_1m)
            .max(base_r);
        let prop_r = (opt_coeff * sigma_1m).max(mfe_runner_r);
        let runner_bps = ((base_r + s_run * (prop_r - base_r)).round() as i64)
            .max(eff_ratchet_bps)
            .clamp(win_floor_bps + fee_rt / 2, max_fib_cap);
        dynamic_maker_floor_price(ctx.side, ctx.avg_price_ticks, ScaledBps(runner_bps))
    } else {
        PriceTicks(0)
    };
    let fib_px = if aligned_fib_rem_bps > 0 && own_flow >= -0.08 && matches!(w_obs.regime, WaveRegime::ImpulseWave3 | WaveRegime::ImpulseWave5) {
        dynamic_maker_floor_price(
            ctx.side, ctx.avg_price_ticks,
            ScaledBps(cur_gross_bps.max(0).saturating_add(aligned_fib_rem_bps).max(eff_ratchet_bps).clamp(win_floor_bps, max_fib_cap)),
        )
    } else { PriceTicks(0) };
    let within_float = hold_s <= (tau_s / 3).max(45);
    let sentinel_px = if s_prop >= INV_PHI_SQ && within_float
        && (flow_accel >= 0.035 || (matches!(w_obs.regime, WaveRegime::NeutralWarmup) && w_obs.hurst_exponent() >= 0.55))
    {
        let delta_sh = (INV_PHI * vol_f * (1.0 + 0.50 * s_prop)).round() as i64;
        dynamic_maker_floor_price(
            ctx.side, ctx.avg_price_ticks,
            ScaledBps(cur_gross_bps.max(0).saturating_add(delta_sh).max(eff_ratchet_bps).clamp(win_floor_bps, max_fib_cap)),
        )
    } else if within_float && s_prop >= 0.18 && ctx.active_exit_price != ctx.structural_tp_price { ctx.active_exit_price } else { PriceTicks(0) };
    let pre_fib_best = pick_more_ambitious_tp(ctx.side, base_tp, lvdbs_px);
    let with_runner_best = pick_more_ambitious_tp(ctx.side, pre_fib_best, runner2_px);
    let combined_best = pick_more_ambitious_tp(ctx.side, pick_more_ambitious_tp(ctx.side, with_runner_best, fib_px), sentinel_px);
    let fib_or_shadow_ext = (fib_px.0 > 0 || sentinel_px.0 > 0) && combined_best != with_runner_best;
    let runner2_ext = runner2_px.0 > 0 && with_runner_best != pre_fib_best;
    let spec = APPROVED_SPECS[ctx.slot_idx.min(MAX_ACTIVE_PAIRS - 1)];
    let ob_tp = snap.order_block_observables().front_run_opposing_wall_tp_with_fees(
        ctx.side, spec, ctx.avg_price_ticks, combined_best, ctx.active_fees,
    );
    let target_price = enforce_maker_bbo_bound(ctx.side, pick_more_ambitious_tp(ctx.side, min_win_px, ob_tp), snap);
    if v55.psi_health > INV_PHI_SQ
        && !kin.in_scar_debt
        && v55.tranche1_harvests_count == 1
        && ctx.tranche1_harvested
        && !v55.tranche2_harvested
        && ctx.open_lots.0 >= 3
    {
        let raw_t1_gross = if ctx.tranche1_exit_price.0 > 0 {
            ((ctx.tranche1_exit_price.0 - ctx.avg_price_ticks.0)
                .saturating_mul(d_sign)
                .saturating_mul(1_000_000))
                / avg_denom
        } else {
            win_floor_bps
        };
        let t1_was_offensive = ctx.tranche1_exit_price.0 <= 0
            || v55.house_money_runner_unlocked
            || raw_t1_gross.saturating_add(25) >= win_floor_bps;
        let t1_gross_sbps = raw_t1_gross.max(win_floor_bps);
        let base_phi_t2_sbps = ((PHI * (t1_gross_sbps as f64)).round() as i64)
            .max(t1_gross_sbps + 40)
            .max(eff_ratchet_bps);
        let min_ext_sbps = ((INV_PHI * (fee_rt as f64)).round() as i64).max(1);
        let learned_struct_sbps = ctx.learned_mfe_bps.max(projected_mfe_bps);
        let learned_mfe_tp = dynamic_maker_floor_price(
            ctx.side,
            ctx.avg_price_ticks,
            ScaledBps(learned_struct_sbps),
        );
        let ext_struct_px = enforce_maker_bbo_bound(
            ctx.side,
            pick_more_ambitious_tp(ctx.side, learned_mfe_tp, fib_px),
            snap,
        );
        let base_t2_px = enforce_maker_bbo_bound(
            ctx.side,
            dynamic_maker_floor_price(ctx.side, ctx.avg_price_ticks, ScaledBps(base_phi_t2_sbps)),
            snap,
        );
        let struct_tick_gap = (ext_struct_px.0 - base_t2_px.0).saturating_mul(d_sign);
        let struct_sbps_gap = (struct_tick_gap.saturating_mul(1_000_000)) / avg_denom;
        let cont_ready = v55.cont_ratio_x1000 >= 950
            || ctx.learned_mfe_bps >= t1_gross_sbps.saturating_add(min_ext_sbps.saturating_mul(2));
        if t1_was_offensive && cont_ready && struct_tick_gap >= 2 && struct_sbps_gap >= min_ext_sbps {
            let full_ext_px = pick_more_ambitious_tp(ctx.side, ext_struct_px, target_price);
            let full_ext_sbps = ((full_ext_px.0 - ctx.avg_price_ticks.0)
                .saturating_mul(d_sign)
                .saturating_mul(1_000_000))
                / avg_denom;
            let span_above_t1 = (full_ext_sbps - t1_gross_sbps).max(0) as f64;
            let golden_sec_t2_sbps = t1_gross_sbps.saturating_add(
                (INV_PHI_SQ * span_above_t1 * (1.0 + INV_PHI * s_run.clamp(0.0, 1.0))).round()
                    as i64,
            );
            let t2_gross_sbps = base_phi_t2_sbps.max(golden_sec_t2_sbps);
            let t2_px = enforce_maker_bbo_bound(
                ctx.side,
                dynamic_maker_floor_price(ctx.side, ctx.avg_price_ticks, ScaledBps(t2_gross_sbps)),
                snap,
            );
            let final_tick_gap = (target_price.0 - t2_px.0).saturating_mul(d_sign);
            let final_sbps_gap = (final_tick_gap.saturating_mul(1_000_000)) / avg_denom;
            let opt_t2_lots = if final_tick_gap >= 2 && final_sbps_gap >= min_ext_sbps {
                compute_tranche2_split_lots_core(ctx.slot_idx, ctx.open_lots, t2_px)
            } else {
                None
            };
            if let Some(t2_lots) = opt_t2_lots {
                return Some(CanonicalExitQuote {
                    role: ExitOrderRole::Tranche2HarvestPhi,
                    target_price: t2_px,
                    order_lots: t2_lots,
                    sub_reason: ExitSubReason::Tranche2HarvestPhi,
                    vac_level: 2,
                    defensive_lock_active: false,
                    urgent_close_repeg: false,
                    cur_gross_bps,
                    wald_pct,
                });
            }
        }
    }
    let (vac_level, sub_reason) = if fib_or_shadow_ext { (lvdbs_vac_level.max(3), ExitSubReason::FibonacciImpulseSurf) }
        else if lvdbs_vac_level >= 2 || runner2_ext { (lvdbs_vac_level.max(2), ExitSubReason::LvdbsDeepVacuumSurf) }
        else { (1, ExitSubReason::StandardStructuralTp) };
    Some(CanonicalExitQuote {
        role, target_price, order_lots: ctx.open_lots, sub_reason, vac_level,
        defensive_lock_active: false, urgent_close_repeg: false, cur_gross_bps, wald_pct,
    })
}

#[inline]
#[must_use]
pub fn compute_canonical_exit_quote_core(ctx: &OpenExitContext, snap: &SymbolLobSnapshot) -> Option<CanonicalExitQuote> {
    compute_canonical_exit_quote_omega(ctx, ShadowRunnerKinematics::neutral(), snap)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lob_shm::BookLevel;

    #[test]
    fn test_v46_exit_quoter_vip_rebate_zero_loss_floor_and_chandelier_profit_lock() {
        let vip0 = FeeSchedule::VIP0_DEFAULT;
        let mm_rebate = FeeSchedule::from_live_rates_clamped(ScaledBps(-50), ScaledBps(300));
        let z_vip0 = defensive_zero_loss_maker_exit_price_with_fees(Side::Bid, PriceTicks(759_000), vip0);
        let z_reb = defensive_zero_loss_maker_exit_price_with_fees(Side::Bid, PriceTicks(759_000), mm_rebate);
        assert!(z_reb.0 > 759_000 && z_reb.0 < z_vip0.0);
        assert!(!is_chandelier_profit_locked_sl_with_fees(Side::Bid, PriceTicks(10_000), PriceTicks(10_004), vip0));
        assert!(is_chandelier_profit_locked_sl_with_fees(Side::Bid, PriceTicks(10_000), PriceTicks(10_004), mm_rebate));
        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = APPROVED_SPECS[3].symbol_id;
        snap.realized_vol_5m = ScaledBps(7_000);
        snap.mark_price = PriceTicks(9_007);
        snap.last_trade_price = PriceTicks(9_008);
        assert!(matches!(
            evaluate_sl_or_emergency_flatten_with_fees(Side::Ask, PriceTicks(8_030), &snap, mm_rebate),
            Ok(ServerSlDecision::BreachEmergencyFlatten { backup_sl, .. }) if backup_sl.0 >= 9_011
        ));
        let mut s_ok = SymbolLobSnapshot::new_empty();
        s_ok.symbol_id = APPROVED_SPECS[0].symbol_id;
        s_ok.realized_vol_5m = ScaledBps(6_500);
        s_ok.bids[0] = BookLevel { price: PriceTicks(759_450), qty: LotQty(380) };
        s_ok.asks[0] = BookLevel { price: PriceTicks(759_451), qty: LotQty(210) };
        s_ok.factor_ofi = BoundedScore::new_clamped(0.75);
        s_ok.factor_tfi = BoundedScore::new_clamped(0.82);
        let ctx = OpenExitContext {
            slot_idx: 0, side: Side::Bid, avg_price_ticks: PriceTicks(759_000),
            peak_favorable_price: PriceTicks(759_450), worst_adverse_price: PriceTicks(759_000),
            initial_lots: LotQty(1), open_lots: LotQty(1), tranche1_harvested: false,
            tranche1_lots: LotQty(0), tranche1_exit_price: PriceTicks(0), defensive_floor_locked: true,
            active_exit_price: PriceTicks(759_494), structural_tp_price: PriceTicks(759_494),
            resting_exit_price: PriceTicks(759_494), sl_trigger_price: PriceTicks(753_000),
            opened_ts: TimestampNs(5_000_000_000), learned_mfe_bps: 680, learned_t1_permille: 500,
            learned_ir_x100: 33, learned_ret_std_bps: 200, intrinsic_tau_mkt_s: 180,
            last_eff_sl_ticks: PriceTicks(753_000), ratchet_locked_gross_bps: 0,
            last_closed_mfe_bps: 200, last_closed_mae_bps: 0, active_fees: mm_rebate, learned_taker_prob_pm: 50,
        };
        assert!(compute_canonical_exit_quote_core(&ctx, &s_ok).is_some_and(|q| q.urgent_close_repeg));
    }

    #[test]
    fn test_v51_f2_reversible_unlock_runner_preservation_and_wave3_escape() {
        let mut eth_s = SymbolLobSnapshot::new_empty();
        eth_s.symbol_id = APPROVED_SPECS[1].symbol_id;
        eth_s.ts_local_ns = TimestampNs(115_000_000_000);
        eth_s.realized_vol_5m = ScaledBps(2_200);
        eth_s.bids[0] = BookLevel { price: PriceTicks(179_618), qty: LotQty(620) };
        eth_s.asks[0] = BookLevel { price: PriceTicks(179_619), qty: LotQty(210) };
        eth_s.factor_ofi = BoundedScore::new_clamped(0.58);
        eth_s.factor_tfi = BoundedScore::new_clamped(0.71);
        let w3 = WaveObservables {
            regime: WaveRegime::ImpulseWave3, climax_active: false, hurst: WaveObservables::encode_hurst(0.68),
            macro_phase: BoundedScore::new_clamped(0.90), oi_impulse: BoundedScore::new_clamped(0.35),
            liq_imbalance: BoundedScore::new_clamped(0.40), fib_proj_bps: ScaledBps(4_559),
        };
        eth_s.set_wave_observables(w3);
        assert_eq!(compute_reversible_defensive_state_c1(ScaledBps(428), ScaledBps(520), BoundedScore::new_clamped(0.62), 50, 25, w3, Side::Bid, true), (false, false));
        let eth_ctx = OpenExitContext {
            slot_idx: 1, side: Side::Bid, avg_price_ticks: PriceTicks(179_542),
            peak_favorable_price: PriceTicks(179_619), worst_adverse_price: PriceTicks(179_352),
            initial_lots: LotQty(1), open_lots: LotQty(1), tranche1_harvested: false,
            tranche1_lots: LotQty(0), tranche1_exit_price: PriceTicks(0), defensive_floor_locked: false,
            active_exit_price: PriceTicks(180_200), structural_tp_price: PriceTicks(180_200),
            resting_exit_price: PriceTicks(179_619), sl_trigger_price: PriceTicks(179_100),
            opened_ts: TimestampNs(5_000_000_000), learned_mfe_bps: 650, learned_t1_permille: 500,
            learned_ir_x100: 25, learned_ret_std_bps: 220, intrinsic_tau_mkt_s: 180,
            last_eff_sl_ticks: PriceTicks(179_100), ratchet_locked_gross_bps: 0,
            last_closed_mfe_bps: 0, last_closed_mae_bps: -1_058, active_fees: FeeSchedule::VIP0_DEFAULT, learned_taker_prob_pm: 50,
        };
        let kin_win = ShadowRunnerKinematics { flow_ema_fast: 0.66, flow_ema_slow: 0.62, in_scar_debt: false, chandelier_armed: false };
        assert!(compute_canonical_exit_quote_omega(&eth_ctx, kin_win, &eth_s).is_some_and(|q| !q.defensive_lock_active && q.target_price.0 > 180_000));
        let mut eth_latched = eth_ctx;
        eth_latched.defensive_floor_locked = true;
        assert!(compute_canonical_exit_quote_omega(&eth_latched, kin_win, &eth_s).is_some_and(|q| !q.defensive_lock_active && q.target_price.0 > 180_000));
        let pnl_json = serde_json::json!({
            "retCode": 0,
            "result": { "list": [
                { "symbol": "ETHUSDT", "closedSize": "0.01", "avgEntryPrice": "1795.42", "avgExitPrice": "1802.00", "closedPnl": "0.0615", "orderType": "Limit", "updatedTime": "1800000010000" },
                { "symbol": "ETHUSDT", "closedSize": "0.01", "avgEntryPrice": "1790.00", "avgExitPrice": "1794.00", "closedPnl": "0.0350", "orderType": "Limit", "updatedTime": "1800000009500" }
            ]}
        });
        let mut rec_ctx = eth_ctx;
        rec_ctx.opened_ts = TimestampNs(1_800_000_009_000_000_000);
        match reconcile_closed_pnl_core(&rec_ctx, &pnl_json, 1, false) {
            ClosedPnlQueryOutcome::Verified(v) => {
                assert_eq!(v.closed_lots, LotQty(1));
                assert_eq!(v.exchange_closed_pnl_1e8, Some(6_150_000));
            }
            other => assert_eq!(other, ClosedPnlQueryOutcome::PendingIndexerRetry, "unexpected outcome"),
        }
        let mut tao_ctx = eth_ctx;
        tao_ctx.slot_idx = 4; tao_ctx.avg_price_ticks = PriceTicks(19_505); tao_ctx.peak_favorable_price = PriceTicks(19_514);
        tao_ctx.worst_adverse_price = PriceTicks(19_495); tao_ctx.open_lots = LotQty(12); tao_ctx.initial_lots = LotQty(12);
        let mut tao_s = eth_s;
        tao_s.symbol_id = APPROVED_SPECS[4].symbol_id;
        tao_s.bids[0] = BookLevel { price: PriceTicks(19_513), qty: LotQty(180) };
        tao_s.asks[0] = BookLevel { price: PriceTicks(19_514), qty: LotQty(90) };
        let mut tao_w = w3; tao_w.regime = WaveRegime::CorrectiveAbc; tao_s.set_wave_observables(tao_w);
        let kin_mild = ShadowRunnerKinematics { flow_ema_fast: 0.16, flow_ema_slow: 0.12, in_scar_debt: false, chandelier_armed: false };
        assert!(compute_canonical_exit_quote_omega(&tao_ctx, kin_mild, &tao_s).is_some_and(|q| q.role == ExitOrderRole::Tranche1Harvest50 && q.defensive_lock_active && q.order_lots.0 < 12));
        let mut eth6_ctx = eth_ctx; eth6_ctx.side = Side::Ask; eth6_ctx.worst_adverse_price = PriceTicks(179_560); eth6_ctx.last_closed_mae_bps = -100;
        eth_s.ts_local_ns = TimestampNs(25_000_000_000);
        let kin_ask = ShadowRunnerKinematics { flow_ema_fast: 0.45, flow_ema_slow: 0.40, in_scar_debt: false, chandelier_armed: false };
        assert!(compute_canonical_exit_quote_omega(&eth6_ctx, kin_ask, &eth_s).is_some_and(|q| q.defensive_lock_active && q.urgent_close_repeg));
    }

    #[test]
    fn test_h2_h5_h6b_limit_bailout_reconcile_deadcat_bounce_and_adaptive_s_run() {
        let base_ctx = OpenExitContext {
            slot_idx: 1, side: Side::Bid, avg_price_ticks: PriceTicks(180_000),
            peak_favorable_price: PriceTicks(180_010), worst_adverse_price: PriceTicks(178_200),
            initial_lots: LotQty(1), open_lots: LotQty(1), tranche1_harvested: false,
            tranche1_lots: LotQty(0), tranche1_exit_price: PriceTicks(0), defensive_floor_locked: true,
            active_exit_price: PriceTicks(180_080), structural_tp_price: PriceTicks(180_100),
            resting_exit_price: PriceTicks(180_080), sl_trigger_price: PriceTicks(177_500),
            opened_ts: TimestampNs(1_800_000_009_000_000_000), learned_mfe_bps: 720, learned_t1_permille: 500,
            learned_ir_x100: 35, learned_ret_std_bps: 1_500, intrinsic_tau_mkt_s: 120,
            last_eff_sl_ticks: PriceTicks(177_500), ratchet_locked_gross_bps: 0,
            last_closed_mfe_bps: 60, last_closed_mae_bps: -1_000, active_fees: FeeSchedule::VIP0_DEFAULT,
            learned_taker_prob_pm: 45,
        };
        // H2: Limit order with negative PnL (-3.20 bps) is ALWAYS MakerPassiveUnwind (never SL)
        let limit_loss_json = serde_json::json!({
            "retCode": 0,
            "result": { "list": [{
                "symbol": "ETHUSDT", "closedSize": "0.01", "avgEntryPrice": "1800.00",
                "avgExitPrice": "1799.50", "closedPnl": "-0.0058", "orderType": "Limit",
                "updatedTime": "1800000010000"
            }]}
        });
        assert!(matches!(
            reconcile_closed_pnl_core(&base_ctx, &limit_loss_json, 1, false),
            ClosedPnlQueryOutcome::Verified(v)
                if v.kind == ClosedTradeKind::MakerPassiveUnwind && v.sub_reason == ExitSubReason::AdverseScratchEscape
        ));

        // H5: Dead-cat bounce with exhausted Wald SPRT keeps maker_bailout_active engaged at BBO
        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = APPROVED_SPECS[1].symbol_id;
        snap.ts_local_ns = TimestampNs(1_800_000_045_000_000_000);
        snap.realized_vol_5m = ScaledBps(2_200);
        snap.bids[0] = BookLevel { price: PriceTicks(179_940), qty: LotQty(150) };
        snap.asks[0] = BookLevel { price: PriceTicks(179_941), qty: LotQty(150) };
        let kin_bounce = ShadowRunnerKinematics { flow_ema_fast: 0.08, flow_ema_slow: 0.04, in_scar_debt: false, chandelier_armed: false };
        let q_bail_opt = compute_canonical_exit_quote_omega_v52(&base_ctx, kin_bounce, &snap, Some(-2.2), 0, 5_000);
        assert!(q_bail_opt.is_some());
        let Some(q_bail) = q_bail_opt else { return };
        let zero_loss_px = defensive_zero_loss_maker_exit_price_with_fees(Side::Bid, base_ctx.avg_price_ticks, base_ctx.active_fees);
        assert!(q_bail.target_price.0 < zero_loss_px.0 && q_bail.target_price.0 == 179_941);
        // Healthy Wald (p_cont >= 0.50) does NOT bail out below zero_loss_px on bounce
        let q_hold_opt = compute_canonical_exit_quote_omega_v52(&base_ctx, kin_bounce, &snap, Some(0.5), 0, 5_000);
        assert!(q_hold_opt.is_some());
        let Some(q_hold) = q_hold_opt else { return };
        let mut healthy_ctx = base_ctx;
        healthy_ctx.last_closed_mfe_bps = 300;
        let q_healthy = compute_canonical_exit_quote_omega_v52(&healthy_ctx, kin_bounce, &snap, Some(0.5), 0, 5_000);
        assert!(q_hold.target_price.0 >= 179_941 && q_healthy.is_some_and(|q| q.target_price.0 >= zero_loss_px.0));

        // H6b: Unsplit 1-lot adaptive s_run unlocks earlier under clean maker toxicity vs toxic tape
        let mut run_ctx = base_ctx;
        run_ctx.defensive_floor_locked = false;
        run_ctx.worst_adverse_price = PriceTicks(180_000);
        run_ctx.last_closed_mae_bps = 0;
        run_ctx.structural_tp_price = PriceTicks(180_105);
        run_ctx.learned_taker_prob_pm = 25;
        run_ctx.learned_ir_x100 = 80;
        snap.bids[0] = BookLevel { price: PriceTicks(180_040), qty: LotQty(200) };
        snap.asks[0] = BookLevel { price: PriceTicks(180_041), qty: LotQty(200) };
        snap.factor_ofi = BoundedScore::new_clamped(0.48);
        snap.factor_tfi = BoundedScore::new_clamped(0.50);
        let kin_clean = ShadowRunnerKinematics { flow_ema_fast: 0.56, flow_ema_slow: 0.44, in_scar_debt: false, chandelier_armed: false };
        let q_clean_opt = compute_canonical_exit_quote_omega_v52(&run_ctx, kin_clean, &snap, None, 0, 5_000);
        assert!(q_clean_opt.is_some());
        let Some(q_clean) = q_clean_opt else { return };
        let mut tox_ctx = run_ctx;
        tox_ctx.learned_taker_prob_pm = 450;
        tox_ctx.learned_ir_x100 = 0;
        let q_tox_opt = compute_canonical_exit_quote_omega_v52(&tox_ctx, kin_clean, &snap, None, 0, 5_000);
        assert!(q_tox_opt.is_some());
        let Some(q_tox) = q_tox_opt else { return };
        assert!(q_clean.target_price.0 > q_tox.target_price.0);
    }
}
