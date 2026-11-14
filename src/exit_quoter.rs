//! Quantum Maker Exit Quoter, Fibonacci 1.618x/2.618x Ceiling, Climax Harvest & P0-A Guard.

#![allow(
    clippy::manual_clamp,
    clippy::manual_range_contains,
    clippy::needless_range_loop,
    clippy::too_many_arguments
)]

use crate::lob_shm::{SymbolLobSnapshot, MAX_ACTIVE_PAIRS};
use crate::types::{
    LotQty, PairSpec, PriceTicks, ScaledBps, Side, TimestampNs, VortexError, APPROVED_SPECS,
    SPLIT_LEG_MIN_NOTIONAL_1E8,
};
use crate::wave_elliott::{CausalElliottWaveState, WaveRegime};
use crate::ws_protocol::parse_verified_avg_price_ticks;

pub const OFFENSIVE_WIN_FLOOR_GROSS_BPS: i64 = 520;
pub const MAX_CLOSE_RECONCILE_RETRIES: u8 = 4;

const PHI_SQ: f64 = 2.618_033_988_749_895;
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
    LvdbsDeepSurf50,
    StandardMakerExit100,
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum ExitSubReason {
    #[default]
    AdverseScratchEscape,
    Tranche1Harvest50,
    StandardStructuralTp,
    LvdbsDeepVacuumSurf,
    FibonacciImpulseSurf,
    ConvexMfeRatchetLock,
    ClimaxExhaustionHarvest,
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
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ClosedPnlQueryOutcome {
    Verified(VerifiedCloseResolution),
    PendingIndexerRetry,
    TransportOrApiError,
}

#[inline]
#[must_use]
pub const fn dynamic_maker_floor_price(
    side: Side,
    avg_price_ticks: PriceTicks,
    target_gross_bps: ScaledBps,
) -> PriceTicks {
    let avg = if avg_price_ticks.0 > 1 { avg_price_ticks.0 } else { 1 };
    let bps = if target_gross_bps.0 > 0 { target_gross_bps.0 } else { 0 };
    let num = (avg as i128) * (bps as i128);
    let raw_delta = ((num + 999_999) / 1_000_000) as i64;
    let delta = if raw_delta > 1 { raw_delta } else { 1 };
    match side {
        Side::Bid => PriceTicks(avg.saturating_add(delta)),
        Side::Ask => {
            let sub = avg - delta;
            PriceTicks(if sub > 1 { sub } else { 1 })
        }
    }
}

#[inline]
#[must_use]
pub const fn defensive_zero_loss_maker_exit_price(
    side: Side,
    avg_price_ticks: PriceTicks,
) -> PriceTicks {
    let fee_rt = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
    dynamic_maker_floor_price(side, avg_price_ticks, ScaledBps(fee_rt + 25))
}

#[inline]
#[must_use]
pub fn offensive_win_maker_exit_price(
    side: Side,
    avg_price_ticks: PriceTicks,
    urgency_0_to_1: f64,
) -> PriceTicks {
    let u = if urgency_0_to_1.is_nan() {
        0.50
    } else {
        urgency_0_to_1.clamp(0.0, 1.0)
    };
    let gross_bps =
        (OFFENSIVE_WIN_FLOOR_GROSS_BPS as f64 + 60.0 * (0.50 - u)).round() as i64;
    let clamped = gross_bps.clamp(OFFENSIVE_WIN_FLOOR_GROSS_BPS, OFFENSIVE_WIN_FLOOR_GROSS_BPS + 60);
    dynamic_maker_floor_price(side, avg_price_ticks, ScaledBps(clamped))
}

#[inline]
#[must_use]
pub fn compute_tranche1_split_lots_core(
    slot_idx: usize,
    open_lots: LotQty,
    exit_price: PriceTicks,
    t1_permille: i64,
) -> Option<LotQty> {
    if open_lots.0 < 2 || exit_price.0 <= 0 {
        return None;
    }
    let spec = APPROVED_SPECS[slot_idx.min(MAX_ACTIVE_PAIRS - 1)];
    let phi = t1_permille.clamp(350, 650);
    let t1_raw = ((open_lots.0.saturating_mul(phi)) + 500) / 1_000;
    let t1_lots = LotQty(t1_raw.clamp(1, open_lots.0 - 1));
    let rem_lots = LotQty(open_lots.0 - t1_lots.0);
    if spec.lot_notional_1e8(t1_lots, exit_price) < SPLIT_LEG_MIN_NOTIONAL_1E8 {
        return None;
    }
    if spec.lot_notional_1e8(rem_lots, exit_price) < SPLIT_LEG_MIN_NOTIONAL_1E8 {
        return None;
    }
    Some(t1_lots)
}

#[inline]
pub fn compute_validated_sl_ticks(
    side: Side,
    verified_avg_price: PriceTicks,
    snap: &SymbolLobSnapshot,
) -> Result<PriceTicks, VortexError> {
    if verified_avg_price.0 <= 0 {
        return Err(VortexError::InvalidPriceOrTickSpec);
    }
    let sl_bps = PairSpec::elastic_server_sl_distance_bps(snap.realized_vol_5m).raw();
    let sl_delta = (((verified_avg_price.0 as i128 * sl_bps as i128) + 999_999) / 1_000_000)
        .max(4) as i64;
    let ref_mark = if snap.mark_price.0 > 0 { snap.mark_price } else { verified_avg_price };
    let ref_last = if snap.last_trade_price.0 > 0 { snap.last_trade_price } else { ref_mark };
    let cand_sl = match side {
        Side::Bid => {
            let strict_lo = ref_mark.0.min(ref_last.0);
            let nominal = verified_avg_price.0 - sl_delta;
            PriceTicks(nominal.min(strict_lo - 4).max(1))
        }
        Side::Ask => {
            let strict_hi = ref_mark.0.max(ref_last.0);
            let nominal = verified_avg_price.0.saturating_add(sl_delta);
            PriceTicks(nominal.max(strict_hi.saturating_add(4)))
        }
    };
    PairSpec::validate_stop_loss_geometry(side, cand_sl, ref_last, ref_mark)
}

/// P0-A Law V: Detects >= 1.5x elastic SL price gaps and arms immediate Market IOC flatten.
#[inline]
pub fn evaluate_sl_or_emergency_flatten_core(
    side: Side,
    verified_avg_price: PriceTicks,
    snap: &SymbolLobSnapshot,
) -> Result<ServerSlDecision, VortexError> {
    let valid_sl = compute_validated_sl_ticks(side, verified_avg_price, snap)?;
    let base_sl_bps = PairSpec::elastic_server_sl_distance_bps(snap.realized_vol_5m).raw();
    let max_allowed_bps = (base_sl_bps * 3) / 2;
    let ref_mark = if snap.mark_price.0 > 0 { snap.mark_price } else { verified_avg_price };
    let ref_last = if snap.last_trade_price.0 > 0 { snap.last_trade_price } else { ref_mark };
    let worst_ref = match side {
        Side::Bid => ref_mark.0.min(ref_last.0),
        Side::Ask => ref_mark.0.max(ref_last.0),
    };
    let adv_ticks = match side {
        Side::Bid => verified_avg_price.0 - worst_ref,
        Side::Ask => worst_ref - verified_avg_price.0,
    };
    let observed_loss_bps =
        (adv_ticks.max(0).saturating_mul(1_000_000)) / verified_avg_price.0.max(1);
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
#[must_use]
pub const fn classify_maker_exit_sub_reason(
    gross_scaled_bps: i64,
    vac_level_hint: usize,
    tranche1_harvested: bool,
    climax_active: bool,
    fib_projected: bool,
) -> ExitSubReason {
    let fee_rt = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
    if climax_active && gross_scaled_bps >= fee_rt {
        ExitSubReason::ClimaxExhaustionHarvest
    } else if fib_projected && gross_scaled_bps >= OFFENSIVE_WIN_FLOOR_GROSS_BPS + 200 {
        ExitSubReason::FibonacciImpulseSurf
    } else if vac_level_hint >= 2 && gross_scaled_bps > fee_rt + 40 {
        ExitSubReason::LvdbsDeepVacuumSurf
    } else if gross_scaled_bps < OFFENSIVE_WIN_FLOOR_GROSS_BPS {
        if tranche1_harvested {
            ExitSubReason::Tranche1Harvest50
        } else {
            ExitSubReason::AdverseScratchEscape
        }
    } else {
        ExitSubReason::StandardStructuralTp
    }
}

fn parse_pnl_decimal_1e8(raw: &str) -> Option<i64> {
    let s = raw.trim();
    if s.is_empty() {
        return None;
    }
    let (neg, body) = if let Some(r) = s.strip_prefix('-') {
        (true, r)
    } else if let Some(r) = s.strip_prefix('+') {
        (false, r)
    } else {
        (false, s)
    };
    if body.is_empty() {
        return None;
    }
    let mut whole: i128 = 0;
    let mut frac: i128 = 0;
    let mut frac_digits: u32 = 0;
    let mut in_frac = false;
    for b in body.bytes() {
        if b == b'.' {
            if in_frac {
                return None;
            }
            in_frac = true;
            continue;
        }
        if !b.is_ascii_digit() {
            return None;
        }
        let d = i128::from(b - b'0');
        if !in_frac {
            whole = whole.saturating_mul(10).saturating_add(d);
        } else if frac_digits < 8 {
            frac = frac.saturating_mul(10).saturating_add(d);
            frac_digits += 1;
        }
    }
    while frac_digits < 8 {
        frac = frac.saturating_mul(10);
        frac_digits += 1;
    }
    let mag = whole.saturating_mul(100_000_000).saturating_add(frac);
    let signed = if neg { -mag } else { mag };
    i64::try_from(signed).ok()
}

#[must_use]
pub fn reconcile_closed_pnl_core(
    ctx: &OpenExitContext,
    payload: &serde_json::Value,
    vac_level_hint: usize,
    allow_partial_lot_coverage: bool,
) -> ClosedPnlQueryOutcome {
    if payload.get("retCode").and_then(serde_json::Value::as_i64) != Some(0) {
        return ClosedPnlQueryOutcome::TransportOrApiError;
    }
    let Some(list) = payload
        .get("result")
        .and_then(|r| r.get("list"))
        .and_then(serde_json::Value::as_array)
    else {
        return ClosedPnlQueryOutcome::TransportOrApiError;
    };
    if list.is_empty() {
        return ClosedPnlQueryOutcome::PendingIndexerRetry;
    }
    let spec = APPROVED_SPECS[ctx.slot_idx.min(MAX_ACTIVE_PAIRS - 1)];
    let expected_sym = spec.symbol_id.name();
    let min_ms = if ctx.opened_ts.0 >= MIN_VALID_UTC_EPOCH_NS {
        (ctx.opened_ts.0 / 1_000_000).saturating_sub(5_000)
    } else {
        0
    };

    let mut acc_lots: i64 = 0;
    let mut w_entry_num: i128 = 0;
    let mut w_exit_num: i128 = 0;
    let mut w_px_lots: i128 = 0;
    let mut sum_pnl_1e8: i64 = 0;
    let mut has_pnl = false;
    let mut legs_count: u32 = 0;
    let mut saw_market_sl = false;

    for row in list {
        let row_sym = row.get("symbol").and_then(serde_json::Value::as_str).unwrap_or("");
        if !row_sym.is_empty() && row_sym != expected_sym {
            continue;
        }
        let upd_ms = row
            .get("updatedTime")
            .or_else(|| row.get("createdTime"))
            .and_then(|v| {
                v.as_i64()
                    .or_else(|| v.as_str().and_then(|s| s.trim().parse::<i64>().ok()))
            })
            .unwrap_or(0);
        if min_ms > 0 && upd_ms > 0 && upd_ms < min_ms {
            continue;
        }
        let q_lots = row
            .get("closedSize")
            .or_else(|| row.get("qty"))
            .and_then(serde_json::Value::as_str)
            .map_or(LotQty(0), |qs| spec.parse_lot_qty(qs));
        let en_opt = row
            .get("avgEntryPrice")
            .and_then(serde_json::Value::as_str)
            .and_then(|s| parse_verified_avg_price_ticks(spec, ctx.side, s).ok());
        let ex_opt = row
            .get("avgExitPrice")
            .and_then(serde_json::Value::as_str)
            .and_then(|s| spec.parse_price_ticks(s).ok());
        let pnl_opt = row
            .get("closedPnl")
            .and_then(|v| {
                v.as_str()
                    .and_then(parse_pnl_decimal_1e8)
                    .or_else(|| v.as_f64().map(|f| (f * 100_000_000.0).round() as i64))
            });
        if q_lots.0 <= 0 && pnl_opt.is_none() && ex_opt.is_none() {
            continue;
        }
        legs_count = legs_count.saturating_add(1);
        if let Some(p1e8) = pnl_opt {
            sum_pnl_1e8 = sum_pnl_1e8.saturating_add(p1e8);
            has_pnl = true;
        }
        let ord_t = row.get("orderType").and_then(serde_json::Value::as_str).unwrap_or("");
        let exec_t = row.get("execType").and_then(serde_json::Value::as_str).unwrap_or("");
        if ord_t.eq_ignore_ascii_case("Market")
            || exec_t.eq_ignore_ascii_case("BustTrade")
            || exec_t.eq_ignore_ascii_case("AdlTrade")
        {
            saw_market_sl = true;
        }
        let eff_leg_q = q_lots.0.max(1);
        if q_lots.0 > 0 {
            acc_lots = acc_lots.saturating_add(q_lots.0);
        }
        if let (Some(en_px), Some(ex_px)) = (en_opt, ex_opt) {
            w_entry_num += (eff_leg_q as i128) * (en_px.0 as i128);
            w_exit_num += (eff_leg_q as i128) * (ex_px.0 as i128);
            w_px_lots += eff_leg_q as i128;
        }
    }

    if legs_count == 0 {
        return ClosedPnlQueryOutcome::PendingIndexerRetry;
    }
    let expected_lots = if ctx.tranche1_harvested {
        ctx.initial_lots.0.max(ctx.open_lots.0).max(1)
    } else {
        ctx.open_lots.0.max(1)
    };
    if !allow_partial_lot_coverage && acc_lots > 0 && acc_lots * 100 < expected_lots * 95 {
        return ClosedPnlQueryOutcome::PendingIndexerRetry;
    }

    let final_lots = LotQty(if acc_lots > 0 { acc_lots } else { expected_lots });
    let verified_entry_price = if w_px_lots > 0 {
        PriceTicks(((w_entry_num + w_px_lots / 2) / w_px_lots).max(1) as i64)
    } else {
        ctx.avg_price_ticks.max(PriceTicks(1))
    };
    let verified_exit_price = if w_px_lots > 0 {
        PriceTicks(((w_exit_num + w_px_lots / 2) / w_px_lots).max(1) as i64)
    } else if ctx.resting_exit_price.0 > 0 {
        ctx.resting_exit_price
    } else if ctx.active_exit_price.0 > 0 {
        ctx.active_exit_price
    } else {
        defensive_zero_loss_maker_exit_price(ctx.side, verified_entry_price)
    };

    let notional_1e8 = spec.lot_notional_1e8(final_lots, verified_entry_price).max(1);
    let est_net_bps = if has_pnl {
        ((sum_pnl_1e8 as i128 * 1_000_000) / (notional_1e8 as i128)) as i64
    } else {
        ((verified_exit_price.0 - verified_entry_price.0)
            .saturating_mul(ctx.side.sign_i64())
            .saturating_mul(1_000_000))
            / verified_entry_price.0.max(1)
            - ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2
    };
    let kind = if saw_market_sl || est_net_bps <= -150 {
        ClosedTradeKind::ServerMarkPriceStopLoss
    } else {
        ClosedTradeKind::MakerPassiveUnwind
    };
    let sub_reason = match kind {
        ClosedTradeKind::ServerMarkPriceStopLoss => ExitSubReason::WaldBaseStopLoss,
        ClosedTradeKind::MakerPassiveUnwind => classify_maker_exit_sub_reason(
            est_net_bps + ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2,
            vac_level_hint,
            ctx.tranche1_harvested,
            false,
            false,
        ),
    };
    ClosedPnlQueryOutcome::Verified(VerifiedCloseResolution {
        kind,
        sub_reason,
        verified_entry_price,
        verified_exit_price,
        closed_lots: final_lots,
        legs_count,
        exchange_closed_pnl_1e8: if has_pnl { Some(sum_pnl_1e8) } else { None },
    })
}

#[must_use]
pub fn resolve_fallback_unverified_close_core(
    ctx: &OpenExitContext,
    snap: &SymbolLobSnapshot,
    vac_level_hint: usize,
) -> VerifiedCloseResolution {
    let en_px = ctx.avg_price_ticks.max(PriceTicks(1));
    let eff_sl = if ctx.last_eff_sl_ticks.0 > 0 {
        ctx.last_eff_sl_ticks
    } else {
        ctx.sl_trigger_price
    };
    let ref_mk = if snap.mark_price.0 > 0 { snap.mark_price } else { en_px };
    let sl_crossed = eff_sl.0 > 0
        && match ctx.side {
            Side::Bid => ref_mk.0 <= eff_sl.0,
            Side::Ask => ref_mk.0 >= eff_sl.0,
        };
    let tot_lots = if ctx.tranche1_harvested {
        LotQty(ctx.initial_lots.0.max(ctx.open_lots.0).max(1))
    } else {
        LotQty(ctx.open_lots.0.max(1))
    };
    if sl_crossed {
        return VerifiedCloseResolution {
            kind: ClosedTradeKind::ServerMarkPriceStopLoss,
            sub_reason: ExitSubReason::WaldBaseStopLoss,
            verified_entry_price: en_px,
            verified_exit_price: eff_sl,
            closed_lots: tot_lots,
            legs_count: if ctx.tranche1_harvested { 2 } else { 1 },
            exchange_closed_pnl_1e8: None,
        };
    }
    let fb_exit = if ctx.resting_exit_price.0 > 0 {
        ctx.resting_exit_price
    } else if ctx.active_exit_price.0 > 0 {
        ctx.active_exit_price
    } else {
        defensive_zero_loss_maker_exit_price(ctx.side, en_px)
    };
    let gross_bps = ((fb_exit.0 - en_px.0)
        .saturating_mul(ctx.side.sign_i64())
        .saturating_mul(1_000_000))
        / en_px.0.max(1);
    let w_obs = snap.wave_observables();
    let aligned_fib = w_obs.fib_proj_bps.raw().saturating_mul(ctx.side.sign_i64()) > 0;
    VerifiedCloseResolution {
        kind: ClosedTradeKind::MakerPassiveUnwind,
        sub_reason: classify_maker_exit_sub_reason(
            gross_bps,
            vac_level_hint,
            ctx.tranche1_harvested,
            w_obs.climax_active,
            aligned_fib,
        ),
        verified_entry_price: en_px,
        verified_exit_price: fb_exit,
        closed_lots: tot_lots,
        legs_count: if ctx.tranche1_harvested { 2 } else { 1 },
        exchange_closed_pnl_1e8: None,
    }
}

#[inline]
const fn enforce_maker_bbo_bound(
    pos_side: Side,
    candidate_exit_px: PriceTicks,
    snap: &SymbolLobSnapshot,
) -> PriceTicks {
    match pos_side {
        Side::Bid => {
            let b0 = snap.bids[0].price.0;
            if b0 > 0 && candidate_exit_px.0 <= b0 {
                PriceTicks(b0.saturating_add(1))
            } else {
                candidate_exit_px
            }
        }
        Side::Ask => {
            let a0 = snap.asks[0].price.0;
            if a0 > 1 && candidate_exit_px.0 >= a0 {
                PriceTicks(a0 - 1)
            } else {
                PriceTicks(if candidate_exit_px.0 > 1 { candidate_exit_px.0 } else { 1 })
            }
        }
    }
}

#[inline]
const fn pick_more_ambitious_tp(pos_side: Side, a: PriceTicks, b: PriceTicks) -> PriceTicks {
    if b.0 <= 0 {
        return a;
    }
    if a.0 <= 0 {
        return b;
    }
    match pos_side {
        Side::Bid => {
            if b.0 > a.0 { b } else { a }
        }
        Side::Ask => {
            if b.0 < a.0 { b } else { a }
        }
    }
}

#[must_use]
pub fn compute_canonical_exit_quote_core(
    ctx: &OpenExitContext,
    snap: &SymbolLobSnapshot,
) -> Option<CanonicalExitQuote> {
    if ctx.avg_price_ticks.0 <= 0 || ctx.open_lots.0 <= 0 {
        return None;
    }
    let fee_rt = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
    let zero_loss_bps = fee_rt + 25;
    let dir_s = ctx.side.sign_f64();
    let d_sign = ctx.side.sign_i64();
    let own_flow = (0.55 * snap.factor_ofi.get() + 0.45 * snap.factor_tfi.get()) * dir_s;

    let bbo_exit_px = match ctx.side {
        Side::Bid => snap.asks[0].price,
        Side::Ask => snap.bids[0].price,
    };
    let ref_px = if bbo_exit_px.0 > 0 {
        bbo_exit_px
    } else if snap.mark_price.0 > 0 {
        snap.mark_price
    } else {
        snap.last_trade_price
    };
    let avg_denom = ctx.avg_price_ticks.0.max(1);
    let cur_gross_bps = if ref_px.0 > 0 {
        ((ref_px.0 - ctx.avg_price_ticks.0)
            .saturating_mul(d_sign)
            .saturating_mul(1_000_000))
            / avg_denom
    } else {
        0
    };
    let state_mfe = ((ctx.peak_favorable_price.0 - ctx.avg_price_ticks.0)
        .saturating_mul(d_sign)
        .saturating_mul(1_000_000))
        / avg_denom;
    let state_mae = ((ctx.worst_adverse_price.0 - ctx.avg_price_ticks.0)
        .saturating_mul(d_sign)
        .saturating_mul(1_000_000))
        / avg_denom;
    let best_mfe_bps = ctx.last_closed_mfe_bps.max(state_mfe).max(cur_gross_bps);
    let worst_mae_bps = ctx.last_closed_mae_bps.min(state_mae);

    let w_obs = snap.wave_observables();
    let wave_mod_opt =
        CausalElliottWaveState::evaluate_wave_entry_modulation(w_obs, ctx.side, own_flow).ok();
    let wave_def_mult = wave_mod_opt.map_or(1_000, |m| m.def_horizon_permille_mult);
    let wave_t1_adj = wave_mod_opt.map_or(0, |m| m.tranche1_permille_delta);
    let aligned_fib_rem_bps = w_obs.fib_proj_bps.raw().saturating_mul(d_sign).max(0);
    let aligned_phase = w_obs.macro_phase.get() * dir_s;

    let hold_s = snap.ts_local_ns.0.saturating_sub(ctx.opened_ts.0).max(0) / 1_000_000_000;
    let tau_s = ctx.intrinsic_tau_mkt_s.max(90);
    let wald_pct = if hold_s >= 20 && cur_gross_bps < fee_rt && own_flow < 0.05 {
        let time_ratio = ((hold_s * 100) / tau_s).clamp(0, 120);
        let flow_pen = ((-own_flow).max(0.0) * 28.0).round() as i64;
        ((time_ratio / 4) + flow_pen).clamp(0, 42)
    } else {
        0
    };

    let climax_harvest = (w_obs.climax_active || w_obs.regime == WaveRegime::ClimaxExhaustion)
        && aligned_phase > 0.0
        && (cur_gross_bps >= zero_loss_bps || best_mfe_bps >= OFFENSIVE_WIN_FLOOR_GROSS_BPS);

    let base_def_s = (tau_s / 4).max(45);
    let eff_def_s = ((base_def_s * wave_def_mult) / 1_000).clamp(30, 240);
    let sl_bps = PairSpec::elastic_server_sl_distance_bps(snap.realized_vol_5m).raw();
    let gamma_pm = (220 + ctx.learned_ir_x100.clamp(-40, 80)).clamp(180, 300);
    let mae_trig_bps = -((sl_bps * gamma_pm) / 1_000)
        .max(ctx.learned_ret_std_bps / 6)
        .max(450);
    let stale_hold =
        hold_s >= eff_def_s && cur_gross_bps < OFFENSIVE_WIN_FLOOR_GROSS_BPS && own_flow < 0.12;
    let defensive_lock_active = ctx.defensive_floor_locked
        || worst_mae_bps <= mae_trig_bps
        || stale_hold
        || climax_harvest;

    let eff_ratchet_bps = if best_mfe_bps >= (fee_rt * 139) / 100 {
        ctx.ratchet_locked_gross_bps
            .max(fee_rt + ((best_mfe_bps - fee_rt) * 72) / 100)
            .max(OFFENSIVE_WIN_FLOOR_GROSS_BPS)
    } else {
        ctx.ratchet_locked_gross_bps
    };
    let ratchet_pullback = eff_ratchet_bps >= OFFENSIVE_WIN_FLOOR_GROSS_BPS
        && ((own_flow <= -0.12) || (cur_gross_bps <= eff_ratchet_bps + 40 && own_flow < 0.25));

    if !ctx.tranche1_harvested && !defensive_lock_active && !ratchet_pullback {
        let urgency = if own_flow >= 0.45 { 0.50 } else { 0.65 };
        let win1_raw = offensive_win_maker_exit_price(ctx.side, ctx.avg_price_ticks, urgency);
        let win1_px = enforce_maker_bbo_bound(ctx.side, win1_raw, snap);
        let eff_t1_pm = (ctx.learned_t1_permille + wave_t1_adj.max(0)).clamp(350, 650);
        if let Some(t1_lots) =
            compute_tranche1_split_lots_core(ctx.slot_idx, ctx.open_lots, win1_px, eff_t1_pm)
        {
            return Some(CanonicalExitQuote {
                role: ExitOrderRole::Tranche1Harvest50,
                target_price: win1_px,
                order_lots: t1_lots,
                sub_reason: ExitSubReason::Tranche1Harvest50,
                vac_level: 1,
                defensive_lock_active: false,
                urgent_close_repeg: false,
                cur_gross_bps,
                wald_pct,
            });
        }
    }

    let role = if ctx.tranche1_harvested {
        ExitOrderRole::LvdbsDeepSurf50
    } else {
        ExitOrderRole::StandardMakerExit100
    };

    if defensive_lock_active || ratchet_pullback {
        let floor_bps = if climax_harvest {
            cur_gross_bps.max(eff_ratchet_bps).max(zero_loss_bps)
        } else if ratchet_pullback || eff_ratchet_bps >= OFFENSIVE_WIN_FLOOR_GROSS_BPS {
            eff_ratchet_bps
        } else {
            zero_loss_bps
        };
        let raw_px = if floor_bps <= zero_loss_bps {
            defensive_zero_loss_maker_exit_price(ctx.side, ctx.avg_price_ticks)
        } else {
            dynamic_maker_floor_price(ctx.side, ctx.avg_price_ticks, ScaledBps(floor_bps))
        };
        let target_price = enforce_maker_bbo_bound(ctx.side, raw_px, snap);
        let sub_reason = if climax_harvest {
            ExitSubReason::ClimaxExhaustionHarvest
        } else if eff_ratchet_bps >= OFFENSIVE_WIN_FLOOR_GROSS_BPS {
            ExitSubReason::ConvexMfeRatchetLock
        } else {
            ExitSubReason::AdverseScratchEscape
        };
        return Some(CanonicalExitQuote {
            role,
            target_price,
            order_lots: ctx.open_lots,
            sub_reason,
            vac_level: 1,
            defensive_lock_active,
            urgent_close_repeg: true,
            cur_gross_bps,
            wald_pct,
        });
    }

    let min_win_px = dynamic_maker_floor_price(
        ctx.side,
        ctx.avg_price_ticks,
        ScaledBps(eff_ratchet_bps.max(OFFENSIVE_WIN_FLOOR_GROSS_BPS)),
    );
    let base_tp = pick_more_ambitious_tp(ctx.side, min_win_px, ctx.structural_tp_price);

    let mut lvdbs_vac_level = 1_usize;
    let mut lvdbs_px = base_tp;
    if own_flow >= 0.35 {
        let (own_lvls, opp_lvls) = match ctx.side {
            Side::Bid => (&snap.bids, &snap.asks),
            Side::Ask => (&snap.asks, &snap.bids),
        };
        let own_l1_q = own_lvls[0].qty.0;
        let opp_l1_q = opp_lvls[0].qty.0.max(1);
        if own_l1_q > 0 && own_l1_q.saturating_mul(100) >= opp_l1_q.saturating_mul(115) {
            for lvl in 1..4 {
                let cand = opp_lvls[lvl];
                if cand.price.0 > 0
                    && cand.qty.0 > 0
                    && own_l1_q.saturating_mul(100) >= cand.qty.0.saturating_mul(120)
                {
                    lvdbs_vac_level = lvl + 1;
                    lvdbs_px = pick_more_ambitious_tp(ctx.side, lvdbs_px, cand.price);
                }
            }
        }
    }

    let mut fib_px = PriceTicks(0);
    if aligned_fib_rem_bps > 0
        && own_flow >= -0.08
        && matches!(w_obs.regime, WaveRegime::ImpulseWave3 | WaveRegime::ImpulseWave5)
    {
        let vol_f = snap.realized_vol_5m.raw().max(1_600) as f64;
        let cap_lo = (fee_rt * 4).max(ctx.learned_mfe_bps.min(fee_rt * 12));
        let max_fib_cap = ((vol_f * PHI_SQ).round() as i64).clamp(cap_lo, fee_rt * 45);
        let fib_gross = cur_gross_bps
            .max(0)
            .saturating_add(aligned_fib_rem_bps)
            .max(eff_ratchet_bps)
            .clamp(OFFENSIVE_WIN_FLOOR_GROSS_BPS, max_fib_cap);
        fib_px = dynamic_maker_floor_price(ctx.side, ctx.avg_price_ticks, ScaledBps(fib_gross));
    }

    let pre_fib_best = pick_more_ambitious_tp(ctx.side, base_tp, lvdbs_px);
    let combined_best = pick_more_ambitious_tp(ctx.side, pre_fib_best, fib_px);
    let fib_extended = fib_px.0 > 0 && combined_best == fib_px && fib_px != pre_fib_best;
    let target_price = enforce_maker_bbo_bound(ctx.side, combined_best, snap);
    let (vac_level, sub_reason) = if fib_extended {
        (lvdbs_vac_level.max(3), ExitSubReason::FibonacciImpulseSurf)
    } else if lvdbs_vac_level >= 2 {
        (lvdbs_vac_level, ExitSubReason::LvdbsDeepVacuumSurf)
    } else {
        (1, ExitSubReason::StandardStructuralTp)
    };

    Some(CanonicalExitQuote {
        role,
        target_price,
        order_lots: ctx.open_lots,
        sub_reason,
        vac_level,
        defensive_lock_active: false,
        urgent_close_repeg: false,
        cur_gross_bps,
        wald_pct,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lob_shm::BookLevel;
    use crate::types::BoundedScore;
    use crate::wave_elliott::WaveObservables;

    #[test]
    fn test_p0a_sl_breach_emergency_flatten_and_quantum_floors() {
        let q_bid = defensive_zero_loss_maker_exit_price(Side::Bid, PriceTicks(8_200));
        let q_ask = defensive_zero_loss_maker_exit_price(Side::Ask, PriceTicks(8_200));
        assert!(q_bid.0 > 8_200);
        assert!(q_ask.0 < 8_200);

        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = APPROVED_SPECS[3].symbol_id;
        snap.realized_vol_5m = ScaledBps(7_000);
        snap.mark_price = PriceTicks(8_200);
        snap.last_trade_price = PriceTicks(8_200);
        assert!(matches!(
            evaluate_sl_or_emergency_flatten_core(Side::Ask, PriceTicks(8_200), &snap),
            Ok(ServerSlDecision::ArmServerSl(_))
        ));

        snap.mark_price = PriceTicks(9_007);
        snap.last_trade_price = PriceTicks(9_008);
        let breach = evaluate_sl_or_emergency_flatten_core(Side::Ask, PriceTicks(8_030), &snap);
        assert!(matches!(
            breach,
            Ok(ServerSlDecision::BreachEmergencyFlatten { backup_sl, .. }) if backup_sl.0 >= 9_011
        ));
    }

    #[test]
    fn test_fibonacci_1618_ceiling_expansion_climax_harvest_and_mfe_ratchet() {
        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = APPROVED_SPECS[0].symbol_id;
        snap.ts_local_ns = TimestampNs(20_000_000_000);
        snap.realized_vol_5m = ScaledBps(6_500);
        snap.bids[0] = BookLevel { price: PriceTicks(759_380), qty: LotQty(350) };
        snap.asks[0] = BookLevel { price: PriceTicks(759_381), qty: LotQty(220) };
        snap.mark_price = PriceTicks(759_380);
        snap.last_trade_price = PriceTicks(759_380);
        snap.factor_ofi = BoundedScore::new_clamped(0.58);
        snap.factor_tfi = BoundedScore::new_clamped(0.64);
        snap.set_wave_observables(WaveObservables {
            regime: WaveRegime::ImpulseWave3,
            climax_active: false,
            hurst: WaveObservables::encode_hurst(0.64),
            macro_phase: BoundedScore::new_clamped(0.85),
            oi_impulse: BoundedScore::new_clamped(0.42),
            liq_imbalance: BoundedScore::new_clamped(0.55),
            fib_proj_bps: ScaledBps(4_500),
        });

        let mut ctx = OpenExitContext {
            slot_idx: 0,
            side: Side::Bid,
            avg_price_ticks: PriceTicks(759_000),
            peak_favorable_price: PriceTicks(759_380),
            worst_adverse_price: PriceTicks(759_000),
            initial_lots: LotQty(1),
            open_lots: LotQty(1),
            tranche1_harvested: false,
            tranche1_lots: LotQty(0),
            tranche1_exit_price: PriceTicks(0),
            defensive_floor_locked: false,
            active_exit_price: PriceTicks(759_494),
            structural_tp_price: PriceTicks(759_494),
            resting_exit_price: PriceTicks(759_494),
            sl_trigger_price: PriceTicks(753_000),
            opened_ts: TimestampNs(5_000_000_000),
            learned_mfe_bps: 680,
            learned_t1_permille: 500,
            learned_ir_x100: 45,
            learned_ret_std_bps: 200,
            intrinsic_tau_mkt_s: 180,
            last_eff_sl_ticks: PriceTicks(753_000),
            ratchet_locked_gross_bps: 0,
            last_closed_mfe_bps: 500,
            last_closed_mae_bps: 0,
        };

        let q_fib = compute_canonical_exit_quote_core(&ctx, &snap);
        assert!(q_fib.is_some());
        if let Some(qf) = q_fib {
            assert_eq!(qf.sub_reason, ExitSubReason::FibonacciImpulseSurf);
            assert!(qf.target_price.0 >= 762_700);
            assert!(!qf.defensive_lock_active);
        }

        ctx.peak_favorable_price = PriceTicks(762_188);
        ctx.last_closed_mfe_bps = 4_200;
        ctx.ratchet_locked_gross_bps = 3_136;
        snap.bids[0] = BookLevel { price: PriceTicks(762_187), qty: LotQty(300) };
        snap.asks[0] = BookLevel { price: PriceTicks(762_188), qty: LotQty(180) };
        snap.mark_price = PriceTicks(762_188);
        snap.last_trade_price = PriceTicks(762_188);
        snap.set_wave_observables(WaveObservables {
            regime: WaveRegime::ClimaxExhaustion,
            climax_active: true,
            hurst: WaveObservables::encode_hurst(0.68),
            macro_phase: BoundedScore::new_clamped(0.92),
            oi_impulse: BoundedScore::new_clamped(-0.30),
            liq_imbalance: BoundedScore::new_clamped(0.82),
            fib_proj_bps: ScaledBps(0),
        });
        let q_climax = compute_canonical_exit_quote_core(&ctx, &snap);
        assert!(q_climax.is_some());
        if let Some(qc) = q_climax {
            assert_eq!(qc.sub_reason, ExitSubReason::ClimaxExhaustionHarvest);
            assert!(qc.defensive_lock_active);
            assert!(qc.urgent_close_repeg);
            assert_eq!(qc.target_price, PriceTicks(762_188));
        }

        snap.set_wave_observables(WaveObservables {
            regime: WaveRegime::ImpulseWave3,
            climax_active: false,
            hurst: WaveObservables::encode_hurst(0.58),
            macro_phase: BoundedScore::new_clamped(0.75),
            oi_impulse: BoundedScore::new_clamped(0.10),
            liq_imbalance: BoundedScore::ZERO,
            fib_proj_bps: ScaledBps(3_000),
        });
        snap.factor_ofi = BoundedScore::new_clamped(-0.30);
        snap.factor_tfi = BoundedScore::new_clamped(-0.25);
        snap.bids[0] = BookLevel { price: PriceTicks(761_400), qty: LotQty(250) };
        snap.asks[0] = BookLevel { price: PriceTicks(761_401), qty: LotQty(320) };
        snap.mark_price = PriceTicks(761_400);
        snap.last_trade_price = PriceTicks(761_400);
        let q_ratchet = compute_canonical_exit_quote_core(&ctx, &snap);
        assert!(q_ratchet.is_some());
        if let Some(qr) = q_ratchet {
            assert_eq!(qr.sub_reason, ExitSubReason::ConvexMfeRatchetLock);
            assert!(qr.urgent_close_repeg);
            assert!(qr.target_price.0 >= 761_380);
        }
    }
}
