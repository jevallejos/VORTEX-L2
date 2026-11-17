//! Dimensional Newtypes, 12-Pair SSOT, Live VIP Fee Accounting (incl. Rebates) & Risk-Parity Kelly (V4.6).
#![allow(
    clippy::manual_range_contains,
    clippy::manual_clamp,
    clippy::suboptimal_flops,
    clippy::too_many_arguments
)]

use core::f64::consts::TAU;
use std::time::{SystemTime, UNIX_EPOCH};

pub use crate::fee_cadence_beta::*;

pub const ANSI_RESET: &str = "\x1b[0m";
pub const ANSI_GREEN: &str = "\x1b[1;38;5;118m";
pub const ANSI_DARK_GREEN: &str = "\x1b[1;38;5;28m";
pub const ANSI_SKY_BLUE: &str = "\x1b[1;38;5;51m";
pub const ANSI_ORANGE: &str = "\x1b[1;38;5;208m";
pub const ANSI_RED: &str = "\x1b[1;31m";
pub const ANSI_YELLOW: &str = "\x1b[1;33m";
pub const ANSI_GRAY: &str = "\x1b[38;5;245m";
pub const ANSI_BLUE: &str = "\x1b[1;36m";

pub const MAX_DIRECTIONAL_OPEN_SLOTS_PER_SIDE: usize = 3;
pub const MAX_PORTFOLIO_OPEN_SLOTS_TOTAL: usize = 6;
pub const OVERSIZED_LOT_MIN_SNR_ABS: f64 = 0.72;
pub const BYBIT_MIN_ORDER_NOTIONAL_1E8: i64 = 500_000_000;
pub const SPLIT_LEG_MIN_NOTIONAL_1E8: i64 = 525_000_000;

const UTC_DAY_SECS: f64 = 86_400.0;
const BYBIT_FUNDING_CYCLE_SECS: f64 = UTC_DAY_SECS / 3.0;

#[repr(transparent)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PriceTicks(pub i64);

#[repr(transparent)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LotQty(pub i64);

#[repr(transparent)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimestampNs(pub i64);

#[repr(transparent)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ScaledBps(pub i64);

impl ScaledBps {
    pub const BYBIT_VIP0_MAKER_FEE_BPS: Self = Self(200);
    pub const BYBIT_VIP0_TAKER_FEE_BPS: Self = Self(550);
    pub const MIN_REQUIRED_VOL_5M_BPS: Self = Self(
        3 * (Self::BYBIT_VIP0_MAKER_FEE_BPS.0
            + (95 * Self::BYBIT_VIP0_MAKER_FEE_BPS.0 + 5 * Self::BYBIT_VIP0_TAKER_FEE_BPS.0) / 100),
    );
    #[inline] #[must_use] pub const fn raw(self) -> i64 { self.0 }
}

#[repr(transparent)]
#[derive(Copy, Clone, Debug, Default, PartialEq)]
pub struct BoundedScore(f64);

impl BoundedScore {
    pub const ZERO: Self = Self(0.0);
    #[inline] #[must_use]
    pub fn new_clamped(val: f64) -> Self {
        if val.is_nan() { Self(0.0) } else { Self(val.clamp(-1.0, 1.0)) }
    }
    #[inline] #[must_use] pub const fn get(self) -> f64 { self.0 }
    #[inline] #[must_use] pub fn to_bps_i32(self) -> i32 { (self.0 * 10_000.0).round() as i32 }
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct TemporalCadenceModulation {
    pub intraday_percentile: f64,
    pub snr_threshold_delta: f64,
    pub kin_energy_min_delta_bps: i64,
    pub kelly_lot_mult_permille: i64,
}

impl Default for TemporalCadenceModulation {
    #[inline] fn default() -> Self { Self::neutral() }
}

impl TemporalCadenceModulation {
    #[inline] #[must_use]
    pub const fn neutral() -> Self {
        Self { intraday_percentile: 0.70, snr_threshold_delta: 0.0, kin_energy_min_delta_bps: 0, kelly_lot_mult_permille: 1_000 }
    }
}

#[must_use]
pub fn evaluate_utc_and_funding_cadence(
    ts_ns: TimestampNs,
    realized_vol_5m: ScaledBps,
    eq_vol_5m: ScaledBps,
) -> TemporalCadenceModulation {
    if ts_ns.0 <= 0 { return TemporalCadenceModulation::neutral(); }
    let ts_sec = (ts_ns.0 as f64) * 1e-9;
    let h_utc_hr = ts_sec.rem_euclid(UTC_DAY_SECS) / 3_600.0;
    let rad = TAU / 24.0;
    let k_asia = (4.8 * ((rad * (h_utc_hr - 1.25)).cos() - 1.0)).exp();
    let k_lon = (4.5 * ((rad * (h_utc_hr - 8.75)).cos() - 1.0)).exp();
    let k_nyse = (3.8 * ((rad * (h_utc_hr - 14.75)).cos() - 1.0)).exp();
    let h_score = (0.14 + 0.56 * k_asia + 0.72 * k_lon + 0.86 * k_nyse).clamp(0.14, 1.00);
    let half_fund = BYBIT_FUNDING_CYCLE_SECS * 0.5;
    let dt_fund = (ts_sec + half_fund).rem_euclid(BYBIT_FUNDING_CYCLE_SECS) - half_fund;
    let z_whip = (dt_fund + 75.0) / 110.0;
    let w_whip = (-0.5 * z_whip * z_whip).exp();
    let z_reb = (dt_fund - 420.0) / 240.0;
    let w_reb = (-0.5 * z_reb * z_reb).exp();
    let nu_vol = ((realized_vol_5m.raw().max(0) as f64) / (eq_vol_5m.raw().max(1) as f64)).max(0.40);
    let p_day = (0.65 * h_score + 0.35 * (0.85 * nu_vol).tanh()).clamp(0.05, 0.99);
    let offpeak_gap = (0.64 - p_day).max(0.0) * (-3.5 * (nu_vol - 1.10).max(0.0)).exp();
    let prime_surplus = (p_day - 0.72).max(0.0) + 0.12 * w_reb;
    let snr_threshold_delta = (0.22 * offpeak_gap + 0.065 * w_whip - 0.045 * prime_surplus).clamp(-0.025, 0.100);
    let fee_rt_i = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
    let raw_e = ((fee_rt_i as f64) * (0.32 * offpeak_gap + 0.15 * w_whip - 0.08 * prime_surplus)).round() as i64;
    let raw_k = (1_000.0 - 420.0 * offpeak_gap - 160.0 * w_whip + 180.0 * prime_surplus).round() as i64;
    TemporalCadenceModulation {
        intraday_percentile: p_day,
        snr_threshold_delta,
        kin_energy_min_delta_bps: raw_e.clamp(-(fee_rt_i / 25), fee_rt_i / 5),
        kelly_lot_mult_permille: raw_k.clamp(780, 1_080),
    }
}

#[repr(transparent)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SymbolId(pub u16);

impl SymbolId {
    #[inline] #[must_use]
    pub const fn name(self) -> &'static str {
        match self.0 {
            1 => "BTCUSDT", 2 => "ETHUSDT", 3 => "SOLUSDT", 4 => "AVAXUSDT",
            5 => "TAOUSDT", 6 => "SUIUSDT", 7 => "ORDIUSDT", 8 => "PENDLEUSDT",
            9 => "WIFUSDT", 10 => "LINKUSDT", 11 => "NEARUSDT", 12 => "LTCUSDT",
            _ => "UNKNOWN",
        }
    }
    #[inline] #[must_use]
    pub const fn slot_index(self) -> Option<usize> {
        if self.0 >= 1 && self.0 <= 12 { Some((self.0 - 1) as usize) } else { None }
    }
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum Side { Bid = 0, Ask = 1 }

impl Side {
    #[inline] #[must_use] pub const fn opposite(self) -> Self { match self { Self::Bid => Self::Ask, Self::Ask => Self::Bid } }
    #[inline] #[must_use] pub const fn sign_i64(self) -> i64 { match self { Self::Bid => 1, Self::Ask => -1 } }
    #[inline] #[must_use] pub const fn sign_f64(self) -> f64 { match self { Self::Bid => 1.0, Self::Ask => -1.0 } }
    #[inline] #[must_use]
    pub const fn select_entry_bbo_price(self, best_bid: PriceTicks, best_ask: PriceTicks) -> PriceTicks {
        match self {
            Self::Bid => if best_bid.0 > 0 { best_bid } else { best_ask },
            Self::Ask => if best_ask.0 > 0 { best_ask } else { best_bid },
        }
    }
}


#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VortexError {
    InvalidPriceOrTickSpec,
    AsciiDecimalParseError,
    TickBpsExceedsInstitutionalCap { observed_scaled_bps: ScaledBps, max_allowed_scaled_bps: ScaledBps },
    RealizedVolBelowStructuralFloor { observed_vol_5m_scaled_bps: ScaledBps, min_required_scaled_bps: ScaledBps },
    FrictionBarrierRejected { expected_excursion_scaled_bps: ScaledBps, required_3x_barrier_scaled_bps: ScaledBps },
    StopLossGeometryViolation { side: Side, proposed_sl_ticks: PriceTicks, reference_mark_ticks: PriceTicks },
    SharedMemorySeqConflict,
    InvalidSymbolSlot,
    BatchCapacityExceeded,
    ColumnarIoFailure,
    ColumnarFormatCorrupted,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct PairSpec {
    pub symbol_id: SymbolId,
    pub tick_size_1e8: i64,
    pub lot_step_1e8: i64,
    pub empirical_taker_exit_prob_pct: i64,
}

impl PairSpec {
    #[inline]
    pub const fn tick_to_price_scaled_bps(self, reference_price_ticks: PriceTicks) -> Result<ScaledBps, VortexError> {
        if reference_price_ticks.0 <= 0 || self.tick_size_1e8 <= 0 { return Err(VortexError::InvalidPriceOrTickSpec); }
        Ok(ScaledBps((1_000_000 + (reference_price_ticks.0 / 2)) / reference_price_ticks.0))
    }

    #[inline]
    pub const fn expected_round_trip_cost_bps(self, reference_price_ticks: PriceTicks) -> Result<ScaledBps, VortexError> {
        let raw_tick_bps = match self.tick_to_price_scaled_bps(reference_price_ticks) { Ok(v) => v, Err(e) => return Err(e) };
        let p_taker = if self.empirical_taker_exit_prob_pct < 0 { 0 } else if self.empirical_taker_exit_prob_pct > 100 { 100 } else { self.empirical_taker_exit_prob_pct };
        let exit_fee_expected = ((100 - p_taker) * ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.0 + p_taker * ScaledBps::BYBIT_VIP0_TAKER_FEE_BPS.0) / 100;
        Ok(ScaledBps(ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.0 + exit_fee_expected + raw_tick_bps.0))
    }

    #[inline]
    pub const fn required_3x_barrier_bps(self, reference_price_ticks: PriceTicks) -> Result<ScaledBps, VortexError> {
        match self.expected_round_trip_cost_bps(reference_price_ticks) { Ok(rt) => Ok(ScaledBps(rt.0.saturating_mul(3))), Err(e) => Err(e) }
    }

    #[inline]
    pub const fn endogenous_min_vol_5m_bps(self, reference_price_ticks: PriceTicks) -> Result<ScaledBps, VortexError> {
        self.required_3x_barrier_bps(reference_price_ticks)
    }

    #[inline]
    pub const fn validate_pair_eligibility(self, reference_price_ticks: PriceTicks, realized_vol_5m: ScaledBps) -> Result<ScaledBps, VortexError> {
        let raw_tick_bps = match self.tick_to_price_scaled_bps(reference_price_ticks) { Ok(v) => v, Err(e) => return Err(e) };
        let min_vol_req = match self.endogenous_min_vol_5m_bps(reference_price_ticks) { Ok(v) => v, Err(e) => return Err(e) };
        if realized_vol_5m.0 < min_vol_req.0 {
            return Err(VortexError::RealizedVolBelowStructuralFloor { observed_vol_5m_scaled_bps: realized_vol_5m, min_required_scaled_bps: min_vol_req });
        }
        let p_taker = if self.empirical_taker_exit_prob_pct < 0 { 0 } else if self.empirical_taker_exit_prob_pct > 100 { 100 } else { self.empirical_taker_exit_prob_pct };
        let tox_div = 10 + (if p_taker > 5 { p_taker - 5 } else { 0 }) * 4;
        let elastic_max_tick = ScaledBps(realized_vol_5m.0 / tox_div);
        if raw_tick_bps.0 > elastic_max_tick.0 {
            return Err(VortexError::TickBpsExceedsInstitutionalCap { observed_scaled_bps: raw_tick_bps, max_allowed_scaled_bps: elastic_max_tick });
        }
        Ok(raw_tick_bps)
    }

    #[inline]
    pub const fn passes_3x_friction_barrier(self, reference_price_ticks: PriceTicks, expected_excursion: ScaledBps) -> Result<ScaledBps, VortexError> {
        let required_3x = match self.required_3x_barrier_bps(reference_price_ticks) { Ok(v) => v, Err(e) => return Err(e) };
        if expected_excursion.0 < required_3x.0 {
            return Err(VortexError::FrictionBarrierRejected { expected_excursion_scaled_bps: expected_excursion, required_3x_barrier_scaled_bps: required_3x });
        }
        Ok(required_3x)
    }

    #[inline] #[must_use]
    pub const fn elastic_server_sl_distance_bps_with_fees(realized_vol_5m: ScaledBps, fees: FeeSchedule) -> ScaledBps {
        let v = if realized_vol_5m.0 > 0 { realized_vol_5m.0 } else { 0 };
        let sum_mt = fees.maker_fee_bps.0.saturating_add(fees.taker_fee_bps.0);
        let eff_mt = if sum_mt > 150 { sum_mt } else { 150 };
        let raw_ref = (eff_mt.saturating_mul(8)) / 15;
        let fee_rt = if raw_ref < 240 { 240 } else if raw_ref > 560 { 560 } else { raw_ref };
        let raw_bps = fee_rt * 11 + (v.saturating_mul(68)) / 100;
        let min_sl = (fee_rt * 29) / 2;
        let max_sl = (fee_rt * 125) / 4;
        ScaledBps(if raw_bps < min_sl { min_sl } else if raw_bps > max_sl { max_sl } else { raw_bps })
    }

    #[inline] #[must_use]
    pub const fn elastic_server_sl_distance_bps(realized_vol_5m: ScaledBps) -> ScaledBps {
        Self::elastic_server_sl_distance_bps_with_fees(realized_vol_5m, FeeSchedule::VIP0_DEFAULT)
    }

    #[inline]
    pub const fn validate_stop_loss_geometry(side: Side, proposed_sl: PriceTicks, last_price: PriceTicks, mark_price: PriceTicks) -> Result<PriceTicks, VortexError> {
        if proposed_sl.0 <= 0 || last_price.0 <= 0 || mark_price.0 <= 0 { return Err(VortexError::InvalidPriceOrTickSpec); }
        match side {
            Side::Bid => {
                let strict_bound = if last_price.0 < mark_price.0 { last_price.0 } else { mark_price.0 };
                let min_cushion = if strict_bound > 2 { 2 } else { 1 };
                if proposed_sl.0 > strict_bound - min_cushion {
                    return Err(VortexError::StopLossGeometryViolation { side, proposed_sl_ticks: proposed_sl, reference_mark_ticks: PriceTicks(strict_bound) });
                }
            }
            Side::Ask => {
                let strict_bound = if last_price.0 > mark_price.0 { last_price.0 } else { mark_price.0 };
                if proposed_sl.0 < strict_bound.saturating_add(2) {
                    return Err(VortexError::StopLossGeometryViolation { side, proposed_sl_ticks: proposed_sl, reference_mark_ticks: PriceTicks(strict_bound) });
                }
            }
        }
        Ok(proposed_sl)
    }

    #[inline] #[must_use]
    pub const fn lot_notional_1e8(self, lots: LotQty, price: PriceTicks) -> i64 {
        if lots.0 <= 0 || price.0 <= 0 || self.tick_size_1e8 <= 0 || self.lot_step_1e8 <= 0 { return 0; }
        let usdt_1e8 = ((lots.0 as i128) * (self.lot_step_1e8 as i128) * (price.0 as i128) * (self.tick_size_1e8 as i128)) / 100_000_000;
        if usdt_1e8 > (i64::MAX as i128) { i64::MAX } else { usdt_1e8 as i64 }
    }

    #[inline] #[must_use]
    pub const fn exchange_min_notional_lots(self, entry_price: PriceTicks) -> LotQty {
        let one_lot = self.lot_notional_1e8(LotQty(1), entry_price);
        if one_lot <= 0 { return LotQty(1); }
        let req = (BYBIT_MIN_ORDER_NOTIONAL_1E8 + one_lot - 1) / one_lot;
        LotQty(if req > 1 { req } else { 1 })
    }

    #[inline] #[must_use]
    pub const fn compute_leg_pnl_and_fee_1e8_with_schedule(
        self, side: Side, lots: LotQty, entry_px: PriceTicks, exit_px: PriceTicks, is_maker_exit: bool, fees: FeeSchedule,
    ) -> (i64, i64, ScaledBps, ScaledBps) {
        if lots.0 <= 0 || entry_px.0 <= 0 || exit_px.0 <= 0 { return (0, 0, ScaledBps(0), ScaledBps(0)); }
        let raw_diff = match side { Side::Bid => exit_px.0 - entry_px.0, Side::Ask => entry_px.0 - exit_px.0 };
        let gross_bps = (raw_diff.saturating_mul(1_000_000)) / entry_px.0;
        let in_fee_bps = fees.maker_fee_bps.0;
        let out_fee_bps = if is_maker_exit { fees.maker_fee_bps.0 } else { fees.taker_fee_bps.0 };
        let fee_bps = in_fee_bps.saturating_add(out_fee_bps);
        let net_bps = gross_bps.saturating_sub(fee_bps);
        let entry_notional = self.lot_notional_1e8(lots, entry_px) as i128;
        let exit_notional = self.lot_notional_1e8(lots, exit_px) as i128;
        let gross_pnl = match side { Side::Bid => exit_notional - entry_notional, Side::Ask => entry_notional - exit_notional };
        let f_num = entry_notional * (in_fee_bps as i128) + exit_notional * (out_fee_bps as i128);
        let fee_raw = if f_num >= 0 { (f_num + 500_000) / 1_000_000 } else { -((-f_num + 500_000) / 1_000_000) };
        let fee_1e8 = if fee_raw > (i64::MAX as i128) { i64::MAX } else if fee_raw < (i64::MIN as i128) { i64::MIN } else { fee_raw as i64 };
        let net_raw = gross_pnl - (fee_1e8 as i128);
        let net_pnl_1e8 = if net_raw > (i64::MAX as i128) { i64::MAX } else if net_raw < (i64::MIN as i128) { i64::MIN } else { net_raw as i64 };
        (net_pnl_1e8, fee_1e8, ScaledBps(gross_bps), ScaledBps(net_bps))
    }

    #[inline] #[must_use]
    pub const fn compute_leg_pnl_and_fee_1e8(
        self, side: Side, lots: LotQty, entry_px: PriceTicks, exit_px: PriceTicks, fee_scaled_bps: ScaledBps,
    ) -> (i64, i64, ScaledBps, ScaledBps) {
        if lots.0 <= 0 || entry_px.0 <= 0 || exit_px.0 <= 0 { return (0, 0, ScaledBps(0), ScaledBps(0)); }
        let raw_diff = match side { Side::Bid => exit_px.0 - entry_px.0, Side::Ask => entry_px.0 - exit_px.0 };
        let gross_bps = (raw_diff.saturating_mul(1_000_000)) / entry_px.0;
        let fee_bps = fee_scaled_bps.0;
        let net_bps = gross_bps.saturating_sub(fee_bps);
        let entry_notional = self.lot_notional_1e8(lots, entry_px) as i128;
        let exit_notional = self.lot_notional_1e8(lots, exit_px) as i128;
        let gross_pnl = match side { Side::Bid => exit_notional - entry_notional, Side::Ask => entry_notional - exit_notional };
        let in_fee_bps = if fee_bps > ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.0 * 2 {
            ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.0 as i128
        } else {
            (fee_bps / 2) as i128
        };
        let out_fee_bps = (fee_bps as i128) - in_fee_bps;
        let f_num = entry_notional * in_fee_bps + exit_notional * out_fee_bps;
        let fee_raw = if f_num >= 0 { (f_num + 500_000) / 1_000_000 } else { -((-f_num + 500_000) / 1_000_000) };
        let fee_1e8 = if fee_raw > (i64::MAX as i128) { i64::MAX } else if fee_raw < (i64::MIN as i128) { i64::MIN } else { fee_raw as i64 };
        let net_raw = gross_pnl - (fee_1e8 as i128);
        let net_pnl_1e8 = if net_raw > (i64::MAX as i128) { i64::MAX } else if net_raw < (i64::MIN as i128) { i64::MIN } else { net_raw as i64 };
        (net_pnl_1e8, fee_1e8, ScaledBps(gross_bps), ScaledBps(net_bps))
    }

    #[inline] #[must_use]
    pub const fn all_in_sl_loss_ppm_with_fees(self, entry_price: PriceTicks, realized_vol_5m: ScaledBps, fees: FeeSchedule) -> i64 {
        let sl_bps = Self::elastic_server_sl_distance_bps_with_fees(realized_vol_5m, fees).0;
        let sum_mt = fees.maker_fee_bps.0.saturating_add(fees.taker_fee_bps.0);
        let fee_sl = if sum_mt > 50 { sum_mt } else { 50 };
        let tick_bps = match self.tick_to_price_scaled_bps(entry_price) { Ok(v) => v.0, Err(_) => 50 };
        sl_bps.saturating_add(fee_sl).saturating_add(tick_bps)
    }

    #[inline] #[must_use]
    pub const fn all_in_sl_loss_ppm(self, entry_price: PriceTicks, realized_vol_5m: ScaledBps) -> i64 {
        self.all_in_sl_loss_ppm_with_fees(entry_price, realized_vol_5m, FeeSchedule::VIP0_DEFAULT)
    }

    #[inline] #[must_use]
    pub const fn base_slot_risk_budget_1e8(wallet_usdt_1e8: i64) -> i64 {
        let w_eff = if wallet_usdt_1e8 < 5_000_000_000 { 5_000_000_000 } else if wallet_usdt_1e8 > 10_000_000_000_000 { 10_000_000_000_000 } else { wallet_usdt_1e8 };
        let risk_ppm = (MAX_PORTFOLIO_OPEN_SLOTS_TOTAL as i128) * ((ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.0 * 2) as i128);
        (((w_eff as i128) * risk_ppm) / 1_000_000) as i64
    }

    #[inline] #[must_use]
    pub const fn base_kelly_notional_1e8(wallet_usdt_1e8: i64) -> i64 {
        let w_c = if wallet_usdt_1e8 < 5_000_000_000 { 5_000_000_000 } else if wallet_usdt_1e8 > 1_000_000_000_000 { 1_000_000_000_000 } else { wallet_usdt_1e8 };
        let raw_n = ((w_c as i128 * 29) / 100) as i64;
        if raw_n < 1_800_000_000 { 1_800_000_000 } else if raw_n > 25_000_000_000 { 25_000_000_000 } else { raw_n }
    }

    #[inline] #[must_use]
    pub const fn requires_oversized_lot_conviction_with_vol_and_fees(
        self, wallet_usdt_1e8: i64, entry_price: PriceTicks, realized_vol_5m: ScaledBps, fees: FeeSchedule,
    ) -> bool {
        let min_lots = self.exchange_min_notional_lots(entry_price);
        let min_notional = self.lot_notional_1e8(min_lots, entry_price) as i128;
        let all_in_ppm = self.all_in_sl_loss_ppm_with_fees(entry_price, realized_vol_5m, fees);
        let safe_ppm = if all_in_ppm > 1 { all_in_ppm as i128 } else { 1 };
        let min_lot_loss = (min_notional * safe_ppm) / 1_000_000;
        let base_risk = Self::base_slot_risk_budget_1e8(wallet_usdt_1e8);
        let safe_risk = if base_risk > 1 { base_risk as i128 } else { 1 };
        min_lot_loss * 5 > safe_risk * 11
    }

    #[inline] #[must_use]
    pub const fn requires_oversized_lot_conviction_with_vol(self, wallet_usdt_1e8: i64, entry_price: PriceTicks, realized_vol_5m: ScaledBps) -> bool {
        self.requires_oversized_lot_conviction_with_vol_and_fees(wallet_usdt_1e8, entry_price, realized_vol_5m, FeeSchedule::VIP0_DEFAULT)
    }

    #[inline] #[must_use]
    pub const fn requires_oversized_lot_conviction(self, wallet_usdt_1e8: i64, entry_price: PriceTicks) -> bool {
        self.requires_oversized_lot_conviction_with_vol(wallet_usdt_1e8, entry_price, ScaledBps(ScaledBps::MIN_REQUIRED_VOL_5M_BPS.0 * 5))
    }

    #[inline] #[must_use]
    pub fn passes_oversized_lot_conviction_guard_with_vol_and_fees(
        self, wallet_usdt_1e8: i64, entry_price: PriceTicks, realized_vol_5m: ScaledBps, composite_snr_abs: f64, in_local_slot_scar_debt: bool, fees: FeeSchedule,
    ) -> bool {
        if !self.requires_oversized_lot_conviction_with_vol_and_fees(wallet_usdt_1e8, entry_price, realized_vol_5m, fees) { return true; }
        if in_local_slot_scar_debt { return false; }
        let min_lots = self.exchange_min_notional_lots(entry_price);
        let min_notional = self.lot_notional_1e8(min_lots, entry_price) as f64;
        let all_in_ppm = (self.all_in_sl_loss_ppm_with_fees(entry_price, realized_vol_5m, fees).max(1)) as f64;
        let rho_over = ((min_notional * all_in_ppm) * 1e-6) / (Self::base_slot_risk_budget_1e8(wallet_usdt_1e8).max(1) as f64);
        let excess = (rho_over - 2.20).max(0.0);
        composite_snr_abs >= (0.66 + 0.045 * (excess / 1.80).tanh() + 0.010 * excess).min(0.82)
    }

    #[inline] #[must_use]
    pub fn passes_oversized_lot_conviction_guard_with_vol(
        self, wallet_usdt_1e8: i64, entry_price: PriceTicks, realized_vol_5m: ScaledBps, composite_snr_abs: f64, in_local_slot_scar_debt: bool,
    ) -> bool {
        self.passes_oversized_lot_conviction_guard_with_vol_and_fees(wallet_usdt_1e8, entry_price, realized_vol_5m, composite_snr_abs, in_local_slot_scar_debt, FeeSchedule::VIP0_DEFAULT)
    }

    #[inline] #[must_use]
    pub fn passes_oversized_lot_conviction_guard(self, wallet_usdt_1e8: i64, entry_price: PriceTicks, composite_snr_abs: f64, in_local_slot_scar_debt: bool) -> bool {
        self.passes_oversized_lot_conviction_guard_with_vol(wallet_usdt_1e8, entry_price, ScaledBps(ScaledBps::MIN_REQUIRED_VOL_5M_BPS.0 * 5), composite_snr_abs, in_local_slot_scar_debt)
    }

    #[inline] #[must_use]
    pub fn characteristic_lot_scale(self, reference_price: PriceTicks) -> f64 {
        if reference_price.0 <= 0 || self.tick_size_1e8 <= 0 || self.lot_step_1e8 <= 0 { return 1.0; }
        (250_000_000.0 / (self.lot_notional_1e8(LotQty(1), reference_price).max(1) as f64)).clamp(0.25, 500.0)
    }

    #[inline] #[must_use]
    pub const fn min_resting_l1_lots_dynamic(
        self,
        entry_price: PriceTicks,
        realized_vol_5m: ScaledBps,
        taker_prob_pm: i64,
        fees: crate::fee_cadence_beta::FeeSchedule,
    ) -> LotQty {
        let one_lot_1e8 = self.lot_notional_1e8(LotQty(1), entry_price);
        if one_lot_1e8 <= 0 { return LotQty(1); }
        let b3x_raw = self.three_x_friction_barrier_bps_dynamic(entry_price, taker_prob_pm, fees).0;
        let b3x = if b3x_raw > 0 { b3x_raw } else { ScaledBps::MIN_REQUIRED_VOL_5M_BPS.0 };
        let v_eff = if realized_vol_5m.0 > b3x { realized_vol_5m.0 } else { b3x };
        let min_sup = ((v_eff as i128) * (b3x as i128) * 14) / 1_000;
        let raw = ((min_sup + (one_lot_1e8 as i128) - 1) / (one_lot_1e8 as i128)) as i64;
        LotQty(if raw < 1 { 1 } else { raw })
    }

    #[inline] #[must_use]
    pub const fn min_resting_l1_lots(self, entry_price: PriceTicks, realized_vol_5m: ScaledBps) -> LotQty {
        self.min_resting_l1_lots_dynamic(
            entry_price,
            realized_vol_5m,
            50,
            crate::fee_cadence_beta::FeeSchedule::VIP0_DEFAULT,
        )
    }

    #[inline] #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn passes_l1_queue_depth_and_wall_guard_dynamic(
        self,
        entry_price: PriceTicks,
        realized_vol_5m: ScaledBps,
        learned_edge_bps_x100: i64,
        own_l1_lots: LotQty,
        opp_l1_lots: LotQty,
        own_deep_l1_l5_lots: LotQty,
        ofi_abs: f64,
        tfi_abs: f64,
        taker_prob_pm: i64,
        fees: crate::fee_cadence_beta::FeeSchedule,
    ) -> bool {
        if own_l1_lots.0 <= 0 || entry_price.0 <= 0 || self.lot_notional_1e8(own_l1_lots, entry_price) < 2_400_000_000 { return false; }
        if own_deep_l1_l5_lots.0.max(own_l1_lots.0) < self.min_resting_l1_lots_dynamic(entry_price, realized_vol_5m, taker_prob_pm, fees).0 { return false; }
        let Ok(tau_tick) = self.tick_to_price_scaled_bps(entry_price) else { return false };
        let diffusion_ticks = (realized_vol_5m.0.max(ScaledBps::MIN_REQUIRED_VOL_5M_BPS.0) as f64) / (tau_tick.0.max(1) as f64);
        let tfi_c = tfi_abs.clamp(0.0, 1.0);
        let ofi_c = ofi_abs.clamp(0.0, 1.0);
        let max_q_1e8 = (1_250_000_000.0 * diffusion_ticks * (12.0 + 10.0 * tfi_c + ((learned_edge_bps_x100.max(0) as f64) / 60.0).min(8.0))).round() as i128;
        let own_l1_1e8 = self.lot_notional_1e8(own_l1_lots, entry_price) as i128;
        if own_l1_1e8 > max_q_1e8 { return false; }
        if own_l1_1e8 * 100 <= max_q_1e8 * 55 || tfi_c >= 0.65 { return true; }
        let tot_q = (own_l1_lots.0.saturating_add(opp_l1_lots.0.max(0))).max(1);
        let max_q_pm = (780 + (((tfi_c - ofi_c).max(0.0) * 140.0) as i64)).min(920);
        (own_l1_lots.0.saturating_mul(1_000)) / tot_q <= max_q_pm
    }

    #[inline] #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn passes_l1_queue_depth_and_wall_guard(
        self, entry_price: PriceTicks, realized_vol_5m: ScaledBps, learned_edge_bps_x100: i64,
        own_l1_lots: LotQty, opp_l1_lots: LotQty, own_deep_l1_l5_lots: LotQty, ofi_abs: f64, tfi_abs: f64,
    ) -> bool {
        self.passes_l1_queue_depth_and_wall_guard_dynamic(
            entry_price, realized_vol_5m, learned_edge_bps_x100,
            own_l1_lots, opp_l1_lots, own_deep_l1_l5_lots, ofi_abs, tfi_abs,
            50, crate::fee_cadence_beta::FeeSchedule::VIP0_DEFAULT,
        )
    }

    #[inline] #[must_use]
    pub const fn compute_risk_parity_kelly_lots_with_fees(
        self, wallet_usdt_1e8: i64, entry_price: PriceTicks, realized_vol_5m: ScaledBps,
        learned_edge_bps_x100: i64, info_ratio_x100: i64, cadence_mult_permille: i64, in_scar_debt: bool, fees: FeeSchedule,
    ) -> LotQty {
        let one_lot_1e8 = self.lot_notional_1e8(LotQty(1), entry_price);
        if one_lot_1e8 <= 0 { return LotQty(1); }
        let w_eff = if wallet_usdt_1e8 < 5_000_000_000 { 5_000_000_000 } else if wallet_usdt_1e8 > 10_000_000_000_000 { 10_000_000_000_000 } else { wallet_usdt_1e8 };
        let base_risk = Self::base_slot_risk_budget_1e8(w_eff) as i128;
        let kappa_pm = if in_scar_debt {
            550_i64
        } else {
            let edge_exc = if learned_edge_bps_x100 > 150 { learned_edge_bps_x100 - 150 } else { 0 };
            let ir_adj = if info_ratio_x100 > 50 { ((info_ratio_x100 - 50) * 3) / 2 } else if info_ratio_x100 < 0 { info_ratio_x100 * 2 } else { 0 };
            let raw_k = 1_000 + edge_exc + ir_adj;
            if raw_k < 700 { 700 } else if raw_k > 1_450 { 1_450 } else { raw_k }
        };
        let cad_c = if cadence_mult_permille < 550 { 550 } else if cadence_mult_permille > 1_200 { 1_200 } else { cadence_mult_permille };
        let adj_risk = (((base_risk * (kappa_pm as i128)) / 1_000) * (cad_c as i128)) / 1_000;
        let all_in_ppm = self.all_in_sl_loss_ppm_with_fees(entry_price, realized_vol_5m, fees);
        let raw_notional = (adj_risk * 1_000_000) / (if all_in_ppm > 1 { all_in_ppm as i128 } else { 1 });
        let min_fl = ((BYBIT_MIN_ORDER_NOTIONAL_1E8 as i128) * 12) / 5;
        let max_sl = ((w_eff as i128) * 42) / 100;
        let eff_notional_1e8 = if raw_notional < min_fl { min_fl } else if raw_notional > max_sl { max_sl } else { raw_notional };
        let min_exch = self.exchange_min_notional_lots(entry_price).0;
        if (one_lot_1e8 as i128) > eff_notional_1e8 {
            let single = (eff_notional_1e8 / (one_lot_1e8 as i128)) as i64;
            return LotQty(if single > min_exch { single } else { min_exch });
        }
        let raw_lots = ((eff_notional_1e8 + (one_lot_1e8 as i128) / 2) / (one_lot_1e8 as i128)) as i64;
        let bounded = if raw_lots < min_exch { min_exch } else { raw_lots };
        if bounded <= 1 && min_exch == 1 && ((one_lot_1e8 * 2) as i128) > eff_notional_1e8 { return LotQty(1); }
        let split_min = if bounded < 2 { 2 } else { bounded };
        LotQty(if (split_min & 1) != 0 { split_min + 1 } else { split_min })
    }

    #[inline] #[must_use]
    pub const fn compute_risk_parity_kelly_lots(
        self, wallet_usdt_1e8: i64, entry_price: PriceTicks, realized_vol_5m: ScaledBps,
        learned_edge_bps_x100: i64, info_ratio_x100: i64, cadence_mult_permille: i64, in_scar_debt: bool,
    ) -> LotQty {
        self.compute_risk_parity_kelly_lots_with_fees(
            wallet_usdt_1e8, entry_price, realized_vol_5m, learned_edge_bps_x100,
            info_ratio_x100, cadence_mult_permille, in_scar_debt, FeeSchedule::VIP0_DEFAULT,
        )
    }

    #[inline] #[must_use]
    pub const fn compute_bayesian_kelly_lots(self, wallet_usdt_1e8: i64, entry_price: PriceTicks, learned_edge_bps_x100: i64, info_ratio_x100: i64, in_scar_debt: bool) -> LotQty {
        self.compute_risk_parity_kelly_lots(wallet_usdt_1e8, entry_price, ScaledBps(ScaledBps::MIN_REQUIRED_VOL_5M_BPS.0 * 5), learned_edge_bps_x100, info_ratio_x100, 1_000, in_scar_debt)
    }

    #[inline] #[must_use]
    pub const fn compute_compounding_kelly_lots(self, wallet_usdt_1e8: i64, entry_price: PriceTicks, learned_edge_bps_x100: i64, in_scar_debt: bool) -> LotQty {
        self.compute_bayesian_kelly_lots(wallet_usdt_1e8, entry_price, learned_edge_bps_x100, 50, in_scar_debt)
    }

    #[inline] #[must_use]
    pub fn format_lot_qty_str(self, lots: LotQty) -> String {
        let sc = (lots.0.max(1) as i128) * (self.lot_step_1e8.max(1) as i128);
        let wh = sc / 100_000_000;
        let fr = (sc % 100_000_000).unsigned_abs();
        if fr == 0 { format!("{wh}") } else { let s_f = format!("{fr:08}"); format!("{wh}.{}", s_f.trim_end_matches('0')) }
    }

    #[inline] #[must_use]
    pub fn format_price_ticks_str(self, ticks: PriceTicks) -> String {
        let sc = (ticks.0.max(1) as i128) * (self.tick_size_1e8.max(1) as i128);
        let wh = sc / 100_000_000;
        let fr = (sc % 100_000_000).unsigned_abs();
        if fr == 0 { format!("{wh}") } else { let s_f = format!("{fr:08}"); format!("{wh}.{}", s_f.trim_end_matches('0')) }
    }

    #[inline] #[must_use]
    pub const fn format_price_usdt_parts(self, ticks: PriceTicks) -> (i64, i64) {
        let sc = (ticks.0 as i128) * (self.tick_size_1e8 as i128);
        ((sc / 100_000_000) as i64, ((sc.abs() % 100_000_000) / 10_000) as i64)
    }

    pub fn parse_price_ticks(self, decimal_str: &str) -> Result<PriceTicks, VortexError> {
        let s = decimal_str.trim();
        if s.is_empty() || self.tick_size_1e8 <= 0 { return Err(VortexError::AsciiDecimalParseError); }
        let (mut whole, mut frac, mut frac_digits, mut in_frac) = (0_i128, 0_i128, 0_u32, false);
        for b in s.bytes() {
            if b == b'.' { if in_frac { return Err(VortexError::AsciiDecimalParseError); } in_frac = true; continue; }
            if !b.is_ascii_digit() { return Err(VortexError::AsciiDecimalParseError); }
            let d = i128::from(b - b'0');
            if !in_frac { whole = whole.saturating_mul(10).saturating_add(d); }
            else if frac_digits < 8 { frac = frac.saturating_mul(10).saturating_add(d); frac_digits += 1; }
        }
        while frac_digits < 8 { frac = frac.saturating_mul(10); frac_digits += 1; }
        let tick = self.tick_size_1e8 as i128;
        let ticks = (whole.saturating_mul(100_000_000).saturating_add(frac) + (tick / 2)) / tick;
        if ticks <= 0 { Err(VortexError::InvalidPriceOrTickSpec) } else { Ok(PriceTicks(ticks as i64)) }
    }

    #[must_use]
    pub fn parse_lot_qty(self, decimal_str: &str) -> LotQty {
        let s = decimal_str.trim();
        if s.is_empty() || self.lot_step_1e8 <= 0 { return LotQty(0); }
        let (mut whole, mut frac, mut frac_digits, mut in_frac) = (0_i128, 0_i128, 0_u32, false);
        for b in s.bytes() {
            if b == b'.' { if in_frac { return LotQty(0); } in_frac = true; continue; }
            if !b.is_ascii_digit() { return LotQty(0); }
            let d = i128::from(b - b'0');
            if !in_frac { whole = whole.saturating_mul(10).saturating_add(d); }
            else if frac_digits < 8 { frac = frac.saturating_mul(10).saturating_add(d); frac_digits += 1; }
        }
        while frac_digits < 8 { frac = frac.saturating_mul(10); frac_digits += 1; }
        let step = self.lot_step_1e8 as i128;
        LotQty(((whole.saturating_mul(100_000_000).saturating_add(frac) + (step / 2)) / step) as i64)
    }
}

pub const APPROVED_SPECS: [PairSpec; 12] = [
    PairSpec { symbol_id: SymbolId(1), tick_size_1e8: 10_000_000, lot_step_1e8: 100_000, empirical_taker_exit_prob_pct: 5 },
    PairSpec { symbol_id: SymbolId(2), tick_size_1e8: 1_000_000, lot_step_1e8: 1_000_000, empirical_taker_exit_prob_pct: 5 },
    PairSpec { symbol_id: SymbolId(3), tick_size_1e8: 1_000_000, lot_step_1e8: 10_000_000, empirical_taker_exit_prob_pct: 5 },
    PairSpec { symbol_id: SymbolId(4), tick_size_1e8: 100_000, lot_step_1e8: 10_000_000, empirical_taker_exit_prob_pct: 5 },
    PairSpec { symbol_id: SymbolId(5), tick_size_1e8: 1_000_000, lot_step_1e8: 1_000_000, empirical_taker_exit_prob_pct: 5 },
    PairSpec { symbol_id: SymbolId(6), tick_size_1e8: 10_000, lot_step_1e8: 100_000_000, empirical_taker_exit_prob_pct: 5 },
    PairSpec { symbol_id: SymbolId(7), tick_size_1e8: 100_000, lot_step_1e8: 10_000_000, empirical_taker_exit_prob_pct: 5 },
    PairSpec { symbol_id: SymbolId(8), tick_size_1e8: 10_000, lot_step_1e8: 100_000_000, empirical_taker_exit_prob_pct: 5 },
    PairSpec { symbol_id: SymbolId(9), tick_size_1e8: 10_000, lot_step_1e8: 100_000_000, empirical_taker_exit_prob_pct: 5 },
    PairSpec { symbol_id: SymbolId(10), tick_size_1e8: 100_000, lot_step_1e8: 10_000_000, empirical_taker_exit_prob_pct: 5 },
    PairSpec { symbol_id: SymbolId(11), tick_size_1e8: 100_000, lot_step_1e8: 10_000_000, empirical_taker_exit_prob_pct: 5 },
    PairSpec { symbol_id: SymbolId(12), tick_size_1e8: 1_000_000, lot_step_1e8: 10_000_000, empirical_taker_exit_prob_pct: 5 },
];

#[inline] #[must_use] pub const fn spec_from_symbol_id(id: SymbolId) -> Option<PairSpec> { match id.slot_index() { Some(idx) => Some(APPROVED_SPECS[idx]), None => None } }
#[inline] #[must_use] pub fn slot_from_symbol_name(name: &str) -> Option<usize> { APPROVED_SPECS.iter().position(|s| s.symbol_id.name() == name) }
#[inline] #[must_use] pub fn side_from_str(s: &str) -> Side { if s == "Ask" || s == "Sell" { Side::Ask } else { Side::Bid } }
#[inline] #[must_use]
pub fn classify_pnl_badge(net_bps: f64) -> (&'static str, &'static str, &'static str) {
    if net_bps >= 1.00 { (ANSI_GREEN, "\u{1f7e2}", "WIN    ") }
    else if net_bps >= -0.50 { (ANSI_SKY_BLUE, "\u{1fa75}", "SCRATCH") }
    else { (ANSI_RED, "\u{1f534}", "LOSS   ") }
}
#[inline] #[must_use]
pub fn classify_trade_badge(net_bps: f64, is_partial_harvest: bool) -> (&'static str, &'static str, &'static str) {
    if is_partial_harvest { (ANSI_DARK_GREEN, "\u{1f332}", "PARTIAL") } else { classify_pnl_badge(net_bps) }
}
#[inline] #[must_use]
pub fn now_epoch_ns() -> TimestampNs {
    match SystemTime::now().duration_since(UNIX_EPOCH) { Ok(d) => TimestampNs(d.as_nanos() as i64), Err(_) => TimestampNs(0) }
}
#[must_use]
pub fn visible_terminal_width(s: &str) -> usize {
    let mut width = 0_usize;
    let mut chars = s.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\x1b' {
            if chars.peek() == Some(&'[') { let _ = chars.next(); for c in chars.by_ref() { if c.is_ascii_alphabetic() { break; } } }
            continue;
        }
        let cp = ch as u32;
        if cp == 0xFE0F { continue; }
        width += if (0x1F300..=0x1FAFF).contains(&cp) || (0x2600..=0x27BF).contains(&cp) { 2 } else { 1 };
    }
    width
}
pub fn print_auto_aligned_box(sections: &[Vec<String>]) {
    let mut max_w = 0_usize;
    for sec in sections {
        for line in sec {
            let w = visible_terminal_width(line);
            if w > max_w { max_w = w; }
        }
    }
    let horiz = "\u{2550}".repeat(max_w + 2);
    println!("\u{2554}{horiz}\u{2557}");
    for (s_idx, sec) in sections.iter().enumerate() {
        if s_idx > 0 { println!("\u{2560}{horiz}\u{2563}"); }
        for line in sec { println!("\u{2551} {}{} \u{2551}", line, " ".repeat(max_w.saturating_sub(visible_terminal_width(line)))); }
    }
    println!("\u{255a}{horiz}\u{255d}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_12_specs_oversized_guard_and_sl_geometry() {
        let sample_ticks: [i64; 12] = [759_000, 223_700, 8_930, 8_170, 22_400, 8_510, 3_130, 16_140, 1_598, 10_720, 3_690, 5_176];
        for (idx, spec) in APPROVED_SPECS.iter().enumerate() {
            assert!(spec.validate_pair_eligibility(PriceTicks(sample_ticks[idx]), ScaledBps(6_500)).is_ok());
        }
        let btc = APPROVED_SPECS[0];
        assert!(btc.requires_oversized_lot_conviction(6_628_080_000, PriceTicks(759_000)));
        assert!(PairSpec::validate_stop_loss_geometry(Side::Bid, PriceTicks(8_197), PriceTicks(8_198), PriceTicks(8_199)).is_err());
    }

    #[test]
    fn test_v46_negative_maker_rebates_and_kelly_550_to_1200_range() {
        let btc = APPROVED_SPECS[0];
        let avax = APPROVED_SPECS[3];
        let (pnl_vip0, fee_vip0, _, _) = btc.compute_leg_pnl_and_fee_1e8(Side::Bid, LotQty(1), PriceTicks(759_000), PriceTicks(759_600), ScaledBps(400));
        assert_eq!((pnl_vip0, fee_vip0), (2_962_800, 3_037_200));
        let mm_fees = FeeSchedule::from_live_rates_clamped(ScaledBps(-50), ScaledBps(325));
        let (pnl_reb, fee_reb, g_reb, n_reb) = btc.compute_leg_pnl_and_fee_1e8_with_schedule(
            Side::Bid, LotQty(1), PriceTicks(759_000), PriceTicks(759_600), true, mm_fees,
        );
        assert!(fee_reb < 0 && pnl_reb > 6_000_000 && n_reb.raw() == g_reb.raw() + 100);

        let w_large = 50_000_000_000_i64;
        let lots_550 = avax.compute_risk_parity_kelly_lots_with_fees(w_large, PriceTicks(8_170), ScaledBps(5_000), 250, 50, 550, false, mm_fees);
        let lots_780 = avax.compute_risk_parity_kelly_lots_with_fees(w_large, PriceTicks(8_170), ScaledBps(5_000), 250, 50, 780, false, mm_fees);
        let lots_1200 = avax.compute_risk_parity_kelly_lots_with_fees(w_large, PriceTicks(8_170), ScaledBps(5_000), 250, 50, 1_200, false, mm_fees);
        assert!(lots_550.0 < lots_780.0 && lots_1200.0 > lots_780.0 && lots_550.0 % 2 == 0 && lots_1200.0 % 2 == 0);
    }
}

impl PairSpec {
    /// Dimensionless quantum lot distortion ratio `rho_quant >= 1.0` (`min_lot_sl_loss / base_slot_risk`).
    #[inline]
    #[must_use]
    pub fn quantum_lot_distortion_ratio(
        self,
        wallet_usdt_1e8: i64,
        entry_price: PriceTicks,
        realized_vol_5m: ScaledBps,
        fees: FeeSchedule,
    ) -> f64 {
        let min_lots = self.exchange_min_notional_lots(entry_price);
        let min_notional = self.lot_notional_1e8(min_lots, entry_price) as f64;
        let all_in_ppm = (self.all_in_sl_loss_ppm_with_fees(entry_price, realized_vol_5m, fees).max(1)) as f64;
        let min_lot_loss = (min_notional * all_in_ppm) * 1e-6;
        let base_risk = (Self::base_slot_risk_budget_1e8(wallet_usdt_1e8).max(1)) as f64;
        (min_lot_loss / base_risk).max(1.0)
    }

    /// Quantum-penalized geometric drift in ScaledBps (`1 bp = 100`) using live `p_Taker`, `MFE` & `FeeSchedule`.
    #[inline]
    #[must_use]
    pub fn quantum_ev_net_drift_bps(
        self,
        wallet_usdt_1e8: i64,
        entry_price: PriceTicks,
        realized_vol_5m: ScaledBps,
        learned_mfe_bps: ScaledBps,
        taker_prob_pm: i64,
        fees: FeeSchedule,
    ) -> f64 {
        const PHI_Q: f64 = 1.618_033_988_749_895;
        const INV_PHI2_Q: f64 = 0.381_966_011_250_105;
        let rho_q = self.quantum_lot_distortion_ratio(wallet_usdt_1e8, entry_price, realized_vol_5m, fees);
        let p_t = (taker_prob_pm.clamp(20, 450) as f64) * 1e-3;
        let mfe_f = learned_mfe_bps.raw().max(0) as f64;
        let e_gross = if rho_q <= PHI_Q {
            mfe_f.max(INV_PHI2_Q * (realized_vol_5m.raw().max(0) as f64))
        } else {
            mfe_f
        };
        let pos_rt_mkr = (fees.round_trip_maker_exit_bps().raw().max(0)) as f64;
        let g_maker = (e_gross - pos_rt_mkr).max(25.0);
        let sl_bps = Self::elastic_server_sl_distance_bps_with_fees(realized_vol_5m, fees).raw() as f64;
        let tau_bps = self.tick_to_price_scaled_bps(entry_price).unwrap_or(ScaledBps(50)).raw() as f64;
        let tkr_fee = (fees.round_trip_taker_exit_bps().raw().max(50)) as f64;
        let lambda_taker = (1.0 - INV_PHI2_Q) * sl_bps + tkr_fee + tau_bps;
        (1.0 - p_t) * g_maker - rho_q.powf(PHI_Q) * p_t * lambda_taker
    }

    /// Endogenous Law III/V/VII gate: blocks indivisible oversized contracts with negative Kelly log-growth.
    #[inline]
    #[must_use]
    pub fn passes_quantum_ev_growth_gate(
        self,
        wallet_usdt_1e8: i64,
        entry_price: PriceTicks,
        realized_vol_5m: ScaledBps,
        learned_mfe_bps: ScaledBps,
        taker_prob_pm: i64,
        fees: FeeSchedule,
    ) -> bool {
        const INV_PHI2_Q: f64 = 0.381_966_011_250_105;
        let rho_q = self.quantum_lot_distortion_ratio(wallet_usdt_1e8, entry_price, realized_vol_5m, fees);
        let c_rt = self.expected_round_trip_cost_bps_dynamic(entry_price, taker_prob_pm, fees).raw() as f64;
        let req_floor = (rho_q - 1.0).clamp(0.0, 1.0) * INV_PHI2_Q * c_rt;
        self.quantum_ev_net_drift_bps(wallet_usdt_1e8, entry_price, realized_vol_5m, learned_mfe_bps, taker_prob_pm, fees) > req_floor
    }
}

#[cfg(test)]
mod quantum_ev_v48_tests {
    use super::*;

    #[test]
    fn test_v48_quantum_ev_gate_silences_65usdt_btc_preserves_11_slots_and_unlocks_at_scale() {
        let w_65 = 6_513_770_000_i64;
        let vip0 = FeeSchedule::VIP0_DEFAULT;
        assert!(!APPROVED_SPECS[0].passes_quantum_ev_growth_gate(
            w_65, PriceTicks(646_312), ScaledBps(3_069), ScaledBps(568), 46, vip0
        ));
        assert!(APPROVED_SPECS[1].passes_quantum_ev_growth_gate(
            w_65, PriceTicks(181_047), ScaledBps(3_238), ScaledBps(624), 48, vip0
        ));
        assert!(APPROVED_SPECS[3].passes_quantum_ev_growth_gate(
            w_65, PriceTicks(7_263), ScaledBps(2_206), ScaledBps(628), 50, vip0
        ));
        assert!(APPROVED_SPECS[0].passes_quantum_ev_growth_gate(
            20_000_000_000, PriceTicks(646_312), ScaledBps(3_500), ScaledBps(680), 46, vip0
        ));
    }
}

impl PairSpec {
    /// True iff `allocated_lots` can split into `Tranche 1 + Runner` above exchange leg minimum.
    #[inline]
    #[must_use]
    pub const fn can_split_runner_legs(self, allocated_lots: LotQty, mid_price: PriceTicks) -> bool {
        if allocated_lots.0 < 2 || mid_price.0 <= 0 {
            return false;
        }
        let half_lots = LotQty(allocated_lots.0 / 2);
        self.lot_notional_1e8(half_lots, mid_price) >= SPLIT_LEG_MIN_NOTIONAL_1E8
    }

    /// Continuous C1 single-lot asymmetry EV gate (`EV_1lot > 0` when Runner split is impossible).
    #[must_use]
    #[allow(
        clippy::cast_precision_loss,
        clippy::manual_clamp,
        clippy::suboptimal_flops,
        clippy::too_many_arguments
    )]
    pub fn passes_single_lot_asymmetry_ev_gate(
        self,
        allocated_lots: LotQty,
        mid_price: PriceTicks,
        realized_vol_5m: ScaledBps,
        learned_mfe_bps: i64,
        taker_prob_pm: i64,
        signed_geom: f64,
        ob_bull_pm: u16,
        ob_bear_pm: u16,
        side: Side,
        fees: FeeSchedule,
    ) -> bool {
        const PHI: f64 = 1.618_033_988_749_895;
        const INV_PHI: f64 = 0.618_033_988_749_895;
        const INV_PHI2: f64 = 0.381_966_011_250_105;
        const INV_PHI3: f64 = 0.236_067_977_499_790;

        if allocated_lots.0 <= 0 || mid_price.0 <= 0 {
            return false;
        }
        if self.can_split_runner_legs(allocated_lots, mid_price) {
            return true;
        }
        let p_tkr = (taker_prob_pm.clamp(5, 950) as f64) / 1_000.0;
        let p_mkr = 1.0 - p_tkr;
        let c_mkr_rt = fees.round_trip_maker_exit_bps().raw() as f64;
        let c_tkr_rt = (fees.round_trip_taker_exit_bps().raw().max(100)) as f64;
        let mfe_eff = (learned_mfe_bps.max(0) as f64) - c_mkr_rt;
        if mfe_eff <= 10.0 {
            return false;
        }
        let srv_sl =
            Self::elastic_server_sl_distance_bps_with_fees(realized_vol_5m, fees).raw() as f64;
        let wald_sl_net = INV_PHI * srv_sl + c_tkr_rt;
        let u_g = ((signed_geom.clamp(-1.0, 1.0) - INV_PHI3) / (1.0 - INV_PHI3)).clamp(0.0, 1.0);
        let h_geom = u_g * u_g * (3.0 - 2.0 * u_g);
        let (al_raw, op_raw) = match side {
            Side::Bid => (ob_bull_pm, ob_bear_pm),
            Side::Ask => (ob_bear_pm, ob_bull_pm),
        };
        let al_f = f64::from(al_raw.max(382));
        let op_f = f64::from(op_raw.max(382));
        let ob_ratio = (al_f / op_f).clamp(INV_PHI, PHI);
        let q_struct = (h_geom * ob_ratio).clamp(0.0, PHI);
        let eff_tail_cost = (p_tkr * wald_sl_net) / (INV_PHI2 + q_struct);
        (p_mkr * mfe_eff) > eff_tail_cost
    }
}

#[cfg(test)]
mod single_lot_ev_v49_tests {
    use super::*;

    #[test]
    fn test_v49_single_lot_asymmetry_ev_blocks_eth18_preserves_eth15_16_and_divisible_pairs() {
        let eth = APPROVED_SPECS[1];
        let avax = APPROVED_SPECS[3];
        let fees = FeeSchedule::VIP0_DEFAULT;

        // Divisible multi-lot contracts (AVAX 18 lots @ 7.263 USDT) split into Runner legs.
        assert!(avax.can_split_runner_legs(LotQty(18), PriceTicks(7_263)));
        assert!(avax.passes_single_lot_asymmetry_ev_gate(
            LotQty(18), PriceTicks(7_263), ScaledBps(2_206), 628, 50, 0.23, 742, 675, Side::Bid, fees,
        ));

        // Real ETH #18 LOSS (1 lot, geom_in = +0.09, vol5m = 17.25bp, mfe = 6.47bp, p_taker = 42pm) -> BLOCKED.
        assert!(!eth.can_split_runner_legs(LotQty(1), PriceTicks(179_267)));
        assert!(!eth.passes_single_lot_asymmetry_ev_gate(
            LotQty(1), PriceTicks(179_267), ScaledBps(1_725), 647, 42, 0.09, 438, 415, Side::Bid, fees,
        ));

        // Real ETH #15 & #16 WINS (1 lot, strong book wall geom_in >= +0.96) -> AUTHORIZED.
        assert!(eth.passes_single_lot_asymmetry_ev_gate(
            LotQty(1), PriceTicks(182_033), ScaledBps(2_991), 670, 46, 0.96, 457, 471, Side::Bid, fees,
        ));
        assert!(eth.passes_single_lot_asymmetry_ev_gate(
            LotQty(1), PriceTicks(183_268), ScaledBps(1_507), 670, 44, 0.99, 448, 471, Side::Bid, fees,
        ));

        // Self-learning tightening: elevated p_Taker (125pm) blocks marginal geom (+0.72), unlocks at 2 lots (~$145+ wallet).
        assert!(!eth.passes_single_lot_asymmetry_ev_gate(
            LotQty(1), PriceTicks(182_000), ScaledBps(2_500), 650, 125, 0.72, 460, 480, Side::Bid, fees,
        ));
        assert!(eth.passes_single_lot_asymmetry_ev_gate(
            LotQty(2), PriceTicks(182_000), ScaledBps(2_500), 650, 125, 0.72, 460, 480, Side::Bid, fees,
        ));
    }
}
