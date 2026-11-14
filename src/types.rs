//! Dimensional Newtypes, 12-Pair SSOT, Risk-Parity Kelly & C-Inf UTC/Funding Cadence Physics.

#![allow(
    clippy::manual_range_contains,
    clippy::manual_clamp,
    clippy::too_many_arguments
)]

use core::f64::consts::TAU;
use std::time::{SystemTime, UNIX_EPOCH};

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
    /// Theoretical zero-tick 3x VIP-0 round-trip friction bound (derived, zero magic constant).
    pub const MIN_REQUIRED_VOL_5M_BPS: Self = Self(
        3 * (Self::BYBIT_VIP0_MAKER_FEE_BPS.0
            + (95 * Self::BYBIT_VIP0_MAKER_FEE_BPS.0 + 5 * Self::BYBIT_VIP0_TAKER_FEE_BPS.0)
                / 100),
    );

    #[inline]
    #[must_use]
    pub const fn raw(self) -> i64 {
        self.0
    }
}

#[repr(transparent)]
#[derive(Copy, Clone, Debug, Default, PartialEq)]
pub struct BoundedScore(f64);

impl BoundedScore {
    pub const ZERO: Self = Self(0.0);

    #[inline]
    #[must_use]
    pub fn new_clamped(val: f64) -> Self {
        if val.is_nan() {
            Self(0.0)
        } else {
            Self(val.clamp(-1.0, 1.0))
        }
    }

    #[inline]
    #[must_use]
    pub const fn get(self) -> f64 {
        self.0
    }

    #[inline]
    #[must_use]
    pub fn to_bps_i32(self) -> i32 {
        (self.0 * 10_000.0).round() as i32
    }
}

/// Continuous C^inf UTC liquidity & 8h Bybit funding cadence modulation output.
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct TemporalCadenceModulation {
    pub intraday_percentile: f64,
    pub snr_threshold_delta: f64,
    pub kin_energy_min_delta_bps: i64,
    pub kelly_lot_mult_permille: i64,
}

impl Default for TemporalCadenceModulation {
    #[inline]
    fn default() -> Self {
        Self::neutral()
    }
}

impl TemporalCadenceModulation {
    #[inline]
    #[must_use]
    pub const fn neutral() -> Self {
        Self {
            intraday_percentile: 0.70,
            snr_threshold_delta: 0.0,
            kin_energy_min_delta_bps: 0,
            kelly_lot_mult_permille: 1_000,
        }
    }
}

/// Evaluates C^inf 3-hub UTC liquidity, 8h funding settlement hazard & breakout attenuation.
#[must_use]
pub fn evaluate_utc_and_funding_cadence(
    ts_ns: TimestampNs,
    realized_vol_5m: ScaledBps,
    eq_vol_5m: ScaledBps,
) -> TemporalCadenceModulation {
    if ts_ns.0 <= 0 {
        return TemporalCadenceModulation::neutral();
    }
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

    let eq_f = (eq_vol_5m.raw().max(1)) as f64;
    let nu_vol = ((realized_vol_5m.raw().max(0) as f64) / eq_f).max(0.40);
    let p_day = (0.65 * h_score + 0.35 * (0.85 * nu_vol).tanh()).clamp(0.05, 0.99);
    let breakout_atten = (-3.5 * (nu_vol - 1.10).max(0.0)).exp();

    let offpeak_gap = (0.64 - p_day).max(0.0) * breakout_atten;
    let prime_surplus = (p_day - 0.72).max(0.0) + 0.12 * w_reb;

    let snr_threshold_delta =
        (0.22 * offpeak_gap + 0.065 * w_whip - 0.045 * prime_surplus).clamp(-0.025, 0.100);
    let fee_rt_i = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
    let fee_rt_f = fee_rt_i as f64;
    let raw_e =
        (fee_rt_f * (0.32 * offpeak_gap + 0.15 * w_whip - 0.08 * prime_surplus)).round() as i64;
    let kin_energy_min_delta_bps = raw_e.clamp(-(fee_rt_i / 25), fee_rt_i / 5);
    let raw_k =
        (1_000.0 - 420.0 * offpeak_gap - 160.0 * w_whip + 180.0 * prime_surplus).round() as i64;
    let kelly_lot_mult_permille = raw_k.clamp(780, 1_080);

    TemporalCadenceModulation {
        intraday_percentile: p_day,
        snr_threshold_delta,
        kin_energy_min_delta_bps,
        kelly_lot_mult_permille,
    }
}

#[repr(transparent)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SymbolId(pub u16);

impl SymbolId {
    #[inline]
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self.0 {
            1 => "BTCUSDT",
            2 => "ETHUSDT",
            3 => "SOLUSDT",
            4 => "AVAXUSDT",
            5 => "TAOUSDT",
            6 => "SUIUSDT",
            7 => "ORDIUSDT",
            8 => "PENDLEUSDT",
            9 => "WIFUSDT",
            10 => "LINKUSDT",
            11 => "NEARUSDT",
            12 => "LTCUSDT",
            _ => "UNKNOWN",
        }
    }

    #[inline]
    #[must_use]
    pub const fn slot_index(self) -> Option<usize> {
        if self.0 >= 1 && self.0 <= 12 {
            Some((self.0 - 1) as usize)
        } else {
            None
        }
    }
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum Side {
    Bid = 0,
    Ask = 1,
}

impl Side {
    #[inline]
    #[must_use]
    pub const fn opposite(self) -> Self {
        match self {
            Self::Bid => Self::Ask,
            Self::Ask => Self::Bid,
        }
    }

    #[inline]
    #[must_use]
    pub const fn sign_i64(self) -> i64 {
        match self {
            Self::Bid => 1,
            Self::Ask => -1,
        }
    }

    #[inline]
    #[must_use]
    pub const fn sign_f64(self) -> f64 {
        match self {
            Self::Bid => 1.0,
            Self::Ask => -1.0,
        }
    }

    #[inline]
    #[must_use]
    pub const fn select_entry_bbo_price(
        self,
        best_bid: PriceTicks,
        best_ask: PriceTicks,
    ) -> PriceTicks {
        match self {
            Self::Bid => {
                if best_bid.0 > 0 { best_bid } else { best_ask }
            }
            Self::Ask => {
                if best_ask.0 > 0 { best_ask } else { best_bid }
            }
        }
    }
}

/// Directional Beta Cap Governor: enforces N_Bid <= 3, N_Ask <= 3, and N_Total <= 6.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct PortfolioBetaGuard {
    pub active_bids: usize,
    pub active_asks: usize,
}

impl PortfolioBetaGuard {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            active_bids: 0,
            active_asks: 0,
        }
    }

    #[inline]
    #[must_use]
    pub const fn total_active(self) -> usize {
        self.active_bids.saturating_add(self.active_asks)
    }

    #[inline]
    pub fn observe_active_side(&mut self, active_side: Side) {
        match active_side {
            Side::Bid => self.active_bids = self.active_bids.saturating_add(1),
            Side::Ask => self.active_asks = self.active_asks.saturating_add(1),
        }
    }

    #[inline]
    #[must_use]
    pub const fn can_admit_side(self, candidate_side: Side) -> bool {
        if self.total_active() >= MAX_PORTFOLIO_OPEN_SLOTS_TOTAL {
            return false;
        }
        match candidate_side {
            Side::Bid => self.active_bids < MAX_DIRECTIONAL_OPEN_SLOTS_PER_SIDE,
            Side::Ask => self.active_asks < MAX_DIRECTIONAL_OPEN_SLOTS_PER_SIDE,
        }
    }

    #[inline]
    pub fn try_admit(&mut self, candidate_side: Side) -> bool {
        if !self.can_admit_side(candidate_side) {
            return false;
        }
        self.observe_active_side(candidate_side);
        true
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VortexError {
    InvalidPriceOrTickSpec,
    AsciiDecimalParseError,
    TickBpsExceedsInstitutionalCap {
        observed_scaled_bps: ScaledBps,
        max_allowed_scaled_bps: ScaledBps,
    },
    RealizedVolBelowStructuralFloor {
        observed_vol_5m_scaled_bps: ScaledBps,
        min_required_scaled_bps: ScaledBps,
    },
    FrictionBarrierRejected {
        expected_excursion_scaled_bps: ScaledBps,
        required_3x_barrier_scaled_bps: ScaledBps,
    },
    StopLossGeometryViolation {
        side: Side,
        proposed_sl_ticks: PriceTicks,
        reference_mark_ticks: PriceTicks,
    },
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
    pub const fn tick_to_price_scaled_bps(
        self,
        reference_price_ticks: PriceTicks,
    ) -> Result<ScaledBps, VortexError> {
        if reference_price_ticks.0 <= 0 || self.tick_size_1e8 <= 0 {
            return Err(VortexError::InvalidPriceOrTickSpec);
        }
        let bps_scaled = (1_000_000 + (reference_price_ticks.0 / 2)) / reference_price_ticks.0;
        Ok(ScaledBps(bps_scaled))
    }

    #[inline]
    pub const fn expected_round_trip_cost_bps(
        self,
        reference_price_ticks: PriceTicks,
    ) -> Result<ScaledBps, VortexError> {
        let raw_tick_bps = match self.tick_to_price_scaled_bps(reference_price_ticks) {
            Ok(v) => v,
            Err(e) => return Err(e),
        };
        let p_taker = if self.empirical_taker_exit_prob_pct < 0 {
            0
        } else if self.empirical_taker_exit_prob_pct > 100 {
            100
        } else {
            self.empirical_taker_exit_prob_pct
        };
        let p_maker = 100 - p_taker;
        let exit_fee_expected = (p_maker * ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.0
            + p_taker * ScaledBps::BYBIT_VIP0_TAKER_FEE_BPS.0)
            / 100;
        let round_trip_cost =
            ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.0 + exit_fee_expected + raw_tick_bps.0;
        Ok(ScaledBps(round_trip_cost))
    }

    #[inline]
    pub const fn required_3x_barrier_bps(
        self,
        reference_price_ticks: PriceTicks,
    ) -> Result<ScaledBps, VortexError> {
        match self.expected_round_trip_cost_bps(reference_price_ticks) {
            Ok(rt) => Ok(ScaledBps(rt.0.saturating_mul(3))),
            Err(e) => Err(e),
        }
    }

    #[inline]
    pub const fn endogenous_min_vol_5m_bps(
        self,
        reference_price_ticks: PriceTicks,
    ) -> Result<ScaledBps, VortexError> {
        self.required_3x_barrier_bps(reference_price_ticks)
    }

    #[inline]
    pub const fn validate_pair_eligibility(
        self,
        reference_price_ticks: PriceTicks,
        realized_vol_5m: ScaledBps,
    ) -> Result<ScaledBps, VortexError> {
        let raw_tick_bps = match self.tick_to_price_scaled_bps(reference_price_ticks) {
            Ok(v) => v,
            Err(e) => return Err(e),
        };
        let min_vol_req = match self.endogenous_min_vol_5m_bps(reference_price_ticks) {
            Ok(v) => v,
            Err(e) => return Err(e),
        };
        if realized_vol_5m.0 < min_vol_req.0 {
            return Err(VortexError::RealizedVolBelowStructuralFloor {
                observed_vol_5m_scaled_bps: realized_vol_5m,
                min_required_scaled_bps: min_vol_req,
            });
        }
        let p_taker = if self.empirical_taker_exit_prob_pct < 0 {
            0
        } else if self.empirical_taker_exit_prob_pct > 100 {
            100
        } else {
            self.empirical_taker_exit_prob_pct
        };
        let tox_excess = if p_taker > 5 { p_taker - 5 } else { 0 };
        let tox_div = 10 + tox_excess * 4;
        let elastic_max_tick = ScaledBps(realized_vol_5m.0 / tox_div);
        if raw_tick_bps.0 > elastic_max_tick.0 {
            return Err(VortexError::TickBpsExceedsInstitutionalCap {
                observed_scaled_bps: raw_tick_bps,
                max_allowed_scaled_bps: elastic_max_tick,
            });
        }
        Ok(raw_tick_bps)
    }

    #[inline]
    pub const fn passes_3x_friction_barrier(
        self,
        reference_price_ticks: PriceTicks,
        expected_excursion: ScaledBps,
    ) -> Result<ScaledBps, VortexError> {
        let required_3x = match self.required_3x_barrier_bps(reference_price_ticks) {
            Ok(v) => v,
            Err(e) => return Err(e),
        };
        if expected_excursion.0 < required_3x.0 {
            return Err(VortexError::FrictionBarrierRejected {
                expected_excursion_scaled_bps: expected_excursion,
                required_3x_barrier_scaled_bps: required_3x,
            });
        }
        Ok(required_3x)
    }

    #[inline]
    #[must_use]
    pub const fn elastic_server_sl_distance_bps(realized_vol_5m: ScaledBps) -> ScaledBps {
        let v = if realized_vol_5m.0 > 0 { realized_vol_5m.0 } else { 0 };
        let fee_rt = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.0 * 2;
        let base_sl = fee_rt * 11;
        let raw_bps = base_sl + (v.saturating_mul(68)) / 100;
        let min_sl = (fee_rt * 29) / 2;
        let max_sl = (fee_rt * 125) / 4;
        let bounded = if raw_bps < min_sl {
            min_sl
        } else if raw_bps > max_sl {
            max_sl
        } else {
            raw_bps
        };
        ScaledBps(bounded)
    }

    #[inline]
    pub const fn validate_stop_loss_geometry(
        side: Side,
        proposed_sl: PriceTicks,
        last_price: PriceTicks,
        mark_price: PriceTicks,
    ) -> Result<PriceTicks, VortexError> {
        if proposed_sl.0 <= 0 || last_price.0 <= 0 || mark_price.0 <= 0 {
            return Err(VortexError::InvalidPriceOrTickSpec);
        }
        match side {
            Side::Bid => {
                let strict_bound = if last_price.0 < mark_price.0 {
                    last_price.0
                } else {
                    mark_price.0
                };
                let min_cushion = if strict_bound > 2 { 2 } else { 1 };
                if proposed_sl.0 > strict_bound - min_cushion {
                    return Err(VortexError::StopLossGeometryViolation {
                        side,
                        proposed_sl_ticks: proposed_sl,
                        reference_mark_ticks: PriceTicks(strict_bound),
                    });
                }
            }
            Side::Ask => {
                let strict_bound = if last_price.0 > mark_price.0 {
                    last_price.0
                } else {
                    mark_price.0
                };
                if proposed_sl.0 < strict_bound.saturating_add(2) {
                    return Err(VortexError::StopLossGeometryViolation {
                        side,
                        proposed_sl_ticks: proposed_sl,
                        reference_mark_ticks: PriceTicks(strict_bound),
                    });
                }
            }
        }
        Ok(proposed_sl)
    }

    #[inline]
    #[must_use]
    pub const fn lot_notional_1e8(self, lots: LotQty, price: PriceTicks) -> i64 {
        if lots.0 <= 0 || price.0 <= 0 || self.tick_size_1e8 <= 0 || self.lot_step_1e8 <= 0 {
            return 0;
        }
        let qty_1e8 = (lots.0 as i128) * (self.lot_step_1e8 as i128);
        let px_1e8 = (price.0 as i128) * (self.tick_size_1e8 as i128);
        let usdt_1e8 = (qty_1e8 * px_1e8) / 100_000_000;
        if usdt_1e8 > (i64::MAX as i128) {
            i64::MAX
        } else {
            usdt_1e8 as i64
        }
    }

    #[inline]
    #[must_use]
    pub const fn exchange_min_notional_lots(self, entry_price: PriceTicks) -> LotQty {
        let one_lot = self.lot_notional_1e8(LotQty(1), entry_price);
        if one_lot <= 0 {
            return LotQty(1);
        }
        let req = (BYBIT_MIN_ORDER_NOTIONAL_1E8 + one_lot - 1) / one_lot;
        LotQty(if req > 1 { req } else { 1 })
    }

    #[inline]
    #[must_use]
    pub const fn compute_leg_pnl_and_fee_1e8(
        self,
        side: Side,
        lots: LotQty,
        entry_px: PriceTicks,
        exit_px: PriceTicks,
        fee_scaled_bps: ScaledBps,
    ) -> (i64, i64, ScaledBps, ScaledBps) {
        if lots.0 <= 0 || entry_px.0 <= 0 || exit_px.0 <= 0 {
            return (0, 0, ScaledBps(0), ScaledBps(0));
        }
        let raw_diff = match side {
            Side::Bid => exit_px.0 - entry_px.0,
            Side::Ask => entry_px.0 - exit_px.0,
        };
        let gross_bps = (raw_diff.saturating_mul(1_000_000)) / entry_px.0;
        let fee_bps = if fee_scaled_bps.0 > 0 { fee_scaled_bps.0 } else { 0 };
        let net_bps = gross_bps - fee_bps;

        let entry_notional = self.lot_notional_1e8(lots, entry_px) as i128;
        let exit_notional = self.lot_notional_1e8(lots, exit_px) as i128;
        let gross_pnl = match side {
            Side::Bid => exit_notional - entry_notional,
            Side::Ask => entry_notional - exit_notional,
        };
        let in_fee_bps = if fee_bps >= ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.0 {
            ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.0 as i128
        } else {
            fee_bps as i128
        };
        let out_raw = (fee_bps as i128) - in_fee_bps;
        let out_fee_bps = if out_raw > 0 { out_raw } else { 0 };
        let fee_raw =
            (entry_notional * in_fee_bps + exit_notional * out_fee_bps + 500_000) / 1_000_000;
        let fee_1e8 = if fee_raw > (i64::MAX as i128) { i64::MAX } else { fee_raw as i64 };
        let net_raw = gross_pnl - (fee_1e8 as i128);
        let net_pnl_1e8 = if net_raw > (i64::MAX as i128) {
            i64::MAX
        } else if net_raw < (i64::MIN as i128) {
            i64::MIN
        } else {
            net_raw as i64
        };
        (net_pnl_1e8, fee_1e8, ScaledBps(gross_bps), ScaledBps(net_bps))
    }

    /// Exact all-in SL liquidation distance in ppm (1e-6 of notional = ScaledBps).
    #[inline]
    #[must_use]
    pub const fn all_in_sl_loss_ppm(
        self,
        entry_price: PriceTicks,
        realized_vol_5m: ScaledBps,
    ) -> i64 {
        let sl_bps = Self::elastic_server_sl_distance_bps(realized_vol_5m).0;
        let fee_sl =
            ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.0 + ScaledBps::BYBIT_VIP0_TAKER_FEE_BPS.0;
        let tick_bps = match self.tick_to_price_scaled_bps(entry_price) {
            Ok(v) => v.0,
            Err(_) => 50,
        };
        sl_bps.saturating_add(fee_sl).saturating_add(tick_bps)
    }

    /// Endogenous per-slot risk budget in USDT (1e8): N_max * 2 * tau_maker = 2,400 ppm of W.
    #[inline]
    #[must_use]
    pub const fn base_slot_risk_budget_1e8(wallet_usdt_1e8: i64) -> i64 {
        let w_eff = if wallet_usdt_1e8 < 5_000_000_000 {
            5_000_000_000
        } else if wallet_usdt_1e8 > 10_000_000_000_000 {
            10_000_000_000_000
        } else {
            wallet_usdt_1e8
        };
        let risk_ppm = (MAX_PORTFOLIO_OPEN_SLOTS_TOTAL as i128)
            * ((ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.0 * 2) as i128);
        (((w_eff as i128) * risk_ppm) / 1_000_000) as i64
    }

    #[inline]
    #[must_use]
    pub const fn base_kelly_notional_1e8(wallet_usdt_1e8: i64) -> i64 {
        let w_clamped = if wallet_usdt_1e8 < 5_000_000_000 {
            5_000_000_000
        } else if wallet_usdt_1e8 > 1_000_000_000_000 {
            1_000_000_000_000
        } else {
            wallet_usdt_1e8
        };
        let raw_n = ((w_clamped as i128 * 29) / 100) as i64;
        if raw_n < 1_800_000_000 {
            1_800_000_000
        } else if raw_n > 25_000_000_000 {
            25_000_000_000
        } else {
            raw_n
        }
    }

    /// True when the minimum executable lot SL loss exceeds 2.20x the slot dollar risk budget.
    #[inline]
    #[must_use]
    pub const fn requires_oversized_lot_conviction_with_vol(
        self,
        wallet_usdt_1e8: i64,
        entry_price: PriceTicks,
        realized_vol_5m: ScaledBps,
    ) -> bool {
        let min_lots = self.exchange_min_notional_lots(entry_price);
        let min_notional = self.lot_notional_1e8(min_lots, entry_price) as i128;
        let all_in_ppm = self.all_in_sl_loss_ppm(entry_price, realized_vol_5m);
        let safe_ppm = if all_in_ppm > 1 { all_in_ppm as i128 } else { 1 };
        let min_lot_loss = (min_notional * safe_ppm) / 1_000_000;
        let base_risk = Self::base_slot_risk_budget_1e8(wallet_usdt_1e8);
        let safe_risk = if base_risk > 1 { base_risk as i128 } else { 1 };
        min_lot_loss * 5 > safe_risk * 11
    }

    #[inline]
    #[must_use]
    pub const fn requires_oversized_lot_conviction(
        self,
        wallet_usdt_1e8: i64,
        entry_price: PriceTicks,
    ) -> bool {
        let eq_vol = ScaledBps(ScaledBps::MIN_REQUIRED_VOL_5M_BPS.0 * 5);
        self.requires_oversized_lot_conviction_with_vol(wallet_usdt_1e8, entry_price, eq_vol)
    }

    /// Continuous C1 conviction threshold for oversized minimum exchange lots (e.g. BTC Slot 0).
    #[inline]
    #[must_use]
    pub fn passes_oversized_lot_conviction_guard_with_vol(
        self,
        wallet_usdt_1e8: i64,
        entry_price: PriceTicks,
        realized_vol_5m: ScaledBps,
        composite_snr_abs: f64,
        in_local_slot_scar_debt: bool,
    ) -> bool {
        if !self.requires_oversized_lot_conviction_with_vol(
            wallet_usdt_1e8,
            entry_price,
            realized_vol_5m,
        ) {
            return true;
        }
        if in_local_slot_scar_debt {
            return false;
        }
        let min_lots = self.exchange_min_notional_lots(entry_price);
        let min_notional = self.lot_notional_1e8(min_lots, entry_price) as f64;
        let all_in_ppm = (self.all_in_sl_loss_ppm(entry_price, realized_vol_5m).max(1)) as f64;
        let min_lot_loss = (min_notional * all_in_ppm) * 1e-6;
        let base_risk = (Self::base_slot_risk_budget_1e8(wallet_usdt_1e8).max(1)) as f64;
        let rho_over = min_lot_loss / base_risk;
        let excess = (rho_over - 2.20).max(0.0);
        let req_snr = (0.66 + 0.045 * (excess / 1.80).tanh() + 0.010 * excess).min(0.82);
        composite_snr_abs >= req_snr
    }

    #[inline]
    #[must_use]
    pub fn passes_oversized_lot_conviction_guard(
        self,
        wallet_usdt_1e8: i64,
        entry_price: PriceTicks,
        composite_snr_abs: f64,
        in_local_slot_scar_debt: bool,
    ) -> bool {
        let eq_vol = ScaledBps(ScaledBps::MIN_REQUIRED_VOL_5M_BPS.0 * 5);
        self.passes_oversized_lot_conviction_guard_with_vol(
            wallet_usdt_1e8,
            entry_price,
            eq_vol,
            composite_snr_abs,
            in_local_slot_scar_debt,
        )
    }

    #[inline]
    #[must_use]
    pub fn characteristic_lot_scale(self, reference_price: PriceTicks) -> f64 {
        if reference_price.0 <= 0 || self.tick_size_1e8 <= 0 || self.lot_step_1e8 <= 0 {
            return 1.0;
        }
        let one_lot_1e8 = self.lot_notional_1e8(LotQty(1), reference_price).max(1) as f64;
        (250_000_000.0 / one_lot_1e8).clamp(0.25, 500.0)
    }

    #[inline]
    #[must_use]
    pub const fn min_resting_l1_lots(
        self,
        entry_price: PriceTicks,
        realized_vol_5m: ScaledBps,
    ) -> LotQty {
        let one_lot_1e8 = self.lot_notional_1e8(LotQty(1), entry_price);
        if one_lot_1e8 <= 0 {
            return LotQty(1);
        }
        let b3x = match self.required_3x_barrier_bps(entry_price) {
            Ok(v) => v.0,
            Err(_) => ScaledBps::MIN_REQUIRED_VOL_5M_BPS.0,
        };
        let v_eff = if realized_vol_5m.0 > b3x { realized_vol_5m.0 } else { b3x };
        let min_support_notional_1e8 = ((v_eff as i128) * (b3x as i128) * 14) / 1_000;
        let raw = ((min_support_notional_1e8 + (one_lot_1e8 as i128) - 1)
            / (one_lot_1e8 as i128)) as i64;
        LotQty(if raw < 1 { 1 } else { raw })
    }

    #[inline]
    #[must_use]
    pub fn passes_l1_queue_depth_and_wall_guard(
        self,
        entry_price: PriceTicks,
        realized_vol_5m: ScaledBps,
        learned_edge_bps_x100: i64,
        own_l1_lots: LotQty,
        opp_l1_lots: LotQty,
        own_deep_l1_l5_lots: LotQty,
        ofi_abs: f64,
        tfi_abs: f64,
    ) -> bool {
        if own_l1_lots.0 <= 0 || entry_price.0 <= 0 {
            return false;
        }
        let min_l1_notional_1e8 = 2_400_000_000_i64;
        if self.lot_notional_1e8(own_l1_lots, entry_price) < min_l1_notional_1e8 {
            return false;
        }
        let eff_deep_lots = LotQty(own_deep_l1_l5_lots.0.max(own_l1_lots.0));
        let min_support = self.min_resting_l1_lots(entry_price, realized_vol_5m);
        if eff_deep_lots.0 < min_support.0 {
            return false;
        }
        let Ok(tau_tick) = self.tick_to_price_scaled_bps(entry_price) else {
            return false;
        };
        let tau_safe = tau_tick.0.max(1) as f64;
        let vol_safe = realized_vol_5m.0.max(ScaledBps::MIN_REQUIRED_VOL_5M_BPS.0) as f64;
        let diffusion_ticks = vol_safe / tau_safe;
        let tfi_c = tfi_abs.clamp(0.0, 1.0);
        let ofi_c = ofi_abs.clamp(0.0, 1.0);
        let edge_pos = learned_edge_bps_x100.max(0) as f64;
        let absorption_mult = 12.0 + 10.0 * tfi_c + (edge_pos / 60.0).min(8.0);
        let max_queue_usdt_1e8 = (1_250_000_000.0 * diffusion_ticks * absorption_mult).round() as i128;
        let own_l1_notional_1e8 = self.lot_notional_1e8(own_l1_lots, entry_price) as i128;
        if own_l1_notional_1e8 > max_queue_usdt_1e8 {
            return false;
        }
        if own_l1_notional_1e8 * 100 <= max_queue_usdt_1e8 * 55 || tfi_c >= 0.65 {
            return true;
        }
        let tot_q = (own_l1_lots.0.saturating_add(opp_l1_lots.0.max(0))).max(1);
        let sweep_relief = ((tfi_c - ofi_c).max(0.0) * 140.0) as i64;
        let max_q_permille = (780 + sweep_relief).min(920);
        (own_l1_lots.0.saturating_mul(1_000)) / tot_q <= max_q_permille
    }

    /// Constant-Dollar Risk Parity + Bayesian Kelly + UTC Cadence Lot Sizing (Laws I, II, VI).
    #[inline]
    #[must_use]
    pub const fn compute_risk_parity_kelly_lots(
        self,
        wallet_usdt_1e8: i64,
        entry_price: PriceTicks,
        realized_vol_5m: ScaledBps,
        learned_edge_bps_x100: i64,
        info_ratio_x100: i64,
        cadence_mult_permille: i64,
        in_scar_debt: bool,
    ) -> LotQty {
        let one_lot_1e8 = self.lot_notional_1e8(LotQty(1), entry_price);
        if one_lot_1e8 <= 0 {
            return LotQty(1);
        }
        let w_eff = if wallet_usdt_1e8 < 5_000_000_000 {
            5_000_000_000
        } else if wallet_usdt_1e8 > 10_000_000_000_000 {
            10_000_000_000_000
        } else {
            wallet_usdt_1e8
        };
        let base_risk = Self::base_slot_risk_budget_1e8(w_eff) as i128;
        let kappa_permille = if in_scar_debt {
            550_i64
        } else {
            let edge_exc = if learned_edge_bps_x100 > 150 {
                learned_edge_bps_x100 - 150
            } else {
                0
            };
            let ir_adj = if info_ratio_x100 > 50 {
                ((info_ratio_x100 - 50) * 3) / 2
            } else if info_ratio_x100 < 0 {
                info_ratio_x100 * 2
            } else {
                0
            };
            let raw_k = 1_000 + edge_exc + ir_adj;
            if raw_k < 700 {
                700
            } else if raw_k > 1_450 {
                1_450
            } else {
                raw_k
            }
        };
        let cad_c = if cadence_mult_permille < 780 {
            780
        } else if cadence_mult_permille > 1_080 {
            1_080
        } else {
            cadence_mult_permille
        };
        let adj_risk =
            (((base_risk * (kappa_permille as i128)) / 1_000) * (cad_c as i128)) / 1_000;
        let all_in_ppm = self.all_in_sl_loss_ppm(entry_price, realized_vol_5m);
        let safe_ppm = if all_in_ppm > 1 { all_in_ppm as i128 } else { 1 };
        let raw_notional = (adj_risk * 1_000_000) / safe_ppm;
        let min_floor_notional = ((BYBIT_MIN_ORDER_NOTIONAL_1E8 as i128) * 12) / 5;
        let max_slot_notional = ((w_eff as i128) * 42) / 100;
        let eff_notional_1e8 = if raw_notional < min_floor_notional {
            min_floor_notional
        } else if raw_notional > max_slot_notional {
            max_slot_notional
        } else {
            raw_notional
        };

        let min_exch = self.exchange_min_notional_lots(entry_price).0;
        if (one_lot_1e8 as i128) > eff_notional_1e8 {
            let single_scaled = (eff_notional_1e8 / (one_lot_1e8 as i128)) as i64;
            return LotQty(if single_scaled > min_exch { single_scaled } else { min_exch });
        }
        let raw_lots = ((eff_notional_1e8 + (one_lot_1e8 as i128) / 2)
            / (one_lot_1e8 as i128)) as i64;
        let bounded = if raw_lots < min_exch { min_exch } else { raw_lots };
        if bounded <= 1 && min_exch == 1 && ((one_lot_1e8 * 2) as i128) > eff_notional_1e8 {
            return LotQty(1);
        }
        let split_min = if bounded < 2 { 2 } else { bounded };
        let even_lots = if (split_min & 1) != 0 { split_min + 1 } else { split_min };
        LotQty(even_lots)
    }

    #[inline]
    #[must_use]
    pub const fn compute_bayesian_kelly_lots(
        self,
        wallet_usdt_1e8: i64,
        entry_price: PriceTicks,
        learned_edge_bps_x100: i64,
        info_ratio_x100: i64,
        in_scar_debt: bool,
    ) -> LotQty {
        let eq_vol = ScaledBps(ScaledBps::MIN_REQUIRED_VOL_5M_BPS.0 * 5);
        self.compute_risk_parity_kelly_lots(
            wallet_usdt_1e8,
            entry_price,
            eq_vol,
            learned_edge_bps_x100,
            info_ratio_x100,
            1_000,
            in_scar_debt,
        )
    }

    #[inline]
    #[must_use]
    pub const fn compute_compounding_kelly_lots(
        self,
        wallet_usdt_1e8: i64,
        entry_price: PriceTicks,
        learned_edge_bps_x100: i64,
        in_scar_debt: bool,
    ) -> LotQty {
        self.compute_bayesian_kelly_lots(
            wallet_usdt_1e8,
            entry_price,
            learned_edge_bps_x100,
            50,
            in_scar_debt,
        )
    }

    #[inline]
    #[must_use]
    pub fn format_lot_qty_str(self, lots: LotQty) -> String {
        let l_qty = lots.0.max(1);
        let sc = (l_qty as i128) * (self.lot_step_1e8.max(1) as i128);
        let wh = sc / 100_000_000;
        let fr = (sc % 100_000_000).unsigned_abs();
        if fr == 0 {
            format!("{wh}")
        } else {
            let s_f = format!("{fr:08}");
            format!("{wh}.{}", s_f.trim_end_matches('0'))
        }
    }

    #[inline]
    #[must_use]
    pub fn format_price_ticks_str(self, ticks: PriceTicks) -> String {
        let p_ticks = ticks.0.max(1);
        let sc = (p_ticks as i128) * (self.tick_size_1e8.max(1) as i128);
        let wh = sc / 100_000_000;
        let fr = (sc % 100_000_000).unsigned_abs();
        if fr == 0 {
            format!("{wh}")
        } else {
            let s_f = format!("{fr:08}");
            format!("{wh}.{}", s_f.trim_end_matches('0'))
        }
    }

    #[inline]
    #[must_use]
    pub const fn format_price_usdt_parts(self, ticks: PriceTicks) -> (i64, i64) {
        let scaled_1e8 = (ticks.0 as i128) * (self.tick_size_1e8 as i128);
        let whole = (scaled_1e8 / 100_000_000) as i64;
        let frac_4 = ((scaled_1e8.abs() % 100_000_000) / 10_000) as i64;
        (whole, frac_4)
    }

    pub fn parse_price_ticks(self, decimal_str: &str) -> Result<PriceTicks, VortexError> {
        let s = decimal_str.trim();
        if s.is_empty() || self.tick_size_1e8 <= 0 {
            return Err(VortexError::AsciiDecimalParseError);
        }
        let mut whole: i128 = 0;
        let mut frac: i128 = 0;
        let mut frac_digits: u32 = 0;
        let mut in_frac = false;
        for b in s.bytes() {
            if b == b'.' {
                if in_frac {
                    return Err(VortexError::AsciiDecimalParseError);
                }
                in_frac = true;
                continue;
            }
            if !b.is_ascii_digit() {
                return Err(VortexError::AsciiDecimalParseError);
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
        let total_1e8 = whole.saturating_mul(100_000_000).saturating_add(frac);
        let tick = self.tick_size_1e8 as i128;
        let ticks = (total_1e8 + (tick / 2)) / tick;
        if ticks <= 0 {
            return Err(VortexError::InvalidPriceOrTickSpec);
        }
        Ok(PriceTicks(ticks as i64))
    }

    #[must_use]
    pub fn parse_lot_qty(self, decimal_str: &str) -> LotQty {
        let s = decimal_str.trim();
        if s.is_empty() || self.lot_step_1e8 <= 0 {
            return LotQty(0);
        }
        let mut whole: i128 = 0;
        let mut frac: i128 = 0;
        let mut frac_digits: u32 = 0;
        let mut in_frac = false;
        for b in s.bytes() {
            if b == b'.' {
                if in_frac {
                    return LotQty(0);
                }
                in_frac = true;
                continue;
            }
            if !b.is_ascii_digit() {
                return LotQty(0);
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
        let total_1e8 = whole.saturating_mul(100_000_000).saturating_add(frac);
        let step = self.lot_step_1e8 as i128;
        LotQty(((total_1e8 + (step / 2)) / step) as i64)
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

#[inline]
#[must_use]
pub const fn spec_from_symbol_id(id: SymbolId) -> Option<PairSpec> {
    match id.slot_index() {
        Some(idx) => Some(APPROVED_SPECS[idx]),
        None => None,
    }
}

#[inline]
#[must_use]
pub fn slot_from_symbol_name(name: &str) -> Option<usize> {
    for (idx, spec) in APPROVED_SPECS.iter().enumerate() {
        if spec.symbol_id.name() == name {
            return Some(idx);
        }
    }
    None
}

#[inline]
#[must_use]
pub fn side_from_str(s: &str) -> Side {
    if s == "Ask" || s == "Sell" { Side::Ask } else { Side::Bid }
}

#[inline]
#[must_use]
pub fn classify_pnl_badge(net_bps: f64) -> (&'static str, &'static str, &'static str) {
    if net_bps >= 1.00 {
        (ANSI_GREEN, "\u{1f7e2}", "WIN    ")
    } else if net_bps >= -0.50 {
        (ANSI_SKY_BLUE, "\u{1fa75}", "SCRATCH")
    } else {
        (ANSI_RED, "\u{1f534}", "LOSS   ")
    }
}

#[inline]
#[must_use]
pub fn classify_trade_badge(
    net_bps: f64,
    is_partial_harvest: bool,
) -> (&'static str, &'static str, &'static str) {
    if is_partial_harvest {
        (ANSI_DARK_GREEN, "\u{1f332}", "PARTIAL")
    } else {
        classify_pnl_badge(net_bps)
    }
}

#[inline]
#[must_use]
pub fn now_epoch_ns() -> TimestampNs {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => TimestampNs(d.as_nanos() as i64),
        Err(_) => TimestampNs(0),
    }
}

#[must_use]
pub fn visible_terminal_width(s: &str) -> usize {
    let mut width = 0_usize;
    let mut chars = s.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\x1b' {
            if chars.peek() == Some(&'[') {
                let _ = chars.next();
                for c in chars.by_ref() {
                    if c.is_ascii_alphabetic() {
                        break;
                    }
                }
            }
            continue;
        }
        let cp = ch as u32;
        if cp == 0xFE0F {
            continue;
        }
        let w = if (0x1F300..=0x1FAFF).contains(&cp) || (0x2600..=0x27BF).contains(&cp) {
            2
        } else {
            1
        };
        width += w;
    }
    width
}

pub fn print_auto_aligned_box(sections: &[Vec<String>]) {
    let mut max_w = 0_usize;
    for sec in sections {
        for line in sec {
            let w = visible_terminal_width(line);
            if w > max_w {
                max_w = w;
            }
        }
    }
    let inner_w = max_w + 2;
    let horiz = "\u{2550}".repeat(inner_w);
    println!("\u{2554}{}\u{2557}", horiz);
    for (s_idx, sec) in sections.iter().enumerate() {
        if s_idx > 0 {
            println!("\u{2560}{}\u{2563}", horiz);
        }
        for line in sec {
            let w = visible_terminal_width(line);
            let pad = max_w.saturating_sub(w);
            println!("\u{2551} {}{} \u{2551}", line, " ".repeat(pad));
        }
    }
    println!("\u{255a}{}\u{255d}", horiz);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_12_approved_specs_and_kelly_compounding_divisibility() {
        assert_eq!(APPROVED_SPECS.len(), 12);
        let wallet_66_usdt = 6_628_080_000_i64;
        let wallet_200_usdt = 20_000_000_000_i64;
        let sample_ticks: [i64; 12] = [
            759_000, 223_700, 8_930, 8_170, 22_400, 8_510,
            3_130, 16_140, 1_598, 10_720, 3_690, 5_176,
        ];
        for (idx, spec) in APPROVED_SPECS.iter().enumerate() {
            let px = PriceTicks(sample_ticks[idx]);
            assert!(spec.validate_pair_eligibility(px, ScaledBps(6_500)).is_ok());
            assert_eq!(spec_from_symbol_id(spec.symbol_id), Some(*spec));
            let scale = spec.characteristic_lot_scale(px);
            assert!((0.25..=500.0).contains(&scale));
            let lots_66 = spec.compute_compounding_kelly_lots(wallet_66_usdt, px, 250, false);
            let lots_200 = spec.compute_compounding_kelly_lots(wallet_200_usdt, px, 350, false);
            assert!(lots_66.0 >= 1);
            assert!(lots_200.0 >= lots_66.0);
            assert!(spec.lot_notional_1e8(lots_66, px) >= BYBIT_MIN_ORDER_NOTIONAL_1E8);
            if idx == 1 {
                assert_eq!(lots_66, LotQty(1));
                assert_eq!(lots_200.0 % 2, 0);
            } else if idx >= 2 {
                assert_eq!(lots_66.0 % 2, 0);
                assert_eq!(lots_200.0 % 2, 0);
            }
        }
        assert_eq!(APPROVED_SPECS[0].format_price_ticks_str(PriceTicks(738_779)), "73877.9");
        assert_eq!(APPROVED_SPECS[5].format_price_ticks_str(PriceTicks(8_351)), "0.8351");
    }

    #[test]
    fn test_lot_notional_oversized_guard_and_strict_2tick_sl_geometry() {
        let btc = APPROVED_SPECS[0];
        let eth = APPROVED_SPECS[1];
        let w_live = 6_628_080_000_i64;
        assert!(btc.requires_oversized_lot_conviction(w_live, PriceTicks(759_000)));
        assert!(!eth.requires_oversized_lot_conviction(w_live, PriceTicks(223_700)));
        assert!(!btc.passes_oversized_lot_conviction_guard(w_live, PriceTicks(759_000), 0.68, false));
        assert!(!btc.passes_oversized_lot_conviction_guard(w_live, PriceTicks(759_000), 0.80, true));
        assert!(btc.passes_oversized_lot_conviction_guard(w_live, PriceTicks(759_000), 0.75, false));
        assert!(eth.passes_oversized_lot_conviction_guard(w_live, PriceTicks(223_700), 0.45, false));
        assert!(PairSpec::validate_stop_loss_geometry(
            Side::Bid, PriceTicks(8_197), PriceTicks(8_198), PriceTicks(8_199),
        )
        .is_err());
    }

    #[test]
    fn test_portfolio_beta_cap_and_dimensionless_queue_physics() {
        let mut guard = PortfolioBetaGuard::new();
        for _ in 0..3 {
            assert!(guard.try_admit(Side::Bid));
            assert!(guard.try_admit(Side::Ask));
        }
        assert_eq!(guard.total_active(), 6);
        assert!(!guard.try_admit(Side::Bid));
        assert!(!guard.try_admit(Side::Ask));

        let btc = APPROVED_SPECS[0];
        assert!(btc.passes_l1_queue_depth_and_wall_guard(
            PriceTicks(748_845), ScaledBps(4_346), 130, LotQty(5_623),
            LotQty(800), LotQty(12_000), 0.71, 0.98,
        ));
    }

    #[test]
    fn test_risk_parity_dollar_invariance_and_utc_funding_cadence() {
        let avax = APPROVED_SPECS[3];
        let w_live = 6_628_080_000_i64;
        let px = PriceTicks(8_170);
        let lots_lo = avax.compute_risk_parity_kelly_lots(
            w_live, px, ScaledBps(2_200), 250, 50, 1_000, false,
        );
        let lots_hi = avax.compute_risk_parity_kelly_lots(
            w_live, px, ScaledBps(15_000), 250, 50, 1_000, false,
        );
        assert!(lots_lo.0 > lots_hi.0);
        assert_eq!(lots_lo.0 % 2, 0);
        assert_eq!(lots_hi.0 % 2, 0);
        let risk_lo_1e8 = (avax.lot_notional_1e8(lots_lo, px) as i128
            * avax.all_in_sl_loss_ppm(px, ScaledBps(2_200)) as i128)
            / 1_000_000;
        let risk_hi_1e8 = (avax.lot_notional_1e8(lots_hi, px) as i128
            * avax.all_in_sl_loss_ppm(px, ScaledBps(15_000)) as i128)
            / 1_000_000;
        assert!((risk_lo_1e8 - risk_hi_1e8).abs() < 1_500_000);

        let prime_ns = TimestampNs(14 * 3_600_000_000_000 + 1_800_000_000_000);
        let trough_ns = TimestampNs(20 * 3_600_000_000_000 + 1_800_000_000_000);
        let fund_whip_ns = TimestampNs(8 * 3_600_000_000_000 - 75_000_000_000);

        let m_prime = evaluate_utc_and_funding_cadence(prime_ns, ScaledBps(4_500), ScaledBps(5_000));
        let m_trough = evaluate_utc_and_funding_cadence(trough_ns, ScaledBps(3_200), ScaledBps(5_000));
        let m_brk = evaluate_utc_and_funding_cadence(trough_ns, ScaledBps(10_000), ScaledBps(5_000));
        let m_whip = evaluate_utc_and_funding_cadence(fund_whip_ns, ScaledBps(4_500), ScaledBps(5_000));

        assert!(m_prime.intraday_percentile > 0.78);
        assert!(m_trough.snr_threshold_delta > 0.035);
        assert!(m_trough.kin_energy_min_delta_bps >= 20);
        assert!(m_brk.snr_threshold_delta < 0.005);
        assert!(m_whip.snr_threshold_delta > 0.045);
    }
}
