//! Dimensional Newtypes, 12-Pair Institutional SSOT Catalog, Beta Cap & Elastic Lot Physics.

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
            + (95 * Self::BYBIT_VIP0_MAKER_FEE_BPS.0
                + 5 * Self::BYBIT_VIP0_TAKER_FEE_BPS.0)
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

#[repr(transparent)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SymbolId(pub u16);

#[allow(clippy::manual_range_contains)]
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
                if best_bid.0 > 0 {
                    best_bid
                } else {
                    best_ask
                }
            }
            Self::Ask => {
                if best_ask.0 > 0 {
                    best_ask
                } else {
                    best_bid
                }
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

#[allow(
    clippy::manual_range_contains,
    clippy::manual_clamp,
    clippy::too_many_arguments
)]
impl PairSpec {
    #[inline]
    pub const fn tick_to_price_scaled_bps(
        self,
        reference_price_ticks: PriceTicks,
    ) -> Result<ScaledBps, VortexError> {
        if reference_price_ticks.0 <= 0 || self.tick_size_1e8 <= 0 {
            return Err(VortexError::InvalidPriceOrTickSpec);
        }
        let bps_scaled =
            (1_000_000 + (reference_price_ticks.0 / 2)) / reference_price_ticks.0;
        Ok(ScaledBps(bps_scaled))
    }

    /// Exact expected round-trip fee + 1-tick quantization friction in ScaledBps.
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

    /// Endogenous 3x round-trip friction barrier B_3x(spec, P_t) without static constants.
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

    /// Endogenous minimum 5m diffusion floor derived strictly from contract friction.
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

    /// Canonical server-side MarkPrice SL distance in [11_000, 14_500] ScaledBps (110..145 bps).
    #[inline]
    #[must_use]
    pub const fn elastic_server_sl_distance_bps(realized_vol_5m: ScaledBps) -> ScaledBps {
        let v = if realized_vol_5m.0 > 0 { realized_vol_5m.0 } else { 0 };
        let raw_bps = 9_500 + (v.saturating_mul(28)) / 100;
        let bounded = if raw_bps < 11_000 {
            11_000
        } else if raw_bps > 14_500 {
            14_500
        } else {
            raw_bps
        };
        ScaledBps(bounded)
    }

    /// Enforces >= 2 ticks cushion against LastPrice and MarkPrice (prevents retCode 10001).
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

    /// Exact executed position notional in USDT scaled by 1e8.
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

    /// Minimum executable LotQty satisfying Bybit 5.00 USDT minimum order notional (no ID branch).
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

    /// Exact executed leg net PnL and fee in USDT (1e8) plus gross/net ScaledBps.
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
        let fee_raw = (entry_notional * in_fee_bps + exit_notional * out_fee_bps + 500_000)
            / 1_000_000;
        let fee_1e8 = if fee_raw > (i64::MAX as i128) {
            i64::MAX
        } else {
            fee_raw as i64
        };
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

    /// Canonical 29% base Kelly notional in USDT scaled by 1e8.
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

    /// True when the exchange minimum executable order notional exceeds 2.2x base Kelly budget.
    #[inline]
    #[must_use]
    pub const fn requires_oversized_lot_conviction(
        self,
        wallet_usdt_1e8: i64,
        entry_price: PriceTicks,
    ) -> bool {
        let min_lots = self.exchange_min_notional_lots(entry_price);
        let min_notional = self.lot_notional_1e8(min_lots, entry_price) as i128;
        let kelly_base = Self::base_kelly_notional_1e8(wallet_usdt_1e8) as i128;
        min_notional * 10 > kelly_base * 22
    }

    /// Enforces |SNR| >= 0.72 and zero local slot scar debt on oversized minimum lots.
    #[inline]
    #[must_use]
    pub fn passes_oversized_lot_conviction_guard(
        self,
        wallet_usdt_1e8: i64,
        entry_price: PriceTicks,
        composite_snr_abs: f64,
        in_local_slot_scar_debt: bool,
    ) -> bool {
        if !self.requires_oversized_lot_conviction(wallet_usdt_1e8, entry_price) {
            return true;
        }
        !in_local_slot_scar_debt && composite_snr_abs >= OVERSIZED_LOT_MIN_SNR_ABS
    }

    /// Characteristic lot scale normalized to ~2.50 USDT of book depth for OFI/TFI priors.
    #[inline]
    #[must_use]
    pub fn characteristic_lot_scale(self, reference_price: PriceTicks) -> f64 {
        if reference_price.0 <= 0 || self.tick_size_1e8 <= 0 || self.lot_step_1e8 <= 0 {
            return 1.0;
        }
        let one_lot_1e8 = self.lot_notional_1e8(LotQty(1), reference_price).max(1) as f64;
        (250_000_000.0 / one_lot_1e8).clamp(0.25, 500.0)
    }

    /// Endogenous minimum resting support lots scaled by friction & diffusion (zero ID branch).
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
        let v_eff = if realized_vol_5m.0 > b3x {
            realized_vol_5m.0
        } else {
            b3x
        };
        let min_support_notional_1e8 =
            ((v_eff as i128) * (b3x as i128) * 14) / 1_000;
        let raw = ((min_support_notional_1e8 + (one_lot_1e8 as i128) - 1)
            / (one_lot_1e8 as i128)) as i64;
        LotQty(if raw < 1 { 1 } else { raw })
    }

    /// Pure dimensionless L1..L5 queue capacity & wall guard (zero symbol_id branching).
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
        let eff_deep_lots = LotQty(own_deep_l1_l5_lots.0.max(own_l1_lots.0));
        let min_support = self.min_resting_l1_lots(entry_price, realized_vol_5m);
        if eff_deep_lots.0 < min_support.0 {
            return false;
        }
        let Ok(tau_tick) = self.tick_to_price_scaled_bps(entry_price) else {
            return false;
        };
        let tau_safe = tau_tick.0.max(1) as f64;
        let vol_safe = realized_vol_5m
            .0
            .max(ScaledBps::MIN_REQUIRED_VOL_5M_BPS.0) as f64;
        let diffusion_ticks = vol_safe / tau_safe;
        let tfi_c = tfi_abs.clamp(0.0, 1.0);
        let ofi_c = ofi_abs.clamp(0.0, 1.0);
        let edge_pos = learned_edge_bps_x100.max(0) as f64;
        let absorption_mult = 12.0 + 10.0 * tfi_c + (edge_pos / 60.0).min(8.0);
        let max_queue_usdt_1e8 =
            (1_250_000_000.0 * diffusion_ticks * absorption_mult).round() as i128;
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

    /// Continuous Bayesian Information-Ratio & Replicator Kelly sizing (zero symbol_id branch).
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
        let one_lot_1e8 = self.lot_notional_1e8(LotQty(1), entry_price);
        if one_lot_1e8 <= 0 {
            return LotQty(1);
        }
        let base_kelly_1e8 = Self::base_kelly_notional_1e8(wallet_usdt_1e8);
        let kappa_permille = if in_scar_debt {
            650_i64
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
        let eff_notional_1e8 =
            ((base_kelly_1e8 as i128) * (kappa_permille as i128)) / 1_000;
        let min_exch = self.exchange_min_notional_lots(entry_price).0;
        if (one_lot_1e8 as i128) > (base_kelly_1e8 as i128) {
            let single_scaled = (eff_notional_1e8 / (one_lot_1e8 as i128)) as i64;
            return LotQty(if single_scaled > min_exch {
                single_scaled
            } else {
                min_exch
            });
        }
        let raw_lots = ((eff_notional_1e8 + (one_lot_1e8 as i128) / 2)
            / (one_lot_1e8 as i128)) as i64;
        let bounded = if raw_lots < min_exch { min_exch } else { raw_lots };
        if bounded <= 1 && min_exch == 1 && (one_lot_1e8 * 2) > base_kelly_1e8 {
            return LotQty(1);
        }
        let split_min = if bounded < 2 { 2 } else { bounded };
        let even_lots = if (split_min & 1) != 0 {
            split_min + 1
        } else {
            split_min
        };
        LotQty(even_lots)
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

    /// Formats PriceTicks into exact Bybit V5 tick-aligned decimal string (no 4-dec drift).
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
    PairSpec {
        symbol_id: SymbolId(1),
        tick_size_1e8: 10_000_000,
        lot_step_1e8: 100_000,
        empirical_taker_exit_prob_pct: 5,
    },
    PairSpec {
        symbol_id: SymbolId(2),
        tick_size_1e8: 1_000_000,
        lot_step_1e8: 1_000_000,
        empirical_taker_exit_prob_pct: 5,
    },
    PairSpec {
        symbol_id: SymbolId(3),
        tick_size_1e8: 1_000_000,
        lot_step_1e8: 10_000_000,
        empirical_taker_exit_prob_pct: 5,
    },
    PairSpec {
        symbol_id: SymbolId(4),
        tick_size_1e8: 100_000,
        lot_step_1e8: 10_000_000,
        empirical_taker_exit_prob_pct: 5,
    },
    PairSpec {
        symbol_id: SymbolId(5),
        tick_size_1e8: 1_000_000,
        lot_step_1e8: 1_000_000,
        empirical_taker_exit_prob_pct: 5,
    },
    PairSpec {
        symbol_id: SymbolId(6),
        tick_size_1e8: 10_000,
        lot_step_1e8: 100_000_000,
        empirical_taker_exit_prob_pct: 5,
    },
    PairSpec {
        symbol_id: SymbolId(7),
        tick_size_1e8: 100_000,
        lot_step_1e8: 10_000_000,
        empirical_taker_exit_prob_pct: 5,
    },
    PairSpec {
        symbol_id: SymbolId(8),
        tick_size_1e8: 10_000,
        lot_step_1e8: 100_000_000,
        empirical_taker_exit_prob_pct: 5,
    },
    PairSpec {
        symbol_id: SymbolId(9),
        tick_size_1e8: 10_000,
        lot_step_1e8: 100_000_000,
        empirical_taker_exit_prob_pct: 5,
    },
    PairSpec {
        symbol_id: SymbolId(10),
        tick_size_1e8: 100_000,
        lot_step_1e8: 10_000_000,
        empirical_taker_exit_prob_pct: 5,
    },
    PairSpec {
        symbol_id: SymbolId(11),
        tick_size_1e8: 100_000,
        lot_step_1e8: 10_000_000,
        empirical_taker_exit_prob_pct: 5,
    },
    PairSpec {
        symbol_id: SymbolId(12),
        tick_size_1e8: 1_000_000,
        lot_step_1e8: 10_000_000,
        empirical_taker_exit_prob_pct: 5,
    },
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
    if s == "Ask" || s == "Sell" {
        Side::Ask
    } else {
        Side::Bid
    }
}

/// Classifies PnL into Light Green (WIN), Sky Blue / Celeste (SCRATCH), or Red (LOSS).
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

/// Classifies trade with Dark Green for Partial Harvests (50%), Light Green for Wins.
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
            let spaces = " ".repeat(pad);
            println!("\u{2551} {}{} \u{2551}", line, spaces);
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
        assert!(APPROVED_SPECS[1]
            .validate_pair_eligibility(PriceTicks(206_677), ScaledBps(2_272))
            .is_ok());
        assert!(APPROVED_SPECS[7]
            .validate_pair_eligibility(PriceTicks(15_586), ScaledBps(2_272))
            .is_ok());
        assert_eq!(
            APPROVED_SPECS[0].format_price_ticks_str(PriceTicks(738_779)),
            "73877.9"
        );
        assert_eq!(
            APPROVED_SPECS[5].format_price_ticks_str(PriceTicks(8_351)),
            "0.8351"
        );
    }

    #[test]
    fn test_lot_notional_oversized_guard_and_strict_2tick_sl_geometry() {
        let btc = APPROVED_SPECS[0];
        let eth = APPROVED_SPECS[1];
        let sui = APPROVED_SPECS[5];
        assert_eq!(btc.lot_notional_1e8(LotQty(1), PriceTicks(759_000)), 7_590_000_000);
        assert_eq!(eth.lot_notional_1e8(LotQty(1), PriceTicks(223_700)), 2_237_000_000);
        assert_eq!(sui.lot_notional_1e8(LotQty(24), PriceTicks(8_510)), 2_042_400_000);

        let w_live = 6_628_080_000_i64;
        assert!(btc.requires_oversized_lot_conviction(w_live, PriceTicks(759_000)));
        assert!(!eth.requires_oversized_lot_conviction(w_live, PriceTicks(223_700)));
        assert!(!btc.passes_oversized_lot_conviction_guard(
            w_live,
            PriceTicks(759_000),
            0.68,
            false,
        ));
        assert!(!btc.passes_oversized_lot_conviction_guard(
            w_live,
            PriceTicks(759_000),
            0.80,
            true,
        ));
        assert!(btc.passes_oversized_lot_conviction_guard(
            w_live,
            PriceTicks(759_000),
            0.75,
            false,
        ));
        assert!(eth.passes_oversized_lot_conviction_guard(
            w_live,
            PriceTicks(223_700),
            0.45,
            false,
        ));

        let xrp = PairSpec {
            symbol_id: SymbolId(901),
            tick_size_1e8: 10_000,
            lot_step_1e8: 100_000_000,
            empirical_taker_exit_prob_pct: 15,
        };
        assert!(xrp
            .validate_pair_eligibility(PriceTicks(5_520), ScaledBps(4_800))
            .is_err());

        let sl_low = PairSpec::elastic_server_sl_distance_bps(ScaledBps(4_000));
        let sl_mid = PairSpec::elastic_server_sl_distance_bps(ScaledBps(8_000));
        let sl_high = PairSpec::elastic_server_sl_distance_bps(ScaledBps(18_000));
        assert_eq!(sl_low, ScaledBps(11_000));
        assert_eq!(sl_mid, ScaledBps(11_740));
        assert_eq!(sl_high, ScaledBps(14_500));

        assert!(PairSpec::validate_stop_loss_geometry(
            Side::Bid,
            PriceTicks(8_197),
            PriceTicks(8_198),
            PriceTicks(8_199),
        )
        .is_err());
    }

    #[test]
    fn test_portfolio_beta_cap_and_dimensionless_queue_physics() {
        let mut guard = PortfolioBetaGuard::new();
        assert!(guard.try_admit(Side::Bid));
        assert!(guard.try_admit(Side::Bid));
        assert!(guard.try_admit(Side::Bid));
        assert!(!guard.try_admit(Side::Bid));
        assert!(guard.try_admit(Side::Ask));
        assert!(guard.try_admit(Side::Ask));
        assert!(guard.try_admit(Side::Ask));
        assert_eq!(guard.total_active(), 6);
        assert!(!guard.try_admit(Side::Ask));

        let btc = APPROVED_SPECS[0];
        let eth = APPROVED_SPECS[1];
        let ltc = APPROVED_SPECS[11];
        let near = APPROVED_SPECS[10];
        assert!(btc.passes_l1_queue_depth_and_wall_guard(
            PriceTicks(748_845),
            ScaledBps(4_346),
            130,
            LotQty(5_623),
            LotQty(800),
            LotQty(12_000),
            0.71,
            0.98,
        ));
        assert!(eth.passes_l1_queue_depth_and_wall_guard(
            PriceTicks(206_446),
            ScaledBps(4_087),
            111,
            LotQty(4_832),
            LotQty(900),
            LotQty(9_500),
            0.43,
            0.97,
        ));
        assert!(ltc.passes_l1_queue_depth_and_wall_guard(
            PriceTicks(5_182),
            ScaledBps(4_386),
            200,
            LotQty(1_220),
            LotQty(450),
            LotQty(3_200),
            0.63,
            0.86,
        ));
        assert!(!near.passes_l1_queue_depth_and_wall_guard(
            PriceTicks(3_738),
            ScaledBps(3_800),
            256,
            LotQty(28_919),
            LotQty(1_200),
            LotQty(40_000),
            0.46,
            0.96,
        ));

        let (net_1e8, fee_1e8, g_bps, n_bps) = btc.compute_leg_pnl_and_fee_1e8(
            Side::Bid,
            LotQty(1),
            PriceTicks(759_000),
            PriceTicks(759_600),
            ScaledBps(400),
        );
        assert_eq!(g_bps, ScaledBps(790));
        assert_eq!(n_bps, ScaledBps(390));
        assert_eq!(fee_1e8, 3_037_200);
        assert_eq!(net_1e8, 2_962_800);
    }
}
