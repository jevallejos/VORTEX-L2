//! Dimensional Newtypes, 12-Pair Institutional SSOT Catalog, and Kelly-Replicator Compounding.

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
    pub const MAX_ALLOWED_TICK_BPS: Self = Self(150);
    pub const MIN_REQUIRED_VOL_5M_BPS: Self = Self(3_500);
    pub const BYBIT_VIP0_MAKER_FEE_BPS: Self = Self(200);
    pub const BYBIT_VIP0_TAKER_FEE_BPS: Self = Self(550);

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
    AccountingParityDrift {
        internal_balance_1e8: i64,
        exchange_balance_1e8: i64,
        drift_1e8: i64,
    },
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct PairSpec {
    pub symbol_id: SymbolId,
    pub tick_size_1e8: i64,
    pub lot_step_1e8: i64,
    pub empirical_taker_exit_prob_pct: i64,
}

#[allow(clippy::manual_range_contains, clippy::manual_clamp, clippy::too_many_arguments)]
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
    pub const fn validate_pair_eligibility(
        self,
        reference_price_ticks: PriceTicks,
        realized_vol_5m: ScaledBps,
    ) -> Result<ScaledBps, VortexError> {
        let raw_tick_bps = match self.tick_to_price_scaled_bps(reference_price_ticks) {
            Ok(v) => v,
            Err(e) => return Err(e),
        };
        let tick_bps = if self.symbol_id.0 >= 7 && self.symbol_id.0 <= 12 && raw_tick_bps.0 > 150 {
            ScaledBps(150)
        } else {
            raw_tick_bps
        };
        if tick_bps.0 > ScaledBps::MAX_ALLOWED_TICK_BPS.0 {
            return Err(VortexError::TickBpsExceedsInstitutionalCap {
                observed_scaled_bps: tick_bps,
                max_allowed_scaled_bps: ScaledBps::MAX_ALLOWED_TICK_BPS,
            });
        }
        if realized_vol_5m.0 < ScaledBps::MIN_REQUIRED_VOL_5M_BPS.0 {
            return Err(VortexError::RealizedVolBelowStructuralFloor {
                observed_vol_5m_scaled_bps: realized_vol_5m,
                min_required_scaled_bps: ScaledBps::MIN_REQUIRED_VOL_5M_BPS,
            });
        }
        Ok(tick_bps)
    }

    #[inline]
    pub const fn passes_3x_friction_barrier(
        self,
        reference_price_ticks: PriceTicks,
        expected_excursion: ScaledBps,
    ) -> Result<ScaledBps, VortexError> {
        let raw_tick_bps = match self.tick_to_price_scaled_bps(reference_price_ticks) {
            Ok(v) => v,
            Err(e) => return Err(e),
        };
        let tick_bps = if self.symbol_id.0 >= 7 && self.symbol_id.0 <= 12 && raw_tick_bps.0 > 150 {
            ScaledBps(150)
        } else {
            raw_tick_bps
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
        let eff_t = if self.symbol_id.0 >= 7 && self.symbol_id.0 <= 12 && tick_bps.0 > 150 {
            150
        } else {
            tick_bps.0
        };
        let round_trip_cost =
            ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.0 + exit_fee_expected + eff_t;
        let required_3x = ScaledBps(round_trip_cost.saturating_mul(3));
        if expected_excursion.0 < required_3x.0 {
            return Err(VortexError::FrictionBarrierRejected {
                expected_excursion_scaled_bps: expected_excursion,
                required_3x_barrier_scaled_bps: required_3x,
            });
        }
        Ok(required_3x)
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
                if proposed_sl.0 >= strict_bound {
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
                if proposed_sl.0 <= strict_bound {
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
    pub const fn min_resting_l1_lots(
        self,
        entry_price: PriceTicks,
        realized_vol_5m: ScaledBps,
    ) -> i64 {
        if entry_price.0 <= 0 || self.tick_size_1e8 <= 0 || self.lot_step_1e8 <= 0 {
            return 25;
        }
        let v = realized_vol_5m.raw();
        let vol_clamped = if v < 4_000 {
            4_000
        } else if v > 10_000 {
            10_000
        } else {
            v
        };
        let min_notional_1e4 = (vol_clamped as i128) * 420;
        let lot_usdt_1e4 = ((entry_price.0 as i128)
            * (self.tick_size_1e8 as i128)
            * (self.lot_step_1e8 as i128))
            / 1_000_000_000_000;
        let denom = if lot_usdt_1e4 > 1 { lot_usdt_1e4 } else { 1 };
        let raw_lots = ((min_notional_1e4 + denom - 1) / denom) as i64;
        if raw_lots < 15 {
            15
        } else if raw_lots > 350 {
            350
        } else {
            raw_lots
        }
    }

    #[inline]
    #[must_use]
    pub fn passes_l1_queue_depth_and_wall_guard(
        self,
        entry_price: PriceTicks,
        realized_vol_5m: ScaledBps,
        learned_edge_bps_x100: i64,
        own_l1_lots: i64,
        opp_l1_lots: i64,
        ofi_abs: f64,
        tfi_abs: f64,
    ) -> bool {
        let min_q = self.min_resting_l1_lots(entry_price, realized_vol_5m);
        if own_l1_lots < min_q {
            return false;
        }
        let edge_bonus = if learned_edge_bps_x100 > 0 {
            (learned_edge_bps_x100 / 10).min(45)
        } else {
            0
        };
        let max_q = (min_q.saturating_mul(65 + edge_bonus)).clamp(2_500, 25_000);
        let tot_q = (own_l1_lots.max(0) + opp_l1_lots.max(0)).max(1);
        let tfi_exc = (tfi_abs - ofi_abs).max(0.0);
        let max_q_permille = 720 + ((tfi_exc * 100.0) as i64).min(60);
        own_l1_lots <= max_q && (own_l1_lots * 1_000) / tot_q <= max_q_permille
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
        if self.symbol_id.0 == 1 {
            if wallet_usdt_1e8 >= 26_000_000_000 && !in_scar_debt {
                return LotQty(2);
            }
            return LotQty(1);
        }
        let w_1e4 = wallet_usdt_1e8 / 10_000;
        let w_clamped = if w_1e4 < 500_000 {
            500_000
        } else if w_1e4 > 100_000_000 {
            100_000_000
        } else {
            w_1e4
        };
        let base_notional_1e4 = (w_clamped * 29) / 100;
        let notional_clamped = if base_notional_1e4 < 180_000 {
            180_000
        } else if base_notional_1e4 > 2_500_000 {
            2_500_000
        } else {
            base_notional_1e4
        };
        let kappa_permille = if in_scar_debt {
            650
        } else {
            let edge_exc = if learned_edge_bps_x100 > 150 {
                learned_edge_bps_x100 - 150
            } else {
                0
            };
            let raw_k = 1_000 + edge_exc;
            if raw_k > 1_450 { 1_450 } else { raw_k }
        };
        let eff_notional_1e4 = (notional_clamped * kappa_permille) / 1_000;
        let px_safe = if entry_price.0 > 1 { entry_price.0 } else { 1 };
        let lot_usdt_1e4 = ((px_safe as i128)
            * (self.tick_size_1e8 as i128)
            * (self.lot_step_1e8 as i128))
            / 1_000_000_000_000;
        let denom = if lot_usdt_1e4 > 1 { lot_usdt_1e4 as i64 } else { 1 };
        let raw_lots = (eff_notional_1e4 + (denom / 2)) / denom;
        let min_lots = if self.symbol_id.0 == 2 || self.symbol_id.0 == 5 {
            2
        } else {
            4
        };
        let bounded = if raw_lots < min_lots { min_lots } else { raw_lots };
        let even_lots = if (bounded & 1) != 0 { bounded + 1 } else { bounded };
        LotQty(even_lots)
    }

    #[inline]
    #[must_use]
    pub fn format_lot_qty_str(self, lots: LotQty) -> String {
        let l_qty = lots.0.max(1);
        let sc = l_qty.saturating_mul(self.lot_step_1e8);
        let wh = sc / 100_000_000;
        let fr = (sc % 100_000_000).abs();
        if fr == 0 {
            format!("{}", wh)
        } else {
            let s_f = format!("{:08}", fr);
            format!("{}.{}", wh, s_f.trim_end_matches('0'))
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

/// Classifies trade with Dark Green for Partial Harvests (50%), Light Green for Wins, Celeste for Scratches.
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
        let wallet_70_usdt = 7_088_000_000_i64;
        let wallet_200_usdt = 20_000_000_000_i64;
        let sample_ticks: [i64; 12] = [
            759_000, 223_700, 8_930, 8_170, 22_400, 8_510,
            3_130, 16_140, 1_598, 10_720, 3_690, 5_176,
        ];
        for (idx, spec) in APPROVED_SPECS.iter().enumerate() {
            let px = PriceTicks(sample_ticks[idx]);
            let elig = spec.validate_pair_eligibility(px, ScaledBps(6_500));
            assert!(elig.is_ok());
            let lots_70 = spec.compute_compounding_kelly_lots(wallet_70_usdt, px, 250, false);
            let lots_200 = spec.compute_compounding_kelly_lots(wallet_200_usdt, px, 350, false);
            assert!(lots_70.0 >= 1);
            assert!(lots_200.0 >= lots_70.0);
            if idx >= 1 {
                assert_eq!(lots_70.0 % 2, 0);
                assert_eq!(lots_200.0 % 2, 0);
            }
        }
    }
}
