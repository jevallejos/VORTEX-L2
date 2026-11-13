//! Execution FSM, Quantum Floors, Anti-Sweep dQ/dt, Wald SL Ratchet & Bayesian Governor.

use crate::alpha_3factor::OrthogonalFactorState;
use crate::forensic_parquet::ParitySyncLedger;
use crate::lob_shm::{
    SlotTelemetrySnapshot, SymbolLobSnapshot, VortexShmArena, MAX_ACTIVE_PAIRS,
};
use crate::types::{
    LotQty, PairSpec, PriceTicks, ScaledBps, Side, TimestampNs, VortexError, APPROVED_SPECS,
};

pub const MAX_CLOSE_RECONCILE_RETRIES: u8 = 5;
pub const UNIFIED_LOSS_LAB_COUNT: usize = 63;
const VOL_5M_HORIZON_NS: i64 = 300_000_000_000;
const INIT_RETURN_VAR_BPS2: i64 = 40_000;

#[inline]
const fn c_min_i64(a: i64, b: i64) -> i64 {
    if a < b { a } else { b }
}

#[inline]
const fn c_max_i64(a: i64, b: i64) -> i64 {
    if a > b { a } else { b }
}

#[inline]
#[allow(clippy::manual_clamp)]
const fn c_clamp_i64(val: i64, lo: i64, hi: i64) -> i64 {
    if val < lo {
        lo
    } else if val > hi {
        hi
    } else {
        val
    }
}

#[inline]
const fn c_min_u32(a: u32, b: u32) -> u32 {
    if a < b { a } else { b }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ClosedTradeKind {
    MakerPassiveUnwind,
    ServerMarkPriceStopLoss,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ExitSubReason {
    Tranche1Harvest50,
    LvdbsDeepVacuumSurf,
    AdverseScratchEscape,
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

/// Explicit role of the active maker exit order on the exchange.
#[repr(u8)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum ExitOrderRole {
    #[default]
    None = 0,
    Tranche1Harvest50 = 1,
    LvdbsDeepSurf50 = 2,
    StandardMakerExit100 = 3,
}

pub type ScarEventKey = (i64, usize, u8);
pub type ScarEventPayload = (PriceTicks, ScaledBps, bool);

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct CanonicalExitQuote {
    pub role: ExitOrderRole,
    pub sub_reason: ExitSubReason,
    pub target_price: PriceTicks,
    pub order_lots: LotQty,
    pub vac_level: usize,
    pub cur_gross_bps: i64,
    pub wald_pct: i64,
    pub defensive_lock_active: bool,
    pub urgent_close_repeg: bool,
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

const SHA256_K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1,
    0x923f82a4, 0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
    0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786,
    0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147,
    0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
    0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
    0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a,
    0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
    0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

#[inline]
#[allow(clippy::needless_range_loop)]
fn sha256_compress_block(h: &mut [u32; 8], chunk: &[u8; 64]) {
    let mut w = [0_u32; 64];
    for i in 0..16 {
        let j = i * 4;
        w[i] = u32::from_be_bytes([chunk[j], chunk[j + 1], chunk[j + 2], chunk[j + 3]]);
    }
    for i in 16..64 {
        let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
        let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
        w[i] = w[i - 16]
            .wrapping_add(s0)
            .wrapping_add(w[i - 7])
            .wrapping_add(s1);
    }
    let (mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh) =
        (h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7]);
    for i in 0..64 {
        let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
        let ch = (e & f) ^ ((!e) & g);
        let temp1 = hh
            .wrapping_add(s1)
            .wrapping_add(ch)
            .wrapping_add(SHA256_K[i])
            .wrapping_add(w[i]);
        let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
        let maj = (a & b) ^ (a & c) ^ (b & c);
        let temp2 = s0.wrapping_add(maj);
        hh = g;
        g = f;
        f = e;
        e = d.wrapping_add(temp1);
        d = c;
        c = b;
        b = a;
        a = temp1.wrapping_add(temp2);
    }
    h[0] = h[0].wrapping_add(a);
    h[1] = h[1].wrapping_add(b);
    h[2] = h[2].wrapping_add(c);
    h[3] = h[3].wrapping_add(d);
    h[4] = h[4].wrapping_add(e);
    h[5] = h[5].wrapping_add(f);
    h[6] = h[6].wrapping_add(g);
    h[7] = h[7].wrapping_add(hh);
}

#[allow(clippy::needless_range_loop)]
fn sha256_digest_2parts(part1: &[u8], part2: &[u8]) -> [u8; 32] {
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
        0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
    ];
    let total_len = part1.len().saturating_add(part2.len());
    let bit_len = (total_len as u64).wrapping_mul(8);
    let mut block = [0_u8; 64];
    let mut fill = 0_usize;

    for &b in part1.iter().chain(part2.iter()) {
        block[fill] = b;
        fill += 1;
        if fill == 64 {
            sha256_compress_block(&mut h, &block);
            fill = 0;
        }
    }
    block[fill] = 0x80;
    fill += 1;
    if fill > 56 {
        while fill < 64 {
            block[fill] = 0;
            fill += 1;
        }
        sha256_compress_block(&mut h, &block);
        fill = 0;
    }
    while fill < 56 {
        block[fill] = 0;
        fill += 1;
    }
    block[56..64].copy_from_slice(&bit_len.to_be_bytes());
    sha256_compress_block(&mut h, &block);

    let mut out = [0_u8; 32];
    for (i, word) in h.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    out
}

#[must_use]
pub fn sign_bybit_v5_hmac_sha256(secret: &str, payload: &str) -> String {
    let mut key_block = [0_u8; 64];
    let sec_bytes = secret.as_bytes();
    if sec_bytes.len() > 64 {
        let hashed = sha256_digest_2parts(sec_bytes, &[]);
        key_block[..32].copy_from_slice(&hashed);
    } else {
        key_block[..sec_bytes.len()].copy_from_slice(sec_bytes);
    }
    let mut ipad = [0x36_u8; 64];
    let mut opad = [0x5c_u8; 64];
    for i in 0..64 {
        ipad[i] ^= key_block[i];
        opad[i] ^= key_block[i];
    }
    let inner_hash = sha256_digest_2parts(&ipad, payload.as_bytes());
    let digest = sha256_digest_2parts(&opad, &inner_hash);

    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut hex_str = String::with_capacity(64);
    for &b in &digest {
        hex_str.push(HEX[(b >> 4) as usize] as char);
        hex_str.push(HEX[(b & 0x0f) as usize] as char);
    }
    hex_str
}

/// Parses Bybit V5 avgPrice into conservative directional PriceTicks (ceil Bid, floor Ask).
pub fn parse_verified_avg_price_ticks(
    spec: PairSpec,
    side: Side,
    decimal_str: &str,
) -> Result<PriceTicks, VortexError> {
    let s = decimal_str.trim();
    if s.is_empty() || spec.tick_size_1e8 <= 0 {
        return Err(VortexError::AsciiDecimalParseError);
    }
    let mut whole: i128 = 0;
    let mut frac: i128 = 0;
    let mut frac_digits: u32 = 0;
    let mut in_frac = false;
    let mut has_extra_nonzero_frac = false;
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
        } else if d > 0 {
            has_extra_nonzero_frac = true;
        }
    }
    while frac_digits < 8 {
        frac = frac.saturating_mul(10);
        frac_digits += 1;
    }
    let total_1e8 = whole.saturating_mul(100_000_000).saturating_add(frac);
    if total_1e8 <= 0 && !has_extra_nonzero_frac {
        return Err(VortexError::InvalidPriceOrTickSpec);
    }
    let tick = spec.tick_size_1e8 as i128;
    let ticks = match side {
        Side::Bid => {
            let extra = i128::from(has_extra_nonzero_frac);
            (total_1e8 + extra + tick - 1) / tick
        }
        Side::Ask => total_1e8 / tick,
    };
    if ticks <= 0 {
        return Err(VortexError::InvalidPriceOrTickSpec);
    }
    Ok(PriceTicks(ticks as i64))
}

/// Parses Bybit V5 avgExitPrice conservatively (floor for Long Bid, ceil for Short Ask).
#[inline]
pub fn parse_verified_exit_price_ticks(
    spec: PairSpec,
    open_side: Side,
    decimal_str: &str,
) -> Result<PriceTicks, VortexError> {
    parse_verified_avg_price_ticks(spec, open_side.opposite(), decimal_str)
}

/// Parses signed decimal strings (e.g. Bybit closedPnl) into exact integer 1e8 units.
pub fn parse_signed_decimal_1e8(decimal_str: &str) -> Result<i64, VortexError> {
    let s = decimal_str.trim();
    if s.is_empty() {
        return Err(VortexError::AsciiDecimalParseError);
    }
    let bytes = s.as_bytes();
    let (neg, start_idx) = if bytes[0] == b'-' {
        (true, 1_usize)
    } else if bytes[0] == b'+' {
        (false, 1_usize)
    } else {
        (false, 0_usize)
    };
    if start_idx >= bytes.len() {
        return Err(VortexError::AsciiDecimalParseError);
    }
    let mut whole: i128 = 0;
    let mut frac: i128 = 0;
    let mut frac_digits: u32 = 0;
    let mut in_frac = false;
    let mut saw_digit = false;
    for &b in &bytes[start_idx..] {
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
        saw_digit = true;
        let d = i128::from(b - b'0');
        if !in_frac {
            whole = whole.saturating_mul(10).saturating_add(d);
        } else if frac_digits < 8 {
            frac = frac.saturating_mul(10).saturating_add(d);
            frac_digits += 1;
        }
    }
    if !saw_digit {
        return Err(VortexError::AsciiDecimalParseError);
    }
    while frac_digits < 8 {
        frac = frac.saturating_mul(10);
        frac_digits += 1;
    }
    let mag = whole.saturating_mul(100_000_000).saturating_add(frac);
    let clamped = mag.min(i64::MAX as i128) as i64;
    Ok(if neg { -clamped } else { clamped })
}

pub const OFFENSIVE_WIN_FLOOR_GROSS_BPS: i64 = 515;

/// Offensive Maker WIN floor (>= +5.15..+5.70 bps gross => >= +1.15..+1.70 bps net WIN).
#[inline]
#[must_use]
pub fn offensive_win_maker_exit_price(
    side: Side,
    avg_price_ticks: PriceTicks,
    hurst_exponent: f64,
) -> PriceTicks {
    let h_bonus = if hurst_exponent > 0.52 {
        (((hurst_exponent - 0.52) * 160.0).round() as i64).clamp(0, 55)
    } else {
        0
    };
    let win_bps = ScaledBps(OFFENSIVE_WIN_FLOOR_GROSS_BPS + h_bonus);
    dynamic_maker_floor_price(side, avg_price_ticks, win_bps)
}
/// Tick-quantized Zero-Loss Quantum Floor (4.00 bps Maker-Maker fee + >= 0.40 bps net).
#[inline]
#[must_use]
pub const fn defensive_zero_loss_maker_exit_price(
    side: Side,
    entry_price: PriceTicks,
) -> PriceTicks {
    let min_gross_bps = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2 + 40;
    let quantum_ticks = (entry_price.0.saturating_mul(min_gross_bps) + 999_999) / 1_000_000;
    let min_delta = if quantum_ticks > 2 { quantum_ticks } else { 2 };
    match side {
        Side::Bid => PriceTicks(entry_price.0 + min_delta),
        Side::Ask => {
            let p = entry_price.0 - min_delta;
            PriceTicks(if p > 1 { p } else { 1 })
        }
    }
}

#[inline]
#[must_use]
pub const fn dynamic_maker_floor_price(
    side: Side,
    entry_price: PriceTicks,
    reference_gross_bps: ScaledBps,
) -> PriceTicks {
    let quantum_floor = defensive_zero_loss_maker_exit_price(side, entry_price);
    let fee_rt_bps = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
    let g = reference_gross_bps.raw();
    if g <= fee_rt_bps {
        return quantum_floor;
    }
    let target_ticks = (entry_price.0.saturating_mul(g) + 999_999) / 1_000_000;
    let min_delta = if target_ticks > 2 { target_ticks } else { 2 };
    match side {
        Side::Bid => {
            let cand = entry_price.0 + min_delta;
            PriceTicks(if cand > quantum_floor.0 { cand } else { quantum_floor.0 })
        }
        Side::Ask => {
            let p = entry_price.0 - min_delta;
            let cand = if p > 1 { p } else { 1 };
            PriceTicks(if cand < quantum_floor.0 { cand } else { quantum_floor.0 })
        }
    }
}

/// Moallemi-Yuan L1 Queue Depletion & Decoupled L2 Sweep Shield (anti-Picked-Off Fill).
#[inline]
#[must_use]
pub fn check_queue_depletion_velocity_abort(
    side: Side,
    init_queue_ahead: LotQty,
    current_queue_ahead: LotQty,
    snap: &SymbolLobSnapshot,
) -> bool {
    let dir_s = side.sign_f64();
    let signed_ofi = snap.factor_ofi.get() * dir_s;
    let signed_tfi = snap.factor_tfi.get() * dir_s;
    let signed_geom = snap.factor_geom.get() * dir_s;
    let flow_impulse = 0.55 * signed_ofi + 0.45 * signed_tfi;
    if flow_impulse <= -0.42 || (signed_ofi <= -0.22 && signed_geom <= -0.05) {
        return true;
    }
    if init_queue_ahead.0 <= 0 {
        return false;
    }
    let ref_px = snap.bids[0].price.0.max(snap.asks[0].price.0).max(1);
    let b3x_raw = crate::types::spec_from_symbol_id(snap.symbol_id)
        .and_then(|sp| sp.required_3x_barrier_bps(PriceTicks(ref_px)).ok())
        .unwrap_or(ScaledBps::MIN_REQUIRED_VOL_5M_BPS)
        .raw();
    let eq_anchor = (b3x_raw.saturating_mul(4)).max(1) as f64;
    let vol_ratio = (snap.realized_vol_5m.raw().max(b3x_raw) as f64) / eq_anchor;
    let nu_sigma = vol_ratio.sqrt().clamp(0.80, 1.35);
    let depleted_lots = (init_queue_ahead.0 - current_queue_ahead.0).max(0) as f64;
    let depleted_frac = depleted_lots / (init_queue_ahead.0 as f64);
    let sweep_book_weak = flow_impulse <= (-0.04 * nu_sigma)
        || signed_ofi <= 0.02
        || signed_geom <= -0.10;
    let tail_exposed =
        depleted_frac >= (0.78 * nu_sigma).min(0.88) && signed_ofi < 0.14;
    (depleted_frac >= (0.50 * nu_sigma) && sweep_book_weak) || tail_exposed
}

#[inline]
fn extract_matrix_i64_12x2(
    val: &serde_json::Value,
    key: &str,
    dst: &mut [[i64; 2]; MAX_ACTIVE_PAIRS],
) {
    if let Some(rows) = val.get(key).and_then(|v| v.as_array()) {
        for (s, row_v) in rows.iter().take(MAX_ACTIVE_PAIRS).enumerate() {
            if let Some(cols) = row_v.as_array() {
                for (d, cell) in cols.iter().take(2).enumerate() {
                    if let Some(n) = cell.as_i64() {
                        dst[s][d] = n;
                    }
                }
            }
        }
    }
}

#[inline]
fn extract_matrix_u32_12x2(
    val: &serde_json::Value,
    key: &str,
    dst: &mut [[u32; 2]; MAX_ACTIVE_PAIRS],
) {
    if let Some(rows) = val.get(key).and_then(|v| v.as_array()) {
        for (s, row_v) in rows.iter().take(MAX_ACTIVE_PAIRS).enumerate() {
            if let Some(cols) = row_v.as_array() {
                for (d, cell) in cols.iter().take(2).enumerate() {
                    if let Some(n) = cell.as_u64() {
                        dst[s][d] = n.min(u64::from(u32::MAX)) as u32;
                    }
                }
            }
        }
    }
}

/// 12x2 Online Bayesian Governor with Lyapunov Crest Shield, Welford Var & Intrinsic Clock.
#[derive(Clone, Debug)]
pub struct OnlineAdaptiveGovernor {
    ewma_net_bps_x100: [[i64; 2]; MAX_ACTIVE_PAIRS],
    ewma_var_bps2: [[i64; 2]; MAX_ACTIVE_PAIRS],
    eq_vol_5m_bps: [i64; MAX_ACTIVE_PAIRS],
    cum_net_bps_x100: [[i64; 2]; MAX_ACTIVE_PAIRS],
    sl_streak: [[u32; 2]; MAX_ACTIVE_PAIRS],
    win_streak: [[u32; 2]; MAX_ACTIVE_PAIRS],
    directional_cooldown_until_ns: [[i64; 2]; MAX_ACTIVE_PAIRS],
    global_dir_debt_bps_x100: [i64; 2],
    global_dir_sl_streak: [u32; 2],
    global_dir_win_streak: [u32; 2],
    global_dir_cooldown_until_ns: [i64; 2],
    last_global_win_dir: u8,
    ewma_fast_px_1e4: [i64; MAX_ACTIVE_PAIRS],
    ewma_slow_px_1e4: [i64; MAX_ACTIVE_PAIRS],
    last_price_sample_ts_ns: [i64; MAX_ACTIVE_PAIRS],
    wave_anchor_px_1e4: [[i64; 2]; MAX_ACTIVE_PAIRS],
    wave_anchor_ts_ns: [[i64; 2]; MAX_ACTIVE_PAIRS],
    abort_streak: [[u32; 2]; MAX_ACTIVE_PAIRS],
    abort_anchor_px_1e4: [[i64; 2]; MAX_ACTIVE_PAIRS],
    abort_last_ts_ns: [[i64; 2]; MAX_ACTIVE_PAIRS],
}

impl Default for OnlineAdaptiveGovernor {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

#[allow(clippy::manual_clamp, clippy::manual_range_contains, clippy::too_many_arguments)]
impl OnlineAdaptiveGovernor {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        let init_eq_vol = ScaledBps::MIN_REQUIRED_VOL_5M_BPS.raw() * 4;
        Self {
            ewma_net_bps_x100: [[ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw(); 2]; MAX_ACTIVE_PAIRS],
            ewma_var_bps2: [[INIT_RETURN_VAR_BPS2; 2]; MAX_ACTIVE_PAIRS],
            eq_vol_5m_bps: [init_eq_vol; MAX_ACTIVE_PAIRS],
            cum_net_bps_x100: [[0; 2]; MAX_ACTIVE_PAIRS],
            sl_streak: [[0; 2]; MAX_ACTIVE_PAIRS],
            win_streak: [[0; 2]; MAX_ACTIVE_PAIRS],
            directional_cooldown_until_ns: [[0; 2]; MAX_ACTIVE_PAIRS],
            global_dir_debt_bps_x100: [0; 2],
            global_dir_sl_streak: [0; 2],
            global_dir_win_streak: [0; 2],
            global_dir_cooldown_until_ns: [0; 2],
            last_global_win_dir: 255,
            ewma_fast_px_1e4: [0; MAX_ACTIVE_PAIRS],
            ewma_slow_px_1e4: [0; MAX_ACTIVE_PAIRS],
            last_price_sample_ts_ns: [0; MAX_ACTIVE_PAIRS],
            wave_anchor_px_1e4: [[0; 2]; MAX_ACTIVE_PAIRS],
            wave_anchor_ts_ns: [[0; 2]; MAX_ACTIVE_PAIRS],
            abort_streak: [[0; 2]; MAX_ACTIVE_PAIRS],
            abort_anchor_px_1e4: [[i64::MIN; 2]; MAX_ACTIVE_PAIRS],
            abort_last_ts_ns: [[0; 2]; MAX_ACTIVE_PAIRS],
        }
    }

    #[inline]
    #[must_use]
    pub const fn dir_idx(side: Side) -> usize {
        match side {
            Side::Bid => 0,
            Side::Ask => 1,
        }
    }

    /// Online Welford-EWMA return standard deviation in ScaledBps.
    #[inline]
    #[must_use]
    pub const fn slot_return_std_bps(&self, slot_idx: usize, side: Side) -> i64 {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return 200;
        }
        let d = Self::dir_idx(side);
        let var_u = if self.ewma_var_bps2[slot_idx][d] > 1 {
            self.ewma_var_bps2[slot_idx][d] as u64
        } else {
            1_u64
        };
        var_u.isqrt() as i64
    }

    /// Bayesian Information Ratio x100: IR_{s,d} = mu / (sqrt(var) + fee_rt).
    #[inline]
    #[must_use]
    pub const fn slot_information_ratio_x100(&self, slot_idx: usize, side: Side) -> i64 {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return 0;
        }
        let d = Self::dir_idx(side);
        let mu = self.ewma_net_bps_x100[slot_idx][d];
        let sigma = self.slot_return_std_bps(slot_idx, side);
        let fee_rt = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
        (mu.saturating_mul(100)) / c_max_i64(sigma.saturating_add(fee_rt), 1)
    }

    /// Continuous C1 intrinsic diffusion clock tau_mkt(s) (zero jump discontinuity).
    #[inline]
    #[must_use]
    pub const fn intrinsic_diffusion_clock_ns(&self, slot_idx: usize) -> i64 {
        let min_v = ScaledBps::MIN_REQUIRED_VOL_5M_BPS.raw();
        let ref_v = min_v * 4;
        let eq_v = if slot_idx < MAX_ACTIVE_PAIRS && self.eq_vol_5m_bps[slot_idx] > 0 {
            c_max_i64(self.eq_vol_5m_bps[slot_idx], min_v)
        } else {
            ref_v
        };
        let raw_ns = ((VOL_5M_HORIZON_NS as i128) * (ref_v as i128)) / (eq_v as i128);
        let lo = VOL_5M_HORIZON_NS / 3;
        let hi = VOL_5M_HORIZON_NS * 3;
        if raw_ns < (lo as i128) {
            lo
        } else if raw_ns > (hi as i128) {
            hi
        } else {
            raw_ns as i64
        }
    }

    /// Self-learned Tranche 1 harvest fraction in permille [350, 650] (0.35..0.65).
    #[inline]
    #[must_use]
    pub const fn learned_tranche1_fraction_permille(
        &self,
        slot_idx: usize,
        side: Side,
        learned_mfe_bps: i64,
    ) -> i64 {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return 500;
        }
        let fee_rt = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
        let ir_x100 = c_clamp_i64(
            self.slot_information_ratio_x100(slot_idx, side),
            -60,
            120,
        );
        let mfe_c = c_clamp_i64(learned_mfe_bps, fee_rt, fee_rt * 4);
        let ir_shift = (ir_x100 - 40) * 2;
        let mfe_shift = ((mfe_c - 2 * fee_rt) * 125) / c_max_i64(fee_rt, 1);
        let scar_add = if self.is_in_scar_debt(slot_idx, side) {
            120
        } else {
            0
        };
        c_clamp_i64(500 - ir_shift - mfe_shift + scar_add, 350, 650)
    }

    /// Bayesian MAE trigger (< 0 ScaledBps) & intrinsic T1 defensive horizon in seconds.
    #[inline]
    #[must_use]
    pub fn elastic_mae_trigger_and_def_horizon_s(
        &self,
        slot_idx: usize,
        side: Side,
        avg_price_ticks: PriceTicks,
        realized_vol_5m: ScaledBps,
        learned_mfe_bps: i64,
    ) -> (i64, i64) {
        let s_clamped = slot_idx.min(MAX_ACTIVE_PAIRS - 1);
        let spec = APPROVED_SPECS[s_clamped];
        let fee_rt = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
        let tau_bps = spec
            .tick_to_price_scaled_bps(avg_price_ticks)
            .unwrap_or(ScaledBps(fee_rt / 8))
            .raw();
        let sl_dist_bps = PairSpec::elastic_server_sl_distance_bps(realized_vol_5m).raw();
        let ir_x100 = self
            .slot_information_ratio_x100(s_clamped, side)
            .clamp(-60, 120);
        let ret_std = self.slot_return_std_bps(s_clamped, side);
        let mfe_c = learned_mfe_bps.clamp((fee_rt * 7) / 5, fee_rt * 4);

        let gamma_permille = (220
            + (ir_x100 * 5) / 6
            + ((mfe_c - 2 * fee_rt) * 100) / (10 * fee_rt).max(1))
        .clamp(180, 340);
        let sl_cushion_bps = (sl_dist_bps.saturating_mul(gamma_permille)) / 1_000;
        let mae_mag = (4 * tau_bps).max(sl_cushion_bps).max(ret_std / 6);

        let tau_mkt_s = (self.intrinsic_diffusion_clock_ns(s_clamped) / 1_000_000_000).max(90);
        let init_mfe = (fee_rt * 17) / 10;
        let ir_mult_permille = 1_000 + (ir_x100.clamp(-40, 80) * 10) / 3;
        let raw_def_s = ((((tau_mkt_s / 4) * mfe_c) / init_mfe.max(1))
            * ir_mult_permille)
            / 1_000;
        let t_def_s = raw_def_s.clamp(tau_mkt_s / 6, (tau_mkt_s * 2) / 5);
        (-mae_mag, t_def_s)
    }

    #[inline]
    #[must_use]
    const fn elastic_wave_anchor_ttl_ns(&self, slot_idx: usize, d: usize) -> i64 {
        let tau_ns = self.intrinsic_diffusion_clock_ns(slot_idx);
        let fee_rt = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
        let edge_pos = if self.ewma_net_bps_x100[slot_idx][d] > 0 {
            c_min_i64(self.ewma_net_bps_x100[slot_idx][d], fee_rt * 2)
        } else {
            0
        };
        (tau_ns.saturating_mul(2 * fee_rt + edge_pos)) / fee_rt
    }

    #[inline]
    pub fn observe_market_price(&mut self, slot_idx: usize, snap: &SymbolLobSnapshot) {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return;
        }
        let mid_ticks = if snap.bids[0].price.0 > 0 && snap.asks[0].price.0 > 0 {
            i64::midpoint(snap.bids[0].price.0, snap.asks[0].price.0)
        } else {
            snap.mark_price.0
        };
        if mid_ticks <= 0 {
            return;
        }
        let px_1e4 = mid_ticks.saturating_mul(10_000);
        let b3x_floor = APPROVED_SPECS[slot_idx]
            .required_3x_barrier_bps(PriceTicks(mid_ticks))
            .unwrap_or(ScaledBps::MIN_REQUIRED_VOL_5M_BPS)
            .raw();
        let active_regime_floor = b3x_floor.saturating_mul(2);
        let obs_vol = snap.realized_vol_5m.raw();
        let fast_prev = self.ewma_fast_px_1e4[slot_idx];
        let slow_prev = self.ewma_slow_px_1e4[slot_idx];
        if fast_prev <= 0 || slow_prev <= 0 {
            self.ewma_fast_px_1e4[slot_idx] = px_1e4;
            self.ewma_slow_px_1e4[slot_idx] = px_1e4;
            if obs_vol > 0 {
                self.eq_vol_5m_bps[slot_idx] = obs_vol.max(active_regime_floor);
            }
            self.last_price_sample_ts_ns[slot_idx] = snap.ts_local_ns.0;
            return;
        }
        let sample_dt =
            snap.ts_local_ns.0.saturating_sub(self.last_price_sample_ts_ns[slot_idx]);
        if sample_dt < 1_000_000_000 {
            return;
        }
        self.last_price_sample_ts_ns[slot_idx] = snap.ts_local_ns.0;
        let dt_s = ((sample_dt as f64) * 1e-9).clamp(1.0, 30.0);
        let a_fast = (1.0 - (-dt_s / 20.0).exp()).clamp(0.02, 1.0);
        let a_slow = (1.0 - (-dt_s / 300.0).exp()).clamp(0.002, 1.0);
        self.ewma_fast_px_1e4[slot_idx] =
            (fast_prev as f64 + a_fast * ((px_1e4 - fast_prev) as f64)).round() as i64;
        self.ewma_slow_px_1e4[slot_idx] =
            (slow_prev as f64 + a_slow * ((px_1e4 - slow_prev) as f64)).round() as i64;
        if obs_vol > 0 {
            let prev_eq = self.eq_vol_5m_bps[slot_idx].max(active_regime_floor) as f64;
            let obs_f = (obs_vol.max(active_regime_floor)) as f64;
            let tau_eq = if obs_f > prev_eq { 120.0 } else { 900.0 };
            let a_eq = (1.0 - (-dt_s / tau_eq).exp()).clamp(0.001, 1.0);
            let next_eq = (prev_eq + a_eq * (obs_f - prev_eq)).round() as i64;
            self.eq_vol_5m_bps[slot_idx] = next_eq.clamp(active_regime_floor, 24_000);
        }
    }

    #[inline]
    #[must_use]
    pub const fn price_drift_and_extension_scaled_bps(
        &self,
        slot_idx: usize,
        snap: &SymbolLobSnapshot,
    ) -> (i64, i64) {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return (0, 0);
        }
        let fast = self.ewma_fast_px_1e4[slot_idx];
        let slow = self.ewma_slow_px_1e4[slot_idx];
        if fast <= 0 || slow <= 0 {
            return (0, 0);
        }
        let drift_bps = ((fast - slow).saturating_mul(1_000_000)) / slow;
        let cur_ticks = if snap.bids[0].price.0 > 0 && snap.asks[0].price.0 > 0 {
            i64::midpoint(snap.bids[0].price.0, snap.asks[0].price.0)
        } else {
            snap.mark_price.0
        };
        let cur_1e4 = cur_ticks.saturating_mul(10_000);
        let ext_bps = if cur_1e4 > 0 {
            ((cur_1e4 - slow).saturating_mul(1_000_000)) / slow
        } else {
            0
        };
        (drift_bps, ext_bps)
    }

    #[inline]
    #[must_use]
    pub const fn slot_direction_diagnostics(&self, slot_idx: usize, side: Side) -> (i64, u32, u32) {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return (0, 0, 0);
        }
        let d = Self::dir_idx(side);
        (
            self.ewma_net_bps_x100[slot_idx][d],
            self.sl_streak[slot_idx][d],
            self.win_streak[slot_idx][d],
        )
    }

    /// Evaluates strictly local slot scar debt (prevents cross-asset Bid deadlock on Slot 0).
    #[inline]
    #[must_use]
    pub const fn is_in_local_slot_scar_debt(&self, slot_idx: usize, side: Side) -> bool {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return false;
        }
        let d = Self::dir_idx(side);
        let local_floor = -(ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 8);
        self.cum_net_bps_x100[slot_idx][d] <= local_floor || self.sl_streak[slot_idx][d] > 0
    }

    /// Evaluates portfolio-wide directional or local scar debt for conservative sizing.
    #[inline]
    #[must_use]
    pub const fn is_in_scar_debt(&self, slot_idx: usize, side: Side) -> bool {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return false;
        }
        let d = Self::dir_idx(side);
        let global_floor = -(ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 30);
        self.is_in_local_slot_scar_debt(slot_idx, side)
            || self.global_dir_debt_bps_x100[d] < global_floor
    }

    #[inline]
    #[must_use]
    pub const fn audit_historical_entry_veto(
        &self,
        slot_idx: usize,
        side: Side,
        entry_ticks: i64,
        check_ts_ns: i64,
    ) -> Option<&'static str> {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return Some("VETO_INVALID_SLOT");
        }
        let d = Self::dir_idx(side);
        if check_ts_ns > 0 && check_ts_ns < self.directional_cooldown_until_ns[slot_idx][d] {
            return Some("VETO_SLOT_COOLDOWN");
        }
        if check_ts_ns > 0 && check_ts_ns < self.global_dir_cooldown_until_ns[d] {
            return Some("VETO_GLOBAL_WAVE_OR_SL_BREATHER");
        }
        let fee_rt = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
        let tau_ns = self.intrinsic_diffusion_clock_ns(slot_idx);
        let anchor_1e4 = self.wave_anchor_px_1e4[slot_idx][d];
        let anchor_ts = self.wave_anchor_ts_ns[slot_idx][d];
        let wave_fresh = check_ts_ns <= 0
            || anchor_ts <= 0
            || check_ts_ns.saturating_sub(anchor_ts)
                <= self.elastic_wave_anchor_ttl_ns(slot_idx, d);
        let cur_1e4 = entry_ticks.saturating_mul(10_000);
        let wave_active = self.win_streak[slot_idx][d] >= 3
            || (self.win_streak[slot_idx][d] >= 1 && self.global_dir_win_streak[d] >= 3);
        if wave_active && wave_fresh && anchor_1e4 > 0 && cur_1e4 > 0 {
            let chased_bps = match side {
                Side::Bid => ((cur_1e4 - anchor_1e4).saturating_mul(1_000_000)) / anchor_1e4,
                Side::Ask => ((anchor_1e4 - cur_1e4).saturating_mul(1_000_000)) / anchor_1e4,
            };
            let edge_adj = c_clamp_i64(
                self.ewma_net_bps_x100[slot_idx][d],
                -fee_rt,
                fee_rt,
            ) / 4;
            let stair_min = fee_rt * 6 + edge_adj;
            let stair_max = self.eq_vol_5m_bps[slot_idx].saturating_mul(2) + fee_rt * 8;
            if chased_bps > stair_min && chased_bps < stair_max {
                return Some("VETO_WAVE_STAIRCASE_25BPS");
            }
        }
        let abort_breather_ns = tau_ns / 20;
        if self.abort_streak[slot_idx][d] >= 4
            && self.abort_anchor_px_1e4[slot_idx][d] > 0
            && check_ts_ns > 0
            && check_ts_ns.saturating_sub(self.abort_last_ts_ns[slot_idx][d]) < abort_breather_ns
        {
            return Some("VETO_ABORT_STREAK_BREATHER");
        }
        let deep_debt_floor = -(fee_rt * 11);
        if self.sl_streak[slot_idx][d] >= 1
            && self.cum_net_bps_x100[slot_idx][d] <= deep_debt_floor
        {
            let slot_cd = self.directional_cooldown_until_ns[slot_idx][d];
            let debt_mult = c_clamp_i64(
                (-self.cum_net_bps_x100[slot_idx][d]) / fee_rt,
                2,
                16,
            );
            let sl_m = c_min_u32(self.sl_streak[slot_idx][d], 4) as i64;
            let deep_q_ns = (tau_ns.saturating_mul(debt_mult).saturating_mul(sl_m)) / 2;
            if check_ts_ns <= 0 || slot_cd <= 0 || check_ts_ns < slot_cd.saturating_add(deep_q_ns)
            {
                return Some("VETO_DEEP_SCAR_DEBT_QUARANTINE");
            }
        }
        if slot_idx > 0 {
            let glob_cd = self.global_dir_cooldown_until_ns[d];
            if self.global_dir_sl_streak[d] >= 1 && self.last_global_win_dir == ((1 - d) as u8) {
                let g_sl_m = c_min_u32(self.global_dir_sl_streak[d], 4) as i64;
                let dom_ns = tau_ns.saturating_mul(g_sl_m) * 2;
                if check_ts_ns <= 0 || glob_cd <= 0 || check_ts_ns < glob_cd.saturating_add(dom_ns)
                {
                    return Some("VETO_POST_SL_OPPOSITE_DOMINANCE");
                }
            }
            let symbol_total_net =
                self.cum_net_bps_x100[slot_idx][0] + self.cum_net_bps_x100[slot_idx][1];
            if symbol_total_net <= -(fee_rt * 6)
                && self.global_dir_debt_bps_x100[d] <= -(fee_rt * 25)
                && self.cum_net_bps_x100[slot_idx][d] <= 0
            {
                let glob_mult = c_clamp_i64(
                    (-self.global_dir_debt_bps_x100[d]) / (fee_rt * 4),
                    4,
                    16,
                );
                let contagion_ns = tau_ns.saturating_mul(glob_mult);
                if check_ts_ns <= 0
                    || glob_cd <= 0
                    || check_ts_ns < glob_cd.saturating_add(contagion_ns)
                {
                    return Some("VETO_SYMBOL_AND_GLOBAL_DEBT_CONTAGION");
                }
            }
        }
        None
    }

    /// Evaluates C1 Kinematic Free-Energy & Lyapunov Crest-Reversal Boundary (SCAR #63).
    #[inline]
    pub fn evaluate_kinematic_energy_surface(
        &self,
        slot_idx: usize,
        side: Side,
        drift_bps: i64,
        ext_bps: i64,
        signed_geom: f64,
        snr_abs: f64,
    ) -> Result<(i64, i64), &'static str> {
        let min_geom_floor = if snr_abs > 0.58 {
            (0.106 - (snr_abs - 0.58) * 0.85).clamp(0.02, 0.15)
        } else if snr_abs >= 0.50 {
            (0.15 - (snr_abs - 0.50) * 0.55).clamp(0.05, 0.15)
        } else {
            0.15
        };
        if signed_geom < min_geom_floor {
            return Err("VETO_LOW_L2_GEOM_VACUUM (<min_geom)");
        }
        let fee_rt = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
        let eq_vol = if slot_idx < MAX_ACTIVE_PAIRS {
            self.eq_vol_5m_bps[slot_idx].max(fee_rt * 6)
        } else {
            fee_rt * 12
        };
        let d_sign = side.sign_i64();
        let s_drift = drift_bps.saturating_mul(d_sign);
        let s_ext = ext_bps.saturating_mul(d_sign);
        let lead_impulse = s_ext.saturating_sub(s_drift);
        let absorp_span = (eq_vol / 64).clamp(fee_rt / 10, fee_rt / 3);
        let snr_exc = ((snr_abs - 0.55).max(0.0) / 0.35).clamp(0.0, 1.0);
        let pullback_floor =
            -(fee_rt / 26) - ((absorp_span as f64 * snr_exc).round() as i64);
        let max_ext_cap = (eq_vol / 2).max(fee_rt * 3);
        let base_lead = (fee_rt * 12) / 100;
        let lead_ext_gate = (fee_rt * 29) / 100;
        let elastic_lead_cap = base_lead
            + (((snr_abs - 0.58).max(0.0) * (eq_vol as f64 / 20.0)).round() as i64);

        let snr_pull = ((snr_abs - 0.50) / 0.35).clamp(0.0, 1.0);
        let base_neg_lead =
            -(fee_rt / 3 + (((fee_rt as f64) * 0.25 * snr_pull).round() as i64));
        let ext_crest_tighten = (s_drift - (fee_rt * 5) / 4).max(0) / 2;
        let min_lead_floor = base_neg_lead + ext_crest_tighten;

        if s_drift < pullback_floor
            || s_ext < pullback_floor
            || s_ext > max_ext_cap
            || (lead_impulse > 0 && lead_impulse > elastic_lead_cap && s_ext >= lead_ext_gate)
            || lead_impulse < min_lead_floor
        {
            return Err("VETO_KINEMATIC_PHASE_OR_EXHAUSTION");
        }
        let d = Self::dir_idx(side);
        let (learned_edge, ir_x100) = if slot_idx < MAX_ACTIVE_PAIRS {
            (
                self.ewma_net_bps_x100[slot_idx][d],
                self.slot_information_ratio_x100(slot_idx, side),
            )
        } else {
            (fee_rt / 2, 33)
        };
        let impulse_contrib = if lead_impulse >= 0 {
            lead_impulse.min(75) + (s_drift.max(0) / 2).min(85)
        } else if snr_abs >= 0.50 && s_drift >= pullback_floor && s_ext >= pullback_floor {
            ((s_drift.max(0) / 4) + ((snr_abs - 0.50) * 220.0).round() as i64).min(115)
        } else {
            (lead_impulse / 2).max(-60)
        };
        let geom_contrib = ((signed_geom - min_geom_floor).max(0.0) * 240.0).round() as i64;
        let snr_contrib = ((snr_abs - 0.42).max(0.0) * 280.0).round() as i64;
        let fresh_drift_cap = (eq_vol / 32).max((fee_rt * 7) / 20);
        let fresh_ext_cap = (eq_vol / 26).max((fee_rt * 9) / 20);
        let fresh_init_bonus = if s_drift >= pullback_floor
            && s_drift <= fresh_drift_cap
            && s_ext >= pullback_floor
            && s_ext <= fresh_ext_cap
            && lead_impulse >= -(fee_rt / 16)
            && lead_impulse <= elastic_lead_cap
            && snr_abs >= 0.45
        {
            (fee_rt * 19) / 80
        } else {
            0_i64
        };
        let e_kin = impulse_contrib + geom_contrib + snr_contrib + fresh_init_bonus;
        let base_e = (fee_rt * 5) / 16;
        let edge_deficit = (fee_rt / 2 - learned_edge).max(0) / 4;
        let ir_relief = (ir_x100 - 33).clamp(-60, 80) / 5;
        let req_e_min =
            (base_e + edge_deficit - ir_relief).clamp(fee_rt / 4, (fee_rt * 13) / 20);
        if e_kin < req_e_min {
            return Err("VETO_KINEMATIC_ENERGY_SURFACE (E_kin < E_min)");
        }
        Ok((e_kin, req_e_min))
    }

    /// Updates online mean, Welford-EWMA variance, wave anchor & intrinsic cooldowns.
    pub fn observe_closed_trade_at_entry(
        &mut self,
        slot_idx: usize,
        side: Side,
        entry_price: PriceTicks,
        net_scaled_bps: ScaledBps,
        is_maker_exit: bool,
        ts_ns: TimestampNs,
    ) {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return;
        }
        let d = Self::dir_idx(side);
        let fee_rt = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
        let prev_mean = self.ewma_net_bps_x100[slot_idx][d];
        let diff = net_scaled_bps.raw().saturating_sub(prev_mean);
        let sq_clamped = diff.saturating_mul(diff).min(25_000_000);
        self.ewma_var_bps2[slot_idx][d] =
            ((self.ewma_var_bps2[slot_idx][d] * 80 + sq_clamped * 20) / 100).max(2_500);
        self.ewma_net_bps_x100[slot_idx][d] = (prev_mean * 75 + net_scaled_bps.raw() * 25) / 100;
        self.cum_net_bps_x100[slot_idx][d] = self.cum_net_bps_x100[slot_idx][d]
            .saturating_add(net_scaled_bps.raw())
            .clamp(-(fee_rt * 60), fee_rt * 60);
        self.global_dir_debt_bps_x100[d] = self.global_dir_debt_bps_x100[d]
            .saturating_add(net_scaled_bps.raw())
            .clamp(-(fee_rt * 75), fee_rt * 30);

        let tau_ns = self.intrinsic_diffusion_clock_ns(slot_idx);
        let ir_x100 = self.slot_information_ratio_x100(slot_idx, side).clamp(-50, 100);

        if !is_maker_exit || net_scaled_bps.raw() < -(fee_rt / 8) {
            let s = self.sl_streak[slot_idx][d].saturating_add(1).min(4);
            self.sl_streak[slot_idx][d] = s;
            self.win_streak[slot_idx][d] = 0;
            self.global_dir_win_streak[d] = 0;
            self.wave_anchor_px_1e4[slot_idx][d] = 0;
            self.wave_anchor_ts_ns[slot_idx][d] = 0;
            if ts_ns.0 > 0 {
                let backoff_ns = (tau_ns / 2).saturating_mul(1_i64 << s.min(4));
                self.directional_cooldown_until_ns[slot_idx][d] =
                    ts_ns.0.saturating_add(backoff_ns);
            }
            let gs = self.global_dir_sl_streak[d].saturating_add(1).min(4);
            self.global_dir_sl_streak[d] = gs;
            if ts_ns.0 > 0 {
                let global_pause_ns = ((tau_ns * 3) / 5).saturating_mul(i64::from(gs));
                self.global_dir_cooldown_until_ns[d] = ts_ns.0.saturating_add(global_pause_ns);
            }
        } else if net_scaled_bps.raw() < fee_rt / 4 {
            if ts_ns.0 > 0 {
                let scratch_ns = (tau_ns * (120 - ir_x100 / 2)) / 360;
                let scratch_cd = ts_ns.0.saturating_add(scratch_ns);
                if scratch_cd > self.directional_cooldown_until_ns[slot_idx][d] {
                    self.directional_cooldown_until_ns[slot_idx][d] = scratch_cd;
                }
            }
        } else {
            self.clear_abort_streak(slot_idx, side);
            let prev_anchor_ts = self.wave_anchor_ts_ns[slot_idx][d];
            let anchor_expired = ts_ns.0 > 0
                && prev_anchor_ts > 0
                && ts_ns.0.saturating_sub(prev_anchor_ts)
                    > self.elastic_wave_anchor_ttl_ns(slot_idx, d);
            if entry_price.0 > 0
                && (self.wave_anchor_px_1e4[slot_idx][d] <= 0 || anchor_expired)
            {
                self.wave_anchor_px_1e4[slot_idx][d] = entry_price.0.saturating_mul(10_000);
            }
            if ts_ns.0 > 0 {
                self.wave_anchor_ts_ns[slot_idx][d] = ts_ns.0;
            }
            let w = if anchor_expired {
                1_u32
            } else {
                self.win_streak[slot_idx][d].saturating_add(1)
            };
            self.win_streak[slot_idx][d] = w;
            self.win_streak[slot_idx][1 - d] = 0;
            self.wave_anchor_px_1e4[slot_idx][1 - d] = 0;
            self.wave_anchor_ts_ns[slot_idx][1 - d] = 0;

            let gw = self.global_dir_win_streak[d].saturating_add(1);
            self.global_dir_win_streak[d] = gw;
            self.global_dir_win_streak[1 - d] = 0;
            self.last_global_win_dir = d as u8;
            if gw >= 6 {
                self.global_dir_win_streak[d] = 0;
            }
            if ts_ns.0 > 0 {
                let win_ns =
                    ((tau_ns * (100 - ir_x100 / 3)) / 1_500) * (w.min(2) as i64);
                let win_cd = ts_ns.0.saturating_add(win_ns);
                if win_cd > self.directional_cooldown_until_ns[slot_idx][d] {
                    self.directional_cooldown_until_ns[slot_idx][d] = win_cd;
                }
            }
            if self.sl_streak[slot_idx][d] > 0
                && w >= 3
                && self.cum_net_bps_x100[slot_idx][d] >= -(fee_rt * 8)
            {
                self.sl_streak[slot_idx][d] -= 1;
                self.win_streak[slot_idx][d] = 0;
            }
            if self.global_dir_sl_streak[d] > 0
                && self.global_dir_debt_bps_x100[d] >= -(fee_rt * 15)
            {
                self.global_dir_sl_streak[d] -= 1;
            }
        }
    }

    #[inline]
    pub fn clear_abort_streak(&mut self, slot_idx: usize, side: Side) {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return;
        }
        let d = Self::dir_idx(side);
        self.abort_streak[slot_idx][d] = 0;
        self.abort_anchor_px_1e4[slot_idx][d] = i64::MIN;
        self.abort_last_ts_ns[slot_idx][d] = 0;
    }

    #[inline]
    pub fn observe_entry_abort(
        &mut self,
        slot_idx: usize,
        side: Side,
        aborted_price: PriceTicks,
        ts_ns: TimestampNs,
    ) {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return;
        }
        let d = Self::dir_idx(side);
        let decay_win_ns = self.intrinsic_diffusion_clock_ns(slot_idx) / 5;
        let prev_ts = self.abort_last_ts_ns[slot_idx][d];
        if prev_ts > 0 && ts_ns.0 > prev_ts && ts_ns.0.saturating_sub(prev_ts) >= decay_win_ns {
            self.abort_streak[slot_idx][d] = 0;
        }
        self.abort_streak[slot_idx][d] = self.abort_streak[slot_idx][d].saturating_add(1).min(8);
        self.abort_anchor_px_1e4[slot_idx][d] = aborted_price.0.saturating_mul(10_000);
        self.abort_last_ts_ns[slot_idx][d] = ts_ns.0;
    }

    /// SCAR-LOCK #64: Post-abort adverse sweep, BTC debt floor & compression decel shield.
    pub fn audit_scar64_entry_and_sweep_shield(
        &self,
        slot_idx: usize,
        side: Side,
        snap: &SymbolLobSnapshot,
        btc_signed_snr: f64,
    ) -> Result<(), &'static str> {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return Ok(());
        }
        let Some((best_bid, best_ask, _)) = snap.bbo_and_spread_ticks() else {
            return Ok(());
        };
        let d = Self::dir_idx(side);
        let d_sign = side.sign_i64();
        let entry_px = side.select_entry_bbo_price(best_bid.price, best_ask.price);
        if self.abort_streak[slot_idx][d] >= 1
            && self.abort_anchor_px_1e4[slot_idx][d] > 0
            && snap.ts_local_ns.0 > 0
        {
            let vol_raw = snap.realized_vol_5m.raw().clamp(1_200, 15_000);
            let sweep_ttl_ns = ((25_000_000_000_i64 * 6_000) / vol_raw)
                .clamp(18_000_000_000, 45_000_000_000);
            let since_abort_ns =
                snap.ts_local_ns.0.saturating_sub(self.abort_last_ts_ns[slot_idx][d]);
            let cur_1e4 = entry_px.0.saturating_mul(10_000);
            let move_1e4 = (cur_1e4 - self.abort_anchor_px_1e4[slot_idx][d])
                .saturating_mul(d_sign);
            if since_abort_ns < sweep_ttl_ns && move_1e4 < 0 {
                return Err("VETO_POST_ABORT_ADVERSE_SWEEP");
            }
        }
        if slot_idx > 0 {
            let glob_debt = (-self.global_dir_debt_bps_x100[d]).max(0) as f64;
            let opp_macro = (-snap.wave_macro_phase.get() * side.sign_f64()).max(0.0);
            if glob_debt >= 8_000.0
                || self.global_dir_sl_streak[d] >= 1
                || opp_macro > 0.05
            {
                let min_btc_snr = (-0.04
                    + 0.04 * (glob_debt / 10_000.0).tanh()
                    + 0.85 * (opp_macro - 0.05).max(0.0))
                    .clamp(-0.04, 0.28);
                if btc_signed_snr < min_btc_snr {
                    return Err("VETO_SCAR64_BTC_DEBT_HEADWIND");
                }
            }
            let fast_1e4 = self.ewma_fast_px_1e4[slot_idx];
            let slow_1e4 = self.ewma_slow_px_1e4[slot_idx];
            if fast_1e4 > 0 && slow_1e4 > 0 {
                let mid_t = i64::midpoint(best_bid.price.0, best_ask.price.0);
                let px_1e4 = mid_t.saturating_mul(10_000);
                let s_drift = (((fast_1e4 - slow_1e4).saturating_mul(1_000_000))
                    / slow_1e4)
                    .saturating_mul(d_sign);
                let s_ext = (((px_1e4 - slow_1e4).saturating_mul(1_000_000)) / slow_1e4)
                    .saturating_mul(d_sign);
                let lead_impulse = s_ext.saturating_sub(s_drift);
                let signed_ofi = snap.factor_ofi.get() * side.sign_f64();
                let sym_sl_tot = self.sl_streak[slot_idx][0]
                    .saturating_add(self.sl_streak[slot_idx][1]);
                if sym_sl_tot >= 1 && (signed_ofi < 0.34 || lead_impulse < 0) {
                    return Err("VETO_SYMBOL_CROSS_SCAR_WEAK_IMPULSE");
                }
                let low_vol_decel = snap.realized_vol_5m.raw() < 2_500
                    || (snap.realized_vol_5m.raw() < 3_000 && signed_ofi < 0.35);
                if s_drift > 0
                    && lead_impulse < -15
                    && (low_vol_decel || btc_signed_snr < 0.08)
                {
                    return Err("VETO_FOLLOWER_DECEL_UNDER_COMPRESSION");
                }
            } else {
                let signed_ofi = snap.factor_ofi.get() * side.sign_f64();
                let sym_sl_tot = self.sl_streak[slot_idx][0]
                    .saturating_add(self.sl_streak[slot_idx][1]);
                if sym_sl_tot >= 1 && signed_ofi < 0.34 {
                    return Err("VETO_SYMBOL_CROSS_SCAR_WEAK_IMPULSE");
                }
            }
        }
        Ok(())
    }
    #[must_use]
    pub fn to_scar_snapshot_json(&self, ts_ns: TimestampNs) -> serde_json::Value {
        serde_json::json!({
            "event": "GOV_SCAR_SNAPSHOT",
            "ts_ns": ts_ns.0,
            "ewma_net_bps_x100": self.ewma_net_bps_x100,
            "ewma_var_bps2": self.ewma_var_bps2,
            "eq_vol_5m_bps": self.eq_vol_5m_bps,
            "cum_net_bps_x100": self.cum_net_bps_x100,
            "sl_streak": self.sl_streak,
            "win_streak": self.win_streak,
            "directional_cooldown_until_ns": self.directional_cooldown_until_ns,
            "wave_anchor_px_1e4": self.wave_anchor_px_1e4,
            "wave_anchor_ts_ns": self.wave_anchor_ts_ns,
            "global_dir_debt_bps_x100": self.global_dir_debt_bps_x100,
            "global_dir_sl_streak": self.global_dir_sl_streak,
            "global_dir_win_streak": self.global_dir_win_streak,
            "global_dir_cooldown_until_ns": self.global_dir_cooldown_until_ns,
            "last_global_win_dir": self.last_global_win_dir,
        })
    }

    pub fn apply_scar_snapshot_json(&mut self, val: &serde_json::Value) -> Option<TimestampNs> {
        if val.get("event").and_then(|v| v.as_str()) != Some("GOV_SCAR_SNAPSHOT") {
            return None;
        }
        let snap_ts = val.get("ts_ns").and_then(|v| v.as_i64()).unwrap_or(0);
        extract_matrix_i64_12x2(val, "ewma_net_bps_x100", &mut self.ewma_net_bps_x100);
        self.ewma_var_bps2 = [[INIT_RETURN_VAR_BPS2; 2]; MAX_ACTIVE_PAIRS];
        extract_matrix_i64_12x2(val, "ewma_var_bps2", &mut self.ewma_var_bps2);
        let min_regime_floor = ScaledBps::MIN_REQUIRED_VOL_5M_BPS.raw() * 2;
        if let Some(eq_arr) = val.get("eq_vol_5m_bps").and_then(|v| v.as_array()) {
            for (s, cell) in eq_arr.iter().take(MAX_ACTIVE_PAIRS).enumerate() {
                if let Some(n) = cell.as_i64() {
                    if n > 0 {
                        self.eq_vol_5m_bps[s] = n.max(min_regime_floor);
                    }
                }
            }
        }
        extract_matrix_i64_12x2(val, "cum_net_bps_x100", &mut self.cum_net_bps_x100);
        extract_matrix_u32_12x2(val, "sl_streak", &mut self.sl_streak);
        extract_matrix_u32_12x2(val, "win_streak", &mut self.win_streak);
        extract_matrix_i64_12x2(
            val,
            "directional_cooldown_until_ns",
            &mut self.directional_cooldown_until_ns,
        );
        extract_matrix_i64_12x2(val, "wave_anchor_px_1e4", &mut self.wave_anchor_px_1e4);
        self.wave_anchor_ts_ns = [[0; 2]; MAX_ACTIVE_PAIRS];
        extract_matrix_i64_12x2(val, "wave_anchor_ts_ns", &mut self.wave_anchor_ts_ns);
        for s in 0..MAX_ACTIVE_PAIRS {
            for d in 0..2 {
                if self.wave_anchor_px_1e4[s][d] > 0 && self.wave_anchor_ts_ns[s][d] <= 0 {
                    let fallback_ts = if self.directional_cooldown_until_ns[s][d] > 0 {
                        self.directional_cooldown_until_ns[s][d]
                    } else {
                        snap_ts
                    };
                    self.wave_anchor_ts_ns[s][d] = fallback_ts;
                }
            }
        }
        if let Some(arr) = val.get("global_dir_debt_bps_x100").and_then(|v| v.as_array()) {
            for (d, cell) in arr.iter().take(2).enumerate() {
                if let Some(n) = cell.as_i64() {
                    self.global_dir_debt_bps_x100[d] = n;
                }
            }
        }
        if let Some(arr) = val.get("global_dir_sl_streak").and_then(|v| v.as_array()) {
            for (d, cell) in arr.iter().take(2).enumerate() {
                if let Some(n) = cell.as_u64() {
                    self.global_dir_sl_streak[d] = n.min(4) as u32;
                }
            }
        }
        if let Some(arr) = val.get("global_dir_win_streak").and_then(|v| v.as_array()) {
            for (d, cell) in arr.iter().take(2).enumerate() {
                if let Some(n) = cell.as_u64() {
                    self.global_dir_win_streak[d] = n.min(6) as u32;
                }
            }
        }
        if let Some(arr) = val.get("global_dir_cooldown_until_ns").and_then(|v| v.as_array()) {
            for (d, cell) in arr.iter().take(2).enumerate() {
                if let Some(n) = cell.as_i64() {
                    self.global_dir_cooldown_until_ns[d] = n;
                }
            }
        }
        if let Some(ld) = val.get("last_global_win_dir").and_then(|v| v.as_u64()) {
            self.last_global_win_dir = ld.min(255) as u8;
        }
        Some(TimestampNs(snap_ts))
    }

    #[must_use]
    pub fn authorizes_adaptive_entry_with_wallet(
        &self,
        slot_idx: usize,
        side: Side,
        wallet_usdt_1e8: i64,
        candidate_snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
    ) -> bool {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return false;
        }
        let has_bbo =
            candidate_snap.bids[0].price.0 > 0 && candidate_snap.asks[0].price.0 > 0;
        let entry_px = if has_bbo {
            side.select_entry_bbo_price(
                candidate_snap.bids[0].price,
                candidate_snap.asks[0].price,
            )
        } else {
            candidate_snap.mark_price
        };
        if entry_px.0 <= 0 {
            return false;
        }
        if self
            .audit_historical_entry_veto(
                slot_idx,
                side,
                entry_px.0,
                candidate_snap.ts_local_ns.0,
            )
            .is_some()
        {
            return false;
        }
        let d = Self::dir_idx(side);
        let spec = APPROVED_SPECS[slot_idx];
        let (own_q, opp_q, own_deep) = match side {
            Side::Bid => (
                candidate_snap.bids[0].qty,
                candidate_snap.asks[0].qty,
                candidate_snap.deep_bid_lots_l1_l5,
            ),
            Side::Ask => (
                candidate_snap.asks[0].qty,
                candidate_snap.bids[0].qty,
                candidate_snap.deep_ask_lots_l1_l5,
            ),
        };
        if !spec.passes_l1_queue_depth_and_wall_guard(
            entry_px,
            candidate_snap.realized_vol_5m,
            self.ewma_net_bps_x100[slot_idx][d],
            own_q,
            opp_q,
            own_deep,
            candidate_snap.factor_ofi.get().abs(),
            candidate_snap.factor_tfi.get().abs(),
        ) {
            return false;
        }

        let (self_drift_bps, self_ext_bps) =
            self.price_drift_and_extension_scaled_bps(slot_idx, candidate_snap);
        let dir_s = side.sign_f64();
        let signed_geom = candidate_snap.factor_geom.get() * dir_s;
        let cand_comp = OrthogonalFactorState::snr_composite_score(candidate_snap).abs();
        let in_local_scar = self.is_in_local_slot_scar_debt(slot_idx, side);
        let in_scar = self.is_in_scar_debt(slot_idx, side);

        if !spec.passes_oversized_lot_conviction_guard(
            wallet_usdt_1e8,
            entry_px,
            cand_comp,
            in_local_scar,
        ) {
            return false;
        }
        if self
            .evaluate_kinematic_energy_surface(
                slot_idx,
                side,
                self_drift_bps,
                self_ext_bps,
                signed_geom,
                cand_comp,
            )
            .is_err()
        {
            return false;
        }

        let sl_s = self.sl_streak[slot_idx][d] as f64;
        let win_s = (self.win_streak[slot_idx][d].min(4)) as f64;
        let debt_bps = self.cum_net_bps_x100[slot_idx][d] as f64;
        let debt_penalty = if debt_bps < 0.0 {
            ((-debt_bps) / 16_000.0).min(0.28)
        } else {
            0.0
        };
        let global_debt_bps = self.global_dir_debt_bps_x100[d] as f64;
        let global_penalty = if global_debt_bps < -2_000.0 {
            ((-global_debt_bps - 2_000.0) / 35_000.0).min(0.14)
        } else {
            0.0
        };
        let staircase_penalty = if win_s >= 2.0 { 0.03 * (win_s - 1.0) } else { 0.0 };
        let ir_adj =
            (self.slot_information_ratio_x100(slot_idx, side) as f64 / 2_000.0).clamp(-0.04, 0.05);

        let req_scar_tfi = if in_local_scar { 0.45 } else { 0.38 };
        if in_scar && candidate_snap.factor_tfi.get().abs() < req_scar_tfi {
            return false;
        }
        if slot_idx > 0 {
            let btc_comp = OrthogonalFactorState::snr_composite_score(btc_leader_snap) * dir_s;
            let btc_ofi = btc_leader_snap.factor_ofi.get() * dir_s;
            let btc_tfi = btc_leader_snap.factor_tfi.get() * dir_s;
            let strong_independent = cand_comp >= 0.50 && !in_local_scar;
            if strong_independent {
                if btc_comp < -0.08 || (btc_ofi < -0.18 && btc_tfi < -0.18) {
                    return false;
                }
            } else {
                let min_req_btc = 0.08 + 0.08 * sl_s + 0.45 * (debt_penalty + global_penalty);
                if btc_ofi < -0.05 || btc_tfi < -0.05 || btc_comp < min_req_btc {
                    return false;
                }
            }
        }
        let btc_signed_snr =
            OrthogonalFactorState::snr_composite_score(btc_leader_snap) * dir_s;
        let Ok(wave_mod) =
            crate::wave_elliott::CausalElliottWaveState::evaluate_wave_entry_modulation(
                candidate_snap.wave_observables(),
                side,
                btc_signed_snr,
            )
        else {
            return false;
        };
        if self
            .audit_scar64_entry_and_sweep_shield(
                slot_idx,
                side,
                candidate_snap,
                btc_signed_snr,
            )
            .is_err()
        {
            return false;
        }
        let adaptive_threshold = (0.42
            + 0.08 * sl_s
            + debt_penalty
            + global_penalty
            + staircase_penalty
            + wave_mod.snr_threshold_delta
            - ir_adj)
            .clamp(0.36, 0.82);
        cand_comp >= adaptive_threshold
    }

    /// Elastic L1 queue chase horizon coupled to 5.0s flow half-life & slot IR [3.2s..5.6s].
    #[inline]
    #[must_use]
    pub fn max_total_entry_chase_ns(&self, slot_idx: usize, realized_vol_5m: ScaledBps) -> i64 {
        let min_v = ScaledBps::MIN_REQUIRED_VOL_5M_BPS.raw();
        let eq_v = if slot_idx < MAX_ACTIVE_PAIRS {
            self.eq_vol_5m_bps[slot_idx].max(min_v * 2)
        } else {
            min_v * 4
        };
        let vol_c = realized_vol_5m.raw().max(min_v);
        let base_flow_ns = VOL_5M_HORIZON_NS / 60;
        let ir_best = c_max_i64(
            self.slot_information_ratio_x100(slot_idx, Side::Bid),
            self.slot_information_ratio_x100(slot_idx, Side::Ask),
        )
        .clamp(-40, 80);
        let ir_permille = 1_000 + ir_best * 2;
        let raw_ns = (((base_flow_ns as i128 * eq_v as i128) / (vol_c as i128))
            * (ir_permille as i128))
            / 1_000;
        let lo = (base_flow_ns * 16) / 25;
        let hi = (base_flow_ns * 28) / 25;
        (raw_ns as i64).clamp(lo, hi)
    }

    /// Continuous C1 hysteresis preflight validator with OFI sign & crest-reversal guard.
    pub fn confirm_preflight_kinematics_non_choking(
        &self,
        slot_idx: usize,
        side: Side,
        wallet_usdt_1e8: i64,
        snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
    ) -> Result<(i64, i64), &'static str> {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return Err("VETO_INVALID_SLOT");
        }
        let Some((best_bid, best_ask, _)) = snap.bbo_and_spread_ticks() else {
            return Err("VETO_PREFLIGHT_INVALID_BBO");
        };
        let ofi = snap.factor_ofi.get();
        let tfi = snap.factor_tfi.get();
        if (ofi.abs() > 0.32 && tfi.abs() > 0.32 && (ofi * tfi) < -0.08)
            || (ofi.abs() > 0.55 && tfi.abs() < 0.08)
        {
            return Err("VETO_PREFLIGHT_SPOOFING");
        }
        let entry_px = side.select_entry_bbo_price(best_bid.price, best_ask.price);
        if let Some(veto) = self.audit_historical_entry_veto(
            slot_idx,
            side,
            entry_px.0,
            snap.ts_local_ns.0,
        ) {
            return Err(veto);
        }
        let dir_s = side.sign_f64();
        if (ofi * dir_s) < 0.08 {
            return Err("VETO_PREFLIGHT_OFI_DECAY");
        }
        let signed_snr = OrthogonalFactorState::snr_composite_score(snap) * dir_s;
        if signed_snr < 0.30 {
            return Err("VETO_PREFLIGHT_SNR_COLLAPSE");
        }
        let spec = APPROVED_SPECS[slot_idx];
        let in_local_scar = self.is_in_local_slot_scar_debt(slot_idx, side);
        if !spec.passes_oversized_lot_conviction_guard(
            wallet_usdt_1e8,
            entry_px,
            signed_snr,
            in_local_scar,
        ) {
            return Err("VETO_PREFLIGHT_OVERSIZED_LOT");
        }
        let signed_geom = snap.factor_geom.get() * dir_s;
        let (own_q_pre, opp_q_pre) = match side {
            Side::Bid => (best_bid.qty.0, best_ask.qty.0),
            Side::Ask => (best_ask.qty.0, best_bid.qty.0),
        };
        let geom_floor_lo = if signed_snr >= 0.68 && own_q_pre >= opp_q_pre {
            -0.06
        } else {
            0.00
        };
        let min_pre_geom =
            (0.06 - (signed_snr - 0.42) * 0.45).clamp(geom_floor_lo, 0.06);
        if signed_geom < min_pre_geom {
            return Err("VETO_PREFLIGHT_GEOM_REVERSAL");
        }
        let btc_snr =
            OrthogonalFactorState::snr_composite_score(btc_leader_snap) * dir_s;
        if slot_idx > 0 && btc_snr < -0.08 && signed_snr < 0.62 {
            return Err("VETO_PREFLIGHT_BTC_HEADWIND");
        }
        self.audit_scar64_entry_and_sweep_shield(slot_idx, side, snap, btc_snr)?;
        let _ =
            crate::wave_elliott::CausalElliottWaveState::evaluate_wave_entry_modulation(
                snap.wave_observables(),
                side,
                btc_snr,
            )?;
        let (drift_bps, ext_bps) =
            self.price_drift_and_extension_scaled_bps(slot_idx, snap);
        if let Ok(ok_pair) = self.evaluate_kinematic_energy_surface(
            slot_idx,
            side,
            drift_bps,
            ext_bps,
            signed_geom,
            signed_snr,
        ) {
            return Ok(ok_pair);
        }
        let fee_rt = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
        let eq_vol = self.eq_vol_5m_bps[slot_idx].max(fee_rt * 6);
        let d_sign = side.sign_i64();
        let s_drift = drift_bps.saturating_mul(d_sign);
        let s_ext = ext_bps.saturating_mul(d_sign);
        let lead_impulse = s_ext.saturating_sub(s_drift);
        let pre_pullback =
            -(fee_rt / 16) - ((eq_vol / 64) * i64::from(signed_snr > 0.55));
        let max_pre_ext = (eq_vol / 2).max(fee_rt * 3) + (fee_rt / 8);
        let elastic_lead_cap = (fee_rt * 15) / 100
            + (((signed_snr - 0.55).max(0.0) * (eq_vol as f64 / 18.0)).round() as i64);

        let snr_pull = ((signed_snr - 0.50) / 0.35).clamp(0.0, 1.0);
        let base_neg_lead =
            -(fee_rt / 3 + (((fee_rt as f64) * 0.25 * snr_pull).round() as i64));
        let ext_crest_tighten = (s_drift - (fee_rt * 5) / 4).max(0) / 2;
        let min_pre_lead = base_neg_lead + ext_crest_tighten - (fee_rt / 10);

        if s_drift < pre_pullback
            || s_ext < pre_pullback
            || s_ext > max_pre_ext
            || (lead_impulse > elastic_lead_cap && s_ext >= (fee_rt * 3) / 10)
            || lead_impulse < min_pre_lead
        {
            return Err("VETO_PREFLIGHT_KINEMATIC_EXHAUSTION");
        }
        let d = Self::dir_idx(side);
        let learned_edge = self.ewma_net_bps_x100[slot_idx][d];
        let ir_x100 = self.slot_information_ratio_x100(slot_idx, side);
        let base_e = (fee_rt * 5) / 16;
        let edge_deficit = (fee_rt / 2 - learned_edge).max(0) / 4;
        let ir_relief = (ir_x100 - 33).clamp(-60, 80) / 5;
        let req_e_min =
            (base_e + edge_deficit - ir_relief).clamp(fee_rt / 4, (fee_rt * 13) / 20);
        let hyst_e_min = ((req_e_min * 80) / 100).clamp(fee_rt / 5, (fee_rt * 11) / 20);
        let geom_c = ((signed_geom - min_pre_geom).max(0.0) * 240.0).round() as i64;
        let snr_c = ((signed_snr - 0.34).max(0.0) * 300.0).round() as i64;
        let imp_c = if lead_impulse >= 0 {
            lead_impulse.min(75) + (s_drift.max(0) / 2).min(85)
        } else {
            (lead_impulse / 2).max(-60)
        };
        let snr_ramp = ((signed_snr - 0.26) / 0.10).clamp(0.0, 1.0);
        let bonus = if s_drift >= pre_pullback
            && s_drift <= (eq_vol / 28).max(fee_rt / 2)
            && s_ext >= pre_pullback
            && s_ext <= (eq_vol / 22).max((fee_rt * 3) / 5)
        {
            (((fee_rt * 19) / 80) as f64 * snr_ramp).round() as i64
        } else {
            0_i64
        };
        let e_kin_hyst = imp_c + geom_c + snr_c + bonus;
        if e_kin_hyst < hyst_e_min {
            return Err("VETO_PREFLIGHT_ENERGY_COLLAPSE");
        }
        Ok((e_kin_hyst, hyst_e_min))
    }

    #[must_use]
    pub fn parse_historical_trade_scar_tuple(
        val: &serde_json::Value,
        min_exclusive_ts_ns: i64,
    ) -> Option<(ScarEventKey, ScarEventPayload)> {
        if let Some(ev) = val.get("event").and_then(|v| v.as_str()) {
            if ev != "TRADE_CLOSE" {
                return None;
            }
        }
        let ts_ns = val.get("ts_ns").and_then(|v| v.as_i64()).unwrap_or(0);
        if ts_ns <= 0 || ts_ns <= min_exclusive_ts_ns {
            return None;
        }
        let sym = val.get("symbol").and_then(|v| v.as_str())?;
        let slot_idx = crate::types::slot_from_symbol_name(sym)?;
        let side_s = val.get("side").and_then(|v| v.as_str()).unwrap_or("Bid");
        let side = crate::types::side_from_str(side_s);
        let d_u8 = Self::dir_idx(side) as u8;
        let en_t =
            PriceTicks(val.get("entry_ticks").and_then(|v| v.as_i64()).unwrap_or(0));
        let net_f = val.get("net_bps").and_then(|v| v.as_f64())?;
        let net_sc = ScaledBps((net_f * 100.0).round() as i64);
        let mkr = val
            .get("maker_exit")
            .and_then(|v| v.as_bool())
            .unwrap_or(net_sc.raw() >= -50);
        Some(((ts_ns, slot_idx, d_u8), (en_t, net_sc, mkr)))
    }

    pub fn rehydrate_from_scar_and_loss_files(
        &mut self,
        scar_path: &std::path::Path,
        red_loss_path: &std::path::Path,
        session_vault_path: Option<&std::path::Path>,
    ) -> (i64, usize) {
        use std::io::BufRead;
        let mut snap_ts_ns = 0_i64;
        if let Ok(s_file) = std::fs::File::open(scar_path) {
            let reader = std::io::BufReader::new(s_file);
            let mut latest_snap: Option<serde_json::Value> = None;
            for line in reader.lines().map_while(Result::ok) {
                if let Ok(val) = serde_json::from_str::<serde_json::Value>(&line) {
                    let is_snap = val.get("event").and_then(|v| v.as_str())
                        == Some("GOV_SCAR_SNAPSHOT");
                    if is_snap {
                        latest_snap = Some(val);
                    }
                }
            }
            if let Some(ref snap_v) = latest_snap {
                if let Some(restored) = self.apply_scar_snapshot_json(snap_v) {
                    snap_ts_ns = restored.0;
                }
            }
        }
        let mut dedup_events: std::collections::BTreeMap<
            ScarEventKey,
            ScarEventPayload,
        > = std::collections::BTreeMap::new();
        let mut paths: [&std::path::Path; 3] = [scar_path, red_loss_path, scar_path];
        let path_count = if let Some(vp) = session_vault_path {
            paths[2] = vp;
            3_usize
        } else {
            2_usize
        };
        for p in &paths[..path_count] {
            let Ok(file) = std::fs::File::open(p) else {
                continue;
            };
            let reader = std::io::BufReader::new(file);
            for line in reader.lines().map_while(Result::ok) {
                let Ok(val) = serde_json::from_str::<serde_json::Value>(&line) else {
                    continue;
                };
                if let Some((key, payload)) =
                    Self::parse_historical_trade_scar_tuple(&val, snap_ts_ns)
                {
                    dedup_events.entry(key).or_insert(payload);
                }
            }
        }
        let mut max_ts = snap_ts_ns;
        let ingested = dedup_events.len();
        for (&(ts_ns, slot_idx, d_u8), &(en_t, net_sc, mkr)) in &dedup_events {
            let side = if d_u8 == 1 { Side::Ask } else { Side::Bid };
            self.observe_closed_trade_at_entry(
                slot_idx,
                side,
                en_t,
                net_sc,
                mkr,
                TimestampNs(ts_ns),
            );
            if ts_ns > max_ts {
                max_ts = ts_ns;
            }
        }
        (max_ts, ingested)
    }

    /// Unified LOSS-LAB Verifier: Certifies 63/63 (incl. SCAR #63 LINK Bid) + 2,160 manifold.
    #[must_use]
    pub fn audit_unified_62_losses_and_2160_manifold() -> (usize, usize) {
        let gov = Self::new();
        let cases: [(usize, Side, i64, i64, f64, f64); UNIFIED_LOSS_LAB_COUNT] = [
            (0, Side::Ask, -331, -347, 0.11, 0.55), (3, Side::Bid, 113, 120, 0.11, 0.52),
            (0, Side::Bid, 182, 248, 0.38, 0.58), (1, Side::Bid, 142, 267, 0.25, 0.54),
            (0, Side::Bid, 247, 157, 0.16, 0.44), (0, Side::Bid, 109, 36, 0.20, 0.45),
            (1, Side::Bid, 275, 209, 0.15, 0.43), (1, Side::Bid, 263, 155, 0.15, 0.43),
            (0, Side::Ask, 694, 302, 0.51, 0.79), (1, Side::Ask, -185, -240, 0.22, 0.50),
            (0, Side::Bid, 170, 235, 0.24, 0.51), (0, Side::Ask, -160, -220, 0.21, 0.49),
            (0, Side::Ask, -145, -210, 0.20, 0.48), (0, Side::Bid, 190, 255, 0.23, 0.52),
            (1, Side::Bid, 210, 275, 0.25, 0.53), (0, Side::Bid, 220, 145, 0.15, 0.42),
            (4, Side::Ask, -218, -148, 0.15, 0.42), (0, Side::Ask, -215, -140, 0.15, 0.42),
            (0, Side::Bid, 230, 150, 0.15, 0.42), (1, Side::Bid, 228, 152, 0.15, 0.42),
            (0, Side::Bid, 235, 165, 0.15, 0.42), (0, Side::Ask, -225, -155, 0.15, 0.42),
            (1, Side::Ask, -240, -168, 0.15, 0.42), (0, Side::Ask, -232, -160, 0.15, 0.42),
            (0, Side::Bid, 250, 162, 0.15, 0.42), (7, Side::Bid, -42, -68, -0.10, 0.39),
            (4, Side::Bid, 135, 195, 0.14, 0.49), (3, Side::Bid, 148, 212, 0.13, 0.50),
            (4, Side::Bid, 128, 184, 0.12, 0.47), (4, Side::Bid, 162, 229, 0.16, 0.51),
            (1, Side::Bid, 175, 244, 0.18, 0.52), (2, Side::Ask, -154, -218, 0.14, 0.48),
            (3, Side::Bid, 139, 204, 0.15, 0.49), (5, Side::Ask, -166, -231, 0.17, 0.50),
            (1, Side::Bid, 188, 262, 0.19, 0.53), (1, Side::Ask, -45, 18, 0.08, 0.41),
            (4, Side::Ask, -38, 22, 0.09, 0.42), (1, Side::Bid, 42, -19, 0.07, 0.41),
            (4, Side::Ask, -32, 25, 0.06, 0.40), (5, Side::Bid, 152, 219, 0.14, 0.48),
            (0, Side::Ask, -51, 14, 0.09, 0.43), (0, Side::Ask, -48, 16, 0.08, 0.42),
            (4, Side::Ask, -172, -245, 0.16, 0.51), (0, Side::Ask, -184, -258, 0.18, 0.52),
            (1, Side::Bid, 168, 239, 0.15, 0.50), (0, Side::Ask, -179, -251, 0.17, 0.51),
            (0, Side::Bid, 36, -24, 0.07, 0.41), (1, Side::Bid, -28, -55, 0.05, 0.43),
            (5, Side::Bid, -34, -62, 0.06, 0.44), (1, Side::Bid, -49, -84, 0.04, 0.45),
            (0, Side::Bid, 144, 208, 0.14, 0.48), (1, Side::Ask, -169, -238, 0.16, 0.50),
            (0, Side::Bid, 158, 226, 0.15, 0.49), (1, Side::Ask, -163, -232, 0.15, 0.49),
            (0, Side::Ask, -174, -246, 0.17, 0.51), (0, Side::Bid, 132, 194, 0.13, 0.47),
            (0, Side::Ask, -136, -198, 0.13, 0.47), (3, Side::Ask, -151, -216, 0.14, 0.48),
            (0, Side::Ask, -165, -234, 0.16, 0.50), (1, Side::Ask, -159, -225, 0.15, 0.49),
            (0, Side::Ask, -161, -228, 0.15, 0.49), (4, Side::Bid, 146, 211, 0.14, 0.48),
            (9, Side::Bid, 898, 591, 0.51, 0.70),
        ];
        let mut mitigated_all = 0_usize;
        for &(s_idx, side, dr, ex, gm, snr) in &cases {
            if gov.evaluate_kinematic_energy_surface(s_idx, side, dr, ex, gm, snr).is_err() {
                mitigated_all += 1;
            }
        }
        let mut manifold_blocked = 0_usize;
        for s_idx in 0..MAX_ACTIVE_PAIRS {
            for &side in &[Side::Bid, Side::Ask] {
                for step in 0..90_i64 {
                    let dr =
                        if matches!(side, Side::Bid) { 120 + step } else { -(120 + step) };
                    let ex = if matches!(side, Side::Bid) {
                        190 + step * 2
                    } else {
                        -(190 + step * 2)
                    };
                    if gov
                        .evaluate_kinematic_energy_surface(s_idx, side, dr, ex, 0.12, 0.44)
                        .is_err()
                    {
                        manifold_blocked += 1;
                    }
                }
            }
        }
        (mitigated_all, manifold_blocked)
    }
}

#[inline]
fn extract_json_epoch_ms(item: &serde_json::Value, key: &str) -> i64 {
    item.get(key)
        .and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.trim().parse::<i64>().ok()))
        })
        .unwrap_or(0)
}

/// Per-Slot Autonomous Lifecycle State Machine (Monotonic Tranche, Wald SL & LVDBS Native).
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

#[allow(clippy::manual_clamp, clippy::too_many_arguments)]
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
            Self::new(0),
            Self::new(1),
            Self::new(2),
            Self::new(3),
            Self::new(4),
            Self::new(5),
            Self::new(6),
            Self::new(7),
            Self::new(8),
            Self::new(9),
            Self::new(10),
            Self::new(11),
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
        if slot_idx == 0
            || slot_idx >= MAX_ACTIVE_PAIRS
            || open_lots.0 < 2
            || exit_price.0 <= 0
        {
            return None;
        }
        let spec = APPROVED_SPECS[slot_idx];
        let one_lot_1e8 = spec.lot_notional_1e8(LotQty(1), exit_price);
        if one_lot_1e8 <= 0 {
            return None;
        }
        let min_leg_1e8 = crate::types::SPLIT_LEG_MIN_NOTIONAL_1E8;
        let tot_1e8 = spec.lot_notional_1e8(open_lots, exit_price);
        if tot_1e8 < min_leg_1e8.saturating_mul(2) {
            return None;
        }
        let min_leg_lots = (min_leg_1e8 + one_lot_1e8 - 1) / one_lot_1e8;
        let max_t1_lots = open_lots.0 - min_leg_lots;
        if min_leg_lots < 1 || min_leg_lots > max_t1_lots {
            return None;
        }
        let pct = t1_permille.clamp(350, 650);
        let raw_t1 = (open_lots.0.saturating_mul(pct) + 500) / 1_000;
        let t1_lots = raw_t1.clamp(min_leg_lots, max_t1_lots);
        Some(LotQty(t1_lots))
    }

    /// Synchronizes slot exit physics with live Bayesian Governor posterior parameters.
    #[inline]
    pub fn sync_governor_for_side(&mut self, gov: &OnlineAdaptiveGovernor, side: Side) {
        self.learned_t1_permille = gov.learned_tranche1_fraction_permille(
            self.slot_idx,
            side,
            self.learned_mfe_bps,
        );
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
    pub fn active_side(&self) -> Option<Side> {
        match self.state {
            OrderLifecycleState::Idle => None,
            OrderLifecycleState::PlacingEntryMaker { side, .. }
            | OrderLifecycleState::RestingEntryQueue { side, .. }
            | OrderLifecycleState::PositionOpenMakerExit { side, .. } => Some(side),
        }
    }

    /// Elastic preflight window scaled by intrinsic clock & learned MFE (zero static anchor).
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

    /// Law 2 & Law 4: Defensive scratches (< fee_rt + 100) never shrink learned_mfe_bps.
    #[inline]
    pub fn observe_mfe_sample(&mut self, gross_mfe_scaled_bps: i64) {
        let fee_rt = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
        let win_mfe_gate = fee_rt + 100;
        if gross_mfe_scaled_bps >= win_mfe_gate {
            let clamped = gross_mfe_scaled_bps.clamp((fee_rt * 7) / 5, fee_rt * 4);
            self.learned_mfe_bps = (self.learned_mfe_bps * 80 + clamped * 20) / 100;
        }
    }

    /// Enforces regime-scaled 58..125 bps server SL distance & >= 2-tick Mark/Last cushion.
    #[inline]
    pub fn compute_fill_or_adopt_sl_ticks(
        side: Side,
        verified_avg_price: PriceTicks,
        snap: &SymbolLobSnapshot,
    ) -> Result<PriceTicks, VortexError> {
        if verified_avg_price.0 <= 0 {
            return Err(VortexError::InvalidPriceOrTickSpec);
        }
        let sl_bps = PairSpec::elastic_server_sl_distance_bps(snap.realized_vol_5m).raw();
        let sl_delta = ((verified_avg_price.0.saturating_mul(sl_bps)) / 1_000_000).max(4);
        let ref_mark = if snap.mark_price.0 > 0 {
            snap.mark_price
        } else {
            verified_avg_price
        };
        let ref_last = if snap.last_trade_price.0 > 0 {
            snap.last_trade_price
        } else {
            ref_mark
        };
        let raw_sl = match side {
            Side::Bid => {
                let nominal = verified_avg_price.0 - sl_delta;
                let live_cap = ref_mark.0.min(ref_last.0) - 3;
                PriceTicks(nominal.min(live_cap).max(1))
            }
            Side::Ask => {
                let nominal = verified_avg_price.0.saturating_add(sl_delta);
                let live_floor = ref_mark.0.max(ref_last.0).saturating_add(3);
                PriceTicks(nominal.max(live_floor))
            }
        };
        PairSpec::validate_stop_loss_geometry(side, raw_sl, ref_last, ref_mark)
    }

    /// Monotonically contracts Server SL via Abraham Wald SPRT when a trade stalls (MFE ~ 0).
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
        let min_stall_s = (self.intrinsic_tau_mkt_s.max(90) / 5).max(45);
        if hold_s < min_stall_s {
            return None;
        }
        let dir_s = side.sign_f64();
        let own_flow =
            (0.55 * snap.factor_ofi.get() + 0.45 * snap.factor_tfi.get()) * dir_s;
        let btc_snr =
            OrthogonalFactorState::snr_composite_score(btc_leader_snap) * dir_s;
        if own_flow >= -0.05 && btc_snr >= -0.20 {
            return None;
        }

        let base_sl_bps =
            PairSpec::elastic_server_sl_distance_bps(snap.realized_vol_5m).raw();
        let btc_headwind = if btc_snr < -0.25 {
            ((-btc_snr - 0.25) * 20.0).round() as i64
        } else {
            0
        };
        let eff_wald_pct = (self.last_closed_wald_pct + btc_headwind).clamp(12, 42);
        let min_wald_sl_bps = fee_rt * 12;
        let contracted_bps =
            ((base_sl_bps * (100 - eff_wald_pct)) / 100).max(min_wald_sl_bps);
        let sl_delta =
            ((avg_price_ticks.0.saturating_mul(contracted_bps)) / 1_000_000).max(4);
        let ref_mark = if snap.mark_price.0 > 0 {
            snap.mark_price
        } else {
            avg_price_ticks
        };
        let ref_last = if snap.last_trade_price.0 > 0 {
            snap.last_trade_price
        } else {
            ref_mark
        };
        let cand_sl = match side {
            Side::Bid => {
                let nominal = avg_price_ticks.0 - sl_delta;
                let live_cap = ref_mark.0.min(ref_last.0) - 4;
                PriceTicks(nominal.min(live_cap).max(1))
            }
            Side::Ask => {
                let nominal = avg_price_ticks.0.saturating_add(sl_delta);
                let live_floor = ref_mark.0.max(ref_last.0).saturating_add(4);
                PriceTicks(nominal.max(live_floor))
            }
        };
        let Ok(valid_sl) =
            PairSpec::validate_stop_loss_geometry(side, cand_sl, ref_last, ref_mark)
        else {
            return None;
        };
        let prev_sl = if self.last_eff_sl_ticks.0 > 0 {
            self.last_eff_sl_ticks
        } else {
            cur_sl
        };
        let tighter_by_3_ticks = match side {
            Side::Bid => {
                valid_sl.0 >= prev_sl.0.saturating_add(3)
                    && valid_sl.0 < avg_price_ticks.0
            }
            Side::Ask => {
                valid_sl.0 <= prev_sl.0.saturating_sub(3)
                    && valid_sl.0 > avg_price_ticks.0
            }
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

    /// Publishes tear-free Seqlock telemetry via SlotExecTelemetry::write_consistent.
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
                side,
                entry_price,
                qty,
                sl_trigger_price,
                submitted_ts,
                ..
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
                side,
                entry_price,
                qty,
                sl_trigger_price,
                resting_since_ts,
                ..
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
                side,
                entry_price,
                avg_price_ticks,
                peak_favorable_price,
                initial_lots,
                open_lots,
                tranche1_harvested,
                active_exit_price,
                resting_exit_price,
                sl_trigger_price,
                opened_ts,
                ..
            } => {
                let state_code = if tranche1_harvested {
                    4_u8
                } else if open_lots.0 < initial_lots.0 {
                    5_u8
                } else {
                    3_u8
                };
                let eff_tp = if resting_exit_price.0 > 0 {
                    resting_exit_price.0
                } else {
                    active_exit_price.0
                };
                let eff_sl = if self.last_eff_sl_ticks.0 > 0 {
                    self.last_eff_sl_ticks.0
                } else {
                    sl_trigger_price.0
                };
                SlotTelemetrySnapshot {
                    state_code,
                    side_code: side as u64,
                    entry_ticks: entry_price.0,
                    avg_price_ticks: avg_price_ticks.0,
                    peak_ticks: peak_favorable_price.0,
                    tp_ticks: eff_tp,
                    sl_ticks: eff_sl,
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
        verdict: crate::alpha_3factor::SignalVerdict,
        qty: LotQty,
    ) -> Result<OrderLifecycleState, VortexError> {
        if !matches!(self.state, OrderLifecycleState::Idle) {
            return Ok(self.state);
        }
        if snap.ts_local_ns.0 < self.cooldown_until_ns.0 {
            return Ok(self.state);
        }
        let crate::alpha_3factor::SignalVerdict::AuthorizeMakerEntry {
            side,
            target_excursion_scaled_bps,
        } = verdict
        else {
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

    /// Mutates PlacingEntryMaker in-place with post-preflight L1 price & SL.
    pub fn sync_preflight_l1_entry(
        &mut self,
        snap: &SymbolLobSnapshot,
        max_drift_ticks: i64,
    ) -> Result<(PriceTicks, PriceTicks), VortexError> {
        let OrderLifecycleState::PlacingEntryMaker {
            side,
            entry_price: armed_px,
            ..
        } = self.state
        else {
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
            side,
            entry_price,
            qty,
            target_excursion,
            sl_trigger_price,
            ..
        } = self.state
        {
            self.state = OrderLifecycleState::RestingEntryQueue {
                side,
                entry_price,
                qty,
                target_excursion,
                sl_trigger_price,
                resting_since_ts: resting_ts,
            };
        }
        Ok(self.state)
    }

    /// Promotes entry to PositionOpenMakerExit with bilateral $5.00 notional split guard.
    pub fn on_maker_entry_filled_verified(
        &mut self,
        fill_ts: TimestampNs,
        verified_side: Option<Side>,
        verified_avg_price: Option<PriceTicks>,
        verified_open_lots: Option<LotQty>,
    ) -> Result<OrderLifecycleState, VortexError> {
        let (state_side, state_entry_px, state_qty, target_excursion, state_sl_px) =
            match self.state {
                OrderLifecycleState::RestingEntryQueue {
                    side,
                    entry_price,
                    qty,
                    target_excursion,
                    sl_trigger_price,
                    ..
                }
                | OrderLifecycleState::PlacingEntryMaker {
                    side,
                    entry_price,
                    qty,
                    target_excursion,
                    sl_trigger_price,
                    ..
                } => (side, entry_price, qty, target_excursion, sl_trigger_price),
                OrderLifecycleState::Idle
                | OrderLifecycleState::PositionOpenMakerExit { .. } => return Ok(self.state),
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
            self.slot_idx,
            eff_lots,
            win1_px,
            self.learned_t1_permille,
        );
        let tp_bps = if split_t1_opt.is_some() {
            target_excursion.raw().max(min_win_gross + 40)
        } else {
            let vol_raw = self.open_vol_5m_bps.raw();
            let vol_unit_cap = if vol_raw > 0 {
                ((vol_raw * 27) / 100).clamp(min_win_gross + 15, (fee_rt * 9) / 4)
            } else {
                (fee_rt * 9) / 4
            };
            let mfe_cap = self.learned_mfe_bps.clamp(min_win_gross + 15, (fee_rt * 9) / 4);
            let unit_cap = mfe_cap.min(vol_unit_cap);
            target_excursion.raw().min(unit_cap).max(min_win_gross)
        };
        let structural_tp_price =
            dynamic_maker_floor_price(side, eff_entry_px, ScaledBps(tp_bps));
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

    /// Adopts live Bybit position on startup with validated 2-tick SL & $5.00 split guard.
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
        let struct_bps = self.learned_mfe_bps.max((fee_rt * 27) / 20);
        let struct_tp =
            dynamic_maker_floor_price(side, verified_avg_price, ScaledBps(struct_bps));
        let harvested_lots = LotQty((eff_init.0 - eff_open.0).max(0));
        let t1_done = tranche1_harvested || harvested_lots.0 > 0;
        let t1_px = if t1_done { q_fl } else { PriceTicks(0) };
        let split_opt = if !t1_done {
            Self::compute_tranche1_split_lots(
                self.slot_idx,
                eff_open,
                q_fl,
                self.learned_t1_permille,
            )
        } else {
            None
        };
        let (role, init_px, init_lots) = match split_opt {
            Some(t1_lots) => (ExitOrderRole::Tranche1Harvest50, q_fl, t1_lots),
            None if t1_done => (ExitOrderRole::LvdbsDeepSurf50, struct_tp, eff_open),
            None => (ExitOrderRole::StandardMakerExit100, struct_tp, eff_open),
        };

        self.last_opened_ts = opened_ts;
        self.last_eff_sl_ticks = sl_t;
        self.open_vol_5m_bps = snap.realized_vol_5m;
        self.ratchet_locked_gross_bps = 0;
        self.last_closed_peak_ticks =
            if peak_price.0 > 0 { peak_price } else { verified_avg_price };
        self.last_closed_worst_ticks =
            if worst_price.0 > 0 { worst_price } else { verified_avg_price };
        self.tranche1_harvests_count = u64::from(t1_done);
        self.lvdbs_deep_surfs_count = 0;
        self.exit_peg_attempts = 0;
        self.close_reconcile_retries = 0;
        self.next_peg_allowed_ts_ns = opened_ts.0;
        self.state = OrderLifecycleState::PositionOpenMakerExit {
            side,
            entry_price: verified_avg_price,
            avg_price_ticks: verified_avg_price,
            peak_favorable_price: if peak_price.0 > 0 { peak_price } else { verified_avg_price },
            worst_adverse_price: if worst_price.0 > 0 {
                worst_price
            } else {
                verified_avg_price
            },
            initial_lots: eff_init,
            open_lots: eff_open,
            tranche1_harvested: t1_done,
            tranche1_lots: harvested_lots,
            tranche1_exit_price: t1_px,
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
                let prev_bps =
                    ((prev_diff.saturating_mul(1_000_000)) / prev_avg).max(min_win_gross);
                *entry_price = verified_avg_price;
                *avg_price_ticks = verified_avg_price;
                *structural_tp_price = dynamic_maker_floor_price(
                    side,
                    verified_avg_price,
                    ScaledBps(prev_bps),
                );
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
        let ratchet_arm = (fee_rt * 139) / 100;
        let state_mfe = if let OrderLifecycleState::PositionOpenMakerExit {
            side,
            avg_price_ticks,
            peak_favorable_price,
            ..
        } = self.state
        {
            let diff =
                (peak_favorable_price.0 - avg_price_ticks.0).saturating_mul(side.sign_i64());
            (diff.saturating_mul(1_000_000)) / avg_price_ticks.0.max(1)
        } else {
            0
        };
        let best_mfe = self.last_closed_mfe_bps.max(state_mfe);
        if best_mfe >= ratchet_arm {
            let win_fl = fee_rt + (fee_rt * 3) / 10;
            let cand = (fee_rt + ((best_mfe - fee_rt) * 72) / 100).max(win_fl);
            if cand > self.ratchet_locked_gross_bps {
                self.ratchet_locked_gross_bps = cand;
            }
        }
        if !lock_active {
            return;
        }
        if let OrderLifecycleState::PositionOpenMakerExit {
            ref mut defensive_floor_locked,
            ..
        } = self.state
        {
            *defensive_floor_locked = true;
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
        let Some(harvested) =
            Self::compute_tranche1_split_lots(s_idx, *open_lots, fill_price, phi)
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
    fn fold_price_into_excursions(
        &mut self,
        side: Side,
        avg_px: PriceTicks,
        obs_px: PriceTicks,
    ) {
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
        self.last_closed_mfe_bps = self
            .last_closed_mfe_bps
            .max((pk_diff.saturating_mul(1_000_000)) / denom);
        self.last_closed_mae_bps = self
            .last_closed_mae_bps
            .min((wr_diff.saturating_mul(1_000_000)) / denom);
    }

    #[inline]
    fn classify_maker_exit_sub_reason(
        side: Side,
        entry_px: PriceTicks,
        exit_px: PriceTicks,
        tranche1_harvested: bool,
        legs_count: u32,
        vac_level_hint: usize,
    ) -> ExitSubReason {
        let q_floor = defensive_zero_loss_maker_exit_price(side, entry_px);
        let strictly_above_q_floor = match side {
            Side::Bid => exit_px.0 > q_floor.0,
            Side::Ask => exit_px.0 > 0 && exit_px.0 < q_floor.0,
        };
        let raw_d = (exit_px.0 - entry_px.0).saturating_mul(side.sign_i64());
        let gross_bps = (raw_d.saturating_mul(1_000_000)) / entry_px.0.max(1);
        let win_gross = (ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 26) / 10;
        if strictly_above_q_floor && (gross_bps >= win_gross || vac_level_hint >= 2) {
            ExitSubReason::LvdbsDeepVacuumSurf
        } else if tranche1_harvested || legs_count >= 2 {
            ExitSubReason::Tranche1Harvest50
        } else {
            ExitSubReason::AdverseScratchEscape
        }
    }

    /// Reconciles position closure 1:1 against /v5/position/closed-pnl with algebraic leg fusion.
    #[must_use]
    pub fn reconcile_closed_pnl_payload(
        &self,
        payload: &serde_json::Value,
        vac_level_hint: usize,
        allow_partial_lot_coverage: bool,
    ) -> ClosedPnlQueryOutcome {
        let OrderLifecycleState::PositionOpenMakerExit {
            side,
            avg_price_ticks,
            worst_adverse_price,
            initial_lots,
            open_lots,
            tranche1_harvested,
            tranche1_exit_price,
            active_exit_price,
            resting_exit_price,
            sl_trigger_price,
            opened_ts,
            ..
        } = self.state
        else {
            return ClosedPnlQueryOutcome::TransportOrApiError;
        };
        if self.slot_idx >= MAX_ACTIVE_PAIRS {
            return ClosedPnlQueryOutcome::TransportOrApiError;
        }
        if payload.get("retCode").and_then(|c| c.as_i64()) != Some(0) {
            return ClosedPnlQueryOutcome::TransportOrApiError;
        }
        let Some(list) = payload
            .get("result")
            .and_then(|r| r.get("list"))
            .and_then(|l| l.as_array())
        else {
            return ClosedPnlQueryOutcome::TransportOrApiError;
        };
        if list.is_empty() {
            return ClosedPnlQueryOutcome::PendingIndexerRetry;
        }
        let spec = APPROVED_SPECS[self.slot_idx];
        let sym_expected = spec.symbol_id.name();
        let expected_close_side = match side {
            Side::Bid => "Sell",
            Side::Ask => "Buy",
        };
        let opened_ms = (opened_ts.0 / 1_000_000).max(0);
        let min_latest_ms = opened_ms.saturating_sub(1_500);
        let min_adopt_leg_ms = if tranche1_harvested {
            opened_ms.saturating_sub(86_400_000)
        } else {
            min_latest_ms
        };
        let max_entry_tol_ticks = (avg_price_ticks.0.max(1) / 400).max(25);
        let target_total_lots = initial_lots.0.max(open_lots.0).max(1);
        let min_req_lots = if allow_partial_lot_coverage {
            open_lots.0.max(1)
        } else {
            target_total_lots
        };

        let mut accum_lots: i64 = 0;
        let mut weighted_entry_sum: i128 = 0;
        let mut weighted_exit_sum: i128 = 0;
        let mut total_closed_pnl_1e8: i64 = 0;
        let mut has_pnl_field = false;
        let mut any_market_or_forced = false;
        let mut legs_count: u32 = 0;
        let mut first_leg_exit_ticks: i64 = 0;

        for item in list {
            if accum_lots >= target_total_lots {
                break;
            }
            if item
                .get("symbol")
                .and_then(|v| v.as_str())
                .is_some_and(|sym_s| sym_s != sym_expected)
            {
                continue;
            }
            if item
                .get("side")
                .and_then(|v| v.as_str())
                .is_some_and(|cs| cs != expected_close_side)
            {
                continue;
            }
            let upd_ms = extract_json_epoch_ms(item, "updatedTime");
            let crt_ms = extract_json_epoch_ms(item, "createdTime");
            let rec_ms = upd_ms.max(crt_ms);
            let min_bound_ms = if legs_count == 0 {
                min_latest_ms
            } else {
                min_adopt_leg_ms
            };
            if rec_ms > 0 && min_bound_ms > 0 && rec_ms < min_bound_ms {
                continue;
            }
            let Some(avg_en_s) = item.get("avgEntryPrice").and_then(|v| v.as_str()) else {
                continue;
            };
            let Some(avg_ex_s) = item.get("avgExitPrice").and_then(|v| v.as_str()) else {
                continue;
            };
            let Ok(leg_en_t) = parse_verified_avg_price_ticks(spec, side, avg_en_s) else {
                continue;
            };
            let Ok(leg_ex_t) = parse_verified_exit_price_ticks(spec, side, avg_ex_s) else {
                continue;
            };
            if (leg_en_t.0 - avg_price_ticks.0).abs() > max_entry_tol_ticks {
                continue;
            }
            let raw_sz_s = item
                .get("closedSize")
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty() && s.trim() != "0")
                .or_else(|| item.get("qty").and_then(|v| v.as_str()))
                .unwrap_or("0");
            let parsed_lots = spec.parse_lot_qty(raw_sz_s).0;
            if parsed_lots <= 0 {
                continue;
            }
            let rem_cap = (target_total_lots - accum_lots).max(1);
            let take_lots = parsed_lots.min(rem_cap);

            let order_type_s = item
                .get("orderType")
                .and_then(|v| v.as_str())
                .unwrap_or("Limit");
            let exec_type_s = item
                .get("execType")
                .and_then(|v| v.as_str())
                .unwrap_or("Trade");
            let is_forced_exec = matches!(
                exec_type_s,
                "BustTrade" | "AdlTrade" | "TakeOver" | "Settle"
            );
            if order_type_s != "Limit" || is_forced_exec {
                any_market_or_forced = true;
            }
            if let Some(Ok(leg_pnl)) = item
                .get("closedPnl")
                .and_then(|v| v.as_str())
                .map(parse_signed_decimal_1e8)
            {
                let scaled_pnl = if take_lots < parsed_lots {
                    ((leg_pnl as i128 * take_lots as i128) / (parsed_lots as i128)) as i64
                } else {
                    leg_pnl
                };
                total_closed_pnl_1e8 = total_closed_pnl_1e8.saturating_add(scaled_pnl);
                has_pnl_field = true;
            }
            if legs_count == 0 {
                first_leg_exit_ticks = leg_ex_t.0;
            }
            accum_lots = accum_lots.saturating_add(take_lots);
            weighted_entry_sum += (take_lots as i128) * (leg_en_t.0 as i128);
            weighted_exit_sum += (take_lots as i128) * (leg_ex_t.0 as i128);
            legs_count = legs_count.saturating_add(1);
        }

        if legs_count == 0 || accum_lots < min_req_lots {
            return ClosedPnlQueryOutcome::PendingIndexerRetry;
        }

        if allow_partial_lot_coverage
            && tranche1_harvested
            && accum_lots < target_total_lots
        {
            let missing_lots = target_total_lots - accum_lots;
            let q_fl = defensive_zero_loss_maker_exit_price(side, avg_price_ticks);
            let t1_mem_px = if tranche1_exit_price.0 > 0 {
                tranche1_exit_price
            } else {
                q_fl
            };
            let eff_sl = if self.last_eff_sl_ticks.0 > 0 {
                self.last_eff_sl_ticks
            } else {
                sl_trigger_price
            };
            let sl_breached = match side {
                Side::Bid => worst_adverse_price.0 > 0 && worst_adverse_price.0 <= eff_sl.0,
                Side::Ask => worst_adverse_price.0 >= eff_sl.0,
            };
            let indexed_is_t1 = (first_leg_exit_ticks - t1_mem_px.0).abs() <= 1;
            let (fused_px, fused_is_sl) = if indexed_is_t1 {
                if sl_breached {
                    (eff_sl, true)
                } else if resting_exit_price.0 > 0 {
                    (resting_exit_price, false)
                } else if active_exit_price.0 > 0 {
                    (active_exit_price, false)
                } else {
                    (q_fl, false)
                }
            } else {
                (t1_mem_px, false)
            };
            if fused_is_sl {
                any_market_or_forced = true;
            }
            let fused_fee = if fused_is_sl {
                ScaledBps(
                    ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw()
                        + ScaledBps::BYBIT_VIP0_TAKER_FEE_BPS.raw(),
                )
            } else {
                ScaledBps(ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2)
            };
            let (fused_pnl_1e8, _, _, _) = spec.compute_leg_pnl_and_fee_1e8(
                side,
                LotQty(missing_lots),
                avg_price_ticks,
                fused_px,
                fused_fee,
            );
            if has_pnl_field {
                total_closed_pnl_1e8 = total_closed_pnl_1e8.saturating_add(fused_pnl_1e8);
            }
            accum_lots = accum_lots.saturating_add(missing_lots);
            weighted_entry_sum += (missing_lots as i128) * (avg_price_ticks.0 as i128);
            weighted_exit_sum += (missing_lots as i128) * (fused_px.0 as i128);
            legs_count = legs_count.saturating_add(1);
        }

        let denom = accum_lots as i128;
        let verified_entry_ticks = match side {
            Side::Bid => ((weighted_entry_sum + denom - 1) / denom) as i64,
            Side::Ask => (weighted_entry_sum / denom).max(1) as i64,
        };
        let verified_exit_ticks = match side {
            Side::Bid => (weighted_exit_sum / denom).max(1) as i64,
            Side::Ask => ((weighted_exit_sum + denom - 1) / denom) as i64,
        };
        let verified_entry_price = PriceTicks(verified_entry_ticks.max(1));
        let verified_exit_price = PriceTicks(verified_exit_ticks.max(1));

        let (kind, sub_reason) = if any_market_or_forced {
            (
                ClosedTradeKind::ServerMarkPriceStopLoss,
                ExitSubReason::WaldBaseStopLoss,
            )
        } else {
            let sub = Self::classify_maker_exit_sub_reason(
                side,
                verified_entry_price,
                verified_exit_price,
                tranche1_harvested,
                legs_count,
                vac_level_hint,
            );
            (ClosedTradeKind::MakerPassiveUnwind, sub)
        };

        ClosedPnlQueryOutcome::Verified(VerifiedCloseResolution {
            kind,
            sub_reason,
            verified_entry_price,
            verified_exit_price,
            closed_lots: LotQty(accum_lots),
            legs_count,
            exchange_closed_pnl_1e8: if has_pnl_field {
                Some(total_closed_pnl_1e8)
            } else {
                None
            },
        })
    }

    /// Safe Fallback: Used only if /v5/position/closed-pnl fails after bounded retries.
    #[must_use]
    pub fn resolve_fallback_unverified_close(
        &self,
        snap: &SymbolLobSnapshot,
        vac_level_hint: usize,
    ) -> Option<VerifiedCloseResolution> {
        let OrderLifecycleState::PositionOpenMakerExit {
            side,
            avg_price_ticks,
            worst_adverse_price,
            initial_lots,
            open_lots,
            tranche1_harvested,
            tranche1_lots,
            tranche1_exit_price,
            resting_exit_price,
            sl_trigger_price,
            ..
        } = self.state
        else {
            return None;
        };
        let eff_sl = if self.last_eff_sl_ticks.0 > 0 {
            self.last_eff_sl_ticks
        } else {
            sl_trigger_price
        };
        let cur_bbo = match side {
            Side::Bid => snap.asks[0].price,
            Side::Ask => snap.bids[0].price,
        };
        let mkt_ref = if snap.mark_price.0 > 0 {
            snap.mark_price
        } else if snap.last_trade_price.0 > 0 {
            snap.last_trade_price
        } else {
            cur_bbo
        };
        let sl_breached = match side {
            Side::Bid => {
                (mkt_ref.0 > 0 && mkt_ref.0 <= eff_sl.0.saturating_add(2))
                    || (worst_adverse_price.0 > 0 && worst_adverse_price.0 <= eff_sl.0)
            }
            Side::Ask => {
                (mkt_ref.0 > 0 && mkt_ref.0 >= eff_sl.0.saturating_sub(2))
                    || worst_adverse_price.0 >= eff_sl.0
            }
        };
        let no_resting_tp_underwater = resting_exit_price.0 <= 0
            && match side {
                Side::Bid => mkt_ref.0 > 0 && mkt_ref.0 < avg_price_ticks.0,
                Side::Ask => mkt_ref.0 > avg_price_ticks.0,
            };
        let tot_lots = initial_lots.max(open_lots).max(LotQty(1));
        if sl_breached || no_resting_tp_underwater {
            let sl_exit_px = if sl_breached {
                eff_sl
            } else {
                PriceTicks(mkt_ref.0.max(1))
            };
            return Some(VerifiedCloseResolution {
                kind: ClosedTradeKind::ServerMarkPriceStopLoss,
                sub_reason: ExitSubReason::WaldBaseStopLoss,
                verified_entry_price: avg_price_ticks,
                verified_exit_price: sl_exit_px,
                closed_lots: tot_lots,
                legs_count: if tranche1_harvested { 2 } else { 1 },
                exchange_closed_pnl_1e8: None,
            });
        }

        let q_fl = defensive_zero_loss_maker_exit_price(side, avg_price_ticks);
        let t2_px = if resting_exit_price.0 > 0 {
            resting_exit_price
        } else {
            q_fl
        };
        let exit_px = if tranche1_harvested && tranche1_lots.0 > 0 {
            let t1_px = if tranche1_exit_price.0 > 0 {
                tranche1_exit_price
            } else {
                q_fl
            };
            let q1 = tranche1_lots.0 as i128;
            let q2 = open_lots.0.max(1) as i128;
            let w_sum = q1 * (t1_px.0 as i128) + q2 * (t2_px.0 as i128);
            let denom = (q1 + q2).max(1);
            let avg_ex = match side {
                Side::Bid => (w_sum / denom).max(1) as i64,
                Side::Ask => ((w_sum + denom - 1) / denom) as i64,
            };
            PriceTicks(avg_ex)
        } else {
            t2_px
        };
        let sub_reason = Self::classify_maker_exit_sub_reason(
            side,
            avg_price_ticks,
            exit_px,
            tranche1_harvested,
            if tranche1_harvested { 2 } else { 1 },
            vac_level_hint,
        );
        Some(VerifiedCloseResolution {
            kind: ClosedTradeKind::MakerPassiveUnwind,
            sub_reason,
            verified_entry_price: avg_price_ticks,
            verified_exit_price: exit_px,
            closed_lots: tot_lots,
            legs_count: if tranche1_harvested { 2 } else { 1 },
            exchange_closed_pnl_1e8: None,
        })
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
            && matches!(resolution.sub_reason, ExitSubReason::LvdbsDeepVacuumSurf)
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

    /// Unified Canonical Exit Quoter (Bayesian MAE Cushion + $5.00 Split + LVDBS L1..L4).
    #[must_use]
    pub fn compute_canonical_exit_quote(
        &self,
        snap: &SymbolLobSnapshot,
    ) -> Option<CanonicalExitQuote> {
        let OrderLifecycleState::PositionOpenMakerExit {
            side,
            avg_price_ticks,
            peak_favorable_price,
            open_lots,
            tranche1_harvested,
            defensive_floor_locked,
            structural_tp_price,
            opened_ts,
            ..
        } = self.state
        else {
            return None;
        };
        if self.slot_idx >= MAX_ACTIVE_PAIRS {
            return None;
        }
        let spec = APPROVED_SPECS[self.slot_idx];
        let fee_rt = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
        let avg_denom = avg_price_ticks.0.max(1);
        let cur_bbo = match side {
            Side::Bid => snap.asks[0].price,
            Side::Ask => snap.bids[0].price,
        };
        let cur_diff = match side {
            Side::Bid => cur_bbo.0 - avg_price_ticks.0,
            Side::Ask => avg_price_ticks.0 - cur_bbo.0,
        };
        let cur_gross_bps = (cur_diff.saturating_mul(1_000_000)) / avg_denom;

        let hold_ns = snap.ts_local_ns.0.saturating_sub(opened_ts.0).max(0);
        let hold_s = hold_ns / 1_000_000_000;
        let dir_s = side.sign_f64();
        let s_ofi = snap.factor_ofi.get() * dir_s;
        let s_tfi = snap.factor_tfi.get() * dir_s;
        let s_geom = snap.factor_geom.get() * dir_s;
        let signed_flow = 0.55 * s_ofi + 0.45 * s_tfi;
        let deep_joint_adverse =
            s_ofi <= -0.32 && s_tfi <= -0.35 && signed_flow <= -0.38;

        let tau_mkt_s = self.intrinsic_tau_mkt_s.max(90);
        let time_step_s = (tau_mkt_s / 5).max(18);
        let time_c = (hold_s / time_step_s).min(18) * 12;
        let flow_c = if signed_flow < -0.35 {
            ((-signed_flow - 0.35) * 280.0).round() as i64
        } else {
            0
        };
        let depth_c = if s_geom < -0.25 {
            ((-s_geom - 0.25) * 180.0).round() as i64
        } else {
            0
        };
        let wald_pct = ((time_c + flow_c + depth_c) / 10).clamp(0, 48);

        let state_pk_diff =
            (peak_favorable_price.0 - avg_price_ticks.0).saturating_mul(side.sign_i64());
        let state_pk_bps = (state_pk_diff.saturating_mul(1_000_000)) / avg_denom;
        let peak_gross_bps = self
            .last_closed_mfe_bps
            .max(state_pk_bps)
            .max(cur_gross_bps);
        let worst_mae_bps = self.last_closed_mae_bps.min(cur_gross_bps.min(0));
        let init_mfe = (fee_rt * 17) / 10;
        let mfe_c = self.learned_mfe_bps.clamp((fee_rt * 27) / 20, fee_rt * 4);
        let tau_bps = spec
            .tick_to_price_scaled_bps(avg_price_ticks)
            .unwrap_or(ScaledBps(fee_rt / 8))
            .raw();
        let sl_dist_bps = PairSpec::elastic_server_sl_distance_bps(snap.realized_vol_5m).raw();
        let ir_c = self.learned_ir_x100.clamp(-60, 120);
        let gamma_permille = (220
            + (ir_c * 5) / 6
            + ((mfe_c - 2 * fee_rt) * 100) / (10 * fee_rt).max(1))
        .clamp(180, 340);
        let sl_cushion_bps = (sl_dist_bps.saturating_mul(gamma_permille)) / 1_000;
        let mae_mag = 1_200_i64
            .max(4 * tau_bps)
            .max(sl_cushion_bps)
            .max(self.learned_ret_std_bps / 5);
        let mae_trig_bps = -mae_mag;

        let hurst_h = snap.wave_observables().hurst_exponent();
        let wave_mod_opt =
            crate::wave_elliott::CausalElliottWaveState::evaluate_wave_entry_modulation(
                snap.wave_observables(),
                side,
                0.50,
            )
            .ok();
        let wave_def_mult = wave_mod_opt.map_or(1_000, |m| m.def_horizon_permille_mult);
        let flow_persist_mult = if signed_flow > 0.12 {
            let f_b = ((signed_flow - 0.12) * 620.0).round() as i64;
            let h_b = ((hurst_h - 0.50).max(0.0) * 450.0).round() as i64;
            (1_000 + f_b + h_b).clamp(1_000, 1_650)
        } else {
            1_000
        };
        let ir_mult_permille = 1_000 + (ir_c.clamp(-40, 80) * 10) / 3;
        let raw_def_s = ((((tau_mkt_s / 4) * mfe_c) / init_mfe.max(1))
            * ir_mult_permille)
            / 1_000;
        let base_t_def_s = raw_def_s.clamp(40, 75);
        let t_def_s = (((base_t_def_s * wave_def_mult) / 1_000) * flow_persist_mult)
            / 1_000;
        let flow_w_s = (signed_flow.max(0.0) * 30.0).round() as i64;
        let calm_w_s = if signed_flow > 0.08 && worst_mae_bps > -600 {
            ((600 + worst_mae_bps).clamp(0, 600) * 12) / 600
        } else {
            0
        };
        let win_window_s = 25 + flow_w_s + calm_w_s;
        let t_scratch_s = t_def_s + win_window_s;

        let adverse_stress_bps = -((mae_mag * 3) / 5);
        let confirmed_joint_adverse = deep_joint_adverse
            && cur_gross_bps <= adverse_stress_bps
            && worst_mae_bps <= adverse_stress_bps;
        let hard_adverse = worst_mae_bps <= mae_trig_bps
            || (hold_s >= (t_def_s * 3) / 5 && confirmed_joint_adverse);
        let defensive_now = if tranche1_harvested {
            let t_def_t2 = t_scratch_s + 30;
            let t2_joint_adv = deep_joint_adverse
                && cur_gross_bps <= mae_trig_bps
                && worst_mae_bps <= mae_trig_bps;
            defensive_floor_locked
                || worst_mae_bps <= (mae_trig_bps - (fee_rt * 3) / 10)
                || hold_s >= t_def_t2
                || (hold_s >= t_def_s && t2_joint_adv)
        } else {
            defensive_floor_locked || hard_adverse || hold_s >= t_scratch_s
        };

        let win_gross = (fee_rt * 13) / 10;
        let ratchet_arm = (fee_rt * 139) / 100;
        let ratchet_floor_min = fee_rt + (fee_rt * 16) / 100;
        let win_ratchet_floor = fee_rt + (fee_rt * 3) / 10;
        let retrace_from_peak = peak_gross_bps.saturating_sub(cur_gross_bps);
        let dyn_win_floor = if peak_gross_bps >= ratchet_arm {
            (fee_rt + ((peak_gross_bps - fee_rt) * 72) / 100).max(win_ratchet_floor)
        } else {
            0
        };
        let eff_latched_win = self.ratchet_locked_gross_bps.max(
            if peak_gross_bps >= ratchet_arm
                && (retrace_from_peak >= (fee_rt * 29) / 100
                    || (retrace_from_peak >= fee_rt / 8 && signed_flow < 0.12))
            {
                dyn_win_floor
            } else {
                0
            },
        );
        let mfe_ratchet_lock = if tranche1_harvested {
            (hold_s >= (t_def_s * 3) / 5
                && cur_gross_bps >= fee_rt + (fee_rt * 21) / 100
                && signed_flow < -0.10)
                || (peak_gross_bps >= fee_rt * 2 + 100
                    && cur_gross_bps >= win_gross
                    && retrace_from_peak >= (fee_rt * 5) / 8
                    && signed_flow < 0.05)
        } else {
            eff_latched_win >= win_ratchet_floor
                || (peak_gross_bps >= ratchet_arm
                    && cur_gross_bps >= ratchet_floor_min
                    && signed_flow < 0.12
                    && (retrace_from_peak >= (fee_rt * 3) / 20 || signed_flow < -0.05))
        };

        let ratchet_bps = if eff_latched_win >= win_ratchet_floor && !tranche1_harvested {
            if cur_gross_bps >= win_ratchet_floor {
                eff_latched_win.min(cur_gross_bps.max(win_ratchet_floor))
            } else {
                win_ratchet_floor
            }
        } else if peak_gross_bps > fee_rt * 2 {
            let r72 = fee_rt + ((peak_gross_bps - fee_rt) * 72) / 100;
            r72.min(cur_gross_bps.max(win_ratchet_floor))
        } else {
            cur_gross_bps.clamp(ratchet_floor_min, (fee_rt * 27) / 20)
        };

        let q_floor = defensive_zero_loss_maker_exit_price(side, avg_price_ticks);
        let win1_floor = offensive_win_maker_exit_price(side, avg_price_ticks, hurst_h);
        let wave_t1_adj = wave_mod_opt.map_or(0, |m| m.tranche1_permille_delta);
        let eff_t1_permille = (self.learned_t1_permille + wave_t1_adj).clamp(350, 650);
        let split_t1_opt = if !tranche1_harvested && !defensive_now && !mfe_ratchet_lock {
            Self::compute_tranche1_split_lots(
                self.slot_idx,
                open_lots,
                win1_floor,
                eff_t1_permille,
            )
        } else {
            None
        };
        let struct_floor = if structural_tp_price.0 > 0 {
            structural_tp_price
        } else {
            dynamic_maker_floor_price(side, avg_price_ticks, ScaledBps(win_gross))
        };

        let win_harvest_stage = !tranche1_harvested
            && !defensive_now
            && !mfe_ratchet_lock
            && hold_s >= t_def_s;

        let (role, sub_reason, raw_target_px, order_lots, vac_level, def_lock) =
            if let Some(t1_order_lots) = split_t1_opt {
                (
                    ExitOrderRole::Tranche1Harvest50,
                    ExitSubReason::Tranche1Harvest50,
                    win1_floor,
                    t1_order_lots,
                    1_usize,
                    false,
                )
            } else if win_harvest_stage {
                (
                    ExitOrderRole::StandardMakerExit100,
                    ExitSubReason::LvdbsDeepVacuumSurf,
                    win1_floor,
                    open_lots,
                    1_usize,
                    false,
                )
            } else if defensive_now || mfe_ratchet_lock {
                let use_win_ratchet = mfe_ratchet_lock
                    && (!defensive_now || cur_gross_bps >= win_ratchet_floor);
                let fl_bps = if use_win_ratchet {
                    ScaledBps(ratchet_bps)
                } else {
                    ScaledBps(if tranche1_harvested {
                        ratchet_floor_min
                    } else {
                        fee_rt + 40
                    })
                };
                let fl_px = dynamic_maker_floor_price(side, avg_price_ticks, fl_bps);
                let above_qf = match side {
                    Side::Bid => fl_px.0 > q_floor.0,
                    Side::Ask => fl_px.0 < q_floor.0,
                };
                let sub = if above_qf && fl_bps.raw() >= win_gross {
                    ExitSubReason::LvdbsDeepVacuumSurf
                } else {
                    ExitSubReason::AdverseScratchEscape
                };
                (
                    ExitOrderRole::StandardMakerExit100,
                    sub,
                    fl_px,
                    open_lots,
                    1_usize,
                    defensive_now,
                )
            } else {
                let (vac_lvl, lvdbs_px, _) =
                    OrthogonalFactorState::compute_l2_l3_vacuum_surf_target_with_structural(
                        side,
                        snap,
                        q_floor,
                        struct_floor,
                    );
                let role_surf = if tranche1_harvested {
                    ExitOrderRole::LvdbsDeepSurf50
                } else {
                    ExitOrderRole::StandardMakerExit100
                };
                let above_qf = match side {
                    Side::Bid => lvdbs_px.0 > q_floor.0,
                    Side::Ask => lvdbs_px.0 < q_floor.0,
                };
                let sub_surf = if above_qf && (vac_lvl >= 2 || struct_floor.0 != q_floor.0) {
                    ExitSubReason::LvdbsDeepVacuumSurf
                } else {
                    ExitSubReason::AdverseScratchEscape
                };
                (role_surf, sub_surf, lvdbs_px, open_lots, vac_lvl, false)
            };

        let min_sell_px = snap.asks[0].price.0.max(snap.bids[0].price.0 + 1).max(1);
        let max_buy_px = if snap.bids[0].price.0 > 0 && snap.asks[0].price.0 > 1 {
            snap.bids[0].price.0.min(snap.asks[0].price.0 - 1).max(1)
        } else {
            snap.bids[0].price.0.max(1)
        };
        let target_price = match side {
            Side::Bid => PriceTicks(raw_target_px.0.max(min_sell_px).max(q_floor.0)),
            Side::Ask => PriceTicks(raw_target_px.0.min(max_buy_px).min(q_floor.0).max(1)),
        };
        Some(CanonicalExitQuote {
            role,
            sub_reason,
            target_price,
            order_lots,
            vac_level,
            cur_gross_bps,
            wald_pct,
            defensive_lock_active: def_lock,
            urgent_close_repeg: defensive_now || mfe_ratchet_lock,
        })
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
            spec.compute_leg_pnl_and_fee_1e8(
                side,
                eff_lots,
                entry_price,
                exit_price,
                fee_scaled_bps,
            );
        let net_pnl_1e8 = exchange_closed_pnl_1e8.unwrap_or(calc_pnl_1e8);
        let entry_notional_1e8 = spec.lot_notional_1e8(eff_lots, entry_price);
        let (gross_scaled_bps, net_scaled_bps) = if exchange_closed_pnl_1e8.is_some()
            && entry_notional_1e8 > 0
        {
            let exact_net =
                ((net_pnl_1e8 as i128 * 1_000_000) / (entry_notional_1e8 as i128)) as i64;
            let exact_gross = exact_net.saturating_add(fee_scaled_bps.raw());
            let is_t1_sub =
                matches!(self.last_exit_sub_reason, ExitSubReason::Tranche1Harvest50);
            if exact_net >= 100 && is_t1_sub {
                self.last_exit_sub_reason = ExitSubReason::LvdbsDeepVacuumSurf;
            }
            (ScaledBps(exact_gross), ScaledBps(exact_net))
        } else {
            (calc_gross_bps, calc_net_bps)
        };

        self.fold_price_into_excursions(side, entry_price, exit_price);
        ledger.record_closed_trade_with_sinking_split(
            net_pnl_1e8,
            fee_1e8,
            net_scaled_bps,
        );
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
            side,
            avg_price_ticks,
            open_lots,
            tranche1_harvested,
            tranche1_lots,
            tranche1_exit_price,
            ..
        } = self.state
        else {
            return None;
        };
        let q2 = LotQty(open_lots.0.max(1));
        if !tranche1_harvested || tranche1_lots.0 <= 0 || tranche1_exit_price.0 <= 0 {
            return Some(self.finalize_close_with_lots(
                leg2_kind,
                side,
                avg_price_ticks,
                leg2_exit_price,
                q2,
                1,
                None,
                closed_ts,
                ledger,
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
        let (pnl1, fee1, g1, _) = spec.compute_leg_pnl_and_fee_1e8(
            side,
            q1,
            avg_price_ticks,
            tranche1_exit_price,
            fee1_bps,
        );
        let (pnl2, fee2, g2, _) = spec.compute_leg_pnl_and_fee_1e8(
            side,
            q2,
            avg_price_ticks,
            leg2_exit_price,
            fee2_bps,
        );

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
        ledger.record_closed_trade_with_sinking_split(
            net_pnl_1e8,
            fee_1e8,
            ScaledBps(net_bps),
        );
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
            side,
            avg_price_ticks,
            mut peak_favorable_price,
            mut worst_adverse_price,
            sl_trigger_price,
            ..
        } = self.state
        else {
            return None;
        };

        let cur_bbo_ref = match side {
            Side::Bid => snap.asks[0].price,
            Side::Ask => snap.bids[0].price,
        };
        let ref_trade = if snap.last_trade_price.0 > 0 {
            snap.last_trade_price
        } else {
            cur_bbo_ref
        };

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

        let peak_diff = match side {
            Side::Bid => peak_favorable_price.0 - avg_price_ticks.0,
            Side::Ask => avg_price_ticks.0 - peak_favorable_price.0,
        };
        let worst_diff = match side {
            Side::Bid => worst_adverse_price.0 - avg_price_ticks.0,
            Side::Ask => avg_price_ticks.0 - worst_adverse_price.0,
        };
        let avg_denom = avg_price_ticks.0.max(1);
        self.last_closed_mfe_bps = (peak_diff.saturating_mul(1_000_000)) / avg_denom;
        self.last_closed_mae_bps = (worst_diff.saturating_mul(1_000_000)) / avg_denom;
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
                ClosedTradeKind::ServerMarkPriceStopLoss,
                eff_sl_now,
                snap.ts_local_ns,
                ledger,
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
            && (quote.vac_level >= 2
                || matches!(quote.sub_reason, ExitSubReason::LvdbsDeepVacuumSurf))
        {
            self.lvdbs_deep_surfs_count = self.lvdbs_deep_surfs_count.saturating_add(1);
            self.last_exit_sub_reason = ExitSubReason::LvdbsDeepVacuumSurf;
        } else {
            self.last_exit_sub_reason = quote.sub_reason;
        }
        self.finalize_open_position_close(
            ClosedTradeKind::MakerPassiveUnwind,
            quote.target_price,
            snap.ts_local_ns,
            ledger,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lob_shm::{BookLevel, MappedShmRegion};
    use crate::types::{BoundedScore, BYBIT_MIN_ORDER_NOTIONAL_1E8};

    #[test]
    fn test_hmac_sha256_bybit_v5_signature_and_quantum_floors() {
        let sig = sign_bybit_v5_hmac_sha256("test_secret", "1700000000000api_key5000");
        assert_eq!(sig.len(), 64);
        let floor_bid = defensive_zero_loss_maker_exit_price(Side::Bid, PriceTicks(8_500));
        assert!(floor_bid.0 >= 8_504);
        let floor_ask = defensive_zero_loss_maker_exit_price(Side::Ask, PriceTicks(8_500));
        assert!(floor_ask.0 <= 8_496);
    }

    #[test]
    fn test_moallemi_queue_depletion_velocity_shield() {
        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = APPROVED_SPECS[3].symbol_id;
        snap.bids[0] = BookLevel { price: PriceTicks(8_200), qty: LotQty(300) };
        snap.asks[0] = BookLevel { price: PriceTicks(8_201), qty: LotQty(250) };
        snap.realized_vol_5m = ScaledBps(6_000);
        snap.factor_ofi = BoundedScore::new_clamped(-0.60);
        snap.factor_tfi = BoundedScore::new_clamped(-0.55);
        assert!(check_queue_depletion_velocity_abort(
            Side::Bid,
            LotQty(1_000),
            LotQty(300),
            &snap,
        ));

        snap.factor_ofi = BoundedScore::new_clamped(0.01);
        snap.factor_tfi = BoundedScore::new_clamped(0.85);
        snap.factor_geom = BoundedScore::new_clamped(-0.15);
        assert!(check_queue_depletion_velocity_abort(
            Side::Bid,
            LotQty(717),
            LotQty(90),
            &snap,
        ));
    }

    #[test]
    fn test_governor_12x2_loss_lab_62_of_62_and_2160_manifold() {
        let (mitigated_all, manifold_2160) =
            OnlineAdaptiveGovernor::audit_unified_62_losses_and_2160_manifold();
        assert_eq!(mitigated_all, UNIFIED_LOSS_LAB_COUNT);
        assert_eq!(manifold_2160, 2_160);

        let gov = OnlineAdaptiveGovernor::new();
        assert!(gov
            .evaluate_kinematic_energy_surface(1, Side::Bid, 105, 148, 0.24, 0.60)
            .is_ok());
        assert!(gov
            .evaluate_kinematic_energy_surface(7, Side::Bid, 514, 381, 0.71, 0.84)
            .is_ok());
        assert!(gov
            .evaluate_kinematic_energy_surface(9, Side::Bid, 898, 591, 0.51, 0.70)
            .is_err());
        assert!(gov
            .evaluate_kinematic_energy_surface(1, Side::Bid, 220, 150, 0.22, 0.47)
            .is_err());

        let chase_ns = gov.max_total_entry_chase_ns(9, ScaledBps(2_090));
        assert!((3_200_000_000..=5_600_000_000).contains(&chase_ns));
    }

    #[test]
    fn test_elastic_notional_split_guard_sui_10001_bayesian_mae_and_leg_fusion() {
        let sui_spec = APPROVED_SPECS[5];
        assert_eq!(
            SlotExecutionEngine::compute_tranche1_split_lots(
                5,
                LotQty(10),
                PriceTicks(8_389),
                500,
            ),
            None
        );

        let mut sui_eng = SlotExecutionEngine::new(5);
        let mut sui_snap = SymbolLobSnapshot::new_empty();
        sui_snap.symbol_id = sui_spec.symbol_id;
        sui_snap.realized_vol_5m = ScaledBps(6_000);
        sui_snap.ts_local_ns = TimestampNs(1_000_000_000);
        sui_snap.bids[0] = BookLevel { price: PriceTicks(8_385), qty: LotQty(300) };
        sui_snap.asks[0] = BookLevel { price: PriceTicks(8_386), qty: LotQty(280) };
        sui_snap.mark_price = PriceTicks(8_385);
        sui_snap.last_trade_price = PriceTicks(8_385);
        let v_sui = crate::alpha_3factor::SignalVerdict::AuthorizeMakerEntry {
            side: Side::Bid,
            target_excursion_scaled_bps: ScaledBps(650),
        };
        assert!(sui_eng.try_arm_maker_entry(&sui_snap, v_sui, LotQty(16)).is_ok());
        assert!(sui_eng
            .on_maker_entry_filled_verified(
                TimestampNs(1_500_000_000),
                Some(Side::Bid),
                Some(PriceTicks(8_385)),
                Some(LotQty(10)),
            )
            .is_ok());
        let q_partial = sui_eng.compute_canonical_exit_quote(&sui_snap);
        assert!(q_partial.is_some());
        if let Some(qp) = q_partial {
            assert_eq!(qp.role, ExitOrderRole::StandardMakerExit100);
            assert_eq!(qp.order_lots, LotQty(10));
            assert!(
                sui_spec.lot_notional_1e8(qp.order_lots, qp.target_price)
                    >= BYBIT_MIN_ORDER_NOTIONAL_1E8
            );
        }

        let mut gov = OnlineAdaptiveGovernor::new();
        for k in 0..4_i64 {
            gov.observe_closed_trade_at_entry(
                5,
                Side::Bid,
                PriceTicks(8_400),
                ScaledBps(450),
                true,
                TimestampNs((k + 1) * 900_000_000_000),
            );
        }
        let phi_champ = gov.learned_tranche1_fraction_permille(5, Side::Bid, 1_100);
        assert!((350..450).contains(&phi_champ));
        gov.observe_closed_trade_at_entry(
            7,
            Side::Bid,
            PriceTicks(16_411),
            ScaledBps(-3_274),
            false,
            TimestampNs(5_000_000_000_000),
        );
        let phi_scar = gov.learned_tranche1_fraction_permille(7, Side::Bid, 680);
        assert!((600..=650).contains(&phi_scar));

        let (mae_tao, def_s_tao) = gov.elastic_mae_trigger_and_def_horizon_s(
            4,
            Side::Ask,
            PriceTicks(22_113),
            ScaledBps(1_510),
            688,
        );
        assert!(mae_tao <= -1_300);
        assert!(def_s_tao >= 60);

        let mut link_eng = SlotExecutionEngine::new(9);
        let mut link_snap = SymbolLobSnapshot::new_empty();
        link_snap.symbol_id = APPROVED_SPECS[9].symbol_id;
        link_snap.realized_vol_5m = ScaledBps(2_090);
        link_snap.ts_local_ns = TimestampNs(1_000_000_000);
        link_snap.bids[0] = BookLevel { price: PriceTicks(10_468), qty: LotQty(200) };
        link_snap.asks[0] = BookLevel { price: PriceTicks(10_469), qty: LotQty(200) };
        link_snap.mark_price = PriceTicks(10_468);
        link_snap.last_trade_price = PriceTicks(10_468);
        let v_link = crate::alpha_3factor::SignalVerdict::AuthorizeMakerEntry {
            side: Side::Bid,
            target_excursion_scaled_bps: ScaledBps(650),
        };
        assert!(link_eng.try_arm_maker_entry(&link_snap, v_link, LotQty(12)).is_ok());
        assert!(link_eng
            .on_maker_entry_filled(TimestampNs(2_000_000_000))
            .is_ok());
        let init_sl = link_eng.last_eff_sl_ticks;
        assert!(init_sl.0 >= 10_405 && init_sl.0 < 10_468);
        link_snap.ts_local_ns = TimestampNs(320_000_000_000);
        link_snap.mark_price = PriceTicks(10_450);
        link_snap.last_trade_price = PriceTicks(10_450);
        link_snap.factor_ofi = BoundedScore::new_clamped(-0.64);
        link_snap.factor_tfi = BoundedScore::new_clamped(-0.45);
        link_snap.factor_geom = BoundedScore::new_clamped(-0.35);
        let mut btc_bear = link_snap;
        btc_bear.symbol_id = APPROVED_SPECS[0].symbol_id;
        btc_bear.factor_ofi = BoundedScore::new_clamped(-0.55);
        btc_bear.factor_tfi = BoundedScore::new_clamped(-0.75);
        if let Some(q_l) = link_eng.compute_canonical_exit_quote(&link_snap) {
            link_eng.last_closed_wald_pct = q_l.wald_pct;
        }
        let ratcheted = link_eng.maybe_ratchet_wald_stop_loss(&link_snap, &btc_bear);
        assert!(ratcheted.is_some());
        if let Some(new_sl) = ratcheted {
            assert!(new_sl.0 >= init_sl.0 + 3);
        }

        let mut pendle_eng = SlotExecutionEngine::new(7);
        let mut p_snap = SymbolLobSnapshot::new_empty();
        p_snap.symbol_id = APPROVED_SPECS[7].symbol_id;
        p_snap.realized_vol_5m = ScaledBps(2_000);
        p_snap.ts_local_ns = TimestampNs(1_794_426_523_000_000_000);
        p_snap.bids[0] = BookLevel { price: PriceTicks(15_520), qty: LotQty(100) };
        p_snap.asks[0] = BookLevel { price: PriceTicks(15_521), qty: LotQty(100) };
        p_snap.mark_price = PriceTicks(15_520);
        p_snap.last_trade_price = PriceTicks(15_520);
        let v_pen = crate::alpha_3factor::SignalVerdict::AuthorizeMakerEntry {
            side: Side::Bid,
            target_excursion_scaled_bps: ScaledBps(650),
        };
        assert!(pendle_eng.try_arm_maker_entry(&p_snap, v_pen, LotQty(8)).is_ok());
        assert!(pendle_eng
            .on_maker_entry_filled_verified(
                TimestampNs(1_794_426_523_689_691_597),
                Some(Side::Bid),
                Some(PriceTicks(15_520)),
                Some(LotQty(8)),
            )
            .is_ok());
        assert!(pendle_eng.record_tranche1_harvest(PriceTicks(15_528)));
        if let OrderLifecycleState::PositionOpenMakerExit {
            ref mut resting_exit_price,
            ..
        } = pendle_eng.state
        {
            *resting_exit_price = PriceTicks(15_537);
        }
        let lagging_payload = serde_json::json!({
            "retCode": 0,
            "result": {
                "list": [{
                    "symbol": "PENDLEUSDT",
                    "side": "Sell",
                    "orderType": "Limit",
                    "execType": "Trade",
                    "closedSize": "4",
                    "avgEntryPrice": "1.5520",
                    "avgExitPrice": "1.5528",
                    "closedPnl": "0.00071616",
                    "updatedTime": "1794426609000"
                }]
            }
        });
        assert_eq!(
            pendle_eng.reconcile_closed_pnl_payload(&lagging_payload, 3, false),
            ClosedPnlQueryOutcome::PendingIndexerRetry
        );
        let fused_out =
            pendle_eng.reconcile_closed_pnl_payload(&lagging_payload, 3, true);
        if let ClosedPnlQueryOutcome::Verified(res) = fused_out {
            assert_eq!(res.closed_lots, LotQty(8));
            assert_eq!(res.legs_count, 2);
            assert_eq!(res.sub_reason, ExitSubReason::LvdbsDeepVacuumSurf);
            assert_eq!(res.verified_exit_price, PriceTicks(15_532));
            assert!(res.exchange_closed_pnl_1e8.unwrap_or(0) > 140_000);
        } else {
            assert_eq!(pendle_eng.slot_idx, 999);
        }
    }

    #[test]
    fn test_live_singapur_bid_deadlock_eradicated_and_zombie_anchor_expires() {
        let live_snap_json = serde_json::json!({
            "event": "GOV_SCAR_SNAPSHOT",
            "ts_ns": 1_794_379_228_780_565_050_i64,
            "cum_net_bps_x100": [
                [1038, -133], [-5389, 3556], [1580, 8780], [9646, 10982],
                [4198, 7192], [5866, 7503], [6478, 0], [-24000, 301],
                [5560, 0], [4426, 2801], [1280, 425], [4087, 764]
            ],
            "directional_cooldown_until_ns": [
                [1_794_239_964_703_437_143_i64, 1_794_242_544_152_966_210_i64],
                [1_794_239_969_540_647_262_i64, 1_794_379_339_780_565_050_i64],
                [1_794_191_589_360_755_839_i64, 1_794_189_735_509_464_253_i64],
                [1_794_211_446_079_196_625_i64, 1_794_223_784_956_420_890_i64],
                [1_794_235_041_996_258_884_i64, 1_794_241_637_703_685_870_i64],
                [1_794_239_784_220_319_099_i64, 1_794_241_195_997_923_027_i64],
                [1_794_214_953_878_548_667_i64, 0],
                [1_794_242_814_128_285_563_i64, 1_794_092_589_432_475_272_i64],
                [1_794_239_440_385_316_436_i64, 0],
                [1_794_215_220_723_034_949_i64, 1_794_217_856_259_737_923_i64],
                [1_794_194_445_603_752_116_i64, 1_794_192_799_823_027_285_i64],
                [1_794_223_730_791_615_378_i64, 1_794_241_560_790_167_716_i64]
            ],
            "eq_vol_5m_bps": [607, 837, 847, 1098, 1240, 1247, 1812, 1592, 1559, 1158, 1860, 647],
            "ewma_net_bps_x100": [
                [65, 58], [111, 78], [178, 211], [309, 247],
                [157, 107], [198, 282], [992, 200], [-8046, 178],
                [1207, 200], [261, 231], [330, 256], [402, 278]
            ],
            "global_dir_cooldown_until_ns": [
                1_794_242_694_128_285_563_i64,
                1_794_102_421_290_056_964_i64
            ],
            "global_dir_debt_bps_x100": [-20740, 12000],
            "global_dir_sl_streak": [1, 0],
            "global_dir_win_streak": [0, 3],
            "last_global_win_dir": 1,
            "sl_streak": [
                [0, 1], [2, 0], [0, 0], [0, 0],
                [0, 0], [0, 0], [0, 0], [1, 0],
                [0, 0], [0, 0], [0, 0], [0, 0]
            ],
            "wave_anchor_px_1e4": [
                [7_473_640_000_i64, 0], [2_106_670_000_i64, 0], [88_020_000, 0],
                [0, 80_170_000], [215_980_000, 0], [0, 85_730_000],
                [32_270_000, 0], [0, 0], [15_630_000, 0],
                [0, 104_500_000], [36_050_000, 0], [0, 51_820_000]
            ],
            "win_streak": [
                [6, 0], [6, 0], [3, 0], [0, 3],
                [3, 0], [0, 2], [10, 0], [0, 0],
                [2, 0], [0, 3], [1, 0], [0, 1]
            ]
        });

        let mut gov = OnlineAdaptiveGovernor::new();
        assert!(gov.apply_scar_snapshot_json(&live_snap_json).is_some());
        assert!(gov.eq_vol_5m_bps[0] >= ScaledBps::MIN_REQUIRED_VOL_5M_BPS.raw() * 2);
        let now_live_ns = 1_794_411_268_000_000_000_i64;

        assert!(!gov.is_in_local_slot_scar_debt(0, Side::Bid));
        assert!(gov.is_in_scar_debt(0, Side::Bid));
        assert_eq!(
            gov.audit_historical_entry_veto(3, Side::Bid, 8_550, now_live_ns),
            None
        );
        assert_eq!(
            gov.audit_historical_entry_veto(9, Side::Ask, 10_400, now_live_ns),
            None
        );
        assert_eq!(
            gov.audit_historical_entry_veto(11, Side::Ask, 5_121, now_live_ns),
            None
        );

        let mut btc = SymbolLobSnapshot::new_empty();
        btc.symbol_id = APPROVED_SPECS[0].symbol_id;
        btc.ts_local_ns = TimestampNs(now_live_ns);
        btc.realized_vol_5m = ScaledBps(2_800);
        btc.bids[0] = BookLevel { price: PriceTicks(738_779), qty: LotQty(200) };
        btc.asks[0] = BookLevel { price: PriceTicks(738_780), qty: LotQty(180) };
        btc.refresh_deep_l1_l5_totals();
        btc.factor_ofi = BoundedScore::new_clamped(0.82);
        btc.factor_tfi = BoundedScore::new_clamped(0.92);
        btc.factor_geom = BoundedScore::new_clamped(0.45);
        assert!(gov.authorizes_adaptive_entry_with_wallet(
            0,
            Side::Bid,
            6_628_080_000,
            &btc,
            &btc,
        ));
    }

    #[test]
    fn test_f07_wave_anchor_staircase_25bps_abort_hygiene_and_scar_json() {
        let mut gov = OnlineAdaptiveGovernor::new();
        let t0 = 1_700_000_000_000_000_000_i64;
        for step in 0..3_i64 {
            gov.observe_closed_trade_at_entry(
                3,
                Side::Bid,
                PriceTicks(8_200 + step * 5),
                ScaledBps(220),
                true,
                TimestampNs(t0 + step * 120_000_000_000),
            );
        }
        assert_eq!(gov.wave_anchor_px_1e4[3][0], 82_000_000);
        assert!(gov.slot_information_ratio_x100(3, Side::Bid) > 35);
        gov.observe_closed_trade_at_entry(
            3,
            Side::Ask,
            PriceTicks(8_212),
            ScaledBps(45),
            true,
            TimestampNs(t0 + 400_000_000_000),
        );
        assert_eq!(gov.wave_anchor_px_1e4[3][0], 82_000_000);
        assert_eq!(
            gov.audit_historical_entry_veto(3, Side::Bid, 8_225, t0 + 600_000_000_000),
            Some("VETO_WAVE_STAIRCASE_25BPS")
        );
        assert_eq!(
            gov.audit_historical_entry_veto(3, Side::Bid, 8_225, t0 + 1_600_000_000_000),
            None
        );

        for a in 0..4_i64 {
            gov.observe_entry_abort(
                3,
                Side::Bid,
                PriceTicks(8_205),
                TimestampNs(t0 + 700_000_000_000 + a * 1_000_000_000),
            );
        }
        assert_eq!(
            gov.audit_historical_entry_veto(3, Side::Bid, 8_205, t0 + 705_000_000_000),
            Some("VETO_ABORT_STREAK_BREATHER")
        );
        gov.clear_abort_streak(3, Side::Bid);
        assert_eq!(
            gov.audit_historical_entry_veto(3, Side::Bid, 8_205, t0 + 705_000_000_000),
            None
        );

        gov.observe_closed_trade_at_entry(
            4,
            Side::Ask,
            PriceTicks(22_400),
            ScaledBps(-1_200),
            false,
            TimestampNs(t0 + 800_000_000_000),
        );
        let scar_json = gov.to_scar_snapshot_json(TimestampNs(t0 + 800_000_000_000));
        let mut restored = OnlineAdaptiveGovernor::new();
        assert_eq!(
            restored.apply_scar_snapshot_json(&scar_json),
            Some(TimestampNs(t0 + 800_000_000_000))
        );
        assert_eq!(restored.wave_anchor_px_1e4[3][0], 82_000_000);
        assert_eq!(restored.slot_direction_diagnostics(4, Side::Ask).1, 1);
    }

    #[test]
    fn test_f02_f06_preflight_sync_verified_avg_price_110_145_sl_and_live_adopt() {
        let avax = APPROVED_SPECS[3];
        assert_eq!(
            parse_verified_avg_price_ticks(avax, Side::Bid, "8.20333333"),
            Ok(PriceTicks(8_204))
        );
        assert_eq!(
            parse_verified_avg_price_ticks(avax, Side::Ask, "8.20366667"),
            Ok(PriceTicks(8_203))
        );

        let mut eng = SlotExecutionEngine::new(3);
        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = avax.symbol_id;
        snap.realized_vol_5m = ScaledBps(7_000);
        snap.ts_local_ns = TimestampNs(1_000_000_000);
        snap.bids[0] = BookLevel { price: PriceTicks(8_200), qty: LotQty(300) };
        snap.asks[0] = BookLevel { price: PriceTicks(8_201), qty: LotQty(250) };
        snap.mark_price = PriceTicks(8_200);
        snap.last_trade_price = PriceTicks(8_200);

        let verdict = crate::alpha_3factor::SignalVerdict::AuthorizeMakerEntry {
            side: Side::Bid,
            target_excursion_scaled_bps: ScaledBps(650),
        };
        assert!(eng.try_arm_maker_entry(&snap, verdict, LotQty(26)).is_ok());
        if let OrderLifecycleState::PlacingEntryMaker { sl_trigger_price, .. } = eng.state {
            let sl_dist_bps = ((8_200 - sl_trigger_price.0) * 1_000_000) / 8_200;
            assert!((5_800..=12_500).contains(&sl_dist_bps));
        } else {
            assert_eq!(eng.slot_idx, 999);
        }

        snap.bids[0] = BookLevel { price: PriceTicks(8_204), qty: LotQty(300) };
        snap.asks[0] = BookLevel { price: PriceTicks(8_205), qty: LotQty(250) };
        snap.mark_price = PriceTicks(8_204);
        snap.last_trade_price = PriceTicks(8_204);
        assert_eq!(
            eng.sync_preflight_l1_entry(&snap, 8).map(|p| p.0),
            Ok(PriceTicks(8_204))
        );
        assert!(eng.on_entry_order_resting(TimestampNs(1_030_000_000)).is_ok());
        assert!(eng
            .on_maker_entry_filled_verified(
                TimestampNs(2_000_000_000),
                Some(Side::Bid),
                Some(PriceTicks(8_206)),
                Some(LotQty(24)),
            )
            .is_ok());
        assert_eq!(eng.active_side(), Some(Side::Bid));

        let mut adopt_eng = SlotExecutionEngine::new(3);
        snap.mark_price = PriceTicks(8_100);
        snap.last_trade_price = PriceTicks(8_098);
        let adopted_sl = adopt_eng.adopt_live_open_position(
            Side::Bid,
            PriceTicks(8_200),
            LotQty(26),
            LotQty(13),
            true,
            PriceTicks(8_205),
            PriceTicks(8_098),
            TimestampNs(1_000_000_000),
            &snap,
        );
        assert_eq!(adopted_sl, Ok(PriceTicks(8_095)));
    }

    #[test]
    fn test_f03_closed_pnl_reconciler_eradicates_false_sl_and_catches_real_sl() {
        assert_eq!(parse_signed_decimal_1e8("-0.2845"), Ok(-28_450_000));
        let spec = APPROVED_SPECS[3];
        let mut eng = SlotExecutionEngine::new(3);
        let mut ledger = ParitySyncLedger::new(6_628_080_000);
        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = spec.symbol_id;
        snap.realized_vol_5m = ScaledBps(7_000);
        snap.ts_local_ns = TimestampNs(1_700_000_010_000_000_000);
        snap.bids[0] = BookLevel { price: PriceTicks(8_200), qty: LotQty(300) };
        snap.asks[0] = BookLevel { price: PriceTicks(8_201), qty: LotQty(250) };
        snap.mark_price = PriceTicks(8_200);
        snap.last_trade_price = PriceTicks(8_200);
        let verdict = crate::alpha_3factor::SignalVerdict::AuthorizeMakerEntry {
            side: Side::Bid,
            target_excursion_scaled_bps: ScaledBps(650),
        };
        assert!(eng.try_arm_maker_entry(&snap, verdict, LotQty(26)).is_ok());
        assert!(eng
            .on_maker_entry_filled(TimestampNs(1_700_000_010_500_000_000))
            .is_ok());
        if let OrderLifecycleState::PositionOpenMakerExit {
            ref mut resting_exit_price,
            ..
        } = eng.state
        {
            *resting_exit_price = PriceTicks(8_205);
        }
        snap.mark_price = PriceTicks(8_195);
        let fb = eng.resolve_fallback_unverified_close(&snap, 2);
        assert!(fb.is_some());
        if let Some(fb_res) = fb {
            assert_eq!(fb_res.kind, ClosedTradeKind::MakerPassiveUnwind);
            assert_eq!(fb_res.verified_exit_price, PriceTicks(8_205));
        }

        let win_payload = serde_json::json!({
            "retCode": 0,
            "result": {
                "list": [{
                    "symbol": "AVAXUSDT",
                    "side": "Sell",
                    "orderType": "Limit",
                    "execType": "Trade",
                    "closedSize": "2.6",
                    "avgEntryPrice": "8.200",
                    "avgExitPrice": "8.207",
                    "closedPnl": "0.00968000",
                    "updatedTime": "1700000012000"
                }]
            }
        });
        let out1 = eng.reconcile_closed_pnl_payload(&win_payload, 2, false);
        if let ClosedPnlQueryOutcome::Verified(res1) = out1 {
            assert_eq!(res1.kind, ClosedTradeKind::MakerPassiveUnwind);
            let closed1 = eng.apply_verified_close_resolution(
                res1,
                TimestampNs(1_700_000_012_500_000_000),
                &mut ledger,
            );
            assert!(closed1.is_some());
            if let Some(s1) = closed1 {
                assert!(s1.is_maker_exit);
                assert_eq!(s1.net_pnl_1e8, 968_000);
            }
        } else {
            assert_eq!(eng.slot_idx, 999);
        }
    }

    #[test]
    fn test_f04_f05_f09_lvdbs_btc_unlock_exact_lot_consolidation_and_tear_free_shm() {
        let gov = OnlineAdaptiveGovernor::new();
        let mut ledger = ParitySyncLedger::new(6_628_080_000);

        let mut btc_eng = SlotExecutionEngine::new(0);
        let mut btc_snap = SymbolLobSnapshot::new_empty();
        btc_snap.symbol_id = APPROVED_SPECS[0].symbol_id;
        btc_snap.realized_vol_5m = ScaledBps(6_500);
        btc_snap.ts_local_ns = TimestampNs(1_000_000_000);
        btc_snap.bids[0] = BookLevel { price: PriceTicks(759_000), qty: LotQty(100) };
        btc_snap.asks[0] = BookLevel { price: PriceTicks(759_001), qty: LotQty(100) };
        btc_snap.mark_price = PriceTicks(759_000);
        btc_snap.last_trade_price = PriceTicks(759_000);
        let btc_v = crate::alpha_3factor::SignalVerdict::AuthorizeMakerEntry {
            side: Side::Bid,
            target_excursion_scaled_bps: ScaledBps(600),
        };
        assert!(btc_eng.try_arm_maker_entry(&btc_snap, btc_v, LotQty(1)).is_ok());
        assert!(btc_eng.on_maker_entry_filled(TimestampNs(2_000_000_000)).is_ok());

        btc_snap.ts_local_ns = TimestampNs(20_000_000_000);
        btc_snap.bids[0] = BookLevel { price: PriceTicks(759_333), qty: LotQty(100) };
        btc_snap.asks[0] = BookLevel { price: PriceTicks(759_334), qty: LotQty(100) };
        btc_snap.mark_price = PriceTicks(759_334);
        btc_snap.last_trade_price = PriceTicks(759_334);
        assert!(btc_eng
            .step_open_position(&btc_snap, &btc_snap, &gov, &mut ledger)
            .is_none());

        btc_snap.ts_local_ns = TimestampNs(125_000_000_000);
        let btc_closed = btc_eng.step_open_position(&btc_snap, &btc_snap, &gov, &mut ledger);
        assert!(btc_closed.is_some());
        if let Some(bs) = btc_closed {
            assert_eq!(bs.exit_price, PriceTicks(759_334));
            assert!(bs.net_scaled_bps.raw() >= 40);
            assert_eq!(ledger.closed_trades_count, 1);
        }

        let shm_path = MappedShmRegion::ephemeral_self_test_path("chat13_fsm_f09");
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

        let verdict = crate::alpha_3factor::SignalVerdict::AuthorizeMakerEntry {
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
        assert_eq!(ledger.closed_trades_count, 1);

        if let Ok(ref r) = reg {
            eng.sync_to_shm_telemetry(r.arena());
            if let Ok(tel) = r.arena().telemetry(3) {
                let ts = tel.read_consistent_or_default(4);
                assert_eq!(ts.state_code, 4);
                assert!(ts.tranche1_harvested);
                assert_eq!(ts.open_lots, 12);
                assert_eq!(ts.initial_lots, 26);
            }
        }

        let ask_px = [8_203, 8_205, 8_208, 8_211];
        for (i, &p) in ask_px.iter().enumerate() {
            snap.bids[i] = BookLevel {
                price: PriceTicks(8_202 - i as i64),
                qty: LotQty(350),
            };
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
            assert_eq!(summary.exit_price, PriceTicks(8_206));
            assert_eq!(summary.net_scaled_bps, ScaledBps(659));
            assert!(summary.net_pnl_1e8 > 1_250_000);
            assert_eq!(ledger.closed_trades_count, 2);
        }
        let _ = std::fs::remove_file(&shm_path);
    }

    #[test]
    fn test_v3_forensic_shield_suite_v301_to_v306_and_zero_self_choke() {
        let tmp_scar = std::env::temp_dir().join("vortex_v3_scar_dedup_test.jsonl");
        let tmp_red = std::env::temp_dir().join("vortex_v3_red_loss_test.jsonl");
        let scar_lines = concat!(
            "{\"ts_ns\":1793979746124021595,\"symbol\":\"TAOUSDT\",",
            "\"side\":\"Bid\",\"entry_ticks\":22413,",
            "\"net_bps\":0.46,\"maker_exit\":true}\n",
            "{\"ts_ns\":1793979746124021595,\"symbol\":\"TAOUSDT\",",
            "\"kind\":\"MakerPassiveUnwind\",\"side\":\"Bid\",",
            "\"entry_ticks\":22413,\"net_bps\":0.46,\"maker_exit\":true}\n",
            "{\"ts_ns\":1793979820548628565,\"symbol\":\"SUIUSDT\",",
            "\"side\":\"Bid\",\"entry_ticks\":8695,",
            "\"net_bps\":4.05,\"maker_exit\":true}\n"
        );
        let red_line = concat!(
            "{\"event\":\"TRADE_CLOSE\",\"ts_ns\":1794242514128285563,",
            "\"symbol\":\"PENDLEUSDT\",\"side\":\"Bid\",\"entry_ticks\":16411,",
            "\"net_bps\":-327.4,\"maker_exit\":false}\n"
        );
        assert!(std::fs::write(&tmp_scar, scar_lines).is_ok());
        assert!(std::fs::write(&tmp_red, red_line).is_ok());

        let mut gov = OnlineAdaptiveGovernor::new();
        let (max_ts, unique_cnt) =
            gov.rehydrate_from_scar_and_loss_files(&tmp_scar, &tmp_red, Some(&tmp_red));
        let _ = std::fs::remove_file(&tmp_scar);
        let _ = std::fs::remove_file(&tmp_red);
        assert_eq!(unique_cnt, 3);
        assert_eq!(max_ts, 1_794_242_514_128_285_563);
        assert!(gov.is_in_scar_debt(7, Side::Bid));
        assert_eq!(gov.slot_direction_diagnostics(5, Side::Bid).2, 1);

        let mut btc = SymbolLobSnapshot::new_empty();
        btc.symbol_id = APPROVED_SPECS[0].symbol_id;
        btc.ts_local_ns = TimestampNs(1_800_000_000_000_000_000);
        btc.realized_vol_5m = ScaledBps(6_500);
        btc.bids[0] = BookLevel { price: PriceTicks(759_000), qty: LotQty(400) };
        btc.asks[0] = BookLevel { price: PriceTicks(759_001), qty: LotQty(300) };
        btc.factor_ofi = BoundedScore::new_clamped(0.55);
        btc.factor_tfi = BoundedScore::new_clamped(0.60);
        btc.factor_geom = BoundedScore::new_clamped(0.25);

        let mut link = SymbolLobSnapshot::new_empty();
        link.symbol_id = APPROVED_SPECS[9].symbol_id;
        link.ts_local_ns = TimestampNs(1_800_000_000_000_000_000);
        link.realized_vol_5m = ScaledBps(6_100);
        link.bids[0] = BookLevel { price: PriceTicks(10_500), qty: LotQty(320) };
        link.asks[0] = BookLevel { price: PriceTicks(10_501), qty: LotQty(240) };
        link.factor_ofi = BoundedScore::new_clamped(0.42);
        link.factor_tfi = BoundedScore::new_clamped(0.44);
        link.factor_geom = BoundedScore::new_clamped(0.08);

        assert!(gov
            .confirm_preflight_kinematics_non_choking(
                9,
                Side::Bid,
                6_628_080_000,
                &link,
                &btc,
            )
            .is_ok());

        let mut ltc = link;
        ltc.symbol_id = APPROVED_SPECS[11].symbol_id;
        ltc.factor_ofi = BoundedScore::new_clamped(0.84);
        ltc.factor_tfi = BoundedScore::new_clamped(0.97);
        ltc.factor_geom = BoundedScore::new_clamped(-0.04);
        assert!(gov
            .confirm_preflight_kinematics_non_choking(
                11,
                Side::Bid,
                6_628_080_000,
                &ltc,
                &btc,
            )
            .is_ok());

        link.factor_geom = BoundedScore::new_clamped(-0.04);
        assert!(gov
            .confirm_preflight_kinematics_non_choking(
                9,
                Side::Bid,
                6_628_080_000,
                &link,
                &btc,
            )
            .is_err());

        link.factor_geom = BoundedScore::new_clamped(0.25);
        link.factor_ofi = BoundedScore::new_clamped(0.02);
        assert_eq!(
            gov.confirm_preflight_kinematics_non_choking(
                9,
                Side::Bid,
                6_628_080_000,
                &link,
                &btc,
            ),
            Err("VETO_PREFLIGHT_OFI_DECAY")
        );

        let mut eng = SlotExecutionEngine::new(5);
        let mut ledger = ParitySyncLedger::new(6_628_080_000);
        let mut sui = SymbolLobSnapshot::new_empty();
        sui.symbol_id = APPROVED_SPECS[5].symbol_id;
        sui.realized_vol_5m = ScaledBps(6_800);
        sui.ts_local_ns = TimestampNs(1_000_000_000);
        sui.bids[0] = BookLevel { price: PriceTicks(8_265), qty: LotQty(500) };
        sui.asks[0] = BookLevel { price: PriceTicks(8_266), qty: LotQty(400) };
        sui.mark_price = PriceTicks(8_265);
        sui.last_trade_price = PriceTicks(8_265);

        let v_sui = crate::alpha_3factor::SignalVerdict::AuthorizeMakerEntry {
            side: Side::Bid,
            target_excursion_scaled_bps: ScaledBps(650),
        };
        assert!(eng.try_arm_maker_entry(&sui, v_sui, LotQty(1)).is_ok());
        assert!(eng.on_maker_entry_filled(TimestampNs(1_025_000_000)).is_ok());
        assert_eq!(eng.last_closed_peak_ticks, PriceTicks(8_265));
        assert_eq!(eng.last_closed_worst_ticks, PriceTicks(8_265));

        eng.last_closed_mfe_bps = 720;
        sui.ts_local_ns = TimestampNs(20_000_000_000);
        sui.bids[0] = BookLevel { price: PriceTicks(8_270), qty: LotQty(500) };
        sui.asks[0] = BookLevel { price: PriceTicks(8_271), qty: LotQty(500) };
        sui.factor_ofi = BoundedScore::new_clamped(-0.15);
        sui.factor_tfi = BoundedScore::new_clamped(-0.12);
        let q_ratchet = eng.compute_canonical_exit_quote(&sui);
        assert!(q_ratchet.is_some());
        if let Some(qr) = q_ratchet {
            assert!(!qr.defensive_lock_active);
            assert!(qr.urgent_close_repeg);
        }

        let summary = eng.finalize_close_with_lots(
            ClosedTradeKind::MakerPassiveUnwind,
            Side::Bid,
            PriceTicks(8_265),
            PriceTicks(8_270),
            LotQty(24),
            1,
            None,
            TimestampNs(25_000_000_000),
            &mut ledger,
        );
        assert_eq!(summary.exit_price, PriceTicks(8_270));
        assert_eq!(eng.last_closed_peak_ticks, PriceTicks(8_270));

        let qf_sui =
            defensive_zero_loss_maker_exit_price(Side::Bid, PriceTicks(8_265));
        assert_eq!(qf_sui, PriceTicks(8_269));
        let mut eth22 = SlotExecutionEngine::new(1);
        let mut eth_s = SymbolLobSnapshot::new_empty();
        eth_s.symbol_id = APPROVED_SPECS[1].symbol_id;
        eth_s.realized_vol_5m = ScaledBps(1_992);
        eth_s.ts_local_ns = TimestampNs(1_000_000_000);
        eth_s.bids[0] = BookLevel { price: PriceTicks(205_217), qty: LotQty(500) };
        eth_s.asks[0] = BookLevel { price: PriceTicks(205_218), qty: LotQty(500) };
        eth_s.mark_price = PriceTicks(205_218);
        eth_s.last_trade_price = PriceTicks(205_218);
        let v_eth = crate::alpha_3factor::SignalVerdict::AuthorizeMakerEntry {
            side: Side::Ask,
            target_excursion_scaled_bps: ScaledBps(900),
        };
        assert!(eth22.try_arm_maker_entry(&eth_s, v_eth, LotQty(1)).is_ok());
        assert!(eth22.on_maker_entry_filled(TimestampNs(1_500_000_000)).is_ok());
        eth_s.ts_local_ns = TimestampNs(12_000_000_000);
        eth_s.bids[0] = BookLevel { price: PriceTicks(205_084), qty: LotQty(400) };
        eth_s.asks[0] = BookLevel { price: PriceTicks(205_085), qty: LotQty(400) };
        eth_s.mark_price = PriceTicks(205_084);
        eth_s.last_trade_price = PriceTicks(205_084);
        let eth_win = eth22.step_open_position(&eth_s, &btc, &gov, &mut ledger);
        assert!(eth_win.is_some());
        if let Some(ew) = eth_win {
            assert!(ew.net_scaled_bps.raw() >= 100);
        }

        let mut tao_bad = link;
        tao_bad.symbol_id = APPROVED_SPECS[4].symbol_id;
        tao_bad.bids[0] = BookLevel { price: PriceTicks(21_609), qty: LotQty(47) };
        tao_bad.asks[0] = BookLevel { price: PriceTicks(21_610), qty: LotQty(190) };
        tao_bad.factor_ofi = BoundedScore::new_clamped(0.68);
        tao_bad.factor_tfi = BoundedScore::new_clamped(0.93);
        tao_bad.factor_geom = BoundedScore::new_clamped(-0.02);
        assert!(gov
            .confirm_preflight_kinematics_non_choking(
                4,
                Side::Bid,
                6_628_080_000,
                &tao_bad,
                &btc,
            )
            .is_err());
        assert_eq!(
            SlotExecutionEngine::classify_maker_exit_sub_reason(
                Side::Bid,
                PriceTicks(8_265),
                qf_sui,
                false,
                1,
                1,
            ),
            ExitSubReason::AdverseScratchEscape
        );
        let mut eth22 = SlotExecutionEngine::new(1);
        let mut eth_s = SymbolLobSnapshot::new_empty();
        eth_s.symbol_id = APPROVED_SPECS[1].symbol_id;
        eth_s.realized_vol_5m = ScaledBps(1_992);
        eth_s.ts_local_ns = TimestampNs(1_000_000_000);
        eth_s.bids[0] = BookLevel { price: PriceTicks(205_217), qty: LotQty(500) };
        eth_s.asks[0] = BookLevel { price: PriceTicks(205_218), qty: LotQty(500) };
        eth_s.mark_price = PriceTicks(205_218);
        eth_s.last_trade_price = PriceTicks(205_218);
        let v_eth = crate::alpha_3factor::SignalVerdict::AuthorizeMakerEntry {
            side: Side::Ask,
            target_excursion_scaled_bps: ScaledBps(900),
        };
        assert!(eth22.try_arm_maker_entry(&eth_s, v_eth, LotQty(1)).is_ok());
        assert!(eth22.on_maker_entry_filled(TimestampNs(1_500_000_000)).is_ok());
        eth_s.ts_local_ns = TimestampNs(12_000_000_000);
        eth_s.bids[0] = BookLevel { price: PriceTicks(205_084), qty: LotQty(400) };
        eth_s.asks[0] = BookLevel { price: PriceTicks(205_085), qty: LotQty(400) };
        eth_s.mark_price = PriceTicks(205_084);
        eth_s.last_trade_price = PriceTicks(205_084);
        let eth_win = eth22.step_open_position(&eth_s, &btc, &gov, &mut ledger);
        assert!(eth_win.is_some());
        if let Some(ew) = eth_win {
            assert!(ew.net_scaled_bps.raw() >= 100);
        }

        let mut tao_bad = link;
        tao_bad.symbol_id = APPROVED_SPECS[4].symbol_id;
        tao_bad.bids[0] = BookLevel { price: PriceTicks(21_609), qty: LotQty(47) };
        tao_bad.asks[0] = BookLevel { price: PriceTicks(21_610), qty: LotQty(190) };
        tao_bad.factor_ofi = BoundedScore::new_clamped(0.68);
        tao_bad.factor_tfi = BoundedScore::new_clamped(0.93);
        tao_bad.factor_geom = BoundedScore::new_clamped(-0.02);
        assert!(gov
            .confirm_preflight_kinematics_non_choking(
                4,
                Side::Bid,
                6_628_080_000,
                &tao_bad,
                &btc,
            )
            .is_err());
        assert_eq!(
            SlotExecutionEngine::classify_maker_exit_sub_reason(
                Side::Bid,
                PriceTicks(5_074),
                PriceTicks(5_077),
                false,
                1,
                2,
            ),
            ExitSubReason::AdverseScratchEscape
        );
        let mut eth22 = SlotExecutionEngine::new(1);
        let mut eth_s = SymbolLobSnapshot::new_empty();
        eth_s.symbol_id = APPROVED_SPECS[1].symbol_id;
        eth_s.realized_vol_5m = ScaledBps(1_992);
        eth_s.ts_local_ns = TimestampNs(1_000_000_000);
        eth_s.bids[0] = BookLevel { price: PriceTicks(205_217), qty: LotQty(500) };
        eth_s.asks[0] = BookLevel { price: PriceTicks(205_218), qty: LotQty(500) };
        eth_s.mark_price = PriceTicks(205_218);
        eth_s.last_trade_price = PriceTicks(205_218);
        let v_eth = crate::alpha_3factor::SignalVerdict::AuthorizeMakerEntry {
            side: Side::Ask,
            target_excursion_scaled_bps: ScaledBps(900),
        };
        assert!(eth22.try_arm_maker_entry(&eth_s, v_eth, LotQty(1)).is_ok());
        assert!(eth22.on_maker_entry_filled(TimestampNs(1_500_000_000)).is_ok());
        eth_s.ts_local_ns = TimestampNs(12_000_000_000);
        eth_s.bids[0] = BookLevel { price: PriceTicks(205_084), qty: LotQty(400) };
        eth_s.asks[0] = BookLevel { price: PriceTicks(205_085), qty: LotQty(400) };
        eth_s.mark_price = PriceTicks(205_084);
        eth_s.last_trade_price = PriceTicks(205_084);
        let eth_win = eth22.step_open_position(&eth_s, &btc, &gov, &mut ledger);
        assert!(eth_win.is_some());
        if let Some(ew) = eth_win {
            assert!(ew.net_scaled_bps.raw() >= 100);
        }

        let mut tao_bad = link;
        tao_bad.symbol_id = APPROVED_SPECS[4].symbol_id;
        tao_bad.bids[0] = BookLevel { price: PriceTicks(21_609), qty: LotQty(47) };
        tao_bad.asks[0] = BookLevel { price: PriceTicks(21_610), qty: LotQty(190) };
        tao_bad.factor_ofi = BoundedScore::new_clamped(0.68);
        tao_bad.factor_tfi = BoundedScore::new_clamped(0.93);
        tao_bad.factor_geom = BoundedScore::new_clamped(-0.02);
        assert!(gov
            .confirm_preflight_kinematics_non_choking(
                4,
                Side::Bid,
                6_628_080_000,
                &tao_bad,
                &btc,
            )
            .is_err());
        assert_eq!(
            SlotExecutionEngine::classify_maker_exit_sub_reason(
                Side::Bid,
                PriceTicks(8_265),
                PriceTicks(8_270),
                false,
                1,
                2,
            ),
            ExitSubReason::LvdbsDeepVacuumSurf
        );
    }

    #[test]
    fn test_scar64_taousdt_bid_loss23_all_4_shields_and_win_preservation() {
        let obs_t23 = crate::wave_elliott::WaveObservables {
            regime: crate::wave_elliott::WaveRegimeKind::NeutralEquilibrium,
            hurst_score: BoundedScore::new_clamped(-0.60),
            oi_impulse: BoundedScore::new_clamped(0.14),
            macro_phase: BoundedScore::new_clamped(-0.19),
            retracement_ratio: BoundedScore::new_clamped(0.42),
            climax_intensity: BoundedScore::ZERO,
        };
        assert_eq!(
            crate::wave_elliott::CausalElliottWaveState::evaluate_wave_entry_modulation(
                obs_t23, Side::Bid, -0.07,
            ),
            Err("VETO_COUNTER_TREND_SUBDIFFUSIVE_CHOP")
        );
        assert!(
            crate::wave_elliott::CausalElliottWaveState::evaluate_wave_entry_modulation(
                obs_t23, Side::Bid, 0.80,
            )
            .is_ok()
        );

        let t0 = 1_794_456_163_000_000_000_i64;
        let mut tao = SymbolLobSnapshot::new_empty();
        tao.symbol_id = APPROVED_SPECS[4].symbol_id;
        tao.is_eligible = true;
        tao.realized_vol_5m = ScaledBps(1_868);
        tao.ts_local_ns = TimestampNs(t0 + 14_000_000_000);
        tao.bids[0] = BookLevel { price: PriceTicks(21_548), qty: LotQty(268) };
        tao.asks[0] = BookLevel { price: PriceTicks(21_549), qty: LotQty(90) };
        tao.mark_price = PriceTicks(21_548);
        tao.factor_ofi = BoundedScore::new_clamped(0.62);
        tao.factor_tfi = BoundedScore::new_clamped(0.73);
        tao.factor_geom = BoundedScore::new_clamped(0.81);

        let mut gov_swp = OnlineAdaptiveGovernor::new();
        gov_swp.observe_entry_abort(4, Side::Bid, PriceTicks(21_550), TimestampNs(t0));
        assert_eq!(
            gov_swp.audit_scar64_entry_and_sweep_shield(4, Side::Bid, &tao, 0.25),
            Err("VETO_POST_ABORT_ADVERSE_SWEEP")
        );
        let mut tao_step_up = tao;
        tao_step_up.bids[0] = BookLevel { price: PriceTicks(21_552), qty: LotQty(300) };
        tao_step_up.asks[0] = BookLevel { price: PriceTicks(21_553), qty: LotQty(100) };
        assert!(gov_swp
            .audit_scar64_entry_and_sweep_shield(4, Side::Bid, &tao_step_up, 0.25)
            .is_ok());

        let mut gov_debt = OnlineAdaptiveGovernor::new();
        gov_debt.observe_closed_trade_at_entry(
            9,
            Side::Bid,
            PriceTicks(10_468),
            ScaledBps(-11_735),
            false,
            TimestampNs(t0 - 600_000_000_000),
        );
        let mut tao_macro = tao;
        tao_macro.set_wave_observables(obs_t23);
        assert_eq!(
            gov_debt.audit_scar64_entry_and_sweep_shield(4, Side::Bid, &tao_macro, -0.07),
            Err("VETO_SCAR64_BTC_DEBT_HEADWIND")
        );
        assert!(gov_debt
            .audit_scar64_entry_and_sweep_shield(0, Side::Bid, &tao_macro, 0.80)
            .is_ok());

        let mut gov_kin = OnlineAdaptiveGovernor::new();
        let mut s_warm = tao;
        s_warm.ts_local_ns = TimestampNs(t0);
        s_warm.bids[0] = BookLevel { price: PriceTicks(21_500), qty: LotQty(300) };
        s_warm.asks[0] = BookLevel { price: PriceTicks(21_501), qty: LotQty(100) };
        gov_kin.observe_market_price(4, &s_warm);
        for k in 1..=10_i64 {
            s_warm.ts_local_ns = TimestampNs(t0 + k * 5_000_000_000);
            s_warm.bids[0] = BookLevel { price: PriceTicks(21_530), qty: LotQty(300) };
            s_warm.asks[0] = BookLevel { price: PriceTicks(21_531), qty: LotQty(100) };
            gov_kin.observe_market_price(4, &s_warm);
        }
        s_warm.ts_local_ns = TimestampNs(t0 + 51_000_000_000);
        s_warm.bids[0] = BookLevel { price: PriceTicks(21_518), qty: LotQty(268) };
        s_warm.asks[0] = BookLevel { price: PriceTicks(21_519), qty: LotQty(90) };
        assert_eq!(
            gov_kin.audit_scar64_entry_and_sweep_shield(4, Side::Bid, &s_warm, 0.20),
            Err("VETO_FOLLOWER_DECEL_UNDER_COMPRESSION")
        );
    }

    #[test]
    fn test_offensive_win_floor_515bps_1050_split_and_trade25_tao_win_conversion() {
        assert_eq!(
            SlotExecutionEngine::compute_tranche1_split_lots(
                5, LotQty(14), PriceTicks(8_000), 380,
            ),
            Some(LotQty(7))
        );
        assert_eq!(
            SlotExecutionEngine::compute_tranche1_split_lots(
                4, LotQty(10), PriceTicks(21_120), 500,
            ),
            Some(LotQty(5))
        );

        let gov = OnlineAdaptiveGovernor::new();
        let mut ledger = ParitySyncLedger::new(6_612_720_000);
        let mut tao_eng = SlotExecutionEngine::new(4);
        let mut tao = SymbolLobSnapshot::new_empty();
        tao.symbol_id = APPROVED_SPECS[4].symbol_id;
        tao.is_eligible = true;
        tao.realized_vol_5m = ScaledBps(1_994);
        tao.ts_local_ns = TimestampNs(1_000_000_000);
        tao.bids[0] = BookLevel { price: PriceTicks(21_130), qty: LotQty(200) };
        tao.asks[0] = BookLevel { price: PriceTicks(21_131), qty: LotQty(218) };
        tao.mark_price = PriceTicks(21_131);
        tao.last_trade_price = PriceTicks(21_131);
        tao.factor_ofi = BoundedScore::new_clamped(-0.50);
        tao.factor_tfi = BoundedScore::new_clamped(-0.83);
        tao.factor_geom = BoundedScore::new_clamped(-0.32);
        tao.set_wave_observables(crate::wave_elliott::WaveObservables {
            regime: crate::wave_elliott::WaveRegimeKind::MotiveImpulseBear,
            hurst_score: BoundedScore::new_clamped(0.268),
            oi_impulse: BoundedScore::new_clamped(-0.42),
            macro_phase: BoundedScore::new_clamped(-0.78),
            retracement_ratio: BoundedScore::new_clamped(0.12),
            climax_intensity: BoundedScore::ZERO,
        });

        let verdict = crate::alpha_3factor::SignalVerdict::AuthorizeMakerEntry {
            side: Side::Ask,
            target_excursion_scaled_bps: ScaledBps(750),
        };
        assert!(tao_eng.try_arm_maker_entry(&tao, verdict, LotQty(10)).is_ok());
        assert!(tao_eng.on_maker_entry_filled(TimestampNs(2_000_000_000)).is_ok());

        tao.ts_local_ns = TimestampNs(30_000_000_000);
        tao.bids[0] = BookLevel { price: PriceTicks(21_142), qty: LotQty(180) };
        tao.asks[0] = BookLevel { price: PriceTicks(21_143), qty: LotQty(180) };
        tao.mark_price = PriceTicks(21_143);
        tao.last_trade_price = PriceTicks(21_143);
        assert!(tao_eng.step_open_position(&tao, &tao, &gov, &mut ledger).is_none());

        tao.ts_local_ns = TimestampNs(60_000_000_000);
        tao.factor_ofi = BoundedScore::new_clamped(-0.72);
        tao.factor_tfi = BoundedScore::new_clamped(-0.99);
        tao.bids[0] = BookLevel { price: PriceTicks(21_119), qty: LotQty(150) };
        tao.asks[0] = BookLevel { price: PriceTicks(21_120), qty: LotQty(220) };
        tao.mark_price = PriceTicks(21_119);
        tao.last_trade_price = PriceTicks(21_119);
        let q_58s = tao_eng.compute_canonical_exit_quote(&tao);
        assert!(q_58s.is_some());
        if let Some(q) = q_58s {
            assert!(!q.defensive_lock_active);
            assert_eq!(q.order_lots, LotQty(5));
        }
        assert!(tao_eng.step_open_position(&tao, &tao, &gov, &mut ledger).is_none());
        assert_eq!(tao_eng.tranche1_harvests_count, 1);

        tao.ts_local_ns = TimestampNs(61_000_000_000);
        tao.bids[0] = BookLevel { price: PriceTicks(21_116), qty: LotQty(120) };
        tao.asks[0] = BookLevel { price: PriceTicks(21_117), qty: LotQty(180) };
        tao.mark_price = PriceTicks(21_116);
        tao.last_trade_price = PriceTicks(21_116);
        assert!(tao_eng.step_open_position(&tao, &tao, &gov, &mut ledger).is_none());

        tao.ts_local_ns = TimestampNs(260_000_000_000);
        tao.factor_ofi = BoundedScore::ZERO;
        tao.factor_tfi = BoundedScore::ZERO;
        tao.bids[0] = BookLevel { price: PriceTicks(21_120), qty: LotQty(160) };
        tao.asks[0] = BookLevel { price: PriceTicks(21_121), qty: LotQty(160) };
        tao.mark_price = PriceTicks(21_120);
        tao.last_trade_price = PriceTicks(21_120);
        let closed_t25 = tao_eng.step_open_position(&tao, &tao, &gov, &mut ledger);
        assert!(closed_t25.is_some());
        if let Some(s25) = closed_t25 {
            assert!(s25.net_scaled_bps.raw() >= 120);
        }
    }

    #[test]
    fn test_scar65_taousdt_ask_loss33_coupled_chop_l1_dust_and_win_preservation() {
        let obs_t33 = crate::wave_elliott::WaveObservables {
            regime: crate::wave_elliott::WaveRegimeKind::NeutralEquilibrium,
            hurst_score: BoundedScore::new_clamped(-0.152),
            oi_impulse: BoundedScore::new_clamped(-0.19),
            macro_phase: BoundedScore::new_clamped(0.36),
            retracement_ratio: BoundedScore::new_clamped(0.45),
            climax_intensity: BoundedScore::ZERO,
        };
        assert_eq!(
            crate::wave_elliott::CausalElliottWaveState::evaluate_wave_entry_modulation(
                obs_t33, Side::Ask, 0.26,
            ),
            Err("VETO_COUNTER_TREND_SUBDIFFUSIVE_CHOP")
        );
        assert!(
            crate::wave_elliott::CausalElliottWaveState::evaluate_wave_entry_modulation(
                obs_t33, Side::Ask, 0.81,
            )
            .is_ok()
        );

        let tao_spec = APPROVED_SPECS[4];
        assert!(!tao_spec.passes_l1_queue_depth_and_wall_guard(
            PriceTicks(20_916),
            ScaledBps(2_530),
            75,
            LotQty(6),
            LotQty(120),
            LotQty(400),
            0.24,
            0.98,
        ));
        assert!(tao_spec.passes_l1_queue_depth_and_wall_guard(
            PriceTicks(20_916),
            ScaledBps(2_530),
            75,
            LotQty(218),
            LotQty(120),
            LotQty(600),
            0.50,
            0.83,
        ));

        let mut gov = OnlineAdaptiveGovernor::new();
        gov.observe_closed_trade_at_entry(
            4,
            Side::Bid,
            PriceTicks(21_548),
            ScaledBps(-5_852),
            false,
            TimestampNs(1_794_457_992_000_000_000),
        );
        let mut tao = SymbolLobSnapshot::new_empty();
        tao.symbol_id = tao_spec.symbol_id;
        tao.is_eligible = true;
        tao.realized_vol_5m = ScaledBps(2_530);
        tao.ts_local_ns = TimestampNs(1_794_513_852_000_000_000);
        tao.bids[0] = BookLevel { price: PriceTicks(20_915), qty: LotQty(150) };
        tao.asks[0] = BookLevel { price: PriceTicks(20_916), qty: LotQty(84) };
        tao.factor_ofi = BoundedScore::new_clamped(-0.24);
        tao.factor_tfi = BoundedScore::new_clamped(-0.98);
        tao.factor_geom = BoundedScore::new_clamped(-0.66);
        assert_eq!(
            gov.audit_scar64_entry_and_sweep_shield(4, Side::Ask, &tao, 0.26),
            Err("VETO_SYMBOL_CROSS_SCAR_WEAK_IMPULSE")
        );
    }

    #[test]
    fn test_joint_adverse_anti_flicker_preserves_win_floor_and_confirms_real_stress() {
        let mut sui_eng = SlotExecutionEngine::new(5);
        let mut sui = SymbolLobSnapshot::new_empty();
        sui.symbol_id = APPROVED_SPECS[5].symbol_id;
        sui.is_eligible = true;
        sui.realized_vol_5m = ScaledBps(4_717);
        sui.ts_local_ns = TimestampNs(1_000_000_000);
        sui.bids[0] = BookLevel { price: PriceTicks(8_021), qty: LotQty(6_470) };
        sui.asks[0] = BookLevel { price: PriceTicks(8_022), qty: LotQty(5_200) };
        sui.mark_price = PriceTicks(8_022);
        sui.last_trade_price = PriceTicks(8_022);
        sui.factor_ofi = BoundedScore::new_clamped(-0.55);
        sui.factor_tfi = BoundedScore::new_clamped(-0.92);
        sui.factor_geom = BoundedScore::new_clamped(-0.16);

        let verdict = crate::alpha_3factor::SignalVerdict::AuthorizeMakerEntry {
            side: Side::Ask,
            target_excursion_scaled_bps: ScaledBps(700),
        };
        assert!(sui_eng.try_arm_maker_entry(&sui, verdict, LotQty(20)).is_ok());
        assert!(sui_eng.on_maker_entry_filled(TimestampNs(2_000_000_000)).is_ok());

        sui.ts_local_ns = TimestampNs(22_000_000_000);
        sui.bids[0] = BookLevel { price: PriceTicks(8_024), qty: LotQty(3_200) };
        sui.asks[0] = BookLevel { price: PriceTicks(8_025), qty: LotQty(3_200) };
        sui.mark_price = PriceTicks(8_025);
        sui.factor_ofi = BoundedScore::new_clamped(0.28);
        sui.factor_tfi = BoundedScore::new_clamped(0.26);
        let q_flicker = sui_eng.compute_canonical_exit_quote(&sui);
        assert!(q_flicker.is_some());
        if let Some(qf) = q_flicker {
            assert!(!qf.defensive_lock_active);
            assert_eq!(qf.role, ExitOrderRole::Tranche1Harvest50);
            assert_eq!(qf.target_price, PriceTicks(8_017));
        }

        sui.ts_local_ns = TimestampNs(52_000_000_000);
        sui.bids[0] = BookLevel { price: PriceTicks(8_038), qty: LotQty(3_200) };
        sui.asks[0] = BookLevel { price: PriceTicks(8_039), qty: LotQty(3_200) };
        sui.mark_price = PriceTicks(8_038);
        sui.factor_ofi = BoundedScore::new_clamped(0.55);
        sui.factor_tfi = BoundedScore::new_clamped(0.65);
        let q_stress = sui_eng.compute_canonical_exit_quote(&sui);
        assert!(q_stress.is_some());
        if let Some(qs) = q_stress {
            assert!(qs.defensive_lock_active);
            assert_eq!(qs.target_price, PriceTicks(8_018));
        }
    }
}
