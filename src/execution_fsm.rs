//! Execution FSM Primitives, Quantum Floors, Moallemi dQ/dt Shield & 12x2 Adaptive Governor.

use crate::alpha_3factor::OrthogonalFactorState;
use crate::lob_shm::{SymbolLobSnapshot, MAX_ACTIVE_PAIRS};
use crate::types::{
    LotQty, PairSpec, PriceTicks, ScaledBps, Side, TimestampNs, VortexError, APPROVED_SPECS,
};

pub const MAX_CLOSE_RECONCILE_RETRIES: u8 = 3;

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
    SinkingFundSiphonExit,
    WaldBaseStopLoss,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct ClosedTradeSummary {
    pub kind: ClosedTradeKind,
    pub side: Side,
    pub entry_price: PriceTicks,
    pub exit_price: PriceTicks,
    pub gross_scaled_bps: ScaledBps,
    pub fee_scaled_bps: ScaledBps,
    pub net_scaled_bps: ScaledBps,
    pub net_pnl_1e8: i64,
    pub is_maker_exit: bool,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct CanonicalExitQuote {
    pub role: ExitOrderRole,
    pub sub_reason: ExitSubReason,
    pub target_price: PriceTicks,
    pub order_lots: LotQty,
    pub vac_level: usize,
    pub cur_gross_bps: i64,
    pub wald_pct: i64,
    pub lock_l1_now: bool,
    pub siphon_subsidy_bps: i64,
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
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5,
    0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
    0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc,
    0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
    0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
    0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3,
    0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5,
    0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
    0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

#[allow(clippy::needless_range_loop)]
fn sha256_digest(data: &[u8]) -> [u8; 32] {
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
        0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
    ];
    let bit_len = (data.len() as u64).wrapping_mul(8);
    let mut msg = Vec::with_capacity(data.len() + 72);
    msg.extend_from_slice(data);
    msg.push(0x80);
    while (msg.len() % 64) != 56 {
        msg.push(0x00);
    }
    msg.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in msg.as_chunks::<64>().0 {
        let mut w = [0_u32; 64];
        for i in 0..16 {
            let j = i * 4;
            w[i] = u32::from_be_bytes([chunk[j], chunk[j + 1], chunk[j + 2], chunk[j + 3]]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16].wrapping_add(s0).wrapping_add(w[i - 7]).wrapping_add(s1);
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

    let mut out = [0_u8; 32];
    for (i, word) in h.iter().enumerate() {
        let bytes = word.to_be_bytes();
        out[i * 4..i * 4 + 4].copy_from_slice(&bytes);
    }
    out
}

#[must_use]
pub fn sign_bybit_v5_hmac_sha256(secret: &str, payload: &str) -> String {
    let mut key_block = [0_u8; 64];
    let sec_bytes = secret.as_bytes();
    if sec_bytes.len() > 64 {
        let hashed = sha256_digest(sec_bytes);
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
    let mut inner_data = Vec::with_capacity(64 + payload.len());
    inner_data.extend_from_slice(&ipad);
    inner_data.extend_from_slice(payload.as_bytes());
    let inner_hash = sha256_digest(&inner_data);

    let mut outer_data = Vec::with_capacity(96);
    outer_data.extend_from_slice(&opad);
    outer_data.extend_from_slice(&inner_hash);
    let digest = sha256_digest(&outer_data);

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

/// F-03: Parses Bybit V5 avgExitPrice conservatively (floor for Long Bid, ceil for Short Ask).
#[inline]
pub fn parse_verified_exit_price_ticks(
    spec: PairSpec,
    open_side: Side,
    decimal_str: &str,
) -> Result<PriceTicks, VortexError> {
    parse_verified_avg_price_ticks(spec, open_side.opposite(), decimal_str)
}

/// F-03: Parses signed decimal strings (e.g. Bybit closedPnl) into exact integer 1e8 units.
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

/// Tick-quantized Zero-Loss Quantum Floor (4.00 bps Maker-Maker fee + >= 1 structural tick).
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

/// Moallemi-Yuan L1 Queue Depletion Velocity Shield (dQ/dt).
#[inline]
#[must_use]
pub fn check_queue_depletion_velocity_abort(
    side: Side,
    init_queue_ahead: LotQty,
    current_queue_ahead: LotQty,
    snap: &SymbolLobSnapshot,
) -> bool {
    let dir_s = if matches!(side, Side::Bid) { 1.0 } else { -1.0 };
    let signed_ofi = snap.factor_ofi.get() * dir_s;
    let signed_tfi = snap.factor_tfi.get() * dir_s;
    let flow_impulse = 0.55 * signed_ofi + 0.45 * signed_tfi;
    if flow_impulse <= -0.48 {
        return true;
    }
    if init_queue_ahead.0 <= 0 {
        return false;
    }
    let vol_ratio = (snap.realized_vol_5m.raw().clamp(4_000, 12_000) as f64) / 6_000.0;
    let nu_sigma = vol_ratio.sqrt().clamp(0.80, 1.35);
    let depleted_frac =
        ((init_queue_ahead.0 - current_queue_ahead.0).max(0) as f64) / (init_queue_ahead.0 as f64);
    depleted_frac >= (0.55 * nu_sigma) && flow_impulse <= (-0.05 * nu_sigma)
}

/// 12x2 Native Online Adaptive Governor with 6 Shields & E_kin Surface.
#[derive(Clone, Debug)]
pub struct OnlineAdaptiveGovernor {
    ewma_net_bps_x100: [[i64; 2]; MAX_ACTIVE_PAIRS],
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
    last_exit_px_1e4: [[i64; 2]; MAX_ACTIVE_PAIRS],
    wave_step_count: [[u32; 2]; MAX_ACTIVE_PAIRS],
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
        Self {
            ewma_net_bps_x100: [[200; 2]; MAX_ACTIVE_PAIRS],
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
            last_exit_px_1e4: [[0; 2]; MAX_ACTIVE_PAIRS],
            wave_step_count: [[0; 2]; MAX_ACTIVE_PAIRS],
            abort_streak: [[0; 2]; MAX_ACTIVE_PAIRS],
            abort_anchor_px_1e4: [[i64::MIN; 2]; MAX_ACTIVE_PAIRS],
            abort_last_ts_ns: [[0; 2]; MAX_ACTIVE_PAIRS],
        }
    }

    #[inline]
    pub const fn dir_idx(side: Side) -> usize {
        match side {
            Side::Bid => 0,
            Side::Ask => 1,
        }
    }

    #[inline]
    pub fn observe_market_price(&mut self, slot_idx: usize, snap: &SymbolLobSnapshot) {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return;
        }
        let mid_ticks = if snap.bids[0].price.0 > 0 && snap.asks[0].price.0 > 0 {
            (snap.bids[0].price.0 + snap.asks[0].price.0) / 2
        } else {
            snap.mark_price.0
        };
        if mid_ticks <= 0 {
            return;
        }
        let px_1e4 = mid_ticks.saturating_mul(10_000);
        let fast_prev = self.ewma_fast_px_1e4[slot_idx];
        let slow_prev = self.ewma_slow_px_1e4[slot_idx];
        if fast_prev <= 0 || slow_prev <= 0 {
            self.ewma_fast_px_1e4[slot_idx] = px_1e4;
            self.ewma_slow_px_1e4[slot_idx] = px_1e4;
            self.last_price_sample_ts_ns[slot_idx] = snap.ts_local_ns.0;
            return;
        }
        let sample_dt = snap.ts_local_ns.0.saturating_sub(self.last_price_sample_ts_ns[slot_idx]);
        if sample_dt < 1_000_000_000 {
            return;
        }
        self.last_price_sample_ts_ns[slot_idx] = snap.ts_local_ns.0;
        self.ewma_fast_px_1e4[slot_idx] = (fast_prev * 19 + px_1e4) / 20;
        self.ewma_slow_px_1e4[slot_idx] = (slow_prev * 299 + px_1e4) / 300;
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
            (snap.bids[0].price.0 + snap.asks[0].price.0) / 2
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

    #[inline]
    #[must_use]
    pub const fn is_in_scar_debt(&self, slot_idx: usize, side: Side) -> bool {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return false;
        }
        let d = Self::dir_idx(side);
        self.cum_net_bps_x100[slot_idx][d] < -1_500
            || self.sl_streak[slot_idx][d] > 0
            || self.global_dir_debt_bps_x100[d] < -6_000
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
        if check_ts_ns < self.directional_cooldown_until_ns[slot_idx][d] {
            return Some("VETO_SLOT_COOLDOWN");
        }
        if check_ts_ns < self.global_dir_cooldown_until_ns[d] {
            return Some("VETO_GLOBAL_WAVE_OR_SL_BREATHER");
        }
        let anchor_1e4 = self.wave_anchor_px_1e4[slot_idx][d];
        let cur_1e4 = entry_ticks.saturating_mul(10_000);
        if self.win_streak[slot_idx][d] >= 3 && anchor_1e4 > 0 && cur_1e4 > 0 {
            let chased_bps = match side {
                Side::Bid => ((cur_1e4 - anchor_1e4).saturating_mul(1_000_000)) / anchor_1e4,
                Side::Ask => ((anchor_1e4 - cur_1e4).saturating_mul(1_000_000)) / anchor_1e4,
            };
            if chased_bps > 2_500 && chased_bps < 15_000 {
                return Some("VETO_WAVE_STAIRCASE_25BPS");
            }
        }
        if self.abort_streak[slot_idx][d] >= 4
            && self.abort_anchor_px_1e4[slot_idx][d] > 0
            && check_ts_ns.saturating_sub(self.abort_last_ts_ns[slot_idx][d]) < 15_000_000_000
        {
            return Some("VETO_ABORT_STREAK_BREATHER");
        }
        if self.sl_streak[slot_idx][d] >= 1 && self.cum_net_bps_x100[slot_idx][d] <= -4_500 {
            return Some("VETO_DEEP_SCAR_DEBT_QUARANTINE");
        }
        if slot_idx > 0 {
            if self.global_dir_sl_streak[d] >= 1 && self.last_global_win_dir == ((1 - d) as u8) {
                return Some("VETO_POST_SL_OPPOSITE_DOMINANCE");
            }
            let symbol_total_net =
                self.cum_net_bps_x100[slot_idx][0] + self.cum_net_bps_x100[slot_idx][1];
            if symbol_total_net <= -2_500
                && self.global_dir_debt_bps_x100[d] <= -10_000
                && self.cum_net_bps_x100[slot_idx][d] <= 0
            {
                return Some("VETO_SYMBOL_AND_GLOBAL_DEBT_CONTAGION");
            }
        }
        None
    }

    /// Evaluates continuous Kinematic Free-Energy Surface (E_kin >= E_min).
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
        let min_geom_floor = if snr_abs >= 0.50 {
            (0.15 - (snr_abs - 0.50) * 0.55).clamp(0.05, 0.15)
        } else {
            0.15
        };
        if signed_geom < min_geom_floor {
            return Err("VETO_LOW_L2_GEOM_VACUUM (<min_geom)");
        }
        let d_sign = if matches!(side, Side::Bid) { 1_i64 } else { -1_i64 };
        let s_drift = drift_bps.saturating_mul(d_sign);
        let s_ext = ext_bps.saturating_mul(d_sign);
        let lead_impulse = s_ext.saturating_sub(s_drift);
        let elastic_lead_cap = 48 + ((snr_abs - 0.58).max(0.0) * 110.0).round() as i64;
        if s_drift < -15
            || s_ext < -15
            || s_ext > 1_200
            || (lead_impulse > 0 && lead_impulse > elastic_lead_cap && s_ext >= 115)
            || lead_impulse < -420
        {
            return Err("VETO_KINEMATIC_PHASE_OR_EXHAUSTION");
        }
        let d = Self::dir_idx(side);
        let learned_edge = if slot_idx < MAX_ACTIVE_PAIRS {
            self.ewma_net_bps_x100[slot_idx][d]
        } else {
            180
        };
        let impulse_contrib = if lead_impulse >= 0 {
            lead_impulse.min(75) + (s_drift.max(0) / 2).min(85)
        } else if snr_abs >= 0.50 && s_drift > 0 && s_ext >= -10 {
            ((s_drift / 4) + ((snr_abs - 0.50) * 220.0).round() as i64).min(115)
        } else {
            (lead_impulse / 2).max(-60)
        };
        let geom_contrib = ((signed_geom - min_geom_floor).max(0.0) * 240.0).round() as i64;
        let snr_contrib = ((snr_abs - 0.42).max(0.0) * 280.0).round() as i64;
        let fresh_init_bonus = if s_drift >= 0
            && s_drift <= 140
            && s_ext >= -10
            && s_ext <= 165
            && lead_impulse >= -20
            && lead_impulse <= elastic_lead_cap
            && snr_abs >= 0.45
        {
            95_i64
        } else {
            0_i64
        };
        let e_kin = impulse_contrib + geom_contrib + snr_contrib + fresh_init_bonus;
        let req_e_min = (125 + ((200 - learned_edge).max(0) / 4)).clamp(110, 260);
        if e_kin < req_e_min {
            return Err("VETO_KINEMATIC_ENERGY_SURFACE (E_kin < E_min)");
        }
        Ok((e_kin, req_e_min))
    }

    pub fn observe_closed_trade(
        &mut self,
        slot_idx: usize,
        side: Side,
        net_scaled_bps: ScaledBps,
        is_maker_exit: bool,
        ts_ns: TimestampNs,
    ) {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return;
        }
        let d = Self::dir_idx(side);
        let prev = self.ewma_net_bps_x100[slot_idx][d];
        let edge_clamped = prev.clamp(80, 450);
        self.ewma_net_bps_x100[slot_idx][d] = (prev * 75 + net_scaled_bps.raw() * 25) / 100;
        self.cum_net_bps_x100[slot_idx][d] = self.cum_net_bps_x100[slot_idx][d]
            .saturating_add(net_scaled_bps.raw())
            .clamp(-24_000, 24_000);
        self.global_dir_debt_bps_x100[d] = self.global_dir_debt_bps_x100[d]
            .saturating_add(net_scaled_bps.raw())
            .clamp(-30_000, 12_000);

        if !is_maker_exit || net_scaled_bps.raw() <= -800 {
            let s = self.sl_streak[slot_idx][d].saturating_add(1).min(4);
            self.sl_streak[slot_idx][d] = s;
            self.win_streak[slot_idx][d] = 0;
            self.wave_step_count[slot_idx][d] = 0;
            self.global_dir_win_streak[d] = 0;
            self.wave_anchor_px_1e4[slot_idx][d] = 0;
            self.last_exit_px_1e4[slot_idx][d] = 0;
            let backoff_ns = 150_000_000_000_i64.saturating_mul(1_i64 << s.min(4));
            self.directional_cooldown_until_ns[slot_idx][d] =
                ts_ns.0.saturating_add(backoff_ns);
            let gs = self.global_dir_sl_streak[d].saturating_add(1).min(4);
            self.global_dir_sl_streak[d] = gs;
            let global_pause_ns = 180_000_000_000_i64.saturating_mul(i64::from(gs));
            self.global_dir_cooldown_until_ns[d] = ts_ns.0.saturating_add(global_pause_ns);
        } else if net_scaled_bps.raw() < 100 {
            let scratch_secs = (115 - (edge_clamped / 20)).clamp(60, 130);
            let scratch_cooldown_ns = ts_ns.0.saturating_add(scratch_secs * 1_000_000_000);
            if scratch_cooldown_ns > self.directional_cooldown_until_ns[slot_idx][d] {
                self.directional_cooldown_until_ns[slot_idx][d] = scratch_cooldown_ns;
            }
        } else {
            let w = self.win_streak[slot_idx][d].saturating_add(1);
            self.win_streak[slot_idx][d] = w;
            self.win_streak[slot_idx][1 - d] = 0;
            self.wave_anchor_px_1e4[slot_idx][1 - d] = 0;
            self.wave_step_count[slot_idx][1 - d] = 0;
            let gw = self.global_dir_win_streak[d].saturating_add(1);
            self.global_dir_win_streak[d] = gw;
            self.global_dir_win_streak[1 - d] = 0;
            self.last_global_win_dir = d as u8;
            if gw >= 6 {
                self.global_dir_win_streak[d] = 0;
            }
            let breather_secs = (25 - edge_clamped / 40).clamp(12, 30) * (w.min(2) as i64);
            let win_breather_ns = ts_ns.0.saturating_add(breather_secs * 1_000_000_000);
            if win_breather_ns > self.directional_cooldown_until_ns[slot_idx][d] {
                self.directional_cooldown_until_ns[slot_idx][d] = win_breather_ns;
            }
            if self.sl_streak[slot_idx][d] > 0
                && w >= 5
                && self.cum_net_bps_x100[slot_idx][d] >= -1_500
            {
                self.sl_streak[slot_idx][d] -= 1;
                self.win_streak[slot_idx][d] = 0;
            }
            if self.global_dir_sl_streak[d] > 0 && self.global_dir_debt_bps_x100[d] >= -3_000 {
                self.global_dir_sl_streak[d] -= 1;
            }
        }
    }

    /// Records a pre-fill queue abort to prevent repeated toxic re-arming at the same level.
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
        self.abort_streak[slot_idx][d] = self.abort_streak[slot_idx][d].saturating_add(1).min(8);
        self.abort_anchor_px_1e4[slot_idx][d] = aborted_price.0.saturating_mul(10_000);
        self.abort_last_ts_ns[slot_idx][d] = ts_ns.0;
    }

    #[must_use]
    pub fn authorizes_adaptive_entry(
        &self,
        slot_idx: usize,
        side: Side,
        candidate_snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
    ) -> bool {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return false;
        }
        let has_bbo =
            candidate_snap.bids[0].price.0 > 0 && candidate_snap.asks[0].price.0 > 0;
        let entry_ticks = if has_bbo {
            (candidate_snap.bids[0].price.0 + candidate_snap.asks[0].price.0) / 2
        } else {
            candidate_snap.mark_price.0
        };
        if self
            .audit_historical_entry_veto(
                slot_idx,
                side,
                entry_ticks,
                candidate_snap.ts_local_ns.0,
            )
            .is_some()
        {
            return false;
        }
        let d = Self::dir_idx(side);
        let spec = APPROVED_SPECS[slot_idx];
        let (own_q, opp_q) = match side {
            Side::Bid => (candidate_snap.bids[0].qty.0, candidate_snap.asks[0].qty.0),
            Side::Ask => (candidate_snap.asks[0].qty.0, candidate_snap.bids[0].qty.0),
        };
        if !spec.passes_l1_queue_depth_and_wall_guard(
            PriceTicks(entry_ticks),
            candidate_snap.realized_vol_5m,
            self.ewma_net_bps_x100[slot_idx][d],
            own_q,
            opp_q,
            candidate_snap.factor_ofi.get().abs(),
            candidate_snap.factor_tfi.get().abs(),
        ) {
            return false;
        }
        let (self_drift_bps, self_ext_bps) =
            self.price_drift_and_extension_scaled_bps(slot_idx, candidate_snap);
        let dir_s = if matches!(side, Side::Bid) { 1.0 } else { -1.0 };
        let signed_geom = candidate_snap.factor_geom.get() * dir_s;
        let cand_comp = OrthogonalFactorState::snr_composite_score(candidate_snap).abs();
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
        let learned_edge = (self.ewma_net_bps_x100[slot_idx][d].clamp(0, 450) as f64) / 10_000.0;
        if self.is_in_scar_debt(slot_idx, side) && candidate_snap.factor_tfi.get().abs() < 0.40 {
            return false;
        }
        if slot_idx > 0 {
            let btc_comp = OrthogonalFactorState::snr_composite_score(btc_leader_snap) * dir_s;
            let btc_ofi = btc_leader_snap.factor_ofi.get() * dir_s;
            let btc_tfi = btc_leader_snap.factor_tfi.get() * dir_s;
            let strong_independent = cand_comp >= 0.50 && sl_s == 0.0 && debt_penalty == 0.0;
            if strong_independent {
                if btc_comp < -0.08 || (btc_ofi < -0.18 && btc_tfi < -0.18) {
                    return false;
                }
            } else {
                let min_req_btc = 0.10 + 0.10 * sl_s + 0.55 * (debt_penalty + global_penalty);
                if btc_ofi < 0.0 || btc_tfi < 0.0 || btc_comp < min_req_btc {
                    return false;
                }
            }
        }
        let adaptive_threshold = (0.42
            + 0.08 * sl_s
            + debt_penalty
            + global_penalty
            + staircase_penalty
            - learned_edge)
            .clamp(0.38, 0.80);
        cand_comp >= adaptive_threshold
    }

    /// Unified LOSS-LAB Verifier: Certifies 62/62 historical/V2/PAR1 losses + 2,160 manifold.
    #[must_use]
    pub fn audit_unified_62_losses_and_2160_manifold() -> (usize, usize) {
        let gov = Self::new();
        let cases: [(usize, Side, i64, i64, f64, f64); 62] = [
            (0, Side::Ask, -331, -347, 0.11, 0.55),
            (3, Side::Bid, 113, 120, 0.11, 0.52),
            (0, Side::Bid, 182, 248, 0.38, 0.58),
            (1, Side::Bid, 142, 267, 0.25, 0.54),
            (0, Side::Bid, 247, 157, 0.16, 0.44),
            (0, Side::Bid, 109, 36, 0.20, 0.45),
            (1, Side::Bid, 275, 209, 0.15, 0.43),
            (1, Side::Bid, 263, 155, 0.15, 0.43),
            (0, Side::Ask, 694, 302, 0.51, 0.79),
            (1, Side::Ask, -185, -240, 0.22, 0.50),
            (0, Side::Bid, 170, 235, 0.24, 0.51),
            (0, Side::Ask, -160, -220, 0.21, 0.49),
            (0, Side::Ask, -145, -210, 0.20, 0.48),
            (0, Side::Bid, 190, 255, 0.23, 0.52),
            (1, Side::Bid, 210, 275, 0.25, 0.53),
            (0, Side::Bid, 220, 145, 0.15, 0.42),
            (4, Side::Ask, -218, -148, 0.15, 0.42),
            (0, Side::Ask, -215, -140, 0.15, 0.42),
            (0, Side::Bid, 230, 150, 0.15, 0.42),
            (1, Side::Bid, 228, 152, 0.15, 0.42),
            (0, Side::Bid, 235, 165, 0.15, 0.42),
            (0, Side::Ask, -225, -155, 0.15, 0.42),
            (1, Side::Ask, -240, -168, 0.15, 0.42),
            (0, Side::Ask, -232, -160, 0.15, 0.42),
            (0, Side::Bid, 250, 162, 0.15, 0.42),
            (7, Side::Bid, -42, -68, -0.10, 0.39),
            (4, Side::Bid, 135, 195, 0.14, 0.49),
            (3, Side::Bid, 148, 212, 0.13, 0.50),
            (4, Side::Bid, 128, 184, 0.12, 0.47),
            (4, Side::Bid, 162, 229, 0.16, 0.51),
            (1, Side::Bid, 175, 244, 0.18, 0.52),
            (2, Side::Ask, -154, -218, 0.14, 0.48),
            (3, Side::Bid, 139, 204, 0.15, 0.49),
            (5, Side::Ask, -166, -231, 0.17, 0.50),
            (1, Side::Bid, 188, 262, 0.19, 0.53),
            (1, Side::Ask, -45, 18, 0.08, 0.41),
            (4, Side::Ask, -38, 22, 0.09, 0.42),
            (1, Side::Bid, 42, -19, 0.07, 0.41),
            (4, Side::Ask, -32, 25, 0.06, 0.40),
            (5, Side::Bid, 152, 219, 0.14, 0.48),
            (0, Side::Ask, -51, 14, 0.09, 0.43),
            (0, Side::Ask, -48, 16, 0.08, 0.42),
            (4, Side::Ask, -172, -245, 0.16, 0.51),
            (0, Side::Ask, -184, -258, 0.18, 0.52),
            (1, Side::Bid, 168, 239, 0.15, 0.50),
            (0, Side::Ask, -179, -251, 0.17, 0.51),
            (0, Side::Bid, 36, -24, 0.07, 0.41),
            (1, Side::Bid, -28, -55, 0.05, 0.43),
            (5, Side::Bid, -34, -62, 0.06, 0.44),
            (1, Side::Bid, -49, -84, 0.04, 0.45),
            (0, Side::Bid, 144, 208, 0.14, 0.48),
            (1, Side::Ask, -169, -238, 0.16, 0.50),
            (0, Side::Bid, 158, 226, 0.15, 0.49),
            (1, Side::Ask, -163, -232, 0.15, 0.49),
            (0, Side::Ask, -174, -246, 0.17, 0.51),
            (0, Side::Bid, 132, 194, 0.13, 0.47),
            (0, Side::Ask, -136, -198, 0.13, 0.47),
            (3, Side::Ask, -151, -216, 0.14, 0.48),
            (0, Side::Ask, -165, -234, 0.16, 0.50),
            (1, Side::Ask, -159, -225, 0.15, 0.49),
            (0, Side::Ask, -161, -228, 0.15, 0.49),
            (4, Side::Bid, 146, 211, 0.14, 0.48),
        ];
        let mut mitigated_62 = 0_usize;
        for &(s_idx, side, dr, ex, gm, snr) in &cases {
            if gov.evaluate_kinematic_energy_surface(s_idx, side, dr, ex, gm, snr).is_err() {
                mitigated_62 += 1;
            }
        }
        let mut manifold_blocked = 0_usize;
        for s_idx in 0..MAX_ACTIVE_PAIRS {
            for &side in &[Side::Bid, Side::Ask] {
                for step in 0..90_i64 {
                    let dr = if matches!(side, Side::Bid) { 120 + step } else { -(120 + step) };
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
        (mitigated_62, manifold_blocked)
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

/// Per-Slot Autonomous Lifecycle State Machine (Tranche & LVDBS Native).
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
        active_exit_role: ExitOrderRole,
        active_exit_price: PriceTicks,
        active_exit_lots: LotQty,
        tp_maker_price: PriceTicks,
        sl_trigger_price: PriceTicks,
        opened_ts: TimestampNs,
    },
}

/// Autonomous Per-Slot Execution Engine (1 Instance per Slot in [SlotExecutionEngine; 12]).
#[derive(Clone, Debug)]
pub struct SlotExecutionEngine {
    pub slot_idx: usize,
    pub state: OrderLifecycleState,
    pub notional_per_trade_1e8: i64,
    pub cooldown_until_ns: TimestampNs,
    pub maker_exits_count: u64,
    pub taker_sl_exits_count: u64,
    pub tranche1_harvests_count: u64,
    pub lvdbs_deep_surfs_count: u64,
    pub gross_profit_accum_1e8: i64,
    pub gross_loss_accum_1e8: i64,
    pub learned_mfe_bps: i64,
    pub last_eff_sl_ticks: PriceTicks,
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
    pub const fn new(slot_idx: usize, notional_per_trade_1e8: i64) -> Self {
        Self {
            slot_idx,
            state: OrderLifecycleState::Idle,
            notional_per_trade_1e8,
            cooldown_until_ns: TimestampNs(0),
            maker_exits_count: 0,
            taker_sl_exits_count: 0,
            tranche1_harvests_count: 0,
            lvdbs_deep_surfs_count: 0,
            gross_profit_accum_1e8: 0,
            gross_loss_accum_1e8: 0,
            learned_mfe_bps: 680,
            last_eff_sl_ticks: PriceTicks(0),
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
    pub const fn new_12_slot_array(notional_per_trade_1e8: i64) -> [Self; MAX_ACTIVE_PAIRS] {
        [
            Self::new(0, notional_per_trade_1e8),
            Self::new(1, notional_per_trade_1e8),
            Self::new(2, notional_per_trade_1e8),
            Self::new(3, notional_per_trade_1e8),
            Self::new(4, notional_per_trade_1e8),
            Self::new(5, notional_per_trade_1e8),
            Self::new(6, notional_per_trade_1e8),
            Self::new(7, notional_per_trade_1e8),
            Self::new(8, notional_per_trade_1e8),
            Self::new(9, notional_per_trade_1e8),
            Self::new(10, notional_per_trade_1e8),
            Self::new(11, notional_per_trade_1e8),
        ]
    }

    #[inline]
    #[must_use]
    pub fn elastic_preflight_window_ns(&self, realized_vol_5m: ScaledBps) -> i64 {
        let vol_c = realized_vol_5m.raw().clamp(4_000, 12_500);
        let mfe_c = self.learned_mfe_bps.clamp(560, 1_200);
        let raw_ns = ((25_000_000_i64 * 6_000) / vol_c * mfe_c) / 720;
        raw_ns.clamp(15_000_000, 35_000_000)
    }

    #[inline]
    pub fn observe_mfe_sample(&mut self, gross_mfe_scaled_bps: i64) {
        if gross_mfe_scaled_bps >= 500 {
            let clamped = gross_mfe_scaled_bps.clamp(560, 1_600);
            self.learned_mfe_bps = (self.learned_mfe_bps * 80 + clamped * 20) / 100;
        }
    }

    #[inline]
    fn compute_validated_sl_trigger(
        side: Side,
        entry_price: PriceTicks,
        snap: &SymbolLobSnapshot,
    ) -> Result<PriceTicks, VortexError> {
        let vol_c = snap.realized_vol_5m.raw().clamp(4_000, 12_500);
        let elastic_sl_bps = (16_500 + (vol_c * 35) / 100).clamp(18_000, 22_500);
        let sl_delta_ticks = ((entry_price.0 * elastic_sl_bps) / 1_000_000).max(4);
        let ref_mark = if snap.mark_price.0 > 0 { snap.mark_price } else { entry_price };
        let ref_last = if snap.last_trade_price.0 > 0 {
            snap.last_trade_price
        } else {
            entry_price
        };
        let raw_sl = match side {
            Side::Bid => {
                let bound = ref_mark.0.min(ref_last.0).min(entry_price.0);
                PriceTicks((bound - sl_delta_ticks).max(1))
            }
            Side::Ask => {
                let bound = ref_mark.0.max(ref_last.0).max(entry_price.0);
                PriceTicks(bound + sl_delta_ticks)
            }
        };
        PairSpec::validate_stop_loss_geometry(side, raw_sl, ref_last, ref_mark)
    }

    pub fn sync_to_shm_telemetry(&self, arena: &crate::lob_shm::VortexShmArena) {
        use core::sync::atomic::Ordering;
        let Ok(tel) = arena.telemetry(self.slot_idx) else {
            return;
        };
        match self.state {
            OrderLifecycleState::Idle => {
                tel.state_code.store(0, Ordering::Release);
                tel.open_lots.store(0, Ordering::Release);
            }
            OrderLifecycleState::PlacingEntryMaker {
                side,
                entry_price,
                qty,
                sl_trigger_price,
                submitted_ts,
                ..
            } => {
                tel.state_code.store(1, Ordering::Release);
                tel.side_code.store(side as u64, Ordering::Release);
                tel.entry_ticks.store(entry_price.0 as u64, Ordering::Release);
                tel.avg_price_ticks.store(entry_price.0 as u64, Ordering::Release);
                tel.sl_ticks.store(sl_trigger_price.0 as u64, Ordering::Release);
                tel.initial_lots.store(qty.0 as u64, Ordering::Release);
                tel.resting_since_ns.store(submitted_ts.0 as u64, Ordering::Release);
            }
            OrderLifecycleState::RestingEntryQueue {
                side,
                entry_price,
                qty,
                sl_trigger_price,
                resting_since_ts,
                ..
            } => {
                tel.state_code.store(2, Ordering::Release);
                tel.side_code.store(side as u64, Ordering::Release);
                tel.entry_ticks.store(entry_price.0 as u64, Ordering::Release);
                tel.avg_price_ticks.store(entry_price.0 as u64, Ordering::Release);
                tel.sl_ticks.store(sl_trigger_price.0 as u64, Ordering::Release);
                tel.initial_lots.store(qty.0 as u64, Ordering::Release);
                tel.resting_since_ns.store(resting_since_ts.0 as u64, Ordering::Release);
            }
            OrderLifecycleState::PositionOpenMakerExit {
                side,
                entry_price,
                avg_price_ticks,
                peak_favorable_price,
                initial_lots,
                open_lots,
                tranche1_harvested,
                tp_maker_price,
                sl_trigger_price,
                opened_ts,
                ..
            } => {
                let code = if tranche1_harvested {
                    4_u64
                } else if open_lots.0 < initial_lots.0 {
                    5_u64
                } else {
                    3_u64
                };
                tel.state_code.store(code, Ordering::Release);
                tel.side_code.store(side as u64, Ordering::Release);
                tel.entry_ticks.store(entry_price.0 as u64, Ordering::Release);
                tel.avg_price_ticks.store(avg_price_ticks.0 as u64, Ordering::Release);
                tel.peak_ticks.store(peak_favorable_price.0 as u64, Ordering::Release);
                let eff_sl = if self.last_eff_sl_ticks.0 > 0 {
                    self.last_eff_sl_ticks.0
                } else {
                    sl_trigger_price.0
                };
                tel.tp_ticks.store(tp_maker_price.0 as u64, Ordering::Release);
                tel.sl_ticks.store(eff_sl as u64, Ordering::Release);
                tel.open_lots.store(open_lots.0 as u64, Ordering::Release);
                tel.initial_lots.store(initial_lots.0 as u64, Ordering::Release);
                tel.peristaltic_cycles.store(0, Ordering::Release);
                tel.tranche1_harvested.store(u64::from(tranche1_harvested), Ordering::Release);
                tel.resting_since_ns.store(opened_ts.0 as u64, Ordering::Release);
            }
        }
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
            ..
        } = verdict
        else {
            return Ok(self.state);
        };
        let Some((best_bid, best_ask, _)) = snap.bbo_and_spread_ticks() else {
            return Ok(self.state);
        };
        let entry_price = match side {
            Side::Bid => best_bid.price,
            Side::Ask => best_ask.price,
        };
        let sl_trigger_price = Self::compute_validated_sl_trigger(side, entry_price, snap)?;

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

    /// F-02 Fix: Mutates PlacingEntryMaker in-place with post-preflight L1 price & SL.
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
        let cur_l1_px = match side {
            Side::Bid => best_bid.price,
            Side::Ask => best_ask.price,
        };
        if cur_l1_px.0 <= 0 || (cur_l1_px.0 - armed_px.0).abs() > max_drift_ticks {
            return Err(VortexError::InvalidPriceOrTickSpec);
        }
        let new_sl = Self::compute_validated_sl_trigger(side, cur_l1_px, snap)?;
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

    /// Validates Stop-Loss geometry at a new L1 price prior to re-pegging a resting order.
    pub fn compute_repeg_sl_trigger(
        &self,
        snap: &SymbolLobSnapshot,
        new_l1_price: PriceTicks,
    ) -> Result<PriceTicks, VortexError> {
        let OrderLifecycleState::RestingEntryQueue { side, .. } = self.state else {
            return Err(VortexError::InvalidPriceOrTickSpec);
        };
        if new_l1_price.0 <= 0 {
            return Err(VortexError::InvalidPriceOrTickSpec);
        }
        Self::compute_validated_sl_trigger(side, new_l1_price, snap)
    }

    /// Updates RestingEntryQueue in-place once a re-pegged PostOnly order succeeds.
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

    /// F-02 Fix: Promotes entry to PositionOpenMakerExit anchored 1:1 to Bybit avgPrice & size.
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
                _ => return Ok(self.state),
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
                Side::Ask => PriceTicks(eff_entry_px.0 + sl_dist),
            }
        } else {
            state_sl_px
        };

        let tp_bps = target_excursion.raw().max(520);
        let tp_maker_price = dynamic_maker_floor_price(side, eff_entry_px, ScaledBps(tp_bps));
        let q_floor = defensive_zero_loss_maker_exit_price(side, eff_entry_px);
        let can_split = self.slot_idx >= 1 && eff_lots.0 >= 2;
        let (init_role, init_exit_px, init_exit_lots) = if can_split {
            (ExitOrderRole::Tranche1Harvest50, q_floor, LotQty(eff_lots.0 / 2))
        } else {
            (ExitOrderRole::StandardMakerExit100, tp_maker_price, eff_lots)
        };

        self.last_opened_ts = fill_ts;
        self.last_eff_sl_ticks = eff_sl_px;
        self.tranche1_harvests_count = 0;
        self.lvdbs_deep_surfs_count = 0;
        self.last_closed_mfe_bps = 0;
        self.last_closed_mae_bps = 0;
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
            active_exit_role: init_role,
            active_exit_price: init_exit_px,
            active_exit_lots: init_exit_lots,
            tp_maker_price,
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

    /// Keeps open position entry_price and avg_price_ticks locked to Bybit avgPrice.
    #[inline]
    pub fn sync_open_avg_price_ticks(&mut self, verified_avg_price: PriceTicks) {
        if verified_avg_price.0 <= 0 {
            return;
        }
        if let OrderLifecycleState::PositionOpenMakerExit {
            side,
            ref mut entry_price,
            ref mut avg_price_ticks,
            ref mut peak_favorable_price,
            ref mut worst_adverse_price,
            ..
        } = self.state
        {
            if *avg_price_ticks != verified_avg_price {
                *entry_price = verified_avg_price;
                *avg_price_ticks = verified_avg_price;
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
    fn classify_maker_exit_sub_reason(
        side: Side,
        entry_px: PriceTicks,
        exit_px: PriceTicks,
        tranche1_harvested: bool,
        legs_count: u32,
        vac_level_hint: usize,
        siphon_subsidy_bps: i64,
    ) -> ExitSubReason {
        if siphon_subsidy_bps > 0 {
            return ExitSubReason::SinkingFundSiphonExit;
        }
        let raw_d = match side {
            Side::Bid => exit_px.0 - entry_px.0,
            Side::Ask => entry_px.0 - exit_px.0,
        };
        let gross_bps = (raw_d.saturating_mul(1_000_000)) / entry_px.0.max(1);
        if gross_bps >= 500 || vac_level_hint >= 2 {
            ExitSubReason::LvdbsDeepVacuumSurf
        } else if tranche1_harvested || legs_count >= 2 {
            ExitSubReason::Tranche1Harvest50
        } else {
            ExitSubReason::AdverseScratchEscape
        }
    }

    /// F-03 Fix: Reconciles position closure deterministically against /v5/position/closed-pnl.
    #[must_use]
    pub fn reconcile_closed_pnl_payload(
        &self,
        payload: &serde_json::Value,
        vac_level_hint: usize,
        siphon_subsidy_bps: i64,
        allow_partial_lot_coverage: bool,
    ) -> ClosedPnlQueryOutcome {
        let OrderLifecycleState::PositionOpenMakerExit {
            side,
            avg_price_ticks,
            initial_lots,
            open_lots,
            tranche1_harvested,
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

        for item in list {
            if accum_lots >= target_total_lots {
                break;
            }
            if let Some(sym_s) = item.get("symbol").and_then(|v| v.as_str()) {
                if sym_s != sym_expected {
                    continue;
                }
            }
            if let Some(close_side_s) = item.get("side").and_then(|v| v.as_str()) {
                if close_side_s != expected_close_side {
                    continue;
                }
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

            if let Some(pnl_s) = item.get("closedPnl").and_then(|v| v.as_str()) {
                if let Ok(leg_pnl) = parse_signed_decimal_1e8(pnl_s) {
                    let scaled_pnl = if take_lots < parsed_lots {
                        ((leg_pnl as i128 * take_lots as i128) / (parsed_lots as i128)) as i64
                    } else {
                        leg_pnl
                    };
                    total_closed_pnl_1e8 = total_closed_pnl_1e8.saturating_add(scaled_pnl);
                    has_pnl_field = true;
                }
            }

            accum_lots = accum_lots.saturating_add(take_lots);
            weighted_entry_sum += (take_lots as i128) * (leg_en_t.0 as i128);
            weighted_exit_sum += (take_lots as i128) * (leg_ex_t.0 as i128);
            legs_count = legs_count.saturating_add(1);
        }

        if legs_count == 0 || accum_lots < min_req_lots {
            return ClosedPnlQueryOutcome::PendingIndexerRetry;
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
                siphon_subsidy_bps,
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

    /// F-03 Safe Fallback: Used only if /v5/position/closed-pnl fails after bounded retries.
    #[must_use]
    pub fn resolve_fallback_unverified_close(
        &self,
        snap: &SymbolLobSnapshot,
        vac_level_hint: usize,
        siphon_subsidy_bps: i64,
    ) -> Option<VerifiedCloseResolution> {
        let OrderLifecycleState::PositionOpenMakerExit {
            side,
            avg_price_ticks,
            worst_adverse_price,
            initial_lots,
            open_lots,
            tranche1_harvested,
            tp_maker_price: prev_tp_state,
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
        let no_resting_tp_underwater = prev_tp_state.0 <= 0
            && match side {
                Side::Bid => mkt_ref.0 > 0 && mkt_ref.0 < avg_price_ticks.0,
                Side::Ask => mkt_ref.0 > avg_price_ticks.0,
            };

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
                closed_lots: initial_lots.max(open_lots).max(LotQty(1)),
                legs_count: 1,
                exchange_closed_pnl_1e8: None,
            });
        }

        let q_fl = defensive_zero_loss_maker_exit_price(side, avg_price_ticks);
        let t2_fill_px = if prev_tp_state.0 > 0 {
            prev_tp_state
        } else {
            q_fl
        };
        let exit_px = if tranche1_harvested {
            let t1_fl = dynamic_maker_floor_price(side, avg_price_ticks, ScaledBps(440));
            match side {
                Side::Bid => PriceTicks((t1_fl.0 + t2_fill_px.0) / 2),
                Side::Ask => PriceTicks(((t1_fl.0 + t2_fill_px.0 + 1) / 2).max(1)),
            }
        } else {
            t2_fill_px
        };
        let sub_reason = Self::classify_maker_exit_sub_reason(
            side,
            avg_price_ticks,
            exit_px,
            tranche1_harvested,
            if tranche1_harvested { 2 } else { 1 },
            vac_level_hint,
            siphon_subsidy_bps,
        );

        Some(VerifiedCloseResolution {
            kind: ClosedTradeKind::MakerPassiveUnwind,
            sub_reason,
            verified_entry_price: avg_price_ticks,
            verified_exit_price: exit_px,
            closed_lots: initial_lots.max(open_lots).max(LotQty(1)),
            legs_count: if tranche1_harvested { 2 } else { 1 },
            exchange_closed_pnl_1e8: None,
        })
    }

    /// Applies a VerifiedCloseResolution atomically and records the canonical close summary.
    pub fn apply_verified_close_resolution(
        &mut self,
        resolution: VerifiedCloseResolution,
        siphon_subsidy_bps: i64,
        closed_ts: TimestampNs,
        ledger: &mut crate::forensic_parquet::ParitySyncLedger,
    ) -> Option<ClosedTradeSummary> {
        let OrderLifecycleState::PositionOpenMakerExit { side, .. } = self.state else {
            return None;
        };
        self.last_exit_sub_reason = resolution.sub_reason;
        if matches!(resolution.kind, ClosedTradeKind::MakerPassiveUnwind) {
            if matches!(resolution.sub_reason, ExitSubReason::SinkingFundSiphonExit)
                && siphon_subsidy_bps > 0
            {
                let _ = ledger.try_consume_sinking_fund_subsidy(siphon_subsidy_bps);
            } else if matches!(resolution.sub_reason, ExitSubReason::LvdbsDeepVacuumSurf) {
                self.lvdbs_deep_surfs_count = self.lvdbs_deep_surfs_count.saturating_add(1);
            }
        }
        Some(self.finalize_close(
            resolution.kind,
            side,
            resolution.verified_entry_price,
            resolution.verified_exit_price,
            closed_ts,
            ledger,
        ))
    }

    /// Innovation #2 (LVDBS): Executes Tranche 1 (50%) harvest at quantum floor.
    pub fn apply_tranche1_harvest_50pct(
        &mut self,
        fill_price: PriceTicks,
        ledger: &mut crate::forensic_parquet::ParitySyncLedger,
    ) -> bool {
        let OrderLifecycleState::PositionOpenMakerExit {
            side,
            avg_price_ticks,
            ref mut open_lots,
            ref mut tranche1_harvested,
            ref mut active_exit_role,
            ref mut active_exit_lots,
            ..
        } = self.state
        else {
            return false;
        };
        if *tranche1_harvested || open_lots.0 < 2 {
            return false;
        }
        let harvested = LotQty(open_lots.0 / 2);
        *open_lots = LotQty(open_lots.0 - harvested.0);
        *tranche1_harvested = true;
        *active_exit_role = ExitOrderRole::LvdbsDeepSurf50;
        *active_exit_lots = *open_lots;
        self.tranche1_harvests_count = self.tranche1_harvests_count.saturating_add(1);

        let raw_diff = match side {
            Side::Bid => fill_price.0 - avg_price_ticks.0,
            Side::Ask => avg_price_ticks.0 - fill_price.0,
        };
        let gross_bps = (raw_diff.saturating_mul(1_000_000)) / avg_price_ticks.0.max(1);
        let net_leg_bps = (gross_bps - 400).max(40);
        let weighted_net_bps = (net_leg_bps / 2).max(20);
        let locked_70 = (weighted_net_bps * 70) / 100;
        let reserve_30 = weighted_net_bps - locked_70;
        ledger.vault_locked_profit_bps_x100 =
            ledger.vault_locked_profit_bps_x100.saturating_add(locked_70);
        ledger.sinking_fund_reserve_bps_x100 = ledger
            .sinking_fund_reserve_bps_x100
            .saturating_add(reserve_30)
            .min(crate::forensic_parquet::MAX_SINKING_FUND_SCALED_BPS);
        true
    }

    /// Canonical Unified Exit Quoter (SSOT shared 1:1 by live daemon and offline simulator).
    #[must_use]
    pub fn compute_canonical_exit_quote(
        &self,
        snap: &SymbolLobSnapshot,
        sinking_fund_reserve_bps_x100: i64,
    ) -> Option<CanonicalExitQuote> {
        let OrderLifecycleState::PositionOpenMakerExit {
            side,
            avg_price_ticks,
            initial_lots,
            open_lots,
            tranche1_harvested,
            opened_ts,
            ..
        } = self.state
        else {
            return None;
        };
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

        let dir_s = if matches!(side, Side::Bid) { 1.0 } else { -1.0 };
        let s_ofi = snap.factor_ofi.get() * dir_s;
        let s_tfi = snap.factor_tfi.get() * dir_s;
        let s_geom = snap.factor_geom.get() * dir_s;
        let signed_flow = 0.55 * s_ofi + 0.45 * s_tfi;
        let joint_adverse = s_ofi < -0.18 && s_tfi < -0.18;

        let time_c = (hold_s / 60).min(18) * 12;
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

        let peak_gross_bps = self.last_closed_mfe_bps;
        let worst_mae_bps = self.last_closed_mae_bps;
        let vol_c = snap.realized_vol_5m.raw().clamp(4_000, 12_500);
        let mfe_c = self.learned_mfe_bps.clamp(540, 1_250);
        let raw_siphon_ns = ((880_000_000_000_i64 * 6_500) / vol_c) * mfe_c / 680;
        let elastic_siphon_window_ns = raw_siphon_ns.clamp(680_000_000_000, 940_000_000_000);

        let early_phase_siphon = hold_s >= 35
            && peak_gross_bps <= 120
            && (cur_gross_bps <= -40 || worst_mae_bps <= -60)
            && sinking_fund_reserve_bps_x100 >= 100;
        let deep_stagnant_siphon =
            hold_ns >= elastic_siphon_window_ns && sinking_fund_reserve_bps_x100 >= 40;
        let siphon_eligible = !tranche1_harvested && (early_phase_siphon || deep_stagnant_siphon);

        let allow_scratch_floor = if tranche1_harvested {
            hold_s >= 90 && joint_adverse
        } else {
            worst_mae_bps <= -220 || hold_s >= 65 || (hold_s >= 25 && joint_adverse)
        };
        let mfe_pullback_lock = if tranche1_harvested {
            hold_s >= 45 && cur_gross_bps >= 485 && signed_flow < -0.10
        } else {
            peak_gross_bps >= 520
                && (cur_gross_bps <= peak_gross_bps - 35 || signed_flow < 0.08)
        };

        let can_split_tranche1 = self.slot_idx >= 1
            && !tranche1_harvested
            && initial_lots.0 >= 2
            && open_lots.0 >= 2
            && !allow_scratch_floor
            && !siphon_eligible;

        let q_floor = defensive_zero_loss_maker_exit_price(side, avg_price_ticks);
        let (vac_lvl, lvdbs_px, _) =
            OrthogonalFactorState::compute_l2_l3_vacuum_surf_target(side, snap, q_floor);

        let (role, sub_reason, raw_target_px, order_lots, lock_l1_now, siphon_sub) =
            if can_split_tranche1 {
                (
                    ExitOrderRole::Tranche1Harvest50,
                    ExitSubReason::Tranche1Harvest50,
                    q_floor,
                    LotQty((open_lots.0 / 2).max(1)),
                    true,
                    0_i64,
                )
            } else if siphon_eligible {
                let sub_bps = if deep_stagnant_siphon {
                    40_i64
                } else {
                    (440 - cur_gross_bps).clamp(40, 240)
                };
                (
                    ExitOrderRole::StandardMakerExit100,
                    ExitSubReason::SinkingFundSiphonExit,
                    q_floor,
                    open_lots,
                    true,
                    sub_bps,
                )
            } else if allow_scratch_floor || mfe_pullback_lock {
                let fl_bps = if mfe_pullback_lock {
                    ScaledBps(cur_gross_bps.clamp(485, 525))
                } else {
                    ScaledBps(if tranche1_harvested { 465 } else { 440 })
                };
                let fl_px = dynamic_maker_floor_price(side, avg_price_ticks, fl_bps);
                (
                    ExitOrderRole::StandardMakerExit100,
                    ExitSubReason::AdverseScratchEscape,
                    fl_px,
                    open_lots,
                    true,
                    0_i64,
                )
            } else {
                let role_surf = if tranche1_harvested {
                    ExitOrderRole::LvdbsDeepSurf50
                } else {
                    ExitOrderRole::StandardMakerExit100
                };
                let sub_surf = if vac_lvl >= 2 {
                    ExitSubReason::LvdbsDeepVacuumSurf
                } else {
                    ExitSubReason::AdverseScratchEscape
                };
                (role_surf, sub_surf, lvdbs_px, open_lots, false, 0_i64)
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
            vac_level: if lock_l1_now { 1 } else { vac_lvl },
            cur_gross_bps,
            wald_pct,
            lock_l1_now,
            siphon_subsidy_bps: siphon_sub,
        })
    }

    pub fn finalize_close(
        &mut self,
        kind: ClosedTradeKind,
        side: Side,
        entry_price: PriceTicks,
        exit_price: PriceTicks,
        closed_ts: TimestampNs,
        ledger: &mut crate::forensic_parquet::ParitySyncLedger,
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
        let raw_diff = match side {
            Side::Bid => exit_price.0 - entry_price.0,
            Side::Ask => entry_price.0 - exit_price.0,
        };
        let gross_bps = (raw_diff.saturating_mul(1_000_000)) / entry_price.0.max(1);
        let net_bps = gross_bps - fee_scaled_bps.raw();
        let fee_num = self.notional_per_trade_1e8.saturating_mul(fee_scaled_bps.raw());
        let fee_1e8 = fee_num / 1_000_000;
        let net_pnl_1e8 = (self.notional_per_trade_1e8.saturating_mul(net_bps)) / 1_000_000;

        if is_maker_exit {
            self.maker_exits_count = self.maker_exits_count.saturating_add(1);
        } else {
            self.taker_sl_exits_count = self.taker_sl_exits_count.saturating_add(1);
        }
        if net_pnl_1e8 >= 0 {
            self.gross_profit_accum_1e8 =
                self.gross_profit_accum_1e8.saturating_add(net_pnl_1e8);
        } else {
            self.gross_loss_accum_1e8 =
                self.gross_loss_accum_1e8.saturating_add(net_pnl_1e8.abs());
        }

        ledger.record_closed_trade_with_sinking_split(net_pnl_1e8, fee_1e8, ScaledBps(net_bps));
        self.observe_mfe_sample(self.last_closed_mfe_bps.max(gross_bps));
        self.cooldown_until_ns = TimestampNs(closed_ts.0 + 3_000_000_000);
        self.close_reconcile_retries = 0;
        self.state = OrderLifecycleState::Idle;

        ClosedTradeSummary {
            kind,
            side,
            entry_price,
            exit_price,
            gross_scaled_bps: ScaledBps(gross_bps),
            fee_scaled_bps,
            net_scaled_bps: ScaledBps(net_bps),
            net_pnl_1e8,
            is_maker_exit,
        }
    }

    pub fn step_open_position(
        &mut self,
        snap: &SymbolLobSnapshot,
        _btc_leader_snap: &SymbolLobSnapshot,
        _gov: &OnlineAdaptiveGovernor,
        ledger: &mut crate::forensic_parquet::ParitySyncLedger,
    ) -> Option<ClosedTradeSummary> {
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

        let reserve_bps = ledger.sinking_fund_reserve_bps_x100;
        let quote = self.compute_canonical_exit_quote(snap, reserve_bps)?;
        self.last_eff_sl_ticks = sl_trigger_price;
        self.last_closed_wald_pct = quote.wald_pct;

        let sl_hit = match side {
            Side::Bid => snap.mark_price.0 > 0 && snap.mark_price.0 <= sl_trigger_price.0,
            Side::Ask => snap.mark_price.0 > 0 && snap.mark_price.0 >= sl_trigger_price.0,
        };
        if sl_hit {
            self.last_exit_sub_reason = ExitSubReason::WaldBaseStopLoss;
            return Some(self.finalize_close(
                ClosedTradeKind::ServerMarkPriceStopLoss,
                side,
                avg_price_ticks,
                sl_trigger_price,
                snap.ts_local_ns,
                ledger,
            ));
        }

        if quote.cur_gross_bps < 440 {
            return None;
        }

        if matches!(quote.role, ExitOrderRole::Tranche1Harvest50) {
            let _ = self.apply_tranche1_harvest_50pct(quote.target_price, ledger);
            return None;
        }

        self.exit_peg_attempts = self.exit_peg_attempts.saturating_add(1);
        let reached_target = match side {
            Side::Bid => ref_trade.0 >= quote.target_price.0,
            Side::Ask => ref_trade.0 > 0 && ref_trade.0 <= quote.target_price.0,
        };
        let mfe_c = self.learned_mfe_bps.clamp(540, 1_250);
        let elastic_l1_harvest_bps = (460 + (mfe_c - 540) / 18).clamp(460, 500);

        if reached_target
            && (quote.vac_level >= 2
                || quote.cur_gross_bps >= elastic_l1_harvest_bps
                || quote.siphon_subsidy_bps > 0)
        {
            if quote.siphon_subsidy_bps > 0 {
                let _ = ledger.try_consume_sinking_fund_subsidy(quote.siphon_subsidy_bps);
                self.last_exit_sub_reason = ExitSubReason::SinkingFundSiphonExit;
            } else if quote.vac_level >= 2 {
                self.lvdbs_deep_surfs_count = self.lvdbs_deep_surfs_count.saturating_add(1);
                self.last_exit_sub_reason = ExitSubReason::LvdbsDeepVacuumSurf;
            } else {
                self.last_exit_sub_reason = quote.sub_reason;
            }
            return Some(self.finalize_close(
                ClosedTradeKind::MakerPassiveUnwind,
                side,
                avg_price_ticks,
                quote.target_price,
                snap.ts_local_ns,
                ledger,
            ));
        }
        None
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::lob_shm::BookLevel;
    use crate::types::BoundedScore;

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
        snap.realized_vol_5m = ScaledBps(6_000);
        snap.factor_ofi = BoundedScore::new_clamped(-0.60);
        snap.factor_tfi = BoundedScore::new_clamped(-0.55);
        assert!(check_queue_depletion_velocity_abort(
            Side::Bid,
            LotQty(1_000),
            LotQty(300),
            &snap,
        ));
    }

    #[test]
    fn test_governor_12x2_loss_lab_62_of_62_and_2160_manifold() {
        let (mitigated_62, manifold_2160) =
            OnlineAdaptiveGovernor::audit_unified_62_losses_and_2160_manifold();
        assert_eq!(mitigated_62, 62);
        assert_eq!(manifold_2160, 2_160);
        let gov = OnlineAdaptiveGovernor::new();
        assert!(gov
            .evaluate_kinematic_energy_surface(1, Side::Bid, 105, 148, 0.24, 0.60)
            .is_ok());
        assert!(gov
            .evaluate_kinematic_energy_surface(1, Side::Bid, 220, 150, 0.22, 0.47)
            .is_err());
    }

    #[test]
    fn test_canonical_exit_quote_ssot_parity_and_monotonic_tranche_lock() {
        let mut eng = SlotExecutionEngine::new(3, 1_250_000_000);
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
        let q1 = eng.compute_canonical_exit_quote(&snap, 0);
        assert!(q1.is_some());
        if let Some(qt) = q1 {
            assert_eq!(qt.role, ExitOrderRole::Tranche1Harvest50);
            assert_eq!(qt.order_lots, LotQty(13));
            assert_eq!(qt.target_price, PriceTicks(8_202));
        }
    }

    #[test]
    fn test_slot_engine_tranche1_and_lvdbs_deep_surf_close() {
        let mut eng = SlotExecutionEngine::new(3, 1_250_000_000);
        let gov = OnlineAdaptiveGovernor::new();
        let mut ledger = crate::forensic_parquet::ParitySyncLedger::new(7_088_000_000);
        let spec = APPROVED_SPECS[3];
        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = spec.symbol_id;
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
        assert!(eng.on_entry_order_resting(TimestampNs(1_025_000_000)).is_ok());
        assert!(eng.on_maker_entry_filled(TimestampNs(1_500_000_000)).is_ok());

        snap.ts_local_ns = TimestampNs(10_000_000_000);
        snap.bids[0] = BookLevel { price: PriceTicks(8_201), qty: LotQty(400) };
        snap.asks[0] = BookLevel { price: PriceTicks(8_202), qty: LotQty(200) };
        snap.last_trade_price = PriceTicks(8_202);
        assert!(eng.step_open_position(&snap, &snap, &gov, &mut ledger).is_none());
        assert_eq!(eng.tranche1_harvests_count, 1);

        let ask_px = [8_203, 8_205, 8_208, 8_211];
        for (i, &p) in ask_px.iter().enumerate() {
            snap.bids[i] = BookLevel { price: PriceTicks(8_202 - i as i64), qty: LotQty(350) };
            snap.asks[i] = BookLevel { price: PriceTicks(p), qty: LotQty(200) };
        }
        snap.factor_ofi = BoundedScore::new_clamped(0.65);
        snap.factor_tfi = BoundedScore::new_clamped(0.75);
        snap.last_trade_price = PriceTicks(8_211);
        let closed = eng.step_open_position(&snap, &snap, &gov, &mut ledger);
        assert!(closed.is_some());
        if let Some(summary) = closed {
            assert_eq!(summary.exit_price, PriceTicks(8_211));
            assert!(summary.net_scaled_bps.raw() >= 1_100);
            assert_eq!(eng.last_exit_sub_reason, ExitSubReason::LvdbsDeepVacuumSurf);
        }
    }

    #[test]
    fn test_slot_engine_sinking_fund_siphon_rescue_at_15min() {
        let mut eng = SlotExecutionEngine::new(0, 1_250_000_000);
        let gov = OnlineAdaptiveGovernor::new();
        let mut ledger = crate::forensic_parquet::ParitySyncLedger::new(7_088_000_000);
        ledger.sinking_fund_reserve_bps_x100 = 500;
        ledger.vault_locked_profit_bps_x100 = 1_200;
        let spec = APPROVED_SPECS[0];
        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = spec.symbol_id;
        snap.realized_vol_5m = ScaledBps(6_500);
        snap.ts_local_ns = TimestampNs(1_000_000_000);
        snap.bids[0] = BookLevel { price: PriceTicks(759_000), qty: LotQty(100) };
        snap.asks[0] = BookLevel { price: PriceTicks(759_001), qty: LotQty(100) };
        snap.mark_price = PriceTicks(759_000);
        snap.last_trade_price = PriceTicks(759_000);

        let verdict = crate::alpha_3factor::SignalVerdict::AuthorizeMakerEntry {
            side: Side::Bid,
            target_excursion_scaled_bps: ScaledBps(600),
        };
        assert!(eng.try_arm_maker_entry(&snap, verdict, LotQty(1)).is_ok());
        assert!(eng.on_maker_entry_filled(TimestampNs(2_000_000_000)).is_ok());

        snap.ts_local_ns = TimestampNs(955_000_000_000);
        snap.bids[0] = BookLevel { price: PriceTicks(759_045), qty: LotQty(100) };
        snap.asks[0] = BookLevel { price: PriceTicks(759_046), qty: LotQty(100) };
        snap.mark_price = PriceTicks(759_046);
        snap.last_trade_price = PriceTicks(759_046);
        assert!(eng.step_open_position(&snap, &snap, &gov, &mut ledger).is_none());

        snap.ts_local_ns = TimestampNs(960_000_000_000);
        snap.bids[0] = BookLevel { price: PriceTicks(759_333), qty: LotQty(100) };
        snap.asks[0] = BookLevel { price: PriceTicks(759_334), qty: LotQty(100) };
        snap.mark_price = PriceTicks(759_334);
        snap.last_trade_price = PriceTicks(759_334);
        let closed = eng.step_open_position(&snap, &snap, &gov, &mut ledger);
        assert!(closed.is_some());
        if let Some(summary) = closed {
            assert_eq!(summary.exit_price, PriceTicks(759_334));
            assert!(summary.net_scaled_bps.raw() >= 40);
            assert_eq!(eng.last_exit_sub_reason, ExitSubReason::SinkingFundSiphonExit);
            assert!(ledger.vault_locked_profit_bps_x100 >= 1_200);
        }
    }

    #[test]
    fn test_f02_preflight_sync_and_verified_bybit_avg_price_floor() {
        let avax = APPROVED_SPECS[3];
        let sui = APPROVED_SPECS[5];
        assert_eq!(
            parse_verified_avg_price_ticks(avax, Side::Bid, "8.204"),
            Ok(PriceTicks(8_204))
        );
        assert_eq!(
            parse_verified_avg_price_ticks(avax, Side::Ask, "8.204"),
            Ok(PriceTicks(8_204))
        );
        assert_eq!(
            parse_verified_avg_price_ticks(avax, Side::Bid, "8.20333333"),
            Ok(PriceTicks(8_204))
        );
        assert_eq!(
            parse_verified_avg_price_ticks(avax, Side::Ask, "8.20366667"),
            Ok(PriceTicks(8_203))
        );
        assert_eq!(
            parse_verified_avg_price_ticks(sui, Side::Bid, "0.851000003"),
            Ok(PriceTicks(8_511))
        );
        assert_eq!(
            parse_verified_avg_price_ticks(sui, Side::Ask, "0.851000003"),
            Ok(PriceTicks(8_510))
        );

        let mut eng = SlotExecutionEngine::new(3, 1_250_000_000);
        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = avax.symbol_id;
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

        snap.bids[0] = BookLevel { price: PriceTicks(8_215), qty: LotQty(300) };
        snap.asks[0] = BookLevel { price: PriceTicks(8_216), qty: LotQty(250) };
        assert!(eng.sync_preflight_l1_entry(&snap, 8).is_err());

        snap.bids[0] = BookLevel { price: PriceTicks(8_204), qty: LotQty(300) };
        snap.asks[0] = BookLevel { price: PriceTicks(8_205), qty: LotQty(250) };
        snap.mark_price = PriceTicks(8_204);
        snap.last_trade_price = PriceTicks(8_204);
        let synced = eng.sync_preflight_l1_entry(&snap, 8);
        assert!(synced.is_ok());
        if let Ok((px, sl)) = synced {
            assert_eq!(px, PriceTicks(8_204));
            assert!(sl.0 <= 8_202);
        }

        let resting = eng.on_entry_order_resting(TimestampNs(1_030_000_000));
        assert!(resting.is_ok());
        if let OrderLifecycleState::RestingEntryQueue { entry_price, .. } = eng.state {
            assert_eq!(entry_price, PriceTicks(8_204));
        } else {
            assert_eq!(eng.slot_idx, 999);
        }

        snap.bids[0] = BookLevel { price: PriceTicks(8_207), qty: LotQty(320) };
        snap.asks[0] = BookLevel { price: PriceTicks(8_208), qty: LotQty(240) };
        snap.mark_price = PriceTicks(8_207);
        snap.last_trade_price = PriceTicks(8_207);
        let repeg_sl = eng.compute_repeg_sl_trigger(&snap, PriceTicks(8_207));
        assert!(repeg_sl.is_ok());
        if let Ok(new_sl) = repeg_sl {
            eng.apply_resting_repeg(PriceTicks(8_207), new_sl, TimestampNs(3_600_000_000));
        }

        let real_avg = parse_verified_avg_price_ticks(avax, Side::Bid, "8.2074");
        assert_eq!(real_avg, Ok(PriceTicks(8_208)));
        assert!(eng
            .on_maker_entry_filled_verified(
                TimestampNs(4_000_000_000),
                Some(Side::Bid),
                real_avg.ok(),
                Some(LotQty(24)),
            )
            .is_ok());

        snap.bids[0] = BookLevel { price: PriceTicks(8_208), qty: LotQty(300) };
        snap.asks[0] = BookLevel { price: PriceTicks(8_209), qty: LotQty(250) };
        let q_after = eng.compute_canonical_exit_quote(&snap, 0);
        assert!(q_after.is_some());
        if let Some(qt) = q_after {
            assert_eq!(qt.target_price, PriceTicks(8_212));
            assert_eq!(qt.order_lots, LotQty(12));
        }

        eng.sync_open_avg_price_ticks(PriceTicks(8_209));
        let q_synced = eng.compute_canonical_exit_quote(&snap, 0);
        if let Some(qt2) = q_synced {
            assert_eq!(qt2.target_price, PriceTicks(8_213));
        }
    }

    #[test]
    fn test_f03_closed_pnl_reconciler_eradicates_false_sl_and_catches_real_sl() {
        assert_eq!(parse_signed_decimal_1e8("-0.2845"), Ok(-28_450_000));
        assert_eq!(parse_signed_decimal_1e8("0.01250000"), Ok(1_250_000));

        let spec = APPROVED_SPECS[3];
        assert_eq!(
            parse_verified_exit_price_ticks(spec, Side::Bid, "8.2065"),
            Ok(PriceTicks(8_206))
        );
        assert_eq!(
            parse_verified_exit_price_ticks(spec, Side::Ask, "8.1935"),
            Ok(PriceTicks(8_194))
        );

        let mut eng = SlotExecutionEngine::new(3, 1_250_000_000);
        let mut ledger = crate::forensic_parquet::ParitySyncLedger::new(7_088_000_000);
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

        // Case 1: Maker win filled @ 8.207, then market dipped to 8.195 (< avg - 2).
        snap.mark_price = PriceTicks(8_195);
        snap.last_trade_price = PriceTicks(8_195);
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
        let out1 = eng.reconcile_closed_pnl_payload(&win_payload, 2, 0, false);
        if let ClosedPnlQueryOutcome::Verified(res1) = out1 {
            assert_eq!(res1.kind, ClosedTradeKind::MakerPassiveUnwind);
            assert_eq!(res1.sub_reason, ExitSubReason::LvdbsDeepVacuumSurf);
            assert_eq!(res1.verified_exit_price, PriceTicks(8_207));
            assert_eq!(res1.exchange_closed_pnl_1e8, Some(968_000));
            let closed1 = eng.apply_verified_close_resolution(
                res1,
                0,
                TimestampNs(1_700_000_012_500_000_000),
                &mut ledger,
            );
            assert!(closed1.is_some());
            if let Some(s1) = closed1 {
                assert!(s1.is_maker_exit);
                assert!(s1.net_scaled_bps.raw() >= 450);
            }
        } else {
            assert_eq!(eng.slot_idx, 999);
        }

        // Case 2: 2-leg trade where Leg 1 closed Limit and Leg 2 hit Market SL (indexer lag).
        snap.ts_local_ns = TimestampNs(1_700_000_030_000_000_000);
        snap.mark_price = PriceTicks(8_202);
        snap.last_trade_price = PriceTicks(8_202);
        assert!(eng.try_arm_maker_entry(&snap, verdict, LotQty(26)).is_ok());
        assert!(eng
            .on_maker_entry_filled(TimestampNs(1_700_000_030_500_000_000))
            .is_ok());

        let partial_lag_payload = serde_json::json!({
            "retCode": 0,
            "result": {
                "list": [{
                    "symbol": "AVAXUSDT",
                    "side": "Sell",
                    "orderType": "Limit",
                    "execType": "Trade",
                    "closedSize": "1.3",
                    "avgEntryPrice": "8.200",
                    "avgExitPrice": "8.204",
                    "closedPnl": "0.00093000",
                    "updatedTime": "1700000035000"
                }]
            }
        });
        assert_eq!(
            eng.reconcile_closed_pnl_payload(&partial_lag_payload, 1, 0, false),
            ClosedPnlQueryOutcome::PendingIndexerRetry
        );

        let full_sl_payload = serde_json::json!({
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "symbol": "AVAXUSDT",
                        "side": "Sell",
                        "orderType": "Market",
                        "execType": "Trade",
                        "closedSize": "1.3",
                        "avgEntryPrice": "8.200",
                        "avgExitPrice": "8.050",
                        "closedPnl": "-0.20300000",
                        "updatedTime": "1700000042000"
                    },
                    {
                        "symbol": "AVAXUSDT",
                        "side": "Sell",
                        "orderType": "Limit",
                        "execType": "Trade",
                        "closedSize": "1.3",
                        "avgEntryPrice": "8.200",
                        "avgExitPrice": "8.204",
                        "closedPnl": "0.00093000",
                        "updatedTime": "1700000035000"
                    }
                ]
            }
        });
        let out2 = eng.reconcile_closed_pnl_payload(&full_sl_payload, 1, 0, false);
        if let ClosedPnlQueryOutcome::Verified(res2) = out2 {
            assert_eq!(res2.kind, ClosedTradeKind::ServerMarkPriceStopLoss);
            assert_eq!(res2.sub_reason, ExitSubReason::WaldBaseStopLoss);
            assert_eq!(res2.verified_exit_price, PriceTicks(8_127));
            assert_eq!(res2.legs_count, 2);
            assert_eq!(res2.exchange_closed_pnl_1e8, Some(-20_207_000));
        } else {
            assert_eq!(eng.slot_idx, 999);
        }

        // Case 3: Fallback does NOT trigger false SL at 8.195 when resting TP > 0 & SL ~ 8.044.
        snap.mark_price = PriceTicks(8_195);
        let fb = eng.resolve_fallback_unverified_close(&snap, 1, 0);
        assert!(fb.is_some());
        if let Some(fb_res) = fb {
            assert_eq!(fb_res.kind, ClosedTradeKind::MakerPassiveUnwind);
            assert!(fb_res.verified_exit_price.0 >= 8_204);
        }
    }
}
