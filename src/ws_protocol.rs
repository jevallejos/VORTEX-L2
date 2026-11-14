//! Native FIPS 180-4 HMAC-SHA256, Conservative Decimal Parsers, WS Lattice & VIP-0 Rate Governor.

use crate::types::{LotQty, PairSpec, PriceTicks, Side, VortexError};

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

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum OrderLinkRole {
    Entry = 0,
    Tranche1 = 1,
    Runner2 = 2,
}

impl OrderLinkRole {
    #[inline]
    #[must_use]
    pub const fn code_str(self) -> &'static str {
        match self {
            Self::Entry => "E",
            Self::Tranche1 => "T1",
            Self::Runner2 => "R2",
        }
    }
}

/// Bijective generator for Bybit V5 orderLinkId (<= 32 ASCII bytes) and reqId.
#[inline]
#[must_use]
pub fn format_bijective_order_link_id(
    boot_hash32: u32,
    slot_idx: usize,
    cycle_id: u32,
    role: OrderLinkRole,
    sub_idx: u16,
) -> String {
    let s_clamped = slot_idx.min(99);
    let cyc24 = cycle_id & 0x00FF_FFFF;
    let sub12 = sub_idx & 0x0FFF;
    format!(
        "vx-{boot_hash32:08x}-s{s_clamped:02}-c{cyc24:06x}-{}{sub12:03x}",
        role.code_str()
    )
}

#[inline]
#[must_use]
pub fn format_ws_trade_req_id(boot_hash32: u32, seq_id: u32) -> String {
    format!("r-{boot_hash32:08x}-{seq_id:08x}")
}

/// Exact LeavesQty conservation equation for Bybit V5 order.amend: q_tot' = q_exec + q_des.
#[inline]
#[must_use]
pub const fn compute_amend_total_lots(
    cum_exec_lots: LotQty,
    desired_leaves_lots: LotQty,
) -> LotQty {
    let exec_pos = if cum_exec_lots.0 > 0 { cum_exec_lots.0 } else { 0 };
    let des_pos = if desired_leaves_lots.0 > 1 { desired_leaves_lots.0 } else { 1 };
    LotQty(exec_pos.saturating_add(des_pos))
}

/// Exact FIFO priority transition T_amend: preserves queue_ahead when P'=P and q_des' <= q_cur.
#[inline]
#[must_use]
pub const fn amend_fifo_queue_priority(
    cur_price: PriceTicks,
    cur_leaves_qty: LotQty,
    cur_queue_ahead: LotQty,
    new_price: PriceTicks,
    new_leaves_qty: LotQty,
    book_level_total_qty: LotQty,
) -> (PriceTicks, LotQty, LotQty) {
    let clamped_q = LotQty(if new_leaves_qty.0 > 1 { new_leaves_qty.0 } else { 1 });
    if cur_price.0 == new_price.0 && clamped_q.0 <= cur_leaves_qty.0 {
        (new_price, clamped_q, cur_queue_ahead)
    } else {
        let tail_q = if book_level_total_qty.0 > 0 {
            book_level_total_qty
        } else {
            LotQty(0)
        };
        (new_price, clamped_q, tail_q)
    }
}

/// Asymmetric recv_window (ms): 2,000ms for speculative entry create, 10,000ms for protective ops.
#[inline]
#[must_use]
pub const fn asymmetric_recv_window_ms(is_entry_create: bool) -> u32 {
    if is_entry_create {
        2_000
    } else {
        10_000
    }
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum WsOrderRank {
    Dispatched = 0,
    GatewayAccepted = 1,
    RestingBook = 2,
    PartialFill = 3,
    Terminal = 4,
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum WsTerminalKind {
    None = 0,
    Filled = 1,
    Cancelled = 2,
    PostOnlyRejected = 3,
    ExchangeRejected = 4,
}

/// Commutative join-semilattice state for full-duplex WS Trade + WS Private synchronization.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct WsOrderLatticeState {
    pub rank: WsOrderRank,
    pub price_ticks: PriceTicks,
    pub total_lots: LotQty,
    pub cum_exec_lots: LotQty,
    pub terminal_kind: WsTerminalKind,
}

impl WsOrderLatticeState {
    #[inline]
    #[must_use]
    pub const fn new_dispatched(price_ticks: PriceTicks, total_lots: LotQty) -> Self {
        Self {
            rank: WsOrderRank::Dispatched,
            price_ticks,
            total_lots,
            cum_exec_lots: LotQty(0),
            terminal_kind: WsTerminalKind::None,
        }
    }

    #[inline]
    #[must_use]
    pub const fn join(self, other: Self) -> Self {
        let r_self = self.rank as u8;
        let r_oth = other.rank as u8;
        if r_oth > r_self {
            let max_exec = if other.cum_exec_lots.0 > self.cum_exec_lots.0 {
                other.cum_exec_lots
            } else {
                self.cum_exec_lots
            };
            Self {
                cum_exec_lots: max_exec,
                ..other
            }
        } else if r_self > r_oth {
            let max_exec = if self.cum_exec_lots.0 > other.cum_exec_lots.0 {
                self.cum_exec_lots
            } else {
                other.cum_exec_lots
            };
            Self {
                cum_exec_lots: max_exec,
                ..self
            }
        } else {
            let max_exec = if other.cum_exec_lots.0 >= self.cum_exec_lots.0 {
                other.cum_exec_lots
            } else {
                self.cum_exec_lots
            };
            let px = if other.price_ticks.0 > 0 {
                other.price_ticks
            } else {
                self.price_ticks
            };
            let tot = if other.total_lots.0 > 0 {
                other.total_lots
            } else {
                self.total_lots
            };
            let term = if (other.terminal_kind as u8) > (self.terminal_kind as u8) {
                other.terminal_kind
            } else {
                self.terminal_kind
            };
            Self {
                rank: self.rank,
                price_ticks: px,
                total_lots: tot,
                cum_exec_lots: max_exec,
                terminal_kind: term,
            }
        }
    }
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum WsMutationOp {
    Create = 0,
    Amend = 1,
    Cancel = 2,
}

/// Orthogonal VIP-0 3x10 req/s token-bucket governor with 2-token emergency reserve.
#[derive(Copy, Clone, Debug)]
pub struct Vip0WsRateGovernor {
    pub srv_remaining: [i64; 3],
    pub reset_ts_ns: [i64; 3],
    pub inflight: [i64; 3],
}

impl Default for Vip0WsRateGovernor {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

#[allow(clippy::manual_clamp)]
impl Vip0WsRateGovernor {
    pub const VIP0_LIMIT_PER_SEC: i64 = 10;
    pub const EMERGENCY_RESERVE_TOKENS: i64 = 2;

    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            srv_remaining: [Self::VIP0_LIMIT_PER_SEC; 3],
            reset_ts_ns: [0; 3],
            inflight: [0; 3],
        }
    }

    #[inline]
    #[must_use]
    pub const fn available_tokens(&self, op: WsMutationOp, now_ns: i64) -> i64 {
        let idx = op as usize;
        let base = if now_ns >= self.reset_ts_ns[idx] {
            Self::VIP0_LIMIT_PER_SEC
        } else {
            self.srv_remaining[idx]
        };
        let avail = base - self.inflight[idx];
        if avail > 0 { avail } else { 0 }
    }

    #[inline]
    #[must_use]
    pub const fn authorizes_mutation(
        &self,
        op: WsMutationOp,
        is_defensive_or_abort: bool,
        now_ns: i64,
    ) -> bool {
        let avail = self.available_tokens(op, now_ns);
        if is_defensive_or_abort {
            avail >= 1
        } else {
            avail > Self::EMERGENCY_RESERVE_TOKENS
        }
    }

    #[inline]
    pub fn on_dispatch(&mut self, op: WsMutationOp, now_ns: i64) {
        let idx = op as usize;
        if now_ns >= self.reset_ts_ns[idx] {
            self.srv_remaining[idx] = Self::VIP0_LIMIT_PER_SEC;
            self.reset_ts_ns[idx] = now_ns.saturating_add(1_000_000_000);
        }
        self.inflight[idx] = self.inflight[idx].saturating_add(1);
    }

    #[inline]
    pub fn on_ack_headers(
        &mut self,
        op: WsMutationOp,
        limit_status: Option<i64>,
        reset_ts_ms: Option<i64>,
    ) {
        let idx = op as usize;
        self.inflight[idx] = (self.inflight[idx] - 1).max(0);
        if let Some(rem) = limit_status {
            self.srv_remaining[idx] = rem.clamp(0, Self::VIP0_LIMIT_PER_SEC);
        } else {
            self.srv_remaining[idx] = (self.srv_remaining[idx] - 1).max(0);
        }
        if let Some(rst_ms) = reset_ts_ms {
            self.reset_ts_ns[idx] = rst_ms.saturating_mul(1_000_000);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::APPROVED_SPECS;

    #[test]
    fn test_hmac_sha256_parsers_lattice_and_vip0_rate_governor() {
        let sig = sign_bybit_v5_hmac_sha256("test_secret", "1700000000000api_key5000");
        assert_eq!(sig.len(), 64);

        let avax = APPROVED_SPECS[3];
        assert_eq!(
            parse_verified_avg_price_ticks(avax, Side::Bid, "8.20333333"),
            Ok(PriceTicks(8_204))
        );
        assert_eq!(
            parse_verified_exit_price_ticks(avax, Side::Bid, "8.20366667"),
            Ok(PriceTicks(8_203))
        );
        assert_eq!(parse_signed_decimal_1e8("-0.2845"), Ok(-28_450_000));

        let id_e0 = format_bijective_order_link_id(0xABCD_1234, 4, 15, OrderLinkRole::Entry, 0);
        let id_t1 = format_bijective_order_link_id(0xABCD_1234, 4, 15, OrderLinkRole::Tranche1, 0);
        let id_r2 = format_bijective_order_link_id(0xABCD_1234, 4, 15, OrderLinkRole::Runner2, 0);
        assert!(id_e0.len() <= 32 && id_t1.len() <= 32 && id_r2.len() <= 32);
        assert_ne!(id_e0, id_t1);
        assert_ne!(id_t1, id_r2);
        assert_eq!(
            format_ws_trade_req_id(0xABCD_1234, 7),
            "r-abcd1234-00000007"
        );
        assert_eq!(compute_amend_total_lots(LotQty(6), LotQty(10)), LotQty(16));
        assert_eq!(
            amend_fifo_queue_priority(
                PriceTicks(8_318),
                LotQty(16),
                LotQty(420),
                PriceTicks(8_318),
                LotQty(8),
                LotQty(1_900),
            ),
            (PriceTicks(8_318), LotQty(8), LotQty(420))
        );
        assert_eq!(asymmetric_recv_window_ms(true), 2_000);
        assert_eq!(asymmetric_recv_window_ms(false), 10_000);

        let s_disp = WsOrderLatticeState::new_dispatched(PriceTicks(21_000), LotQty(10));
        let s_ack = WsOrderLatticeState {
            rank: WsOrderRank::GatewayAccepted,
            price_ticks: PriceTicks(21_000),
            total_lots: LotQty(10),
            cum_exec_lots: LotQty(0),
            terminal_kind: WsTerminalKind::None,
        };
        let s_fill = WsOrderLatticeState {
            rank: WsOrderRank::Terminal,
            price_ticks: PriceTicks(21_000),
            total_lots: LotQty(10),
            cum_exec_lots: LotQty(10),
            terminal_kind: WsTerminalKind::Filled,
        };
        assert_eq!(s_disp.join(s_ack).join(s_fill), s_disp.join(s_fill).join(s_ack));

        let mut rg = Vip0WsRateGovernor::new();
        let t0 = 1_000_000_000_i64;
        for _ in 0..8 {
            assert!(rg.authorizes_mutation(WsMutationOp::Amend, false, t0));
            rg.on_dispatch(WsMutationOp::Amend, t0);
        }
        assert!(!rg.authorizes_mutation(WsMutationOp::Amend, false, t0));
        assert!(rg.authorizes_mutation(WsMutationOp::Amend, true, t0));
    }
}
