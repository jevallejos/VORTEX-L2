//! Zero-Alloc Bi-Directional Order Block Engine (OB+ / OB-) with Self-Learned Plasticity.
#![allow(clippy::manual_clamp, clippy::manual_range_contains, clippy::needless_range_loop, clippy::too_many_arguments)]

use crate::types::{BoundedScore, LotQty, PairSpec, PriceTicks, ScaledBps, Side};

const PHI: f64 = 1.618_033_988_749_895;
const INV_PHI: f64 = 0.618_033_988_749_895;
pub const MAX_OB_ZONES_PER_SIDE: usize = 4;
const BAR_RING_CAP: usize = 20;
const SEED_EFFICACY_PM: i64 = 618;
const MIN_EFFICACY_PM: i64 = 382;
const MAX_EFFICACY_PM: i64 = 950;

#[repr(u8)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum ObLifecycleState { #[default] Empty = 0, Virgin = 1, MitigatedShallow = 2 }

#[repr(u8)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum ObFuelKind { #[default] FreshOpenInterest = 0, SqueezeLiquidation = 1 }

#[repr(u8)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum ObHorizon { #[default] Tactical1m = 0, Structural5m = 1 }

#[repr(C)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct OrderBlockZone {
    pub state: ObLifecycleState,
    pub fuel: ObFuelKind,
    pub horizon: ObHorizon,
    pub bounce_verified: bool,
    pub low_ticks: PriceTicks,
    pub high_ticks: PriceTicks,
    pub eq_ticks: PriceTicks,
    pub strength_pm: i64,
}

#[repr(C)]
#[derive(Copy, Clone, Debug, Default, PartialEq)]
pub struct OrderBlockObservables {
    pub bull_ob_score: BoundedScore,
    pub bear_ob_score: BoundedScore,
    pub dist_bull_ob_bps: ScaledBps,
    pub dist_bear_ob_bps: ScaledBps,
    pub bull_wall_ticks: PriceTicks,
    pub bear_wall_ticks: PriceTicks,
    pub learned_efficacy_pm: i64,
}

impl OrderBlockObservables {
    #[inline]
    #[must_use]
    pub const fn effective_efficacy_pm(self) -> i64 {
        if self.learned_efficacy_pm > 0 { self.learned_efficacy_pm } else { SEED_EFFICACY_PM }
    }

    /// Endogenous volatility + 3x friction + self-learned wall collision radius.
    #[inline]
    #[must_use]
    pub fn elastic_wall_veto_radius_bps(self, spec: PairSpec, mid_px: PriceTicks, vol_5m: ScaledBps) -> ScaledBps {
        let b3x = spec.required_3x_barrier_bps(mid_px).unwrap_or(ScaledBps::MIN_REQUIRED_VOL_5M_BPS).raw().max(600);
        let base = (vol_5m.raw() / 5).clamp((b3x * 2) / 3, b3x * 2);
        ScaledBps((base.saturating_mul(500 + self.effective_efficacy_pm())) / 1_000)
    }

    #[inline]
    #[must_use]
    pub fn vetoes_into_opposing_wall(self, side: Side, spec: PairSpec, mid_px: PriceTicks, vol_5m: ScaledBps, signed_tfi: f64) -> bool {
        if signed_tfi >= (1.0 - 0.20 * INV_PHI) { return false; }
        let radius = self.elastic_wall_veto_radius_bps(spec, mid_px, vol_5m).raw();
        let min_sc = INV_PHI * (self.effective_efficacy_pm() as f64 * 1e-3);
        match side {
            Side::Bid => self.bear_wall_ticks.0 > 0 && self.bear_ob_score.get() >= min_sc && self.dist_bear_ob_bps.raw() >= 0 && self.dist_bear_ob_bps.raw() <= radius,
            Side::Ask => self.bull_wall_ticks.0 > 0 && self.bull_ob_score.get() >= min_sc && self.dist_bull_ob_bps.raw() >= 0 && self.dist_bull_ob_bps.raw() <= radius,
        }
    }

    /// Continuous C^1 confluence resonance in `[0.0, 1.0]` when pullback lands on aligned OB.
    #[inline]
    #[must_use]
    pub fn aligned_confluence_resonance(self, side: Side, spec: PairSpec, mid_px: PriceTicks, vol_5m: ScaledBps) -> f64 {
        let (w_t, d_bps, sc) = match side {
            Side::Bid => (self.bull_wall_ticks, self.dist_bull_ob_bps.raw(), self.bull_ob_score.get()),
            Side::Ask => (self.bear_wall_ticks, self.dist_bear_ob_bps.raw(), self.bear_ob_score.get()),
        };
        if w_t.0 <= 0 || sc <= 0.05 { return 0.0; }
        let span = self.elastic_wall_veto_radius_bps(spec, mid_px, vol_5m).raw().max(400) as f64;
        let d = d_bps as f64;
        if d < -INV_PHI * span || d > PHI * span { return 0.0; }
        let z = d.max(0.0) / (INV_PHI * span);
        (sc * (-0.5 * z * z).exp()).clamp(0.0, 1.0)
    }

    /// Elastic Maker front-run TP ahead of opposing institutional Order Block wall.
    #[inline]
    #[must_use]
    pub fn front_run_opposing_wall_tp(self, open_side: Side, spec: PairSpec, avg_entry: PriceTicks, cand_tp: PriceTicks) -> PriceTicks {
        let tau_bps = spec.tick_to_price_scaled_bps(avg_entry).unwrap_or(ScaledBps(100)).raw().max(1);
        let pad_ticks = (ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() / tau_bps).clamp(2, 12);
        let min_sc = INV_PHI * 0.80;
        match open_side {
            Side::Bid => {
                let w = self.bear_wall_ticks.0;
                if w > avg_entry.0 + pad_ticks + 1 && self.bear_ob_score.get() >= min_sc && cand_tp.0 >= w {
                    PriceTicks(w - pad_ticks)
                } else { cand_tp }
            }
            Side::Ask => {
                let w = self.bull_wall_ticks.0;
                if w > 0 && w + pad_ticks + 1 < avg_entry.0 && self.bull_ob_score.get() >= min_sc && cand_tp.0 <= w {
                    PriceTicks(w + pad_ticks)
                } else { cand_tp }
            }
        }
    }
}

#[derive(Copy, Clone, Debug, Default)]
struct OhlcBar { bucket_id: i64, open: PriceTicks, high: PriceTicks, low: PriceTicks, close: PriceTicks, turnover_1e8: i64, end_oi_x100: i64 }

#[derive(Copy, Clone, Debug)]
struct BarRing { bars: [OhlcBar; BAR_RING_CAP], len: usize, cur: OhlcBar, has_cur: bool }

impl BarRing {
    const fn new() -> Self {
        let z = OhlcBar { bucket_id: 0, open: PriceTicks(0), high: PriceTicks(0), low: PriceTicks(0), close: PriceTicks(0), turnover_1e8: 0, end_oi_x100: 0 };
        Self { bars: [z; BAR_RING_CAP], len: 0, cur: z, has_cur: false }
    }
    fn push_closed(&mut self, bar: OhlcBar) {
        if self.len < BAR_RING_CAP { self.bars[self.len] = bar; self.len += 1; } else {
            for i in 0..(BAR_RING_CAP - 1) { self.bars[i] = self.bars[i + 1]; }
            self.bars[BAR_RING_CAP - 1] = bar;
        }
    }
    fn step_tick(&mut self, bucket: i64, px: PriceTicks, turn_1e8: i64, oi_x100: i64) -> bool {
        if !self.has_cur {
            self.cur = OhlcBar { bucket_id: bucket, open: px, high: px, low: px, close: px, turnover_1e8: turn_1e8.max(1), end_oi_x100: oi_x100 };
            self.has_cur = true;
            return false;
        }
        if bucket <= self.cur.bucket_id {
            if px.0 > self.cur.high.0 { self.cur.high = px; }
            if px.0 < self.cur.low.0 { self.cur.low = px; }
            self.cur.close = px;
            self.cur.turnover_1e8 = self.cur.turnover_1e8.saturating_add(turn_1e8.max(1));
            if oi_x100 > 0 { self.cur.end_oi_x100 = oi_x100; }
            return false;
        }
        let closed = self.cur;
        self.push_closed(closed);
        self.cur = OhlcBar { bucket_id: bucket, open: px, high: px, low: px, close: px, turnover_1e8: turn_1e8.max(1), end_oi_x100: oi_x100 };
        true
    }
}

#[derive(Copy, Clone, Debug)]
pub struct CausalOrderBlockEngine {
    bull_zones: [OrderBlockZone; MAX_OB_ZONES_PER_SIDE],
    bear_zones: [OrderBlockZone; MAX_OB_ZONES_PER_SIDE],
    ring_1m: BarRing,
    ring_5m: BarRing,
    last_oi_x100: i64,
    learned_efficacy_pm: i64,
    cached_obs: OrderBlockObservables,
}

impl Default for CausalOrderBlockEngine { #[inline] fn default() -> Self { Self::new() } }

impl CausalOrderBlockEngine {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        let ez = OrderBlockZone { state: ObLifecycleState::Empty, fuel: ObFuelKind::FreshOpenInterest, horizon: ObHorizon::Tactical1m, bounce_verified: false, low_ticks: PriceTicks(0), high_ticks: PriceTicks(0), eq_ticks: PriceTicks(0), strength_pm: 0 };
        Self {
            bull_zones: [ez; MAX_OB_ZONES_PER_SIDE],
            bear_zones: [ez; MAX_OB_ZONES_PER_SIDE],
            ring_1m: BarRing::new(),
            ring_5m: BarRing::new(),
            last_oi_x100: 0,
            learned_efficacy_pm: SEED_EFFICACY_PM,
            cached_obs: OrderBlockObservables { bull_ob_score: BoundedScore::ZERO, bear_ob_score: BoundedScore::ZERO, dist_bull_ob_bps: ScaledBps(0), dist_bear_ob_bps: ScaledBps(0), bull_wall_ticks: PriceTicks(0), bear_wall_ticks: PriceTicks(0), learned_efficacy_pm: SEED_EFFICACY_PM },
        }
    }

    #[inline] pub fn reset(&mut self) { *self = Self::new(); }
    #[inline] #[must_use] pub const fn observables(&self) -> OrderBlockObservables { self.cached_obs }
    #[inline] pub fn observe_open_interest(&mut self, oi_contracts: f64) {
        if oi_contracts.is_finite() && oi_contracts > 0.0 { self.last_oi_x100 = (oi_contracts * 100.0).round() as i64; }
    }

    fn insert_zone(slots: &mut [OrderBlockZone; MAX_OB_ZONES_PER_SIDE], zone: OrderBlockZone) {
        for s in &mut *slots { if s.state == ObLifecycleState::Empty { *s = zone; return; } }
        let mut w = 0_usize;
        for i in 1..MAX_OB_ZONES_PER_SIDE { if slots[i].strength_pm <= slots[w].strength_pm { w = i; } }
        slots[w] = zone;
    }

    fn detect_from_ring(&mut self, is_5m: bool) {
        let ring = if is_5m { &self.ring_5m } else { &self.ring_1m };
        if ring.len < 7 { return; }
        let n = ring.len;
        let (b0, b1, b2) = (ring.bars[n - 3], ring.bars[n - 2], ring.bars[n - 1]);
        let hist_n = n - 3;
        let (mut sum_r, mut sum_t) = (0.0_f64, 0.0_f64);
        for i in 0..hist_n {
            sum_r += (ring.bars[i].high.0 - ring.bars[i].low.0).max(1) as f64;
            sum_t += ring.bars[i].turnover_1e8.max(1) as f64;
        }
        let avg_r = (sum_r / (hist_n as f64)).max(1.0);
        let turn_ratio = (b0.turnover_1e8.max(b1.turnover_1e8).max(b2.turnover_1e8) as f64) / (sum_t / (hist_n as f64)).max(1.0);
        if turn_ratio < (1.0 + 0.25 * INV_PHI) { return; }
        let fuel = if b2.end_oi_x100 >= b0.end_oi_x100 { ObFuelKind::FreshOpenInterest } else { ObFuelKind::SqueezeLiquidation };
        let horizon = if is_5m { ObHorizon::Structural5m } else { ObHorizon::Tactical1m };
        let h_w = if is_5m { 1.0 } else { f64::midpoint(1.0, INV_PHI) };
        let f_w = if matches!(fuel, ObFuelKind::FreshOpenInterest) { 1.0 } else { 1.0 - 0.15 * INV_PHI };
        if b0.close.0 < b0.open.0 {
            let disp = (b1.high.0.max(b2.high.0) - b0.close.0).max(0) as f64;
            if b1.close.0.max(b2.close.0) > b0.high.0 && (b2.low.0 > b0.high.0 || disp >= (PHI + INV_PHI) * avg_r) && disp >= PHI * avg_r {
                let raw_s = ((disp / (3.0 * avg_r)).min(1.0) * 650.0 + (turn_ratio / 3.0).min(1.0) * 350.0) * h_w * f_w;
                Self::insert_zone(&mut self.bull_zones, OrderBlockZone { state: ObLifecycleState::Virgin, fuel, horizon, bounce_verified: false, low_ticks: b0.low, high_ticks: b0.open, eq_ticks: PriceTicks(i64::midpoint(b0.low.0, b0.open.0)), strength_pm: (raw_s.round() as i64).clamp(MIN_EFFICACY_PM, 1_000) });
            }
        } else if b0.close.0 > b0.open.0 {
            let disp = (b0.close.0 - b1.low.0.min(b2.low.0)).max(0) as f64;
            if b1.close.0.min(b2.close.0) < b0.low.0 && (b2.high.0 < b0.low.0 || disp >= (PHI + INV_PHI) * avg_r) && disp >= PHI * avg_r {
                let raw_s = ((disp / (3.0 * avg_r)).min(1.0) * 650.0 + (turn_ratio / 3.0).min(1.0) * 350.0) * h_w * f_w;
                Self::insert_zone(&mut self.bear_zones, OrderBlockZone { state: ObLifecycleState::Virgin, fuel, horizon, bounce_verified: false, low_ticks: b0.open, high_ticks: b0.high, eq_ticks: PriceTicks(i64::midpoint(b0.open.0, b0.high.0)), strength_pm: (raw_s.round() as i64).clamp(MIN_EFFICACY_PM, 1_000) });
            }
        }
    }

    fn step_mitigation_and_plasticity(&mut self, spec: PairSpec, px: PriceTicks) {
        if px.0 <= 0 { return; }
        let b3x = spec.required_3x_barrier_bps(px).unwrap_or(ScaledBps::MIN_REQUIRED_VOL_5M_BPS).raw();
        let mut bull_b = (PriceTicks(0), i64::MAX, 0.0_f64);
        for z in &mut self.bull_zones {
            if z.state == ObLifecycleState::Empty { continue; }
            if px.0 < z.low_ticks.0 || (z.state == ObLifecycleState::MitigatedShallow && px.0 < z.eq_ticks.0) {
                if z.state == ObLifecycleState::MitigatedShallow && !z.bounce_verified {
                    self.learned_efficacy_pm = ((self.learned_efficacy_pm * 88 + MIN_EFFICACY_PM * 12) / 100).clamp(MIN_EFFICACY_PM, MAX_EFFICACY_PM);
                }
                z.state = ObLifecycleState::Empty;
                continue;
            }
            if px.0 <= z.high_ticks.0 {
                z.state = ObLifecycleState::MitigatedShallow;
            } else if z.state == ObLifecycleState::MitigatedShallow && !z.bounce_verified {
                let reb = ((px.0 - z.high_ticks.0).saturating_mul(1_000_000)) / z.high_ticks.0.max(1);
                if reb >= b3x { z.bounce_verified = true; self.learned_efficacy_pm = ((self.learned_efficacy_pm * 85 + MAX_EFFICACY_PM * 15) / 100).clamp(MIN_EFFICACY_PM, MAX_EFFICACY_PM); }
            }
            let dist = ((px.0 - z.high_ticks.0).saturating_mul(1_000_000)) / px.0.max(1);
            let sc = ((z.strength_pm as f64) * 1e-3) * (if z.state == ObLifecycleState::Virgin { 1.0 } else { INV_PHI }) * ((self.learned_efficacy_pm as f64) / (SEED_EFFICACY_PM as f64));
            if dist >= -b3x && dist < bull_b.1 { bull_b = (z.high_ticks, dist, sc.clamp(0.0, 1.0)); }
        }
        let mut bear_b = (PriceTicks(0), i64::MAX, 0.0_f64);
        for z in &mut self.bear_zones {
            if z.state == ObLifecycleState::Empty { continue; }
            if px.0 > z.high_ticks.0 || (z.state == ObLifecycleState::MitigatedShallow && px.0 > z.eq_ticks.0) {
                if z.state == ObLifecycleState::MitigatedShallow && !z.bounce_verified {
                    self.learned_efficacy_pm = ((self.learned_efficacy_pm * 88 + MIN_EFFICACY_PM * 12) / 100).clamp(MIN_EFFICACY_PM, MAX_EFFICACY_PM);
                }
                z.state = ObLifecycleState::Empty;
                continue;
            }
            if px.0 >= z.low_ticks.0 {
                z.state = ObLifecycleState::MitigatedShallow;
            } else if z.state == ObLifecycleState::MitigatedShallow && !z.bounce_verified {
                let reb = ((z.low_ticks.0 - px.0).saturating_mul(1_000_000)) / z.low_ticks.0.max(1);
                if reb >= b3x { z.bounce_verified = true; self.learned_efficacy_pm = ((self.learned_efficacy_pm * 85 + MAX_EFFICACY_PM * 15) / 100).clamp(MIN_EFFICACY_PM, MAX_EFFICACY_PM); }
            }
            let dist = ((z.low_ticks.0 - px.0).saturating_mul(1_000_000)) / px.0.max(1);
            let sc = ((z.strength_pm as f64) * 1e-3) * (if z.state == ObLifecycleState::Virgin { 1.0 } else { INV_PHI }) * ((self.learned_efficacy_pm as f64) / (SEED_EFFICACY_PM as f64));
            if dist >= -b3x && dist < bear_b.1 { bear_b = (z.low_ticks, dist, sc.clamp(0.0, 1.0)); }
        }
        self.cached_obs = OrderBlockObservables {
            bull_ob_score: BoundedScore::new_clamped(bull_b.2),
            bear_ob_score: BoundedScore::new_clamped(bear_b.2),
            dist_bull_ob_bps: ScaledBps(if bull_b.0 .0 > 0 { bull_b.1 } else { 0 }),
            dist_bear_ob_bps: ScaledBps(if bear_b.0 .0 > 0 { bear_b.1 } else { 0 }),
            bull_wall_ticks: bull_b.0,
            bear_wall_ticks: bear_b.0,
            learned_efficacy_pm: self.learned_efficacy_pm,
        };
    }

    #[inline]
    pub fn observe_mid_quote(&mut self, spec: PairSpec, mid_px: PriceTicks) -> OrderBlockObservables {
        if mid_px.0 > 0 { self.step_mitigation_and_plasticity(spec, mid_px); }
        self.cached_obs
    }

    pub fn observe_live_tick(&mut self, spec: PairSpec, price: PriceTicks, qty: LotQty, ts_ns: i64) -> OrderBlockObservables {
        if price.0 <= 0 || ts_ns <= 0 { return self.cached_obs; }
        let turn_1e8 = spec.lot_notional_1e8(LotQty(qty.0.max(1)), price);
        let oi = self.last_oi_x100;
        if self.ring_1m.step_tick(ts_ns / 60_000_000_000, price, turn_1e8, oi) { self.detect_from_ring(false); }
        if self.ring_5m.step_tick(ts_ns / 300_000_000_000, price, turn_1e8, oi) { self.detect_from_ring(true); }
        self.step_mitigation_and_plasticity(spec, price);
        self.cached_obs
    }

    pub fn ingest_closed_bar(&mut self, spec: PairSpec, is_5m: bool, bucket_id: i64, open: PriceTicks, high: PriceTicks, low: PriceTicks, close: PriceTicks, turnover_1e8: i64, oi_x100: i64) {
        if open.0 <= 0 || high.0 < low.0 || low.0 <= 0 || close.0 <= 0 { return; }
        let bar = OhlcBar { bucket_id, open, high, low, close, turnover_1e8: turnover_1e8.max(1), end_oi_x100: oi_x100 };
        if is_5m { self.ring_5m.push_closed(bar); } else { self.ring_1m.push_closed(bar); }
        self.detect_from_ring(is_5m);
        self.step_mitigation_and_plasticity(spec, close);
    }

    pub fn warm_start_from_bybit_bundle(&mut self, spec: PairSpec, kline_5m: Option<&serde_json::Value>, kline_1m: Option<&serde_json::Value>, oi_5m: Option<&serde_json::Value>) -> usize {
        let mut oi_pts = [(0_i64, 0_i64); 64];
        let mut oi_len = 0_usize;
        if let Some(rows) = oi_5m.and_then(|v| v.get("result")).and_then(|r| r.get("list")).and_then(|l| l.as_array()) {
            for item in rows.iter().rev().take(64) {
                let ts = item.get("timestamp").and_then(|v| v.as_str()).and_then(|s| s.parse::<i64>().ok()).unwrap_or(0);
                if let Some(of) = item.get("openInterest").and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok()) {
                    self.observe_open_interest(of);
                    oi_pts[oi_len] = (ts, self.last_oi_x100);
                    oi_len += 1;
                }
            }
        }
        let mut count = 0_usize;
        for (is_5m, k_opt) in [(true, kline_5m), (false, kline_1m)] {
            let Some(rows) = k_opt.and_then(|v| v.get("result")).and_then(|r| r.get("list")).and_then(|l| l.as_array()) else { continue };
            for row in rows.iter().rev() {
                let Some(arr) = row.as_array() else { continue };
                if arr.len() < 7 { continue; }
                let ts_ms = arr[0].as_str().and_then(|s| s.parse::<i64>().ok()).unwrap_or(0);
                let (Ok(op), Ok(hi), Ok(lo), Ok(cl)) = (
                    spec.parse_price_ticks(arr[1].as_str().unwrap_or("")),
                    spec.parse_price_ticks(arr[2].as_str().unwrap_or("")),
                    spec.parse_price_ticks(arr[3].as_str().unwrap_or("")),
                    spec.parse_price_ticks(arr[4].as_str().unwrap_or("")),
                ) else { continue };
                let mut bar_oi = self.last_oi_x100;
                for i in 0..oi_len { if oi_pts[i].0 <= ts_ms { bar_oi = oi_pts[i].1; } }
                let turn_f = arr[6].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(100.0);
                let div_ms = if is_5m { 300_000 } else { 60_000 };
                self.ingest_closed_bar(spec, is_5m, ts_ms / div_ms, op, hi, lo, cl, (turn_f.max(1.0) * 1e8).round() as i64, bar_oi);
                count += 1;
            }
        }
        count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::APPROVED_SPECS;

    #[test]
    fn test_ob_bullish_demand_self_learning_wall_veto_and_zero_ms_invalidation() {
        let avax = APPROVED_SPECS[3];
        let vol = ScaledBps(6_200);
        let mut eng = CausalOrderBlockEngine::new();
        assert!(!eng.observables().vetoes_into_opposing_wall(Side::Ask, avax, PriceTicks(7_500), vol, 0.20));
        for b in 1..=8_i64 {
            eng.ingest_closed_bar(avax, false, b, PriceTicks(7_502), PriceTicks(7_505), PriceTicks(7_499), PriceTicks(7_501), 100_000_000_000, 10_000);
        }
        eng.ingest_closed_bar(avax, false, 9, PriceTicks(7_500), PriceTicks(7_502), PriceTicks(7_492), PriceTicks(7_494), 180_000_000_000, 10_000);
        eng.ingest_closed_bar(avax, false, 10, PriceTicks(7_494), PriceTicks(7_512), PriceTicks(7_494), PriceTicks(7_510), 260_000_000_000, 10_250);
        eng.ingest_closed_bar(avax, false, 11, PriceTicks(7_510), PriceTicks(7_522), PriceTicks(7_505), PriceTicks(7_520), 220_000_000_000, 10_500);
        assert_eq!(eng.observables().bull_wall_ticks, PriceTicks(7_500));

        let near_obs = eng.observe_live_tick(avax, PriceTicks(7_503), LotQty(20), 12 * 60_000_000_000);
        assert!(near_obs.vetoes_into_opposing_wall(Side::Ask, avax, PriceTicks(7_503), vol, 0.45));
        assert!(!near_obs.vetoes_into_opposing_wall(Side::Ask, avax, PriceTicks(7_503), vol, 0.92));
        assert!(near_obs.aligned_confluence_resonance(Side::Bid, avax, PriceTicks(7_503), vol) > 0.45);
        assert_eq!(near_obs.front_run_opposing_wall_tp(Side::Ask, avax, PriceTicks(7_520), PriceTicks(7_495)), PriceTicks(7_502));

        let _ = eng.observe_live_tick(avax, PriceTicks(7_498), LotQty(20), 12 * 60_000_000_000 + 5_000_000_000);
        let bounced = eng.observe_live_tick(avax, PriceTicks(7_520), LotQty(30), 12 * 60_000_000_000 + 15_000_000_000);
        assert!(bounced.learned_efficacy_pm > SEED_EFFICACY_PM);

        let broken = eng.observe_live_tick(avax, PriceTicks(7_490), LotQty(40), 12 * 60_000_000_000 + 25_000_000_000);
        assert_eq!(broken.bull_wall_ticks, PriceTicks(0));
    }

    #[test]
    fn test_ob_bearish_supply_squeeze_fuel_and_bybit_bundle_warm_start() {
        let btc = APPROVED_SPECS[0];
        let mut eng = CausalOrderBlockEngine::new();
        let k5 = serde_json::json!({ "result": { "list": [
            ["1700003000000", "66380.0", "66390.0", "66100.0", "66120.0", "50", "3500000"],
            ["1700002700000", "66580.0", "66585.0", "66350.0", "66380.0", "60", "4200000"],
            ["1700002400000", "66420.0", "66620.0", "66410.0", "66580.0", "45", "3100000"],
            ["1700002100000", "66430.0", "66450.0", "66400.0", "66420.0", "15", "1000000"],
            ["1700001800000", "66420.0", "66445.0", "66395.0", "66430.0", "15", "1000000"],
            ["1700001500000", "66425.0", "66450.0", "66400.0", "66420.0", "15", "1000000"],
            ["1700001200000", "66420.0", "66445.0", "66395.0", "66425.0", "15", "1000000"],
            ["1700000900000", "66415.0", "66440.0", "66390.0", "66420.0", "15", "1000000"]
        ]}} );
        let oi = serde_json::json!({ "result": { "list": [
            {"openInterest": "94000", "timestamp": "1700003000000"},
            {"openInterest": "98000", "timestamp": "1700002400000"}
        ]}} );
        assert_eq!(eng.warm_start_from_bybit_bundle(btc, Some(&k5), None, Some(&oi)), 8);
        let obs = eng.observe_live_tick(btc, PriceTicks(663_900), LotQty(2), 1_700_003_100_000_000_000);
        assert_eq!(obs.bear_wall_ticks, PriceTicks(664_200));
        assert!(obs.vetoes_into_opposing_wall(Side::Bid, btc, PriceTicks(663_900), ScaledBps(5_800), 0.35));
        assert_eq!(obs.front_run_opposing_wall_tp(Side::Bid, btc, PriceTicks(658_000), PriceTicks(665_000)).0, 664_188);
    }

    #[test]
    fn test_omega_v44_pareto_dominance_v43_vs_v44_campaign() {
        use crate::alpha_3factor::SignalVerdict;
        use crate::bayesian_governor::OnlineAdaptiveGovernor;
        use crate::execution_fsm::{OrderLifecycleState, SlotExecutionEngine};
        use crate::forensic_parquet::ParitySyncLedger;
        use crate::lob_shm::{BookLevel, SymbolLobSnapshot};
        use crate::types::TimestampNs;
        use crate::wave_elliott::{WaveObservables, WaveRegime};
        let avax = APPROVED_SPECS[3];
        let mut ob = CausalOrderBlockEngine::new();
        for b in 1..=6_i64 { ob.ingest_closed_bar(avax, true, b, PriceTicks(7_511), PriceTicks(7_515), PriceTicks(7_510), PriceTicks(7_512), 100_000_000_000, 10_000); }
        ob.ingest_closed_bar(avax, true, 7, PriceTicks(7_526), PriceTicks(7_534), PriceTicks(7_525), PriceTicks(7_532), 180_000_000_000, 10_000);
        ob.ingest_closed_bar(avax, true, 8, PriceTicks(7_532), PriceTicks(7_532), PriceTicks(7_512), PriceTicks(7_514), 260_000_000_000, 9_700);
        ob.ingest_closed_bar(avax, true, 9, PriceTicks(7_514), PriceTicks(7_520), PriceTicks(7_500), PriceTicks(7_502), 240_000_000_000, 9_500);
        for b in 10..=14_i64 { ob.ingest_closed_bar(avax, true, b, PriceTicks(7_501), PriceTicks(7_504), PriceTicks(7_500), PriceTicks(7_502), 100_000_000_000, 9_500); }
        ob.ingest_closed_bar(avax, true, 15, PriceTicks(7_500), PriceTicks(7_502), PriceTicks(7_492), PriceTicks(7_494), 180_000_000_000, 9_500);
        ob.ingest_closed_bar(avax, true, 16, PriceTicks(7_494), PriceTicks(7_512), PriceTicks(7_494), PriceTicks(7_510), 260_000_000_000, 9_900);
        ob.ingest_closed_bar(avax, true, 17, PriceTicks(7_510), PriceTicks(7_520), PriceTicks(7_505), PriceTicks(7_518), 240_000_000_000, 10_300);
        let live_ob = ob.observe_mid_quote(avax, PriceTicks(7_503));
        let gov = OnlineAdaptiveGovernor::new();
        let (mut l43, mut l44) = (ParitySyncLedger::new(6_568_240_000), ParitySyncLedger::new(6_568_240_000));
        let (mut e43, mut e44) = (SlotExecutionEngine::new(3), SlotExecutionEngine::new(3));
        let mut btc = SymbolLobSnapshot::new_empty();
        btc.symbol_id = APPROVED_SPECS[0].symbol_id; btc.is_eligible = true; btc.realized_vol_5m = ScaledBps(5_800);
        btc.bids[0] = BookLevel { price: PriceTicks(656_000), qty: LotQty(400) }; btc.asks[0] = BookLevel { price: PriceTicks(656_001), qty: LotQty(350) };
        let mut s43 = SymbolLobSnapshot::new_empty();
        s43.symbol_id = avax.symbol_id; s43.is_eligible = true; s43.realized_vol_5m = ScaledBps(6_200); s43.ts_local_ns = TimestampNs(1_000_000_000);
        s43.bids[0] = BookLevel { price: PriceTicks(7_502), qty: LotQty(400) }; s43.asks[0] = BookLevel { price: PriceTicks(7_503), qty: LotQty(350) };
        s43.mark_price = PriceTicks(7_503); s43.last_trade_price = PriceTicks(7_503);
        s43.factor_ofi = BoundedScore::new_clamped(-0.55); s43.factor_tfi = BoundedScore::new_clamped(-0.60);
        let mut s44 = s43; s44.set_order_block_observables(live_ob);
        let v_trap = SignalVerdict::AuthorizeMakerEntry { side: Side::Ask, target_excursion_scaled_bps: ScaledBps(700) };
        assert!(e43.try_arm_maker_entry(&s43, v_trap, LotQty(10)).is_ok() && e43.on_maker_entry_filled(TimestampNs(1_500_000_000)).is_ok());
        assert!(e44.try_arm_maker_entry(&s44, v_trap, LotQty(10)).is_ok() && e44.state == OrderLifecycleState::Idle);
        s43.ts_local_ns = TimestampNs(6_000_000_000); s43.mark_price = PriceTicks(7_595); s43.last_trade_price = PriceTicks(7_595);
        s43.bids[0].price = PriceTicks(7_594); s43.asks[0].price = PriceTicks(7_595);
        let loss_43 = e43.step_open_position(&s43, &btc, &gov, &mut l43).map_or(0, |c| c.net_scaled_bps.raw());
        assert!(loss_43 < -5_000);
        let mut e43_b = SlotExecutionEngine::new(3);
        s43.ts_local_ns = TimestampNs(20_000_000_000); s43.bids[0].price = PriceTicks(7_503); s43.asks[0].price = PriceTicks(7_504);
        s43.mark_price = PriceTicks(7_503); s43.last_trade_price = PriceTicks(7_503); s43.factor_ofi = BoundedScore::new_clamped(0.72); s43.factor_tfi = BoundedScore::new_clamped(0.85);
        s43.set_wave_observables(WaveObservables { regime: WaveRegime::ImpulseWave3, climax_active: false, hurst: WaveObservables::encode_hurst(0.64), macro_phase: BoundedScore::new_clamped(0.95), oi_impulse: BoundedScore::new_clamped(0.25), liq_imbalance: BoundedScore::ZERO, fib_proj_bps: ScaledBps(4_500) });
        s44 = s43; s44.set_order_block_observables(live_ob);
        let v_long = SignalVerdict::AuthorizeMakerEntry { side: Side::Bid, target_excursion_scaled_bps: ScaledBps(900) };
        assert!(e43_b.try_arm_maker_entry(&s43, v_long, LotQty(1)).is_ok() && e43_b.on_maker_entry_filled(TimestampNs(20_500_000_000)).is_ok());
        assert!(e44.try_arm_maker_entry(&s44, v_long, LotQty(1)).is_ok() && e44.on_maker_entry_filled(TimestampNs(20_500_000_000)).is_ok());
        let mut win_44 = 0_i64;
        for k in 1..=6_i64 {
            let ts = 21_000_000_000 + k * 500_000_000;
            let px = if k == 1 { 7_515 } else { 7_525 };
            for s in [&mut s43, &mut s44] { s.ts_local_ns = TimestampNs(ts); s.bids[0].price = PriceTicks(px - 1); s.asks[0].price = PriceTicks(px); s.mark_price = PriceTicks(px); s.last_trade_price = PriceTicks(px); }
            assert!(e43_b.step_open_position(&s43, &btc, &gov, &mut l43).is_none());
            if win_44 == 0 { if let Some(c) = e44.step_open_position(&s44, &btc, &gov, &mut l44) { assert!(c.is_maker_exit); win_44 = c.net_scaled_bps.raw(); } }
        }
        assert!(win_44 >= 2_200 && win_44 > loss_43);
    }
}

pub const OB_PLASTICITY_VAULT_PATH: &str = "data/vault/vortex_ob_plasticity.json";

impl CausalOrderBlockEngine {
    #[inline]
    #[must_use]
    pub const fn learned_efficacy_permille(&self) -> i64 {
        self.learned_efficacy_pm
    }

    #[inline]
    pub fn set_learned_efficacy_permille(&mut self, efficacy_pm: i64) {
        if efficacy_pm > 0 {
            let clamped = efficacy_pm.clamp(MIN_EFFICACY_PM, MAX_EFFICACY_PM);
            self.learned_efficacy_pm = clamped;
            self.cached_obs.learned_efficacy_pm = clamped;
        }
    }

    pub fn reset_preserving_plasticity(&mut self) {
        let keep_pm = self.learned_efficacy_pm;
        *self = Self::new();
        self.set_learned_efficacy_permille(keep_pm);
    }

    pub fn save_plasticity_array_to_path<const N: usize>(
        engines: &[Self; N],
        path: &std::path::Path,
        ts_ns: i64,
    ) {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let vals: Vec<i64> = engines.iter().map(Self::learned_efficacy_permille).collect();
        let payload = serde_json::json!({
            "event": "OB_PLASTICITY_V44",
            "ts_ns": ts_ns,
            "learned_efficacy_pm": vals,
        });
        if let Ok(txt) = serde_json::to_string(&payload) {
            let _ = std::fs::write(path, txt);
        }
    }

    #[must_use]
    pub fn load_plasticity_array_from_path<const N: usize>(
        engines: &mut [Self; N],
        path: &std::path::Path,
    ) -> usize {
        let Ok(raw) = std::fs::read_to_string(path) else {
            return 0;
        };
        let Ok(val) = serde_json::from_str::<serde_json::Value>(&raw) else {
            return 0;
        };
        let Some(arr) = val.get("learned_efficacy_pm").and_then(|v| v.as_array()) else {
            return 0;
        };
        let mut loaded = 0_usize;
        for (idx, item) in arr.iter().take(N).enumerate() {
            if let Some(pm) = item.as_i64() {
                if (MIN_EFFICACY_PM..=MAX_EFFICACY_PM).contains(&pm) {
                    engines[idx].set_learned_efficacy_permille(pm);
                    loaded += 1;
                }
            }
        }
        loaded
    }
}

#[cfg(test)]
mod omega_v441_plasticity_tests {
    use super::*;

    #[test]
    fn test_ob_plasticity_vault_roundtrip_and_reconnect_preservation() {
        let tmp = std::env::temp_dir().join(format!("vortex_ob_plast_{}.json", std::process::id()));
        let mut src = [CausalOrderBlockEngine::new(); 12];
        for (idx, eng) in src.iter_mut().enumerate() {
            eng.set_learned_efficacy_permille(500 + (idx as i64) * 25);
        }
        CausalOrderBlockEngine::save_plasticity_array_to_path(&src, &tmp, 1_794_690_000_000_000_000);

        let mut dst = [CausalOrderBlockEngine::new(); 12];
        let loaded = CausalOrderBlockEngine::load_plasticity_array_from_path(&mut dst, &tmp);
        let _ = std::fs::remove_file(&tmp);

        assert_eq!(loaded, 12);
        assert_eq!(dst[0].learned_efficacy_permille(), 500);
        assert_eq!(dst[11].learned_efficacy_permille(), 775);

        dst[3].reset_preserving_plasticity();
        assert_eq!(dst[3].learned_efficacy_permille(), 575);
        assert_eq!(dst[3].observables().learned_efficacy_pm, 575);
    }
}
