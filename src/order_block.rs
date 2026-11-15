//! Zero-Alloc Bi-Directional Order Block Engine (OB+ / OB-) with Distance-Weighted Eviction & Live VIP Fees (V4.6).
#![allow(
    clippy::manual_clamp,
    clippy::manual_range_contains,
    clippy::needless_range_loop,
    clippy::suboptimal_flops,
    clippy::too_many_arguments
)]

use core::cmp::Ordering;

use crate::fee_cadence_beta::FeeSchedule;
use crate::types::{BoundedScore, LotQty, PairSpec, PriceTicks, ScaledBps, Side, TimestampNs};

const PHI: f64 = 1.618_033_988_749_895;
const INV_PHI: f64 = 0.618_033_988_749_895;
pub const MAX_OB_ZONES_PER_SIDE: usize = 4;
const BAR_RING_CAP: usize = 20;
const SEED_EFFICACY_PM: i64 = 618;
const MIN_EFFICACY_PM: i64 = 382;
const MAX_EFFICACY_PM: i64 = 950;
pub const OB_DIRECTIONAL_MIN_PM: i64 = 382;
pub const OB_DIRECTIONAL_SEED_PM: i64 = 618;
pub const OB_DIRECTIONAL_MAX_PM: i64 = 854;
const OB_PLASTICITY_ETA_PM: i64 = 120;
pub const OB_PLASTICITY_VAULT_PATH: &str = "data/vault/vortex_ob_plasticity.json";

#[repr(u8)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum ObLifecycleState {
    #[default]
    Empty = 0,
    Virgin = 1,
    MitigatedShallow = 2,
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum ObFuelKind {
    #[default]
    FreshOpenInterest = 0,
    SqueezeLiquidation = 1,
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum ObHorizon {
    #[default]
    Tactical1m = 0,
    Structural5m = 1,
}

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

    #[inline]
    #[must_use]
    pub fn elastic_wall_veto_radius_bps(self, spec: PairSpec, mid_px: PriceTicks, vol_5m: ScaledBps) -> ScaledBps {
        let b3x = spec.required_3x_barrier_bps(mid_px).unwrap_or(ScaledBps::MIN_REQUIRED_VOL_5M_BPS).raw().max(600);
        let base = (vol_5m.raw() / 5).clamp((b3x * 2) / 3, b3x * 2);
        ScaledBps((base.saturating_mul(500 + self.effective_efficacy_pm())) / 1_000)
    }

    #[inline]
    #[must_use]
    pub fn elastic_wall_veto_radius_bps_dynamic(
        self,
        spec: PairSpec,
        mid_px: PriceTicks,
        vol_5m: ScaledBps,
        taker_prob_pm: i64,
        fees: FeeSchedule,
    ) -> ScaledBps {
        let b3x = spec.three_x_friction_barrier_bps_dynamic(mid_px, taker_prob_pm, fees).raw().max(400);
        let base = (vol_5m.raw() / 5).clamp((b3x * 2) / 3, b3x * 2);
        ScaledBps((base.saturating_mul(500 + self.effective_efficacy_pm())) / 1_000)
    }

    #[inline]
    #[must_use]
    pub fn vetoes_into_opposing_wall(
        self,
        side: Side,
        spec: PairSpec,
        mid_px: PriceTicks,
        vol_5m: ScaledBps,
        signed_tfi: f64,
    ) -> bool {
        if signed_tfi >= (1.0 - 0.20 * INV_PHI) {
            return false;
        }
        let radius = self.elastic_wall_veto_radius_bps(spec, mid_px, vol_5m).raw();
        let min_sc = INV_PHI * (self.effective_efficacy_pm() as f64 * 1e-3);
        match side {
            Side::Bid => {
                self.bear_wall_ticks.0 > 0
                    && self.bear_ob_score.get() >= min_sc
                    && self.dist_bear_ob_bps.raw() >= 0
                    && self.dist_bear_ob_bps.raw() <= radius
            }
            Side::Ask => {
                self.bull_wall_ticks.0 > 0
                    && self.bull_ob_score.get() >= min_sc
                    && self.dist_bull_ob_bps.raw() >= 0
                    && self.dist_bull_ob_bps.raw() <= radius
            }
        }
    }

    #[inline]
    #[must_use]
    pub fn vetoes_into_opposing_wall_dynamic(
        self,
        side: Side,
        spec: PairSpec,
        mid_px: PriceTicks,
        vol_5m: ScaledBps,
        signed_tfi: f64,
        taker_prob_pm: i64,
        fees: FeeSchedule,
    ) -> bool {
        if signed_tfi >= (1.0 - 0.20 * INV_PHI) {
            return false;
        }
        let radius = self.elastic_wall_veto_radius_bps_dynamic(spec, mid_px, vol_5m, taker_prob_pm, fees).raw();
        let min_sc = INV_PHI * (self.effective_efficacy_pm() as f64 * 1e-3);
        match side {
            Side::Bid => {
                self.bear_wall_ticks.0 > 0
                    && self.bear_ob_score.get() >= min_sc
                    && self.dist_bear_ob_bps.raw() >= 0
                    && self.dist_bear_ob_bps.raw() <= radius
            }
            Side::Ask => {
                self.bull_wall_ticks.0 > 0
                    && self.bull_ob_score.get() >= min_sc
                    && self.dist_bull_ob_bps.raw() >= 0
                    && self.dist_bull_ob_bps.raw() <= radius
            }
        }
    }

    #[inline]
    #[must_use]
    pub fn aligned_confluence_resonance(self, side: Side, spec: PairSpec, mid_px: PriceTicks, vol_5m: ScaledBps) -> f64 {
        let (w_t, d_bps, sc) = match side {
            Side::Bid => (self.bull_wall_ticks, self.dist_bull_ob_bps.raw(), self.bull_ob_score.get()),
            Side::Ask => (self.bear_wall_ticks, self.dist_bear_ob_bps.raw(), self.bear_ob_score.get()),
        };
        if w_t.0 <= 0 || sc <= 0.05 {
            return 0.0;
        }
        let span = self.elastic_wall_veto_radius_bps(spec, mid_px, vol_5m).raw().max(400) as f64;
        let d = d_bps as f64;
        if d < -INV_PHI * span || d > PHI * span {
            return 0.0;
        }
        let z = d.max(0.0) / (INV_PHI * span);
        (sc * (-0.5 * z * z).exp()).clamp(0.0, 1.0)
    }

    /// Elastic Maker front-run TP ahead of opposing OB wall parameterized by live `FeeSchedule`.
    #[inline]
    #[must_use]
    pub fn front_run_opposing_wall_tp_with_fees(
        self,
        open_side: Side,
        spec: PairSpec,
        avg_entry: PriceTicks,
        cand_tp: PriceTicks,
        fees: FeeSchedule,
    ) -> PriceTicks {
        let tau_bps = spec.tick_to_price_scaled_bps(avg_entry).unwrap_or(ScaledBps(100)).raw().max(1);
        let mkr_abs = fees.maker_fee_bps.raw().abs().max(50);
        let pad_ticks = (mkr_abs / tau_bps).clamp(2, 12);
        let min_sc = INV_PHI * 0.80;
        match open_side {
            Side::Bid => {
                let w = self.bear_wall_ticks.0;
                if w > avg_entry.0 + pad_ticks + 1 && self.bear_ob_score.get() >= min_sc && cand_tp.0 >= w {
                    PriceTicks(w - pad_ticks)
                } else {
                    cand_tp
                }
            }
            Side::Ask => {
                let w = self.bull_wall_ticks.0;
                if w > 0 && w + pad_ticks + 1 < avg_entry.0 && self.bull_ob_score.get() >= min_sc && cand_tp.0 <= w {
                    PriceTicks(w + pad_ticks)
                } else {
                    cand_tp
                }
            }
        }
    }

    #[inline]
    #[must_use]
    pub fn front_run_opposing_wall_tp(
        self,
        open_side: Side,
        spec: PairSpec,
        avg_entry: PriceTicks,
        cand_tp: PriceTicks,
    ) -> PriceTicks {
        self.front_run_opposing_wall_tp_with_fees(open_side, spec, avg_entry, cand_tp, FeeSchedule::VIP0_DEFAULT)
    }
}

/// Decoupled directional Order Block plasticity (`[OB+ Demand, OB- Supply]` in `[382..=854] permille`).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct DirectionalObPlasticity {
    bull_demand_pm: i64,
    bear_supply_pm: i64,
    samples_seen: u64,
}

impl Default for DirectionalObPlasticity {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl DirectionalObPlasticity {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            bull_demand_pm: OB_DIRECTIONAL_SEED_PM,
            bear_supply_pm: OB_DIRECTIONAL_SEED_PM,
            samples_seen: 0,
        }
    }

    #[inline]
    #[must_use]
    pub const fn directional_efficacy_pm(&self) -> (i64, i64) {
        (self.bull_demand_pm, self.bear_supply_pm)
    }

    #[inline]
    #[must_use]
    pub const fn midpoint_efficacy_pm(&self) -> i64 {
        i64::midpoint(self.bull_demand_pm, self.bear_supply_pm)
    }

    #[inline]
    pub fn set_directional_efficacy_pm(&mut self, bull_pm: i64, bear_pm: i64) {
        self.bull_demand_pm = bull_pm.clamp(OB_DIRECTIONAL_MIN_PM, OB_DIRECTIONAL_MAX_PM);
        self.bear_supply_pm = bear_pm.clamp(OB_DIRECTIONAL_MIN_PM, OB_DIRECTIONAL_MAX_PM);
    }

    #[inline]
    pub fn observe_directional_outcome(&mut self, is_bull_demand_ob: bool, held_and_bounced: bool) {
        let reward_pm = if held_and_bounced { OB_DIRECTIONAL_MAX_PM } else { OB_DIRECTIONAL_MIN_PM };
        if is_bull_demand_ob {
            let next = ((1_000 - OB_PLASTICITY_ETA_PM) * self.bull_demand_pm + OB_PLASTICITY_ETA_PM * reward_pm) / 1_000;
            self.bull_demand_pm = next.clamp(OB_DIRECTIONAL_MIN_PM, OB_DIRECTIONAL_MAX_PM);
        } else {
            let next = ((1_000 - OB_PLASTICITY_ETA_PM) * self.bear_supply_pm + OB_PLASTICITY_ETA_PM * reward_pm) / 1_000;
            self.bear_supply_pm = next.clamp(OB_DIRECTIONAL_MIN_PM, OB_DIRECTIONAL_MAX_PM);
        }
        self.samples_seen = self.samples_seen.saturating_add(1);
    }
}

#[inline]
#[must_use]
pub const fn compute_distance_weighted_zone_utility(strength_pm: i64, dist_bps: ScaledBps, vol_5m: ScaledBps) -> i64 {
    let s_pos = if strength_pm > 0 { strength_pm } else { 0 };
    let d_pos = if dist_bps.0 > 0 { dist_bps.0 } else { 0 };
    let v_eff = if vol_5m.0 > 1_500 { vol_5m.0 } else { 1_500 };
    let dist_pen = (1_000_i64.saturating_mul(d_pos)) / v_eff;
    (s_pos.saturating_mul(1_000)) / 1_000_i64.saturating_add(dist_pen)
}

#[inline]
#[must_use]
pub fn select_distance_weighted_eviction_slot(
    strengths_pm: &[i64; 4],
    dists_bps: &[ScaledBps; 4],
    vol_5m: ScaledBps,
) -> usize {
    let mut min_idx = 0_usize;
    let mut min_util = i64::MAX;
    for (idx, (&st, &ds)) in strengths_pm.iter().zip(dists_bps.iter()).enumerate() {
        if st <= 0 {
            return idx;
        }
        let u = compute_distance_weighted_zone_utility(st, ds, vol_5m);
        if u < min_util {
            min_util = u;
            min_idx = idx;
        }
    }
    min_idx
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct ProximityObZone {
    pub low_ticks: PriceTicks,
    pub high_ticks: PriceTicks,
    pub strength_pm: i64,
    pub created_ts: TimestampNs,
}

#[derive(Copy, Clone, Debug, Default)]
pub struct ProximityObBank {
    pub bull_zones: [ProximityObZone; 4],
    pub bear_zones: [ProximityObZone; 4],
    pub plasticity: DirectionalObPlasticity,
}

impl ProximityObBank {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        let z = ProximityObZone {
            low_ticks: PriceTicks(0),
            high_ticks: PriceTicks(0),
            strength_pm: 0,
            created_ts: TimestampNs(0),
        };
        Self { bull_zones: [z; 4], bear_zones: [z; 4], plasticity: DirectionalObPlasticity::new() }
    }

    #[inline]
    #[must_use]
    pub const fn directional_efficacy_pm(&self) -> (i64, i64) {
        self.plasticity.directional_efficacy_pm()
    }

    #[inline]
    pub fn set_directional_efficacy_pm(&mut self, bull_pm: i64, bear_pm: i64) {
        self.plasticity.set_directional_efficacy_pm(bull_pm, bear_pm);
    }

    pub fn insert_zone_distance_weighted(
        &mut self,
        is_bull_demand: bool,
        zone: ProximityObZone,
        live_mid: PriceTicks,
        vol_5m: ScaledBps,
    ) -> usize {
        let arr = if is_bull_demand { &mut self.bull_zones } else { &mut self.bear_zones };
        let mid = live_mid.0.max(1);
        let mut strengths = [0_i64; 4];
        let mut dists = [ScaledBps(0); 4];
        for (i, z) in arr.iter().enumerate() {
            strengths[i] = z.strength_pm;
            let ref_edge = if is_bull_demand { z.high_ticks.0 } else { z.low_ticks.0 };
            dists[i] = ScaledBps((((live_mid.0 - ref_edge).abs() as i128 * 1_000_000) / (mid as i128)) as i64);
        }
        let target_idx = select_distance_weighted_eviction_slot(&strengths, &dists, vol_5m);
        arr[target_idx] = zone;
        target_idx
    }
}

#[derive(Copy, Clone, Debug, Default)]
struct OhlcBar {
    bucket_id: i64,
    open: PriceTicks,
    high: PriceTicks,
    low: PriceTicks,
    close: PriceTicks,
    turnover_1e8: i64,
    end_oi_x100: i64,
}

#[derive(Copy, Clone, Debug)]
struct BarRing {
    bars: [OhlcBar; BAR_RING_CAP],
    len: usize,
    cur: OhlcBar,
    has_cur: bool,
}

impl BarRing {
    const fn new() -> Self {
        let z = OhlcBar {
            bucket_id: 0,
            open: PriceTicks(0),
            high: PriceTicks(0),
            low: PriceTicks(0),
            close: PriceTicks(0),
            turnover_1e8: 0,
            end_oi_x100: 0,
        };
        Self { bars: [z; BAR_RING_CAP], len: 0, cur: z, has_cur: false }
    }

    fn push_closed(&mut self, bar: OhlcBar) {
        if self.len < BAR_RING_CAP {
            self.bars[self.len] = bar;
            self.len += 1;
        } else {
            for i in 0..(BAR_RING_CAP - 1) {
                self.bars[i] = self.bars[i + 1];
            }
            self.bars[BAR_RING_CAP - 1] = bar;
        }
    }

    fn step_tick(&mut self, bucket: i64, px: PriceTicks, turn_1e8: i64, oi_x100: i64) -> bool {
        if !self.has_cur {
            self.cur = OhlcBar {
                bucket_id: bucket, open: px, high: px, low: px, close: px,
                turnover_1e8: turn_1e8.max(1), end_oi_x100: oi_x100,
            };
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
        self.cur = OhlcBar {
            bucket_id: bucket, open: px, high: px, low: px, close: px,
            turnover_1e8: turn_1e8.max(1), end_oi_x100: oi_x100,
        };
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
    last_vol_5m: ScaledBps,
    learned_efficacy_pm: i64,
    plasticity: DirectionalObPlasticity,
    cached_obs: OrderBlockObservables,
}

impl Default for CausalOrderBlockEngine {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl CausalOrderBlockEngine {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        let ez = OrderBlockZone {
            state: ObLifecycleState::Empty,
            fuel: ObFuelKind::FreshOpenInterest,
            horizon: ObHorizon::Tactical1m,
            bounce_verified: false,
            low_ticks: PriceTicks(0),
            high_ticks: PriceTicks(0),
            eq_ticks: PriceTicks(0),
            strength_pm: 0,
        };
        Self {
            bull_zones: [ez; MAX_OB_ZONES_PER_SIDE],
            bear_zones: [ez; MAX_OB_ZONES_PER_SIDE],
            ring_1m: BarRing::new(),
            ring_5m: BarRing::new(),
            last_oi_x100: 0,
            last_vol_5m: ScaledBps(5_000),
            learned_efficacy_pm: SEED_EFFICACY_PM,
            plasticity: DirectionalObPlasticity::new(),
            cached_obs: OrderBlockObservables {
                bull_ob_score: BoundedScore::ZERO,
                bear_ob_score: BoundedScore::ZERO,
                dist_bull_ob_bps: ScaledBps(0),
                dist_bear_ob_bps: ScaledBps(0),
                bull_wall_ticks: PriceTicks(0),
                bear_wall_ticks: PriceTicks(0),
                learned_efficacy_pm: SEED_EFFICACY_PM,
            },
        }
    }

    #[inline]
    pub fn reset(&mut self) {
        *self = Self::new();
    }

    #[inline]
    #[must_use]
    pub const fn observables(&self) -> OrderBlockObservables {
        self.cached_obs
    }

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

    #[inline]
    #[must_use]
    pub const fn directional_efficacy_pm(&self) -> (i64, i64) {
        self.plasticity.directional_efficacy_pm()
    }

    #[inline]
    pub fn set_directional_efficacy_pm(&mut self, bull_pm: i64, bear_pm: i64) {
        self.plasticity.set_directional_efficacy_pm(bull_pm, bear_pm);
        self.set_learned_efficacy_permille(self.plasticity.midpoint_efficacy_pm());
    }

    #[inline]
    pub fn set_realized_vol_5m(&mut self, vol_5m: ScaledBps) {
        if vol_5m.0 > 0 {
            self.last_vol_5m = ScaledBps(vol_5m.0.clamp(1_500, 24_000));
        }
    }

    pub fn reset_preserving_plasticity(&mut self) {
        let keep_pm = self.learned_efficacy_pm;
        let keep_plast = self.plasticity;
        *self = Self::new();
        self.plasticity = keep_plast;
        self.set_learned_efficacy_permille(keep_pm);
    }

    pub fn save_plasticity_array_to_path<const N: usize>(engines: &[Self; N], path: &std::path::Path, ts_ns: i64) {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let vals: Vec<i64> = engines.iter().map(Self::learned_efficacy_permille).collect();
        let payload = serde_json::json!({ "event": "OB_PLASTICITY_V46", "ts_ns": ts_ns, "learned_efficacy_pm": vals });
        if let Ok(txt) = serde_json::to_string(&payload) {
            let _ = crate::vault_io::atomic_write_bytes_sync(path, txt.as_bytes());
        }
    }

    #[must_use]
    pub fn load_plasticity_array_from_path<const N: usize>(engines: &mut [Self; N], path: &std::path::Path) -> usize {
        let Ok(raw) = std::fs::read_to_string(path) else { return 0 };
        let Ok(val) = serde_json::from_str::<serde_json::Value>(&raw) else { return 0 };
        let Some(arr) = val.get("learned_efficacy_pm").and_then(serde_json::Value::as_array) else { return 0 };
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

    #[inline]
    pub fn observe_open_interest(&mut self, oi_contracts: f64) {
        if oi_contracts.is_finite() && oi_contracts > 0.0 {
            self.last_oi_x100 = (oi_contracts * 100.0).round() as i64;
        }
    }

    fn insert_zone(
        slots: &mut [OrderBlockZone; MAX_OB_ZONES_PER_SIDE],
        is_bull_demand: bool,
        zone: OrderBlockZone,
        live_mid: PriceTicks,
        vol_5m: ScaledBps,
    ) {
        for s in &mut *slots {
            if s.state == ObLifecycleState::Empty {
                *s = zone;
                return;
            }
        }
        let mid = live_mid.0.max(1);
        let mut strengths = [0_i64; MAX_OB_ZONES_PER_SIDE];
        let mut dists = [ScaledBps(0); MAX_OB_ZONES_PER_SIDE];
        for (i, z) in slots.iter().enumerate() {
            strengths[i] = z.strength_pm;
            let ref_edge = if is_bull_demand { z.high_ticks.0 } else { z.low_ticks.0 };
            dists[i] = ScaledBps((((mid - ref_edge).abs() as i128 * 1_000_000) / (mid as i128)) as i64);
        }
        let evict_idx = select_distance_weighted_eviction_slot(&strengths, &dists, vol_5m);
        slots[evict_idx] = zone;
    }

    fn detect_from_ring(&mut self, is_5m: bool) {
        let ring = if is_5m { &self.ring_5m } else { &self.ring_1m };
        if ring.len < 7 {
            return;
        }
        let n = ring.len;
        let (b0, b1, b2) = (ring.bars[n - 3], ring.bars[n - 2], ring.bars[n - 1]);
        let hist_n = n - 3;
        let (mut sum_r, mut sum_t) = (0.0_f64, 0.0_f64);
        for i in 0..hist_n {
            sum_r += (ring.bars[i].high.0 - ring.bars[i].low.0).max(1) as f64;
            sum_t += ring.bars[i].turnover_1e8.max(1) as f64;
        }
        let avg_r = (sum_r / (hist_n as f64)).max(1.0);
        let turn_ratio = (b0.turnover_1e8.max(b1.turnover_1e8).max(b2.turnover_1e8) as f64)
            / (sum_t / (hist_n as f64)).max(1.0);
        if turn_ratio < (1.0 + 0.25 * INV_PHI) {
            return;
        }
        let fuel = if b2.end_oi_x100 >= b0.end_oi_x100 { ObFuelKind::FreshOpenInterest } else { ObFuelKind::SqueezeLiquidation };
        let horizon = if is_5m { ObHorizon::Structural5m } else { ObHorizon::Tactical1m };
        let h_w = if is_5m { 1.0 } else { f64::midpoint(1.0, INV_PHI) };
        let f_w = if matches!(fuel, ObFuelKind::FreshOpenInterest) { 1.0 } else { 1.0 - 0.15 * INV_PHI };
        let bar_vol_bps = ((avg_r * 2.236 * 1_000_000.0) / (b2.close.0.max(1) as f64)).round() as i64;
        let eff_vol = ScaledBps(self.last_vol_5m.0.max(bar_vol_bps).max(1_500));
        match b0.close.0.cmp(&b0.open.0) {
            Ordering::Less => {
                let disp = (b1.high.0.max(b2.high.0) - b0.close.0).max(0) as f64;
                if b1.close.0.max(b2.close.0) > b0.high.0
                    && (b2.low.0 > b0.high.0 || disp >= (PHI + INV_PHI) * avg_r)
                    && disp >= PHI * avg_r
                {
                    let raw_s = ((disp / (3.0 * avg_r)).min(1.0) * 650.0 + (turn_ratio / 3.0).min(1.0) * 350.0) * h_w * f_w;
                    Self::insert_zone(
                        &mut self.bull_zones,
                        true,
                        OrderBlockZone {
                            state: ObLifecycleState::Virgin, fuel, horizon, bounce_verified: false,
                            low_ticks: b0.low, high_ticks: b0.open,
                            eq_ticks: PriceTicks(i64::midpoint(b0.low.0, b0.open.0)),
                            strength_pm: (raw_s.round() as i64).clamp(MIN_EFFICACY_PM, 1_000),
                        },
                        b2.close,
                        eff_vol,
                    );
                }
            }
            Ordering::Greater => {
                let disp = (b0.close.0 - b1.low.0.min(b2.low.0)).max(0) as f64;
                if b1.close.0.min(b2.close.0) < b0.low.0
                    && (b2.high.0 < b0.low.0 || disp >= (PHI + INV_PHI) * avg_r)
                    && disp >= PHI * avg_r
                {
                    let raw_s = ((disp / (3.0 * avg_r)).min(1.0) * 650.0 + (turn_ratio / 3.0).min(1.0) * 350.0) * h_w * f_w;
                    Self::insert_zone(
                        &mut self.bear_zones,
                        false,
                        OrderBlockZone {
                            state: ObLifecycleState::Virgin, fuel, horizon, bounce_verified: false,
                            low_ticks: b0.open, high_ticks: b0.high,
                            eq_ticks: PriceTicks(i64::midpoint(b0.open.0, b0.high.0)),
                            strength_pm: (raw_s.round() as i64).clamp(MIN_EFFICACY_PM, 1_000),
                        },
                        b2.close,
                        eff_vol,
                    );
                }
            }
            Ordering::Equal => {}
        }
    }

    fn step_mitigation_and_plasticity(&mut self, spec: PairSpec, px: PriceTicks) {
        if px.0 <= 0 {
            return;
        }
        let b3x = spec.required_3x_barrier_bps(px).unwrap_or(ScaledBps::MIN_REQUIRED_VOL_5M_BPS).raw();
        let mut bull_b = (PriceTicks(0), i64::MAX, 0.0_f64);
        for z in &mut self.bull_zones {
            if z.state == ObLifecycleState::Empty { continue; }
            if px.0 < z.low_ticks.0 || (z.state == ObLifecycleState::MitigatedShallow && px.0 < z.eq_ticks.0) {
                if z.state == ObLifecycleState::MitigatedShallow && !z.bounce_verified {
                    self.plasticity.observe_directional_outcome(true, false);
                    self.learned_efficacy_pm = ((self.learned_efficacy_pm * 88 + MIN_EFFICACY_PM * 12) / 100)
                        .clamp(MIN_EFFICACY_PM, MAX_EFFICACY_PM);
                }
                z.state = ObLifecycleState::Empty;
                continue;
            }
            if px.0 <= z.high_ticks.0 {
                z.state = ObLifecycleState::MitigatedShallow;
            } else if z.state == ObLifecycleState::MitigatedShallow && !z.bounce_verified {
                let reb = ((px.0 - z.high_ticks.0).saturating_mul(1_000_000)) / z.high_ticks.0.max(1);
                if reb >= b3x {
                    z.bounce_verified = true;
                    self.plasticity.observe_directional_outcome(true, true);
                    self.learned_efficacy_pm = ((self.learned_efficacy_pm * 85 + MAX_EFFICACY_PM * 15) / 100)
                        .clamp(MIN_EFFICACY_PM, MAX_EFFICACY_PM);
                }
            }
            let (bull_pm, _) = self.plasticity.directional_efficacy_pm();
            let eff_bull_pm = bull_pm.max(self.learned_efficacy_pm);
            let dist = ((px.0 - z.high_ticks.0).saturating_mul(1_000_000)) / px.0.max(1);
            let sc = ((z.strength_pm as f64) * 1e-3)
                * (if z.state == ObLifecycleState::Virgin { 1.0 } else { INV_PHI })
                * ((eff_bull_pm as f64) / (SEED_EFFICACY_PM as f64));
            if dist >= -b3x && dist < bull_b.1 {
                bull_b = (z.high_ticks, dist, sc.clamp(0.0, 1.0));
            }
        }
        let mut bear_b = (PriceTicks(0), i64::MAX, 0.0_f64);
        for z in &mut self.bear_zones {
            if z.state == ObLifecycleState::Empty { continue; }
            if px.0 > z.high_ticks.0 || (z.state == ObLifecycleState::MitigatedShallow && px.0 > z.eq_ticks.0) {
                if z.state == ObLifecycleState::MitigatedShallow && !z.bounce_verified {
                    self.plasticity.observe_directional_outcome(false, false);
                    self.learned_efficacy_pm = ((self.learned_efficacy_pm * 88 + MIN_EFFICACY_PM * 12) / 100)
                        .clamp(MIN_EFFICACY_PM, MAX_EFFICACY_PM);
                }
                z.state = ObLifecycleState::Empty;
                continue;
            }
            if px.0 >= z.low_ticks.0 {
                z.state = ObLifecycleState::MitigatedShallow;
            } else if z.state == ObLifecycleState::MitigatedShallow && !z.bounce_verified {
                let reb = ((z.low_ticks.0 - px.0).saturating_mul(1_000_000)) / z.low_ticks.0.max(1);
                if reb >= b3x {
                    z.bounce_verified = true;
                    self.plasticity.observe_directional_outcome(false, true);
                    self.learned_efficacy_pm = ((self.learned_efficacy_pm * 85 + MAX_EFFICACY_PM * 15) / 100)
                        .clamp(MIN_EFFICACY_PM, MAX_EFFICACY_PM);
                }
            }
            let (_, bear_pm) = self.plasticity.directional_efficacy_pm();
            let eff_bear_pm = bear_pm.max(self.learned_efficacy_pm);
            let dist = ((z.low_ticks.0 - px.0).saturating_mul(1_000_000)) / px.0.max(1);
            let sc = ((z.strength_pm as f64) * 1e-3)
                * (if z.state == ObLifecycleState::Virgin { 1.0 } else { INV_PHI })
                * ((eff_bear_pm as f64) / (SEED_EFFICACY_PM as f64));
            if dist >= -b3x && dist < bear_b.1 {
                bear_b = (z.low_ticks, dist, sc.clamp(0.0, 1.0));
            }
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
        if mid_px.0 > 0 {
            self.step_mitigation_and_plasticity(spec, mid_px);
        }
        self.cached_obs
    }

    pub fn observe_live_tick(&mut self, spec: PairSpec, price: PriceTicks, qty: LotQty, ts_ns: i64) -> OrderBlockObservables {
        if price.0 <= 0 || ts_ns <= 0 {
            return self.cached_obs;
        }
        let turn_1e8 = spec.lot_notional_1e8(LotQty(qty.0.max(1)), price);
        let oi = self.last_oi_x100;
        if self.ring_1m.step_tick(ts_ns / 60_000_000_000, price, turn_1e8, oi) {
            self.detect_from_ring(false);
        }
        if self.ring_5m.step_tick(ts_ns / 300_000_000_000, price, turn_1e8, oi) {
            self.detect_from_ring(true);
        }
        self.step_mitigation_and_plasticity(spec, price);
        self.cached_obs
    }

    pub fn ingest_closed_bar(
        &mut self,
        spec: PairSpec,
        is_5m: bool,
        bucket_id: i64,
        open: PriceTicks,
        high: PriceTicks,
        low: PriceTicks,
        close: PriceTicks,
        turnover_1e8: i64,
        oi_x100: i64,
    ) {
        if open.0 <= 0 || high.0 < low.0 || low.0 <= 0 || close.0 <= 0 {
            return;
        }
        let bar = OhlcBar { bucket_id, open, high, low, close, turnover_1e8: turnover_1e8.max(1), end_oi_x100: oi_x100 };
        if is_5m { self.ring_5m.push_closed(bar); } else { self.ring_1m.push_closed(bar); }
        self.detect_from_ring(is_5m);
        self.step_mitigation_and_plasticity(spec, close);
    }

    pub fn warm_start_from_bybit_bundle(
        &mut self,
        spec: PairSpec,
        kline_5m: Option<&serde_json::Value>,
        kline_1m: Option<&serde_json::Value>,
        oi_5m: Option<&serde_json::Value>,
    ) -> usize {
        let mut oi_pts = [(0_i64, 0_i64); 64];
        let mut oi_len = 0_usize;
        if let Some(rows) = oi_5m.and_then(|v| v.get("result")).and_then(|r| r.get("list")).and_then(serde_json::Value::as_array) {
            for item in rows.iter().rev().take(64) {
                let ts = item.get("timestamp").and_then(serde_json::Value::as_str).and_then(|s| s.parse::<i64>().ok()).unwrap_or(0);
                if let Some(of) = item.get("openInterest").and_then(serde_json::Value::as_str).and_then(|s| s.parse::<f64>().ok()) {
                    self.observe_open_interest(of);
                    oi_pts[oi_len] = (ts, self.last_oi_x100);
                    oi_len += 1;
                }
            }
        }
        let mut count = 0_usize;
        for (is_5m, k_opt) in [(true, kline_5m), (false, kline_1m)] {
            let Some(rows) = k_opt.and_then(|v| v.get("result")).and_then(|r| r.get("list")).and_then(serde_json::Value::as_array) else { continue };
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
                for i in 0..oi_len {
                    if oi_pts[i].0 <= ts_ms { bar_oi = oi_pts[i].1; }
                }
                let turn_f = arr[6].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(100.0);
                let div_ms = if is_5m { 300_000 } else { 60_000 };
                self.ingest_closed_bar(spec, is_5m, ts_ms / div_ms, op, hi, lo, cl, (turn_f.max(1.0) * 1e8).round() as i64, bar_oi);
                count += 1;
            }
        }
        count
    }
}

impl crate::lob_shm::SymbolLobSnapshot {
    #[inline]
    pub fn set_order_block_observables_directional(&mut self, obs: OrderBlockObservables, bull_pm: i64, bear_pm: i64) {
        let b_clamped = bull_pm.clamp(OB_DIRECTIONAL_MIN_PM, OB_DIRECTIONAL_MAX_PM);
        let s_clamped = bear_pm.clamp(OB_DIRECTIONAL_MIN_PM, OB_DIRECTIONAL_MAX_PM);
        self.set_order_block_observables(obs);
        self.ob_bull_efficacy_pm = b_clamped;
        self.ob_bear_efficacy_pm = s_clamped;
        self.ob_learned_efficacy_pm = i64::midpoint(b_clamped, s_clamped);
    }
}

#[inline]
#[must_use]
pub fn passes_directional_ob_wall_guard(side: Side, snap: &crate::lob_shm::SymbolLobSnapshot, three_x_barrier: ScaledBps) -> bool {
    let tfi_aligned = side.sign_f64() * (f64::from(snap.factor_tfi.to_bps_i32()) / 10_000.0);
    if tfi_aligned >= 0.88 {
        return true;
    }
    let base_r = (snap.realized_vol_5m.raw() / 5).clamp((three_x_barrier.raw() * 2) / 3, three_x_barrier.raw().saturating_mul(2));
    match side {
        Side::Bid => {
            if snap.ob_bear_wall_ticks.0 <= 0 || snap.ob_bear_score.to_bps_i32() < 4_500 { return true; }
            let eff_r = (base_r.saturating_mul(snap.ob_bear_efficacy_pm)) / OB_DIRECTIONAL_SEED_PM;
            snap.ob_dist_bear_bps.raw() > eff_r.max(400)
        }
        Side::Ask => {
            if snap.ob_bull_wall_ticks.0 <= 0 || snap.ob_bull_score.to_bps_i32() < 4_500 { return true; }
            let eff_r = (base_r.saturating_mul(snap.ob_bull_efficacy_pm)) / OB_DIRECTIONAL_SEED_PM;
            snap.ob_dist_bull_bps.raw() > eff_r.max(400)
        }
    }
}

#[must_use]
pub fn load_directional_plasticity_from_path(path: &std::path::Path) -> ([i64; 12], [i64; 12]) {
    let mut bull = [OB_DIRECTIONAL_SEED_PM; 12];
    let mut bear = [OB_DIRECTIONAL_SEED_PM; 12];
    let Ok(txt) = std::fs::read_to_string(path) else { return (bull, bear) };
    let Ok(val) = serde_json::from_str::<serde_json::Value>(&txt) else { return (bull, bear) };
    if let Some(legacy) = val.get("learned_efficacy_pm").and_then(serde_json::Value::as_array) {
        for (i, item) in legacy.iter().take(12).enumerate() {
            if let Some(v) = item.as_i64() {
                let cl = v.clamp(OB_DIRECTIONAL_MIN_PM, OB_DIRECTIONAL_MAX_PM);
                bull[i] = cl;
                bear[i] = cl;
            }
        }
    }
    if let Some(b_arr) = val.get("learned_bull_pm").and_then(serde_json::Value::as_array) {
        for (i, item) in b_arr.iter().take(12).enumerate() {
            if let Some(v) = item.as_i64() { bull[i] = v.clamp(OB_DIRECTIONAL_MIN_PM, OB_DIRECTIONAL_MAX_PM); }
        }
    }
    if let Some(s_arr) = val.get("learned_bear_pm").and_then(serde_json::Value::as_array) {
        for (i, item) in s_arr.iter().take(12).enumerate() {
            if let Some(v) = item.as_i64() { bear[i] = v.clamp(OB_DIRECTIONAL_MIN_PM, OB_DIRECTIONAL_MAX_PM); }
        }
    }
    (bull, bear)
}

pub fn save_directional_plasticity_to_path_atomic(
    path: &std::path::Path,
    ts_ns: i64,
    bull_pm: &[i64; 12],
    bear_pm: &[i64; 12],
) -> Result<(), crate::types::VortexError> {
    let mut mean_pm = [0_i64; 12];
    for (i, m) in mean_pm.iter_mut().enumerate() {
        *m = i64::midpoint(bull_pm[i], bear_pm[i]);
    }
    let payload = serde_json::json!({
        "event": "OB_PLASTICITY_V46",
        "ts_ns": ts_ns,
        "learned_efficacy_pm": mean_pm,
        "learned_bull_pm": bull_pm,
        "learned_bear_pm": bear_pm,
    });
    let s = serde_json::to_string(&payload).map_err(|_| crate::types::VortexError::ColumnarFormatCorrupted)?;
    crate::vault_io::atomic_write_bytes_sync(path, s.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lob_shm::SymbolLobSnapshot;
    use crate::types::APPROVED_SPECS;

    #[test]
    fn test_ob_bullish_demand_self_learning_wall_veto_and_zero_ms_invalidation() {
        let avax = APPROVED_SPECS[3];
        let vol = ScaledBps(6_200);
        let mut eng = CausalOrderBlockEngine::new();
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
        assert_eq!(near_obs.front_run_opposing_wall_tp(Side::Ask, avax, PriceTicks(7_520), PriceTicks(7_495)), PriceTicks(7_502));
        let _ = eng.observe_live_tick(avax, PriceTicks(7_498), LotQty(20), 12 * 60_000_000_000 + 5_000_000_000);
        let bounced = eng.observe_live_tick(avax, PriceTicks(7_520), LotQty(30), 12 * 60_000_000_000 + 15_000_000_000);
        assert!(bounced.learned_efficacy_pm > SEED_EFFICACY_PM);
        assert!(eng.directional_efficacy_pm().0 > OB_DIRECTIONAL_SEED_PM);
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
        assert_eq!(obs.front_run_opposing_wall_tp(Side::Bid, btc, PriceTicks(658_000), PriceTicks(665_000)).0, 664_188);
    }

    #[test]
    fn test_ob_plasticity_vault_roundtrip_and_reconnect_preservation() {
        let tmp = std::env::temp_dir().join(format!("vortex_ob_plast_v46_{}.json", std::process::id()));
        let mut src = [CausalOrderBlockEngine::new(); 12];
        for (idx, eng) in src.iter_mut().enumerate() {
            eng.set_learned_efficacy_permille(500 + (idx as i64) * 25);
        }
        CausalOrderBlockEngine::save_plasticity_array_to_path(&src, &tmp, 1_794_690_000_000_000_000);
        let mut dst = [CausalOrderBlockEngine::new(); 12];
        let loaded = CausalOrderBlockEngine::load_plasticity_array_from_path(&mut dst, &tmp);
        let _ = std::fs::remove_file(&tmp);
        assert_eq!(loaded, 12);
        dst[3].reset_preserving_plasticity();
        assert_eq!(dst[3].learned_efficacy_permille(), 575);
    }

    #[test]
    fn test_decoupled_ob_bull_bear_plasticity_and_v46_engine_distance_eviction_and_vip_tp() {
        let mut plast = DirectionalObPlasticity::new();
        for _ in 0..10 { plast.observe_directional_outcome(true, false); }
        for _ in 0..6 { plast.observe_directional_outcome(false, true); }
        let (bull_after, bear_after) = plast.directional_efficacy_pm();
        assert!(bull_after < 460 && bear_after > 730);

        let mut snap = SymbolLobSnapshot::new_empty();
        snap.realized_vol_5m = ScaledBps(6_000);
        snap.set_order_block_observables_directional(
            OrderBlockObservables {
                bull_ob_score: BoundedScore::new_clamped(0.75),
                bear_ob_score: BoundedScore::new_clamped(0.75),
                dist_bull_ob_bps: ScaledBps(1_050),
                dist_bear_ob_bps: ScaledBps(1_350),
                bull_wall_ticks: PriceTicks(8_180),
                bear_wall_ticks: PriceTicks(8_220),
                learned_efficacy_pm: plast.midpoint_efficacy_pm(),
            },
            bull_after,
            bear_after,
        );
        assert!(!passes_directional_ob_wall_guard(Side::Bid, &snap, ScaledBps(1_300)));
        assert!(passes_directional_ob_wall_guard(Side::Ask, &snap, ScaledBps(1_300)));

        // V4.6 Island 2: CausalOrderBlockEngine internal distance-weighted eviction & VIP FeeSchedule front-run TP
        let btc = APPROVED_SPECS[0];
        let mut eng = CausalOrderBlockEngine::new();
        eng.set_realized_vol_5m(ScaledBps(5_000));
        CausalOrderBlockEngine::insert_zone(
            &mut eng.bull_zones,
            true,
            OrderBlockZone {
                state: ObLifecycleState::Virgin, fuel: ObFuelKind::FreshOpenInterest, horizon: ObHorizon::Structural5m,
                bounce_verified: false, low_ticks: PriceTicks(639_000), high_ticks: PriceTicks(640_000),
                eq_ticks: PriceTicks(639_500), strength_pm: 940,
            },
            PriceTicks(660_000),
            ScaledBps(5_000),
        );
        for (k, hi) in [658_800_i64, 659_100, 659_400].into_iter().enumerate() {
            CausalOrderBlockEngine::insert_zone(
                &mut eng.bull_zones,
                true,
                OrderBlockZone {
                    state: ObLifecycleState::Virgin, fuel: ObFuelKind::FreshOpenInterest, horizon: ObHorizon::Tactical1m,
                    bounce_verified: false, low_ticks: PriceTicks(hi - 100), high_ticks: PriceTicks(hi),
                    eq_ticks: PriceTicks(hi - 50), strength_pm: 720 + (k as i64) * 20,
                },
                PriceTicks(660_000),
                ScaledBps(5_000),
            );
        }
        CausalOrderBlockEngine::insert_zone(
            &mut eng.bull_zones,
            true,
            OrderBlockZone {
                state: ObLifecycleState::Virgin, fuel: ObFuelKind::FreshOpenInterest, horizon: ObHorizon::Tactical1m,
                bounce_verified: false, low_ticks: PriceTicks(659_700), high_ticks: PriceTicks(659_850),
                eq_ticks: PriceTicks(659_775), strength_pm: 750,
            },
            PriceTicks(660_000),
            ScaledBps(5_000),
        );
        // Slot 0 (stale 303 bps away) was evicted while all 3 near-BBO walls remained intact!
        assert_eq!(eng.bull_zones[0].high_ticks, PriceTicks(659_850));
        assert_eq!(eng.bull_zones[1].high_ticks, PriceTicks(658_800));

        let obs_btc = OrderBlockObservables {
            bull_ob_score: BoundedScore::ZERO,
            bear_ob_score: BoundedScore::new_clamped(0.85),
            dist_bull_ob_bps: ScaledBps(0),
            dist_bear_ob_bps: ScaledBps(450),
            bull_wall_ticks: PriceTicks(0),
            bear_wall_ticks: PriceTicks(664_200),
            learned_efficacy_pm: 680,
        };
        let vip0_tp = obs_btc.front_run_opposing_wall_tp_with_fees(
            Side::Bid, btc, PriceTicks(658_000), PriceTicks(665_000), FeeSchedule::VIP0_DEFAULT,
        );
        let vip_rebate_tp = obs_btc.front_run_opposing_wall_tp_with_fees(
            Side::Bid, btc, PriceTicks(658_000), PriceTicks(665_000),
            FeeSchedule::from_live_rates_clamped(ScaledBps(-50), ScaledBps(300)),
        );
        // Lower VIP fee / negative rebate pads closer to opposing wall without crossing it (664_197 vs 664_188)
        assert_eq!(vip0_tp, PriceTicks(664_188));
        assert!(vip_rebate_tp.0 > vip0_tp.0 && vip_rebate_tp.0 < 664_200);
    }
}
