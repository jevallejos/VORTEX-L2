//! VORTEX-OMEGA V4.6 Immortal 12-Pair WebSocket Ingestor Daemon (SHM v15 + PAR2 Lossless + Async VaultIO).
#![allow(
    clippy::manual_clamp,
    clippy::manual_range_contains,
    clippy::needless_range_loop,
    clippy::too_many_arguments
)]

use core::sync::atomic::Ordering;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::mpsc::SyncSender;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use vortex_rs::alpha_3factor::{EventClockAlphaModulator, OrthogonalFactorState};
use vortex_rs::bybit_client::parse_decimal_to_scaled_i64;
use vortex_rs::forensic_parquet::{ColumnarMarketRow, ColumnarParquetBatch, MarketEventKind};
#[cfg(target_os = "linux")]
use vortex_rs::http_ws_transport::{parse_endpoint_url, KeepAliveHttpConn};
use vortex_rs::lob_shm::{
    classify_l2_sequence_transition, is_l2_seq_or_basis_anomaly, BookLevel, L2SeqDecision,
    MappedShmRegion, SymbolLobSnapshot, VortexShmArena, MAX_ACTIVE_PAIRS,
    MAX_CONTIGUOUS_L2_SEQ_JUMP, VORTEX_SHM_VERSION,
};
use vortex_rs::order_block::{
    load_directional_plasticity_from_path, CausalOrderBlockEngine, DirectionalObPlasticity,
    OrderBlockObservables, OB_PLASTICITY_VAULT_PATH,
};
use vortex_rs::tap_ring::{
    inspect_live_shm_readonly, prune_expired_parquet_segment, run_isolated_tap_self_test,
    AsyncParquetRingWriter, SAMPLE_PRICES_TICKS, SAMPLE_VOL_5M_BPS,
};
use vortex_rs::types::{
    now_epoch_ns, slot_from_symbol_name, LotQty, PairSpec, PriceTicks, ScaledBps, Side,
    TimestampNs, VortexError, ANSI_GREEN, ANSI_RESET, APPROVED_SPECS,
};
use vortex_rs::vault_io::{spawn_vault_io_worker, AtomicLockWatch, VaultIoCommand};
use vortex_rs::wave_elliott::{
    parse_bybit_all_liquidation_event, CausalElliottWaveState, EndogenousLiquidationFloor,
};

#[cfg(target_os = "linux")]
const BYBIT_PUBLIC_LINEAR_WS: &str = "wss://stream.bybit.com/v5/public/linear";
#[cfg(target_os = "linux")]
const BYBIT_PUBLIC_REST_URL: &str = "https://api.bybit.com";
const TICKER_PAR2_MIN_INTERVAL_NS: i64 = 1_000_000_000;

#[derive(Debug)]
struct TapLiveState {
    factor_states: [OrthogonalFactorState; MAX_ACTIVE_PAIRS],
    event_clocks: [EventClockAlphaModulator; MAX_ACTIVE_PAIRS],
    wave_states: [CausalElliottWaveState; MAX_ACTIVE_PAIRS],
    liq_floors: [EndogenousLiquidationFloor; MAX_ACTIVE_PAIRS],
    ob_states: [CausalOrderBlockEngine; MAX_ACTIVE_PAIRS],
    ob_plasticity: [DirectionalObPlasticity; MAX_ACTIVE_PAIRS],
    prev_bull_wall: [PriceTicks; MAX_ACTIVE_PAIRS],
    prev_bear_wall: [PriceTicks; MAX_ACTIVE_PAIRS],
    bull_mitigated: [bool; MAX_ACTIVE_PAIRS],
    bear_mitigated: [bool; MAX_ACTIVE_PAIRS],
    active_snaps: [SymbolLobSnapshot; MAX_ACTIVE_PAIRS],
    has_live_mark_px: [bool; MAX_ACTIVE_PAIRS],
    has_book_snapshot: [bool; MAX_ACTIVE_PAIRS],
    last_ticker_par2_ns: [i64; MAX_ACTIVE_PAIRS],
    batch: Box<ColumnarParquetBatch>,
    vault_tx: Option<SyncSender<VaultIoCommand>>,
    last_ob_persist_ns: i64,
}

impl TapLiveState {
    #[must_use]
    fn new(enable_async_vault: bool) -> Self {
        let mut ob_states = [CausalOrderBlockEngine::new(); MAX_ACTIVE_PAIRS];
        let mut ob_plasticity = [DirectionalObPlasticity::new(); MAX_ACTIVE_PAIRS];
        if enable_async_vault {
            let (bull_arr, bear_arr) =
                load_directional_plasticity_from_path(Path::new(OB_PLASTICITY_VAULT_PATH));
            for s in 0..MAX_ACTIVE_PAIRS {
                ob_plasticity[s].set_directional_efficacy_pm(bull_arr[s], bear_arr[s]);
                ob_states[s]
                    .set_learned_efficacy_permille(ob_plasticity[s].midpoint_efficacy_pm());
            }
            println!(
                "{}\u{1f7e2} [OB PLASTICITY V4.6 REHYDRATE] Hydrated 12/12 directional [OB+, OB-] slots{}",
                ANSI_GREEN, ANSI_RESET
            );
        }
        let vault_tx = if enable_async_vault {
            Some(spawn_vault_io_worker(Arc::new(AtomicLockWatch::default())))
        } else {
            None
        };
        let mut st = Self {
            factor_states: [OrthogonalFactorState::new(); MAX_ACTIVE_PAIRS],
            event_clocks: [EventClockAlphaModulator::new(); MAX_ACTIVE_PAIRS],
            wave_states: [CausalElliottWaveState::new(); MAX_ACTIVE_PAIRS],
            liq_floors: [EndogenousLiquidationFloor::default(); MAX_ACTIVE_PAIRS],
            ob_states,
            ob_plasticity,
            prev_bull_wall: [PriceTicks(0); MAX_ACTIVE_PAIRS],
            prev_bear_wall: [PriceTicks(0); MAX_ACTIVE_PAIRS],
            bull_mitigated: [false; MAX_ACTIVE_PAIRS],
            bear_mitigated: [false; MAX_ACTIVE_PAIRS],
            active_snaps: [SymbolLobSnapshot::new_empty(); MAX_ACTIVE_PAIRS],
            has_live_mark_px: [false; MAX_ACTIVE_PAIRS],
            has_book_snapshot: [false; MAX_ACTIVE_PAIRS],
            last_ticker_par2_ns: [0; MAX_ACTIVE_PAIRS],
            batch: Box::<ColumnarParquetBatch>::default(),
            vault_tx,
            last_ob_persist_ns: 0,
        };
        st.reset_session_preserving_plasticity();
        st
    }

    fn reset_session_preserving_plasticity(&mut self) {
        for s in 0..MAX_ACTIVE_PAIRS {
            let spec = APPROVED_SPECS[s];
            self.factor_states[s].reset();
            self.event_clocks[s] = EventClockAlphaModulator::new();
            self.wave_states[s].reset();
            self.liq_floors[s] =
                EndogenousLiquidationFloor::new_for_spec(spec, PriceTicks(SAMPLE_PRICES_TICKS[s]));
            self.ob_states[s].reset_preserving_plasticity();
            let (bull_pm, bear_pm) = self.ob_plasticity[s].directional_efficacy_pm();
            self.ob_states[s]
                .set_learned_efficacy_permille(self.ob_plasticity[s].midpoint_efficacy_pm());
            self.prev_bull_wall[s] = PriceTicks(0);
            self.prev_bear_wall[s] = PriceTicks(0);
            self.bull_mitigated[s] = false;
            self.bear_mitigated[s] = false;
            let prev_resyncs = self.active_snaps[s].seq_resync_count;
            self.active_snaps[s] = SymbolLobSnapshot::new_empty();
            self.active_snaps[s].symbol_id = spec.symbol_id;
            self.active_snaps[s].is_eligible = true;
            self.active_snaps[s].realized_vol_5m = ScaledBps(SAMPLE_VOL_5M_BPS[s]);
            self.active_snaps[s].seq_resync_count = prev_resyncs;
            self.active_snaps[s].set_order_block_observables_directional(
                self.ob_states[s].observables(),
                bull_pm,
                bear_pm,
            );
            self.has_live_mark_px[s] = false;
            self.has_book_snapshot[s] = false;
            self.last_ticker_par2_ns[s] = 0;
        }
    }

    #[inline]
    fn dispatch_ob_plasticity_async(&mut self, ts_ns: i64) {
        self.last_ob_persist_ns = ts_ns;
        let mut bull_pm = [0_i64; MAX_ACTIVE_PAIRS];
        let mut bear_pm = [0_i64; MAX_ACTIVE_PAIRS];
        for s in 0..MAX_ACTIVE_PAIRS {
            let (b_pm, a_pm) = self.ob_plasticity[s].directional_efficacy_pm();
            bull_pm[s] = b_pm;
            bear_pm[s] = a_pm;
        }
        if let Some(ref tx) = self.vault_tx {
            let _ = tx.try_send(VaultIoCommand::SaveObPlasticity {
                ts_ns,
                bull_pm,
                bear_pm,
            });
        }
    }

    #[inline]
    fn push_par2_row(
        &mut self,
        row: &ColumnarMarketRow,
        writer: Option<&AsyncParquetRingWriter>,
    ) {
        if self.batch.push_row(row).is_err() {
            if let Some(w) = writer {
                let _ = w.try_flush_and_clear(&mut self.batch);
            } else {
                self.batch.clear();
            }
            let _ = self.batch.push_row(row);
        }
    }

    #[inline]
    fn step_directional_ob_and_publish(
        &mut self,
        slot_idx: usize,
        spec: PairSpec,
        ref_px: PriceTicks,
        obs: OrderBlockObservables,
    ) {
        let b3x = spec
            .required_3x_barrier_bps(ref_px)
            .unwrap_or(ScaledBps::MIN_REQUIRED_VOL_5M_BPS)
            .raw();

        let prev_bw = self.prev_bull_wall[slot_idx];
        if prev_bw.0 > 0 {
            if obs.bull_wall_ticks == prev_bw {
                if obs.dist_bull_ob_bps.0 <= 0 {
                    self.bull_mitigated[slot_idx] = true;
                } else if self.bull_mitigated[slot_idx] && obs.dist_bull_ob_bps.0 >= b3x {
                    self.ob_plasticity[slot_idx].observe_directional_outcome(true, true);
                    self.bull_mitigated[slot_idx] = false;
                }
            } else {
                if self.bull_mitigated[slot_idx] && ref_px.0 < prev_bw.0 {
                    self.ob_plasticity[slot_idx].observe_directional_outcome(true, false);
                }
                self.bull_mitigated[slot_idx] =
                    obs.bull_wall_ticks.0 > 0 && obs.dist_bull_ob_bps.0 <= 0;
            }
        } else if obs.bull_wall_ticks.0 > 0 && obs.dist_bull_ob_bps.0 <= 0 {
            self.bull_mitigated[slot_idx] = true;
        }
        self.prev_bull_wall[slot_idx] = obs.bull_wall_ticks;

        let prev_sw = self.prev_bear_wall[slot_idx];
        if prev_sw.0 > 0 {
            if obs.bear_wall_ticks == prev_sw {
                if obs.dist_bear_ob_bps.0 <= 0 {
                    self.bear_mitigated[slot_idx] = true;
                } else if self.bear_mitigated[slot_idx] && obs.dist_bear_ob_bps.0 >= b3x {
                    self.ob_plasticity[slot_idx].observe_directional_outcome(false, true);
                    self.bear_mitigated[slot_idx] = false;
                }
            } else {
                if self.bear_mitigated[slot_idx] && ref_px.0 > prev_sw.0 {
                    self.ob_plasticity[slot_idx].observe_directional_outcome(false, false);
                }
                self.bear_mitigated[slot_idx] =
                    obs.bear_wall_ticks.0 > 0 && obs.dist_bear_ob_bps.0 <= 0;
            }
        } else if obs.bear_wall_ticks.0 > 0 && obs.dist_bear_ob_bps.0 <= 0 {
            self.bear_mitigated[slot_idx] = true;
        }
        self.prev_bear_wall[slot_idx] = obs.bear_wall_ticks;

        let (bull_pm, bear_pm) = self.ob_plasticity[slot_idx].directional_efficacy_pm();
        self.ob_states[slot_idx]
            .set_learned_efficacy_permille(self.ob_plasticity[slot_idx].midpoint_efficacy_pm());
        self.active_snaps[slot_idx]
            .set_order_block_observables_directional(obs, bull_pm, bear_pm);
    }

    /// Processes 1 Bybit V5 public WS frame; returns `true` iff a severe L2 sequence gap requires resync.
    fn ingest_public_ws_text(
        &mut self,
        txt: &str,
        local_ns: TimestampNs,
        arena: &VortexShmArena,
        writer: Option<&AsyncParquetRingWriter>,
    ) -> bool {
        let Ok(val) = serde_json::from_str::<serde_json::Value>(txt) else {
            return false;
        };
        let Some(topic) = val.get("topic").and_then(serde_json::Value::as_str) else {
            return false;
        };
        let exch_ms = val.get("ts").and_then(serde_json::Value::as_i64).unwrap_or(0);
        let exch_ns = TimestampNs(if exch_ms > 0 {
            exch_ms.saturating_mul(1_000_000)
        } else {
            local_ns.0
        });

        if let Some(sym_name) = topic.strip_prefix("orderbook.50.") {
            let Some(slot_idx) = slot_from_symbol_name(sym_name) else {
                return false;
            };
            let spec = APPROVED_SPECS[slot_idx];
            let is_snapshot = val
                .get("type")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|t| t == "snapshot");
            let Some(data) = val.get("data") else {
                return false;
            };
            let incoming_u = data.get("u").and_then(serde_json::Value::as_u64).unwrap_or(0);
            let prev_seq = self.active_snaps[slot_idx].sequence_id;
            match classify_l2_sequence_transition(
                self.has_book_snapshot[slot_idx],
                prev_seq,
                is_snapshot,
                incoming_u,
            ) {
                L2SeqDecision::ResetAndApplySnapshot => {
                    self.active_snaps[slot_idx].bids = [BookLevel::default(); 50];
                    self.active_snaps[slot_idx].asks = [BookLevel::default(); 50];
                    self.factor_states[slot_idx].reset_l2_book_only();
                    self.has_book_snapshot[slot_idx] = true;
                }
                L2SeqDecision::ApplyMonotonicDelta => {
                    if prev_seq > 0
                        && incoming_u > prev_seq
                        && (incoming_u - prev_seq) > MAX_CONTIGUOUS_L2_SEQ_JUMP
                    {
                        self.active_snaps[slot_idx].seq_resync_count =
                            self.active_snaps[slot_idx].seq_resync_count.saturating_add(1);
                        arena.tap_seq_resync_count.fetch_add(1, Ordering::Relaxed);
                        self.has_book_snapshot[slot_idx] = false;
                        return true;
                    }
                }
                L2SeqDecision::IgnorePreSnapshotOrStaleDelta => return false,
            }
            if incoming_u > 0 {
                self.active_snaps[slot_idx].sequence_id = incoming_u;
            }
            if let Some(bids) = data.get("b").and_then(serde_json::Value::as_array) {
                for arr in bids.iter().filter_map(serde_json::Value::as_array) {
                    if arr.len() >= 2 {
                        if let Ok(px) = spec.parse_price_ticks(arr[0].as_str().unwrap_or("")) {
                            let qty = spec.parse_lot_qty(arr[1].as_str().unwrap_or(""));
                            self.active_snaps[slot_idx].apply_level_update(Side::Bid, px, qty);
                        }
                    }
                }
            }
            if let Some(asks) = data.get("a").and_then(serde_json::Value::as_array) {
                for arr in asks.iter().filter_map(serde_json::Value::as_array) {
                    if arr.len() >= 2 {
                        if let Ok(px) = spec.parse_price_ticks(arr[0].as_str().unwrap_or("")) {
                            let qty = spec.parse_lot_qty(arr[1].as_str().unwrap_or(""));
                            self.active_snaps[slot_idx].apply_level_update(Side::Ask, px, qty);
                        }
                    }
                }
            }
            if self.active_snaps[slot_idx].bbo_and_spread_ticks().is_none() {
                return false;
            }
            let mid_l1 = PriceTicks(i64::midpoint(
                self.active_snaps[slot_idx].bids[0].price.0,
                self.active_snaps[slot_idx].asks[0].price.0,
            ));
            if !self.has_live_mark_px[slot_idx] || self.active_snaps[slot_idx].mark_price.0 <= 0 {
                self.active_snaps[slot_idx].mark_price = mid_l1;
            }
            if self.active_snaps[slot_idx].last_trade_price.0 <= 0 {
                self.active_snaps[slot_idx].last_trade_price = mid_l1;
            }
            let b3x = spec
                .required_3x_barrier_bps(mid_l1)
                .unwrap_or(ScaledBps::MIN_REQUIRED_VOL_5M_BPS);
            if self.has_live_mark_px[slot_idx]
                && is_l2_seq_or_basis_anomaly(
                    prev_seq,
                    incoming_u,
                    mid_l1,
                    self.active_snaps[slot_idx].mark_price,
                    self.active_snaps[slot_idx].realized_vol_5m,
                    b3x,
                )
            {
                self.active_snaps[slot_idx].seq_resync_count =
                    self.active_snaps[slot_idx].seq_resync_count.saturating_add(1);
                arena.tap_seq_resync_count.fetch_add(1, Ordering::Relaxed);
            }
            self.active_snaps[slot_idx].realized_vol_5m = self.factor_states[slot_idx]
                .observe_mid_and_step_elastic_vol(
                    spec,
                    mid_l1,
                    local_ns.0,
                    self.active_snaps[slot_idx].realized_vol_5m,
                );
            let mut w_obs = self.wave_states[slot_idx].observe_price_tick(
                spec,
                mid_l1,
                local_ns.0,
                self.active_snaps[slot_idx].realized_vol_5m,
            );
            w_obs.liq_imbalance = self.liq_floors[slot_idx].current_imbalance();
            self.active_snaps[slot_idx].set_wave_observables(w_obs);

            let ob_q = self.ob_states[slot_idx].observe_mid_quote(spec, mid_l1);
            self.step_directional_ob_and_publish(slot_idx, spec, mid_l1, ob_q);

            let virt_ts = self.event_clocks[slot_idx].step_event_clock_and_spoof(
                local_ns,
                self.active_snaps[slot_idx].factor_ofi,
                self.active_snaps[slot_idx].factor_tfi,
            );
            self.active_snaps[slot_idx].ts_local_ns = virt_ts;
            self.factor_states[slot_idx].on_l2_book_update(&mut self.active_snaps[slot_idx]);
            self.active_snaps[slot_idx].ts_exchange_ns = exch_ns;
            self.active_snaps[slot_idx].ts_local_ns = local_ns;

            if let Ok(slot_ref) = arena.slot(slot_idx) {
                slot_ref.write_snapshot(&self.active_snaps[slot_idx]);
            }
            let row = ColumnarMarketRow::from_lob_snapshot(
                &self.active_snaps[slot_idx],
                MarketEventKind::L2BookSnapshot,
                Side::Bid,
                self.active_snaps[slot_idx].mark_price,
                self.active_snaps[slot_idx].bids[0].qty,
            );
            self.push_par2_row(&row, writer);
        } else if let Some(sym_name) = topic.strip_prefix("publicTrade.") {
            let Some(slot_idx) = slot_from_symbol_name(sym_name) else {
                return false;
            };
            let spec = APPROVED_SPECS[slot_idx];
            if let Some(trades) = val.get("data").and_then(serde_json::Value::as_array) {
                for tr in trades {
                    let p_s = tr.get("p").and_then(serde_json::Value::as_str).unwrap_or("");
                    let q_s = tr.get("v").and_then(serde_json::Value::as_str).unwrap_or("");
                    let s_s = tr.get("S").and_then(serde_json::Value::as_str).unwrap_or("Buy");
                    if let Ok(px) = spec.parse_price_ticks(p_s) {
                        let qty = spec.parse_lot_qty(q_s).max(LotQty(1));
                        let side = if s_s == "Sell" { Side::Ask } else { Side::Bid };
                        self.liq_floors[slot_idx].observe_trade_print(spec, local_ns, px, qty);
                        let virt_ts = self.event_clocks[slot_idx].step_event_clock_and_spoof(
                            local_ns,
                            self.active_snaps[slot_idx].factor_ofi,
                            self.active_snaps[slot_idx].factor_tfi,
                        );
                        self.active_snaps[slot_idx].last_trade_price = px;
                        self.active_snaps[slot_idx].ts_local_ns = virt_ts;
                        self.factor_states[slot_idx].on_trade_print(
                            side,
                            qty,
                            &mut self.active_snaps[slot_idx],
                        );
                        self.active_snaps[slot_idx].ts_exchange_ns = exch_ns;
                        self.active_snaps[slot_idx].ts_local_ns = local_ns;

                        let ob_t =
                            self.ob_states[slot_idx].observe_live_tick(spec, px, qty, local_ns.0);
                        self.step_directional_ob_and_publish(slot_idx, spec, px, ob_t);

                        if !self.has_book_snapshot[slot_idx]
                            || self.active_snaps[slot_idx].bbo_and_spread_ticks().is_none()
                        {
                            continue;
                        }
                        if let Ok(slot_ref) = arena.slot(slot_idx) {
                            slot_ref.write_snapshot(&self.active_snaps[slot_idx]);
                        }
                        let row = ColumnarMarketRow::from_lob_snapshot(
                            &self.active_snaps[slot_idx],
                            MarketEventKind::PublicTradePrint,
                            side,
                            px,
                            qty,
                        );
                        self.push_par2_row(&row, writer);
                    }
                }
            }
        } else if let Some(sym_name) = topic.strip_prefix("allLiquidation.") {
            let Some(slot_idx) = slot_from_symbol_name(sym_name) else {
                return false;
            };
            let spec = APPROVED_SPECS[slot_idx];
            if let Some(data_v) = val.get("data") {
                let mut updated = false;
                let single_slice = core::slice::from_ref(data_v);
                let items = data_v.as_array().map_or(single_slice, Vec::as_slice);
                for liq in items {
                    if let Some((forced_side, qty, liq_px)) =
                        parse_bybit_all_liquidation_event(spec, liq)
                    {
                        let eff_px = if liq_px.0 > 0 {
                            liq_px
                        } else {
                            self.active_snaps[slot_idx].mark_price.max(PriceTicks(1))
                        };
                        let mut w_obs = self.wave_states[slot_idx].observe_liquidation_with_price(
                            spec,
                            forced_side,
                            qty,
                            eff_px,
                            local_ns.0,
                        );
                        w_obs.liq_imbalance = self.liq_floors[slot_idx].observe_liquidation_print(
                            spec,
                            local_ns,
                            forced_side,
                            eff_px,
                            qty,
                        );
                        self.active_snaps[slot_idx].set_wave_observables(w_obs);
                        updated = true;
                        if self.has_book_snapshot[slot_idx]
                            && self.active_snaps[slot_idx].bbo_and_spread_ticks().is_some()
                        {
                            self.active_snaps[slot_idx].ts_exchange_ns = exch_ns;
                            self.active_snaps[slot_idx].ts_local_ns = local_ns;
                            let row = ColumnarMarketRow::from_liquidation_print(
                                &self.active_snaps[slot_idx],
                                forced_side,
                                eff_px,
                                qty,
                                self.active_snaps[slot_idx].open_interest_x100,
                            );
                            self.push_par2_row(&row, writer);
                        }
                    }
                }
                if updated
                    && self.has_book_snapshot[slot_idx]
                    && self.active_snaps[slot_idx].bbo_and_spread_ticks().is_some()
                {
                    if let Ok(slot_ref) = arena.slot(slot_idx) {
                        slot_ref.write_snapshot(&self.active_snaps[slot_idx]);
                    }
                }
            }
        } else if let Some(sym_name) = topic.strip_prefix("tickers.") {
            let Some(slot_idx) = slot_from_symbol_name(sym_name) else {
                return false;
            };
            let spec = APPROVED_SPECS[slot_idx];
            let Some(d_root) = val.get("data") else {
                return false;
            };
            let item = d_root
                .as_array()
                .and_then(|a| a.first())
                .unwrap_or(d_root);

            let mut meta_changed = false;
            let mut mp_updated = false;

            if let Some(oi_s) = item.get("openInterest").and_then(serde_json::Value::as_str) {
                if let Some(oi_x100) = parse_decimal_to_scaled_i64(oi_s, 2) {
                    let clean_oi = oi_x100.max(0);
                    if clean_oi != self.active_snaps[slot_idx].open_interest_x100 {
                        self.active_snaps[slot_idx].open_interest_x100 = clean_oi;
                        meta_changed = true;
                    }
                }
                if let Ok(oi_f) = oi_s.trim().parse::<f64>() {
                    self.ob_states[slot_idx].observe_open_interest(oi_f);
                    let mut w_obs =
                        self.wave_states[slot_idx].observe_open_interest(spec, oi_f, local_ns.0);
                    w_obs.liq_imbalance = self.liq_floors[slot_idx].current_imbalance();
                    self.active_snaps[slot_idx].set_wave_observables(w_obs);
                }
            }

            if let Some(fr_s) = item.get("fundingRate").and_then(serde_json::Value::as_str) {
                if let Some(fr_bps) = parse_decimal_to_scaled_i64(fr_s, 6) {
                    let clamped_fr = ScaledBps(fr_bps.clamp(-5_000, 5_000));
                    if clamped_fr != self.active_snaps[slot_idx].funding_rate_scaled_bps {
                        self.active_snaps[slot_idx].funding_rate_scaled_bps = clamped_fr;
                        meta_changed = true;
                    }
                }
            }

            let next_f_ms = item.get("nextFundingTime").and_then(|v| {
                v.as_i64()
                    .or_else(|| v.as_str().and_then(|s| s.trim().parse::<i64>().ok()))
            });
            if let Some(nf_ms) = next_f_ms {
                if nf_ms > 0 {
                    self.active_snaps[slot_idx].next_funding_ts_ns =
                        TimestampNs(nf_ms.saturating_mul(1_000_000));
                }
            }

            if let Some(mp_s) = item.get("markPrice").and_then(serde_json::Value::as_str) {
                if let Ok(mp_t) = spec.parse_price_ticks(mp_s) {
                    self.active_snaps[slot_idx].mark_price = mp_t;
                    self.has_live_mark_px[slot_idx] = true;
                    mp_updated = true;
                }
            }

            if (meta_changed || mp_updated)
                && self.has_book_snapshot[slot_idx]
                && self.active_snaps[slot_idx].bbo_and_spread_ticks().is_some()
            {
                self.active_snaps[slot_idx].ts_exchange_ns = exch_ns;
                self.active_snaps[slot_idx].ts_local_ns = local_ns;
                if let Ok(slot_ref) = arena.slot(slot_idx) {
                    slot_ref.write_snapshot(&self.active_snaps[slot_idx]);
                }
                let since_last = local_ns.0.saturating_sub(self.last_ticker_par2_ns[slot_idx]);
                if meta_changed || since_last >= TICKER_PAR2_MIN_INTERVAL_NS {
                    self.last_ticker_par2_ns[slot_idx] = local_ns.0;
                    let row = ColumnarMarketRow::from_ticker_meta(
                        &self.active_snaps[slot_idx],
                        self.active_snaps[slot_idx].mark_price,
                        self.active_snaps[slot_idx].open_interest_x100,
                        self.active_snaps[slot_idx].funding_rate_scaled_bps,
                        self.active_snaps[slot_idx].next_funding_ts_ns,
                    );
                    self.push_par2_row(&row, writer);
                }
            }
        }
        false
    }
}

fn run_par2_shm_v15_self_check() -> Result<(), VortexError> {
    let eph_shm = MappedShmRegion::ephemeral_self_test_path("tap_par2_v15");
    let res = (|| -> Result<(), VortexError> {
        let reg = MappedShmRegion::open_or_create(&eph_shm, true)?;
        let arena = reg.arena();
        let mut st = TapLiveState::new(false);
        let t0 = 1_794_700_000_000_000_000_i64;

        let f_l2 = r#"{"topic":"orderbook.50.AVAXUSDT","type":"snapshot","ts":1794700000000,"data":{"s":"AVAXUSDT","u":100,"b":[["8.180","400"],["8.179","350"],["8.178","320"],["8.177","300"],["8.176","280"]],"a":[["8.181","360"],["8.182","340"],["8.183","310"],["8.184","290"],["8.185","270"]]}}"#;
        let f_tk = r#"{"topic":"tickers.AVAXUSDT","ts":1794700000100,"data":{"symbol":"AVAXUSDT","markPrice":"8.182","openInterest":"125000.50","fundingRate":"-0.000145","nextFundingTime":"1794700800000"}}"#;
        let f_tr = r#"{"topic":"publicTrade.AVAXUSDT","ts":1794700000200,"data":[{"p":"8.181","v":"25.0","S":"Buy"}]}"#;
        let f_lq = r#"{"topic":"allLiquidation.AVAXUSDT","ts":1794700000300,"data":[{"s":"AVAXUSDT","S":"Sell","v":"400.0","p":"8.185"}]}"#;

        let _ = st.ingest_public_ws_text(f_l2, TimestampNs(t0), arena, None);
        let _ = st.ingest_public_ws_text(f_tk, TimestampNs(t0 + 100_000_000), arena, None);
        let _ = st.ingest_public_ws_text(f_tr, TimestampNs(t0 + 200_000_000), arena, None);
        let _ = st.ingest_public_ws_text(f_lq, TimestampNs(t0 + 300_000_000), arena, None);
        st.dispatch_ob_plasticity_async(t0 + 300_000_000);

        let rd = arena.slot(3)?.read_snapshot(4)?;
        if rd.funding_rate_scaled_bps != ScaledBps(-145)
            || rd.open_interest_x100 != 12_500_050
            || rd.mark_price != PriceTicks(8_182)
            || rd.wave_observables().liq_imbalance.to_bps_i32() <= 0
            || st.batch.len() != 4
        {
            return Err(VortexError::ColumnarFormatCorrupted);
        }
        Ok(())
    })();
    let _ = std::fs::remove_file(&eph_shm);
    res
}

#[cfg(target_os = "linux")]
fn configure_ws_tcp_stream(
    socket: &mut tungstenite::WebSocket<
        tungstenite::stream::MaybeTlsStream<std::net::TcpStream>,
    >,
) {
    let tcp_opt = match socket.get_mut() {
        tungstenite::stream::MaybeTlsStream::Plain(s) => Some(s),
        tungstenite::stream::MaybeTlsStream::NativeTls(tls) => Some(tls.get_mut()),
        _ => None,
    };
    if let Some(tcp) = tcp_opt {
        let _ = tcp.set_nodelay(true);
        let _ = tcp.set_read_timeout(Some(Duration::from_secs(10)));
        let _ = tcp.set_write_timeout(Some(Duration::from_secs(5)));
    }
}

#[cfg(target_os = "linux")]
fn fetch_bybit_public_json_keepalive(
    conn: &mut KeepAliveHttpConn,
    query_path: &str,
) -> Option<serde_json::Value> {
    let raw = conn.send_json_request("GET", query_path, &[], "")?;
    serde_json::from_slice(raw).ok()
}

#[cfg(target_os = "linux")]
fn warm_start_elliott_and_order_blocks(state: &mut TapLiveState) {
    let Some(ep) = parse_endpoint_url(BYBIT_PUBLIC_REST_URL) else {
        return;
    };
    let mut conn = KeepAliveHttpConn::new(ep);
    let mut hydrated_slots = 0_usize;
    for s in 0..MAX_ACTIVE_PAIRS {
        let spec = APPROVED_SPECS[s];
        let sym = spec.symbol_id.name();
        let p5 = format!("/v5/market/kline?category=linear&symbol={sym}&interval=5&limit=300");
        let p1 = format!("/v5/market/kline?category=linear&symbol={sym}&interval=1&limit=240");
        let po = format!(
            "/v5/market/open-interest?category=linear&symbol={sym}&intervalTime=5min&limit=200"
        );
        let k5 = fetch_bybit_public_json_keepalive(&mut conn, &p5);
        let k1 = fetch_bybit_public_json_keepalive(&mut conn, &p1);
        let oi = fetch_bybit_public_json_keepalive(&mut conn, &po);
        let w_bars =
            state.wave_states[s].warm_start_from_bybit_bundle(spec, k5.as_ref(), k1.as_ref(), oi.as_ref());
        if w_bars > 0 {
            state.active_snaps[s].set_wave_observables(state.wave_states[s].observables());
        }
        let ob_bars =
            state.ob_states[s].warm_start_from_bybit_bundle(spec, k5.as_ref(), k1.as_ref(), oi.as_ref());
        if ob_bars > 0 {
            let (bull_pm, bear_pm) = state.ob_plasticity[s].directional_efficacy_pm();
            let obs = state.ob_states[s].observables();
            state.prev_bull_wall[s] = obs.bull_wall_ticks;
            state.prev_bear_wall[s] = obs.bear_wall_ticks;
            state.active_snaps[s].set_order_block_observables_directional(obs, bull_pm, bear_pm);
            hydrated_slots += 1;
        }
    }
    println!(
        "{}\u{1f7e2} [TAP WARM-START V4.6] Hydrated {}/12 slots (Chunked Keep-Alive TLS -> Elliott + [OB+, OB-]){}",
        ANSI_GREEN, hydrated_slots, ANSI_RESET
    );
}

#[cfg(not(target_os = "linux"))]
fn run_live_bybit_ws_daemon(arena: &VortexShmArena, _writer: AsyncParquetRingWriter) -> ! {
    loop {
        let local_ns = now_epoch_ns();
        arena.heartbeat_ns.store(local_ns.0 as u64, Ordering::Release);
        thread::sleep(Duration::from_millis(500));
    }
}

#[cfg(target_os = "linux")]
fn run_live_bybit_ws_daemon(arena: &VortexShmArena, writer: AsyncParquetRingWriter) -> ! {
    let mut state = TapLiveState::new(true);
    loop {
        state.reset_session_preserving_plasticity();
        warm_start_elliott_and_order_blocks(&mut state);
        state.dispatch_ob_plasticity_async(now_epoch_ns().0);

        if let Ok((mut socket, _)) = tungstenite::connect(BYBIT_PUBLIC_LINEAR_WS) {
            configure_ws_tcp_stream(&mut socket);
            let mut topics = Vec::with_capacity(MAX_ACTIVE_PAIRS * 4);
            for spec in &APPROVED_SPECS {
                let sym = spec.symbol_id.name();
                topics.push(format!("\"orderbook.50.{sym}\""));
                topics.push(format!("\"publicTrade.{sym}\""));
                topics.push(format!("\"tickers.{sym}\""));
                topics.push(format!("\"allLiquidation.{sym}\""));
            }
            let sub_msg = format!("{{\"op\":\"subscribe\",\"args\":[{}]}}", topics.join(","));
            if socket.send(tungstenite::Message::Text(sub_msg)).is_err() {
                thread::sleep(Duration::from_millis(500));
                continue;
            }
            let mut last_ping_ns = now_epoch_ns().0;
            while let Ok(msg) = socket.read() {
                let local_ns = now_epoch_ns();
                arena.heartbeat_ns.store(local_ns.0 as u64, Ordering::Release);
                if local_ns.0.saturating_sub(last_ping_ns) >= 15_000_000_000 {
                    if socket
                        .send(tungstenite::Message::Text("{\"op\":\"ping\"}".to_string()))
                        .is_err()
                    {
                        break;
                    }
                    last_ping_ns = local_ns.0;
                }
                if local_ns.0.saturating_sub(state.last_ob_persist_ns) >= 60_000_000_000 {
                    state.dispatch_ob_plasticity_async(local_ns.0);
                }
                let txt = match msg {
                    tungstenite::Message::Text(t) => t,
                    tungstenite::Message::Ping(payload) => {
                        let _ = socket.send(tungstenite::Message::Pong(payload));
                        continue;
                    }
                    tungstenite::Message::Close(_) => break,
                    _ => continue,
                };
                if state.ingest_public_ws_text(&txt, local_ns, arena, Some(&writer)) {
                    break;
                }
            }
        }
        let _ = writer.try_flush_and_clear(&mut state.batch);
        state.dispatch_ob_plasticity_async(now_epoch_ns().0);
        thread::sleep(Duration::from_millis(500));
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let is_daemon = args.iter().any(|a| a == "--daemon");
    let shm_path = MappedShmRegion::default_shm_path();
    let sep = "=".repeat(80);
    let dash = "-".repeat(80);
    println!("{sep}");
    println!(
        "[VORTEX-TAP OMEGA V4.6] 12-PAIR ELIGIBILITY & SEQLOCK DAEMON (SHM v{}: {})",
        VORTEX_SHM_VERSION,
        shm_path.display()
    );
    println!("{sep}");
    let Ok((approved, rows, checksum)) = run_isolated_tap_self_test(true) else {
        return ExitCode::FAILURE;
    };
    if run_par2_shm_v15_self_check().is_err() {
        return ExitCode::FAILURE;
    }
    println!("{dash}");
    println!(
        concat!(
            "[SEQLOCK SHM v15 & PAR2 OK] Isolated Gate Slots: {} | Rows Verified: {} (+4 PAR2 Live) | ",
            "FNV1a64: 0x{:016X}"
        ),
        approved, rows, checksum
    );
    if !is_daemon {
        inspect_live_shm_readonly(&shm_path);
        println!("{sep}");
        return ExitCode::SUCCESS;
    }
    println!("{sep}");
    prune_expired_parquet_segment(Path::new("data"), 0);
    let Ok(region) = MappedShmRegion::open_or_create(&shm_path, true) else {
        return ExitCode::FAILURE;
    };
    let writer = AsyncParquetRingWriter::spawn(PathBuf::from("data"), true);
    run_live_bybit_ws_daemon(region.arena(), writer);
}
