//! VORTEX-OMEGA V5.3.2 Immortal 12-Pair WebSocket Ingestor Daemon (SHM v15 + PAR2 + Live VIP Fees).
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
use vortex_rs::fee_cadence_beta::FeeSchedule;
use vortex_rs::forensic_parquet::{ColumnarMarketRow, ColumnarParquetBatch, MarketEventKind};
#[cfg(target_os = "linux")]
use vortex_rs::http_ws_transport::{
    connect_ws_with_timeout, is_ws_timeout_err, parse_endpoint_url, KeepAliveHttpConn,
};
use vortex_rs::lob_shm::{
    classify_l2_sequence_transition, is_l2_seq_or_basis_anomaly, BookLevel, L2SeqDecision,
    MappedShmRegion, SymbolLobSnapshot, VortexShmArena, MAX_ACTIVE_PAIRS,
    MAX_CONTIGUOUS_L2_SEQ_JUMP, VORTEX_SHM_VERSION,
};
use vortex_rs::order_block::{
    load_directional_plasticity_from_path, CausalOrderBlockEngine, OrderBlockObservables,
    OB_PLASTICITY_VAULT_PATH,
};
use vortex_rs::tap_ring::{
    inspect_live_shm_readonly, prune_expired_parquet_segment, run_isolated_tap_self_test,
    AsyncParquetRingWriter, SAMPLE_VOL_5M_BPS,
};
use vortex_rs::telemetry_hud::load_hud_fee_schedule;
use vortex_rs::types::{
    now_epoch_ns, slot_from_symbol_name, LotQty, PriceTicks, ScaledBps, Side, TimestampNs,
    VortexError, ANSI_GREEN, ANSI_RESET, APPROVED_SPECS,
};
use vortex_rs::vault_io::{spawn_vault_io_worker, AtomicLockWatch, VaultIoCommand};
use vortex_rs::wave_elliott::{parse_bybit_all_liquidation_event, CausalElliottWaveState};

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
    ob_states: [CausalOrderBlockEngine; MAX_ACTIVE_PAIRS],
    active_snaps: [SymbolLobSnapshot; MAX_ACTIVE_PAIRS],
    has_live_mark_px: [bool; MAX_ACTIVE_PAIRS],
    has_book_snapshot: [bool; MAX_ACTIVE_PAIRS],
    last_ticker_par2_ns: [i64; MAX_ACTIVE_PAIRS],
    active_fees: FeeSchedule,
    batch: Box<ColumnarParquetBatch>,
    vault_tx: Option<SyncSender<VaultIoCommand>>,
    candle_aggs: Box<[vortex_rs::candle_store::SlotMultiTfAggregator; MAX_ACTIVE_PAIRS]>,
    candle_tx: Option<SyncSender<vortex_rs::candle_store::Vkl1CandleRecord>>,
    last_ob_persist_ns: i64,
    last_vkl1_1m_flush_ns: i64,
}

impl TapLiveState {
    #[must_use]
    fn new(enable_async_vault: bool) -> Self {
        let mut ob_states = [CausalOrderBlockEngine::new(); MAX_ACTIVE_PAIRS];
        let active_fees = if enable_async_vault {
            let (bull_arr, bear_arr) =
                load_directional_plasticity_from_path(Path::new(OB_PLASTICITY_VAULT_PATH));
            for s in 0..MAX_ACTIVE_PAIRS {
                ob_states[s].set_directional_efficacy_pm(bull_arr[s], bear_arr[s]);
            }
            let live_fees = load_hud_fee_schedule();
            println!(
                "{}\u{1f7e2} [OB PLASTICITY V5.3.2 REHYDRATE] Hydrated 12/12 [OB+, OB-] SSOT slots | Fees: {:+.2}/{:.2}bp{}",
                ANSI_GREEN,
                (live_fees.maker_fee_bps.raw() as f64) / 100.0,
                (live_fees.taker_fee_bps.raw() as f64) / 100.0,
                ANSI_RESET
            );
            live_fees
        } else {
            FeeSchedule::VIP0_DEFAULT
        };
        let vault_tx = if enable_async_vault {
            Some(spawn_vault_io_worker(Arc::new(AtomicLockWatch::default())))
        } else {
            None
        };
        let mut st = Self {
            factor_states: [OrthogonalFactorState::new(); MAX_ACTIVE_PAIRS],
            event_clocks: [EventClockAlphaModulator::new(); MAX_ACTIVE_PAIRS],
            wave_states: [CausalElliottWaveState::new(); MAX_ACTIVE_PAIRS],
            ob_states,
            active_snaps: [SymbolLobSnapshot::new_empty(); MAX_ACTIVE_PAIRS],
            has_live_mark_px: [false; MAX_ACTIVE_PAIRS],
            has_book_snapshot: [false; MAX_ACTIVE_PAIRS],
            last_ticker_par2_ns: [0; MAX_ACTIVE_PAIRS],
            active_fees,
            batch: Box::<ColumnarParquetBatch>::default(),
            vault_tx,
            candle_aggs: Box::new(vortex_rs::candle_store::SlotMultiTfAggregator::new_12_slot_array()),
            candle_tx: enable_async_vault.then(|| { let (ctx, crx) = std::sync::mpsc::sync_channel::<vortex_rs::candle_store::Vkl1CandleRecord>(1024); thread::spawn(move || { let store = vortex_rs::candle_store::PerpetualCandleStore::default_store(); while let Ok(rec) = crx.recv() { let _ = store.append_or_update(&rec); } }); ctx }),
            last_ob_persist_ns: 0,
            last_vkl1_1m_flush_ns: 0,
        };
        st.reset_session_preserving_plasticity();
        st
    }

    #[inline]
    fn set_active_fee_schedule(&mut self, fees: FeeSchedule) {
        self.active_fees = fees;
        for s in 0..MAX_ACTIVE_PAIRS {
            self.ob_states[s].set_fee_and_taker_context(fees, 50);
        }
    }

    fn reset_session_preserving_plasticity(&mut self) {
        for s in 0..MAX_ACTIVE_PAIRS {
            let spec = APPROVED_SPECS[s];
            let init_vol = ScaledBps(SAMPLE_VOL_5M_BPS[s]);
            self.factor_states[s].reset();
            self.event_clocks[s] = EventClockAlphaModulator::new();
            self.wave_states[s].reset();
            self.ob_states[s].reset_preserving_plasticity();
            self.ob_states[s].set_realized_vol_5m(init_vol);
            self.ob_states[s].set_fee_and_taker_context(self.active_fees, 50);
            let (bull_pm, bear_pm) = self.ob_states[s].directional_efficacy_pm();
            let prev_resyncs = self.active_snaps[s].seq_resync_count;
            self.active_snaps[s] = SymbolLobSnapshot::new_empty();
            self.active_snaps[s].symbol_id = spec.symbol_id;
            self.active_snaps[s].is_eligible = true;
            self.active_snaps[s].realized_vol_5m = init_vol;
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
            let (b_pm, a_pm) = self.ob_states[s].directional_efficacy_pm();
            bull_pm[s] = b_pm;
            bear_pm[s] = a_pm;
        }
        if let Some(ref tx) = self.vault_tx {
            let _ = tx.try_send(VaultIoCommand::SaveObPlasticity { ts_ns, bull_pm, bear_pm });
        }
    }

    #[inline]
    fn maybe_run_60s_maintenance(&mut self, ts_ns: i64, reload_vault_fees: bool) {
        if ts_ns.saturating_sub(self.last_vkl1_1m_flush_ns) >= 5_000_000_000 {
            self.last_vkl1_1m_flush_ns = ts_ns;
            if let Some(ref ctx) = self.candle_tx {
                for s in 0..MAX_ACTIVE_PAIRS {
                    let s_u8 = u8::try_from(s).unwrap_or(0);
                    if let Some(cur) = self.candle_aggs[s].active_candle(s_u8, vortex_rs::candle_store::CandleTimeframe::Min1) {
                        let _ = ctx.try_send(cur);
                    }
                }
            }
        }
        if ts_ns.saturating_sub(self.last_ob_persist_ns) < 60_000_000_000 {
            return;
        }
        self.flush_active_vkl1_bars();
        if reload_vault_fees {
            self.set_active_fee_schedule(load_hud_fee_schedule());
        }
        self.dispatch_ob_plasticity_async(ts_ns);
    }

    #[inline]
    fn flush_active_vkl1_bars(&self) { let Some(ref ctx) = self.candle_tx else { return; }; for s in 0..MAX_ACTIVE_PAIRS { let s_u8 = u8::try_from(s).unwrap_or(0); for tf in vortex_rs::candle_store::CandleTimeframe::ALL.into_iter().skip(1) { if let Some(cur) = self.candle_aggs[s].active_candle(s_u8, tf) { let _ = ctx.try_send(cur); } } } }
    fn record_vkl1_from_row(&mut self, row: &ColumnarMarketRow) {
        if self.candle_tx.is_none() {
            return;
        }
        let Some(s_idx) = APPROVED_SPECS.iter().position(|sp| sp.symbol_id == row.symbol_id) else {
            return;
        };
        let is_tr = matches!(row.par2_kind, vortex_rs::forensic_parquet::Par2EventKind::PublicTradePrint);
        let is_lq = matches!(row.par2_kind, vortex_rs::forensic_parquet::Par2EventKind::LiquidationPrint);
        let tr_lots = if is_tr { row.lot_qty } else { LotQty(0) };
        let lq_u16 = u16::try_from(row.lot_qty.0.clamp(0, 65_535)).unwrap_or(0);
        let lq_b = if is_lq && matches!(row.side, Side::Bid) { lq_u16 } else { 0 };
        let lq_s = if is_lq && matches!(row.side, Side::Ask) { lq_u16 } else { 0 };
        let is_spoof_veto = self.event_clocks[s_idx].spoofing_density() >= vortex_rs::alpha_3factor::INV_PHI_SQ;
        let closed_arr = self.candle_aggs[s_idx].ingest_snapshot(
            u8::try_from(s_idx).unwrap_or(0),
            &self.active_snaps[s_idx],
            tr_lots,
            is_spoof_veto,
            lq_b,
            lq_s,
        );
        if let Some(ref ctx) = self.candle_tx {
            for rec in closed_arr.into_iter().flatten() {
                let _ = ctx.try_send(rec);
            }
        }
    }
    fn push_par2_row(&mut self, row: &ColumnarMarketRow, writer: Option<&AsyncParquetRingWriter>) {
        self.record_vkl1_from_row(row);
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
    fn publish_ob_observables(&mut self, slot_idx: usize, obs: OrderBlockObservables) {
        let (bull_pm, bear_pm) = self.ob_states[slot_idx].directional_efficacy_pm();
        self.active_snaps[slot_idx].set_order_block_observables_directional(obs, bull_pm, bear_pm);
    }

    /// Processes 1 Bybit V5 public WS frame; returns `true` iff a severe L2 sequence gap requires resync.
    fn ingest_public_ws_text(
        &mut self,
        txt: &str,
        local_ns: TimestampNs,
        arena: &VortexShmArena,
        writer: Option<&AsyncParquetRingWriter>,
    ) -> bool {
        let Ok(val) = serde_json::from_str::<serde_json::Value>(txt) else { return false };
        let Some(topic) = val.get("topic").and_then(serde_json::Value::as_str) else { return false };
        let exch_ms = val.get("ts").and_then(serde_json::Value::as_i64).unwrap_or(0);
        let exch_ns = TimestampNs(if exch_ms > 0 { exch_ms.saturating_mul(1_000_000) } else { local_ns.0 });

        if let Some(sym_name) = topic.strip_prefix("orderbook.50.") {
            let Some(slot_idx) = slot_from_symbol_name(sym_name) else { return false };
            let spec = APPROVED_SPECS[slot_idx];
            let is_snapshot = val.get("type").and_then(serde_json::Value::as_str).is_some_and(|t| t == "snapshot");
            let Some(data) = val.get("data") else { return false };
            let incoming_u = data.get("u").and_then(serde_json::Value::as_u64).unwrap_or(0);
            let prev_seq = self.active_snaps[slot_idx].sequence_id;

            match classify_l2_sequence_transition(self.has_book_snapshot[slot_idx], prev_seq, is_snapshot, incoming_u) {
                L2SeqDecision::ResetAndApplySnapshot => {
                    self.active_snaps[slot_idx].bids = [BookLevel::default(); 50];
                    self.active_snaps[slot_idx].asks = [BookLevel::default(); 50];
                    self.factor_states[slot_idx].reset_l2_book_only();
                    self.has_book_snapshot[slot_idx] = true;
                }
                L2SeqDecision::ApplyMonotonicDelta => {
                    if prev_seq > 0 && incoming_u > prev_seq && (incoming_u - prev_seq) > MAX_CONTIGUOUS_L2_SEQ_JUMP {
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

            let b3x = spec.three_x_friction_barrier_bps_dynamic(mid_l1, 50, self.active_fees);
            if self.has_live_mark_px[slot_idx]
                && is_l2_seq_or_basis_anomaly(
                    prev_seq, incoming_u, mid_l1, self.active_snaps[slot_idx].mark_price,
                    self.active_snaps[slot_idx].realized_vol_5m, b3x,
                )
            {
                self.active_snaps[slot_idx].seq_resync_count =
                    self.active_snaps[slot_idx].seq_resync_count.saturating_add(1);
                arena.tap_seq_resync_count.fetch_add(1, Ordering::Relaxed);
            }
            let next_vol = self.factor_states[slot_idx].observe_mid_and_step_elastic_vol_with_fees(
                spec, mid_l1, local_ns.0, self.active_snaps[slot_idx].realized_vol_5m, 50, self.active_fees,
            );
            self.active_snaps[slot_idx].realized_vol_5m = next_vol;
            self.ob_states[slot_idx].set_realized_vol_5m(next_vol);
            let w_obs = self.wave_states[slot_idx].observe_price_tick(spec, mid_l1, local_ns.0, next_vol);
            self.active_snaps[slot_idx].set_wave_observables(w_obs);
            let ob_q = self.ob_states[slot_idx].observe_mid_quote(spec, mid_l1);
            self.publish_ob_observables(slot_idx, ob_q);

            let virt_ts = self.event_clocks[slot_idx].step_event_clock_and_spoof(
                local_ns, self.active_snaps[slot_idx].factor_ofi, self.active_snaps[slot_idx].factor_tfi,
            );
            self.active_snaps[slot_idx].ts_local_ns = virt_ts;
            self.factor_states[slot_idx].on_l2_book_update(&mut self.active_snaps[slot_idx]);
            self.active_snaps[slot_idx].ts_exchange_ns = exch_ns;
            self.active_snaps[slot_idx].ts_local_ns = local_ns;
            if let Ok(slot_ref) = arena.slot(slot_idx) {
                slot_ref.write_snapshot(&self.active_snaps[slot_idx]);
            }
            let row = ColumnarMarketRow::from_lob_snapshot(
                &self.active_snaps[slot_idx], MarketEventKind::L2BookSnapshot, Side::Bid,
                self.active_snaps[slot_idx].mark_price, self.active_snaps[slot_idx].bids[0].qty,
            );
            self.push_par2_row(&row, writer);
        } else if let Some(sym_name) = topic.strip_prefix("publicTrade.") {
            let Some(slot_idx) = slot_from_symbol_name(sym_name) else { return false };
            let spec = APPROVED_SPECS[slot_idx];
            if let Some(trades) = val.get("data").and_then(serde_json::Value::as_array) {
                for tr in trades {
                    let p_s = tr.get("p").and_then(serde_json::Value::as_str).unwrap_or("");
                    let q_s = tr.get("v").and_then(serde_json::Value::as_str).unwrap_or("");
                    let s_s = tr.get("S").and_then(serde_json::Value::as_str).unwrap_or("Buy");
                    if let Ok(px) = spec.parse_price_ticks(p_s) {
                        let qty = spec.parse_lot_qty(q_s).max(LotQty(1));
                        let side = if s_s == "Sell" { Side::Ask } else { Side::Bid };
                        self.wave_states[slot_idx].observe_public_trade_notional(spec, local_ns, px, qty);
                        let virt_ts = self.event_clocks[slot_idx].step_event_clock_and_spoof(
                            local_ns, self.active_snaps[slot_idx].factor_ofi, self.active_snaps[slot_idx].factor_tfi,
                        );
                        self.active_snaps[slot_idx].last_trade_price = px;
                        self.active_snaps[slot_idx].ts_local_ns = virt_ts;
                        self.factor_states[slot_idx].on_trade_print(side, qty, &mut self.active_snaps[slot_idx]);
                        self.active_snaps[slot_idx].ts_exchange_ns = exch_ns;
                        self.active_snaps[slot_idx].ts_local_ns = local_ns;
                        let ob_t = self.ob_states[slot_idx].observe_live_tick(spec, px, qty, local_ns.0);
                        self.publish_ob_observables(slot_idx, ob_t);
                        if !self.has_book_snapshot[slot_idx]
                            || self.active_snaps[slot_idx].bbo_and_spread_ticks().is_none()
                        {
                            continue;
                        }
                        if let Ok(slot_ref) = arena.slot(slot_idx) {
                            slot_ref.write_snapshot(&self.active_snaps[slot_idx]);
                        }
                        let row = ColumnarMarketRow::from_lob_snapshot(
                            &self.active_snaps[slot_idx], MarketEventKind::PublicTradePrint, side, px, qty,
                        );
                        self.push_par2_row(&row, writer);
                    }
                }
            }
        } else if let Some(sym_name) = topic.strip_prefix("allLiquidation.") {
            let Some(slot_idx) = slot_from_symbol_name(sym_name) else { return false };
            let spec = APPROVED_SPECS[slot_idx];
            if let Some(data_v) = val.get("data") {
                let mut updated = false;
                let single_slice = core::slice::from_ref(data_v);
                let items = data_v.as_array().map_or(single_slice, Vec::as_slice);
                for liq in items {
                    if let Some((forced_side, qty, liq_px)) = parse_bybit_all_liquidation_event(spec, liq) {
                        let eff_px = if liq_px.0 > 0 {
                            liq_px
                        } else {
                            self.active_snaps[slot_idx].mark_price.max(PriceTicks(1))
                        };
                        let w_obs = self.wave_states[slot_idx].observe_liquidation_with_price(
                            spec, forced_side, qty, eff_px, local_ns.0,
                        );
                        self.active_snaps[slot_idx].set_wave_observables(w_obs);
                        updated = true;
                        if self.has_book_snapshot[slot_idx]
                            && self.active_snaps[slot_idx].bbo_and_spread_ticks().is_some()
                        {
                            self.active_snaps[slot_idx].ts_exchange_ns = exch_ns;
                            self.active_snaps[slot_idx].ts_local_ns = local_ns;
                            let row = ColumnarMarketRow::from_liquidation_print(
                                &self.active_snaps[slot_idx], forced_side, eff_px, qty,
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
            let Some(slot_idx) = slot_from_symbol_name(sym_name) else { return false };
            let spec = APPROVED_SPECS[slot_idx];
            let Some(d_root) = val.get("data") else { return false };
            let item = d_root.as_array().and_then(|a| a.first()).unwrap_or(d_root);
            let (mut meta_changed, mut mp_updated) = (false, false);

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
                    let w_obs = self.wave_states[slot_idx].observe_open_interest(spec, oi_f, local_ns.0);
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
                v.as_i64().or_else(|| v.as_str().and_then(|s| s.trim().parse::<i64>().ok()))
            });
            if let Some(nf_ms) = next_f_ms {
                if nf_ms > 0 {
                    self.active_snaps[slot_idx].next_funding_ts_ns = TimestampNs(nf_ms.saturating_mul(1_000_000));
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
    let eph_shm = MappedShmRegion::ephemeral_self_test_path("tap_par2_v15_v48");
    let res = (|| -> Result<(), VortexError> {
        let reg = MappedShmRegion::open_or_create(&eph_shm, true)?;
        let arena = reg.arena();
        let mut st = TapLiveState::new(false);
        st.set_active_fee_schedule(FeeSchedule::from_live_rates_clamped(ScaledBps(-50), ScaledBps(300)));
        let t0 = 1_794_700_000_000_000_000_i64;
        let f_l2 = r#"{"topic":"orderbook.50.AVAXUSDT","type":"snapshot","ts":1794700000000,"data":{"s":"AVAXUSDT","u":100,"b":[["8.180","400"],["8.179","350"],["8.178","320"],["8.177","300"],["8.176","280"]],"a":[["8.181","360"],["8.182","340"],["8.183","310"],["8.184","290"],["8.185","270"]]}}"#;
        let f_tk = r#"{"topic":"tickers.AVAXUSDT","ts":1794700000100,"data":{"symbol":"AVAXUSDT","markPrice":"8.182","openInterest":"125000.50","fundingRate":"-0.000145","nextFundingTime":"1794700800000"}}"#;
        let f_tr = r#"{"topic":"publicTrade.AVAXUSDT","ts":1794700000200,"data":[{"p":"8.181","v":"25.0","S":"Buy"}]}"#;
        let f_lq = r#"{"topic":"allLiquidation.AVAXUSDT","ts":1794700000300,"data":[{"s":"AVAXUSDT","S":"Sell","v":"400.0","p":"8.185"}]}"#;
        let _ = st.ingest_public_ws_text(f_l2, TimestampNs(t0), arena, None);
        let _ = st.ingest_public_ws_text(f_tk, TimestampNs(t0 + 100_000_000), arena, None);
        let _ = st.ingest_public_ws_text(f_tr, TimestampNs(t0 + 200_000_000), arena, None);
        let _ = st.ingest_public_ws_text(f_lq, TimestampNs(t0 + 300_000_000), arena, None);
        st.maybe_run_60s_maintenance(t0 + 61_000_000_000, false);

        let rd = arena.slot(3)?.read_snapshot(4)?;
        if rd.funding_rate_scaled_bps != ScaledBps(-145)
            || rd.open_interest_x100 != 12_500_050
            || rd.mark_price != PriceTicks(8_182)
            || rd.wave_observables().liq_imbalance.to_bps_i32() <= 0
            || st.wave_states[3].active_liquidation_floor_usdt() < 50.0
            || st.active_fees.maker_fee_bps != ScaledBps(-50)
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
fn fetch_bybit_public_json_keepalive(conn: &mut KeepAliveHttpConn, query_path: &str) -> Option<serde_json::Value> {
    let raw = conn.send_json_request("GET", query_path, &[], "")?;
    serde_json::from_slice(raw).ok()
}

#[cfg(target_os = "linux")]
fn warm_start_elliott_and_order_blocks(state: &mut TapLiveState) {
    let Some(ep) = parse_endpoint_url(BYBIT_PUBLIC_REST_URL) else { return };
    let mut conn = KeepAliveHttpConn::new(ep);
    let mut hydrated_slots = 0_usize;
    for s in 0..MAX_ACTIVE_PAIRS {
        let spec = APPROVED_SPECS[s];
        let sym = spec.symbol_id.name();
        let p5 = format!("/v5/market/kline?category=linear&symbol={sym}&interval=5&limit=300");
        let p1 = format!("/v5/market/kline?category=linear&symbol={sym}&interval=1&limit=240");
        let po = format!("/v5/market/open-interest?category=linear&symbol={sym}&intervalTime=5min&limit=200");
        let c_store = vortex_rs::candle_store::PerpetualCandleStore::default_store(); let k5_net = fetch_bybit_public_json_keepalive(&mut conn, &p5); if let Some(ref j5) = k5_net { let _ = c_store.backfill_from_bybit_kline_json(s, vortex_rs::candle_store::CandleTimeframe::Min5, j5); } let k5 = k5_net.or_else(|| c_store.export_bybit_kline_bundle(s, vortex_rs::candle_store::CandleTimeframe::Min5, 300).ok());
        let k1_net = fetch_bybit_public_json_keepalive(&mut conn, &p1); if let Some(ref j1) = k1_net { let _ = c_store.backfill_from_bybit_kline_json(s, vortex_rs::candle_store::CandleTimeframe::Min1, j1); } let k1 = k1_net.or_else(|| c_store.export_bybit_kline_bundle(s, vortex_rs::candle_store::CandleTimeframe::Min1, 240).ok());
        let oi = fetch_bybit_public_json_keepalive(&mut conn, &po);
        if state.wave_states[s].warm_start_from_bybit_bundle(spec, k5.as_ref(), k1.as_ref(), oi.as_ref()) > 0 {
            state.active_snaps[s].set_wave_observables(state.wave_states[s].observables());
        }
        if state.ob_states[s].warm_start_from_bybit_bundle(spec, k5.as_ref(), k1.as_ref(), oi.as_ref()) > 0 {
            let obs = state.ob_states[s].observables();
            state.publish_ob_observables(s, obs);
            hydrated_slots += 1;
        }
    }
    println!(
        "{}\u{1f7e2} [TAP WARM-START V5.3.2] Hydrated {}/12 slots (Chunked Keep-Alive TLS -> Elliott + [OB+, OB-] + Live VIP){}",
        ANSI_GREEN, hydrated_slots, ANSI_RESET
    );
}

#[cfg(not(target_os = "linux"))]
fn run_live_bybit_ws_daemon(arena: &VortexShmArena, _writer: AsyncParquetRingWriter) -> ! {
    loop {
        arena.heartbeat_ns.store(now_epoch_ns().0 as u64, Ordering::Release);
        thread::sleep(Duration::from_millis(500));
    }
}

#[cfg(target_os = "linux")]
fn run_live_bybit_ws_daemon(arena: &VortexShmArena, writer: AsyncParquetRingWriter) -> ! {
    let mut state = TapLiveState::new(true);
    let mut last_warm_start_ns = 0_i64;
    loop {
        state.set_active_fee_schedule(load_hud_fee_schedule());
        let boot_now = now_epoch_ns().0;
        if last_warm_start_ns == 0 || boot_now.saturating_sub(last_warm_start_ns) >= 900_000_000_000 {
            state.reset_session_preserving_plasticity();
            warm_start_elliott_and_order_blocks(&mut state);
            last_warm_start_ns = now_epoch_ns().0;
        } else {
            for s in 0..MAX_ACTIVE_PAIRS {
                state.has_book_snapshot[s] = false;
                state.active_snaps[s].bids = [BookLevel::default(); 50];
                state.active_snaps[s].asks = [BookLevel::default(); 50];
                state.factor_states[s].reset_l2_book_only();
            }
        }
        state.dispatch_ob_plasticity_async(now_epoch_ns().0);

        if let Some(mut socket) = connect_ws_with_timeout(BYBIT_PUBLIC_LINEAR_WS, 10_000, 5_000) {
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
            loop {
                let msg = match socket.read() {
                    Ok(m) => m,
                    Err(ref err) if is_ws_timeout_err(err) => {
                        let local_ns = now_epoch_ns();
                        arena.heartbeat_ns.store(local_ns.0 as u64, Ordering::Release);
                        if socket.send(tungstenite::Message::Text("{\"op\":\"ping\"}".to_string())).is_err() {
                            break;
                        }
                        last_ping_ns = local_ns.0;
                        continue;
                    }
                    Err(_) => break,
                };
                let local_ns = now_epoch_ns();
                arena.heartbeat_ns.store(local_ns.0 as u64, Ordering::Release);
                if local_ns.0.saturating_sub(last_ping_ns) >= 15_000_000_000 {
                    if socket.send(tungstenite::Message::Text("{\"op\":\"ping\"}".to_string())).is_err() {
                        break;
                    }
                    last_ping_ns = local_ns.0;
                }
                state.maybe_run_60s_maintenance(local_ns.0, true);
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
        "[VORTEX-TAP OMEGA V5.3.2] 12-PAIR ELIGIBILITY & SEQLOCK DAEMON (SHM v{}: {})",
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
        "[SEQLOCK SHM v15 & PAR2 OK] Isolated Gate Slots: {} | Rows Verified: {} (+4 PAR2 Live) | FNV1a64: 0x{:016X}",
        approved, rows, checksum
    );

    if !is_daemon {
        inspect_live_shm_readonly(&shm_path);
        println!("{sep}");
        return ExitCode::SUCCESS;
    }

    println!("{sep}");
    prune_expired_parquet_segment(Path::new("data"), 0);
    let Ok(region) = MappedShmRegion::open_or_create(&shm_path, false) else {
        return ExitCode::FAILURE;
    };
    region.arena().reset_lob_slots_only();
    let writer = AsyncParquetRingWriter::spawn(PathBuf::from("data"), true);
    run_live_bybit_ws_daemon(region.arena(), writer);
}
