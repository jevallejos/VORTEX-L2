//! Zero-Jitter Off-Thread Atomic Vault Persister (V5.5: Queue Aborts, 1Hz Position Paths, Plasticity Audit & -5.00 USDT Total Loss Shield).

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]

use core::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::mpsc::{self, SyncSender};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::bayesian_governor::{OnlineAdaptiveGovernor, EMA_PREWARM_PATH};
use crate::execution_fsm::{ClosedTradeSummary, SlotExecutionEngine};
use crate::fee_cadence_beta::MAX_PORTFOLIO_RISK_PPM;
use crate::forensic_parquet::{ColumnarParquetBatch, ParitySyncLedger};
use crate::governor_plasticity::{DailySessionGovernor, DAILY_GOVERNOR_VAULT_PATH};
use crate::lob_shm::{LastVetoCode, SymbolLobSnapshot, MAX_ACTIVE_PAIRS};
use crate::markout_tracker::{
    MarkoutCurve5hBps, MarkoutCurveBps, ResolvedAbortSample, MARKOUT_HORIZONS_V49,
};
use crate::order_block::{save_directional_plasticity_to_path_atomic, OB_PLASTICITY_VAULT_PATH};
use crate::telemetry_hud::{
    build_closed_trade_vault_json, serialize_open_positions_state, SlotForensicEntrySnapshot,
    AI_FORENSIC_VAULT_PATH, DRAIN_LOCK_PATH, HALT_LOCK_PATH, OPEN_POS_STATE_PATH,
    RED_LOSS_BOX_PATH, SCAR_MEMORY_VAULT_PATH,
};
use crate::types::{LotQty, PriceTicks, ScaledBps, Side, TimestampNs, VortexError, APPROVED_SPECS};

pub const VAULT_IO_QUEUE_CAP: usize = 512;
pub const MAX_INTRA_TRADE_SAMPLES: usize = 128;
/// Hard irreversible operator validation epoch loss ceiling: `5.00000000 USDT` (`1e8` integer scale).
pub const MAX_VALIDATION_EPOCH_LOSS_1E8: i64 = 500_000_000;
pub const MFE_PLASTICITY_VAULT_PATH: &str = "data/vault/vortex_mfe_plasticity.json";
pub const QUEUE_ABORTS_VAULT_PATH: &str = "data/vault/vortex_queue_aborts.jsonl";
pub const POSITION_PATHS_VAULT_PATH: &str = "data/vault/vortex_position_paths.jsonl";
pub const PLASTICITY_AUDIT_VAULT_PATH: &str = "data/vault/vortex_plasticity_audit.jsonl";

#[derive(Debug)]
pub struct AtomicLockWatch {
    halt_locked: AtomicBool,
    halt_latched_ram: AtomicBool,
    drain_active: AtomicBool,
}

impl Default for AtomicLockWatch {
    #[inline]
    fn default() -> Self {
        Self::new(false, false)
    }
}

impl AtomicLockWatch {
    #[inline]
    #[must_use]
    pub const fn new(halt: bool, drain: bool) -> Self {
        Self {
            halt_locked: AtomicBool::new(halt),
            halt_latched_ram: AtomicBool::new(halt),
            drain_active: AtomicBool::new(drain),
        }
    }

    #[inline]
    #[must_use]
    pub fn is_halt_locked(&self) -> bool {
        self.halt_latched_ram.load(Ordering::Acquire) || self.halt_locked.load(Ordering::Acquire)
    }

    #[inline]
    #[must_use]
    pub fn is_drain_active(&self) -> bool {
        self.drain_active.load(Ordering::Acquire)
    }

    #[inline]
    pub fn latch_halt_in_ram(&self) {
        self.halt_latched_ram.store(true, Ordering::Release);
        self.halt_locked.store(true, Ordering::Release);
    }

    pub fn refresh_from_fs(&self, halt_path: &Path, drain_path: &Path) {
        let latched = self.halt_latched_ram.load(Ordering::Acquire) || halt_path.exists();
        self.halt_locked.store(latched, Ordering::Release);
        self.drain_active.store(drain_path.exists(), Ordering::Release);
    }
}

/// Law III & VII: Returns `true` ONLY when a single event breaches the `14,400 ppm` (`1.44%`) portfolio ceiling.
#[inline]
#[must_use]
pub const fn is_catastrophic_loss_breach(net_pnl_1e8: i64, reference_equity_1e8: i64) -> bool {
    if net_pnl_1e8 >= 0 {
        return false;
    }
    let eq = if reference_equity_1e8 > 100_000_000 {
        reference_equity_1e8
    } else {
        100_000_000
    };
    let loss_ppm = (((-net_pnl_1e8) as i128) * 1_000_000) / (eq as i128);
    loss_ppm >= (MAX_PORTFOLIO_RISK_PPM as i128)
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct IntraTradeSample {
    pub rel_sec: u16,
    pub mid_delta_sbps: i16,
    pub tp_delta_sbps: i16,
    pub sl_delta_sbps: i16,
    pub ofi_pm: i16,
    pub tfi_pm: i16,
    pub wald_shrink_pm: u16,
    pub zeta_tox_pm: u16,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct QueueAbortRecord {
    pub slot_idx: u8,
    pub side: Side,
    pub veto_code: LastVetoCode,
    pub armed_ts_ns: TimestampNs,
    pub aborted_ts_ns: TimestampNs,
    pub limit_ticks: PriceTicks,
    pub init_q_lots: LotQty,
    pub rem_q_lots: LotQty,
    pub saved_tox_bps: [ScaledBps; MARKOUT_HORIZONS_V49],
    pub resolved_mask: u8,
    pub is_true_positive: bool,
}

impl QueueAbortRecord {
    #[inline]
    #[must_use]
    pub const fn from_resolved_sample(slot_idx: u8, s: ResolvedAbortSample) -> Self {
        Self {
            slot_idx,
            side: s.side,
            veto_code: s.veto_code,
            armed_ts_ns: s.armed_ts_ns,
            aborted_ts_ns: s.aborted_ts_ns,
            limit_ticks: s.limit_ticks,
            init_q_lots: s.init_q_lots,
            rem_q_lots: s.rem_q_lots,
            saved_tox_bps: s.saved_tox_bps,
            resolved_mask: s.resolved_mask,
            is_true_positive: s.is_true_positive,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PositionPathSummary {
    pub trade_id: u64,
    pub slot_idx: u8,
    pub side: Side,
    pub opened_ts_ns: TimestampNs,
    pub closed_ts_ns: TimestampNs,
    pub entry_ticks: PriceTicks,
    pub exit_ticks: PriceTicks,
    pub net_scaled_bps: ScaledBps,
    pub markout_5h: MarkoutCurve5hBps,
    pub sample_count: u16,
    pub ws_rtt_us: u32,
    pub ws_quota_rem: u16,
    pub samples: [IntraTradeSample; MAX_INTRA_TRADE_SAMPLES],
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct PlasticityAuditSnapshot {
    pub ts_ns: i64,
    pub taker_prob_pm: [[i64; 2]; MAX_ACTIVE_PAIRS],
    pub learned_mfe_bps: [i64; MAX_ACTIVE_PAIRS],
    pub ob_bull_pm: [i64; MAX_ACTIVE_PAIRS],
    pub ob_bear_pm: [i64; MAX_ACTIVE_PAIRS],
    pub epps_corr_pm: [i16; MAX_ACTIVE_PAIRS],
    pub ws_rtt_us: u32,
    pub ws_quota_rem: u16,
}

#[derive(Clone, Debug)]
pub struct OpenPositionsPersistTask {
    pub engines: [SlotExecutionEngine; MAX_ACTIVE_PAIRS],
    pub entry_forensics: [SlotForensicEntrySnapshot; MAX_ACTIVE_PAIRS],
}

#[derive(Clone, Debug)]
pub struct ClosedTradePersistTask {
    pub slot_idx: usize,
    pub summary: ClosedTradeSummary,
    pub eng: SlotExecutionEngine,
    pub entry_forensics: SlotForensicEntrySnapshot,
    pub exit_snap: SymbolLobSnapshot,
    pub ledger: ParitySyncLedger,
    pub gov: OnlineAdaptiveGovernor,
    pub markout_curve: MarkoutCurveBps,
    pub lvr_5s_scaled_bps: i64,
    pub is_catastrophic_halt: bool,
    pub ts_ns: TimestampNs,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct FundingSettlementPersistTask {
    pub ts_ns: i64,
    pub slot_idx: usize,
    pub side: Side,
    pub funding_cut_id: i64,
    pub funding_rate_scaled_bps: i64,
    pub position_notional_1e8: i64,
    pub funding_net_1e8: i64,
    pub cum_funding_1e8: i64,
    pub cum_net_pnl_1e8: i64,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct EmaPrewarmPayload {
    pub ts_ns: i64,
    pub fast_px_1e4: [i64; 12],
    pub slow_px_1e4: [i64; 12],
    pub eq_vol_5m_bps: [i64; 12],
}

#[derive(Clone, Debug)]
pub enum VaultIoCommand {
    SaveOpenPositions(Box<OpenPositionsPersistTask>),
    RecordClosedTrade(Box<ClosedTradePersistTask>),
    RecordFundingSettlement(FundingSettlementPersistTask),
    RecordQueueAbort(QueueAbortRecord),
    RecordPositionPath(Box<PositionPathSummary>),
    SavePlasticityAudit(Box<PlasticityAuditSnapshot>),
    SaveDailyGovernor { ts_ns: i64, state: DailySessionGovernor },
    SaveEmaPrewarm(Box<EmaPrewarmPayload>),
    SaveObPlasticity { ts_ns: i64, bull_pm: [i64; 12], bear_pm: [i64; 12] },
    SaveMfePlasticity { ts_ns: i64, learned_mfe_bps: [i64; 12] },
    LatchTotalLossHalt {
        ts_ns: i64,
        anchor_equity_1e8: i64,
        current_equity_1e8: i64,
        epoch_loss_1e8: i64,
    },
    FlushBarrier(SyncSender<()>),
}

const _: () = assert!(core::mem::size_of::<QueueAbortRecord>() <= 88);
const _: () = assert!(core::mem::size_of::<VaultIoCommand>() <= 256);

/// Pure resolver returning `Some(root)` ONLY when `cwd` contains neither `data/vault` nor `Cargo.toml`,
/// `exe_path` resides directly inside a `bin/` directory, and the parent of `bin/` (`root`) contains
/// `data/vault` or `Cargo.toml`.
#[must_use]
pub fn resolve_project_root_from_exe_if_detached(
    cwd: &Path,
    exe_path: &Path,
) -> Option<std::path::PathBuf> {
    if cwd.join("data/vault").exists() || cwd.join("Cargo.toml").exists() {
        return None;
    }
    let bin_dir = exe_path.parent()?;
    if !bin_dir
        .file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|n| n.eq_ignore_ascii_case("bin"))
    {
        return None;
    }
    let root = bin_dir.parent()?;
    if root.join("data/vault").exists() || root.join("Cargo.toml").exists() {
        Some(root.to_path_buf())
    } else {
        None
    }
}

/// Auto-anchors the process working directory to the project root ONLY when neither `data/vault`
/// nor `Cargo.toml` exists in the current directory and `std::env::current_exe()` resides inside
/// `<root>/bin/` where `<root>` contains `data/vault` or `Cargo.toml`.
pub fn ensure_project_root_cwd_from_exe() {
    if Path::new("data/vault").exists() || Path::new("Cargo.toml").exists() {
        return;
    }
    let Ok(exe) = std::env::current_exe() else {
        return;
    };
    let Some(bin_dir) = exe.parent() else {
        return;
    };
    if !bin_dir
        .file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|n| n.eq_ignore_ascii_case("bin"))
    {
        return;
    }
    let Some(root) = bin_dir.parent() else {
        return;
    };
    if root.join("data/vault").exists() || root.join("Cargo.toml").exists() {
        let _ = std::env::set_current_dir(root);
    }
}

pub fn atomic_write_bytes_sync(target: &Path, payload: &[u8]) -> Result<(), VortexError> {
    static TMP_SEQ: AtomicU64 = AtomicU64::new(1);
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent).map_err(|_| VortexError::ColumnarIoFailure)?;
    }
    let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let base_name = target.file_name().and_then(|n| n.to_str()).unwrap_or("vault_state.json");
    let tmp_name = format!(".{base_name}.tmp.{pid}.{seq}");
    let tmp_path = match target.parent() {
        Some(p) => p.join(tmp_name),
        None => Path::new(&tmp_name).to_path_buf(),
    };
    {
        let mut f = File::create(&tmp_path).map_err(|_| VortexError::ColumnarIoFailure)?;
        f.write_all(payload).map_err(|_| VortexError::ColumnarIoFailure)?;
        let _ = f.sync_all();
    }
    if fs::rename(&tmp_path, target).is_err() {
        let _ = fs::remove_file(&tmp_path);
        return Err(VortexError::ColumnarIoFailure);
    }
    Ok(())
}

pub fn save_mfe_plasticity_to_path_atomic(path: &Path, ts_ns: i64, learned_mfe_bps: &[i64; 12]) -> Result<(), VortexError> {
    let payload = serde_json::json!({ "event": "MFE_PLASTICITY_V47", "ts_ns": ts_ns, "learned_mfe_bps": learned_mfe_bps });
    let txt = serde_json::to_string(&payload).map_err(|_| VortexError::ColumnarFormatCorrupted)?;
    atomic_write_bytes_sync(path, txt.as_bytes())
}

#[must_use]
pub fn load_mfe_plasticity_from_path(path: &Path, seed_mfe_bps: i64) -> [i64; 12] {
    let fallback = seed_mfe_bps.clamp(140, 9_600);
    let mut out = [fallback; 12];
    let Ok(raw) = fs::read_to_string(path) else { return out };
    let Ok(val) = serde_json::from_str::<serde_json::Value>(&raw) else { return out };
    if let Some(arr) = val.get("learned_mfe_bps").and_then(serde_json::Value::as_array) {
        for (idx, item) in arr.iter().take(12).enumerate() {
            if let Some(v) = item.as_i64() { out[idx] = v.clamp(140, 9_600); }
        }
    }
    out
}

pub fn save_daily_governor_to_path_atomic(path: &Path, ts_ns: TimestampNs, state: &DailySessionGovernor) -> Result<(), VortexError> {
    let txt = serde_json::to_string(&state.to_json_value(ts_ns)).map_err(|_| VortexError::ColumnarFormatCorrupted)?;
    atomic_write_bytes_sync(path, txt.as_bytes())
}

#[must_use]
pub fn load_daily_governor_from_path(path: &Path, initial_equity_1e8: i64) -> DailySessionGovernor {
    let mut gov = DailySessionGovernor::new(initial_equity_1e8);
    if let Ok(raw) = fs::read_to_string(path) {
        if let Ok(val) = serde_json::from_str::<serde_json::Value>(&raw) {
            gov.apply_from_json(&val);
        }
    }
    gov
}

pub fn append_jsonl_line_sync(target: &Path, json_line: &str) -> Result<u64, VortexError> {
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent).map_err(|_| VortexError::ColumnarIoFailure)?;
    }
    let trimmed = json_line.trim_end_matches(['\r', '\n']);
    let fnv = ColumnarParquetBatch::compute_fnv1a64(trimmed.as_bytes());
    let mut f = OpenOptions::new().create(true).append(true).open(target).map_err(|_| VortexError::ColumnarIoFailure)?;
    writeln!(f, "{trimmed}").map_err(|_| VortexError::ColumnarIoFailure)?;
    let _ = f.sync_data();
    Ok(fnv)
}

fn stamp_fnv1a64_json(mut val: serde_json::Value) -> String {
    let pre = val.to_string();
    let fnv = ColumnarParquetBatch::compute_fnv1a64(pre.as_bytes());
    if let Some(obj) = val.as_object_mut() {
        obj.insert("fnv1a64_hex".to_string(), serde_json::Value::String(format!("0x{fnv:016X}")));
    }
    val.to_string()
}

#[must_use]
pub fn build_funding_settlement_vault_json(task: &FundingSettlementPersistTask) -> String {
    let spec = APPROVED_SPECS[task.slot_idx.min(MAX_ACTIVE_PAIRS - 1)];
    let side_str = match task.side { Side::Bid => "Bid", Side::Ask => "Ask" };
    let notional = task.position_notional_1e8.max(1);
    let funding_bps = ((task.funding_net_1e8 as f64) * 10_000.0) / (notional as f64);
    stamp_fnv1a64_json(serde_json::json!({
        "event": "FUNDING_SETTLEMENT", "schema": "V5.5", "ts_ns": task.ts_ns,
        "funding_cut_id": task.funding_cut_id, "slot_idx": task.slot_idx,
        "symbol": spec.symbol_id.name(), "side": side_str,
        "funding_rate_bps": (task.funding_rate_scaled_bps as f64) / 100.0,
        "notional_1e8": notional, "funding_1e8": task.funding_net_1e8,
        "funding_bps": (funding_bps * 100.0).round() / 100.0,
        "cum_funding_1e8": task.cum_funding_1e8, "cum_net_pnl_1e8": task.cum_net_pnl_1e8,
    }))
}

#[must_use]
pub fn build_queue_abort_vault_json(rec: &QueueAbortRecord) -> String {
    let s_idx = usize::from(rec.slot_idx).min(MAX_ACTIVE_PAIRS - 1);
    let spec = APPROVED_SPECS[s_idx];
    let side_str = match rec.side { Side::Bid => "Bid", Side::Ask => "Ask" };
    let wait_ms = rec.aborted_ts_ns.0.saturating_sub(rec.armed_ts_ns.0).max(0) / 1_000_000;
    let saved_f: [f64; MARKOUT_HORIZONS_V49] = core::array::from_fn(|k| (rec.saved_tox_bps[k].raw() as f64) / 100.0);
    stamp_fnv1a64_json(serde_json::json!({
        "event": "QUEUE_ABORTED", "schema": "V5.5", "ts_ns": rec.aborted_ts_ns.0,
        "armed_ts_ns": rec.armed_ts_ns.0, "queue_wait_ms": wait_ms,
        "slot_idx": s_idx, "symbol": spec.symbol_id.name(), "side": side_str,
        "veto_code": rec.veto_code as u8, "veto_name": format!("{:?}", rec.veto_code),
        "limit_ticks": rec.limit_ticks.0, "init_q_lots": rec.init_q_lots.0, "rem_q_lots": rec.rem_q_lots.0,
        "saved_tox_5h_bps": saved_f, "resolved_mask": rec.resolved_mask, "true_positive": rec.is_true_positive,
    }))
}

#[must_use]
pub fn build_position_path_vault_json(path_sum: &PositionPathSummary) -> String {
    let s_idx = usize::from(path_sum.slot_idx).min(MAX_ACTIVE_PAIRS - 1);
    let spec = APPROVED_SPECS[s_idx];
    let side_str = match path_sum.side { Side::Bid => "Bid", Side::Ask => "Ask" };
    let n = usize::from(path_sum.sample_count).min(MAX_INTRA_TRADE_SAMPLES);
    let mut rows = Vec::with_capacity(n);
    for smp in &path_sum.samples[..n] {
        rows.push(serde_json::json!([
            smp.rel_sec, smp.mid_delta_sbps, smp.tp_delta_sbps, smp.sl_delta_sbps,
            smp.ofi_pm, smp.tfi_pm, smp.wald_shrink_pm, smp.zeta_tox_pm
        ]));
    }
    let m5_f: [f64; MARKOUT_HORIZONS_V49] = core::array::from_fn(|k| (path_sum.markout_5h.samples_bps[k] as f64) / 100.0);
    stamp_fnv1a64_json(serde_json::json!({
        "event": "POSITION_PATH_1HZ", "schema": "V5.5", "trade_id": path_sum.trade_id,
        "slot_idx": s_idx, "symbol": spec.symbol_id.name(), "side": side_str,
        "opened_ts_ns": path_sum.opened_ts_ns.0, "closed_ts_ns": path_sum.closed_ts_ns.0,
        "entry_ticks": path_sum.entry_ticks.0, "exit_ticks": path_sum.exit_ticks.0,
        "net_bps": (path_sum.net_scaled_bps.raw() as f64) / 100.0,
        "markout_5h_bps": m5_f, "markout_resolved_mask": path_sum.markout_5h.resolved_mask,
        "peak_zeta_tox": (f64::from(path_sum.markout_5h.peak_zeta_tox_pm)) / 1_000.0,
        "ws_rtt_us": path_sum.ws_rtt_us, "ws_quota_rem": path_sum.ws_quota_rem,
        "sample_count": n, "samples_1hz": rows,
    }))
}

#[must_use]
pub fn build_plasticity_audit_vault_json(snap: &PlasticityAuditSnapshot) -> String {
    stamp_fnv1a64_json(serde_json::json!({
        "event": "PLASTICITY_AUDIT_V49", "schema": "V5.5", "ts_ns": snap.ts_ns,
        "taker_prob_pm": snap.taker_prob_pm, "learned_mfe_bps": snap.learned_mfe_bps,
        "ob_bull_pm": snap.ob_bull_pm, "ob_bear_pm": snap.ob_bear_pm,
        "epps_corr_pm": snap.epps_corr_pm, "ws_rtt_us": snap.ws_rtt_us, "ws_quota_rem": snap.ws_quota_rem,
    }))
}

/// Rehydrates recent 12-pair Epps 5s BTC correlations (`epps_corr_pm`) from `vortex_plasticity_audit.jsonl`,
/// preferring converged warm snapshots over un-warmed cold boot rows (`[1000, 618, ...]`).
pub fn rehydrate_epps_trackers_from_audit_path(
    trackers: &mut [crate::alpha_3factor::EppsBtcCorrelationTracker; MAX_ACTIVE_PAIRS],
    path: &Path,
    now_ns: TimestampNs,
) -> usize {
    use std::io::{BufRead, BufReader};
    let Ok(file) = File::open(path) else {
        return 0;
    };
    let max_age_ns = (crate::alpha_3factor::PHI_CUBE * 3_600_000_000_000.0).round() as i64;
    let mut latest_any: Option<[i16; MAX_ACTIVE_PAIRS]> = None;
    let mut latest_warm: Option<[i16; MAX_ACTIVE_PAIRS]> = None;
    for line in BufReader::new(file).lines().map_while(Result::ok) {
        let Ok(val) = serde_json::from_str::<serde_json::Value>(&line) else {
            continue;
        };
        let row_ts = val.get("ts_ns").and_then(serde_json::Value::as_i64).unwrap_or(0);
        if row_ts <= 0 || (now_ns.0 > row_ts && now_ns.0.saturating_sub(row_ts) > max_age_ns) {
            continue;
        }
        let Some(arr) = val.get("epps_corr_pm").and_then(serde_json::Value::as_array) else {
            continue;
        };
        if arr.len() < MAX_ACTIVE_PAIRS {
            continue;
        }
        let mut parsed = [618_i16; MAX_ACTIVE_PAIRS];
        parsed[0] = 1_000;
        let mut has_warm_follower = false;
        for (idx, cell) in arr.iter().take(MAX_ACTIVE_PAIRS).enumerate() {
            if let Some(v) = cell.as_i64() {
                let c = v.clamp(-980, 1_000) as i16;
                parsed[idx] = if idx == 0 { 1_000 } else { c };
                if idx > 0 && c != 618 {
                    has_warm_follower = true;
                }
            }
        }
        latest_any = Some(parsed);
        if has_warm_follower {
            latest_warm = Some(parsed);
        }
    }
    let Some(chosen) = latest_warm.or(latest_any) else {
        return 0;
    };
    for (idx, trk) in trackers.iter_mut().enumerate() {
        trk.seed_correlation_permille(idx == 0, chosen[idx]);
    }
    MAX_ACTIVE_PAIRS
}

pub fn persist_funding_settlement_sync(task: &FundingSettlementPersistTask) {
    let line = build_funding_settlement_vault_json(task);
    let _ = append_jsonl_line_sync(Path::new(AI_FORENSIC_VAULT_PATH), &line);
}

pub fn persist_closed_trade_bundle_sync(task: &ClosedTradePersistTask, locks: Option<&AtomicLockWatch>) {
    let vault_line = build_closed_trade_vault_json(
        task.slot_idx, &task.summary, &task.eng, &task.entry_forensics,
        &task.exit_snap, &task.ledger, &task.markout_curve, task.lvr_5s_scaled_bps, task.ts_ns,
    );
    let scar_line = task.gov.to_scar_snapshot_json(task.ts_ns).to_string();
    let _ = append_jsonl_line_sync(Path::new(AI_FORENSIC_VAULT_PATH), &vault_line);
    let _ = append_jsonl_line_sync(Path::new(SCAR_MEMORY_VAULT_PATH), &scar_line);
    if task.summary.net_scaled_bps.raw() < -50 {
        let _ = append_jsonl_line_sync(Path::new(RED_LOSS_BOX_PATH), &vault_line);
    }
    if task.is_catastrophic_halt {
        if let Some(lw) = locks { lw.latch_halt_in_ram(); }
        let _ = atomic_write_bytes_sync(Path::new(HALT_LOCK_PATH), vault_line.as_bytes());
    }
}

pub fn persist_total_loss_halt_to_path_atomic(
    halt_path: &Path,
    ts_ns: i64,
    anchor_equity_1e8: i64,
    current_equity_1e8: i64,
    epoch_loss_1e8: i64,
    locks: Option<&AtomicLockWatch>,
) -> Result<(), VortexError> {
    if let Some(lw) = locks {
        lw.latch_halt_in_ram();
    }
    let payload = serde_json::json!({
        "event": "TOTAL_LOSS_SHIELD_5USDT_HALT",
        "version": "5.5.3",
        "ts_ns": ts_ns,
        "anchor_equity_1e8": anchor_equity_1e8,
        "current_equity_1e8": current_equity_1e8,
        "epoch_loss_1e8": epoch_loss_1e8,
        "max_epoch_loss_1e8": MAX_VALIDATION_EPOCH_LOSS_1E8,
    });
    let txt = format!("{payload}\n");
    atomic_write_bytes_sync(halt_path, txt.as_bytes())
}

fn handle_vault_io_cmd(
    cmd: VaultIoCommand,
    locks: &AtomicLockWatch,
    halt_path: &Path,
    drain_path: &Path,
) {
    match cmd {
        VaultIoCommand::SaveOpenPositions(task) => {
            if let Some(txt) = serialize_open_positions_state(&task.engines, &task.entry_forensics) {
                let _ = atomic_write_bytes_sync(Path::new(OPEN_POS_STATE_PATH), txt.as_bytes());
            }
        }
        VaultIoCommand::RecordClosedTrade(task) => persist_closed_trade_bundle_sync(&task, Some(locks)),
        VaultIoCommand::RecordFundingSettlement(task) => persist_funding_settlement_sync(&task),
        VaultIoCommand::RecordQueueAbort(rec) => {
            let line = build_queue_abort_vault_json(&rec);
            let _ = append_jsonl_line_sync(Path::new(QUEUE_ABORTS_VAULT_PATH), &line);
        }
        VaultIoCommand::RecordPositionPath(path_sum) => {
            let line = build_position_path_vault_json(&path_sum);
            let _ = append_jsonl_line_sync(Path::new(POSITION_PATHS_VAULT_PATH), &line);
        }
        VaultIoCommand::SavePlasticityAudit(audit) => {
            let line = build_plasticity_audit_vault_json(&audit);
            let _ = append_jsonl_line_sync(Path::new(PLASTICITY_AUDIT_VAULT_PATH), &line);
        }
        VaultIoCommand::SaveDailyGovernor { ts_ns, state } => {
            let _ = save_daily_governor_to_path_atomic(Path::new(DAILY_GOVERNOR_VAULT_PATH), TimestampNs(ts_ns), &state);
        }
        VaultIoCommand::SaveEmaPrewarm(pre) => {
            let payload = serde_json::json!({
                "event": "EMA_PREWARM_V43", "ts_ns": pre.ts_ns,
                "ewma_fast_px_1e4": pre.fast_px_1e4, "ewma_slow_px_1e4": pre.slow_px_1e4, "eq_vol_5m_bps": pre.eq_vol_5m_bps,
            });
            if let Ok(s) = serde_json::to_string(&payload) {
                let _ = atomic_write_bytes_sync(Path::new(EMA_PREWARM_PATH), s.as_bytes());
            }
        }
        VaultIoCommand::SaveObPlasticity { ts_ns, bull_pm, bear_pm } => {
            let _ = save_directional_plasticity_to_path_atomic(Path::new(OB_PLASTICITY_VAULT_PATH), ts_ns, &bull_pm, &bear_pm);
        }
        VaultIoCommand::SaveMfePlasticity { ts_ns, learned_mfe_bps } => {
            let _ = save_mfe_plasticity_to_path_atomic(Path::new(MFE_PLASTICITY_VAULT_PATH), ts_ns, &learned_mfe_bps);
        }
        VaultIoCommand::LatchTotalLossHalt {
            ts_ns,
            anchor_equity_1e8,
            current_equity_1e8,
            epoch_loss_1e8,
        } => {
            let _ = persist_total_loss_halt_to_path_atomic(
                halt_path,
                ts_ns,
                anchor_equity_1e8,
                current_equity_1e8,
                epoch_loss_1e8,
                Some(locks),
            );
        }
        VaultIoCommand::FlushBarrier(ack_tx) => {
            locks.refresh_from_fs(halt_path, drain_path);
            let _ = ack_tx.send(());
        }
    }
}

#[must_use]
pub fn spawn_vault_io_worker_with_lock_paths(
    locks: Arc<AtomicLockWatch>,
    halt_path: std::path::PathBuf,
    drain_path: std::path::PathBuf,
) -> SyncSender<VaultIoCommand> {
    let (tx, rx) = mpsc::sync_channel::<VaultIoCommand>(VAULT_IO_QUEUE_CAP);
    if let Some(parent) = halt_path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    locks.refresh_from_fs(&halt_path, &drain_path);
    thread::spawn(move || loop {
        match rx.recv_timeout(Duration::from_millis(250)) {
            Ok(cmd) => handle_vault_io_cmd(cmd, &locks, &halt_path, &drain_path),
            Err(mpsc::RecvTimeoutError::Timeout) => locks.refresh_from_fs(&halt_path, &drain_path),
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    });
    tx
}

#[must_use]
pub fn spawn_vault_io_worker(locks: Arc<AtomicLockWatch>) -> SyncSender<VaultIoCommand> {
    let _ = fs::create_dir_all("data/vault");
    spawn_vault_io_worker_with_lock_paths(
        locks,
        std::path::PathBuf::from(HALT_LOCK_PATH),
        std::path::PathBuf::from(DRAIN_LOCK_PATH),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_atomic_write_tmp_rename_jsonl_fnv_and_zero_ns_lock_watch() {
        let cmd_bytes = std::hint::black_box(core::mem::size_of::<VaultIoCommand>());
        assert!(cmd_bytes <= 256);
        assert!(!is_catastrophic_loss_breach(-30_211_283, 6_543_090_000));
        assert!(is_catastrophic_loss_breach(-120_000_000, 6_543_090_000));
        let dir = std::env::temp_dir().join(format!("vortex_vault_io_v49_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        assert!(fs::create_dir_all(&dir).is_ok());
        let dg_path = dir.join("daily_gov.json");
        let mut dg = DailySessionGovernor::new(6_513_770_000);
        let _ = dg.sync_utc_day(TimestampNs(1_794_790_000_000_000_000), 6_513_770_000);
        dg.day_realized_pnl_1e8 = 25_000_000;
        assert!(save_daily_governor_to_path_atomic(&dg_path, TimestampNs(1_794_790_100_000_000_000), &dg).is_ok());
        assert_eq!(load_daily_governor_from_path(&dg_path, 6_513_770_000).day_realized_pnl_1e8, 25_000_000);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_mfe_plasticity_atomic_roundtrip_and_bounds() {
        let p = std::env::temp_dir().join(format!("vortex_mfe_v49_{}.json", std::process::id()));
        let _ = fs::remove_file(&p);
        let src = [1_200, 950, 740, 680, 2_100, 520, 810, 640, 1_450, 700, 590, 510];
        assert!(save_mfe_plasticity_to_path_atomic(&p, 1_794_700_000, &src).is_ok());
        assert_eq!(load_mfe_plasticity_from_path(&p, 680), src);
        let _ = fs::remove_file(&p);
    }

    #[test]
    fn test_v49_queue_abort_position_path_and_plasticity_jsonl_fnv() {
        let ab = QueueAbortRecord {
            slot_idx: 3, side: Side::Bid, veto_code: LastVetoCode::HawkesStoikovQueueAbort,
            armed_ts_ns: TimestampNs(1_000_000_000), aborted_ts_ns: TimestampNs(2_500_000_000),
            limit_ticks: PriceTicks(7_200), init_q_lots: LotQty(500), rem_q_lots: LotQty(120),
            saved_tox_bps: [ScaledBps(50), ScaledBps(260), ScaledBps(410), ScaledBps(520), ScaledBps(890)],
            resolved_mask: 0x1F, is_true_positive: true,
        };
        let ab_j = build_queue_abort_vault_json(&ab);
        assert!(ab_j.contains("\"event\":\"QUEUE_ABORTED\"") && ab_j.contains("\"fnv1a64_hex\":\"0x"));
        let mut samples = [IntraTradeSample::default(); MAX_INTRA_TRADE_SAMPLES];
        samples[0] = IntraTradeSample { rel_sec: 1, mid_delta_sbps: 140, tp_delta_sbps: 520, sl_delta_sbps: -4_200, ofi_pm: 620, tfi_pm: 880, wald_shrink_pm: 250, zeta_tox_pm: 110 };
        let pp_j = build_position_path_vault_json(&PositionPathSummary {
            trade_id: 15, slot_idx: 1, side: Side::Bid, opened_ts_ns: TimestampNs(10), closed_ts_ns: TimestampNs(94_000_000_010),
            entry_ticks: PriceTicks(182_033), exit_ticks: PriceTicks(182_145), net_scaled_bps: ScaledBps(215),
            markout_5h: MarkoutCurve5hBps { samples_bps: [40, 80, 120, 210, 340], resolved_mask: 0x1F, initial_edge_bps: 25, peak_zeta_tox_pm: 90 },
            sample_count: 1, ws_rtt_us: 1_850, ws_quota_rem: 98, samples,
        });
        assert!(pp_j.contains("\"event\":\"POSITION_PATH_1HZ\"") && pp_j.contains("\"ws_rtt_us\":1850"));
        let pa_j = build_plasticity_audit_vault_json(&PlasticityAuditSnapshot {
            ts_ns: 99, taker_prob_pm: [[48; 2]; 12], learned_mfe_bps: [670; 12],
            ob_bull_pm: [520; 12], ob_bear_pm: [540; 12], epps_corr_pm: [610; 12], ws_rtt_us: 1_850, ws_quota_rem: 98,
        });
        assert!(pa_j.contains("\"event\":\"PLASTICITY_AUDIT_V49\"") && pa_j.contains("\"fnv1a64_hex\":\"0x"));
    }

    #[test]
    fn test_v551_ensure_project_root_cwd_from_exe_anchors_detached_bin_only() {
        ensure_project_root_cwd_from_exe();
        let tmp = std::env::temp_dir().join(format!("vortex_cwd_anchor_{}", std::process::id()));
        let _ = fs::remove_dir_all(&tmp);
        let detached_home = tmp.join("home_ubuntu");
        let deploy_root = detached_home.join("vortex-rs");
        let bin_dir = deploy_root.join("bin");
        let vault_dir = deploy_root.join("data/vault");
        let target_rel_dir = deploy_root.join("target/release");
        assert!(fs::create_dir_all(&bin_dir).is_ok());
        assert!(fs::create_dir_all(&vault_dir).is_ok());
        assert!(fs::create_dir_all(&target_rel_dir).is_ok());

        // 1. Detached CWD (/home/ubuntu) + deployed binary under <root>/bin/ -> anchors to <root>
        assert_eq!(
            resolve_project_root_from_exe_if_detached(&detached_home, &bin_dir.join("vortex_exec")),
            Some(deploy_root.clone())
        );
        // 2. CWD already at <root> (data/vault exists) -> no-op (None)
        assert_eq!(
            resolve_project_root_from_exe_if_detached(&deploy_root, &bin_dir.join("vortex_exec")),
            None
        );
        // 3. Binary inside target/release (not bin/) -> never touches CWD during cargo test / CI
        assert_eq!(
            resolve_project_root_from_exe_if_detached(&detached_home, &target_rel_dir.join("vortex_replay")),
            None
        );
        let _ = fs::remove_dir_all(&tmp);
    }
}
