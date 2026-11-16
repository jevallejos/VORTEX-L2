//! Zero-Jitter Off-Thread Atomic Vault Persister (V4.8: Cellular vs Catastrophic Halt, 8h Funding & Daily Shield).

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
use crate::lob_shm::{SymbolLobSnapshot, MAX_ACTIVE_PAIRS};
use crate::markout_tracker::MarkoutCurveBps;
use crate::order_block::{save_directional_plasticity_to_path_atomic, OB_PLASTICITY_VAULT_PATH};
use crate::telemetry_hud::{
    build_closed_trade_vault_json, serialize_open_positions_state, SlotForensicEntrySnapshot,
    AI_FORENSIC_VAULT_PATH, DRAIN_LOCK_PATH, HALT_LOCK_PATH, OPEN_POS_STATE_PATH,
    RED_LOSS_BOX_PATH, SCAR_MEMORY_VAULT_PATH,
};
use crate::types::{Side, TimestampNs, VortexError, APPROVED_SPECS};

pub const VAULT_IO_QUEUE_CAP: usize = 64;
pub const MFE_PLASTICITY_VAULT_PATH: &str = "data/vault/vortex_mfe_plasticity.json";

#[derive(Debug)]
pub struct AtomicLockWatch {
    halt_locked: AtomicBool,
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
            drain_active: AtomicBool::new(drain),
        }
    }

    #[inline] #[must_use] pub fn is_halt_locked(&self) -> bool { self.halt_locked.load(Ordering::Acquire) }
    #[inline] #[must_use] pub fn is_drain_active(&self) -> bool { self.drain_active.load(Ordering::Acquire) }
    #[inline] pub fn latch_halt_in_ram(&self) { self.halt_locked.store(true, Ordering::Release); }

    pub fn refresh_from_fs(&self, halt_path: &Path, drain_path: &Path) {
        self.halt_locked.store(halt_path.exists(), Ordering::Release);
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
    let eq = if reference_equity_1e8 > 100_000_000 { reference_equity_1e8 } else { 100_000_000 };
    let loss_ppm = (((-net_pnl_1e8) as i128) * 1_000_000) / (eq as i128);
    loss_ppm >= (MAX_PORTFOLIO_RISK_PPM as i128)
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

#[derive(Clone, Debug)]
pub enum VaultIoCommand {
    SaveOpenPositions(Box<OpenPositionsPersistTask>),
    RecordClosedTrade(Box<ClosedTradePersistTask>),
    RecordFundingSettlement(FundingSettlementPersistTask),
    SaveDailyGovernor { ts_ns: i64, state: DailySessionGovernor },
    SaveEmaPrewarm {
        ts_ns: i64,
        fast_px_1e4: [i64; 12],
        slow_px_1e4: [i64; 12],
        eq_vol_5m_bps: [i64; 12],
    },
    SaveObPlasticity { ts_ns: i64, bull_pm: [i64; 12], bear_pm: [i64; 12] },
    SaveMfePlasticity { ts_ns: i64, learned_mfe_bps: [i64; 12] },
    FlushBarrier(SyncSender<()>),
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

#[must_use]
pub fn build_funding_settlement_vault_json(task: &FundingSettlementPersistTask) -> String {
    let spec = APPROVED_SPECS[task.slot_idx.min(MAX_ACTIVE_PAIRS - 1)];
    let side_str = match task.side { Side::Bid => "Bid", Side::Ask => "Ask" };
    let notional = task.position_notional_1e8.max(1);
    let funding_bps = ((task.funding_net_1e8 as f64) * 10_000.0) / (notional as f64);
    let mut val = serde_json::json!({
        "event": "FUNDING_SETTLEMENT",
        "schema": "V4.8",
        "ts_ns": task.ts_ns,
        "funding_cut_id": task.funding_cut_id,
        "slot_idx": task.slot_idx,
        "symbol": spec.symbol_id.name(),
        "side": side_str,
        "funding_rate_bps": (task.funding_rate_scaled_bps as f64) / 100.0,
        "notional_1e8": notional,
        "funding_1e8": task.funding_net_1e8,
        "funding_bps": (funding_bps * 100.0).round() / 100.0,
        "cum_funding_1e8": task.cum_funding_1e8,
        "cum_net_pnl_1e8": task.cum_net_pnl_1e8,
    });
    let pre = val.to_string();
    let fnv = ColumnarParquetBatch::compute_fnv1a64(pre.as_bytes());
    if let Some(obj) = val.as_object_mut() {
        obj.insert("fnv1a64_hex".to_string(), serde_json::Value::String(format!("0x{fnv:016X}")));
    }
    val.to_string()
}

pub fn persist_funding_settlement_sync(task: &FundingSettlementPersistTask) {
    let line = build_funding_settlement_vault_json(task);
    let _ = append_jsonl_line_sync(Path::new(AI_FORENSIC_VAULT_PATH), &line);
}

pub fn persist_closed_trade_bundle_sync(task: &ClosedTradePersistTask, locks: Option<&AtomicLockWatch>) {
    let vault_line = build_closed_trade_vault_json(
        task.slot_idx,
        &task.summary,
        &task.eng,
        &task.entry_forensics,
        &task.exit_snap,
        &task.ledger,
        &task.markout_curve,
        task.lvr_5s_scaled_bps,
        task.ts_ns,
    );
    let scar_line = task.gov.to_scar_snapshot_json(task.ts_ns).to_string();
    let is_red_loss = task.summary.net_scaled_bps.raw() < -50;
    let _ = append_jsonl_line_sync(Path::new(AI_FORENSIC_VAULT_PATH), &vault_line);
    let _ = append_jsonl_line_sync(Path::new(SCAR_MEMORY_VAULT_PATH), &scar_line);
    if is_red_loss {
        let _ = append_jsonl_line_sync(Path::new(RED_LOSS_BOX_PATH), &vault_line);
    }
    if task.is_catastrophic_halt {
        if let Some(lw) = locks {
            lw.latch_halt_in_ram();
        }
        let _ = atomic_write_bytes_sync(Path::new(HALT_LOCK_PATH), vault_line.as_bytes());
    }
}

fn handle_vault_io_cmd(cmd: VaultIoCommand, locks: &AtomicLockWatch) {
    match cmd {
        VaultIoCommand::SaveOpenPositions(task) => {
            if let Some(txt) = serialize_open_positions_state(&task.engines, &task.entry_forensics) {
                let _ = atomic_write_bytes_sync(Path::new(OPEN_POS_STATE_PATH), txt.as_bytes());
            }
        }
        VaultIoCommand::RecordClosedTrade(task) => {
            persist_closed_trade_bundle_sync(&task, Some(locks));
        }
        VaultIoCommand::RecordFundingSettlement(task) => {
            persist_funding_settlement_sync(&task);
        }
        VaultIoCommand::SaveDailyGovernor { ts_ns, state } => {
            let _ = save_daily_governor_to_path_atomic(Path::new(DAILY_GOVERNOR_VAULT_PATH), TimestampNs(ts_ns), &state);
        }
        VaultIoCommand::SaveEmaPrewarm { ts_ns, fast_px_1e4, slow_px_1e4, eq_vol_5m_bps } => {
            let payload = serde_json::json!({
                "event": "EMA_PREWARM_V43",
                "ts_ns": ts_ns,
                "ewma_fast_px_1e4": fast_px_1e4,
                "ewma_slow_px_1e4": slow_px_1e4,
                "eq_vol_5m_bps": eq_vol_5m_bps,
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
        VaultIoCommand::FlushBarrier(ack_tx) => {
            locks.refresh_from_fs(Path::new(HALT_LOCK_PATH), Path::new(DRAIN_LOCK_PATH));
            let _ = ack_tx.send(());
        }
    }
}

#[must_use]
pub fn spawn_vault_io_worker(locks: Arc<AtomicLockWatch>) -> SyncSender<VaultIoCommand> {
    let (tx, rx) = mpsc::sync_channel::<VaultIoCommand>(VAULT_IO_QUEUE_CAP);
    let _ = fs::create_dir_all("data/vault");
    locks.refresh_from_fs(Path::new(HALT_LOCK_PATH), Path::new(DRAIN_LOCK_PATH));
    thread::spawn(move || loop {
        match rx.recv_timeout(Duration::from_millis(250)) {
            Ok(cmd) => handle_vault_io_cmd(cmd, &locks),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                locks.refresh_from_fs(Path::new(HALT_LOCK_PATH), Path::new(DRAIN_LOCK_PATH));
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    });
    tx
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_atomic_write_tmp_rename_jsonl_fnv_and_zero_ns_lock_watch() {
        let cmd_bytes = std::hint::black_box(core::mem::size_of::<VaultIoCommand>());
        assert!(cmd_bytes <= 512);
        assert!(!is_catastrophic_loss_breach(-30_211_283, 6_543_090_000));
        assert!(is_catastrophic_loss_breach(-120_000_000, 6_543_090_000));

        let dir = std::env::temp_dir().join(format!("vortex_vault_io_v48_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        assert!(fs::create_dir_all(&dir).is_ok());
        let json_target = dir.join("state_atomic.json");
        assert!(atomic_write_bytes_sync(&json_target, b"{\"v\":2,\"ok\":true}").is_ok());
        assert_eq!(fs::read_to_string(&json_target).unwrap_or_default(), "{\"v\":2,\"ok\":true}");

        let dg_path = dir.join("daily_gov.json");
        let mut dg = DailySessionGovernor::new(6_513_770_000);
        let _ = dg.sync_utc_day(TimestampNs(1_794_790_000_000_000_000), 6_513_770_000);
        dg.day_realized_pnl_1e8 = 25_000_000;
        assert!(save_daily_governor_to_path_atomic(&dg_path, TimestampNs(1_794_790_100_000_000_000), &dg).is_ok());
        assert_eq!(load_daily_governor_from_path(&dg_path, 6_513_770_000).day_realized_pnl_1e8, 25_000_000);

        let f_line = build_funding_settlement_vault_json(&FundingSettlementPersistTask {
            ts_ns: 1_794_787_200_000_000_000, slot_idx: 3, side: Side::Ask, funding_cut_id: 62_319,
            funding_rate_scaled_bps: 150, position_notional_1e8: 1_800_000_000, funding_net_1e8: 270_000,
            cum_funding_1e8: 270_000, cum_net_pnl_1e8: 270_000,
        });
        assert!(f_line.contains("\"event\":\"FUNDING_SETTLEMENT\"") && f_line.contains("\"fnv1a64_hex\":\"0x"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_mfe_plasticity_atomic_roundtrip_and_bounds() {
        let p = std::env::temp_dir().join(format!("vortex_mfe_v48_{}.json", std::process::id()));
        let _ = fs::remove_file(&p);
        assert_eq!(load_mfe_plasticity_from_path(&p, 680), [680; 12]);
        let src = [1_200, 950, 740, 680, 2_100, 520, 810, 640, 1_450, 700, 590, 510];
        assert!(save_mfe_plasticity_to_path_atomic(&p, 1_794_700_000, &src).is_ok());
        assert_eq!(load_mfe_plasticity_from_path(&p, 680), src);
        let _ = fs::remove_file(&p);
    }
}
