//! Zero-Jitter Off-Thread Atomic Vault Persister (Write-Tmp-Fsync-Rename) & RAM Lock Watch.

use core::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::mpsc::{self, SyncSender};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::bayesian_governor::EMA_PREWARM_PATH;
use crate::forensic_parquet::ColumnarParquetBatch;
use crate::order_block::OB_PLASTICITY_VAULT_PATH;
use crate::telemetry_hud::{
    AI_FORENSIC_VAULT_PATH, DRAIN_LOCK_PATH, HALT_LOCK_PATH, OPEN_POS_STATE_PATH,
    RED_LOSS_BOX_PATH, SCAR_MEMORY_VAULT_PATH,
};
use crate::types::VortexError;

pub const VAULT_IO_QUEUE_CAP: usize = 128;
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

    #[inline]
    #[must_use]
    pub fn is_halt_locked(&self) -> bool {
        self.halt_locked.load(Ordering::Acquire)
    }

    #[inline]
    #[must_use]
    pub fn is_drain_active(&self) -> bool {
        self.drain_active.load(Ordering::Acquire)
    }

    #[inline]
    pub fn latch_halt_in_ram(&self) {
        self.halt_locked.store(true, Ordering::Release);
    }

    pub fn refresh_from_fs(&self, halt_path: &Path, drain_path: &Path) {
        self.halt_locked
            .store(halt_path.exists(), Ordering::Release);
        self.drain_active
            .store(drain_path.exists(), Ordering::Release);
    }
}

#[derive(Clone, Debug)]
pub enum VaultIoCommand {
    SaveOpenPositionsJson(String),
    SaveEmaPrewarm {
        ts_ns: i64,
        fast_px_1e4: [i64; 12],
        slow_px_1e4: [i64; 12],
        eq_vol_5m_bps: [i64; 12],
    },
    SaveObPlasticity {
        ts_ns: i64,
        bull_pm: [i64; 12],
        bear_pm: [i64; 12],
    },
    SaveMfePlasticity {
        ts_ns: i64,
        learned_mfe_bps: [i64; 12],
    },
    AppendClosedTrade {
        vault_line: String,
        scar_line: String,
        is_red_loss: bool,
    },
    FlushBarrier(SyncSender<()>),
}

pub fn atomic_write_bytes_sync(target: &Path, payload: &[u8]) -> Result<(), VortexError> {
    static TMP_SEQ: AtomicU64 = AtomicU64::new(1);
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent).map_err(|_| VortexError::ColumnarIoFailure)?;
    }
    let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let base_name = target
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("vault_state.json");
    let tmp_name = format!(".{base_name}.tmp.{pid}.{seq}");
    let tmp_path = match target.parent() {
        Some(p) => p.join(tmp_name),
        None => Path::new(&tmp_name).to_path_buf(),
    };
    {
        let mut f = File::create(&tmp_path).map_err(|_| VortexError::ColumnarIoFailure)?;
        f.write_all(payload)
            .map_err(|_| VortexError::ColumnarIoFailure)?;
        let _ = f.sync_all();
    }
    if fs::rename(&tmp_path, target).is_err() {
        let _ = fs::remove_file(&tmp_path);
        return Err(VortexError::ColumnarIoFailure);
    }
    Ok(())
}

pub fn save_mfe_plasticity_to_path_atomic(
    path: &Path,
    ts_ns: i64,
    learned_mfe_bps: &[i64; 12],
) -> Result<(), VortexError> {
    let payload = serde_json::json!({
        "event": "MFE_PLASTICITY_V45",
        "ts_ns": ts_ns,
        "learned_mfe_bps": learned_mfe_bps,
    });
    let txt = serde_json::to_string(&payload).map_err(|_| VortexError::ColumnarFormatCorrupted)?;
    atomic_write_bytes_sync(path, txt.as_bytes())
}

#[must_use]
pub fn load_mfe_plasticity_from_path(path: &Path, seed_mfe_bps: i64) -> [i64; 12] {
    let fallback = seed_mfe_bps.clamp(140, 9_600);
    let mut out = [fallback; 12];
    let Ok(raw) = fs::read_to_string(path) else {
        return out;
    };
    let Ok(val) = serde_json::from_str::<serde_json::Value>(&raw) else {
        return out;
    };
    if let Some(arr) = val.get("learned_mfe_bps").and_then(serde_json::Value::as_array) {
        for (idx, item) in arr.iter().take(12).enumerate() {
            if let Some(v) = item.as_i64() {
                out[idx] = v.clamp(140, 9_600);
            }
        }
    }
    out
}

pub fn append_jsonl_line_sync(target: &Path, json_line: &str) -> Result<u64, VortexError> {
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent).map_err(|_| VortexError::ColumnarIoFailure)?;
    }
    let trimmed = json_line.trim_end_matches(['\r', '\n']);
    let fnv = ColumnarParquetBatch::compute_fnv1a64(trimmed.as_bytes());
    let mut f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(target)
        .map_err(|_| VortexError::ColumnarIoFailure)?;
    writeln!(f, "{trimmed}").map_err(|_| VortexError::ColumnarIoFailure)?;
    let _ = f.sync_data();
    Ok(fnv)
}

fn handle_vault_io_cmd(cmd: VaultIoCommand, locks: &AtomicLockWatch) {
    match cmd {
        VaultIoCommand::SaveOpenPositionsJson(txt) => {
            let _ = atomic_write_bytes_sync(Path::new(OPEN_POS_STATE_PATH), txt.as_bytes());
        }
        VaultIoCommand::SaveEmaPrewarm {
            ts_ns,
            fast_px_1e4,
            slow_px_1e4,
            eq_vol_5m_bps,
        } => {
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
        VaultIoCommand::SaveObPlasticity {
            ts_ns,
            bull_pm,
            bear_pm,
        } => {
            let mut mean_pm = [0_i64; 12];
            for i in 0..12 {
                mean_pm[i] = i64::midpoint(bull_pm[i], bear_pm[i]);
            }
            let payload = serde_json::json!({
                "event": "OB_PLASTICITY_V45",
                "ts_ns": ts_ns,
                "learned_efficacy_pm": mean_pm,
                "learned_bull_pm": bull_pm,
                "learned_bear_pm": bear_pm,
            });
            if let Ok(s) = serde_json::to_string(&payload) {
                let _ = atomic_write_bytes_sync(Path::new(OB_PLASTICITY_VAULT_PATH), s.as_bytes());
            }
        }
        VaultIoCommand::SaveMfePlasticity {
            ts_ns,
            learned_mfe_bps,
        } => {
            let _ = save_mfe_plasticity_to_path_atomic(
                Path::new(MFE_PLASTICITY_VAULT_PATH),
                ts_ns,
                &learned_mfe_bps,
            );
        }
        VaultIoCommand::AppendClosedTrade {
            vault_line,
            scar_line,
            is_red_loss,
        } => {
            let _ = append_jsonl_line_sync(Path::new(AI_FORENSIC_VAULT_PATH), &vault_line);
            let _ = append_jsonl_line_sync(Path::new(SCAR_MEMORY_VAULT_PATH), &scar_line);
            if is_red_loss {
                locks.latch_halt_in_ram();
                let _ = append_jsonl_line_sync(Path::new(RED_LOSS_BOX_PATH), &vault_line);
                let _ = atomic_write_bytes_sync(Path::new(HALT_LOCK_PATH), vault_line.as_bytes());
            }
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
    let halt_p = Path::new(HALT_LOCK_PATH);
    let drain_p = Path::new(DRAIN_LOCK_PATH);
    locks.refresh_from_fs(halt_p, drain_p);
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
        let dir = std::env::temp_dir().join(format!("vortex_vault_io_t42a_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        assert!(fs::create_dir_all(&dir).is_ok());

        let json_target = dir.join("state_atomic.json");
        assert!(atomic_write_bytes_sync(&json_target, b"{\"v\":1}").is_ok());
        assert!(atomic_write_bytes_sync(&json_target, b"{\"v\":2,\"ok\":true}").is_ok());
        assert_eq!(
            fs::read_to_string(&json_target).unwrap_or_default(),
            "{\"v\":2,\"ok\":true}"
        );

        let jsonl_target = dir.join("audit.jsonl");
        let h1 = append_jsonl_line_sync(&jsonl_target, "{\"event\":\"A\"}");
        let h2 = append_jsonl_line_sync(&jsonl_target, "{\"event\":\"B\"}\n");
        assert!(h1.is_ok() && h2.is_ok() && h1 != h2);

        let halt_f = dir.join("HALT.lock");
        let drain_f = dir.join("DRAIN.lock");
        let watch = Arc::new(AtomicLockWatch::new(false, false));
        watch.latch_halt_in_ram();
        assert!(watch.is_halt_locked());
        assert!(atomic_write_bytes_sync(&drain_f, b"1").is_ok());
        watch.refresh_from_fs(&halt_f, &drain_f);
        assert!(!watch.is_halt_locked() && watch.is_drain_active());

        let tx = spawn_vault_io_worker(Arc::clone(&watch));
        let (ack_tx, ack_rx) = mpsc::sync_channel::<()>(1);
        assert!(tx.send(VaultIoCommand::FlushBarrier(ack_tx)).is_ok());
        assert!(ack_rx.recv_timeout(Duration::from_secs(2)).is_ok());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_mfe_plasticity_atomic_roundtrip_and_bounds() {
        let p = std::env::temp_dir().join(format!("vortex_mfe_v45_{}.json", std::process::id()));
        let _ = fs::remove_file(&p);
        assert_eq!(load_mfe_plasticity_from_path(&p, 680), [680; 12]);
        let src = [1_200, 950, 740, 680, 2_100, 520, 810, 640, 1_450, 700, 590, 510];
        assert!(save_mfe_plasticity_to_path_atomic(&p, 1_794_700_000, &src).is_ok());
        assert_eq!(load_mfe_plasticity_from_path(&p, 680), src);
        let _ = fs::remove_file(&p);
    }
}
