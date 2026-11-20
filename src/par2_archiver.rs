//! Lossless Cold PAR2 Archiver (`.par2z` / `VPZ1`, Stride-256 XOR-Delta + Zero-RLE + FNV-1a 64-bit).
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::module_name_repetitions
)]

use crate::forensic_parquet::ColumnarParquetBatch;
use crate::types::VortexError;

pub const VPZ1_MAGIC: [u8; 4] = *b"VPZ1";
pub const VPZ1_HEADER_BYTES: usize = 36;
pub const PAR2_STRIDE_BYTES: usize = 256;
pub const MAX_COLD_PAR2Z_HOURLY_FILES: usize = 12;
const MAX_FRAME_ORIG_BYTES: usize = 4 * 1024 * 1024;

/// Compresses a raw `PAR1`/`PAR2` segment via bijective Stride-256 XOR-Delta + Zero-RLE.
#[must_use]
pub fn compress_par2_stride_zrle(raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(raw.len() / 2 + 16);
    let mut i = 0_usize;
    while i < raw.len() {
        let d = if i >= PAR2_STRIDE_BYTES { raw[i] ^ raw[i - PAR2_STRIDE_BYTES] } else { raw[i] };
        if d != 0 {
            out.push(d);
            i += 1;
        } else {
            let mut run = 1_usize;
            while run < 255 && (i + run) < raw.len() {
                let idx = i + run;
                let next_d = if idx >= PAR2_STRIDE_BYTES { raw[idx] ^ raw[idx - PAR2_STRIDE_BYTES] } else { raw[idx] };
                if next_d != 0 { break; }
                run += 1;
            }
            out.push(0);
            out.push(run as u8);
            i += run;
        }
    }
    out
}

/// Decompresses a `VPZ1` Stride-256 XOR-Delta + Zero-RLE payload to exact original length.
///
/// # Errors
/// Returns `VortexError::ColumnarFormatCorrupted` on truncated run-length or size mismatch.
pub fn decompress_par2_stride_zrle(comp: &[u8], expected_len: usize) -> Result<Vec<u8>, VortexError> {
    if expected_len > MAX_FRAME_ORIG_BYTES {
        return Err(VortexError::ColumnarFormatCorrupted);
    }
    let mut out = Vec::with_capacity(expected_len);
    let mut i = 0_usize;
    while i < comp.len() {
        let b = comp[i];
        if b != 0 {
            let prev = if out.len() >= PAR2_STRIDE_BYTES { out[out.len() - PAR2_STRIDE_BYTES] } else { 0 };
            out.push(b ^ prev);
            i += 1;
        } else {
            let Some(&run) = comp.get(i + 1) else {
                return Err(VortexError::ColumnarFormatCorrupted);
            };
            if run == 0 || out.len().saturating_add(usize::from(run)) > expected_len {
                return Err(VortexError::ColumnarFormatCorrupted);
            }
            for _ in 0..run {
                let prev = if out.len() >= PAR2_STRIDE_BYTES { out[out.len() - PAR2_STRIDE_BYTES] } else { 0 };
                out.push(prev);
            }
            i += 2;
        }
    }
    if out.len() != expected_len {
        return Err(VortexError::ColumnarFormatCorrupted);
    }
    Ok(out)
}

/// Encodes one verified segment into a 36-byte header `VPZ1` frame with dual `FNV-1a` 64-bit checksums.
///
/// # Errors
/// Returns `VortexError::ColumnarFormatCorrupted` if `raw` is empty or exceeds `MAX_FRAME_ORIG_BYTES`.
pub fn encode_vpz1_frame(segment_seq: u64, raw: &[u8]) -> Result<Vec<u8>, VortexError> {
    if raw.is_empty() || raw.len() > MAX_FRAME_ORIG_BYTES {
        return Err(VortexError::ColumnarFormatCorrupted);
    }
    let comp = compress_par2_stride_zrle(raw);
    let orig_crc = ColumnarParquetBatch::compute_fnv1a64(raw);
    let comp_crc = ColumnarParquetBatch::compute_fnv1a64(&comp);
    let mut frame = Vec::with_capacity(VPZ1_HEADER_BYTES + comp.len());
    frame.extend_from_slice(&VPZ1_MAGIC);
    frame.extend_from_slice(&segment_seq.to_le_bytes());
    frame.extend_from_slice(&(raw.len() as u32).to_le_bytes());
    frame.extend_from_slice(&(comp.len() as u32).to_le_bytes());
    frame.extend_from_slice(&orig_crc.to_le_bytes());
    frame.extend_from_slice(&comp_crc.to_le_bytes());
    frame.extend_from_slice(&comp);
    Ok(frame)
}

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

#[must_use]
#[allow(clippy::cast_sign_loss)]
pub const fn utc_ymd_hour_from_ns(ts_ns: i64) -> (i32, u32, u32, u32) {
    let secs = if ts_ns > 0 { ts_ns / 1_000_000_000 } else { 0 };
    let days = secs.div_euclid(86_400);
    let hour = (secs.rem_euclid(86_400) / 3_600) as u32;
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u32;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let mut y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    if m <= 2 { y += 1; }
    (y as i32, m, d, hour)
}

/// Decodes and verifies all `VPZ1` frames in a `.par2z` stream against both `FNV-1a` checksums.
///
/// # Errors
/// Returns `VortexError::ColumnarFormatCorrupted` on magic, length or `FNV-1a` mismatch.
pub fn decode_vpz1_stream(stream: &[u8]) -> Result<Vec<(u64, Vec<u8>)>, VortexError> {
    let mut pos = 0_usize;
    let mut out = Vec::new();
    while pos < stream.len() {
        let Some(hdr) = stream.get(pos..pos + VPZ1_HEADER_BYTES) else {
            if !out.is_empty() && stream[pos..].starts_with(&VPZ1_MAGIC[..stream.len().saturating_sub(pos).min(4)]) {
                break;
            }
            return Err(VortexError::ColumnarFormatCorrupted);
        };
        if hdr[0..4] != VPZ1_MAGIC {
            return Err(VortexError::ColumnarFormatCorrupted);
        }
        let seq = u64::from_le_bytes([hdr[4], hdr[5], hdr[6], hdr[7], hdr[8], hdr[9], hdr[10], hdr[11]]);
        let orig_len = u32::from_le_bytes([hdr[12], hdr[13], hdr[14], hdr[15]]) as usize;
        let comp_len = u32::from_le_bytes([hdr[16], hdr[17], hdr[18], hdr[19]]) as usize;
        let orig_crc = u64::from_le_bytes([hdr[20], hdr[21], hdr[22], hdr[23], hdr[24], hdr[25], hdr[26], hdr[27]]);
        let comp_crc = u64::from_le_bytes([hdr[28], hdr[29], hdr[30], hdr[31], hdr[32], hdr[33], hdr[34], hdr[35]]);
        let start = pos + VPZ1_HEADER_BYTES;
        let Some(comp_slice) = stream.get(start..start.saturating_add(comp_len)) else {
            if !out.is_empty() {
                break;
            }
            return Err(VortexError::ColumnarFormatCorrupted);
        };
        if ColumnarParquetBatch::compute_fnv1a64(comp_slice) != comp_crc {
            return Err(VortexError::ColumnarFormatCorrupted);
        }
        let raw = decompress_par2_stride_zrle(comp_slice, orig_len)?;
        if ColumnarParquetBatch::compute_fnv1a64(&raw) != orig_crc {
            return Err(VortexError::ColumnarFormatCorrupted);
        }
        out.push((seq, raw));
        pos = start + comp_len;
    }
    Ok(out)
}

#[must_use]
pub fn parse_segment_seq_from_path(path: &Path) -> u64 {
    path.file_stem()
        .and_then(|s| s.to_str())
        .and_then(|st| st.rsplit('_').next())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0)
}

/// Recursively scans `archive_root` for `*.par2z` files, retains the `max_hourly_files` most recent
/// files in lexicographical/chronological order (`YYYY-MM-DD/vortex_par2_HH.par2z`), deletes older
/// surplus `.par2z` files, and removes any empty date subdirectories. Returns the deleted file count.
pub fn prune_cold_par2z_archive(archive_root: &Path, max_hourly_files: usize) -> usize {
    if !archive_root.exists() {
        return 0;
    }
    let mut stack = vec![archive_root.to_path_buf()];
    let mut subdirs = Vec::new();
    let mut par2z_files: Vec<(String, PathBuf)> = Vec::new();
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                subdirs.push(p.clone());
                stack.push(p);
            } else if p
                .extension()
                .and_then(|s| s.to_str())
                .is_some_and(|ext| ext.eq_ignore_ascii_case("par2z"))
            {
                let rel_key = p
                    .strip_prefix(archive_root)
                    .unwrap_or(&p)
                    .components()
                    .map(|c| c.as_os_str().to_string_lossy())
                    .collect::<Vec<_>>()
                    .join("/");
                par2z_files.push((rel_key, p));
            }
        }
    }
    par2z_files.sort_by(|a, b| a.0.cmp(&b.0));
    let excess = par2z_files.len().saturating_sub(max_hourly_files);
    let mut removed = 0_usize;
    for (_, old_path) in par2z_files.into_iter().take(excess) {
        if fs::remove_file(&old_path).is_ok() {
            removed += 1;
        }
    }
    subdirs.sort_by(|a, b| b.cmp(a));
    for d in subdirs {
        if fs::read_dir(&d).is_ok_and(|mut it| it.next().is_none()) {
            let _ = fs::remove_dir(&d);
        }
    }
    removed
}

/// Archives one verified `.parquet` segment into `YYYY-MM-DD/vortex_par2_HH.par2z` before ring pruning,
/// invoking rolling cold-archive pruning whenever a new hourly `.par2z` file is created or every 64 segments.
///
/// # Errors
/// Returns `VortexError` if segment verification or `.par2z` append fails.
pub fn archive_parquet_segment_file(seg_path: &Path, archive_root: &Path) -> Result<PathBuf, VortexError> {
    static ARCHIVE_CALL_SEQ: core::sync::atomic::AtomicU64 = core::sync::atomic::AtomicU64::new(0);
    let (batch, _) = ColumnarParquetBatch::read_from_file(seg_path)?;
    let raw = fs::read(seg_path).map_err(|_| VortexError::ColumnarIoFailure)?;
    let ts_ns = batch.get_row(0).map_or(0, |r| r.ts_local_ns.0.max(r.ts_exchange_ns.0));
    let (y, m, d, hh) = utc_ymd_hour_from_ns(ts_ns);
    let day_dir = archive_root.join(format!("{y:04}-{m:02}-{d:02}"));
    fs::create_dir_all(&day_dir).map_err(|_| VortexError::ColumnarIoFailure)?;
    let target = day_dir.join(format!("vortex_par2_{hh:02}.par2z"));
    let created_new_hourly = !target.exists();
    let seg_seq = parse_segment_seq_from_path(seg_path);
    let frame = encode_vpz1_frame(seg_seq, &raw)?;
    {
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&target)
            .map_err(|_| VortexError::ColumnarIoFailure)?;
        f.write_all(&frame).map_err(|_| VortexError::ColumnarIoFailure)?;
    }
    let call_n = ARCHIVE_CALL_SEQ
        .fetch_add(1, core::sync::atomic::Ordering::Relaxed)
        .wrapping_add(1);
    if created_new_hourly
        || (seg_seq > 0 && seg_seq.is_multiple_of(64))
        || call_n.is_multiple_of(64)
    {
        let _ = prune_cold_par2z_archive(archive_root, MAX_COLD_PAR2Z_HOURLY_FILES);
    }
    Ok(target)
}

/// Restores up to `max_segments` chronological segments from `.par2z` archives into `out_dir`.
///
/// # Errors
/// Returns `VortexError` on I/O failure or `FNV-1a` frame corruption.
pub fn restore_par2z_archive_to_dir(
    archive_root: &Path, out_dir: &Path, max_segments: usize,
) -> Result<usize, VortexError> {
    if !archive_root.exists() || max_segments == 0 { return Ok(0); }
    fs::create_dir_all(out_dir).map_err(|_| VortexError::ColumnarIoFailure)?;
    let mut day_dirs: Vec<PathBuf> = fs::read_dir(archive_root)
        .map_err(|_| VortexError::ColumnarIoFailure)?
        .filter_map(|e| e.ok().map(|x| x.path()))
        .filter(|p| p.is_dir())
        .collect();
    day_dirs.sort();
    let mut restored = 0_usize;
    for d_dir in day_dirs {
        let mut files: Vec<PathBuf> = fs::read_dir(&d_dir)
            .map_err(|_| VortexError::ColumnarIoFailure)?
            .filter_map(|e| e.ok().map(|x| x.path()))
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("par2z"))
            .collect();
        files.sort();
        for p_file in files {
            let bytes = fs::read(&p_file).map_err(|_| VortexError::ColumnarIoFailure)?;
            for (seq, raw) in decode_vpz1_stream(&bytes)? {
                let id = if seq > 0 { seq } else { restored as u64 + 1 };
                let dst = out_dir.join(format!("vortex_l2_segment_{id:09}.parquet"));
                fs::write(&dst, &raw).map_err(|_| VortexError::ColumnarIoFailure)?;
                restored += 1;
                if restored >= max_segments { return Ok(restored); }
            }
        }
    }
    Ok(restored)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::forensic_parquet::{ColumnarMarketRow, MarketEventKind};
    use crate::lob_shm::{BookLevel, SymbolLobSnapshot};
    use crate::types::{LotQty, PriceTicks, Side, TimestampNs, APPROVED_SPECS};

    #[test]
    fn test_vpz1_lossless_par2_compression_and_multiday_restore() {
        let root = std::env::temp_dir().join(format!("vortex_par2z_{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let raw_dir = root.join("raw");
        let arc_dir = root.join("arc");
        let rst_dir = root.join("rst");
        assert!(fs::create_dir_all(&raw_dir).is_ok());

        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = APPROVED_SPECS[1].symbol_id;
        snap.ts_local_ns = TimestampNs(1_763_350_000_000_000_000);
        snap.bids[0] = BookLevel { price: PriceTicks(182_000), qty: LotQty(500) };
        snap.asks[0] = BookLevel { price: PriceTicks(182_001), qty: LotQty(500) };
        snap.mark_price = PriceTicks(182_000);

        let mut batch = ColumnarParquetBatch::new();
        for k in 0..64_i64 {
            snap.sequence_id = (k + 1) as u64;
            snap.bids[0].price = PriceTicks(182_000 + (k % 2));
            let row = ColumnarMarketRow::from_lob_snapshot(&snap, MarketEventKind::L2BookSnapshot, Side::Bid, snap.mark_price, LotQty(500));
            assert!(batch.push_row(&row).is_ok());
        }
        let seg_p = raw_dir.join("vortex_l2_segment_001234567.parquet");
        assert!(batch.write_to_file(&seg_p).is_ok());
        let orig_bytes = fs::read(&seg_p).unwrap_or_default();

        let par2z_p = archive_parquet_segment_file(&seg_p, &arc_dir);
        assert!(par2z_p.is_ok());
        let comp_bytes = fs::read(par2z_p.unwrap_or_default()).unwrap_or_default();
        assert!(comp_bytes.len() * 2 < orig_bytes.len());

        let mut tampered = comp_bytes.clone();
        let last_idx = tampered.len() - 1;
        tampered[last_idx] ^= 0x55;
        assert!(decode_vpz1_stream(&tampered).is_err());

        assert_eq!(restore_par2z_archive_to_dir(&arc_dir, &rst_dir, 16), Ok(1));
        let restored_bytes = fs::read(rst_dir.join("vortex_l2_segment_001234567.parquet")).unwrap_or_default();
        assert_eq!(restored_bytes, orig_bytes);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn test_v551_rolling_12_hour_par2z_cap_prunes_15_hours_and_empty_day_dirs() {
        let root = std::env::temp_dir().join(format!("vortex_par2z_15h_{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let raw_dir = root.join("raw");
        let arc_dir = root.join("arc");
        let rst_dir = root.join("rst");
        assert!(fs::create_dir_all(&raw_dir).is_ok());

        // Start at 21:00 UTC so hours 0..=2 land on Day 1 (21h, 22h, 23h) and hours 3..=14 (12 hours) land on Day 2 (00h..=11h).
        let base_day_ns = (20_776_i64 * 86_400 + 21 * 3_600) * 1_000_000_000;
        let (y1, m1, d1, _) = utc_ymd_hour_from_ns(base_day_ns);
        let day1_dir = arc_dir.join(format!("{y1:04}-{m1:02}-{d1:02}"));

        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = APPROVED_SPECS[0].symbol_id;
        snap.bids[0] = BookLevel { price: PriceTicks(652_000), qty: LotQty(300) };
        snap.asks[0] = BookLevel { price: PriceTicks(652_001), qty: LotQty(300) };
        snap.mark_price = PriceTicks(652_000);

        for hr in 0..15_i64 {
            let ts_ns = base_day_ns + hr * 3_600_000_000_000;
            snap.sequence_id = (hr + 1) as u64;
            snap.ts_local_ns = TimestampNs(ts_ns);
            snap.ts_exchange_ns = TimestampNs(ts_ns);
            let mut batch = ColumnarParquetBatch::new();
            let row = ColumnarMarketRow::from_lob_snapshot(
                &snap,
                MarketEventKind::L2BookSnapshot,
                Side::Bid,
                snap.mark_price,
                LotQty(300),
            );
            assert!(batch.push_row(&row).is_ok());
            let seq = (hr + 1) as u64;
            let seg_p = raw_dir.join(format!("vortex_l2_segment_{seq:06}.parquet"));
            assert!(batch.write_to_file(&seg_p).is_ok());
            assert!(archive_parquet_segment_file(&seg_p, &arc_dir).is_ok());
        }

        // Day 1 had hours 0, 1, 2 (3 oldest hours out of 15) -> all 3 pruned and empty Day 1 subdir deleted!
        assert!(!day1_dir.exists());

        let mut kept_rel: Vec<String> = Vec::new();
        for entry in fs::read_dir(&arc_dir).into_iter().flatten().flatten() {
            let p = entry.path();
            if p.is_dir() {
                for sub in fs::read_dir(&p).into_iter().flatten().flatten() {
                    let sp = sub.path();
                    if sp.extension().and_then(|s| s.to_str()) == Some("par2z") {
                        let rel = sp
                            .strip_prefix(&arc_dir)
                            .unwrap_or(&sp)
                            .components()
                            .map(|c| c.as_os_str().to_string_lossy())
                            .collect::<Vec<_>>()
                            .join("/");
                        kept_rel.push(rel);
                    }
                }
            }
        }
        kept_rel.sort();
        assert_eq!(kept_rel.len(), MAX_COLD_PAR2Z_HOURLY_FILES);
        for (idx, rel) in kept_rel.iter().enumerate() {
            assert!(rel.ends_with(&format!("vortex_par2_{idx:02}.par2z")));
        }

        // Restoring all kept frames yields the exact 12 most recent segments (4..=15):
        assert_eq!(restore_par2z_archive_to_dir(&arc_dir, &rst_dir, 32), Ok(12));
        assert!(!rst_dir.join("vortex_l2_segment_000000003.parquet").exists());
        assert!(rst_dir.join("vortex_l2_segment_000000004.parquet").exists());
        assert!(rst_dir.join("vortex_l2_segment_000000015.parquet").exists());

        let _ = fs::remove_dir_all(&root);
    }
}
