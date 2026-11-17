# VORTEX-OMEGA V4.9 — Autonomous 200.0 Hz Maker-First HFT Engine & Loopback Observatory

**Crate:** `vortex-rs` v1.0.0 (Rust 2021, Zero-Alloc Hot Path, `LTO = "fat"`, `panic = "abort"`)
**Shared Memory Contract:** `VORTEX_SHM_VERSION = 15` (`/dev/shm/vortex_lob.mmap`, 25 KB Tear-Free SeqLock)
**Verification Status:** `72/72` Deterministic Tests OK | `Clippy -D warnings = 0` | `SCAR-LOCK 63/63` | `OOS 520` (`PF >= 6.89`, `Maker = 85.00%`, `PARITY_SYNC Drift == 0`)

## 1. Official 4-Binary Architecture (Zero-Jitter Priority Isolation)

1. `vortex_tap` (`Nice=-10`): Immortal 12-pair Bybit UTA V5 L2/Trade/Ticker/Liquidation ingestor, `SHM v15` publisher, lossless `PAR2` ring writer (`256 B/row`), `.par2z` cold archiver hook, and off-thread `VKL1` multi-timeframe candle aggregator (`1s..1d`).
2. `vortex_exec` (`Nice=-15`): 200.0 Hz (`5 ms`) autonomous 12-slot `PostOnly` custodian, C1 elastic `EV > 0` gate, `DailySessionGovernor` UTC shield, `70/30` Sinking-Fund Vault, Glosten-Milgrom + Wald SPRT v2.0 exits, and `1 Hz` intra-trade path telemetry.
3. `vortex_replay` (`Nice=19` on demand): 4-arm Pareto ablation verifier (`+25.80 < +44.88 < +158.28 < +482.07 bps`), `1:1` Digital Twin (`25 ms` RTT FIFO), and multi-day cold `.par2z` walk-forward engine (`--archive-dir`).
4. `vortex_web` (`Nice=19`, `IOSchedulingClass=idle`): Read-only `127.0.0.1:8787` HTTP + `5 Hz` SSE bridge with `X-Vortex-Csrf` protection, embedded TradingView Lightweight Charts v5 UI (`ui/vortex_observatory.html`), `1 Hz` trade player, ANSI-free Gemini clipboard export, `DRAIN_MODE` (`P0-B`) toggle, and `LAB 1..4` runner.

## 2. Perpetual Storage & Deep Telemetry (V4.9)

- **VKL1 Enriched Candles (`src/candle_store.rs`):** `96 B/record` (`#[repr(C)]` + `FNV-1a 64-bit`), `O(log N)` disk binary search across `1s, 1m, 5m, 15m, 1h, 4h, 1d`, Bybit `/v5/market/kline` backfill, and `< 1 ms` local prewarm fallback.
- **Lossless Cold Archive (`src/par2_archiver.rs`):** `VPZ1` Stride-256 XOR-Delta + Zero-RLE compression with dual `FNV-1a` verification in `data/archive/YYYY-MM-DD/vortex_par2_HH.par2z`.
- **Off-Thread Vault Persistence (`src/vault_io.rs`):** Atomic `.tmp -> sync_all -> rename` (`VaultIoCommand <= 512 B`) for `vortex_ai_forensic_vault.jsonl` (`5h` markouts up to `+60s`), `vortex_queue_aborts.jsonl`, `vortex_position_paths.jsonl` (`1 Hz`), and `vortex_plasticity_audit.jsonl`.

## 3. Local Verification & Secure SSH Tunnel

    cargo clippy --all-targets -- -D warnings
    cargo test --lib -- --test-threads=1
    powershell -NoProfile -ExecutionPolicy Bypass -File deploy/preview_web_windows.ps1
    ssh -L 8787:127.0.0.1:8787 sniper-singapur
