# VORTEX-OMEGA V5.5 (`vortex-rs`) — Autonomous 200Hz Zero-Alloc Rust HFT Engine (Bybit UTA V5)

## Architecture & 4 Official LTO-Fat Binaries (`SHM v15` — 25 KB SeqLock)
1. **`vortex_tap` (`Nice=-10`)**: 12-pair Bybit V5 public L2/Trade/Ticker/Liquidation ingestor, `PAR2` lossless ring (256 B/row + FNV-1a), `.par2z` (`VPZ1`) cold archiver, and `VKL1` (96 B/candle) perpetual multi-TF store (`1s..1d`).
2. **`vortex_exec` (`Nice=-15`)**: 200.0 Hz Zero-Alloc 12-slot execution custodian, Multi-Lot Offensive Priority Router (`O1`), Unified-Horizon `BTC` Lead-Lag Ignition (`O2`), 3-Tranche Golden `reduceOnly` Cascade + Learned Continuation Ratio (`O3`), House-Money Free-Ride Recycling (`O4`), Momentum Chain Re-Entry (`O5`), Sub-Second Crest Snap-In (`O6`), Diffusive Anti-Hostage Expiration (`O7`), and irreversible `-5.00 USDT` epoch shield + P0-B `DRAIN_MODE` standby.
3. **`vortex_replay` (`Nice=19`)**: 4-arm Pareto ablation suite, 1:1 Digital Twin (`Drift == 0`), and live `PAR2` / `.par2z` Walk-Forward verifier.
4. **`vortex_web` (`Nice=19`, `IOSchedulingClass=idle`)**: Loopback-only (`127.0.0.1:8787`) HTTP + 5 Hz SSE Observatory protected by 256-bit `X-Vortex-Csrf` via SSH tunnel (`ssh -N -L 8787:127.0.0.1:8787 sniper-singapur`).

## Constitutional Verification Gates
- `cargo clippy --all-targets -- -D warnings` (`0` warnings)
- `cargo test --lib -- --test-threads=1` (`86/86` unit tests)
- `cargo test --tests -- --test-threads=1` (`28/28` Red-Team + `500` MC + `D1..D5` De-Legacy + `V5.1..V5.5` Forensic Counterfactual Suites + `SCAR-LOCK 63/63` + `OOS 520`)
