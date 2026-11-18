# CONSTITUTION VORTEX-OMEGA (Laws I–IX & Addenda V4.1 -> V5.1 SSOT)

## Core Invariants
1. **Compile-Time Dimensional Newtypes**: `PriceTicks`, `LotQty`, `ScaledBps`, `BoundedScore`, `SymbolId`, `TimestampNs`.
2. **Zero-Alloc & Zero-Panic Hot Path (200.0 Hz)**: All `unsafe` isolated in `src/lob_shm.rs` (`VORTEX_SHM_VERSION = 15`, 25 KB); all disk I/O offloaded to `vault_io` (`<= 512 B` command envelope, atomic `.tmp -> sync_all -> rename` + FNV-1a 64-bit).
3. **Law of Parametric Non-Regression (`SCAR-LOCK 63/63`)**: Zero scar reopening across the 2,160-state manifold; zero smoothing by silence; rehabilitation strictly requires verified regime mutation.
4. **Exact 1:1 Twin-Production Parity & `PARITY_SYNC Drift == 0`**: Integer `1e8` accounting, live VIP `FeeSchedule` (including negative Maker rebates), 8h UTC `FUNDING_SETTLEMENT`, and `EPOCH_RESET` non-destructive ledger anchoring.

## Addendum V5.0 -> V5.1 (Elastic C1 Self-Learning Recalibration)
- **V5.0 / V5.0.1 / V5.0.2**: Non-destructive `EPOCH_RESET`, 28/28 `C1` Anti-Win-Wiper Red-Team shields (Kaplan-Meier right-censoring MFE guard, signed BTC contagion, P0-A live gap flatten), and removal of legacy static scalars.
- **V5.1 (F1..F6 Closed)**: Single-flush silent `DRAIN_MODE` standby (0 `systemd` restart loops); reversible `C1` exit unlock over intrinsic `flow_ema_slow` + `Runner` preservation + opposing `ImpulseWave3` escape; `1m` Brownian diffusive SL floor immune to `5 ms` spikes; uninflated 1-lot EV & `[OB+, OB-]` modulated `CorrectiveAbc` Hurst guard; surprise-weighted `BetaBinomialTakerPrior` + cross-side chop shield; physical L1 FIFO trade consumption vs. whale cancellation split + WS-RTT adaptive spoof/sweep shields.
