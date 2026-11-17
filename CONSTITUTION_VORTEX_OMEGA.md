# CONSTITUTION VORTEX-OMEGA (V4.1 -> V4.9 SSOT)

## I. Nine Foundational Evolutionary Laws

1. **Law I (Compile-Time Dimensional Safety):** Prices (`PriceTicks`), quantities (`LotQty`), basis points (`ScaledBps`), and timestamps (`TimestampNs`) use strict newtypes; zero IEEE-754 drift on accounting ledgers (`1e8` integer precision, `PARITY_SYNC Drift == 0`).
2. **Law II (Zero-Jitter 200.0 Hz Hot Path):** Zero heap allocations, zero synchronous disk/network I/O, and zero `.unwrap()` / `.expect()` / `panic!()` inside the `5 ms` execution loop; `VaultIoCommand` stays `<= 512 B`.
3. **Law III (P0-B Custody & Cellular Fault Isolation):** `DRAIN_MODE.lock` and `DailySessionGovernor` hibernation never degrade `200.0 Hz` custody while any position or RPC is active; single-slot Stop-Loss events quarantine cellularly (`12x2`) unless a `>= 14,400 ppm` (`1.44%`) systemic breach occurs.
4. **Law IV (Atomic & Checksummed Persistence):** Every `.vkl1`, `.par2z`, `.parquet`, and `.jsonl` record is sealed with `FNV-1a 64-bit` and persisted via `.tmp -> sync_all -> rename`.
5. **Law V (Maker-First Execution & Dynamic VIP Friction):** Entries use `PostOnly` L1/OB quoting gated by `3x` live VIP round-trip friction (including negative Maker rebates) and `BetaBinomialTakerPrior`.
6. **Law VI (C1 Smooth Surfaces & Zero Magic Scalars):** Kelly sizing (`[550..=1200 permille]`), single-lot quantum/asymmetry EV gates, GEOM anti-spoof weights, and Zero-MFE stagnation compression vary continuously without discontinuous step thresholds.
7. **Law VII (70/30 Sinking-Fund Vault & Elastic UTC Daily Shield):** `70%` of net realized profit locks monotonically into the Vault; `30%` feeds the sinking reserve scaling the UTC daily loss cap between `-3,883 ppm` and `-6,283 ppm`.
8. **Law VIII (1:1 Digital Twin & SCAR-LOCK Non-Regression):** Production changes must preserve `SCAR-LOCK 63/63` (`2,160` manifold), `OOS 520` (`Drift == 0`), and 4-arm Pareto dominance (`+25.80 < +44.88 < +158.28 < +482.07 bps`).
9. **Law IX (Stationary Sample Discipline & Loopback Security):** Production on `sniper-singapur` remains frozen until completing `>= 25/25` live trades; `vortex_web` binds exclusively to `127.0.0.1:8787` (`Nice=19`, `X-Vortex-Csrf` enforced).

## II. Addendum V4.9 — Observatory, Perpetual VKL1/PAR2Z & Deep EV Shield

- **27 Rust Modules & 4 Binaries (`72/72` Tests):** Adds `candle_store.rs` (`VKL1`), `par2_archiver.rs` (`VPZ1` `.par2z`), and `web_server.rs` + `bin/vortex_web.rs` while keeping `VORTEX_SHM_VERSION = 15` (`25 KB`) binary-identical.
- **Elastic EV > 0 & 5-Horizon Markouts:** Enforces `passes_single_lot_asymmetry_ev_gate`, Macro-Wave3 coherence, reactive L1 wall-collapse abort, MFE plasticity orthogonality on zero-discovery SL, and `+250ms..+60s` counterfactual markouts.
