# VORTEX-OMEGA V4.7 — Autonomous 200Hz Maker-First HFT Engine (Bybit UTA V5)

Motor institucional cuantitativo de alta frecuencia en Rust (`Zero-Alloc`, `LTO Fat`, `panic = "abort"`) para ejecucion pasiva `Limit PostOnly` sobre 12 contratos perpetuos lineales USDT en Bybit Unified Trading Account (UTA V5).

## 1. Resumen Ejecutivo e Identidad Operativa V4.7

| Parametro | Especificacion Certificada V4.7 |
| :--- | :--- |
| **Version del Motor** | `VORTEX-OMEGA V4.7` (`vortex-rs v1.0.0`) |
| **Contrato IPC Memoria Compartida** | `VORTEX_SHM_VERSION = 15` (`/dev/shm/vortex_lob.mmap`, `#[repr(C, align(64))]`) |
| **Formato Columnar Forense** | `PAR2` sin perdida (`256 B/fila`, `L1..L5` + `TickerMeta` + `LiquidationPrint` + `FNV-1a 64-bit`) |
| **Cadencia del Demonio Custodio** | `200.0 Hz` (`5 ms`) `Zero-Jitter` (`0` I/O sincrono en ruta caliente) + WS `wss://stream.bybit.com/v5/trade` |
| **Arquitectura Modular** | `24` submodulos `Zero-Alloc` en `src/` + `3` binarios `LTO Fat` (`vortex_tap`, `vortex_replay`, `vortex_exec`) |
| **Suite y Compuertas** | Suite `/dev/shm` `100%` en verde, `Clippy pedantic = 0 warnings`, `OOS 520` (`Maker >= 80%`, `PF >= 1.35`, `Drift == 0`) |

## 2. Las 4 Islas Auto-Aprendidas Conectadas en V4.7 y Tarifas VIP Vivas

- **Isla 1 — Correlacion `Epps` Viva (`5 s`), Pesos Anti-Spoof `GEOM` y Barrera `3x` Dinamica:** `[EppsBtcCorrelationTracker; 12]` alimenta `MonetaryPortfolioBetaGuard` y `evaluate_entry_signal_v46` con beta firmada `[-phi, +phi]`, pesos `GEOM` atenuados bajo spoofing (`w_OFI + w_TFI + w_GEOM == 1`) y puerta `3x` acoplada a `BetaBinomialTakerPrior` + `FeeSchedule` vivo.
- **Isla 2 — Desalojo `Order Block` por Distancia y Guardia Direccional `[OB+, OB-]`:** `CausalOrderBlockEngine` retiene muros cercanos al BBO mediante `select_distance_weighted_eviction_slot`, escala el *front-run* TP con `FeeSchedule` y `OnlineAdaptiveGovernor` separa `ob_bear_efficacy_pm` (`Bid`) de `ob_bull_efficacy_pm` (`Ask`).
- **Isla 3 — Curvas `Markout` y `LVR 5s` en Boveda Forense:** `[SlotMarkoutRing; 12]` muestrea curvas post-fill y post-aborto (`+250ms, +1s, +5s, +15s`) persistiendo `markout_fill_bps` y `lvr_5s_bps` reales en `vortex_ai_forensic_vault.jsonl`.
- **Isla 4 — `Zero-Jitter` Asincrono al 100%, *Rebates* VIP y Paridad `1:1` `PAR2`:** `gov.dispatch_ema_prewarm_async` elimina escrituras sincronas en `200.0 Hz`, `ParitySyncLedger` y `PairSpec` liquidan tarifas VIP y *rebates* negativos (`Drift == 0`), Kelly abre el rango completo `[550..=1200 permille]`, `CausalElliottWaveState` integra `EndogenousLiquidationFloor` interno y `replay_walkforward.rs` inyecta `TickerMeta` + `LiquidationPrint` en paridad `1:1`.

## 3. Comandos de Compilacion, Verificacion y Operacion

Validacion estatica, pruebas deterministas y compilacion optimizada:

    cargo clippy --all-targets -- -D warnings
    cargo test --lib -- --test-threads=1
    cargo build --release --bins

Ejecucion de las 3 compuertas autonomas de certificacion:

    ./target/release/vortex_tap
    ./target/release/vortex_replay
    ./target/release/vortex_exec

Inspeccion operativa en tiempo real (cero impacto sobre el hilo de 200.0 Hz):

    ./target/release/vortex_exec --live
    ./target/release/vortex_exec --trades
