# VORTEX-OMEGA V4.5 — Autonomous 200Hz Maker-First HFT Engine (Bybit UTA V5)

Motor institucional cuantitativo de alta frecuencia en Rust (`Zero-Alloc`, `LTO Fat`, `panic = "abort"`) para ejecucion pasiva `Limit PostOnly` sobre 12 contratos perpetuos lineales USDT en Bybit Unified Trading Account (UTA V5).

---

## 1. Resumen Ejecutivo e Identidad Operativa

| Parametro | Especificacion Certificada V4.5 |
| :--- | :--- |
| **Version del Motor** | `VORTEX-OMEGA V4.5` (`vortex-rs v1.0.0`) |
| **Contrato IPC Memoria Compartida** | `VORTEX_SHM_VERSION = 15` (`/dev/shm/vortex_lob.mmap`, `#[repr(C, align(64))]`) |
| **Formato Columnar Forense** | `PAR2` sin perdida (`256 B/fila`, `L1..L5` + `TickerMeta` + `LiquidationPrint` + `FNV-1a 64-bit`) |
| **Cadencia del Demonio Custodio** | `200.0 Hz` (`5 ms`) sobre 12 carriles autonomos + pasarela WS `wss://stream.bybit.com/v5/trade` |
| **Arquitectura Modular** | `24` submodulos `Zero-Alloc` en `src/` + `3` binarios `LTO Fat` (`vortex_tap`, `vortex_replay`, `vortex_exec`) |
| **Suite y Compuertas** | `85/85` Tests OK, `Clippy pedantic = 0 warnings`, `OOS 520` (`Maker >= 80%`, `PF >= 1.35`, `Drift == 0`) |

---

## 2. Arquitectura de 24 Modulos (`src/`) y 3 Binarios (`src/bin/`)

### Nucleo de Tipos, IPC Volatil y Persistencia Asincrona
- `src/types.rs` y `src/fee_cadence_beta.rs`: Seguridad dimensional `#[repr(transparent)]` (`PriceTicks`, `LotQty`, `ScaledBps`, `BoundedScore`, `TimestampNs`), `FeeSchedule` VIP dinamico, estimador `BetaBinomialTakerPrior`, calendario civil `O(1)` DST (`BST/EDT`) + *carry* de `fundingRate`, tolerancias adimensionales en ticks y `MonetaryPortfolioBetaGuard` (`<= 14,400 ppm`).
- `src/lob_shm.rs`: Arena `VortexShmArena` (`SHM v15`) con cabecera volatil *Publish-Last*, `SeqLock` libre de desgarros, centinela de secuencia/base `Mark - Mid` y `SlotTelemetrySnapshot` (96 B) con `LastVetoCode`.
- `src/forensic_parquet.rs`, `src/tap_ring.rs` y `src/vault_io.rs`: Lotes `PAR2` retrocompatibles con `PAR1`, contabilidad exacta `1e8` (`ParitySyncLedger` + boveda `70/30`), `AtomicLockWatch` en `0 ns` y escritura atomica `.tmp -> sync_all -> rename` fuera del lazo de `200.0 Hz`.

### Microestructura, Plasticidad Continua y Escudos Cuantitativos
- `src/alpha_3factor.rs`: Factores ortogonales (`OFI`, `TFI`, `GEOM`), `EventClockAlphaModulator`, atenuacion anti-*spoof*, correlacion viva `EppsBtcCorrelationTracker` (5 s), micro-precio aureo de Stoikov y surf `LVDBS`.
- `src/wave_elliott.rs` y `src/order_block.rs`: `CausalElliottWaveState` (6 pivotes sin *look-ahead*), `EndogenousLiquidationFloor`, superficie cubica de Hermite `C^1` Elliott-Hurst, *Golden Pocket* `[0.618, 0.650]`, plasticidad desacoplada `[OB+, OB-]` (`[382..=854 permille]`) y desalojo ponderado por distancia.
- `src/kinematic_surface.rs`, `src/governor_plasticity.rs` y `src/bayesian_governor.rs`: Escudo en cola `Moallemi + Stoikov + Hawkes`, `Wald SPRT v2.0`, `SCAR-LOCK` (`63/63` cicatrices sobre variedad `2,160`), soberania local del `35/65` *Runner* y rehabilitacion `Shadow-Decay` bajo cambio estructural de regimen.

### Ejecucion Autonoma, Gemelo Digital `1:1` y Telemetria Forense
- `src/exit_quoter.rs`, `src/execution_fsm.rs`, `src/exec_daemon.rs` y `src/exec_gateway_handlers.rs`: Identidad fisica de cola `0 <= Q_ahead(t) <= Q_L1(t)`, escalera direccional elastica, piso ofensivo elastico, *Shadow-Runner* (*Snap-In* `250 ms`), `ChandelierProfitLockSl`, `P0-A` (`Market IOC reduceOnly`) e invariante `P0-B` (`is_safe_halt_standby`).
- `src/ws_protocol.rs`, `src/http_ws_transport.rs`, `src/bybit_client.rs` y `src/bybit_gateway.rs`: Transporte HTTP/1.1 *Keep-Alive* con decodificacion *chunked*, handshake de arranque (`/v5/account/fee-rate` e `/v5/market/instruments-info`) y *Atomic Amend* por WebSocket.
- `src/fifo_sim.rs`, `src/replay_walkforward.rs`, `src/markout_tracker.rs` y `src/telemetry_hud.rs`: Simulador FIFO `L1..L5` (`25 ms RTT`), ablacion de 4 brazos (`MODEL A < B < A+B < OMEGA`), Gemelo Digital `1:1` sobre `PAR1/PAR2`, curvas *Markout* LVR (`+250ms..+15s`) y HUD `SHM v15`.

---

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

---

## 4. Gobernanza de Produccion y Despliegue Seguro (Ley IX)

1. **Congelacion Estadistica:** Ningun nodo productivo activo (`sniper-singapur`) se recompila en caliente antes de completar su muestra estacionaria (`N >= 25` operaciones cerradas), salvo incidente `P0` patrimonial.
2. **Drenaje Verificado (`P0-B`):** Toda rotacion exige activar `data/vault/DRAIN_MODE.lock` y verificar `0` posiciones abiertas en `vortex_open_positions_state.json` antes de detener `vortex-exec.service` y `vortex-tap.service`.
3. **Respaldo Inmutable y Recreacion `SHM`:** Snapshot previo con sumas `SHA-256` sobre `bin/` y `data/vault/`, compilacion aislada (`nice -n 19 ionice -c 3`) y arranque estricto `vortex-tap` -> `vortex-exec`.
