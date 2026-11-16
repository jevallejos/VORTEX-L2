# VORTEX-OMEGA V4.8 — Autonomous 200Hz Maker-First HFT Engine (Bybit UTA V5)

Motor institucional cuantitativo de alta frecuencia en Rust (`Zero-Alloc`, `LTO Fat`, `panic = "abort"`) para ejecucion pasiva `Limit PostOnly` sobre 12 contratos perpetuos lineales USDT en Bybit Unified Trading Account (UTA V5).

## 1. Resumen Ejecutivo e Identidad Operativa V4.8

| Parametro | Especificacion Certificada V4.8 |
| :--- | :--- |
| **Version del Motor** | `VORTEX-OMEGA V4.8` (`vortex-rs v1.0.0`) |
| **Contrato IPC Memoria Compartida** | `VORTEX_SHM_VERSION = 15` (`/dev/shm/vortex_lob.mmap`, `#[repr(C, align(64))]`) |
| **Formato Columnar Forense** | `PAR2` sin perdida (`256 B/fila`, `L1..L5` + `TickerMeta` + `LiquidationPrint` + `FNV-1a 64-bit`) |
| **Cadencia del Demonio Custodio** | `200.0 Hz` (`5 ms`) `Zero-Jitter` (`0` I/O sincrono en ruta caliente) + WS `wss://stream.bybit.com/v5/trade` |
| **Arquitectura Modular** | `24` submodulos `Zero-Alloc` en `src/` + `3` binarios `LTO Fat` (`vortex_tap`, `vortex_replay`, `vortex_exec`) |
| **Suite y Compuertas** | `56/56` tests en `/dev/shm` (`100%`), `Clippy pedantic = 0 warnings`, `OOS 520` (`Maker >= 80%`, `PF >= 1.35`, `Drift == 0`) |

## 2. Escudos Institucionales V4.8 y las 4 Islas Auto-Aprendidas

- **Escudo Diario Elastico UTC (`DailySessionGovernor`):** Tope diario acoplado a la Reserva del `30%` (`[-3,883..-6,283 ppm]`), meta diaria auto-aprendida con candado convexo *High-Water Mark*, amortiguador Kelly `C^1` (`[618..=1000 permille]`) y despertar autonomo en `00:00:00 UTC`.
- **Compuerta Cuantica de Crecimiento Geometrico (`passes_quantum_ev_growth_gate`):** Bloquea de forma endogena lotes indivisibles sobredimensionados con deriva geometrica negativa (silenciando `BTCUSDT` a capital bajo y reactivandolo automaticamente al escalar capital o *rebates* VIP).
- **Evacuacion Post-Fill `Glosten-Milgrom + Wald SPRT v2.0`:** Evalua `zeta_tox` sobre `SlotMarkoutRing` (`+250ms, +1s, +5s`); ante barridos con `MFE <= 1 tick` e inversion de flujo, activa escape Maker cero-perdida y contrae el Stop-Loss servidor con Hermite `C^1` entre `64%` y `83%`.
- **Desacople Celular vs. Catastrofico y Persistencia `FUNDING_SETTLEMENT`:** Aisla Stop-Loss normales en su celda `12x2` (reservando `HALTED_ON_LOSS.lock` a brechas `>= 14,400 ppm`), persiste liquidaciones `8h` con `FNV-1a` y restaura barras de progreso y panel diario en `vortex_exec --trades` (`vortex 360`).

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
