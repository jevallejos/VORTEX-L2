# CONSTITUCION TECNICA VORTEX-OMEGA V4.5 (SSOT — LEYES I A IX)

- **Sistema:** `VORTEX-OMEGA V4.5` (`vortex-rs v1.0.0`) — Motor Institucional Autonomo HFT Maker-First para Bybit UTA V5
- **Contrato IPC de Memoria Compartida:** `VORTEX_SHM_VERSION = 15` (`/dev/shm/vortex_lob.mmap`, `#[repr(C, align(64))]`, cabecera volatil *Publish-Last*, `SlotTelemetrySnapshot` de 96 B con `LastVetoCode`)
- **Formato Columnar Forense:** `PAR2` sin perdida (`256 B/fila`, `L1..L5` + `TickerMeta` + `LiquidationPrint` + `FNV-1a 64-bit`, retrocompatible con `PAR1`)
- **Arquitectura Regulada:** 24 Submodulos `Zero-Alloc` en `src/` + 3 Binarios `LTO Fat` (`vortex_tap`, `vortex_replay`, `vortex_exec`) + Hook `deploy/prewarm_emas_bybit.py`
- **Certificacion Vigente:** `85/85` Tests OK | `Clippy -D warnings: 0` | `OOS 520` con `PARITY_SYNC Drift == 0` | `SCAR-LOCK 63/63` sobre variedad `2,160`

---

## 1. Preambulo: Los 4 Invariantes Fundacionales

1. **Seguridad Dimensional Estricta (`Newtypes`):** Prohibido intercambiar escalares crudos entre modulos; toda magnitud fisica o financiera usa `PriceTicks`, `LotQty`, `ScaledBps` (`1 bp = 100`), `BoundedScore`, `SymbolId` y `TimestampNs`.
2. **Higiene `Zero-Alloc` y Cero `Panics` en Ruta Caliente:** Cero asignaciones dinamicas en el lazo de `200.0 Hz` (`5 ms`) y prohibicion absoluta de `.unwrap()`, `.expect()` y `panic!()`. Todo bloque `unsafe` reside exclusivamente en `src/lob_shm.rs` bajo barreras `Acquire/Release`.
3. **Ley de No-Regresion Parametrica (`SCAR-LOCK`):** Ningun piso de supervivencia sellado tras una perdida real puede relajarse durante periodos de silencio operativo (`0%` reapertura sobre las `63+` cicatrices).
4. **Paridad Exacta Gemelo-Produccion (`1:1`):** Toda guardia estructural en `vortex_exec` se refleja simetricamente en `OmegaDigitalTwinEngine` y `run_parquet_walk_forward_on_dir` sobre `PAR1/PAR2`.

---

## 2. Las 9 Leyes Constitucionales Evolutivas (V4.1 -> V4.5)

- **Ley I — Verdad Contable `1e8` y Boveda `70/30`:** Liquidacion entera exacta en `ParitySyncLedger` (`1 USDT = 10^8`), invariante `PARITY_SYNC Drift == 0`, soporte de `FeeSchedule` VIP dinamico (`/v5/account/fee-rate`), `70%` a Capital Compuesto Blindado y `30%` a Reserva de Saneamiento.
- **Ley II — Plasticidad Continua `C^1`, Elliott v2.0 y `Order Blocks` `[OB+, OB-]`:** Inferencia causal sin *look-ahead*, `EndogenousLiquidationFloor` por contrato, superficie cubica de Hermite `C^1` Elliott-Hurst, *Golden Pocket* continuo `[0.618, 0.650]`, plasticidad direccional desacoplada `[OB+, OB-]` (`[382..=854 permille]`), desalojo ponderado por distancia y `learned_mfe_bps` simetrico sin censura.
- **Ley III — Gobernanza Bayesiana `12x2`, Paridad `2,400 ppm` y Custodia `P0-B`:** Memoria independiente en 24 celdas `(slot_idx, Side)`, presupuesto invariante `2,400 ppm` al SL, calendario civil `O(1)` DST (`BST/EDT`) + *carry* direccional de `fundingRate`, soberania local del `35/65` *Runner*, rehabilitacion `Shadow-Decay` solo bajo cambio estructural de regimen y custodia incondicional `is_safe_halt_standby` (`P0-B`).
- **Ley IV — Transporte Asincrono `Zero-Jitter` y `SHM v15`:** Aislamiento `vortex_tap` -> `SHM v15` -> `vortex_exec` + `BybitAsyncGateway` (`wss://stream.bybit.com/v5/trade`), vigilancia `AtomicLockWatch` en `0 ns` y trabajador `vault_io` con reemplazo atomico `.tmp -> sync_all -> rename` fuera del lazo de `200.0 Hz`.
- **Ley V — Primacia Pasiva, *Shadow-Runner*, Piso Elastico y *Chandelier*:** Ejecucion `Limit PostOnly`, *Shadow-Runner* con *Snap-In* de `250 ms`, piso ofensivo elastico `compute_elastic_offensive_win_floor_bps`, `ChandelierProfitLockSl` neto positivo garantizado y aplanamiento de emergencia `P0-A` (`Market IOC reduceOnly` ante brecha `>= 1.5x SL`).
- **Ley VI — Escudo `Stoikov + Hawkes`, Cota Fisica de Cola y `Wald SPRT v2.0`:** Identidad libre de parametros `0 <= Q_ahead(t) <= Q_L1(t)` (`clamp_resting_queue_ahead_physical`), estimador `BetaBinomialTakerPrior` acoplado a `3x` y fronteras de Wald, tolerancias adimensionales en ticks y desbloqueo elastico del *Runner*.
- **Ley VII — Universo de 12 Contratos, Correlacion `Epps` y Guardia Monetaria Beta:** Operacion exclusiva sobre `APPROVED_SPECS` verificados en arranque (`/v5/market/instruments-info`), `EppsBtcCorrelationTracker` (5 s) y `MonetaryPortfolioBetaGuard` (`<= 14,400 ppm` total, `<= 10,800 ppm` direccional, max. 6 slots).
- **Ley VIII — Dominancia Estricta de Pareto, `PAR2` y *Markouts* LVR:** Escalera certificada `MODEL A < MODEL B < MODEL A+B < MODEL OMEGA`, `0` rechazos `PostOnly` a `25 ms RTT`, curvas `SlotMarkoutRing` (`+250ms`, `+1s`, `+5s`, `+15s`) y auditoria de abortos *True Positive*.
- **Ley IX — Higiene `UTF-8 LF`, Gate CI y Rotacion Diferida por Drenaje:** Cero advertencias en `clippy::pedantic`, prohibicion de sobre-parchear nodos activos antes de completar muestra estacionaria (`N >= 25` tiros reales en `sniper-singapur`) y corte exclusivamente mediante `DRAIN_MODE.lock` (`0` posiciones abiertas) + respaldo inmutable `SHA-256`.
