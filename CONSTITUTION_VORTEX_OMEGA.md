# CONSTITUCION TECNICA VORTEX-OMEGA V4.6 (SSOT — LEYES I A IX)

- **Sistema:** `VORTEX-OMEGA V4.6` (`vortex-rs v1.0.0`) — Motor Institucional Autonomo HFT Maker-First para Bybit UTA V5
- **Contrato IPC de Memoria Compartida:** `VORTEX_SHM_VERSION = 15` (`/dev/shm/vortex_lob.mmap`, `#[repr(C, align(64))]`, cabecera volatil *Publish-Last*, `SlotTelemetrySnapshot` de 96 B con `LastVetoCode`)
- **Formato Columnar Forense:** `PAR2` sin perdida (`256 B/fila`, `L1..L5` + `TickerMeta` + `LiquidationPrint` + `FNV-1a 64-bit`, retrocompatible con `PAR1`)
- **Arquitectura Regulada:** 24 Submodulos `Zero-Alloc` en `src/` + 3 Binarios `LTO Fat` (`vortex_tap`, `vortex_replay`, `vortex_exec`) + Hook `deploy/prewarm_emas_bybit.py` + CI `.github/workflows/ci.yml`
- **Certificacion Vigente:** `Clippy -D warnings: 0` | `OOS 520` con `PARITY_SYNC Drift == 0` | `SCAR-LOCK 63/63` sobre variedad `2,160`

## 1. Preambulo: Los 4 Invariantes Fundacionales

1. **Seguridad Dimensional Estricta (`Newtypes`):** Prohibido intercambiar escalares crudos entre modulos; toda magnitud usa `PriceTicks`, `LotQty`, `ScaledBps` (`1 bp = 100`), `BoundedScore`, `SymbolId` y `TimestampNs`.
2. **Higiene `Zero-Alloc` y Cero `Panics` en Ruta Caliente:** Cero asignaciones dinamicas y cero I/O sincrono en el lazo de `200.0 Hz` (`5 ms`); prohibicion absoluta de `.unwrap()`, `.expect()` y `panic!()`. Todo `unsafe` reside exclusivamente en `src/lob_shm.rs` bajo barreras `Acquire/Release`.
3. **Ley de No-Regresion Parametrica (`SCAR-LOCK`):** Ningun piso de supervivencia sellado tras una perdida real puede relajarse durante periodos de silencio operativo (`0%` reapertura sobre las `63/63` cicatrices, incluso bajo *rebates* negativos).
4. **Paridad Exacta Gemelo-Produccion (`1:1`):** Toda guardia estructural, temporizador elastico de cola, correlacion `Epps` e ingesta `PAR2` (`TickerMeta` + `LiquidationPrint`) en `vortex_exec` se refleja simetricamente en `OmegaDigitalTwinEngine` y `run_parquet_walk_forward_on_dir`.

## 2. Las 9 Leyes Constitucionales Evolutivas (V4.1 -> V4.6)

- **Ley I — Verdad Contable `1e8`, *Rebates* VIP y Boveda `70/30`:** Liquidacion entera exacta en `ParitySyncLedger` y `PairSpec::compute_leg_pnl_and_fee_1e8_with_schedule` (`1 USDT = 10^8`, soporte completo de *rebates* negativos `[-50, 250] ScaledBps`), sincronizacion `set_live_vip_fee_rates`, `PARITY_SYNC Drift == 0`, `70%` a Boveda Blindada y `30%` a Reserva de Saneamiento.
- **Ley II — Plasticidad Continua `C^1`, Elliott v2.0 y `[OB+, OB-]` por Distancia:** `EndogenousLiquidationFloor` interno en `CausalElliottWaveState` (cero `LIQ_FLOOR` fijo), superficie cubica de Hermite `C^1`, *Golden Pocket* `[0.618, 0.650]`, plasticidad desacoplada `[OB+, OB-]` (`[382..=854 permille]`), desalojo ponderado por distancia en `CausalOrderBlockEngine` y `learned_mfe_bps` sin censura.
- **Ley III — Gobernanza Bayesiana `12x2`, Paridad `2,400 ppm`, Kelly `[550..1200‰]` y `P0-B`:** Memoria independiente `12x2`, presupuesto invariante `2,400 ppm` al SL, calendario civil `O(1)` DST (`BST/EDT`) + *carry* direccional de `fundingRate` con rango completo `[550..=1200 permille]`, colision direccional `[OB+, OB-]`, soberania local del `35/65` *Runner*, rehabilitacion `Shadow-Decay` solo bajo mutacion fisica de regimen y custodia `is_safe_halt_standby` (`P0-B`).
- **Ley IV — Transporte Asincrono `Zero-Jitter` y `SHM v15`:** Aislamiento `vortex_tap` -> `SHM v15` -> `vortex_exec` + `BybitAsyncGateway`, `AtomicLockWatch` en `0 ns` y `dispatch_ema_prewarm_async` + `vault_io` (`.tmp -> sync_all -> rename`) con cero escrituras en el lazo de `200.0 Hz`.
- **Ley V — Primacia Pasiva, *Shadow-Runner*, Piso VIP y *Chandelier*:** Ejecucion `Limit PostOnly`, *Shadow-Runner* (`250 ms`), piso cero-perdida, *Chandelier* neto positivo y trinquete Wald acoplados a `active_fees` + emergencia `P0-A` (`Market IOC reduceOnly` ante brecha `>= 1.5x SL`).
- **Ley VI — Escudo `Stoikov + Hawkes`, Cola Elastica y `Wald SPRT v2.0`:** Identidad `0 <= Q_ahead(t) <= Q_L1(t)`, `evaluate_entry_signal_v46` con pesos anti-*spoof* `GEOM`, barrera `3x` dinamica (`BetaBinomialTakerPrior` + `FeeSchedule`), temporizadores proporcionales a `max_chase_ns` y tolerancias adimensionales en ticks.
- **Ley VII — Universo de 12 Contratos, `Epps` 5s en Vivo y Guardia Monetaria Beta:** `[EppsBtcCorrelationTracker; 12]` activo alimentando `MonetaryPortfolioBetaGuard` (`<= 14,400 ppm` total, `<= 10,800 ppm` beta direccional firmada, max. 6 slots).
- **Ley VIII — Dominancia Estricta de Pareto, `PAR2` `1:1` y *Markouts* LVR Vivos:** Escalera `MODEL A < MODEL B < MODEL A+B < MODEL OMEGA`, `[SlotMarkoutRing; 12]` persistiendo `markout_fill_bps` y `lvr_5s_bps` reales en `TRADE_CLOSE` y paridad `1:1` sobre `PAR2`.
- **Ley IX — Higiene `UTF-8 LF`, Gate CI y Rotacion Diferida por Drenaje:** Cero advertencias en `clippy::pedantic`, seguimiento obligatorio de `.github/workflows/ci.yml`, corte exclusivamente mediante `DRAIN_MODE.lock` (`0` posiciones abiertas) + snapshot `SHA-256` y congelacion estadistica hasta completar `N >= 25` tiros reales en `sniper-singapur`.
