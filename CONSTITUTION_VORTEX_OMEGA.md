# CONSTITUCION TECNICA VORTEX-OMEGA V4.8 (SSOT — LEYES I A IX)

- **Sistema:** `VORTEX-OMEGA V4.8` (`vortex-rs v1.0.0`) — Motor Institucional Autonomo HFT Maker-First para Bybit UTA V5
- **Contrato IPC de Memoria Compartida:** `VORTEX_SHM_VERSION = 15` (`/dev/shm/vortex_lob.mmap`, `#[repr(C, align(64))]`, cabecera volatil *Publish-Last*, `SlotTelemetrySnapshot` de 96 B con `LastVetoCode`)
- **Formato Columnar Forense:** `PAR2` sin perdida (`256 B/fila`, `L1..L5` + `TickerMeta` + `LiquidationPrint` + `FNV-1a 64-bit`, retrocompatible con `PAR1`)
- **Arquitectura Regulada:** 24 Submodulos `Zero-Alloc` en `src/` + 3 Binarios `LTO Fat` (`vortex_tap`, `vortex_replay`, `vortex_exec`) + Hook `deploy/prewarm_emas_bybit.py` (`3x retry`) + CI `.github/workflows/ci.yml`
- **Certificacion Vigente:** `Clippy -D warnings: 0` | `56/56 Suites OK` | `OOS 520` (`PARITY_SYNC Drift == 0`) | `SCAR-LOCK 63/63` sobre variedad `2,160`

## 1. Preambulo: Los 4 Invariantes Fundacionales

1. **Seguridad Dimensional Estricta (`Newtypes`):** Prohibido intercambiar escalares crudos entre modulos; toda magnitud usa `PriceTicks`, `LotQty`, `ScaledBps` (`1 bp = 100`), `BoundedScore`, `SymbolId` y `TimestampNs`.
2. **Higiene `Zero-Alloc` y Cero `Panics` en Ruta Caliente:** Cero asignaciones dinamicas y cero I/O sincrono en el lazo de `200.0 Hz` (`5 ms`); prohibicion absoluta de `.unwrap()`, `.expect()` y `panic!()`. Todo `unsafe` reside exclusivamente en `src/lob_shm.rs` bajo barreras `Acquire/Release`.
3. **Ley de No-Regresion Parametrica (`SCAR-LOCK`):** Ningun piso de supervivencia sellado tras una perdida real puede relajarse durante periodos de silencio operativo (`0%` reapertura sobre las `63/63` cicatrices, incluso bajo *rebates* negativos).
4. **Paridad Exacta Gemelo-Produccion (`1:1`):** Toda guardia estructural, compuerta cuantica, gobernador diario UTC, temporizador elastico de cola, correlacion `Epps` e ingesta `PAR2` en `vortex_exec` se refleja simetricamente en `OmegaDigitalTwinEngine` y `run_parquet_walk_forward_on_dir`.

## 2. Las 9 Leyes Constitucionales Evolutivas (V4.1 -> V4.8)

- **Ley I — Verdad Contable `1e8`, *Rebates* VIP, Funding 8h y Boveda `70/30`:** Liquidacion entera exacta en `ParitySyncLedger` y `PairSpec::compute_leg_pnl_and_fee_1e8_with_schedule` (`1 USDT = 10^8`, *rebates* negativos `[-50, 250] ScaledBps`), persistencia `FUNDING_SETTLEMENT` con `FNV-1a`, `PARITY_SYNC Drift == 0`, `70%` a Boveda Blindada y `30%` a Reserva de Saneamiento.
- **Ley II — Plasticidad Continua `C^1`, Elliott v2.0 y `[OB+, OB-]` por Distancia:** `EndogenousLiquidationFloor` interno en `CausalElliottWaveState`, superficie cubica de Hermite `C^1`, *Golden Pocket* `[0.618, 0.650]`, plasticidad desacoplada `[OB+, OB-]` (`[382..=854 permille]`), desalojo por distancia en `CausalOrderBlockEngine` y `learned_mfe_bps` sin censura.
- **Ley III — Gobernanza Bayesiana `12x2`, Escudo Diario UTC, Kelly `[550..=1200‰]` y `P0-B`:** Memoria independiente `12x2`, `DailySessionGovernor` UTC, compuerta cuantica `passes_quantum_ev_growth_gate`, calendario `O(1)` DST (`BST/EDT`) + *carry* de `fundingRate`, rehabilitacion `Shadow-Decay` solo bajo mutacion fisica de regimen y custodia `is_safe_halt_standby` (`P0-B`).
- **Ley IV — Transporte Asincrono `Zero-Jitter` y `SHM v15`:** Aislamiento `vortex_tap` -> `SHM v15` -> `vortex_exec` + `BybitAsyncGateway`, `AtomicLockWatch` en `0 ns` y trabajador unico `vault_io` (`size_of::<VaultIoCommand>() <= 512 B`, `.tmp -> sync_all -> rename`) con cero escrituras en el lazo de `200.0 Hz`.
- **Ley V — Primacia Pasiva, *Shadow-Runner*, Compresion Post-Fill y *Chandelier*:** Ejecucion `Limit PostOnly`, *Shadow-Runner* (`250 ms`), piso cero-perdida, *Chandelier* neto positivo, contraccion `C^1` de toxicidad `Glosten-Milgrom + Wald` (`64%..83%`) + emergencia `P0-A` (`Market IOC reduceOnly`).
- **Ley VI — Escudo `Stoikov + Hawkes`, Cola Elastica y `Wald SPRT v2.0`:** Identidad `0 <= Q_ahead(t) <= Q_L1(t)`, `evaluate_entry_signal_v46` con pesos anti-*spoof* `GEOM`, barrera `3x` dinamica (`BetaBinomialTakerPrior` + `FeeSchedule`), ventana Parkinson VIP viva y tolerancias adimensionales en ticks.
- **Ley VII — Universo de 12 Contratos, `Epps` 5s en Vivo y Cortacircuito Jerarquico:** `[EppsBtcCorrelationTracker; 12]` alimentando `MonetaryPortfolioBetaGuard` (`<= 14,400 ppm` total, `<= 10,800 ppm` beta firmada, max. 6 slots) y `is_catastrophic_loss_breach` reservando `HALTED_ON_LOSS.lock` solo a eventos catastroficos (`>= 14,400 ppm`).
- **Ley VIII — Dominancia Estricta de Pareto, `PAR2` `1:1` y *Markouts* LVR Vivos:** Escalera `MODEL A < MODEL B < MODEL A+B < MODEL OMEGA`, `[SlotMarkoutRing; 12]` persistiendo `markout_fill_bps` y `lvr_5s_bps` reales en `TRADE_CLOSE` y paridad `1:1` total sobre `PAR2`.
- **Ley IX — Higiene `UTF-8 LF`, Gate CI y Rotacion Diferida por Drenaje:** Cero advertencias en `clippy::pedantic`, seguimiento obligatorio de `.github/workflows/ci.yml`, corte exclusivamente con `0` posiciones abiertas + snapshot `SHA-256` y disciplina estadistica hacia `25 -> 50 -> 100+` tiros reales en `sniper-singapur`.

## Adenda Constitucional V4.7 — V4.8 (Unified Vault, Elastic Daily Shield & Quantum EV)
- **SSOT Microestructural y Tarifas VIP en Ingesta (`P2-3`):** `vortex_tap` recarga `vortex_bybit_handshake.json` cada `60 s` sincronizando `CausalOrderBlockEngine` y `observe_mid_and_step_elastic_vol_with_fees` con `vortex_exec` y `vortex_replay`.
- **Escudo Diario UTC + Crecimiento Cuantico + Desacople Celular (`V4.8`):** `DailySessionGovernor` protege ganancias intradia con trinquete *High-Water Mark* y despierta en `00:00:00 UTC`; `passes_quantum_ev_growth_gate` neutraliza la distorsion de lote minimo en `BTCUSDT`; y `step_open_position_v48` comprime la cola izquierda ante barridos adversos con `MFE == 0` en paridad `1:1` (`P2-1`, `P2-2`, `P3-1`, `P3-2` cerrados).

## Enmienda Vinculante de Gobernanza Analítica (Preámbulo #5 y Ley IX)

1. **Mandato de Verdad Fría, Cero Sesgos y Cero Complacencia (5.º Invariante Fundacional):** Toda auditoría, diagnóstico, respuesta técnica y evaluación de resultados debe emitirse siempre con la verdad matemática y empírica cruda, de forma fría y analítica, sin sesgos optimistas, sin maquillar pérdidas con métricas secundarias y sin darle la razón al operador por complacencia cuando los datos o la física microestructural demuestren lo contrario.
2. **Ley IX — Gobernanza Analítica Sin Sesgos y Cláusula de Suspensión por EV Negativo:** Queda prohibido endulzar pérdidas netas, ocultar asimetrías negativas de Payoff Ratio o confundir estabilidad técnica (`0 panics`, `PARITY_SYNC Drift == 0`, `Vault 70%`) con rentabilidad económica neta (`EV > 0`). La congelación estadística de `25` tiros es suspendible de inmediato mediante `DRAIN_MODE.lock` cuando la relación ganancia/pérdida real demuestre esperanza matemática estructuralmente negativa.
