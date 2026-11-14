# CONSTITUCION TECNICA DE VORTEX-OMEGA (V4.3)

**Arquitectura:** 19 Modulos Rust Zero-Alloc + 3 Binarios LTO Fat (`vortex_tap`, `vortex_replay`, `vortex_exec`)  
**Memoria Compartida:** `VORTEX_SHM_VERSION = 13` (`/dev/shm/vortex_lob.mmap`, SeqLock Tear-Free)  
**Cadencia del Demonio de Custodia:** `200.0 Hz` (`5 ms`) sobre 12 Carriles Autonomos Bybit UTA V5  

---

## Ley I - Verdad Contable Neta, Tarifas Exactas y Reserva Monotonica 70/30
1. Toda operacion cerrada se reconcilia en escala entera exacta $10^8$ (`ParitySyncLedger`) descontando tarifas reales VIP-0 (`200 ScaledBps` Maker por pierna; `550 ScaledBps` Taker en disparo de servidor).
2. Se exige deriva contable nula (`PARITY_SYNC Drift == 0` unidades $10^{-8}\text{ USDT}$) entre el libro mayor acumulado y la suma individual de operaciones.
3. Toda ganancia neta positiva se divide de forma determinista en **70% Capital Compuesto** y **30% Fondo Monotonico de Recuperacion de Cicatrices**.

## Ley II - Plasticidad Matematica y Ondas Causales de Elliott v2.0
1. `CausalElliottWaveState` evalua 6 pivotes ZigZag estrictamente causales (cero *look-ahead*), verificando los Axiomas E1-E3, el exponente fractal de Hurst $H_t \in [0.25, 0.85]$ y el signo real `"S"` de cascadas `allLiquidation` junto al impulso de interes abierto (`OI`).
2. En regimenes `ImpulseWave3` e `ImpulseWave5`, el motor proyecta extensiones estructurales Fibonacci ($1.618\times$ y $2.618\times$), expandiendo la memoria plastica `learned_mfe_bps` hasta $12\times \tau_{\text{RT}}$ (`48.00 bps`).

## Ley III - Gobernanza Bayesiana 12x2, Paridad de Riesgo en USDT y Custodia P0-B
1. `OnlineAdaptiveGovernor` dimensiona cada entrada mediante paridad continua de riesgo (`RISK_BUDGET_PPM = 2,400`, arriesgando exactamente $0.24\%$ del patrimonio al `Stop-Loss` elastico, con piso divisible de lote en contratos mayores como `BTCUSDT` y `ETHUSDT`).
2. Aplica modulacion armonica continua de 24 horas UTC y veta entradas marginales en la ventana de arbitraje pre-*Funding* (`[-75 s, 0 s)` cada `8 h` UTC).
3. **Invariante P0-B (`is_safe_halt_standby`):** La presencia de `HALTED_ON_LOSS.lock` o `DRAIN_MODE.lock` bloquea nuevas entradas, pero prohibe entrar en reposo mientras exista cualquier posicion abierta, orden en cola o comando RPC en vuelo.

## Ley IV - Transporte Asincrono No-Bloqueante y Arena `SHM v13`
1. `vortex_tap` ingiere `orderbook.50`, `publicTrade`, `tickers` y `allLiquidation`, publicando instantaneas atomicas `SymbolLobSnapshot` (incluyendo liquidez profunda `L1..L5` y `WaveObservables`) en `SHM v13` y volcando archivos columnares `PAR1` en segundo plano.
2. Ninguna llamada REST o WebSocket bloquea el bucle de `200.0 Hz`: `BybitAsyncGateway` opera por canales acotados libres de *locks* compartidos.

## Ley V - Primacia Pasiva, *Shadow-Runner*, Particion Aurea y Trinquete *Chandelier*
1. **Shadow-Runner (`Far-Field Sentinel + Snap-In`):** Bajo aceleracion cinematica positiva ($\mathcal{S}_{\text{prop}} \ge \phi^{-2}$), estaciona una orden `Limit PostOnly` Centinela en la envolvente exterior para capturar mechas instantaneas sin truncar lotes unitarios, colapsando al BBO (`ShadowRunnerCrestHarvest`) con preempcion de `250 ms` al desacelerar.
2. **Particion Conjugada Aurea por Onda:** Modula el Tramo 1 en $\phi_{\text{T1}} \in [350, 650]\permil$ (`~38.2%` cosecha / `~61.8%` *Runner* en `ImpulseWave3`; `~58%-62%` defensivo en `ImpulseWave5` o deuda de cicatriz), sujeto a nocional minimo por pierna de `5.25 USDT`.
3. **Trinquete *Chandelier* en Ganancia Neta (`ChandelierProfitLockSl`):** Solo arma el `Stop-Loss` de servidor en ganancia cuando supera $G_{\text{SL, green}} = 850 + 2\,\delta_{\text{tick}}$ `ScaledBps` (`>= +1.00 bps` netos tras pagar tarifa *Taker*), validando geometria anti-`10001`, avance monotonico respecto a `verified_exchange_sl` y credito de racha ganadora sin activar `HALTED_ON_LOSS.lock`.
4. **Escudo P0-A (`BreachEmergencyFlatten`):** Si al llenarse o adoptarse una posicion la perdida observada supera $1.5\times$ la distancia elastica de `Stop-Loss`, despacha liquidacion inmediata `Market IOC` (`reduceOnly`).

## Ley VI - Escudo Anti-*Pick-Off* Hawkes/Stoikov y Parada Continua `Wald SPRT v2.0`
1. **Escudo en Cola L1 (`evaluate_moallemi_stoikov_queue_shield`):** Combina el sesgo aureo del micro-precio de Stoikov ($\mathcal{M}_{\text{skew}} = \phi^{-1} I_{\text{BBO}} + \phi^{-2} I_{\text{deep}}$) con la intensidad markoviana $O(1)$ de `HawkesQueueTracker` sobre impresiones *Taker* opuestas para abortar ordenes pasivas antes de barridos informados, bloqueando ademas `Re-Pegs` incoherentes.
2. **Wald SPRT v2.0 ($\Lambda_t$):** Acumula evidencia continua propia, de micro-precio y *Lead-Lag* endogeno de `BTCUSDT` ($\hat{\beta}_{\text{BTC}} \in [0.75, \phi]$) contra las fronteras exactas $(A_{\text{hold}}, B_{\text{unwind}})$ derivadas de `empirical_taker_exit_prob_pct`. Al cruzar $B_{\text{unwind}}$ con `hold_s >= 3 s`, comprime el `Stop-Loss` de servidor hasta un `48%`.

## Ley VII - Universo Elastico de 12 Contratos y Guardia Beta de Portafolio
1. El universo autorizado consta de 12 perpetuos lineales USDT (`BTC`, `ETH`, `SOL`, `AVAX`, `TAO`, `SUI`, `ORDI`, `PENDLE`, `WIF`, `LINK`, `NEAR`, `LTC`) gobernados por `PortfolioBetaGuard` (maximo 6 carriles activos simultaneos, `3 Bid / 3 Ask`).

## Ley VIII - Evolucion de Pareto y Gemelo Digital Walk-Forward `1:1`
1. Ningun cambio entra a produccion sin demostrar dominancia estricta en la ablacion cuadruple (`MODEL A < MODEL B < MODEL A+B < MODEL OMEGA`), `0` rechazos `PostOnly` en el Gemelo Digital FIFO (`25 ms` RTT), `63/63` cicatrices mitigadas en la variedad `2160` y `OOS 520` multi-regimen con `Maker Ratio >= 80%` y `PF >= 2.0`.

## Ley IX - Higiene Zero-Alloc, Compilacion LTO Fat y Rotacion por Drenaje
1. Cero asignaciones dinamicas en la ruta caliente de `200.0 Hz`, codificacion `UTF-8 sin BOM + LF`, `cargo clippy --all-targets -- -D warnings` limpio y despliegue exclusivo en produccion mediante drenaje verificado (`DRAIN_MODE.lock` hasta `0` posiciones abiertas).

---

## Adenda Evolutiva OMEGA V4.1 - Desbloqueo Elastico del *Runner* y Coherencia Macro-Elliott
1. **Desacoplamiento SL vs. Techo Pasivo (`observe_wald_sprt_step`):** La compresion del `Stop-Loss` de servidor permanece 100% irreversible desde `t >= 3 s`, mientras que `defensive_floor_locked` exige excursion adversa real (`< -2.00 bps` con flujo `< 0.15`) y se desbloquea dinamicamente antes de `180 s` ante resurgimiento de impulso propio ($\Phi_{\text{own}} \ge +0.35$, excursion $\ge +1.50\text{ bps}$, sin climax y fase macro coherente).
2. **Veto de Contradiccion Macro-Elliott (`passes_elliott_macro_coherence_guard`):** Bloquea el armado, pre-vuelo, `Re-Peg` y permanencia en cola (`>= 500 ms`) cuando $\Phi_{\text{macro}} \cdot s_{\text{dir}} \le -0.25$, $H_t \ge 0.55$ e $I_{\text{OI}} \cdot s_{\text{dir}} < 0.10$, o frente a ondas `ImpulseWave3/5` opuestas con proyeccion Fibonacci $\le -50\text{ bps}$ y $H_t \ge 0.50$.

## Adenda Evolutiva OMEGA V4.2 - Kernel Continuo Golden Pocket Estricto (`0.618-0.650`)
1. **Resonancia Dilatada por Tick (`compute_golden_pocket_resonance`):** Centra el nucleo plano en `[0.618, 0.650]` con dilatacion discreta `max(0.015983, 0.55 / w1)` para garantizar `0%` de reticulas muertas en los 12 contratos sin modificar `VORTEX_SHM_VERSION = 13`.
2. **Acoplamiento en 4 Vectores:** Cuando `G_gp >= 0.85` en `ImpulseWave3`, expande la proyeccion Fibonacci `k3`, profundiza `snr_threshold_delta` hasta `-0.038`, extiende la paciencia defensiva a `1,250 permille` y habilita la particion `35% T1 / 65% Runner` (`350 permille`).

## Adenda Evolutiva OMEGA V4.3 - Precalentamiento Persistente EMA, Anti-Rafaga y Signo `OI`
1. **Precalentamiento Cinematico Persistente (`vortex_ema_prewarm.json`):** Hidrata `ewma_fast_px_1e4` (`20 s`), `ewma_slow_px_1e4` (`300 s`) y `eq_vol_5m_bps` al iniciar `vortex-exec` y guarda puntos de control cada `30 s`, erradicando entradas ciegas con `drift_bps == 0.0`.
2. **Serializacion Anti-Rafaga Direccional (`can_stage_same_direction_entry`):** Prohibe abrir un segundo carril en la misma direccion mientras otro carril del mismo lado este en cola o lleve `< 60 s` y `< +4.50 bps` brutos en verde.
3. **Invarianza Escalar de `oi_impulse` (`passes_elliott_macro_coherence_guard`):** Veta entradas contra fase macro opuesta (`<= -0.25`) desde `H >= 0.502` sin invertir el signo de `oi_impulse` en cortos.
