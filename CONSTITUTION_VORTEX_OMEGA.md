# CONSTITUCIÓN TÉCNICA DE VORTEX-OMEGA (V4.0)

**Arquitectura:** 19 Módulos Rust Zero-Alloc + 3 Binarios LTO Fat (`vortex_tap`, `vortex_replay`, `vortex_exec`)  
**Memoria Compartida:** `VORTEX_SHM_VERSION = 13` (`/dev/shm/vortex_lob.mmap`, SeqLock Tear-Free)  
**Cadencia del Demonio de Custodia:** `200.0 Hz` (`5 ms`) sobre 12 Carriles Autónomos Bybit UTA V5  

---

## Ley I — Verdad Contable Neta, Tarifas Exactas y Reserva Monotónica 70/30
1. Toda operación cerrada se reconcilia en escala entera exacta $10^8$ (`ParitySyncLedger`) descontando tarifas reales VIP-0 (`200 ScaledBps` Maker por pierna; `550 ScaledBps` Taker en disparo de servidor).
2. Se exige deriva contable nula (`PARITY_SYNC Drift == 0` unidades $10^{-8}\text{ USDT}$) entre el libro mayor acumulado y la suma individual de operaciones.
3. Toda ganancia neta positiva se divide de forma determinista en **70% Capital Compuesto** y **30% Fondo Monotónico de Recuperación de Cicatrices**.

## Ley II — Plasticidad Matemática y Ondas Causales de Elliott v2.0
1. `CausalElliottWaveState` evalúa 6 pivotes ZigZag estrictamente causales (cero *look-ahead*), verificando los Axiomas E1–E3, el exponente fractal de Hurst $H_t \in [0.25, 0.85]$ y el signo real de cascadas `allLiquidation` junto al impulso de interés abierto (`OI`).
2. En regímenes `ImpulseWave3` e `ImpulseWave5`, el motor proyecta extensiones estructurales Fibonacci ($1.618\times$ y $2.618\times$), expandiendo la memoria plástica `learned_mfe_bps` hasta $12\times \tau_{\text{RT}}$ (`48.00 bps`).

## Ley III — Gobernanza Bayesiana 12x2, Paridad de Riesgo en USDT y Custodia P0-B
1. `OnlineAdaptiveGovernor` dimensiona cada entrada mediante paridad continua de riesgo (`RISK_BUDGET_PPM = 2,400`, arriesgando exactamente $0.24\%$ del patrimonio al `Stop-Loss` elástico, con piso divisible de lote en contratos mayores como `BTCUSDT` y `ETHUSDT`).
2. Aplica modulación armónica continua de 24 horas UTC y veta entradas marginales en la ventana de arbitraje pre-*Funding* (`[-75 s, 0 s)` cada `8 h` UTC).
3. **Invariante P0-B (`is_safe_halt_standby`):** La presencia de `HALTED_ON_LOSS.lock` o `DRAIN_MODE.lock` bloquea nuevas entradas, pero prohíbe entrar en reposo mientras exista cualquier posición abierta, orden en cola o comando RPC en vuelo.

## Ley IV — Transporte Asíncrono No-Bloqueante y Arena `SHM v13`
1. `vortex_tap` ingiere `orderbook.50`, `publicTrade`, `tickers` y `allLiquidation`, publicando instantáneas atómicas `SymbolLobSnapshot` (incluyendo liquidez profunda `L1..L5` y `WaveObservables`) en `SHM v13` y volcando archivos columnares `PAR1` en segundo plano.
2. Ninguna llamada REST o WebSocket bloquea el bucle de `200.0 Hz`: `BybitAsyncGateway` opera por canales acotados libres de *locks* compartidos.

## Ley V — Primacía Pasiva, *Shadow-Runner*, Partición Áurea y Trinquete *Chandelier*
1. **Shadow-Runner (`Far-Field Sentinel + Snap-In`):** Bajo aceleración cinemática positiva ($\mathcal{S}_{\text{prop}} \ge \phi^{-2}$), estaciona una orden `Limit PostOnly` Centinela en la envolvente exterior para capturar mechas instantáneas sin truncar lotes unitarios, colapsando al BBO (`ShadowRunnerCrestHarvest`) con preempción de `250 ms` al desacelerar.
2. **Partición Conjugada Áurea por Onda:** Modula el Tramo 1 en $\phi_{\text{T1}} \in [350, 650]\permil$ (`~38.2%` cosecha / `~61.8%` *Runner* en `ImpulseWave3`; `~58%–62%` defensivo en `ImpulseWave5` o deuda de cicatriz), sujeto a nocional mínimo por pierna de `5.25 USDT`.
3. **Trinquete *Chandelier* en Ganancia Neta (`ChandelierProfitLockSl`):** Solo arma el `Stop-Loss` de servidor en ganancia cuando supera $G_{\text{SL, green}} = 850 + 2\,\delta_{\text{tick}}$ `ScaledBps` (`>= +1.00 bps` netos tras pagar tarifa *Taker*), validando geometría anti-`10001`, avance monotónico respecto a `verified_exchange_sl` y crédito de racha ganadora sin activar `HALTED_ON_LOSS.lock`.
4. **Escudo P0-A (`BreachEmergencyFlatten`):** Si al llenarse o adoptarse una posición la pérdida observada supera $1.5\times$ la distancia elástica de `Stop-Loss`, despacha liquidación inmediata `Market IOC` (`reduceOnly`).

## Ley VI — Escudo Anti-*Pick-Off* Hawkes/Stoikov y Parada Continua `Wald SPRT v2.0`
1. **Escudo en Cola L1 (`evaluate_moallemi_stoikov_queue_shield`):** Combina el sesgo áureo del micro-precio de Stoikov ($\mathcal{M}_{\text{skew}} = \phi^{-1} I_{\text{BBO}} + \phi^{-2} I_{\text{deep}}$) con la intensidad markoviana $O(1)$ de `HawkesQueueTracker` sobre impresiones *Taker* opuestas para abortar órdenes pasivas antes de barridos informados, bloqueando además `Re-Pegs` incoherentes.
2. **Wald SPRT v2.0 ($\Lambda_t$):** Acumula evidencia continua propia, de micro-precio y *Lead-Lag* endógeno de `BTCUSDT` ($\hat{\beta}_{\text{BTC}} \in [0.75, \phi]$) contra las fronteras exactas $(A_{\text{hold}}, B_{\text{unwind}})$ derivadas de `empirical_taker_exit_prob_pct`. Al cruzar $B_{\text{unwind}}$ con `hold_s >= 3 s`, ancla el piso pasivo cero-pérdida (`+4.25 bps` brutos) y comprime el `Stop-Loss` de servidor hasta un `48%`.

## Ley VII — Universo Elástico de 12 Contratos y Guardia Beta de Portafolio
1. El universo autorizado consta de 12 perpetuos lineales USDT (`BTC`, `ETH`, `SOL`, `AVAX`, `TAO`, `SUI`, `ORDI`, `PENDLE`, `WIF`, `LINK`, `NEAR`, `LTC`) gobernados por `PortfolioBetaGuard` (máximo 6 carriles activos simultáneos).

## Ley VIII — Evolución de Pareto y Gemelo Digital Walk-Forward `1:1`
1. Ningún cambio entra a producción sin demostrar dominancia estricta en la ablación cuádruple (`MODEL A < MODEL B < MODEL A+B < MODEL OMEGA`), `0` rechazos `PostOnly` en el Gemelo Digital FIFO (`25 ms` RTT), `63/63` cicatrices mitigadas en la variedad `2160` y `OOS 520` multi-régimen con `Maker Ratio >= 80%` y `PF >= 1.35`.

## Ley IX — Higiene Zero-Alloc, Compilación LTO Fat y Rotación por Drenaje
1. Cero asignaciones dinámicas en la ruta caliente de `200.0 Hz`, codificación `UTF-8 sin BOM + LF`, `cargo clippy --all-targets -- -D warnings` limpio y despliegue exclusivo en producción mediante drenaje verificado (`DRAIN_MODE.lock` hasta `0` posiciones abiertas).
