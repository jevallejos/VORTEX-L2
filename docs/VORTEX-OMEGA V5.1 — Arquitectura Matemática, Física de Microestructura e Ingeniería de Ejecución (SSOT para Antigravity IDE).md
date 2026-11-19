# **VORTEX-OMEGA V5.1 — Arquitectura Matemática, Física de Microestructura e Ingeniería de Ejecución (SSOT para Antigravity IDE)**

Este documento constituye la especificación técnica integral (**Single Source of Truth**) del motor cuantitativo autónomo **vortex-rs (VORTEX-OMEGA V5.1)** para negociación algorítmica de alta frecuencia (200.0 Hz / 5 ms) en perpetuos lineales USDT de **Bybit Unified Trading Account (UTA V5)**. Está diseñado para que cualquier agente en **Antigravity IDE** comprenda la física del libro de órdenes, las ecuaciones continuas (${C}^{1}/{C}^{\infty }$), los invariantes de memoria y las reglas de modificación segura antes de intervenir el código fuente.

## **1\. Coordenadas del Sistema, Topología y Binarios Oficiales**

* **Repositorio y Ruta Local (Windows 11 / Antigravity IDE):** D:\\Desarrollo\\VORTEX-L2 (git@github.com:jevallejos/VORTEX-L2.git, rama principal main).  
* **Nodo Productivo (Tokio / Singapur):** sniper-singapur (ip-172-31-46-228, Ubuntu Linux x86\_64, /home/ubuntu/vortex-rs), conectado al motor de emparejamiento de Bybit UTA V5 (wss://\[stream.bybit.com/v5/public/linear\](https\://stream.bybit.com/v5/public/linear) y wss://\[stream.bybit.com/v5/trade\](https\://stream.bybit.com/v5/trade)) con latencia de red medida ws\_rtt\_us ≈ 73–79 ms.  
* **Perfil de Compilación (Cargo.toml):** Rust 2021/1.99+, lto \= "fat", codegen-units \= 1, opt-level \= 3, panic \= "abort", 0 dependencias externas de C/C++ (100% Rust puro para evitar bloqueos de seguridad en Windows 11 y garantizar determinismo binario).


| Binario (src/bin/) | Servicio systemd | Prioridad Kernel | Función y Frontera Operativa |
| :---- | :---- | :---- | :---- |
| **vortex\_tap** | vortex-tap.service | Nice=-10, OOM=-800 | Ingestor público WebSocket (12 pares), escritor único de /dev/shm/vortex\_lob.mmap (SHM v15), anillo circular PAR2 (4,096 segmentos), archivador frío .par2z y almacén perpetuo de velas VKL1 (1s..1d). |
| **vortex\_exec** | vortex-exec.service | Nice=-15, OOM=-800 | Custodio autónomo a **200.0 Hz (5 ms)** sobre 12 carriles, pasarela privada asíncrona WS/REST, compuerta Bayesiana 12x2, escudos Stoikov \+ Hawkes \+ Wald, *Shadow-Runner*, Chandelier y telemetría fuera de hilo. |
| **vortex\_replay** | Ejecución bajo demanda | Nice=19, ionice \-c 3 | Simulador determinista FIFO L1..L5 (25 ms RTT, \> 1.0 GHz), laboratorio de ablación Pareto de 4 brazos (MODEL A \< B \< A+B \< OMEGA) y Gemelo Digital 1:1 sobre archivos PAR1/PAR2 y .par2z. |
| **vortex\_web** | vortex-web.service | Nice=19, IOSched=idle | Observatorio local en **127.0.0.1:8787** (X-Vortex-Csrf de 256 bits, SSE a 5 Hz, gráficos *Lightweight Charts v5*, portapapeles Markdown limpio para IA, control DRAIN\_MODE y lanzador LAB 1..4). |

## **2\. Los 4 Invariantes de Compilación y las 9 Leyes Constitucionales**

Todo cambio de código debe satisfacer simultáneamente los **4 Invariantes Fundacionales** y las **9 Leyes Constitucionales**:

### **2.1. Preámbulo: Los 4 Invariantes Inviolables**

> 1. **Seguridad Dimensional Estricta (Newtypes en src/types.rs):** Prohibido pasar primitivos crudos (i64, f64) entre módulos. Toda magnitud usa tipos \#\[repr(transparent)\]: PriceTicks(i64), LotQty(i64), ScaledBps(i64) (donde $1bp=100$ unidades; ej. 400 \= 4.00 bps), BoundedScore(f64) acotado a $[-1.0,+1.0]$, SymbolId(u16) y TimestampNs(i64).  
> 2. **Higiene Zero-Alloc y Cero Panics en Ruta Caliente (200.0 Hz):** Cero asignaciones dinámicas en *heap* (Vec, Box, String, HashMap) dentro del lazo de 5 ms y prohibición absoluta (clippy::pedantic \-D warnings) de .unwrap(), .expect() y panic\!(). El único código unsafe reside aislado en src/lob\_shm.rs protegido por barreras atómicas Acquire/Release.  
> 3. **Ley de No-Regresión Paramétrica (SCAR-LOCK):** Ningún umbral de supervivencia sellado tras una pérdida real en producción puede relajarse durante periodos de calma ("suavizado por silencio"). Toda evolución debe mantener SCAR-LOCK 63/63 (cero reapertura de las 63 cicatrices históricas en la variedad de 2,160 estados).  
> 4. **Paridad Exacta Gemelo–Producción (1:1):** Toda ecuación o guardia activa en vortex\_exec debe ejecutarse de forma idéntica en OmegaDigitalTwinEngine y run\_parquet\_walk\_forward\_on\_dir (src/replay\_walkforward.rs) con PARITY\_SYNC Drift \== 0\.

### **2.2. Síntesis de las 9 Leyes Constitucionales (V4.1 \-\> V5.1)**

* **Ley I (Verdad Contable 1e8 y Bóveda 70/30):** Toda liquidación de PnL, comisiones Maker/Taker (incluidos *rebates* negativos) y *funding* se calcula en enteros de 1e-8 USDT (ParitySyncLedger), asignando 70% a Capital Compuesto Blindado y 30% a Reserva de Saneamiento.  
* **Ley II (Plasticidad Continua ${C}^{1}$, Elliott v2.0 y \[OB+, OB-\]):** Prohibidos los cortes escalón discontinuos; toda transición usa el núcleo cúbico de Hermite ${S}_{3}(x;a,b)\in {C}^{1}(R)$, pivotes causales sin *look-ahead*, suelo endógeno de liquidaciones y memoria direccional separada para bloques de demanda (OB+) y oferta (OB-).  
* **Ley III (Gobernanza Bayesiana 12x2, Paridad 2,400 ppm y Custodia P0-B):** Matriz de 24 celdas (slot\_idx, Side), riesgo base de 2,400 ppm (0.24% del capital) hasta el Stop-Loss, calendario civil $O(1)$ con horario de verano (BST/EDT) \+ *carry* de fundingRate, rehabilitación Shadow-Decay solo ante mutación verificada de régimen y espera viva silenciosa bajo DRAIN\_MODE.lock.  
* **Ley IV (Transporte Zero-Jitter e IPC SHM v15):** Desacoplamiento total entre ingestión (vortex\_tap), ejecución (vortex\_exec), web (vortex\_web) y persistencia en disco (vault\_io mediante .tmp \-\> sync\_all \-\> rename con VaultIoCommand \<= 512 B).  
* **Ley V (Primacía Pasiva Limit PostOnly, *Shadow-Runner* y Chandelier):** Prohibido cruzar el *spread* en entradas o salidas normales; *Snap-In* prioritario gobernado por ws\_rtt\_us, trinquete ChandelierProfitLockSl neto positivo y aplanamiento de emergencia P0-A (Market IOC reduceOnly) ante brechas $\geq 1.5\times SL$.  
* **Ley VI (Escudo de Cola Stoikov \+ Hawkes y Parada Óptima Wald SPRT v2.0):** Cota física $0\leq {Q}_{ahead}(t)\leq {Q}_{L1}(t)$, separación entre consumo FIFO legítimo y cancelación ballena, paciencia elástica Hawkes y contracción continua del SL ante toxicidad confirmada.  
* **Ley VII (Universo de 12 Contratos, Correlación Epps y Guardia Monetaria Beta):** Operación exclusiva sobre APPROVED\_SPECS verificados al arranque, correlación EWMA 5s contra BTC (${\hat{\beta }}_{i,BTC}\in [-\phi ,+\phi ]$) y MonetaryPortfolioBetaGuard ($\leq 14,400ppm$ agregado, $\leq 10,800ppm$ direccional, máx. 6 carriles abiertos).  
* **Ley VIII (Dominancia Estricta de Pareto y *Markouts* LVR):** Validación obligatoria $PnL(A)<PnL(B)<PnL(A+B)<PnL(\Omega )$, 0 rechazos PostOnly y trazabilidad de *markouts* en 5 horizontes (+250ms, \+1s, \+5s, \+15s, \+60s).  
* **Ley IX (Higiene UTF-8 LF, Compuertas CI y Estacionariedad N \>= 25):** Clippy \== 0, pruebas en /dev/shm en verde y prohibición de reiniciar vortex-exec.service antes de completar la muestra estacionaria (N \>= 25 operaciones cerradas), usando rotación Zero-Restart sobre vortex-tap.service.

## **3\. Arquitectura Modular de los 25 Módulos (src/)**

| Capa Arquitectónica | Módulos en src/ | Responsabilidad Matemática y de Sistemas |
| :---- | :---- | :---- |
| **Tipos, Tarifas y Riesgo Base** | types.rs, fee\_cadence\_beta.rs | Newtypes dimensionales, APPROVED\_SPECS (12 pares), FeeSchedule VIP dinámico, BetaBinomialTakerPrior, calendario civil BST/EDT, passes\_asymmetry\_ev\_gate\_v51 y MonetaryPortfolioBetaGuard. |
| **IPC y Almacenamiento Binario** | lob\_shm.rs, forensic\_parquet.rs, tap\_ring.rs, candle\_store.rs, par2\_archiver.rs, vault\_io.rs | Arena SHM v15 (25 KB, SeqLock), lotes PAR2 (256 B/fila), archivo frío .par2z (VPZ1), velas perpetuas VKL1 (96 B/vela), trabajador asíncrono vault\_io y AtomicLockWatch (0 ns). |
| **Microestructura y Geometría** | alpha\_3factor.rs, wave\_elliott.rs, order\_block.rs | OFI multi-escala, TFI, GEOM anti-*spoof*, EventClockAlphaModulator, EppsBtcCorrelationTracker, Stoikov, LVDBS, pivotes causales Elliott, frontera fractal CorrectiveAbc, \[OB+, OB-\] y separación consumo FIFO vs. cancelación L1. |
| **Gobernanza y Superficies C1** | kinematic\_surface.rs, governor\_plasticity.rs, bayesian\_governor.rs | Superficie de energía cinemática, SCAR-LOCK 63/63, plasticidad Beta-Binomial ponderada por sorpresa (F6), enfriamiento cruzado Bid \+ Ask en CorrectiveAbc, Shadow-Decay y DailySessionGovernor. |
| **Ejecución FSM y Custodia 200Hz** | exit\_quoter.rs, execution\_fsm.rs, exec\_daemon.rs, exec\_gateway\_handlers.rs | Desbloqueo ganador reversible C1 (F2), piso difusivo 1m de SL (F3), Wald SPRT v2.0, *Shadow-Runner*, Chandelier, histéresis WS-RTT anti-spoof/anti-barrido (F4b), espera viva DRAIN\_MODE (F1) y P0-A/P0-B. |
| **Red Bybit V5 y Simulación** | ws\_protocol.rs, http\_ws\_transport.rs, bybit\_client.rs, bybit\_gateway.rs, fifo\_sim.rs, replay\_walkforward.rs | Firma HMAC-SHA256, HTTP/1.1 *Keep-Alive* (*chunked-safe*), handshake VIP/contratos, *Atomic Amend* WS, simulador FIFO L1..L5 (25 ms RTT) y Gemelo Digital 1:1. |
| **Observabilidad y Web Loopback** | markout\_tracker.rs, telemetry\_hud.rs, web\_server.rs | Curvas de *markout* 5h y toxicidad Glosten-Milgrom ${\zeta }_{5h}$, HUD terminal SHM v15, baúles JSONL con FNV-1a y servidor HTTP/SSE en 127.0.0.1:8787. |

### **Universo Autorizado de 12 Contratos (APPROVED\_SPECS)**

| Slot | Contrato (SymbolId) | tick\_size\_1e8 | lot\_step\_1e8 | Slot | Contrato (SymbolId) | tick\_size\_1e8 | lot\_step\_1e8 |
| :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- |
| 0 | BTCUSDT | 10\_000\_000 (0.1) | 100\_000 (0.001) | 6 | ORDIUSDT | 100\_000 (0.001) | 10\_000\_000 (0.1) |
| 1 | ETHUSDT | 1\_000\_000 (0.01) | 1\_000\_000 (0.01) | 7 | PENDLEUSDT | 10\_000 (0.0001) | 100\_000\_000 (1.0) |
| 2 | SOLUSDT | 1\_000\_000 (0.01) | 10\_000\_000 (0.1) | 8 | WIFUSDT | 10\_000 (0.0001) | 100\_000\_000 (1.0) |
| 3 | AVAXUSDT | 100\_000 (0.001) | 10\_000\_000 (0.1) | 9 | LINKUSDT | 100\_000 (0.001) | 10\_000\_000 (0.1) |
| 4 | TAOUSDT | 1\_000\_000 (0.01) | 1\_000\_000 (0.01) | 10 | NEARUSDT | 100\_000 (0.001) | 10\_000\_000 (0.1) |
| 5 | SUIUSDT | 10\_000 (0.0001) | 100\_000\_000 (1.0) | 11 | LTCUSDT | 1\_000\_000 (0.01) | 10\_000\_000 (0.1) |

## **4\. Ingeniería IPC (SHM v15), Formatos Binarios y Bóveda Forense**

### **4.1. Contrato de Memoria Compartida SHM v15 (src/lob\_shm.rs)**

* **Archivo:** /dev/shm/vortex\_lob.mmap (25 KB, \#\[repr(C, align(64))\], VORTEX\_SHM\_MAGIC \= 0x564F\_5254\_4558\_4C33, VORTEX\_SHM\_VERSION \= 15).  
* **Protocolo de Publicación Volátil *Publish-Last*:** vortex\_tap limpia la arena, escribe version \= 15 con ptr::write\_volatile, emite fence(Ordering::Release) y escribe magic al final. Los lectores (vortex\_exec, vortex\_web, vortex\_exec \--live) validan magic y version con ptr::read\_volatile \+ fence(Ordering::Acquire).  
* **Consistencia SeqLock *Wait-Free* ($O(1)$):**  
  \$\$s\_{\\text{odd}} \= s\_0 \+ 1 \+ (s\_0 \\bmod 2), \\qquad \\text{Lectura Válida} \\iff (s\_1 \\land 1 \= 0\) \\;\\land\\; (s\_1 \= s\_2)\$\$  
  Esto garantiza 0 ns de bloqueo mutuo entre el productor de red y el lazo de 200.0 Hz.  
* **Centinela de Anomalía L2 y Desvío Mark \- Mid:** Si el salto de secuencia supera ${U}_{\max\limits_{}}$ o la divergencia entre precio de marca e índice medio excede la banda elástica de volatilidad, se dispara resincronización limpia del libro y se incrementa tap\_seq\_resync\_count:  
  \$\$\\text{Anomalía}\_{\\text{L2}} \\iff (\\Delta u \> U\_{\\max}) \\;\\lor\\; \\left(\|P\_{\\text{mark}} \- P\_{\\text{mid}}\| \\cdot 10^6 \> P\_{\\text{mid}} \\cdot \\max(3\\,\\sigma\_{5m},\\; 4\\,B\_{3\\times})\\right)\$\$  
* **Telemetría por Carril (SlotTelemetrySnapshot, 96 B):** Publica estado de máquina (0=Idle, 1=Placing, 2=Resting, 3=PositionOpen), PyG UPL en ScaledBps, queue\_ahead\_lots y last\_veto\_code: u8 (LastVetoCode: Friction3xOrEligibility, SpoofingVeto, QueueDepthOrOversizedLot, BetaOrSameSideStagger, ScarCooldownOrContagion, KinematicSurfaceOrCadence, BtcLeadLagHeadwind, MacroElliottContradiction, OpposingOrderBlockWall, PreflightDriftOrDecay, HawkesStoikovQueueAbort, WarmupOrLock).

### **4.2. Formatos de Datos y Persistencia (PAR2, .par2z, VKL1 y data/vault/)**

* **Anillo Columnar PAR2 (256 B/fila, 4 líneas de caché):** Almacena hasta 4,096 segmentos (data/vortex\_l2\_segment\_\*.parquet) con profundidad L1..L5, PublicTradePrint, TickerMeta (openInterest, fundingRate, markPrice), LiquidationPrint y checksum FNV-1a 64-bit.  
* **Archivo Frío .par2z (VPZ1):** Antes de rotar segmentos antiguos del anillo, par2\_archiver.rs (Nice=19) aplica codificación Stride-256 XOR-Delta \+ Zero-RLE con compresión flate2 en data/archive/YYYY-MM-DD/vortex\_par2\_HH.par2z.  
* **Almacén Perpetuo de Velas VKL1 (96 B/vela, \#\[repr(C)\]):** Guarda historial ilimitado en data/candles/{SYMBOL}\_{TF}.vkl1 (1s, 1m, 5m, 15m, 1h, 4h, 1d) con búsqueda binaria $O(\log N)$, OHLCV, medias de OFI/TFI/GEOM, OI, fundingRate, liquidaciones, régimen Elliott, exponente de Hurst, \[OB+, OB-\] y conteo de SpoofVetoes.  
* **Bóveda Operativa (data/vault/):**  
  * **6 Bitácoras Históricas Inmutables (.jsonl con fnv1a64\_hex):** vortex\_ai\_forensic\_vault.jsonl (cierres TRADE\_CLOSE, cortes EPOCH\_RESET y *funding*), vortex\_position\_paths.jsonl (película 1 Hz de hasta 128 muestras por operación \+ *markout* 5h), vortex\_queue\_aborts.jsonl (auditoría contrafactual \+60s de abortos en cola), vortex\_plasticity\_audit.jsonl (fotografía cada 15 min de priors, MFE, \[OB+, OB-\], correlación Epps y ws\_rtt\_us), vortex\_governor\_scar\_memory.jsonl (matriz 12x2 de cicatrices) y vortex\_red\_loss\_box.jsonl (pérdidas rojas para SCAR-LOCK).  
  * **6 Estados Vivos Atómicos (.json vía .tmp \-\> sync\_all \-\> rename):** vortex\_open\_positions\_state.json, vortex\_daily\_governor.json, vortex\_mfe\_plasticity.json, vortex\_ob\_plasticity.json, vortex\_ema\_prewarm.json y vortex\_bybit\_handshake.json.

## **5\. Física Estadística y Ecuaciones Matemáticas (VORTEX-OMEGA V5.1)**

Toda la dinámica del motor se expresa en términos adimensionales escalados por la proporción áurea $\phi =\frac{1+\sqrt{5}}{2}\approx 1.618034$ (${\phi }^{-1}=\phi -1\approx 0.618034$, ${\phi }^{-2}=2-\phi \approx 0.381966$, ${\phi }^{-3}=2\phi -3\approx 0.236068$, $\sqrt{5}=\phi +{\phi }^{-1}$) y el interpolador cúbico continuo ${C}^{1}$ de Hermite:

$${S}_{3}(x;a,b)={u}^{2}(3-2u),u=clamp\left({\frac{x-a}{b-a},0,1}\right)\in {C}^{1}(R)$$

### **5.1. Factores Ortogonales de Microestructura (src/alpha\_3factor.rs)**

> **Order Flow Imbalance Multi-Escala (OFI):** Calcula cambios de volumen en los niveles L1..L5 con pesos $[1.0,0.70,0.45,0.25,0.12]$, acumulando flujos positivos y negativos en tres constantes de tiempo exponenciales (0.50 s, 3.00 s, 15.00 s) acotadas por debajo por flow\_prior\_bounds (escalado según characteristic\_lot\_scale de cada par):

> $$OF{I}_{i}(t)=0.50{f}_{fast}+0.32{f}_{med}+0.18{f}_{slow}\in [-1,+1]$$

> 1.   
> 2. **Trade Flow Imbalance (TFI) y Geometría de Libro (GEOM):**  
>    \$\$\\text{TFI}\_i(t) \= \\frac{V\_{\\text{buy},i}(t) \- V\_{\\text{sell},i}(t)}{V\_{\\text{buy},i}(t) \+ V\_{\\text{sell},i}(t) \+ \\epsilon}, \\qquad \\text{GEOM}\_i(t) \= \\frac{\\sum\_{k=1}^{10} \\frac{1}{k}\\big(Q\_{\\text{bid},k} \- Q\_{\\text{ask},k}\\big)}{\\sum\_{k=1}^{10} \\frac{1}{k}\\big(Q\_{\\text{bid},k} \+ Q\_{\\text{ask},k}\\big) \\cdot \\sqrt{\\max(1, \\text{Spread}\_{\\text{ticks}})}}\$\$  
> 3. **Reloj de Eventos y Atenuación Anti-*Spoofing* (EventClockAlphaModulator):** La dilatación temporal ${\kappa }_{evt,i}(t)=\sqrt{{\lambda }_{fast}/{\lambda }_{eq}}\in [{\phi }^{-1},\phi ]$ acelera los filtros durante ráfagas de órdenes. Cuando $|OFI|\geq 0.35$ diverge de $TFI$ (OFI \* TFI \< \-0.05 o libro inflado sin ejecuciones), sube spoof\_density\_ewma ${s}_{sp}\in [0,1]$ y transfiere peso desde GEOM hacia TFI:  
>    \$\$w\_{\\text{geom}} \= 0.20\\big(1 \- \\phi^{-1}\\tilde{s}\_{\\text{sp}}\\big), \\qquad w\_{\\text{tfi}} \= 0.35 \+ \\phi^{-1}(0.20 \- w\_{\\text{geom}}), \\qquad w\_{\\text{ofi}} \= 1 \- w\_{\\text{tfi}} \- w\_{\\text{geom}}\$\$

> **Micro-Precio Áureo de Stoikov y Surf de Vacío (LVDBS):**

> $${M}_{skew,i}=clamp\left({{\phi }^{-1}{I}_{BBO,i}+{\phi }^{-2}{I}_{deep,i},-1,+1}\right)$$

> 4.   
>    El índice de vacío compute\_vacuum\_index\_l2\_l3 proyecta el precio objetivo Maker hacia niveles L1..L4 cuando la liquidez opuesta es delgada y el flujo TFI empuja en dirección favorable.

> **Propagación Cross-Asset Epps (EppsBtcCorrelationTracker):** Muestrea retornos cada 5 s con memoria exponencial ($\tau =120s$) y prior bayesiano para estimar la correlación viva con BTCUSDT:

> $${\hat{\rho }}_{i,BTC}=\frac{Co{v}_{i,BTC}+{\phi }^{-1}{\sigma }_{0}^{2}}{\sqrt{(Va{r}_{i}+{\sigma }_{0}^{2})(Va{r}_{BTC}+{\sigma }_{0}^{2})}}\in [-1,+1],{\hat{\beta }}_{i,BTC}=clamp\left({{\hat{\rho }}_{i,BTC}\frac{{\sigma }_{5m,i}}{{\sigma }_{5m,BTC}},-\phi ,+\phi }\right)$$

> 5. 

### **5.2. Geometría Fractal Causal de Elliott, Hurst y \[OB+, OB-\]**

* **Pivotes Causales y Régimen de Onda (src/wave\_elliott.rs):** Detecta hasta 6 pivotes alternantes sin *look-ahead*, normalizando el umbral de liquidaciones endógenamente (EndogenousLiquidationFloor) según el flujo nocional de cada contrato, y clasifica el mercado en NeutralWarmup, ImpulseWave3, CorrectiveWave4, ImpulseWave5, ClimaxExhaustion o CorrectiveAbc.

**Frontera Fractal Auto-Aprendida en CorrectiveAbc (F5 — passes\_corrective\_abc\_fractal\_guard\_c1):** En rangos correctivos, el exponente de Hurst admisible ${H}_{crit,i,d}(t)$ se modula continuamente por el equilibrio direccional de eficacia de *Order Blocks* ${\rho }_{ob}=\frac{O{B}_{align}}{O{B}_{align}+O{B}_{opp}}$ y la fase macro alineada ${\Phi }_{al}={s}_{dir}{W}_{macro}$:

$${H}_{crit,i,d}(t)=\frac{1}{2}+{\phi }^{-3}{S}_{3}({\rho }_{ob};{\phi }^{-2},{\phi }^{-1})+{\phi }^{-5}{S}_{3}({\Phi }_{al};{\phi }^{-3},{\phi }^{-1})-{\phi }^{-3}{S}_{3}(-{\Phi }_{al};0,{\phi }^{-1})$$

*   
  Toda entrada en CorrectiveAbc con ${H}_{t}>{H}_{crit,i,d}(t)$ queda bloqueada, eliminando los stops por persistencia lateral sin respaldo institucional. Además, en CorrectiveAbc se suprime el inflado artificial de volatilidad hacia eq\_horizon proporcionalmente a la persistencia fractal adversa.  
* **Bloques Institucionales Desacoplados (src/order\_block.rs):** Mantiene eficacia independiente ob\_bull\_efficacy\_pm y ob\_bear\_efficacy\_pm en \[382..=854‰\], invalida zonas en 0 ms cuando el precio perfora el muro y desaloja zonas obsoletas por distancia ponderada (ProximityObBank).

### **5.3. Fricción VIP Dinámica, Compuerta Real de 1 Lote (F5) e Inferencia Bayesiana (F6)**

* **Costo Round-Trip Dinámico y Barrera 3x (src/fee\_cadence\_beta.rs):** Acopla tarifas reales VIP (maker\_fee\_bps, taker\_fee\_bps, soportando *rebates* negativos) con la probabilidad posterior de salida Taker ${\hat{p}}_{T}={\hat{p}}_{T,i,d}/1000$:  
  \$\$C\_{\\text{RT},i,d}(t) \= f\_{\\text{mkr}} \+ \\big(1 \- \\hat{p}\_T\\big) f\_{\\text{mkr}} \+ \\hat{p}\_T \\big(f\_{\\text{tkr}} \+ \\tau\_{\\text{tick},i}\\big), \\qquad B\_{3\\times,i,d}(t) \= 3 \\cdot C\_{\\text{RT},i,d}(t)\$\$  
* **Esperanza Neta Real en Contratos Indivisibles de 1 Lote (F5 — passes\_asymmetry\_ev\_gate\_v51):** Cuando el capital asigna 1 único lote (can\_split\_runner\_legs \== false, como ETHUSDT a \~\$65 USDT), evalúa la esperanza neta sin divisores artificiales sobre el costo Taker:  
  \$\$\\text{EV}\_{1\\text{-lot}} \= (1 \- \\hat{p}\_T)\\big((\\phi^{-1} \+ \\phi^{-2} q\_{\\text{struct}}) G\_{\\text{cap}} \- C\_{\\text{RT,mkr}}\\big) \- \\hat{p}\_T \\big(\\Delta\_{\\text{SL,diff}} \+ C\_{\\text{RT,tkr}} \+ \\phi^{-2}\\tau\_{\\text{tick}}\\big) \\ge \\phi^{-3} C\_{\\text{RT,mkr}}\$\$  
* **Actualización Beta-Binomial Ponderada por Sorpresa y Escudo Cruzado (F6 — src/governor\_plasticity.rs):** Ante un Stop-Loss sin descubrimiento (${S}_{3}({C}_{RT,mkr}-MFE;0,{C}_{RT,mkr})>0$), contrae el olvido benigno \$\\gamma\_{\\text{eff}} \\in \[724, 960\]\\permil\$ e incrementa ${\alpha }_{obs}$ proporcionalmente a $(1-{\hat{p}}_{T})(1+\phi {s}_{rec})$ sin acelerar jamás el olvido de pérdidas previas (Ley III). Si el SL ocurre en CorrectiveAbc, propaga alerta simpática al lado opuesto (observe\_sympathetic\_chop\_coupling) y activa enfriamiento cruzado passes\_cross\_side\_abc\_chop\_shield.  
* **Dimensionamiento Kelly por Paridad de Riesgo (2,400 ppm) y Escudo Diario UTC:** Calcula lotes para que el riesgo all-in al SL iguale el presupuesto ajustado por ${\widehat{IR}}_{i,d}$, cadencia horaria BST/EDT, *carry* direccional de fundingRate (passes\_elastic\_funding\_cliff\_guard) y el amortiguador diario kelly\_boundary\_damper\_permille (DailySessionGovernor: tope de pérdida diario elástico entre \-3,883 ppm y \-6,283 ppm según reserva 30%, y trinquete *Profit-Lock* con reinicio autónomo a las 00:00:00 UTC).

### **5.4. Física de Cola Pasiva (F4), Desbloqueo Reversible (F2), Piso Difusivo 1m (F3) y Wald SPRT v2.0**

> **Identidad Física de Cola y Separación Consumo FIFO vs. Cancelación (F4a):**

> $$0\leq {Q}_{ahead}(t)\leq {Q}_{L1}(t)$$

> 1.   
>    should\_abort\_l1\_cancellation\_collapse\_v51 resta las ejecuciones reales en cinta $\Delta {V}_{opp}(t)$ de toda caída en L1 para aislar retiros puros de liquidez ballena (${\rho }_{cancel}$), evitando abortar cuando la cola avanza legítimamente hacia nuestro llenado.  
> 2. **Paciencia Elástica Hawkes e Histéresis WS-RTT (F4b):**  
>    \$\$\\Delta t\_{\\text{patience},i}(t) \= \\frac{\\phi^{-6}\\,\\tau\_{\\text{mkt},i}(t)\\big(1 \+ \\phi^{-1}S\_3(\\bar{\\Phi}\_{\\text{slow}}) \- \\phi^{-2}S\_3(-\\bar{\\Phi}\_{\\text{slow}})\\big)}{\\kappa\_{\\text{evt},i}(t)\\sqrt{\\hat{\\lambda}\_{H,i}(t)}} \\cdot \\sqrt{\\frac{\\sigma\_{\\text{eq},i}}{\\sigma\_{5m,i}}}\$\$  
>    La histéresis de libro evaluate\_elastic\_spoof\_geom\_abort\_v51 exige persistencia ${\phi }^{2}{\widehat{RTT}}_{ws}(1+{\phi }^{-1}{s}_{sp})/{\kappa }_{evt}$ ante parpadeos leves de GEOM, mientras evaluate\_toxic\_sweep\_velocity\_abort\_v51 cancela al instante ante barridos institucionales veloces.  
> 3. **Desbloqueo Ganador Reversible ${C}^{1}$, Preservación de *Runner* y Escape ImpulseWave3 (F2):**  
   * Sustituye worst\_mae\_bps por la EMA intrínseca lenta de flujo ${\bar{\Phi }}_{slow,i}(t)$ acoplada a ${\tau }_{flow,i}(t)=\frac{{\phi }^{-6}{\tau }_{mkt,i}}{{\kappa }_{evt,i}}+\phi {\widehat{RTT}}_{ws}(t)$.  
   * Al recuperar flujo propulsivo ${\bar{\Phi }}_{slow,i}(t)\geq {\Phi }_{unlock,i,d}(t)$, desbloquea defensive\_floor\_locked \= false y agenda *re-peg* adaptativo $\Delta {t}_{repeg,i}(t)$ hacia el TP estructural, preservando Tranche1Harvest50 \+ Runner en contratos divisibles y evacuando pasivamente solo si surge una ImpulseWave3 opuesta (${s}_{dir}{W}_{macro}\leq -{\phi }^{-1}$).  
> 4. **Piso Difusivo Browniano 1m en Server SL (F3):** Escalando la volatilidad de apertura mediante ${\sigma }_{1m,open}={\sigma }_{5m,open}/\sqrt{5}$, el piso del trinquete monotónico protege la respiración ordinaria contra picos de 5 ms y contrae hacia ${\phi }^{2}{C}_{RT}(t)$ únicamente bajo toxicidad confirmada ${\chi }_{tox}(t)\in [0,1]$ en SlotMarkoutRing (${\zeta }_{5h}$) o giro ImpulseWave3:  
>    \$\$\\Delta\_{\\text{SL,floor}}(t) \= (1 \- \\chi\_{\\text{tox}}(t))\\,\\max\\\!\\Big(\\phi^3 C\_{\\text{RT}}(t),\\; \\frac{\\phi}{\\sqrt{5}}\\sigma\_{5m,\\text{open}}(1 \- \\phi^{-2}\\hat{p}\_T)\\Big) \+ \\chi\_{\\text{tox}}(t)\\,\\phi^2 C\_{\\text{RT}}(t)\$\$  
> 5. **Prueba Secuencial Wald SPRT v2.0 y ChandelierProfitLockSl:** Acumula la razón de verosimilitud ${\Lambda }_{t}$ entre continuación y agotamiento frente a fronteras $({A}_{hold}({\hat{p}}_{T}),{B}_{unwind}({\hat{p}}_{T}))$. Al cruzar ${B}_{unwind}$, activa escape Maker sobre el piso cuántico cero-pérdida (defensive\_zero\_loss\_maker\_exit\_price\_with\_fees) y contrae suavemente el SL. Cuando el precio avanza en ganancia, arma ChandelierProfitLockSl asegurando cierre neto positivo incluso ante reversión brusca.

## **6\. Protocolo Operativo y Reglas de Intervención para Antigravity IDE**

Cuando el agente en **Antigravity IDE** deba auditar, extender o refactorizar D:\\Desarrollo\\VORTEX-L2, debe cumplir estrictamente este protocolo:

> 1. **Higiene de Archivos y Scripts en Windows 11:**  
   * Escribir siempre en **UTF-8 sin BOM** con saltos de línea **LF (\\n)** ((New-Object System.Text.UTF8Encoding(\[bool\]0)) y .Replace(\[string\]\[char\]13, "")).  
   * En scripts PowerShell pegados en consola, encapsular en & { ... }, usar Set-Variable / Get-Variable sin signos \$ sueltos y mantener expresiones .Replace(...) en línea continua para evitar errores de análisis sintáctico.  
> 2. **Prohibición de Constantes Mágicas:** Toda nueva regla o escudo debe formularse con razones áureas ($\phi ,{\phi }^{-1},{\phi }^{-2},{\phi }^{-3}$), interpolación ${S}_{3}\in {C}^{1}$, escalas intrínsecas (${\tau }_{mkt},{B}_{3\times },{\sigma }_{5m},{\widehat{RTT}}_{ws}$) o estimadores Bayesianos auto-aprendidos.  
> 3. **Compuertas de Certificación Locales Obligatorias (Antes de git push):**  
>    PowerShell  
>    cargo clippy \-\-all-targets \-- \-D warnings  
>    cargo test \-\-lib \-- \-\-test-threads\=1  
>    cargo test \-\-tests \-- \-\-test-threads\=1  
>    cargo build \-\-release \-\-bins

   * **Umbrales vigentes certificados:** Clippy \= 0 warnings, 80/80 pruebas unitarias \--lib, 28/28 vectores Red-Team (v50\_adversarial\_loss\_hunter), 500 episodios Monte Carlo (PF \>= 3.7), De-Legacy A/B (PF \= 1.64, \+570.11 bps, Drift \= 0), V5.1 Forensic Suite (F1..F6), SCAR-LOCK \= 63/63 (2,160 estados), OOS 520 (Maker \>= 80%, PF \>= 1.35, Drift \= 0\) y Digital Twin 1:1 (3/3 trades, \+98.73 bps, Drift \= 0).  
> 4. **Gobernanza de Producción (sniper-singapur — Ley IX):**  
   * Nunca interrumpir vortex-exec.service en medio de una muestra estacionaria activa (N \< 25 cierres en la época vigente), salvo falla crítica patrimonial (P0).  
   * Nunca reiniciar vortex-tap.service en despliegues que conserven VORTEX\_SHM\_VERSION \= 15: usar siempre DRAIN\_MODE.lock (step\_drain\_standby\_latch garantiza espera viva con 0 posiciones abiertas y 0 bucles de reinicio systemd) y compilar en el servidor con prioridad aislada nice \-n 19 ionice \-c 3\.  
   * Para inspección en tiempo real sin impacto sobre 200.0 Hz, usar vortex 360 por SSH o el túnel local ssh \-N \-L 8787:127.0.0.1:8787 sniper-singapur hacia \[http\://127.0.0.1:8787\](http\://127.0.0.1:8787).

Si deseas guardar este documento directamente dentro del repositorio local para que **Antigravity IDE** lo indexe automáticamente en cada sesión, ejecuta este comando único en **PowerShell** (D:\\Desarrollo\\VORTEX-L2):

PowerShell  
& {  
    \[IO.Directory\]::CreateDirectory((Join-Path (Get-Location).Path "docs")) \| Out-Null;  
    Write-Host "Copia el contenido Markdown anterior en docs\\VORTEX\_OMEGA\_V51\_AGENT\_SSOT.md (UTF-8 sin BOM \+ LF) y haz git add docs/VORTEX\_OMEGA\_V51\_AGENT\_SSOT.md." \-ForegroundColor Cyan;  
}  
