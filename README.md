# VORTEX-L3 - Autonomous 12-Slot Seqlock LOB & Deep-Book HFT Core (`vortex-rs v1.0.0`)

**VORTEX-L3** es un motor institucional de creacion de mercado direccional (*Directional
Market Making*) y ejecucion pasiva `Limit PostOnly` escrito 100% en **Rust** (`edition = "2021"`,
`lto = "fat"`, `codegen-units = 1`, `panic = "abort"`, cero `.unwrap()`/`.expect()`/`panic!`)
para futuros perpetuos lineales USDT en **Bybit Unified Trading Account (UTA V5, One-Way)**.

## 1. Arquitectura de 3 Binarios y Memoria Compartida (`SHM v12` @ `200.0 Hz`)

| Binario / Servicio | Prioridad OS | Funcion Institucional |
| :--- | :--- | :--- |
| **`vortex_tap`** (`vortex-tap.service`) | `Nice=-10` | Ingesta WebSocket inmortal (`orderbook.50` + `publicTrade` + `tickers`), modelo ortogonal de 3 factores (`OFI`, `TFI` preservado en snapshots, `GEOM`), escritura *Seqlock* en `/dev/shm/vortex_lob.mmap` (`SHM v12`) y grabador asincrono `PAR1` (`L1..L4` + `Vol5m`, anillo auto-podado de `4,096` segmentos con `FNV1a64`). |
| **`vortex_exec`** (`vortex-exec.service`) | `Nice=-15` | Orquestador a **`200.0 Hz` (`5 ms`)** con `[SlotExecutionEngine; 12]`, Gateway asincrono `HTTP/1.1 Keep-Alive TLS` + WebSocket privado Bybit V5 (cero `curl` en ruta critica), cotizador canonico SSOT `LVDBS L1..L4`, `Beta Cap` (`3 Bid / 3 Ask / Max 6`), SL fisico `110-145 bps` verificado al milisegundo cero y reconciliacion `/v5/position/closed-pnl`. |
| **`vortex_replay`** (`CLI / CI Gate`) | Bajo Demanda | Gemelo digital determinista de cola FIFO `L1..L4` (`25 ms` de latencia de vuelo `PostOnly`, drenaje `25%` L2 / `100%` `publicTrade`), *Walk-Forward PAR1 Replay* continuo con `Profit Factor` honesto y banco de ablacion `A / B / A+B`. |

## 2. Universo Institucional de 12 Contratos Aprobados (`Tick_bps <= 1.50 bps`)

| Slot | Simbolo | `SymbolId` | `tick_size` | `lot_step` | Regimen de Lote y Ejecucion |
| :---: | :--- | :---: | :---: | :---: | :--- |
| `0` | **`BTCUSDT`** | `1` | `0.10 USDT` | `0.001 BTC` | Lider `Lead-Lag` + Guardia Lote Sobredimensionado (`SNR >= 0.72`) + `LVDBS` |
| `1` | **`ETHUSDT`** | `2` | `0.01 USDT` | `0.01 ETH` | `0.01 ETH` (`1` lote) habilitado + Multi-Tramo `50%/50%` con `>= 2` lotes |
| `2` | **`SOLUSDT`** | `3` | `0.01 USDT` | `0.1 SOL` | Lote par compuesto (`Tranche 1 50%` + `LVDBS L2..L4 50%`) |
| `3` | **`AVAXUSDT`** | `4` | `0.001 USDT` | `0.1 AVAX` | Lote par compuesto (`Tranche 1 50%` + `LVDBS L2..L4 50%`) |
| `4` | **`TAOUSDT`** | `5` | `0.01 USDT` | `0.01 TAO` | Lote par compuesto (`Tranche 1 50%` + `LVDBS L2..L4 50%`) |
| `5` | **`SUIUSDT`** | `6` | `0.0001 USDT` | `1 SUI` | Lote par compuesto (`Tranche 1 50%` + `LVDBS L2..L4 50%`) |
| `6` | **`ORDIUSDT`** | `7` | `0.001 USDT` | `0.1 ORDI` | Lote par compuesto (`Tranche 1 50%` + `LVDBS L2..L4 50%`) |
| `7` | **`PENDLEUSDT`** | `8` | `0.0001 USDT` | `1 PENDLE` | Lote par compuesto (`Tranche 1 50%` + `LVDBS L2..L4 50%`) |
| `8` | **`WIFUSDT`** | `9` | `0.0001 USDT` | `1 WIF` | Lote par compuesto (`Tranche 1 50%` + `LVDBS L2..L4 50%`) |
| `9` | **`LINKUSDT`** | `10` | `0.001 USDT` | `0.1 LINK` | Lote par compuesto (`Tranche 1 50%` + `LVDBS L2..L4 50%`) |
| `10` | **`NEARUSDT`** | `11` | `0.001 USDT` | `0.1 NEAR` | Lote par compuesto (`Tranche 1 50%` + `LVDBS L2..L4 50%`) |
| `11` | **`LTCUSDT`** | `12` | `0.01 USDT` | `0.1 LTC` | Lote par compuesto (`Tranche 1 50%` + `LVDBS L2..L4 50%`) |

*Contratos vetados por la compuerta continua Volatilidad-Tick-Toxicidad: `XRPUSDT`,
`ZEREBROUSDT` y `WALUSDT`.*

## 3. Pilares Fisico-Matematicos y Contables (SSOT)

1. **Modelo Ortogonal de 3 Factores y Surf de Vacio `LVDBS` (`L1..L4`):**
   * `OFI` emparejado por `PriceTicks` exacto (`L1..L5`) con decaimiento continuo y priors
     normalizados a la escala de lote de cada contrato; `TFI` sobre cinta ejecutada `publicTrade`
     y `GEOM` sobre `L1..L10` penalizado por `SPR_T`.
   * Cuando el indice espacial de vacio $\mathcal{V}_t$ detecta adelgazamiento o huecos de precio
     en `L2..L4`, cotiza salidas `Limit PostOnly reduceOnly` profundas sin estrangular `BTCUSDT`
     ni el Tramo 2 en `+0.40 bps` netos.
2. **Gobernador Adaptativo `12x2`, Superficie Cinematica y `LOSS-LAB` (`62/62` + `2,160/2,160`):**
   * Superficie de Energia Libre Cinematica ($I_{\text{lead}} = s_{\text{ext}} - s_{\text{drift}}$),
     ancla real `wave_anchor_px_1e4` (`VETO_WAVE_STAIRCASE_25BPS`), escudo $\frac{dQ}{dt}$ de
     Moallemi-Yuan y neutralidad `PortfolioBetaGuard` ($N_{\text{Bid}} \le 3$, $N_{\text{Ask}} \le 3$,
     $N_{\text{Total}} \le 6$).
3. **Paridad 1:1 con Billetera Real Bybit (`WalletBalance`) y Boveda Monotona `70% / 30%`:**
   * Entrada anclada al `avgPrice` conservador de Bybit, Stop-Loss `MarkPrice` en `110-145 bps`
     (`retCode == 0 || 34040`) con colchon `>= 2 ticks` en `LIVE ADOPT`, reconciliacion contra
     `/v5/position/closed-pnl` y consolidacion exacta por nocional de lote (`1 Entrada = 1 Trade`,
     `70%` Boveda Blindada estrictamente creciente $\Delta B_{70\%} \ge 0$).

## 4. Arquitectura de Bovedas Forenses (`data/vault/`)

* `data/vault/vortex_ai_forensic_vault.jsonl`: Unica Fuente de Verdad (SSOT) con `55` variables
  microestructurales y contables por operacion cerrada.
* `data/vault/vortex_red_loss_box.jsonl`: Caja Roja perpetua e inmutable (`net_bps < -0.50 bps`).
* `data/vault/vortex_governor_scar_memory.jsonl`: Memoria perpetua de cicatrices `12x2`.
* `data/vault/vortex_open_positions_state.json`: Custodia en caliente de posiciones abiertas.
* `data/vault/DRAIN_MODE.lock` y `data/vault/HALTED_ON_LOSS.lock`: Candados fisicos de operacion.
* `data/vortex_l2_segment_*.parquet`: Anillo rotativo auto-podado de `4,096` segmentos `PAR1`.

## 5. Comandos Operativos (`/usr/local/bin/vortex`)

| Comando | Descripcion |
| :--- | :--- |
| `vortex 360` | Panel integral: HUD `SHM v12` Read-Only, ordenes, Dashboard, Replay y Bovedas. |
| `vortex live` | Monitor en tiempo real sin desgarros (`read_consistent`) sobre `SHM v12`. |
| `vortex trades` | Historial consolidado y Dashboard Ejecutivo (`Wallet`, `Meta 95 USDT`, `70/30`). |
| `vortex drain` | Activa `DRAIN_MODE.lock`: cosecha en verde las abiertas hasta `0` y detiene. |
| `vortex normal` | Verifica ausencia de `HALTED_ON_LOSS.lock`, retira `DRAIN_MODE.lock` y reanuda. |
| `vortex baul` / `scars` / `redbox` | Inspeccion directa del Baul IA, Memoria de Cicatrices y Caja Roja. |
| `vortex replay` / `oos` / `audit` / `all` | Ejecuta el gemelo FIFO `PAR1`, la compuerta `OOS 520` y las 4 compuertas. |
