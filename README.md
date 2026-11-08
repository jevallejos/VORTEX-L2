# VORTEX-L3 — Autonomous 12-Slot Peristaltic & Deep-Book HFT Engine (`VORTEX-RS V3.0`)

**VORTEX-L3** es un motor institucional de creacion de mercado direccional (*Directional Market Making*) y ejecucion de ultra-baja latencia escrito 100% en **Rust** (`edition = "2021"`, `lto = "fat"`, `codegen-units = 1`, `panic = "abort"`, cero asignaciones dinamicas en la ruta critica) para futuros perpetuos lineales USDT en **Bybit Unified Trading Account (UTA V5)** sobre `sniper-singapur` (AWS `ap-southeast-1`, Ubuntu `x86_64`).

---

## 1. Arquitectura de 3 Demonios y Memoria Compartida (`SHM v12` @ `200.0 Hz`)

| Binario / Servicio | Prioridad OS | Funcion Principal |
| :--- | :--- | :--- |
| **`vortex_tap`** (`vortex-tap.service`) | `Nice=-10` | Ingesta WebSocket inmortal (`orderbook.50` + `publicTrade`) para los **12 contratos aprobados**, calculo en tiempo real de los 3 factores ortogonales (`OFI`, `TFI`, `GEOM`), escritura *lock-free Seqlock* en `/dev/shm/vortex_lob.mmap` (`SHM v12`) y grabacion en anillo de `4,096` archivos `.parquet` (`PAR1` con suma `FNV1a64`). |
| **`vortex_exec`** (`vortex-exec.service`) | `Nice=-15` | Orquestador asincrono a **`200.0 Hz` (`5 ms`)** con **12 motores autonomos `[SlotExecutionEngine; 12]`**, Gobernador Adaptativo `12x2`, **Bombeo Peristaltico (`ZMPQR`)**, **Surf de Vacio L2/L3 (`LVDBS`)**, Fondo de Saneamiento Cruzado (`70%` Blindado / `30%` Reserva) y puente `HMAC-SHA256` sobre Bybit V5. |
| **`vortex_replay`** (`CLI / CI Gate`) | Bajo Demanda | Gemelo digital determinista de cola FIFO L1 (`> 40M–150M eventos/s`, `25 ms` de latencia de vuelo `PostOnly`), Walk-Forward Replay sobre el anillo `.parquet` y **Banco de Ablacion Economica `A / B / A+B`**. |

---

## 2. Universo Institucional de 12 Pares Aprobados (`Tick_bps <= 1.50 bps`)

| Slot | Simbolo | `SymbolId` | `tick_size` | `lot_step` | Division de Lote (50% / 25%) |
| :---: | :--- | :---: | :---: | :---: | :--- |
| `0` | **`BTCUSDT`** | `1` | `0.10 USDT` | `0.001 BTC` | Lider Macroeconomico (`Lead-Lag`) y Carril Direccional |
| `1` | **`ETHUSDT`** | `2` | `0.01 USDT` | `0.01 ETH` | Lote par garantizado (`Tranche 1 50%` + `LVDBS 50%`) |
| `2` | **`SOLUSDT`** | `3` | `0.01 USDT` | `0.1 SOL` | Lote par/cuadruple (`ZMPQR 25%` + `LVDBS 50%`) |
| `3` | **`AVAXUSDT`** | `4` | `0.001 USDT` | `0.1 AVAX` | Lote par/cuadruple (`ZMPQR 25%` + `LVDBS 50%`) |
| `4` | **`TAOUSDT`** | `5` | `0.01 USDT` | `0.01 TAO` | Lote par/cuadruple (`ZMPQR 25%` + `LVDBS 50%`) |
| `5` | **`SUIUSDT`** | `6` | `0.0001 USDT` | `1 SUI` | Lote par/cuadruple (`ZMPQR 25%` + `LVDBS 50%`) |
| `6` | **`ORDIUSDT`** | `7` | `0.001 USDT` | `0.1 ORDI` | Lote par/cuadruple (`ZMPQR 25%` + `LVDBS 50%`) |
| `7` | **`PENDLEUSDT`** | `8` | `0.0001 USDT` | `1 PENDLE` | Lote par/cuadruple (`ZMPQR 25%` + `LVDBS 50%`) |
| `8` | **`WIFUSDT`** | `9` | `0.0001 USDT` | `1 WIF` | Lote par/cuadruple (`ZMPQR 25%` + `LVDBS 50%`) |
| `9` | **`LINKUSDT`** | `10` | `0.001 USDT` | `0.1 LINK` | Lote par/cuadruple (`ZMPQR 25%` + `LVDBS 50%`) |
| `10` | **`NEARUSDT`** | `11` | `0.001 USDT` | `0.1 NEAR` | Lote par/cuadruple (`ZMPQR 25%` + `LVDBS 50%`) |
| `11` | **`LTCUSDT`** | `12` | `0.01 USDT` | `0.1 LTC` | Lote par/cuadruple (`ZMPQR 25%` + `LVDBS 50%`) |

*Contratos vetados matematicamente por exceso de friccion (`Tick_bps > 1.50 bps`): `XRPUSDT` (`1.81 bps`), `ZEREBROUSDT` (`3.36 bps`) y `WALUSDT` (`3.36 bps`).*

---

## 3. Las 3 Innovaciones Microestructurales de `VORTEX-L3`

1. **Bombeo Peristaltico de Cola en Zona Roja (`ZMPQR`):**
   * **Espiracion (`25%` en Cresta de Rebote):** Cuando una posicion estancada recupera al menos el `35%` de su MAE y el flujo en cinta se agota (`detects_rebound_crest_exhaustion`), descarga el `25%` del lote como *Maker* respaldada por el Sifon del `30%`.
   * **Inspiracion (`25%` en Valle con Senal `AUTH_MKR`):** Cuando el precio retrocede hacia el piso del rango con descuento elastico y soporte institucional L2 (`authorizes_peristaltic_valley_inhale`), recarga ese `25%` sin superar jamas el lote inicial (`Cero Martingala`), comprimiendo el precio promedio (`avg_price_ticks`) hacia el precio actual para evacuar `6.1x` mas rapido.
2. **Surf de Vacio de Liquidez L2/L3 en Zona Verde (`LVDBS`):**
   * **Tramo 1 (`50%` en Piso Cuantico `440 ScaledBps`):** Asegura la primera mitad del lote en **Verde Oscuro (`🌲 PARTIAL`)** cubriendo comisiones y sumando ganancia al `70%` Blindado y `30%` Reserva.
   * **Tramo 2 (`50%` en Niveles `L2..L4`):** Mide el Indice de Vacio $\mathcal{V}_t$ en los niveles 1 al 4 del libro L2; cuando el libro opositor esta delgado ($\mathcal{V}_t \ge 1.05\,\nu_\sigma$), cotiza en los niveles 2, 3 o 4 capturando de `+8.50 a +22.00 bps` brutos en **Verde Claro (`🟢 WIN`)** y reduciendo el *Fee Drag* a la mitad (`32.3%`).
3. **Gobernador Adaptativo `12x2`, Superficie Cinematica ($E_{\text{kin}} \ge E_{\min}$) y Contraccion de Wald (`48%`):**
   * Certificacion `100.0%` en **`LOSS-LAB` (`62 / 62` perdidas historicas/V2/PAR1 erradicadas y `2,160 / 2,160` en la variedad combinatoria)**.

---

## 4. Paleta Semantica de Colores en Terminal (`256-Color ANSI`)

* **🟢 `WIN` (Verde Claro Brillante `\x1b[1;38;5;118m`):** Cierre ganador completo (`net_bps >= +1.00 bps`).
* **🌲 `PARTIAL` (Verde Oscuro Esmeralda `\x1b[1;38;5;28m`):** Cosecha parcial del primer `50%` (`Tranche1Harvest50`).
* **🩵 `SCRATCH` (Celeste Brillante `\x1b[1;38;5;51m`):** Escape defensivo en piso cuantico (`-0.50 <= net_bps < +1.00 bps`).
* **🟡 `ARMED / FILL` (Amarillo `\x1b[1;33m`):** Orden `PostOnly` en cola L1 o posicion activa.
* **🟠 `SPOOF_VETO / ABORT` (Naranja `\x1b[1;38;5;208m`):** Bloqueo anti-spoofing o aborto preventivo $\frac{dQ}{dt}$ de Moallemi-Yuan.
* **🔴 `LOSS / VETO` (Rojo `\x1b[1;31m`):** Perdida neta (`< -0.50 bps`, aislada en `RED_LOSS_BOX`) o contrato vetado.

---

## 5. Guia Completa de Comandos Operativos (`/usr/local/bin/vortex`)

| Comando en `sniper-singapur` | Descripcion |
| :--- | :--- |
| **`vortex 360`** | Panel maestro de 360 grados: 1. HUD `SHM v12`, 2. Telemetria Bybit V5, 3. Historial coloreado + Dashboard Ejecutivo (`Meta 95 USDT`), 4. Simulador FIFO + Tabla `A/B/A+B`, 5. Baul Forense IA y 6. Caja Roja. |
| **`vortex live`** *(o `vortex -l`)* | Muestra en tiempo real el `VORTEX-L3 LIVE HUD` sobre `/dev/shm/vortex_lob.mmap` (`SHM v12`) con los 12 contratos. |
| **`vortex trades`** *(o `vortex -t`)* | Muestra todas las operaciones cerradas (Verde Claro / Verde Oscuro / Celeste) y el Dashboard Ejecutivo con `Saldo Billetera`, `Meta 95 USDT` y Fondo `30% / 70%`. |
| **`vortex drain`** *(o `vortex drenar`)* | Activa `data/vault/DRAIN_MODE.lock` en `< 5 ms`: no abre nuevas posiciones, cobra en verde las posiciones abiertas hasta llegar a `0` (`Wallet == Equity`) y apaga limpiamente el ejecutor. |
| **`vortex normal`** *(o `vortex undrain`)* | Elimina `DRAIN_MODE.lock` y `HALTED_ON_LOSS.lock`, y reactiva los `12/12` motores autonomos en vivo. |
| **`vortex doctor`** | Ejecuta `vortex-doctor`: audita y repara reloj `tsc`, sincronizacion atomica `chrony` (`< 1 ms`), pila TCP `BBR + fq + tcp_slow_start_after_idle=0` y prioridades `Nice=-10/-15`. |
| **`vortex maint`** | Ejecuta `vortex-maintenance.sh` (`IOSchedulingClass=idle`): sella respaldos `.bak` en `~/vortex-rs/backups/`, mantiene el anillo en `4,096` archivos `.parquet` y aspira `journald`. |
| **`vortex baul`** | Muestra los ultimos 30 registros completos de 38 variables del Baul Forense (`data/vault/vortex_ai_forensic_vault.jsonl`). |
| **`vortex redbox`** | Muestra el contenido integro de la Caja Roja de Perdidas (`data/vault/vortex_red_loss_box.jsonl`). |
| **`vortex follow`** *(o `vortex -f`)* | Muestra el HUD + Trades y abre el flujo en vivo (`journalctl -f`). |
| **`vortex replay`** *(o `vortex -r`)* | Corre el benchmark FIFO (`200,000` eventos), el laboratorio `A / B / A+B` y el Walk-Forward Replay sobre los archivos `.parquet`. |
| **`vortex all`** | Ejecuta en serie las 4 compuertas de certificacion (`vortex_tap` + `vortex_replay` + `vortex_exec` + `vortex-doctor`). |

---

## 6. Estructura de Carpetas y Bovedas Forenses en Produccion

* `/home/ubuntu/vortex-rs/bin/` — Binarios LTO certificados por `COMMIT_SHA` (`vortex_tap`, `vortex_replay`, `vortex_exec`).
* `/home/ubuntu/vortex-rs/data/` — Anillo activo de `4,096` segmentos `.parquet` (`PAR1`) y `vortex_shadow_trades.jsonl`.
* `/home/ubuntu/vortex-rs/data/vault/` — Carpeta aislada del Baul Forense:
  * `vortex_ai_forensic_vault.jsonl` (38 variables microestructurales por cierre).
  * `vortex_red_loss_box.jsonl` (aislamiento de cualquier cierre `< -0.50 bps`).
  * `vortex_open_positions_state.json` (persistencia de tramos `50%` y ciclos peristalticos `25%`).
* `/home/ubuntu/vortex-rs/backups/` — Respaldos automaticos `.bak` generados cada 6 horas por `vortex-maint.timer`.
