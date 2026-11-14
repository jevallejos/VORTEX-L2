# VORTEX-OMEGA V4.1 (`vortex-rs`)

**Motor Institucional Autonomo de Ejecucion HFT Maker-First para Bybit UTA V5**  
**Version Operativa:** `VORTEX-OMEGA V4.1` (`Cargo pkg v1.0.0`) | **Contrato IPC:** `VORTEX_SHM_VERSION = 13` (`/dev/shm/vortex_lob.mmap`)  
**Cadencia del Custodio:** `200.0 Hz` (`5 ms`) | **Perfil Release:** `Rust 2021 Stable` (`lto = "fat"`, `codegen-units = 1`, `panic = "abort"`)  
**Certificacion:** `50/50 Tests OK` | `Clippy -D warnings: 0` | `OOS 520 PF = 6.56 (Drift == 0)` | `FIFO Sim > 1.8 GHz`

---

## 1. Descripcion General y Evolucion a OMEGA V4.1

**VORTEX-OMEGA V4.1** es un sistema autonomo de ejecucion cuantitativa de baja latencia en Rust disenado con **cero asignaciones dinamicas en la ruta critica (`Zero-Alloc`)**, memoria compartida POSIX `tmpfs` libre de bloqueos mediante `SeqLock` (*tear-free*) y pasarelas WebSocket/REST asincronas para operar simultaneamente **12 contratos perpetuos lineales USDT** en **Bybit Unified Trading Account (UTA V5)**.

| Dimension Tecnica | `VORTEX-L3` (Base) | `VORTEX-OMEGA V4.0` | `VORTEX-OMEGA V4.1` (Produccion Actual) |
| :--- | :--- | :--- | :--- |
| **Memoria Compartida** | `SHM v12` | `SHM v13` (`WaveObservables` + liquidez profunda `L1..L5`) | `SHM v13` con coherencia direccional en tiempo real |
| **Dimensionamiento** | Fraccion escalar | Paridad continua de riesgo (`2,400 ppm` = `0.24%` Wallet al `SL`) | Paridad `2,400 ppm` + piso divisible 1-lote + cadencia armonica UTC |
| **Expansion Cola Derecha** | Salida LVDBS simple | *Shadow-Runner* (`Sentinel + Snap-In`), particion aurea `38/62` y *Chandelier SL* | **Desbloqueo Elastico del *Runner*** ante resurgimiento de flujo propio |
| **Escudo en Cola L1** | Moallemi estatico | Sesgo aureo de Stoikov + intensidad auto-excitada O(1) de **Hawkes** | Stoikov + Hawkes + **aborto en cola (`>= 500 ms`) por contradiccion `wave_macro`** |
| **Parada Optima y `SL`** | Compresion tardia | **Wald SPRT v2.0** desde `t >= 3 s` + piso cero-perdida (`+4.25 bps` brutos) | **Desacoplamiento:** trinquete `SL` irreversible vs. piso pasivo reversible |
| **Filtro Macro Elliott** | Sin veto de fase | Observacion pasiva de ondas causales Elliott v2.0 | **Veto duro `passes_elliott_macro_coherence_guard`** en armado, pre-vuelo y `Re-Peg` |
| **Certificacion y Gemelo** | 3 brazos (`A`, `B`, `A+B`) | 4 brazos (`MODEL OMEGA: +482.07 bps`) + Gemelo `25 ms RTT` (`49` tests) | **`50/50` tests** + validacion contrafactual sobre trazas reales de produccion |

---

## 2. Arquitectura Clean-Room (19 Modulos + 3 Binarios LTO Fat)

```text
Bybit UTA V5 Public WS (orderbook.50, publicTrade, tickers, allLiquidation)
        |
        v
+----------------------+     POSIX tmpfs SeqLock      +------------------------------+
|   bin/vortex_tap     | === /dev/shm/vortex_lob.mmap ===> |       bin/vortex_exec        |
| (Ingestor & PAR1)    |        (SHM v13 @ <5ms)      | (200.0 Hz 12-Slot Custodian) |
+----------+-----------+                              +--------------+---------------+
           | Async Ring                                              | Lock-Free Channels
           v                                                         v
   data/*.parquet (PAR1)                              wss://stream.bybit.com/v5/trade
           |                                          + REST V5 Atomic Sync & Vault JSONL
           v
+----------------------+
|  bin/vortex_replay   |  (200k FIFO Sim + 4-Arm Pareto Ablation + 1:1 Digital Twin)
+----------------------+
```

### Estructura de los 19 Modulos en `src/`

| Modulo (`src/*.rs`) | Responsabilidad Tecnica |
| :--- | :--- |
| `types.rs` | *Newtypes* estrictos (`PriceTicks`, `LotQty`, `ScaledBps`, `BoundedScore`, `TimestampNs`), `APPROVED_SPECS` (12 contratos) y `PortfolioBetaGuard`. |
| `lob_shm.rs` | Arena `VortexShmArena` (`VORTEX_SHM_VERSION = 13`), `SeqLock` libre de desgarros, `SymbolLobSnapshot` con profundidad `L1..L5` y `WaveObservables`. |
| `tap_ring.rs` | Anillo asincrono para persistencia columnar `PAR1` sin *jitter*, poda automatica y autoprueba aislada. |
| `forensic_parquet.rs` | Formato `PAR1` (`L1-L4`), libro mayor entero `ParitySyncLedger` en escala 1e8 (`PARITY_SYNC Drift == 0`) y boveda monotonica `70/30`. |
| `wave_elliott.rs` | Clasificador `CausalElliottWaveState` v2.0 (6 pivotes ZigZag causales sin *look-ahead*, Axiomas E1-E3, exponente de Hurst, impulso `OI`, liquidaciones y Fibonacci). |
| `alpha_3factor.rs` | Modelo ortogonal de 3 factores (`OFI`, `TFI`, `GEOM`), micro-precio aureo de Stoikov, beta *Lead-Lag* de Epps con `BTCUSDT`, volatilidad Parkinson `5m` y surf `LVDBS L1-L4`. |
| `kinematic_surface.rs` | Escudo en cola `Moallemi + Stoikov`, `HawkesQueueTracker` O(1), parada continua `Wald SPRT v2.0` y laboratorio de `63/63` cicatrices sobre variedad `2,160`. |
| `bayesian_governor.rs` | `OnlineAdaptiveGovernor` con matriz `12x2` de cicatrices, paridad de riesgo (`2,400 ppm`), estacionalidad armonica UTC de `24 h` y veto pre-*Funding* (`[-75 s, 0 s)`). |
| `exit_quoter.rs` | Cotizador canonico de salida: *Shadow-Runner* (`Sentinel` + *Snap-In* en cresta), particion aurea `350..650 permille`, `ChandelierProfitLockSl` y pisos cuanticos. |
| `execution_fsm.rs` | Maquina determinista `OrderLifecycleState` de 12 slots, guardia V4.1 `passes_elliott_macro_coherence_guard`, desbloqueo elastico del *Runner* V4.1 y plasticidad `learned_mfe_bps`. |
| `ws_protocol.rs` | Autenticacion `HMAC-SHA256`, parsers JSON de baja latencia para Bybit V5 y control de tasas VIP-0. |
| `http_ws_transport.rs` | Transporte TLS/TCP y WebSocket persistente con deteccion de rezago de indexador y reconexion determinista. |
| `bybit_client.rs` | Ejecucion REST/WS Bybit V5, limpieza de `Stop-Loss` residual (`retCode 34040`) y aplanado de emergencia `P0-A` (`Market IOC reduceOnly`). |
| `bybit_gateway.rs` | Pasarela no bloqueante `BybitAsyncGateway` y tunel persistente `wss://stream.bybit.com/v5/trade` (*Atomic Amend*). |
| `exec_gateway_handlers.rs` | Reconciliacion verificada de cierres, *rollback* ante rechazos de `Chandelier SL` y suite certificadora `run_oos_520_multi_regime_suite`. |
| `exec_daemon.rs` | Demonio autonomo a `200.0 Hz` (`5 ms`), invariante `P0-B` (`is_safe_halt_standby`), preempcion de `250 ms` para *Shadow-Runner* y aborto macro en cola. |
| `fifo_sim.rs` | Simulador determinista de prioridad de cola FIFO (`200k` eventos), ablacion economica de 4 brazos y cinta adversarial de 24 episodios. |
| `replay_walkforward.rs` | Gemelo Digital `1:1` (`25 ms RTT`) y motor *Walk-Forward* sobre capturas reales `.parquet` (`PAR1`). |
| `telemetry_hud.rs` | HUD ANSI en vivo (`--live`), rehidratacion forense `.jsonl` y Dashboard Ejecutivo `70/30` (`--trades`). |

---

## 3. Universo Elastico de 12 Contratos (`APPROVED_SPECS`)

Gobernado por `PortfolioBetaGuard` con limite simultaneo de **6 carriles activos** (`max. 3 Bid / 3 Ask`):

| Slot | Contrato | Rol Operativo | Slot | Contrato | Rol Operativo |
| :---: | :--- | :--- | :---: | :--- | :--- |
| `0` | **`BTCUSDT`** | Lider *Lead-Lag* + *Shadow-Runner* 1-lote | `6` | **`ORDIUSDT`** | Multi-tramo aureo (`38/62`) + desbloqueo elastico |
| `1` | **`ETHUSDT`** | Colider institucional de liquidez profunda | `7` | **`PENDLEUSDT`** | Surf de vacio estructural `LVDBS L1-L4` |
| `2` | **`SOLUSDT`** | Alta rotacion de cola y surf impulsivo | `8` | **`WIFUSDT`** | Expansion de cola derecha de alta volatilidad |
| `3` | **`AVAXUSDT`** | Multi-lote divisible con particion por onda | `9` | **`LINKUSDT`** | Continuacion limpia y reversion controlada |
| `4` | **`TAOUSDT`** | Amplia excursion en tendencia fractal | `10` | **`NEARUSDT`** | Captura pasiva en spread estrecho |
| `5` | **`SUIUSDT`** | Alta densidad de impresiones y surf pasivo | `11` | **`LTCUSDT`** | Ancla defensiva de baja correlacion |

---

## 4. Las 9 Leyes Constitucionales y Cambios Clave de OMEGA V4.1

1. **Ley I - Verdad Contable 1e8 y Reserva `70/30`:** Contabilidad entera exacta descontando tarifas VIP-0 (`2.00 bps` *Maker* por pierna; `5.50 bps` *Taker* en `Stop-Loss` de servidor), `PARITY_SYNC Drift == 0` y division `70%` Capital Compuesto / `30%` Reserva Monotonica.
2. **Ley II - Ondas Causales de Elliott v2.0 y Plasticidad MFE:** Proyecciones estructurales Fibonacci (`1.618x`, `2.618x`) sin *look-ahead* y expansion de `learned_mfe_bps` hasta `12x` friccion RT (`48.00 bps`).
3. **Ley III - Gobernanza Bayesiana `12x2`, Paridad `2,400 ppm` y Custodia `P0-B`:** Riesgo fijo de `0.24%` del patrimonio hasta el `Stop-Loss` elastico, filtro pre-*Funding* cada `8 h` UTC y prohibicion absoluta de abandonar posiciones abiertas ante archivos `.lock`.
4. **Ley IV - Transporte No-Bloqueante y Arena `SHM v13`:** Separacion total entre ingesta de red (`vortex_tap`), memoria `SeqLock` en `/dev/shm` y bucle determinista de `200.0 Hz` (`vortex_exec`).
5. **Ley V - Primacia Pasiva, *Shadow-Runner*, Particion Aurea y *Chandelier SL*:**
   - Estaciona ordenes `Limit PostOnly` Centinela durante rafagas favorables y ejecuta *Snap-In* al BBO (`250 ms` de preempcion) al desacelerar la cresta.
   - Particion conjugada aurea `350..650 permille` (`~38.2%` Tramo 1 / `~61.8%` *Runner* en `ImpulseWave3`, sujeto a nocional minimo de `5.25 USDT` por pierna).
   - `ChandelierProfitLockSl` solo se arma sobre ganancia neta garantizada `850 + 2 * tick_bps ScaledBps` (`>= +1.00 bp` neto tras tarifa *Taker*), con geometria anti-`10001` y aplanado de emergencia `P0-A` ante brechas `> 1.5x`.
6. **Ley VI - Escudo `Stoikov + Hawkes` y Parada Continua `Wald SPRT v2.0`:** Aborto temprano en cola `L1` ante barridos informados y contraccion continua del `Stop-Loss` de servidor hasta un `48%` desde `t >= 3 s`.
7. **Ley VII - Universo Elastico de 12 Contratos:** Control de correlacion cruzada mediante `PortfolioBetaGuard`.
8. **Ley VIII - Dominancia Estricta de Pareto:** Validacion obligatoria en 4 brazos (`MODEL A < B < A+B < OMEGA`), Gemelo Digital `25 ms RTT` con `0` rechazos `PostOnly` y `OOS 520` multi-regimen (`PF >= 2.0`).
9. **Ley IX - Higiene Zero-Alloc y Rotacion por Drenaje:** Codificacion `UTF-8 sin BOM + LF`, `Clippy 0 warnings` y despliegue exclusivo con `0` posiciones abiertas.

### Nuevas Ecuaciones Quirurgicas en `VORTEX-OMEGA V4.1`

- **Desbloqueo Elastico del *Runner* (`observe_wald_sprt_step` en `src/execution_fsm.rs`):**
  - La compresion del `Stop-Loss` de servidor permanece **100% irreversible** (trinquete unidireccional preservado).
  - El piso pasivo `defensive_floor_locked` solo se enclava cuando la posicion esta realmente bajo agua (`cur_gross_bps < -200 ScaledBps` con flujo alineado `< 0.15`), evitando bloqueos falsos en pausas neutrales.
  - Si antes de `180 s` renace un impulso genuino a favor (`cur_gross_bps >= +150 ScaledBps`, flujo propio alineado `>= +0.35`, sin climax y `macro_phase * dir > -0.25`), **desbloquea `defensive_floor_locked = false`** permitiendo cosechar el Tramo 1 y dejar correr el *Runner* completo.
- **Veto de Contradiccion Macro-Elliott (`passes_elliott_macro_coherence_guard` en `src/execution_fsm.rs` y `src/exec_daemon.rs`):**
  - Bloquea el armado inicial, la confirmacion de pre-vuelo, el `Re-Peg` y aborta ordenes estacionadas en cola (`>= 500 ms`) cuando `macro_phase * dir <= -0.25`, `H_t >= 0.55` e `oi_impulse * dir < 0.10`, o frente a ondas `ImpulseWave3/5` opuestas con proyeccion Fibonacci `<= -50 bps` y `H_t >= 0.50`.

---

## 5. Catalogo Completo de Comandos CLI y Monitoreo SRE

### Binarios Principales (`target/release/` o `/home/ubuntu/vortex-rs/bin/`)

```bash
# 1. Demonio Ingestor Bybit V5 -> SHM v13 + Archivos PAR1
./bin/vortex_tap                              # Autoprueba aislada (12 slots + anillo PAR1 + CRC)
./bin/vortex_tap --daemon                     # Ejecucion continua en produccion

# 2. Simulador FIFO, Ablacion Cuadruple y Gemelo Digital 1:1
./bin/vortex_replay                           # 200k FIFO + 4-Arm Pareto + Twin 25ms + Walk-Forward ./data
./bin/vortex_replay --dir /ruta/archivos_par1 # Walk-Forward sobre directorio PAR1 especifico

# 3. Custodio Autonomo a 200.0 Hz, Compuerta OOS 520 y Tableros Read-Only
./bin/vortex_exec                             # Certificador OOS 520 Multi-Regimen (Drift == 0)
./bin/vortex_exec --live                      # Vista instantanea Read-Only del HUD sobre SHM v13
./bin/vortex_exec --trades                    # Ledger completo + Dashboard Ejecutivo 70/30
./bin/vortex_exec --daemon --live-capital     # Custodio autonomo 200.0 Hz con capital real Bybit V5
```

### Comandos y Alias Seguros en `sniper-singapur` (`SIGINT-Safe`)

```bash
vortex 360   # Auditoria integral de 6 paneles (HUD, WS V5, Dashboard 70/30, PAR1 Replay, Vault y Scars)
vhud         # Vista instantanea tear-free de los 12 slots (/home/ubuntu/vortex-rs/bin/vortex_exec --live)
vtrades      # Historial consolidado de tiros y metricas 70/30 (/home/ubuntu/vortex-rs/bin/vortex_exec --trades)
vlogs        # Ultimas 30 lineas operativas del demonio (sudo journalctl -u vortex-exec.service -n 30 --no-pager)
vwatch       # Monitor continuo a 1 Hz con aislamiento de Ctrl+C (nunca interrumpe al demonio)
```

---

## 6. Compilacion y Certificacion Local (Windows 11 & Linux)

### En Windows 11 (PowerShell - Antigravity IDE)

```powershell
$ErrorActionPreference = "Stop"
Remove-Item -Path ".\target\debug\deps\vortex_rs-*.exe", ".\target\debug\deps\vortex_rs-*.pdb" -Force -ErrorAction SilentlyContinue
$env:CARGO_INCREMENTAL = "0"

cargo clippy --all-targets -- -D warnings
cargo test --lib -- --test-threads=1
cargo build --release --bins

.\target\release\vortex_replay.exe
.\target\release\vortex_exec.exe
```

### En Linux (`Ubuntu x86_64`)

```bash
cargo clippy --all-targets -- -D warnings
cargo test --lib --bins -- --test-threads=1
nice -n 19 ionice -c 3 cargo build --release --bins
./target/release/vortex_replay
./target/release/vortex_exec
```

---

## 7. Operacion SRE en Produccion (`sniper-singapur`), Cerrojos y Rollback

- **Ruta raiz en produccion:** `/home/ubuntu/vortex-rs` (binarios activos servidos desde `/home/ubuntu/vortex-rs/bin/`).
- **Unidades `systemd`:** `vortex-tap.service`, `vortex-exec.service`, `vortex-doctor.timer` y `vortex-maint.timer`.
- **Boveda forense (`data/vault/`):**
  - `vortex_ai_forensic_vault.jsonl` - Trazabilidad completa de cada operacion cerrada.
  - `vortex_governor_scar_memory.jsonl` - Instantaneas evolutivas de la matriz bayesiana `12x2`.
  - `vortex_red_loss_box.jsonl` - Registro inmutable de eventos `Stop-Loss`.
  - `vortex_open_positions_state.json` - Posiciones vivas bajo custodia (`[]` = `2 bytes` en exposicion cero).
  - `DRAIN_MODE.lock` / `HALTED_ON_LOSS.lock` - Cerrojos de drenaje ordenado y pausa post-perdida bajo invariante `P0-B`.

### Actualizacion en Caliente por Drenaje Verificado (`OMEGA V4.1`)

```bash
cat << 'EOF' > /tmp/upgrade_omega_v41.sh
#!/usr/bin/env bash
set -uo pipefail

VORTEX_DIR="/home/ubuntu/vortex-rs"
cd "$VORTEX_DIR" || exit 1

touch data/vault/DRAIN_MODE.lock
while [ "$(wc -c < data/vault/vortex_open_positions_state.json 2>/dev/null || echo 2)" -gt 4 ]; do
  echo "Esperando cierre natural de posicion abierta..."
  sleep 2
done

sudo systemctl stop vortex-exec.service vortex-tap.service
sleep 1

SNAP_DIR="$VORTEX_DIR/backups/pre_omega_v41_$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$SNAP_DIR/bin" "$SNAP_DIR/vault"
cp -a "$VORTEX_DIR/bin/." "$SNAP_DIR/bin/"
cp -a "$VORTEX_DIR/data/vault/." "$SNAP_DIR/vault/"

git fetch origin main || exit 1
git reset --hard origin/main || exit 1
source "$HOME/.cargo/env" 2>/dev/null || true
nice -n 19 ionice -c 3 cargo build --release --bins || exit 1
./target/release/vortex_exec || exit 1

install -m 0755 target/release/vortex_tap "$VORTEX_DIR/bin/vortex_tap" || exit 1
install -m 0755 target/release/vortex_replay "$VORTEX_DIR/bin/vortex_replay" || exit 1
install -m 0755 target/release/vortex_exec "$VORTEX_DIR/bin/vortex_exec" || exit 1

rm -f /dev/shm/vortex_lob.mmap data/vault/DRAIN_MODE.lock data/vault/HALTED_ON_LOSS.lock
sudo systemctl start vortex-tap.service && sleep 3 && sudo systemctl start vortex-exec.service
sleep 2
sudo journalctl -u vortex-exec.service -n 12 --no-pager
EOF

chmod +x /tmp/upgrade_omega_v41.sh
bash /tmp/upgrade_omega_v41.sh
```

### Rollback Determinista (`< 15 segundos`)

```bash
SNAP_DIR=$(ls -dt /home/ubuntu/vortex-rs/backups/pre_* | head -n 1)
sudo systemctl stop vortex-exec.service vortex-tap.service
rm -f /dev/shm/vortex_lob.mmap
cp -pf "$SNAP_DIR/bin/"* /home/ubuntu/vortex-rs/bin/
rm -f /home/ubuntu/vortex-rs/data/vault/DRAIN_MODE.lock
sudo systemctl start vortex-tap.service && sleep 2 && sudo systemctl start vortex-exec.service
```

---

## 8. Documentacion Normativa

- [`CONSTITUTION_VORTEX_OMEGA.md`](./CONSTITUTION_VORTEX_OMEGA.md) - Las **9 Leyes Evolutivas** y la **Adenda Quirurgica OMEGA V4.1**.
- [`.github/workflows/ci.yml`](./.github/workflows/ci.yml) - Compuerta CI en `ubuntu-latest` (auditoria `LF`/secretos, `Clippy -D warnings`, `50` tests en `/dev/shm` y artefactos `SHA-256`).
