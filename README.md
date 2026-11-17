# VORTEX-OMEGA V4.8 — Autonomous 200Hz Maker-First HFT Engine (Bybit UTA V5)

Motor institucional cuantitativo de alta frecuencia en Rust (`Zero-Alloc`, `LTO Fat`, `panic = "abort"`) para ejecucion pasiva `Limit PostOnly` sobre 12 contratos perpetuos lineales USDT en Bybit Unified Trading Account (UTA V5).

## 1. Resumen Ejecutivo e Identidad Operativa V4.8

| Parametro | Especificacion Certificada V4.8 |
| :--- | :--- |
| **Version del Motor** | `VORTEX-OMEGA V4.8` (`vortex-rs v1.0.0`) |
| **Contrato IPC Memoria Compartida** | `VORTEX_SHM_VERSION = 15` (`/dev/shm/vortex_lob.mmap`, `25 KB`, `#[repr(C, align(64))]`) |
| **Formato Columnar Forense** | `PAR2` sin perdida (`256 B/fila`, `L1..L5` + `TickerMeta` + `LiquidationPrint` + `FNV-1a 64-bit`) |
| **Cadencia del Demonio Custodio** | `200.0 Hz` (`5 ms`) `Zero-Jitter` (`0` I/O sincrono en ruta caliente) + WS `wss://stream.bybit.com/v5/trade` |
| **Arquitectura Modular** | `24` submodulos `Zero-Alloc` en `src/` + `3` binarios `LTO Fat` (`vortex_tap`, `vortex_replay`, `vortex_exec`) |
| **Suite y Compuertas** | `55/55` tests en `/dev/shm` (`100%`), `Clippy pedantic = 0 warnings`, `OOS 520` (`Maker = 85.00%`, `PF = 7.05`, `Drift == 0`) |

## 2. Escudos Institucionales V4.8, Islas Auto-Aprendidas y Semaforo 4/4

- **Escudo Diario Elastico UTC (`DailySessionGovernor`):** Tope diario acoplado a la Reserva del `30%` (`[-3,883..-6,283 ppm]`), meta diaria auto-aprendida con candado convexo *High-Water Mark*, amortiguador Kelly `C^1` (`[618..=1000 permille]`) y despertar autonomo en `00:00:00 UTC`.
- **Compuerta Cuantica de Crecimiento Geometrico (`passes_quantum_ev_growth_gate`):** Bloquea de forma endogena lotes indivisibles sobredimensionados con deriva geometrica negativa (silenciando `BTCUSDT` a capital bajo y reactivandolo automaticamente al escalar capital a `126-145+ USDT` o *rebates* VIP).
- **Evacuacion Post-Fill `Glosten-Milgrom + Wald SPRT v2.0`:** Evalua `zeta_tox` sobre `SlotMarkoutRing` (`+250ms, +1s, +5s`); ante barridos con `MFE <= 1 tick` e inversion de flujo, activa escape Maker cero-perdida y contrae el Stop-Loss servidor con Hermite `C^1` entre `64%` y `83%`.
- **Desacople Celular vs. Catastrofico, Funding 8h y Semaforo `4/4`:** Aisla Stop-Loss normales en su celda `12x2` (reservando `HALTED_ON_LOSS.lock` a brechas `>= 14,400 ppm`), persiste liquidaciones `8h` (`FUNDING_SETTLEMENT` con `FNV-1a`) e integra en `vortex 360` el cuadro `CAPITAL SCALING SEMAPHORE (4/4)` hacia el escalon de `155-165 USDT`.

## 3. Catalogo Completo de Comandos Operativos (Actuales V4.8 y Heredados)

### 3.1. Atajos Heredados y Alias Rapidos en Consola (`SIGINT-Safe`, `0 ns` Impacto)

Comandos disponibles en `/usr/local/bin/vortex` y `~/.bashrc` sobre `sniper-singapur` (presionar `Ctrl+C` cierra solo el visor sin afectar a los demonios):

- `vortex 360`: Auditoria integral en cascada de 6 paneles (HUD en vivo `SHM v15`, trazas Bybit V5, Ledger + Dashboard `70/30` + Semaforo `4/4`, *Walk-Forward* `PAR2`, Baul Forense JSONL y Memoria de Cicatrices).
- `vortex -f`: Conecta la terminal al flujo continuo en tiempo real (`journalctl -f`) del demonio de ejecucion (`Ctrl+C` seguro).
- `vhud`: Instantanea libre de desgarros de los 12 carriles sobre `/dev/shm/vortex_lob.mmap` (`vortex_exec --live`).
- `vtrades`: Rehidrata la boveda JSONL, concilia saldo vivo en Bybit y despliega el Ledger, Escudo Diario y Semaforo `4/4` (`vortex_exec --trades`).
- `vlogs`: Muestra las ultimas 30 trazas operativas de `vortex-exec.service` en `journald`.
- `vwatch`: Refresca el HUD en vivo a `1 Hz` atrapando `INT/TERM` localmente.

Inspeccion de la configuracion instalada de atajos y alias:

    cat /usr/local/bin/vortex
    grep -E "vortex|vhud|vtrades|vlogs|vwatch" ~/.bashrc

### 3.2. Invocacion Directa de Binarios y Monitoreo `systemd` / `journalctl`

Consulta en solo lectura de binarios nativos y estado de servicios en Ubuntu:

    /home/ubuntu/vortex-rs/bin/vortex_exec --live
    /home/ubuntu/vortex-rs/bin/vortex_exec --trades
    systemctl is-active vortex-tap.service vortex-exec.service
    systemctl status vortex-tap.service vortex-exec.service --no-pager
    sudo journalctl -u vortex-exec.service -f
    sudo journalctl -u vortex-tap.service -f
    sudo journalctl -u vortex-exec.service -n 30 --no-pager
    sudo journalctl -u vortex-tap.service -n 15 --no-pager
    cat /home/ubuntu/vortex-rs/data/vault/vortex_daily_governor.json ; echo
    cat /home/ubuntu/vortex-rs/data/vault/vortex_open_positions_state.json ; echo

### 3.3. Control Operativo, Drenaje Seguro (`P0-B`) y Emergencia

1. **Activar Drenaje Limpio (`P0-B` — bloquea nuevas entradas y cierra en Maker posiciones vivas a `200.0 Hz`):**

    touch /home/ubuntu/vortex-rs/data/vault/DRAIN_MODE.lock

2. **Detencion Total (ejecutar solo tras verificar `Open: 0/6` en `vhud`):**

    sudo systemctl stop vortex-exec.service vortex-tap.service

3. **Reanudacion Limpia con Precalentamiento HTTP (`3x Retry`):**

    rm -f /home/ubuntu/vortex-rs/data/vault/DRAIN_MODE.lock /home/ubuntu/vortex-rs/data/vault/HALTED_ON_LOSS.lock
    python3 /home/ubuntu/vortex-rs/deploy/prewarm_emas_bybit.py
    sudo systemctl start vortex-tap.service && sleep 5 && sudo systemctl start vortex-exec.service

4. **Reversion Instantanea (*Rollback* en `< 15 s` al ultimo respaldo certificado, solo ante contingencia):**

    sudo systemctl stop vortex-exec.service vortex-tap.service && rm -f /dev/shm/vortex_lob.mmap
    cp -pf /home/ubuntu/vortex-rs/backups/pre_omega_v48_<TIMESTAMP>/bin/* /home/ubuntu/vortex-rs/bin/
    rm -f /home/ubuntu/vortex-rs/data/vault/DRAIN_MODE.lock
    sudo systemctl start vortex-tap.service && sleep 5 && sudo systemctl start vortex-exec.service

### 3.4. Validacion Local y Publicacion en Windows (`PowerShell`)

    cargo clippy --all-targets -- -D warnings
    cargo test --lib -- --test-threads=1
    cargo build --release --bins
    git -c http.version=HTTP/1.1 -c http.postBuffer=524288000 push origin main

## 4. Ejecucion e Interpretacion de los 4 Laboratorios Autonomos (`LAB`)

Todos los laboratorios son **100% de solo lectura** y pueden correrse en paralelo sin interferir con el demonio en vivo:

1. **LAB 1 — *Walk-Forward* Real `PAR2` (`4,096` archivos) + Ablacion Pareto 4 Brazos + Gemelo `1:1`:**

    cd /home/ubuntu/vortex-rs && ./bin/vortex_replay

2. **LAB 2 — Benchmark Sintetico Rapido (`200k FIFO` + `MODEL A < B < A+B < OMEGA` + Gemelo `1:1`, sin leer `data/`):**

    mkdir -p /tmp/vortex_empty_par2_gate && /home/ubuntu/vortex-rs/bin/vortex_replay --dir /tmp/vortex_empty_par2_gate && rmdir /tmp/vortex_empty_par2_gate

3. **LAB 3 — Verificador Multi-Regimen `OOS 520` Aislado en `/tmp` (`PARITY_SYNC Drift == 0`):**

    (cd /tmp && /home/ubuntu/vortex-rs/bin/vortex_exec)

4. **LAB 4 — Elegibilidad Institucional `12/12` + Autoprueba Aislada `SHM v15` y `PAR2`:**

    cd /home/ubuntu/vortex-rs && ./bin/vortex_tap

### Guia de Lectura de Contadores en `0` durante el *Walk-Forward* `PAR2` (`LAB 1`)

El directorio `data/` mantiene un anillo rotativo de `4,096` segmentos (`1,048,576` eventos = ultimos `~65 a 90 minutos` de mercado en vivo, reservando los primeros `5 minutos` para calentar indicadores):

- `Arm=N | Abr=N -> Fil=0 | Cls=0`: Si en esa ventana reciente el motor armo `N` ordenes en cola pero el escudo cancelo las `N` antes del llenado (`Abr=N`), hubo `0` *fills* (`Fil=0`) y `0` cierres (`Cls=0`); por diseno reporta `Net=+0.00 bps`, `MakerExit=0.00%` y `PF=N/A (0 closed)`.
- `Rej=0`: Valor optimo; indica `0` rechazos `PostOnly` al cruzar el *spread* con `25 ms` de latencia simulada.
- `TP:0` en `Abr=N(TP:0)`: El auditor contrafactual suma `TP+1` solo si tras cancelar una orden el precio cae en contra mas que la comision Maker en los `15 s` siguientes; si se cancelo porque el precio se escapo a favor o expiro la espera en rango, `TP` permanece en `0`.
- `LVR5s=+0.00bp` y `0 Fnd`: `LVR5s` mide seleccion adversa `5 s` despues de un *fill*, y `Fnd` cuenta cruces `00:00 / 08:00 / 16:00 UTC` con posicion abierta; con `Fil=0`, ambos marcan `0`.
