# VORTEX-OMEGA V5.1 — Auditoría Integral de Código, Seguridad IPC y Blindaje Operativo

**Commit de Referencia:** `ddbc4ba199fbcd52a76db47b7091acf896051f1c` (`38` archivos auditados y endurecidos)  
**Cobertura de Verificación:** `81 / 81` pruebas unitarias + suites de integración + `4 / 4` binarios LTO Fat + `0` advertencias en `cargo clippy --all-targets -- -D warnings`.

---

## 1. Alcance de la Auditoría en 3 Dominios Críticos

Se auditó línea por línea la totalidad del árbol fuente (`src/`, `src/bin/`, `tests/`, `ui/`, `deploy/` y `.github/workflows/`) dividiendo la revisión en tres dominios ortogonales:

| Dominio Auditado | Módulos Principales | Hallazgos Resueltos |
| :--- | :--- | :--- |
| **1. Memoria Compartida, Transporte y Demonios** | `lob_shm.rs`, `http_ws_transport.rs`, `bybit_gateway.rs`, `vortex_tap.rs`, `tap_ring.rs` | Mapeo `SHM v15` de sólo lectura libre de UB, timeouts estrictos de socket WS/HTTPS contra conexiones colgadas (half-open TCP), resincronización limpia L2 y purga atómica de segmentos PAR2. |
| **2. Alfa, Gobernador Bayesiano, FSM y Gemelo 1:1** | `alpha_3factor.rs`, `bayesian_governor.rs`, `governor_plasticity.rs`, `execution_fsm.rs`, `exit_quoter.rs`, `exec_daemon.rs`, `exec_gateway_handlers.rs`, `replay_walkforward.rs` | Paridad determinista 1:1 entre `vortex_exec` y `OmegaDigitalTwinEngine`, blindaje de reconciliación `closed-pnl` ante retraso de indexador Bybit, cadencia de funding 8h sin borrado post-corte y liberación de candados `slot_rpc_inflight` ante timeouts de red. |
| **3. Observatorio Web, UI y Despliegue Productivo** | `web_server.rs`, `vortex_observatory.html`, `deploy/*.sh`, `deploy/preview_web_windows.ps1`, `ci.yml` | Protección CSRF constante en tiempo, límites de payload HTTP, SSE a `5 Hz` sin fugas de descriptores, selección automática de puerto efímero en Windows cuando `127.0.0.1:8787` está ocupado por túnel SSH hacia `sniper-singapur`, y compuertas CI sin falsos positivos. |

---

## 2. Detalle Técnico de las Correcciones Aplicadas

### 2.1. Seguridad de Memoria en `MappedShmRegion` (`src/lob_shm.rs`)
- **Riesgo detectado:** Los lectores secundarios (`vortex_web` y diagnósticos RO) adjuntaban `/dev/shm/vortex_lob.mmap` validando magic/versión, pero requerían garantías estrictas de alineación, tamaño mínimo exacto (`size_of::<VortexShmArena>()`) y lecturas SeqLock sin referencias mutables aliasadas (`&mut`) sobre páginas mapeadas en modo lectura.
- **Solución:** Lecturas `read_volatile` con barreras atómicas `Ordering::Acquire` / `Ordering::Release` sobre punteros crudos constantes `*const T`, recuperación segura ante secuencias impares (escritor interrumpido) y verificación de invariante ABI (`VORTEX_SHM_VERSION == 15`, `size_of::<SlotTelemetrySnapshot>() == 96`).

### 2.2. Transporte WebSocket / HTTPS Resiliente (`src/http_ws_transport.rs`, `src/bybit_gateway.rs`)
- **Riesgo detectado:** Ante silencios asimétricos de red o caída silenciosa de NAT entre Singapur y el motor de Bybit UTA V5, un socket TLS bloqueante sin `SO_RCVTIMEO` / `SO_SNDTIMEO` podía congelar el hilo de pasarela privada o el lector público.
- **Solución:** Timeouts explícitos de lectura/escritura en los streams TCP subyacentes antes y después del handshake TLS, validación estricta de tramas de control (`Ping`/`Pong`/`Close`), drenaje acotado de respuestas HTTP chunked/keep-alive y watchdog de `15 s` sobre `slot_rpc_inflight` en `exec_daemon.rs` para impedir bloqueos permanentes de carril.

### 2.3. Custodia P0-B, Reconciliación Contable y Paridad 1e8 (`src/execution_fsm.rs`, `src/exec_gateway_handlers.rs`)
- **Riesgo detectado:** En cierres multi-tramo (`Tranche 1 + Runner 2`) o bajo latencia del endpoint `/v5/position/closed-pnl` de Bybit, una respuesta parcial del indexador podía sub-contabilizar lotes o desalinear `ParitySyncLedger`.
- **Solución:** Consolidación ponderada exacta en `i128` (`1e-8` USDT) con reintentos acotados (`MAX_CLOSE_RECONCILE_RETRIES`), verificación de cobertura de lotes antes de aceptar PnL remoto y preservación de `PARITY_SYNC Drift == 0` tanto en ejecución real como en `vortex_replay`.

### 2.4. Observatorio Local y Coexistencia con Túnel SSH (`src/web_server.rs`, `ui/vortex_observatory.html`, `deploy/preview_web_windows.ps1`)
- **Riesgo detectado:** En Windows 11, cuando el operador mantiene abierto el túnel `ssh -N -L 8787:127.0.0.1:8787 sniper-singapur`, el script de previsualización local colisionaba contra `127.0.0.1:8787` o mataba procesos ajenos.
- **Solución:** `PickPreviewPort()` detecta si `8787` está ocupado y asigna dinámicamente un puerto loopback libre efímero para previsualización local sin tocar el túnel SSH productivo; en `vortex_observatory.html` se corrigieron los identificadores de los slots `10` (`NEARUSDT`) y `11` (`LTCUSDT`) y el coloreado en vivo de la matriz `12x2`.

### 2.5. Higiene de Repositorio y Compuertas de Despliegue (`deploy/deploy_omega_v51_hot.sh`, `.gitignore`, `.github/workflows/ci.yml`)
- **Solución:** Exclusión explícita en `.gitignore` de artefactos volátiles de runtime (`data/vault/*.jsonl`, `*.mmap`, `*.par2z`, `.env`), validación automática de cero bytes `\r` (LF puro) y ejecución obligatoria de las 4 compuertas binarias (`vortex_tap`, `vortex_replay --dir`, `vortex_exec`, `vortex_web`) antes de promover binarios a `/home/ubuntu/vortex-rs/bin/`.
