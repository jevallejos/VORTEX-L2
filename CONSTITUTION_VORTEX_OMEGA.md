# CONSTITUCIÓN TÉCNICA VORTEX-L3 OMEGA (SHM v13 — EDICIÓN DE SALA LIMPIA)

## 1. Las Nueve Leyes Evolutivas e Invariantes de Producción

- **Ley I — Verdad Contable Neta (`1e8` Exact Lot & Fee-Drag Accounting):** `ParitySyncLedger` contabiliza cada lote ejecutado con deriva cero (`PARITY_SYNC Drift == 0`), aplica partición monotónica `70%` Bóveda Blindada / `30%` Reserva de Saneamiento y reconcilia contra `/v5/position/closed-pnl` con estimador adaptativo Kalman-EWMA (`OnlineIndexerLagEstimator`).
- **Ley II — Plasticidad Matemática y Cero Constantes Mágicas:** Todos los umbrales derivan de la fricción VIP-0 (`200 / 550 ScaledBps`), la volatilidad realizada de 5 minutos, la proporción áurea (`1.618034 / 2.618034`) y los ciclos físicos (`86,400 s` UTC y `28,800 s` Bybit Funding).
- **Ley III — Gobernanza Bayesiana y Paridad Continua de Riesgo en USDT:** Presupuesto endógeno por carril `base_slot_risk_budget_1e8` fijado en `2,400 ppm` (`0.24%` del patrimonio), guardia continua `C^1` para lotes mínimos sobredimensionados (`BTCUSDT Slot 0`) y `PortfolioBetaGuard` (`3 Bid / 3 Ask / 6 Total`).
- **Ley IV — Transporte Asíncrono No-Bloqueante e Idempotencia Conmutativa:** Bucle principal determinista a `200.0 Hz` (`5 ms`) desacoplado de red mediante canales acotados (`BybitAsyncGateway`, `WsTradeTunnel`, `AsyncParquetRingWriter`) y retículo conmutativo `WsOrderLatticeState`.
- **Ley V — Primacía Pasiva (`PostOnly`) y Escudos de Supervivencia (`P0-A / P0-B`):** Ejecución ordinaria `100% Limit PostOnly`; `EmergencyFlattenSlot` (`Market IOC reduceOnly`) ante brecha adversa superior a `1.5x` el `Stop-Loss` elástico (**Parche P0-A**); custodia ininterrumpida `is_safe_halt_standby` bajo `HALTED_ON_LOSS.lock` hasta verificar exposición cero (**Parche P0-B**).
- **Ley VI — Universo Elástico de 12 Contratos y Cadencia Armónica UTC:** Elegibilidad por `Tick-to-Price` + barrera `3x` de fricción + superficie `C^inf` de 3 polos institucionales UTC, protección de arbitraje pre-corte de *Funding* (`-75 s`) y discriminador exponencial de ruptura estructural.
- **Ley VII — Modularidad de Sala Limpia y Paridad Gemelo-Producción (`1:1`):** 18 módulos especializados en `src/` bajo memoria compartida `SHM v13` (`#[repr(C, align(64))]`) y dimensionamiento único `compute_slot_entry_lots` compartido entre `vortex_exec` y `vortex_replay`.
- **Ley VIII — Evolución Pareto-Superior, Elliott v2.0 y *Zero Self-Choke*:** Autómata ZigZag causal de 6 pivotes en pila (`[WavePivot; 6]`), flujo forzado con signo en `allLiquidation` (`"S": "Sell" -> Bid`, `"S": "Buy" -> Ask`), cosecha pasiva en `ClimaxExhaustion`, expansión de techo Fibonacci `1.618x / 2.618x` (`FibonacciImpulseSurf`) y trinquete convexo del `72%` (`ConvexMfeRatchetLock`).
- **Ley IX — Gobernanza de Despliegue Atómico (`Antigravity IDE -> GitHub CI -> sniper-singapur`):** Escritura atómica `UTF-8 sin BOM + LF`, cero parches regex, certificación obligatoria en Ubuntu CI antes del drenaje controlado (`DRAIN_MODE.lock` -> `0 open positions`) en producción.

## 2. Mapa Modular Certificado (`src/` — 18 Unidades + 3 Binarios)

| Módulo | Responsabilidad Principal |
| :--- | :--- |
| `src/types.rs` | Newtypes dimensionales, 12 `APPROVED_SPECS`, Paridad de Riesgo `2,400 ppm` y cadencia `C^inf` UTC/Funding. |
| `src/lob_shm.rs` | Arena POSIX/Windows `SHM v13` (`SeqlockLobSlot` + `SlotTelemetryCell` sin desgarros). |
| `src/alpha_3factor.rs` | Factores ortogonales `OFI`, `TFI`, `GEOM`, volatilidad Parkinson-EWMA y vacío `LVDBS`. |
| `src/wave_elliott.rs` | Autómata ZigZag causal de 6 pivotes, Axiomas E1–E3, Hurst VR, `allLiquidation` y Fibonacci `1.618x/2.618x`. |
| `src/kinematic_surface.rs` | Superficie de energía cinemática, escudo Moallemi de cola L1 y laboratorio `63/63` cicatrices. |
| `src/bayesian_governor.rs` | Gobernador Bayesiano `12x2`, varianza Welford, rehidratación JSONL y dimensionador SSOT. |
| `src/ws_protocol.rs` | Firma HMAC-SHA256, `orderLinkId` biyectivo, retículo `WsOrderLatticeState` y cuota VIP-0. |
| `src/exit_quoter.rs` | Cotizador cuántico de salida, techo Fibonacci, cosecha por `ClimaxExhaustion`, trinquete `72%` y guardia `P0-A`. |
| `src/execution_fsm.rs` | Máquina de estados de 12 carriles, fusión contable multi-tramo, trinquete Wald SL y plasticidad `12x`. |
| `src/forensic_parquet.rs` | Formato columnar `PAR1` con `FNV-1a 64-bit` y `ParitySyncLedger` `70/30`. |
| `src/tap_ring.rs` | Compuerta aislada de elegibilidad de 12 pares y escritor asíncrono de anillo `PAR1`. |
| `src/http_ws_transport.rs` | Conexiones `Keep-Alive HTTP/1.1` chunked, túnel persistente `WsTradeTunnel` y `OnlineIndexerLagEstimator`. |
| `src/bybit_client.rs` | Cliente autenticado Bybit UTA V5 REST/WS y ejecutor de emergencia `flatten_position_market_ioc`. |
| `src/bybit_gateway.rs` | Despachador asíncrono `BybitAsyncGateway`, `EmergencyFlattenSlot` y *listener* WS privado. |
| `src/fifo_sim.rs` | Simulador determinista `L1..L4` FIFO (`200k` ev) y laboratorio de ablación `A / B / A+B`. |
| `src/replay_walkforward.rs` | Motor `Walk-Forward PAR1` `1:1` con paridad exacta de pre-vuelo `25 ms` y dimensionamiento SSOT. |
| `src/telemetry_hud.rs` | Baúl Forense de `58` campos, HUD en vivo sin desgarros, `Executive Dashboard` y alertas Telegram. |
| `src/exec_gateway_handlers.rs` | Manejadores de eventos en vivo, `bootstrap_live_adopt` (`P0-A`) y suite `OOS 520`. |
| `src/exec_daemon.rs` | Demonio autónomo a `200.0 Hz` con `is_safe_halt_standby` (`P0-B`) y drenaje verificado. |
