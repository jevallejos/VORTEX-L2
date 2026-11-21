# INFORME VORTEX-OMEGA V5.5.5 — Embudo Elástico y Desbloqueo FOQ

- **Rama:** `fix/omega-v555-elastic-funnel-and-foq-unlock` (desde `main`)
- **Schema:** `"schema": "V5.5"` (preservado)
- **SHM:** `VORTEX_SHM_VERSION = 15` — `src/lob_shm.rs` con 0 diff
- **Orquestación:** 3 subagentes en secuencia, con archivos disjuntos y sin bloqueos de build en Windows

---

## 1. Diagnóstico de cuellos de botella sobre V5.5.4

| ID | Cuello detectado | Módulo | Síntoma |
|----|------------------|--------|---------|
| B-01 | Escalón discreto `signed_phase < -0.35 && btc_snr < 0.15` y `macro_phase` rezagada sin decaimiento | `wave_elliott.rs`, `alpha_3factor.rs` | `MACRO_C1` veta rebotes reales con OB propio y flujo fuerte (NEAR +0.54/+0.75, LINK +0.57/+0.69) |
| B-02 | `min(ofi_al, tfi_al)` en `q_struct` y truncamiento `BoundedScore::new_clamped` a 1.0 en Pilar 2 | `types.rs` | Órdenes multi-lote pre-print (TFI ≈ 0) con `q_struct = 0`; rango `[0, PHI]` cortado en 1.0 |
| B-03 | Paradoja de punta de cola: `i_spoof` marcaba anticipación pre-print legítima y el multiplicador hiperbólico `0.25 / q_ratio` disparaba auto-abortos | `kinematic_surface.rs` | Órdenes casi al frente de la cola se abortaban por el mero adelgazamiento de L1 |
| B-04 | Sobre-castigo `sqrt(SPR)` en GEOM y edge pasivo Inside-Spread no acreditado | `alpha_3factor.rs`, `types.rs`, `bayesian_governor.rs` | Libros con `SPR >= 2` sistemáticamente sub-valorados |
| B-05 | `kappa_spatial.max(kappa_flow)` rígido | `alpha_3factor.rs` | TFI aplastado en libros de 10k..30k lotes (SUI/WIF) |
| B-06 | Stagger de 2 slots sin salida cuando ambos quedan enterrados en colas profundas | `exec_daemon.rs`, `replay_walkforward.rs` | Throughput bloqueado con colas estancadas |

---

## 2. Formulaciones C^1 (Hermite `S_3` + constantes áureas)

Notación: `S_3(x; a, b)` = `hermite_smoothstep_c1`. Constantes: `PHI`, `INV_PHI`, `INV_PHI_SQ`, `INV_PHI_CUBE`, `INV_PHI_FOURTH`, `INV_PHI_FIFTH`.

### B-01a — Superficie de modulación Elliott (`wave_elliott.rs`)
- En `ImpulseWave3`, `ImpulseWave5` y `CorrectiveWave4`, los escalones se reemplazan por superficies `S_3`. Las ventanas de fase son `[INV_PHI_FIFTH, INV_PHI_CUBE]` y la compuerta BTC es `1 - S_3(btc_snr; INV_PHI_FOURTH, INV_PHI_CUBE)`. Las combina el helper privado `blend_wave_modulation_c1`.
- Atenuación de fase rezagada, auto-aprendida:
  - `ewma_pivot_interval_s`: EWMA con `α = INV_PHI_CUBE`, aprendido en `push_pivot`. El bucle por índice pasa a `copy_within`.
  - `intrinsic_pivot_scale_secs(&self, tau_mkt_s)`.
  - `compute_stale_macro_phase_attenuation_c1(age, scale) = 1 - INV_PHI * S_3(age/scale; PHI, PHI^3)`, con piso `INV_PHI_SQ`.
  - Se aplica a todos los regímenes salvo `ImpulseWave3` (que tiene su propio decaimiento) y `ClimaxExhaustion`.

### B-01b — Alivio de giro estructural en `CorrectiveAbc` (`alpha_3factor.rs`)
```
s_macro_neg     = S_3(-phi_al; 0.0, INV_PHI)
rho_own         = own / (own + max(opp, 382))
r_rev           = (1 - INV_PHI_CUBE) * psi_health * S_3(rho_own; INV_PHI_SQ, INV_PHI)
                  * S_3(min(ofi_al, eff_tfi_al); INV_PHI_SQ, INV_PHI)
                  * (1 - s_opp_ob_wall) * (1 - s_ice)
s_macro_neg_eff = s_macro_neg * (1 - r_rev)
```
- API: `compute_corrective_abc_macro_reversal_relief_c1(...)`.
- Se aplica en `evaluate_corrective_abc_fractal_penalty_with_health_c1_dynamic`, que es la ruta de `MACRO_C1`.
- Con `ob_own == 0`, con muro cercano o con flujo débil, `r_rev == 0` y el veto sigue al 100%.

### B-02 — Puente pre-print hacia `q_struct` (`types.rs`), recalibrado y aprobado por el usuario
```
preprint_coh = S_3(ofi_al; INV_PHI, 1 - INV_PHI_CUBE) * S_3(geom_al; INV_PHI_SQ, INV_PHI)
               * S_3(tfi_al; -INV_PHI_FIFTH, 0.0) * (1 - S_3(spoof; INV_PHI_FOURTH, INV_PHI_SQ))
flow_coh     = max(min(ofi_al, tfi_al), preprint_coh)
```
- Recalibración: la fórmula original llevaba un prefactor `INV_PHI` y la ventana de TFI cerraba en `INV_PHI_CUBE`. Con eso, el caso objetivo daba `preprint_coh = 0.119`, por debajo de los ≈0.52 necesarios.
- Guardias de cero exactas: `geom_al <= 0.25` o `tfi_al < -0.09` dan `preprint_coh = 0`.
- APIs: `compute_preprint_coherence_c1`, `compute_multilot_q_struct_v555_c1`. La función antigua delega con `geom = 0` y `spoof = 0`, y da el mismo resultado bit a bit.
- Pilar 2: la ganancia maker usa `q_struct_raw.clamp(0.0, PHI)` en lugar de `BoundedScore` truncado a 1.0.

### B-03 — Escudo Moallemi-Stoikov (`kinematic_surface.rs`)
- El caso de silencio de TFI en `i_spoof` ahora exige además `ofi_f * factor_geom <= 0`.
- Prioridad de cola:
  - `v_prio = clamp(1 - q_ratio, 0, 1)`
  - `s_align = S_3(own_flow; -INV_PHI_FOURTH, INV_PHI_SQ) * S_3(dir_s*ofi; 0, INV_PHI_SQ) * (1 - S_3(spoof_ewma; INV_PHI_CUBE, INV_PHI))`
  - API: `moallemi_yuan_priority_alignment_c1`.
- Multiplicador de cola:
  - `w_tail = 1 - S_3(q_ratio; INV_PHI_CUBE, INV_PHI_SQ)`
  - `m_eff = m - max(m - 1, 0) * INV_PHI * v_prio * s_align * w_tail`
- `stoikov_collapse`: el skew y los adv_ticks se escalan por `1 - s_align * w_tail * S_3(own_flow; 0, INV_PHI_CUBE)`.
- Con flujo `<= 0` o `s_align = 0`, el rigor es idéntico al de V5.5.4.

### B-04 — Penny-Jump Inside-Spread
- Divisor GEOM: `spread_geom_divisor_c1 = 1 + (sqrt(spr) - 1) * (INV_PHI_SQ + INV_PHI * S_3(spoof; INV_PHI_CUBE, INV_PHI))`.
- Edge sobre la excursión: `penny_jump_edge_c1 = INV_PHI * min(spr - 1, 3) * tau_tick * (1 - S_3(spoof; INV_PHI_CUBE, INV_PHI))`, sumado en `evaluate_entry_signal_v554`.
- Crédito en Pilar 2: `pilar2_penny_jump_credit_c1 = penny_jump_edge_c1 * (1 - p_taker) * fill_rate_est`. Se suma después de todos los gates.
- En el governor, `evaluate_slot_pilar2_ex_ante_ev_with_leader_v555_sbps` usa `qualifies_inside_spread_penny_jump_c1` y `slot_fill_rate_eta`.
- Con `SPR == 1`, el divisor vale 1, el edge 0 y el crédito 0: V05 queda bit-idéntico.

### B-05 — Fusión geométrica espacial-flujo (`tfi_endogenous_prior_lots`)
```
w_sp        = clamp(INV_PHI_SQ + INV_PHI * S_3(spoof_density_ewma; INV_PHI_CUBE, INV_PHI), 0, 1)
kappa_blend = max(kappa_flow, floor)^(1 - w_sp) * max(kappa_spatial, kappa_flow, floor)^(w_sp)
w_warm      = S_3(learned_flow / init_lots; 1, PHI^2)    // mezcla geométrica rígido <-> blend
```
- Desviación justificada respecto de la condición literal `learned_flow_lots > floor_lots`: `on_l2_book_update` siembra `learned_flow = 4 * floor`, así que esa condición habría activado el blend en frío. Con `w_warm`, en frío o con el libro spoofeado sigue mandando `kappa_spatial`.

### B-06 — Desbloqueo del 3er slot con cola enterrada (`exec_daemon.rs`, `replay_walkforward.rs`)
- Helper puro `allows_buried_queue_third_slot_unlock_v555(...)` y constante `MAX_TOTAL_ARMED_SLOTS_V555 = 6`.
- Permite armar un 3er slot solo si se cumple todo lo siguiente:
  1. Hay exactamente 2 slots `RestingEntryQueue` del mismo lado y ambos están enterrados (`v_prio < INV_PHI_CUBE` con `elapsed >= PHI^2 * rtt`).
  2. El candidato está en `Idle` y `psi > INV_PHI_SQ`.
  3. El total queda en `< 6` después de armar.
  4. Entra por Penny-Jump (`SPR >= 2`, `q_ahead = 0`) o por cola ultra-fina (`L1 <= max(1, INV_PHI * lots)`).
  5. `MonetaryPortfolioBetaGuard::can_open_with_risk_ppm` lo admite (≤ 14,400 ppm total / ≤ 10,800 ppm beta firmada).
- El umbral es un corte duro (decisión booleana de admisión), no una transición, así que no aplica `S_3`.
- Cableado 1:1 en daemon, walk-forward y `omega_twin` como `!can_stage_v542 && !unlock`.
- Las 6 llamadas Pilar 2 `_v553` pasan a `_v555` con el mismo `spoof_d` (`spoof_clocks[s].spoofing_density()`) en el daemon y en el replay. Ya no queda ninguna llamada `_v553` en esos archivos.

---

## 3. Matriz de invariantes de seguridad

| Invariante | Mecanismo de preservación | Estado |
|------------|---------------------------|--------|
| SCAR-LOCK 63/63 (variedad 2,160) | Escalones `sniper_gp` / `gp_snr_bonus` de W3 sin tocar; suite lib | ✅ verde |
| Pérdidas `CorrectiveAbc` #18, #19, #22, #23, #27, #30, ETH #2, ETH #5 | `r_rev = 0` sin OB propio, frente a muro o con flujo débil | ✅ verde |
| Red-Team 28/28 (V01..V28) | Suites de integración sin cambios | ✅ verde |
| V05 WIF Nyquist (`SPR == 1`, 6.8 bp) | Divisor 1, edge 0, crédito 0; EV `None` igual que en v553 | ✅ verde |
| V06 Queue Geom Flip 170 ms / V18 / V24 | `s_align = 0` con flujo opuesto: rigor íntegro | ✅ verde |
| BTCUSDT 1-lote en QUANTUM_EV | `passes_quantum_ev_growth_gate` independiente de `q_struct` | ✅ bloqueado |
| Digital Twin (HwkAbort ≥ 1, Rej = 0, Drift = 0) / OOS 520 | Cableado 1:1 daemon ↔ replay ↔ twin | ✅ verde |
| `test_v531` (geometría débil / retail en libro de 30k) | Guardias de cero en B-02; `w_warm` en B-05 | ✅ verde |
| SHM v15 | `git diff src/lob_shm.rs` vacío | ✅ 0 diff |
| `"schema": "V5.5"`, UTF-8 sin BOM, LF puro | Verificado en los archivos nuevos y modificados | ✅ |

---

## 4. Suite focalizada `tests/v555_funnel_and_foq_unlock.rs` (145 líneas, 5 tests)

| Test | Certificación numérica |
|------|------------------------|
| (a) B-01 | psi 0.90, OB propio 700 frente a 300, sin muro. NEAR: `r_rev 0.5118`, pen 0.4882. LINK: `r_rev 0.6137`, pen 0.3863. Ambos `< INV_PHI`. Sin OB propio y con muro 0.85: `r_rev = 0`, pen = 1 (vetado). |
| (b) B-02 + B-04 | `q_struct = 0.618 > INV_PHI_SQ` (0 con geom = 0). Crédito Penny-Jump (3, 50, 100, 0.8, 0.05) = 44.50. EV de LINK con 8 lotes y SPR 3: v555 1082 > v553 1015. WIF SPR 1: crédito 0, EV `None`. BTC 1L bloqueado. |
| (c) B-03 | Punta de cola 5000 → 120 con OFI +0.48 / TFI +0.35: tox 0.297, no aborta. Inversión con OFI -0.45 / TFI -0.40: tox 0.508, aborta. |
| (d) B-05 | Libro de 25k, flujo convergido ≈ 600, impulso de 450: `TFI 0.4708 >= INV_PHI_SQ`. |
| (e) B-06 | `true` con Penny-Jump SPR 2. `false` con SPR 1 y L1 gruesa. `false` si los slots aún no están enterrados por tiempo. |

> [!NOTE]
> Límites conocidos de la certificación:
> - **(a)** certifica la cota de penalización macro `1 * (1 - r_rev)` con las dos `S_3` macro saturadas, no un snapshot `CorrectiveAbc` completo.
> - **(d)** midió `spoof = 0.0` porque la API pública no permite fijarlo en 0.05. Con 0.05 el prior crece apenas ≈3% y el TFI estimado (≈0.46) sigue por encima del umbral.
> - **B-01a:** la atenuación de fase actúa tras ≳ PHI veces la escala intrínseca sin pivote. Conviene vigilarla en replays largos, aunque Drift = 0 se mantiene.

---

## 5. `git diff --stat` y compuertas constitucionales

```
 src/alpha_3factor.rs      | 119 +++++++++++++++++++++++++++---
 src/bayesian_governor.rs  |  53 +++++++++++++-
 src/exec_daemon.rs        | 101 ++++++++++++++++++++++---
 src/kinematic_surface.rs  |  54 +++++++++++++-
 src/replay_walkforward.rs |  71 +++++++++++++-----
 src/types.rs              | 183 +++++++++++++++++++++++++++++++++++++++++++---
 src/wave_elliott.rs       | 149 +++++++++++++++++++++++++++----------
 7 files changed, 639 insertions(+), 91 deletions(-)
 + tests/v555_funnel_and_foq_unlock.rs (nuevo, 145 líneas, sin trackear)
```

| # | Compuerta | Resultado |
|---|-----------|-----------|
| 1 | `cargo clippy --all-targets -- -D warnings` | ✅ 0 warnings |
| 2 | `cargo test --lib -- --test-threads=1` | ✅ 94 passed / 0 failed / 0 ignored |
| 3 | `cargo test --tests -- --test-threads=1` | ✅ 27/27 de integración (1+3+1+14+3+5) + 94/94 unitarios |
| 4 | `git diff src/lob_shm.rs` | ✅ vacío (0 diff) |

Cada compuerta pasó en su única ejecución, sin iteraciones correctivas.
