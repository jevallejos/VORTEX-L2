//! VORTEX-OMEGA V5.2 Live Vault Recalibration Suite: 31 Live Trades, 118 Aborts & 7 Elastic Self-Learning Pillars.
#![allow(clippy::cast_precision_loss, clippy::too_many_lines)]

use vortex_rs::alpha_3factor::{
    is_opposing_impulse_wave3_hazard, passes_corrective_abc_snapshot_guard_c1,
    passes_elastic_geom_and_macro_impulse_guard,
};
use vortex_rs::bayesian_governor::OnlineAdaptiveGovernor;
use vortex_rs::bybit_gateway::should_repeg_resting_exit_tp;
use vortex_rs::exec_gateway_handlers::run_oos_520_multi_regime_suite;
use vortex_rs::execution_fsm::{
    passes_elliott_macro_coherence_guard, record_live_ws_pulse, ClosedTradeKind, ExitOrderRole,
    ExitSubReason, SlotExecutionEngine,
};
use vortex_rs::exit_quoter::{
    compute_canonical_exit_quote_omega, compute_tranche1_split_lots_core, OpenExitContext,
    ShadowRunnerKinematics,
};
use vortex_rs::fee_cadence_beta::FeeSchedule;
use vortex_rs::kinematic_surface::{check_queue_depletion_velocity_abort, UNIFIED_LOSS_LAB_COUNT};
use vortex_rs::lob_shm::{BookLevel, LastVetoCode, SymbolLobSnapshot};
use vortex_rs::replay_walkforward::run_omega_digital_twin_walk_forward_lab;
use vortex_rs::types::{
    passes_asymmetry_ev_gate_v51, BoundedScore, LotQty, PriceTicks, ScaledBps, Side, TimestampNs,
    APPROVED_SPECS,
};
use vortex_rs::wave_elliott::{WaveObservables, WaveRegime};

fn mk_vault_snap(
    slot: usize,
    px: i64,
    vol_sbps: i64,
    ofi: f64,
    tfi: f64,
    geom: f64,
    ts_ns: i64,
) -> SymbolLobSnapshot {
    let mut s = SymbolLobSnapshot::new_empty();
    s.symbol_id = APPROVED_SPECS[slot].symbol_id;
    s.is_eligible = true;
    s.sequence_id = 500;
    s.ts_local_ns = TimestampNs(ts_ns);
    s.realized_vol_5m = ScaledBps(vol_sbps);
    s.bids[0] = BookLevel { price: PriceTicks(px), qty: LotQty(900) };
    s.asks[0] = BookLevel { price: PriceTicks(px + 1), qty: LotQty(900) };
    s.refresh_deep_l1_l5_totals();
    s.mark_price = PriceTicks(px);
    s.last_trade_price = PriceTicks(px);
    s.factor_ofi = BoundedScore::new_clamped(ofi);
    s.factor_tfi = BoundedScore::new_clamped(tfi);
    s.factor_geom = BoundedScore::new_clamped(geom);
    s.ob_bull_efficacy_pm = 520;
    s.ob_bear_efficacy_pm = 520;
    s
}

#[test]
fn test_v52_pillar1_non_amnesic_epoch_reset_and_hierarchical_competing_hazards_pooling() {
    let t0 = 1_795_010_000_000_000_000_i64;
    let w_abc = WaveObservables {
        regime: WaveRegime::CorrectiveAbc,
        climax_active: false,
        hurst: WaveObservables::encode_hurst(0.68),
        macro_phase: BoundedScore::new_clamped(0.20),
        oi_impulse: BoundedScore::ZERO,
        liq_imbalance: BoundedScore::ZERO,
        fib_proj_bps: ScaledBps(0),
    };

    let mut gov_pre = OnlineAdaptiveGovernor::new();
    gov_pre.observe_closed_trade_with_regime_v51(
        4, Side::Ask, PriceTicks(19_650), ScaledBps(-2_536), ScaledBps(100), false, w_abc, 1.0, TimestampNs(t0),
    );
    gov_pre.observe_closed_trade_with_regime_v51(
        3, Side::Ask, PriceTicks(7_156), ScaledBps(-4_219), ScaledBps(0), false, w_abc, 1.0, TimestampNs(t0 + 60_000_000_000),
    );
    gov_pre.observe_closed_trade_with_regime_v51(
        1, Side::Bid, PriceTicks(179_267), ScaledBps(-3_790), ScaledBps(50), false, w_abc, 1.0, TimestampNs(t0 + 120_000_000_000),
    );

    // Untouched SUI slot (slot 5, 0 local trades) inherits systemic contagion lift (> 50pm), while TAO Ask stays undiluted.
    let p_tao_ask = gov_pre.slot_taker_prob_permille(4, Side::Ask);
    let p_sui_untouched = gov_pre.slot_taker_prob_permille(5, Side::Bid);
    assert!(p_tao_ask >= 120);
    assert!(p_sui_untouched >= 65);

    // Persist scar snapshot followed by EPOCH_RESET (mirroring live Singapur #545 & #553) and rehydrate: zero Bayesian amnesia.
    let tmp_dir = std::env::temp_dir().join(format!("vortex_v52_scar_rehyd_{}", std::process::id()));
    let _ = std::fs::create_dir_all(&tmp_dir);
    let scar_path = tmp_dir.join("vortex_governor_scar_memory.jsonl");
    let loss_path = tmp_dir.join("vortex_red_loss_box.jsonl");
    let vault_path = tmp_dir.join("vortex_ai_forensic_vault.jsonl");
    let pre_line = gov_pre
        .to_scar_snapshot_json(TimestampNs(t0 + 130_000_000_000))
        .to_string();
    let reset_line = format!(
        "{{\"schema\":\"V5.0\",\"event\":\"EPOCH_RESET\",\"ts_ns\":{}}}",
        t0 + 200_000_000_000
    );
    assert!(std::fs::write(&scar_path, format!("{pre_line}\n")).is_ok());
    assert!(std::fs::write(&vault_path, format!("{reset_line}\n")).is_ok());
    assert!(std::fs::write(&loss_path, "").is_ok());

    let mut gov_rehyd = OnlineAdaptiveGovernor::new();
    let _ = gov_rehyd.rehydrate_from_scar_and_loss_files(&scar_path, &loss_path, Some(&vault_path));
    assert_eq!(gov_rehyd.slot_taker_prob_permille(4, Side::Ask), p_tao_ask);
    assert_eq!(gov_rehyd.slot_taker_prob_permille(5, Side::Bid), p_sui_untouched);
    let (_, sl_streak_after_reset, _) = gov_rehyd.slot_direction_diagnostics(4, Side::Ask);
    assert_eq!(sl_streak_after_reset, 0);

    // Stressed zero-discovery scratch (+0.20 bps net, +1.10 bps MFE) increases hazard (> 50pm & > clean win) rather than decaying p_Taker.
    let mut gov_clean = gov_rehyd.clone();
    gov_rehyd.observe_closed_trade_with_regime_v51(
        2, Side::Bid, PriceTicks(6_200), ScaledBps(20), ScaledBps(110), true, w_abc, 1.0, TimestampNs(t0 + 260_000_000_000),
    );
    gov_clean.observe_closed_trade_with_regime_v51(
        2, Side::Bid, PriceTicks(6_200), ScaledBps(320), ScaledBps(720), true, w_abc, 1.0, TimestampNs(t0 + 260_000_000_000),
    );
    assert!(
        gov_rehyd.slot_taker_prob_permille(2, Side::Bid)
            > gov_clean.slot_taker_prob_permille(2, Side::Bid)
    );
    let mut gov_single = OnlineAdaptiveGovernor::new();
    gov_single.observe_closed_trade_with_regime_v51(
        2, Side::Bid, PriceTicks(6_200), ScaledBps(20), ScaledBps(110), true, w_abc, 1.0, TimestampNs(t0),
    );
    assert!(gov_single.slot_taker_prob_permille(2, Side::Bid) > 50);
    let _ = std::fs::remove_dir_all(&tmp_dir);
}

#[test]
fn test_v52_pillar2_and_3_blocks_live_v51_losses_tao30_and_sui31_while_preserving_sol10_and_avax5() {
    let fees = FeeSchedule::VIP0_DEFAULT;
    let t0 = 1_795_020_000_000_000_000_i64;

    // 1. Live V5.1 #03 (Trade #30, TAOUSDT Ask 12L, -25.36bp): CorrectiveAbc H=0.744 + unbacked Bull OB wall (88.6bp, sc=0.64) -> VETOED.
    let mut tao30 = mk_vault_snap(4, 19_650, 1_557, -0.48, -0.52, -0.35, t0);
    tao30.ob_bear_efficacy_pm = 588;
    tao30.ob_bull_efficacy_pm = 532;
    tao30.ob_bull_score = BoundedScore::new_clamped(0.64);
    tao30.ob_dist_bull_bps = ScaledBps(8_860);
    tao30.ob_bear_score = BoundedScore::ZERO;
    tao30.ob_dist_bear_bps = ScaledBps(0);
    tao30.set_wave_observables(WaveObservables {
        regime: WaveRegime::CorrectiveAbc,
        climax_active: false,
        hurst: WaveObservables::encode_hurst(0.744),
        macro_phase: BoundedScore::new_clamped(-0.35),
        oi_impulse: BoundedScore::ZERO,
        liq_imbalance: BoundedScore::ZERO,
        fib_proj_bps: ScaledBps(0),
    });
    assert!(!passes_corrective_abc_snapshot_guard_c1(Side::Ask, &tao30));
    assert!(!passes_elliott_macro_coherence_guard(Side::Ask, &tao30));
    assert_eq!(
        passes_elastic_geom_and_macro_impulse_guard(&tao30, Side::Ask, 0.0),
        Err(LastVetoCode::MacroElliottContradiction)
    );

    // Unified multi-lot EV gate blocks marginal/negative EV on divisible TAO 12L under pooled p_Taker=98pm, while preserving AVAX #5 WIN.
    assert!(!APPROVED_SPECS[4].passes_single_lot_asymmetry_ev_gate(
        LotQty(12), PriceTicks(19_650), ScaledBps(1_557), 594, 98, 0.35, 532, 588, Side::Ask, fees,
    ));
    assert!(!passes_asymmetry_ev_gate_v51(
        APPROVED_SPECS[4], PriceTicks(19_650), LotQty(12), ScaledBps(580), ScaledBps(540),
        ScaledBps(2_200), 98, BoundedScore::new_clamped(0.08), fees,
    ));
    assert!(APPROVED_SPECS[3].passes_single_lot_asymmetry_ev_gate(
        LotQty(18), PriceTicks(7_263), ScaledBps(2_206), 628, 50, 0.23, 742, 675, Side::Bid, fees,
    ));

    // 2. Live Trade #10 (SOLUSDT Ask, +2.43bp True Win): CorrectiveAbc H=0.677 backed by stronger Bear OB wall (7.3bp sc=0.97 vs 42.8bp sc=0.78) -> PRESERVED.
    let mut sol10 = mk_vault_snap(5, 14_250, 2_850, -0.56, -0.64, -0.42, t0);
    sol10.ob_bear_efficacy_pm = 618;
    sol10.ob_bull_efficacy_pm = 618;
    sol10.ob_bear_score = BoundedScore::new_clamped(0.97);
    sol10.ob_dist_bear_bps = ScaledBps(730);
    sol10.ob_bull_score = BoundedScore::new_clamped(0.78);
    sol10.ob_dist_bull_bps = ScaledBps(4_280);
    sol10.set_wave_observables(WaveObservables {
        regime: WaveRegime::CorrectiveAbc,
        climax_active: false,
        hurst: WaveObservables::encode_hurst(0.677),
        macro_phase: BoundedScore::new_clamped(-0.36),
        oi_impulse: BoundedScore::ZERO,
        liq_imbalance: BoundedScore::ZERO,
        fib_proj_bps: ScaledBps(0),
    });
    assert!(passes_corrective_abc_snapshot_guard_c1(Side::Ask, &sol10));
    assert!(passes_elliott_macro_coherence_guard(Side::Ask, &sol10));

    // 3. Live V5.1 #04 (Trade #31, SUIUSDT Bid 20L, -24.30bp): Opposing bearish ImpulseWave3 (macro=-0.95, fib=-57.53bp) -> 0ms immediate hazard.
    let mut sui31 = mk_vault_snap(5, 7_198, 2_420, 0.35, 0.40, 0.28, t0);
    sui31.set_wave_observables(WaveObservables {
        regime: WaveRegime::ImpulseWave3,
        climax_active: false,
        hurst: WaveObservables::encode_hurst(0.756),
        macro_phase: BoundedScore::new_clamped(-0.95),
        oi_impulse: BoundedScore::new_clamped(-0.30),
        liq_imbalance: BoundedScore::new_clamped(-0.45),
        fib_proj_bps: ScaledBps(-5_753),
    });
    assert!(is_opposing_impulse_wave3_hazard(Side::Bid, &sui31));
    assert_eq!(
        passes_elastic_geom_and_macro_impulse_guard(&sui31, Side::Bid, 0.0),
        Err(LastVetoCode::MacroElliottContradiction)
    );
}

#[test]
fn test_v52_pillar4_5_6_7_underwater_bbo_bailout_sui20_runner_split_outward_tp_and_fifo() {
    record_live_ws_pulse(75_000, 100);
    let fees = FeeSchedule::VIP0_DEFAULT;
    let t0 = 1_795_030_000_000_000_000_i64;

    // Pillar 4: Path 13 (V5.1 #04 SUI Bid 20L @ 7198) underwater -1.39bp at t=+12s under opposing ImpulseWave3 -> joins Maker Ask BBO (7197) instead of stranded +4.25bp (7202).
    let mut sui_p13 = mk_vault_snap(5, 7_196, 2_420, -0.44, -0.52, -0.25, t0 + 12_000_000_000);
    sui_p13.bids[0] = BookLevel { price: PriceTicks(7_196), qty: LotQty(650) };
    sui_p13.asks[0] = BookLevel { price: PriceTicks(7_197), qty: LotQty(480) };
    sui_p13.mark_price = PriceTicks(7_196);
    sui_p13.set_wave_observables(WaveObservables {
        regime: WaveRegime::ImpulseWave3,
        climax_active: false,
        hurst: WaveObservables::encode_hurst(0.756),
        macro_phase: BoundedScore::new_clamped(-0.95),
        oi_impulse: BoundedScore::new_clamped(-0.32),
        liq_imbalance: BoundedScore::ZERO,
        fib_proj_bps: ScaledBps(-5_753),
    });
    let sui_ctx = OpenExitContext {
        slot_idx: 5,
        side: Side::Bid,
        avg_price_ticks: PriceTicks(7_198),
        peak_favorable_price: PriceTicks(7_198),
        worst_adverse_price: PriceTicks(7_196),
        initial_lots: LotQty(20),
        open_lots: LotQty(20),
        tranche1_harvested: false,
        tranche1_lots: LotQty(0),
        tranche1_exit_price: PriceTicks(0),
        defensive_floor_locked: true,
        active_exit_price: PriceTicks(7_202),
        structural_tp_price: PriceTicks(7_204),
        resting_exit_price: PriceTicks(7_202),
        sl_trigger_price: PriceTicks(7_025),
        opened_ts: TimestampNs(t0),
        learned_mfe_bps: 620,
        learned_t1_permille: 500,
        learned_ir_x100: 20,
        learned_ret_std_bps: 210,
        intrinsic_tau_mkt_s: 180,
        last_eff_sl_ticks: PriceTicks(7_025),
        ratchet_locked_gross_bps: 0,
        last_closed_mfe_bps: 0,
        last_closed_mae_bps: -278,
        active_fees: fees,
        learned_taker_prob_pm: 75,
    };
    let kin_adv = ShadowRunnerKinematics {
        flow_ema_fast: -0.48,
        flow_ema_slow: -0.42,
        in_scar_debt: false,
        chandelier_armed: false,
    };
    let q_bail = compute_canonical_exit_quote_omega(&sui_ctx, kin_adv, &sui_p13);
    assert!(q_bail.is_some_and(|q| q.target_price == PriceTicks(7_197) && q.urgent_close_repeg));

    // Pillar 5A: V5.1 #02 SUI 20L @ $0.7202 with t1_permille=650 splits cleanly into 12 + 8 lots (both >= $5.25) instead of returning None!
    assert_eq!(
        compute_tranche1_split_lots_core(5, LotQty(20), PriceTicks(7_202), 650),
        Some(LotQty(12))
    );

    // Pillar 5B: Runner 2 (tranche1_harvested == true) expands target smoothly with sigma_1m beyond structural_tp_price.
    let mut sui_win = mk_vault_snap(5, 7_202, 4_200, 0.52, 0.60, 0.35, t0 + 18_000_000_000);
    sui_win.set_wave_observables(WaveObservables {
        regime: WaveRegime::CorrectiveAbc,
        climax_active: false,
        hurst: WaveObservables::encode_hurst(0.54),
        macro_phase: BoundedScore::new_clamped(0.25),
        oi_impulse: BoundedScore::ZERO,
        liq_imbalance: BoundedScore::ZERO,
        fib_proj_bps: ScaledBps(0),
    });
    let mut runner_ctx = sui_ctx;
    runner_ctx.tranche1_harvested = true;
    runner_ctx.open_lots = LotQty(8);
    runner_ctx.tranche1_lots = LotQty(12);
    runner_ctx.tranche1_exit_price = PriceTicks(7_202);
    runner_ctx.defensive_floor_locked = false;
    let kin_prop = ShadowRunnerKinematics {
        flow_ema_fast: 0.58,
        flow_ema_slow: 0.44,
        in_scar_debt: false,
        chandelier_armed: false,
    };
    assert!(compute_canonical_exit_quote_omega(&runner_ctx, kin_prop, &sui_win)
        .is_some_and(|q| q.role == ExitOrderRole::LvdbsDeepSurf50 && q.target_price.0 > 7_206));

    // Pillar 5C: Buckley-James right-censored MFE plasticity expands on aligned TP fill and never shrinks on defensive scratch.
    let mut eng_mfe = SlotExecutionEngine::new(5);
    eng_mfe.learned_mfe_bps = 620;
    eng_mfe.open_vol_5m_bps = ScaledBps(4_800);
    eng_mfe.flow_ema_fast = 0.62;
    eng_mfe.last_closed_mfe_bps = 560;
    eng_mfe.last_exit_sub_reason = ExitSubReason::StandardStructuralTp;
    eng_mfe.observe_censored_close_mfe_plasticity(Side::Bid, 560, true);
    assert!(eng_mfe.learned_mfe_bps > 620);
    let expanded_mfe = eng_mfe.learned_mfe_bps;
    eng_mfe.last_closed_mfe_bps = 430;
    eng_mfe.last_exit_sub_reason = ExitSubReason::AdverseScratchEscape;
    let _ = ClosedTradeKind::MakerPassiveUnwind;
    eng_mfe.observe_censored_close_mfe_plasticity(Side::Bid, 430, true);
    assert_eq!(eng_mfe.learned_mfe_bps, expanded_mfe);

    // Pillar 6: Path 10 (V5.1 #01 ETH Ask @ 1976.42): restores resting TP from 1975.57 (+4.30bp) outward to 1975.15 (+6.43bp) when mid is 1975.94 (+2.43bp), without yanking imminent fills.
    let eth = APPROVED_SPECS[1];
    assert!(should_repeg_resting_exit_tp(
        eth, Side::Ask, PriceTicks(197_594), PriceTicks(197_557), LotQty(1),
        PriceTicks(197_515), LotQty(1), false, 243, fees,
    ));
    assert!(!should_repeg_resting_exit_tp(
        eth, Side::Ask, PriceTicks(197_564), PriceTicks(197_557), LotQty(1),
        PriceTicks(197_515), LotQty(1), false, 395, fees,
    ));

    // Pillar 7: FIFO queue consumption under strong aligned TFI (+0.62) does NOT false-abort, whereas genuine adverse sweep DOES abort.
    let fifo_ok = mk_vault_snap(4, 19_505, 2_600, -0.01, 0.62, 0.18, t0);
    assert!(!check_queue_depletion_velocity_abort(Side::Bid, LotQty(800), LotQty(300), &fifo_ok));
    let sweep_bad = mk_vault_snap(4, 19_505, 2_600, -0.18, -0.35, -0.12, t0);
    assert!(check_queue_depletion_velocity_abort(Side::Bid, LotQty(800), LotQty(300), &sweep_bad));
}

#[test]
fn test_v52_preserves_all_constitutional_invariants_scar63_oos520_and_digital_twin() {
    assert_eq!(
        OnlineAdaptiveGovernor::audit_unified_62_losses_and_2160_manifold(),
        (UNIFIED_LOSS_LAB_COUNT, 2_160)
    );
    let oos = run_oos_520_multi_regime_suite();
    assert_eq!(oos.closed_trades, 520);
    assert!(oos.maker_exit_ratio_pct >= 82.0);
    assert!(oos.profit_factor >= 2.0);
    assert_eq!(oos.parity_sync_drift_1e8, 0);
    assert!(oos.wald_v2_zero_loss_escapes >= 20);
    assert!(oos.wald_v2_compressed_sl_closes >= 45);

    let twin = run_omega_digital_twin_walk_forward_lab();
    assert_eq!(twin.closed_trades, 3);
    assert_eq!(twin.hawkes_queue_aborts, 1);
    assert_eq!(twin.post_only_rejects, 0);
    assert_eq!(twin.parity_sync_drift_1e8, 0);
    assert!(twin.cum_net_bps_x100 >= 7_000);
}

#[test]
fn test_v521_trade32_eth5_low_hurst_opposing_wave3_veto_and_idle_ws_quota_100pct() {
    let t0 = 1_795_050_000_000_000_000_i64;
    let mut eth32 = mk_vault_snap(1, 197_200, 3_200, 0.22, 0.45, 0.35, t0);
    eth32.set_wave_observables(WaveObservables {
        regime: WaveRegime::ImpulseWave3,
        climax_active: false,
        hurst: WaveObservables::encode_hurst(0.253),
        macro_phase: BoundedScore::new_clamped(-0.90),
        oi_impulse: BoundedScore::new_clamped(-0.20),
        liq_imbalance: BoundedScore::new_clamped(-0.35),
        fib_proj_bps: ScaledBps(-15_790),
    });
    assert!(is_opposing_impulse_wave3_hazard(Side::Bid, &eth32));
    assert!(!passes_elliott_macro_coherence_guard(Side::Bid, &eth32));
    assert_eq!(
        passes_elastic_geom_and_macro_impulse_guard(&eth32, Side::Bid, 0.0),
        Err(LastVetoCode::MacroElliottContradiction)
    );

    assert!(vortex_rs::bybit_client::BybitV5LiveClient::with_base_url(
        "k".to_owned(),
        "s".to_owned(),
        "https://api.bybit.com",
    )
    .is_some_and(|mut cli| {
        let rtt_us = cli.observe_authenticated_ws_rtt_ns(42_000_000);
        cli.refresh_ws_quota_pulse();
        let (live_rtt_us, live_quota_rem) = vortex_rs::execution_fsm::read_live_ws_pulse();
        rtt_us < 120_000
            && cli.ws_min_available_quota() == 100
            && live_quota_rem == 100
            && live_rtt_us < 120_000
    }));
}

#[test]
fn test_v53_endogenous_tfi_prior_limit_bailout_reconcile_and_bounce_rescue() {
    let fees = FeeSchedule::VIP0_DEFAULT;
    let t0 = 1_795_060_000_000_000_000_i64;

    // (a) Endogenous TFI prior: 500 lots Bid/Ask L1 does not saturate on 2-lot retail print, nor inflate on unilateral 50,000-lot Ask spoof.
    let mut fs_sym = vortex_rs::alpha_3factor::OrthogonalFactorState::new();
    let mut s_sym = mk_vault_snap(5, 7_200, 2_800, 0.0, 0.0, 0.0, t0);
    s_sym.bids[0] = BookLevel { price: PriceTicks(7_200), qty: LotQty(500) };
    s_sym.asks[0] = BookLevel { price: PriceTicks(7_201), qty: LotQty(500) };
    s_sym.refresh_deep_l1_l5_totals();
    fs_sym.on_l2_book_update(&mut s_sym);
    s_sym.ts_local_ns = TimestampNs(t0 + 10_000_000);
    fs_sym.on_trade_print(Side::Bid, LotQty(2), &mut s_sym);
    let tfi_sym = s_sym.factor_tfi.get();
    assert!(tfi_sym > 0.0 && tfi_sym < 0.15);

    let mut fs_spoof = vortex_rs::alpha_3factor::OrthogonalFactorState::new();
    let mut s_spoof = mk_vault_snap(5, 7_200, 2_800, 0.0, 0.0, 0.0, t0);
    s_spoof.bids[0] = BookLevel { price: PriceTicks(7_200), qty: LotQty(500) };
    s_spoof.asks[0] = BookLevel { price: PriceTicks(7_201), qty: LotQty(500) };
    s_spoof.refresh_deep_l1_l5_totals();
    fs_spoof.on_l2_book_update(&mut s_spoof);
    for step in 1..=4_i64 {
        s_spoof.ts_local_ns = TimestampNs(t0 + step * 5_000_000);
        s_spoof.asks[0].qty = LotQty(50_000);
        s_spoof.refresh_deep_l1_l5_totals();
        fs_spoof.on_l2_book_update(&mut s_spoof);
    }
    s_spoof.ts_local_ns = TimestampNs(t0 + 30_000_000);
    fs_spoof.on_trade_print(Side::Bid, LotQty(2), &mut s_spoof);
    let tfi_spoof = s_spoof.factor_tfi.get();
    assert!(tfi_spoof > 0.0 && tfi_spoof < 0.15);
    assert!((tfi_spoof - tfi_sym).abs() < 0.02 && tfi_spoof >= tfi_sym * 0.85);

    // (b) Limit bailout reconcile & soft taker_prob plasticity
    let rec_ctx = OpenExitContext {
        slot_idx: 5,
        side: Side::Bid,
        avg_price_ticks: PriceTicks(7_200),
        peak_favorable_price: PriceTicks(7_200),
        worst_adverse_price: PriceTicks(7_196),
        initial_lots: LotQty(20),
        open_lots: LotQty(20),
        tranche1_harvested: false,
        tranche1_lots: LotQty(0),
        tranche1_exit_price: PriceTicks(0),
        defensive_floor_locked: true,
        active_exit_price: PriceTicks(7_199),
        structural_tp_price: PriceTicks(7_206),
        resting_exit_price: PriceTicks(7_199),
        sl_trigger_price: PriceTicks(7_025),
        opened_ts: TimestampNs(t0),
        learned_mfe_bps: 620,
        learned_t1_permille: 500,
        learned_ir_x100: 20,
        learned_ret_std_bps: 210,
        intrinsic_tau_mkt_s: 180,
        last_eff_sl_ticks: PriceTicks(7_025),
        ratchet_locked_gross_bps: 0,
        last_closed_mfe_bps: 0,
        last_closed_mae_bps: -556,
        active_fees: fees,
        learned_taker_prob_pm: 50,
    };
    let closed_pnl_json = serde_json::json!({
        "retCode": 0,
        "result": {
            "list": [{
                "symbol": "SUIUSDT",
                "orderType": "Limit",
                "side": "Sell",
                "qty": "20",
                "avgEntryPrice": "0.7200",
                "avgExitPrice": "0.7199",
                "closedPnl": "-0.0079",
                "createdTime": "1795060005000",
                "updatedTime": "1795060005000"
            }]
        }
    });
    match vortex_rs::exit_quoter::reconcile_closed_pnl_core(&rec_ctx, &closed_pnl_json, 20, false) {
        vortex_rs::exit_quoter::ClosedPnlQueryOutcome::Verified(res) => {
            assert_eq!(res.kind, ClosedTradeKind::MakerPassiveUnwind);
            assert_ne!(res.kind, ClosedTradeKind::ServerMarkPriceStopLoss);
        }
        other => assert!(matches!(other, vortex_rs::exit_quoter::ClosedPnlQueryOutcome::Verified(_))),
    }

    let mut gp_bail = vortex_rs::governor_plasticity::GovernorPlasticityState::new();
    let mut gp_sl = vortex_rs::governor_plasticity::GovernorPlasticityState::new();
    gp_bail.observe_exit_outcome_with_net_and_mfe(
        5, Side::Bid, false, ScaledBps(-550), ScaledBps(0), 400, 2_000,
    );
    gp_sl.observe_exit_outcome_with_net_and_mfe(
        5, Side::Bid, true, ScaledBps(-2_500), ScaledBps(0), 400, 2_000,
    );
    let p_bail = gp_bail.slot_taker_prob_permille(5, Side::Bid);
    let p_sl = gp_sl.slot_taker_prob_permille(5, Side::Bid);
    assert!(p_bail > 50 && p_bail < p_sl);

    // (c) Dead-cat bounce BBO rescue under exhausted Wald vs healthy Wald
    let zero_loss_px = vortex_rs::execution_fsm::defensive_zero_loss_maker_exit_price_with_fees(
        Side::Bid, PriceTicks(20_000), fees,
    );
    let mut bounce_snap = mk_vault_snap(4, 20_002, 1_800, 0.05, 0.05, 0.05, t0 + 36_000_000_000);
    bounce_snap.bids[0] = BookLevel { price: PriceTicks(20_002), qty: LotQty(600) };
    bounce_snap.asks[0] = BookLevel { price: PriceTicks(20_003), qty: LotQty(600) };
    bounce_snap.mark_price = PriceTicks(20_003);
    bounce_snap.last_trade_price = PriceTicks(20_003);
    let bounce_ctx = OpenExitContext {
        slot_idx: 4,
        side: Side::Bid,
        avg_price_ticks: PriceTicks(20_000),
        peak_favorable_price: PriceTicks(20_003),
        worst_adverse_price: PriceTicks(19_960),
        initial_lots: LotQty(4),
        open_lots: LotQty(4),
        tranche1_harvested: false,
        tranche1_lots: LotQty(0),
        tranche1_exit_price: PriceTicks(0),
        defensive_floor_locked: true,
        active_exit_price: PriceTicks(19_995),
        structural_tp_price: PriceTicks(20_018),
        resting_exit_price: PriceTicks(19_995),
        sl_trigger_price: PriceTicks(19_928),
        opened_ts: TimestampNs(t0),
        learned_mfe_bps: 620,
        learned_t1_permille: 500,
        learned_ir_x100: 20,
        learned_ret_std_bps: 210,
        intrinsic_tau_mkt_s: 180,
        last_eff_sl_ticks: PriceTicks(19_928),
        ratchet_locked_gross_bps: 0,
        last_closed_mfe_bps: 150,
        last_closed_mae_bps: -2_000,
        active_fees: fees,
        learned_taker_prob_pm: 95,
    };
    let kin_bounce = ShadowRunnerKinematics {
        flow_ema_fast: 0.05,
        flow_ema_slow: -0.18,
        in_scar_debt: false,
        chandelier_armed: false,
    };
    let q_exhausted = vortex_rs::exit_quoter::compute_canonical_exit_quote_omega_v52(
        &bounce_ctx, kin_bounce, &bounce_snap, Some(-2.2), 0, 5_000,
    );
    assert!(q_exhausted.is_some_and(|q| {
        q.target_price == PriceTicks(20_003) && q.target_price < zero_loss_px && q.urgent_close_repeg
    }));
    let q_healthy = vortex_rs::exit_quoter::compute_canonical_exit_quote_omega_v52(
        &bounce_ctx, kin_bounce, &bounce_snap, Some(1.5), 0, 5_000,
    );
    assert!(q_healthy.is_some_and(|q| q.target_price >= zero_loss_px));

    // (d) Adverse queue flow C1 frontier tightens when taker_prob_pm rises from 50 to 280
    let mut thr_50 = -1.0_f64;
    let mut thr_280 = -1.0_f64;
    for k in 1..=100_i32 {
        let f = -f64::from(k) * 0.01;
        if thr_50 <= -0.999
            && vortex_rs::exec_daemon::should_abort_queue_on_adverse_flow_c1(f, f, 50, 20, 0.0)
        {
            thr_50 = f;
        }
        if thr_280 <= -0.999
            && vortex_rs::exec_daemon::should_abort_queue_on_adverse_flow_c1(f, f, 280, 20, 0.0)
        {
            thr_280 = f;
        }
    }
    assert!(thr_280 > thr_50);
    let mid_f = f64::midpoint(thr_50, thr_280);
    assert!(!vortex_rs::exec_daemon::should_abort_queue_on_adverse_flow_c1(mid_f, mid_f, 50, 20, 0.0));
    assert!(vortex_rs::exec_daemon::should_abort_queue_on_adverse_flow_c1(mid_f, mid_f, 280, 20, 0.0));
}
