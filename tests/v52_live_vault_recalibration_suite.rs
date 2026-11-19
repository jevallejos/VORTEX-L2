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

#[test]
fn test_v531_thick_book_tfi_front_of_queue_immunity_and_diffusive_ev() {
    let fees = FeeSchedule::VIP0_DEFAULT;
    let t0 = 1_795_080_000_000_000_000_i64;

    // (a) Thick-book TFI (30,000 lots Bid/Ask L1, NEAR / WIF): 10-lot retail print -> TFI < 0.10; 1,800-lot institutional impulse -> TFI > 0.68.
    let near_slot = vortex_rs::types::slot_from_symbol_name("NEARUSDT").unwrap_or(6);
    let wif_slot = vortex_rs::types::slot_from_symbol_name("WIFUSDT").unwrap_or(7);
    for (slot, px_ticks) in [(near_slot, 2_200_i64), (wif_slot, 3_500_i64)] {
        let mut fs_ret = vortex_rs::alpha_3factor::OrthogonalFactorState::new();
        let mut s_ret = mk_vault_snap(slot, px_ticks, 3_200, 0.0, 0.0, 0.0, t0);
        s_ret.bids[0] = BookLevel { price: PriceTicks(px_ticks), qty: LotQty(30_000) };
        s_ret.asks[0] = BookLevel { price: PriceTicks(px_ticks + 1), qty: LotQty(30_000) };
        s_ret.refresh_deep_l1_l5_totals();
        fs_ret.on_l2_book_update(&mut s_ret);
        s_ret.ts_local_ns = TimestampNs(t0 + 10_000_000);
        fs_ret.on_trade_print(Side::Bid, LotQty(10), &mut s_ret);
        let tfi_ret = s_ret.factor_tfi.get();
        assert!(tfi_ret > 0.0 && tfi_ret < 0.10);

        let mut fs_inst = vortex_rs::alpha_3factor::OrthogonalFactorState::new();
        let mut s_inst = mk_vault_snap(slot, px_ticks, 3_200, 0.0, 0.0, 0.0, t0);
        s_inst.bids[0] = BookLevel { price: PriceTicks(px_ticks), qty: LotQty(30_000) };
        s_inst.asks[0] = BookLevel { price: PriceTicks(px_ticks + 1), qty: LotQty(30_000) };
        s_inst.refresh_deep_l1_l5_totals();
        fs_inst.on_l2_book_update(&mut s_inst);
        for k in 1..=36_i64 {
            s_inst.ts_local_ns = TimestampNs(t0 + k * 500_000);
            fs_inst.on_trade_print(Side::Bid, LotQty(50), &mut s_inst);
        }
        let tfi_inst = s_inst.factor_tfi.get();
        assert!(tfi_inst > 0.68);
    }

    // (b) Front-of-queue FIFO immunity (ETH Ask: init_q=4401, rem_q=57, wait=2,131ms, signed_geom=-0.65, signed_ofi=-0.05, signed_slow=0.0)
    // does NOT fire SpoofingVeto, whereas at 170ms with rem_q=4401 it DOES abort; and L1 passive patience avoids 4,357ms truncation.
    let eth_foq = mk_vault_snap(1, 197_600, 2_800, 0.05, 0.0, 0.65, t0 + 2_131_000_000);
    let mut adv_since_foq = 0_i64;
    assert!(!vortex_rs::exec_daemon::evaluate_elastic_spoof_geom_abort_with_queue_v531(
        Side::Ask,
        &eth_foq,
        -0.32,
        2_131_000_000,
        BoundedScore::ZERO,
        0.15,
        1.0,
        50,
        75_000,
        t0 + 2_131_000_000,
        &mut adv_since_foq,
        LotQty(4_401),
        LotQty(57),
    ));
    let mut adv_since_early = 0_i64;
    assert!(vortex_rs::exec_daemon::evaluate_elastic_spoof_geom_abort_with_queue_v531(
        Side::Ask,
        &eth_foq,
        -0.32,
        170_000_000,
        BoundedScore::ZERO,
        0.15,
        1.0,
        50,
        75_000,
        t0 + 170_000_000,
        &mut adv_since_early,
        LotQty(4_401),
        LotQty(4_401),
    ));
    let eth_foq_rev = mk_vault_snap(1, 197_600, 2_800, 0.45, 0.45, 0.65, t0 + 2_131_000_000);
    let mut adv_since_rev = 0_i64;
    assert!(vortex_rs::exec_daemon::evaluate_elastic_spoof_geom_abort_with_queue_v531(
        Side::Ask,
        &eth_foq_rev,
        -0.32,
        2_131_000_000,
        BoundedScore::new_clamped(-0.30),
        0.15,
        1.0,
        50,
        75_000,
        t0 + 2_131_000_000,
        &mut adv_since_rev,
        LotQty(4_401),
        LotQty(57),
    ));

    let mut gov = OnlineAdaptiveGovernor::new();
    gov.seed_prewarm_slot_emas(1, 197_600 * 10_000, 197_600 * 10_000, 14_200, t0);
    let pat_back = gov.elastic_resting_l1_patience_ns_with_queue_v531(
        1,
        Side::Ask,
        ScaledBps(14_200),
        1.618_033_988_749_895,
        1.0,
        BoundedScore::ZERO,
        LotQty(4_401),
        LotQty(4_401),
    );
    let pat_front = gov.elastic_resting_l1_patience_ns_with_queue_v531(
        1,
        Side::Ask,
        ScaledBps(14_200),
        1.618_033_988_749_895,
        1.0,
        BoundedScore::ZERO,
        LotQty(4_401),
        LotQty(57),
    );
    assert!(pat_back > 6_000_000_000 && pat_front > pat_back);

    // (c) evaluate_pilar2_asymmetry_ev_sbps blocks weak geometry (q_struct=0.25, mfe=613, vol_5m=45 bps)
    // and unlocks EV > 0 when q_struct=0.75 and target_bps=1150.
    let eth_spec = APPROVED_SPECS[1];
    assert!(eth_spec
        .evaluate_pilar2_asymmetry_ev_sbps(
            LotQty(1),
            PriceTicks(197_600),
            ScaledBps(4_500),
            613,
            50,
            0.25,
            500,
            500,
            Side::Bid,
            fees,
        )
        .is_none());
    assert!(eth_spec
        .evaluate_pilar2_asymmetry_ev_sbps(
            LotQty(1),
            PriceTicks(197_600),
            ScaledBps(4_500),
            1_150,
            50,
            0.75,
            750,
            500,
            Side::Bid,
            fees,
        )
        .is_some_and(|ev_sbps| ev_sbps > 0));
}

#[test]
fn test_v532_warmup_latch_no_4356ms_truncation_intact_wall_and_mfe_order() {
    let t0 = 1_795_090_000_000_000_000_i64;
    let warmup_until = t0 + 15_000_000_000;

    // (a) Irreversible boot_converged latch: once converged, quiet altcoins (ORDI=slot 6, WIF=slot 8)
    // decaying to factor_tfi == 0.0 (and BTC SNR passing through 0.0) NEVER re-trigger WARMUP_LOCK;
    // only a real SHM drop (hb_age > 5s) or an uninitialized slot (seq==0 / bid==0) blocks.
    let mut rt = vortex_rs::exec_gateway_handlers::ExecRuntimeState::new();
    assert!(!rt.boot_converged);
    let mut gov_boot = OnlineAdaptiveGovernor::new();
    let mut snaps = [SymbolLobSnapshot::new_empty(); vortex_rs::lob_shm::MAX_ACTIVE_PAIRS];
    for (s_idx, snap_slot) in snaps.iter_mut().enumerate() {
        *snap_slot = mk_vault_snap(s_idx, 10_000 + (s_idx as i64) * 100, 2_950, 0.35, 0.40, 0.25, warmup_until + 1_000_000_000);
        gov_boot.seed_prewarm_slot_emas(
            s_idx,
            (10_000 + (s_idx as i64) * 100) * 10_000,
            (10_000 + (s_idx as i64) * 100) * 10_000,
            2_950,
            t0,
        );
    }
    // Before warmup floor: gate blocks and latch remains false.
    assert!(!vortex_rs::exec_daemon::evaluate_live_boot_convergence_with_latch(
        &mut rt.boot_converged,
        1,
        TimestampNs(t0 + 5_000_000_000),
        warmup_until,
        &snaps,
        &gov_boot,
        69_700,
        100,
        Some(50_000_000),
    ));
    assert!(!rt.boot_converged);

    // After warmup floor with live L2 & WS pulse (69.7ms, quota 100): gate passes and latches irreversibly.
    let t_live = TimestampNs(warmup_until + 1_000_000_000);
    assert!(vortex_rs::exec_daemon::evaluate_live_boot_convergence_with_latch(
        &mut rt.boot_converged,
        1,
        t_live,
        warmup_until,
        &snaps,
        &gov_boot,
        69_700,
        100,
        Some(50_000_000),
    ));
    assert!(rt.boot_converged);

    // 4h42m later: ORDI (slot 6) & WIF (slot 8) decay to TFI=0.00 and BTC SNR passes through 0.00 -> still converged!
    let t_4h42m = TimestampNs(t_live.0 + (4 * 3_600 + 42 * 60) * 1_000_000_000);
    snaps[6].factor_tfi = BoundedScore::ZERO;
    snaps[6].cum_buy_trade_lots = LotQty(0);
    snaps[6].cum_sell_trade_lots = LotQty(0);
    snaps[8].factor_tfi = BoundedScore::ZERO;
    snaps[8].cum_buy_trade_lots = LotQty(0);
    snaps[8].cum_sell_trade_lots = LotQty(0);
    snaps[0].factor_ofi = BoundedScore::ZERO;
    snaps[0].factor_tfi = BoundedScore::ZERO;
    snaps[0].factor_geom = BoundedScore::ZERO;
    for s_idx in 0..vortex_rs::lob_shm::MAX_ACTIVE_PAIRS {
        assert!(vortex_rs::exec_daemon::evaluate_live_boot_convergence_with_latch(
            &mut rt.boot_converged,
            s_idx,
            t_4h42m,
            warmup_until,
            &snaps,
            &gov_boot,
            69_700,
            100,
            Some(25_000_000),
        ));
    }
    assert!(rt.boot_converged);
    // Real SHM drop (hb_age > 5s) blocks safely; uninitialized slot blocks only that slot.
    assert!(!vortex_rs::exec_daemon::evaluate_live_boot_convergence_with_latch(
        &mut rt.boot_converged,
        1,
        t_4h42m,
        warmup_until,
        &snaps,
        &gov_boot,
        69_700,
        100,
        Some(5_500_000_000),
    ));
    snaps[11].bids[0].price = PriceTicks(0);
    assert!(!vortex_rs::exec_daemon::evaluate_live_boot_convergence_with_latch(
        &mut rt.boot_converged,
        11,
        t_4h42m,
        warmup_until,
        &snaps,
        &gov_boot,
        69_700,
        100,
        Some(25_000_000),
    ));
    assert!(vortex_rs::exec_daemon::evaluate_live_boot_convergence_with_latch(
        &mut rt.boot_converged,
        1,
        t_4h42m,
        warmup_until,
        &snaps,
        &gov_boot,
        69_700,
        100,
        Some(25_000_000),
    ));

    // (b) No rigid 4,357 ms truncation for ETH Ask (10801 -> 5889 lots) & PENDLE Ask (147 -> 147 lots)
    // under healthy slow flow (even when !at_l1 ticks 1 tick away), while adverse flow / Hawkes still aborts fast;
    // and intact L1 wall (ETH Ask 13,979/13,979 at 872 ms) + neutral flow does NOT fire false SpoofingVeto.
    let mut gov_q = OnlineAdaptiveGovernor::new();
    gov_q.seed_prewarm_slot_emas(1, 176_285 * 10_000, 176_285 * 10_000, 2_926, t0);
    gov_q.seed_prewarm_slot_emas(7, 14_360 * 10_000, 14_360 * 10_000, 3_630, t0);
    let eth_base_chase = gov_q.max_total_entry_chase_ns(1, ScaledBps(2_200));
    let pendle_base_chase = gov_q.max_total_entry_chase_ns(7, ScaledBps(2_600));
    let wait_4357_ns = 4_357_000_000_i64;
    for at_l1 in [true, false] {
        let (eth_max_ns, eth_expired) = vortex_rs::exec_daemon::evaluate_resting_queue_chase_budget_v532(
            &gov_q,
            1,
            Side::Ask,
            ScaledBps(2_200),
            1.0,
            1.0,
            BoundedScore::new_clamped(0.05),
            LotQty(10_801),
            LotQty(5_889),
            eth_base_chase,
            at_l1,
            wait_4357_ns,
            wait_4357_ns,
        );
        assert!(eth_max_ns >= 10_000_000_000 && !eth_expired);

        let (pendle_max_ns, pendle_expired) = vortex_rs::exec_daemon::evaluate_resting_queue_chase_budget_v532(
            &gov_q,
            7,
            Side::Ask,
            ScaledBps(2_600),
            1.0,
            1.0,
            BoundedScore::new_clamped(0.0),
            LotQty(147),
            LotQty(147),
            pendle_base_chase,
            at_l1,
            wait_4357_ns,
            wait_4357_ns,
        );
        assert!(pendle_max_ns >= 10_000_000_000 && !pendle_expired);
    }
    // Adverse slow flow + off-L1 STILL expires quickly at 4,357 ms.
    let (_, adv_off_l1_expired) = vortex_rs::exec_daemon::evaluate_resting_queue_chase_budget_v532(
        &gov_q,
        1,
        Side::Ask,
        ScaledBps(2_200),
        1.4,
        3.5,
        BoundedScore::new_clamped(-0.45),
        LotQty(10_801),
        LotQty(5_889),
        eth_base_chase,
        false,
        wait_4357_ns,
        wait_4357_ns,
    );
    assert!(adv_off_l1_expired);

    // Intact L1 wall (13,979 / 13,979 at 872 ms) + neutral flow does NOT fire false SpoofingVeto,
    // while V06 (170 ms) and mid-queue wall collapse (60% remaining) STILL fire SpoofingVeto.
    let eth_intact = mk_vault_snap(1, 176_285, 2_400, 0.05, 0.0, 0.65, t0 + 872_000_000);
    let mut adv_intact = 0_i64;
    assert!(!vortex_rs::exec_daemon::evaluate_elastic_spoof_geom_abort_with_queue_v531(
        Side::Ask,
        &eth_intact,
        -0.30,
        872_000_000,
        BoundedScore::ZERO,
        0.15,
        1.0,
        50,
        69_700,
        t0 + 872_000_000,
        &mut adv_intact,
        LotQty(13_979),
        LotQty(13_979),
    ));
    let mut adv_v06 = 0_i64;
    assert!(vortex_rs::exec_daemon::evaluate_elastic_spoof_geom_abort_with_queue_v531(
        Side::Ask,
        &eth_intact,
        -0.30,
        170_000_000,
        BoundedScore::ZERO,
        0.15,
        1.0,
        50,
        69_700,
        t0 + 170_000_000,
        &mut adv_v06,
        LotQty(13_979),
        LotQty(13_979),
    ));
    let mut adv_mid_collapse = 0_i64;
    assert!(vortex_rs::exec_daemon::evaluate_elastic_spoof_geom_abort_with_queue_v531(
        Side::Ask,
        &eth_intact,
        -0.30,
        872_000_000,
        BoundedScore::ZERO,
        0.15,
        1.0,
        50,
        69_700,
        t0 + 872_000_000,
        &mut adv_mid_collapse,
        LotQty(13_979),
        LotQty(8_400),
    ));

    // (c) Boot MFE order: rehydrate_cold_mfe_seeds_from_scar_vault executed AFTER hydrate_12_slot_mfe_from_path
    // preserves rehydrated > 613 values instead of overwriting them back to 613.
    let mut engines = SlotExecutionEngine::new_12_slot_array();
    let mfe_vault = std::path::Path::new(vortex_rs::vault_io::MFE_PLASTICITY_VAULT_PATH);
    SlotExecutionEngine::hydrate_12_slot_mfe_from_path(&mut engines, mfe_vault);
    engines[2].learned_mfe_bps = vortex_rs::telemetry_hud::DEFAULT_LEARNED_MFE_SCALED_BPS;
    engines[7].learned_mfe_bps = vortex_rs::telemetry_hud::DEFAULT_LEARNED_MFE_SCALED_BPS;
    engines[1].learned_mfe_bps = 647;
    let scar_fixture = concat!(
        "{\"symbol\":\"SOLUSDT\",\"maker_exit\":true,\"mfe_bps\":8.40}\n",
        "{\"symbol\":\"SOLUSDT\",\"maker_exit\":true,\"mfe_bps\":9.10}\n",
        "{\"symbol\":\"PENDLEUSDT\",\"maker_exit\":true,\"mfe_bps\":7.85}\n",
        "{\"symbol\":\"ETHUSDT\",\"maker_exit\":true,\"mfe_bps\":12.00}\n"
    );
    vortex_rs::telemetry_hud::rehydrate_cold_mfe_seeds_from_reader(
        &mut engines,
        std::io::Cursor::new(scar_fixture.as_bytes()),
    );
    assert!(engines[2].learned_mfe_bps > vortex_rs::telemetry_hud::DEFAULT_LEARNED_MFE_SCALED_BPS);
    assert!(engines[7].learned_mfe_bps > vortex_rs::telemetry_hud::DEFAULT_LEARNED_MFE_SCALED_BPS);
    assert!(engines[2].learned_mfe_bps <= vortex_rs::telemetry_hud::SCAR_MFE_REHYDRATE_CAP_SCALED_BPS);
    assert_eq!(engines[1].learned_mfe_bps, 647);

    // Also verify live vault read-only order if vault files exist on disk (zero temp files created).
    let scar_vault = std::path::Path::new(vortex_rs::telemetry_hud::SCAR_MEMORY_VAULT_PATH);
    if mfe_vault.exists() && scar_vault.exists() {
        let mut live_eng = SlotExecutionEngine::new_12_slot_array();
        SlotExecutionEngine::hydrate_12_slot_mfe_from_path(&mut live_eng, mfe_vault);
        let before = SlotExecutionEngine::extract_12_slot_mfe_array(&live_eng);
        vortex_rs::telemetry_hud::rehydrate_cold_mfe_seeds_from_scar_vault(&mut live_eng, scar_vault);
        let after = SlotExecutionEngine::extract_12_slot_mfe_array(&live_eng);
        let mut rescued_sunk = 0_usize;
        for s_idx in 0..vortex_rs::lob_shm::MAX_ACTIVE_PAIRS {
            let active_fee_rt_mkr = live_eng[s_idx].active_maker_rt_fee_bps().max(100);
            let sunk_floor = (vortex_rs::alpha_3factor::PHI * (active_fee_rt_mkr as f64)).round() as i64;
            if before[s_idx] != vortex_rs::telemetry_hud::DEFAULT_LEARNED_MFE_SCALED_BPS
                && before[s_idx] >= sunk_floor
            {
                assert_eq!(after[s_idx], before[s_idx]);
            } else {
                assert!(
                    after[s_idx] >= before[s_idx]
                        && after[s_idx] <= vortex_rs::telemetry_hud::SCAR_MFE_REHYDRATE_CAP_SCALED_BPS
                );
                if after[s_idx] > before[s_idx] {
                    rescued_sunk += 1;
                }
            }
        }
        assert!(rescued_sunk >= 1);
    }
}

#[test]
fn test_v54_eth2_counterfactual_elastic_bailout_non_censored_mfe_and_epps_sync() {
    record_live_ws_pulse(69_700, 100);
    let fees = FeeSchedule::VIP0_DEFAULT;
    let t0 = 1_795_100_000_000_000_000_i64;
    let w_65 = 6_468_940_634_i64;

    // =========================================================================
    // (a) Counterfactual V5.3.2 #2 ETHUSDT Ask @ 177485 (1 lot, ofi=-0.22, btc=-0.14):
    //     1) Ex-ante EV is NOT inflated to +9.93 bps by crash volatility alone -> vetoed (EV <= 0),
    //        while real live wins ETH #15 (ofi=+0.42, btc=+0.36) & ETH #16 (ofi=+0.22, btc=+0.58) stay EV > 0.
    //     2) Ex-post Maker Bailout: SUI #4 at -1.39 bps (7137t) and bounce +2.80 bps (7140t) activates 100%
    //        maker_bailout_active, whereas ETH #2 deep wick to -18.59 bps (177485 -> 177815 at 38 s)
    //        attenuates maker_bailout_active to false via S_3(cur_gross; PHI*b_floor, b_floor) and refuses
    //        to liquidate voluntarily at the bottom of the wick.
    // =========================================================================
    let eth_spec = APPROVED_SPECS[1];
    assert!(eth_spec
        .evaluate_pilar2_asymmetry_ev_with_flow_sbps(
            LotQty(1),
            PriceTicks(177_485),
            ScaledBps(5_800),
            647,
            50,
            0.92,
            509,
            532,
            Side::Ask,
            fees,
            0.22,
            0.14,
        )
        .is_none());

    let mut gov_eth2 = OnlineAdaptiveGovernor::new();
    gov_eth2.seed_prewarm_slot_emas(1, 177_485 * 10_000, 177_560 * 10_000, 2_926, t0);
    let mut btc_weak = mk_vault_snap(0, 645_000, 2_800, -0.14, -0.14, -0.14, t0);
    btc_weak.bids[0].qty = LotQty(2_500);
    btc_weak.asks[0].qty = LotQty(2_500);
    btc_weak.refresh_deep_l1_l5_totals();
    let mut eth2_in = mk_vault_snap(1, 177_484, 5_800, -0.22, -0.62, -0.92, t0);
    eth2_in.bids[0] = BookLevel { price: PriceTicks(177_484), qty: LotQty(4_200) };
    eth2_in.asks[0] = BookLevel { price: PriceTicks(177_485), qty: LotQty(5_100) };
    eth2_in.refresh_deep_l1_l5_totals();
    eth2_in.ob_bull_efficacy_pm = 509;
    eth2_in.ob_bear_efficacy_pm = 532;
    eth2_in.ob_bear_score = BoundedScore::new_clamped(0.88);
    eth2_in.ob_dist_bear_bps = ScaledBps(850);
    eth2_in.set_wave_observables(WaveObservables {
        regime: WaveRegime::ImpulseWave3,
        climax_active: false,
        hurst: WaveObservables::encode_hurst(0.64),
        macro_phase: BoundedScore::new_clamped(-0.82),
        oi_impulse: BoundedScore::ZERO,
        liq_imbalance: BoundedScore::ZERO,
        fib_proj_bps: ScaledBps(-3_200),
    });
    assert!(gov_eth2
        .evaluate_slot_pilar2_ex_ante_ev_with_leader_sbps(
            1,
            Side::Ask,
            LotQty(1),
            PriceTicks(177_485),
            ScaledBps(647),
            &eth2_in,
            &btc_weak,
            1.0,
        )
        .is_none());
    assert_eq!(
        gov_eth2.authorizes_adaptive_entry_v54(
            1,
            Side::Ask,
            w_65,
            1_000,
            ScaledBps(647),
            &eth2_in,
            &btc_weak,
            0.0,
            1.0,
            0.85,
        ),
        Err(LastVetoCode::QueueDepthOrOversizedLot)
    );

    // ETH #15 (ofi=+0.42, btc=+0.36) & ETH #16 (ofi=+0.22, btc=+0.58) 1-lot wins remain EV > 0:
    assert!(eth_spec
        .evaluate_pilar2_asymmetry_ev_with_flow_sbps(
            LotQty(1),
            PriceTicks(182_033),
            ScaledBps(2_991),
            670,
            46,
            0.96,
            457,
            471,
            Side::Bid,
            fees,
            0.42,
            0.36,
        )
        .is_some_and(|ev| ev > 0));
    assert!(eth_spec
        .evaluate_pilar2_asymmetry_ev_with_flow_sbps(
            LotQty(1),
            PriceTicks(183_268),
            ScaledBps(1_507),
            670,
            44,
            0.99,
            448,
            471,
            Side::Bid,
            fees,
            0.22,
            0.58,
        )
        .is_some_and(|ev| ev > 0));

    // Ex-post Maker Bailout: SUI #4 (entry 7138t) at -1.39 bps (Ask=7137t) & bounce +2.80 bps (Ask=7140t) -> active!
    let mut sui4_dip = mk_vault_snap(5, 7_136, 5_227, -0.42, -0.50, -0.25, t0 + 14_000_000_000);
    sui4_dip.bids[0] = BookLevel { price: PriceTicks(7_136), qty: LotQty(700) };
    sui4_dip.asks[0] = BookLevel { price: PriceTicks(7_137), qty: LotQty(550) };
    sui4_dip.mark_price = PriceTicks(7_136);
    sui4_dip.set_wave_observables(WaveObservables {
        regime: WaveRegime::ImpulseWave3,
        climax_active: false,
        hurst: WaveObservables::encode_hurst(0.756),
        macro_phase: BoundedScore::new_clamped(-0.95),
        oi_impulse: BoundedScore::new_clamped(-0.25),
        liq_imbalance: BoundedScore::ZERO,
        fib_proj_bps: ScaledBps(-5_753),
    });
    let sui4_ctx = OpenExitContext {
        slot_idx: 5,
        side: Side::Bid,
        avg_price_ticks: PriceTicks(7_138),
        peak_favorable_price: PriceTicks(7_140),
        worst_adverse_price: PriceTicks(7_136),
        initial_lots: LotQty(20),
        open_lots: LotQty(20),
        tranche1_harvested: false,
        tranche1_lots: LotQty(0),
        tranche1_exit_price: PriceTicks(0),
        defensive_floor_locked: true,
        active_exit_price: PriceTicks(7_142),
        structural_tp_price: PriceTicks(7_144),
        resting_exit_price: PriceTicks(7_142),
        sl_trigger_price: PriceTicks(7_020),
        opened_ts: TimestampNs(t0),
        learned_mfe_bps: 778,
        learned_t1_permille: 500,
        learned_ir_x100: 20,
        learned_ret_std_bps: 210,
        intrinsic_tau_mkt_s: 180,
        last_eff_sl_ticks: PriceTicks(7_020),
        ratchet_locked_gross_bps: 0,
        last_closed_mfe_bps: 280,
        last_closed_mae_bps: -280,
        active_fees: fees,
        learned_taker_prob_pm: 48,
    };
    let kin_sui4 = ShadowRunnerKinematics {
        flow_ema_fast: -0.45,
        flow_ema_slow: -0.40,
        in_scar_debt: false,
        chandelier_armed: false,
    };
    let q_sui4_dip = vortex_rs::exit_quoter::compute_canonical_exit_quote_omega_v54(
        &sui4_ctx,
        kin_sui4,
        &sui4_dip,
        ScaledBps(5_227),
        Some(-2.0),
        0,
        69_700,
    );
    assert!(q_sui4_dip.is_some_and(|q| q.target_price == PriceTicks(7_137) && q.urgent_close_repeg));

    let mut sui4_bounce = sui4_dip;
    sui4_bounce.bids[0] = BookLevel { price: PriceTicks(7_139), qty: LotQty(700) };
    sui4_bounce.asks[0] = BookLevel { price: PriceTicks(7_140), qty: LotQty(550) };
    sui4_bounce.mark_price = PriceTicks(7_140);
    let q_sui4_bounce = vortex_rs::exit_quoter::compute_canonical_exit_quote_omega_v54(
        &sui4_ctx,
        kin_sui4,
        &sui4_bounce,
        ScaledBps(5_227),
        Some(-2.0),
        0,
        69_700,
    );
    assert!(q_sui4_bounce.is_some_and(|q| q.target_price == PriceTicks(7_140) && q.urgent_close_repeg));

    // ETHUSDT Ask #2 @ 177485 -> 177815 (-18.59 bps in 38 s): deep wick under PHI * b_floor_sbps -> NO panic Limit bailout at 177815!
    let eth2_zero_loss_px = vortex_rs::execution_fsm::defensive_zero_loss_maker_exit_price_with_fees(
        Side::Ask,
        PriceTicks(177_485),
        fees,
    );
    let mut eth2_wick = mk_vault_snap(1, 177_815, 2_980, 0.48, 0.55, 0.35, t0 + 38_000_000_000);
    eth2_wick.bids[0] = BookLevel { price: PriceTicks(177_815), qty: LotQty(3_500) };
    eth2_wick.asks[0] = BookLevel { price: PriceTicks(177_816), qty: LotQty(3_500) };
    eth2_wick.mark_price = PriceTicks(177_815);
    eth2_wick.last_trade_price = PriceTicks(177_815);
    eth2_wick.set_wave_observables(WaveObservables {
        regime: WaveRegime::ImpulseWave3,
        climax_active: false,
        hurst: WaveObservables::encode_hurst(0.72),
        macro_phase: BoundedScore::new_clamped(0.92),
        oi_impulse: BoundedScore::new_clamped(0.30),
        liq_imbalance: BoundedScore::ZERO,
        fib_proj_bps: ScaledBps(4_800),
    });
    let eth2_ctx = OpenExitContext {
        slot_idx: 1,
        side: Side::Ask,
        avg_price_ticks: PriceTicks(177_485),
        peak_favorable_price: PriceTicks(177_480),
        worst_adverse_price: PriceTicks(177_816),
        initial_lots: LotQty(1),
        open_lots: LotQty(1),
        tranche1_harvested: false,
        tranche1_lots: LotQty(0),
        tranche1_exit_price: PriceTicks(0),
        defensive_floor_locked: true,
        active_exit_price: eth2_zero_loss_px,
        structural_tp_price: PriceTicks(176_350),
        resting_exit_price: eth2_zero_loss_px,
        sl_trigger_price: PriceTicks(178_195),
        opened_ts: TimestampNs(t0),
        learned_mfe_bps: 647,
        learned_t1_permille: 500,
        learned_ir_x100: 20,
        learned_ret_std_bps: 220,
        intrinsic_tau_mkt_s: 180,
        last_eff_sl_ticks: PriceTicks(178_195),
        ratchet_locked_gross_bps: 0,
        last_closed_mfe_bps: 28,
        last_closed_mae_bps: -1_859,
        active_fees: fees,
        learned_taker_prob_pm: 50,
    };
    let kin_eth2_wick = ShadowRunnerKinematics {
        flow_ema_fast: 0.48,
        flow_ema_slow: 0.42,
        in_scar_debt: false,
        chandelier_armed: false,
    };
    let q_eth2_wick = vortex_rs::exit_quoter::compute_canonical_exit_quote_omega_v54(
        &eth2_ctx,
        kin_eth2_wick,
        &eth2_wick,
        ScaledBps(2_980),
        Some(-2.6),
        0,
        69_700,
    );
    assert!(q_eth2_wick.is_some_and(|q| {
        q.target_price <= eth2_zero_loss_px && q.target_price < PriceTicks(177_815)
    }));

    // =========================================================================
    // (b) Right-censored learned_mfe_bps non-contraction (LINKUSDT #1: 805 stays 805 on +5.01/+5.42 bps
    //     Maker win with 92.4% capture >= INV_PHI), uncapped upward expansion on higher excursion / markout,
    //     and smooth offensive Tranche 1 + Runner anti-fee-drag scaling.
    // =========================================================================
    let mut eng_link = SlotExecutionEngine::new(9);
    eng_link.learned_mfe_bps = 805;
    eng_link.open_vol_5m_bps = ScaledBps(2_200);
    eng_link.flow_ema_fast = 0.12;
    eng_link.last_closed_mfe_bps = 542;
    eng_link.last_exit_sub_reason = ExitSubReason::LvdbsDeepVacuumSurf;
    eng_link.observe_censored_close_mfe_plasticity(Side::Bid, 501, true);
    assert_eq!(eng_link.learned_mfe_bps, 805);
    eng_link.observe_mfe_sample_uncensored(501, 542, true, false, 400);
    assert_eq!(eng_link.learned_mfe_bps, 805);

    // Post-exit SlotMarkoutRing peak > learned_mfe_bps expands learned_mfe_bps upward without 820 ceiling:
    let mut markout_ring = vortex_rs::markout_tracker::SlotMarkoutRing::new();
    markout_ring.arm_fill_probe(Side::Bid, PriceTicks(9_218), PriceTicks(9_218), TimestampNs(t0));
    markout_ring.step_clock(PriceTicks(9_335), TimestampNs(t0 + 61_000_000_000), 200);
    let uncensored_peak = markout_ring.take_uncensored_fill_peak_sbps().unwrap_or(0);
    assert!(uncensored_peak > 1_200);
    eng_link.observe_mfe_sample_uncensored(501, 542_i64.max(uncensored_peak), true, false, 400);
    assert!(eng_link.learned_mfe_bps > 900);

    // Exhausted close that failed to reach TP (capture < INV_PHI) still decays smoothly:
    let before_decay = eng_link.learned_mfe_bps;
    eng_link.observe_mfe_sample_uncensored(410, 760, true, false, 400);
    assert!(eng_link.learned_mfe_bps < before_decay);

    // Divisible high-propulsion high-vol position expands Tranche 1 above static floor to compress fee drag:
    let mut link_prop_snap = mk_vault_snap(9, 9_222, 5_600, 0.72, 0.78, 0.65, t0 + 8_000_000_000);
    link_prop_snap.set_wave_observables(WaveObservables {
        regime: WaveRegime::ImpulseWave3,
        climax_active: false,
        hurst: WaveObservables::encode_hurst(0.68),
        macro_phase: BoundedScore::new_clamped(0.75),
        oi_impulse: BoundedScore::new_clamped(0.25),
        liq_imbalance: BoundedScore::new_clamped(0.30),
        fib_proj_bps: ScaledBps(3_200),
    });
    let mut link_t1_ctx = sui4_ctx;
    link_t1_ctx.slot_idx = 9;
    link_t1_ctx.avg_price_ticks = PriceTicks(9_218);
    link_t1_ctx.peak_favorable_price = PriceTicks(9_222);
    link_t1_ctx.worst_adverse_price = PriceTicks(9_218);
    link_t1_ctx.initial_lots = LotQty(24);
    link_t1_ctx.open_lots = LotQty(24);
    link_t1_ctx.defensive_floor_locked = false;
    link_t1_ctx.learned_mfe_bps = 1_150;
    link_t1_ctx.structural_tp_price = PriceTicks(9_232);
    let kin_link_prop = ShadowRunnerKinematics {
        flow_ema_fast: 0.74,
        flow_ema_slow: 0.52,
        in_scar_debt: false,
        chandelier_armed: false,
    };
    let q_link_t1 = vortex_rs::exit_quoter::compute_canonical_exit_quote_omega_v54(
        &link_t1_ctx,
        kin_link_prop,
        &link_prop_snap,
        ScaledBps(5_600),
        None,
        0,
        69_700,
    );
    assert!(q_link_t1.is_some_and(|q| {
        q.role == ExitOrderRole::Tranche1Harvest50 && q.target_price > PriceTicks(9_224)
    }));

    // =========================================================================
    // (c) Self-learned Epps + BTC synchronized breakout in CorrectiveAbc:
    //     High |epps_corr|, aligned BTC, strong OFI + TFI, and NO opposing OB wall unlocks CorrectiveAbc,
    //     while an opposing OB wall OR any of the 6 historical CorrectiveAbc range losses
    //     (#18, #19, #22, #23, #27, #30) and ETH #2 remain 100% blocked.
    // =========================================================================
    let mut btc_sync = mk_vault_snap(0, 658_000, 3_600, 0.82, 0.86, 0.78, t0);
    btc_sync.bids[0].qty = LotQty(3_200);
    btc_sync.asks[0].qty = LotQty(1_800);
    btc_sync.refresh_deep_l1_l5_totals();
    let mut tao_sync = mk_vault_snap(4, 19_650, 3_800, 0.74, 0.78, 0.62, t0);
    tao_sync.ob_bull_efficacy_pm = 560;
    tao_sync.ob_bear_efficacy_pm = 530;
    tao_sync.ob_bull_score = BoundedScore::new_clamped(0.68);
    tao_sync.ob_dist_bull_bps = ScaledBps(2_400);
    tao_sync.ob_bear_score = BoundedScore::ZERO;
    tao_sync.ob_dist_bear_bps = ScaledBps(0);
    tao_sync.set_wave_observables(WaveObservables {
        regime: WaveRegime::CorrectiveAbc,
        climax_active: false,
        hurst: WaveObservables::encode_hurst(0.72),
        macro_phase: BoundedScore::new_clamped(-0.35),
        oi_impulse: BoundedScore::ZERO,
        liq_imbalance: BoundedScore::ZERO,
        fib_proj_bps: ScaledBps(0),
    });
    // Without Epps+BTC sync: blocked by slow ZigZag CorrectiveAbc guard!
    assert!(!passes_corrective_abc_snapshot_guard_c1(Side::Bid, &tao_sync));
    // With strong Epps (0.86) + BTC leader sync (+0.82) + OFI/TFI >= 0.74 and no opposing Bear OB wall: UNLOCKED!
    let btc_al = vortex_rs::alpha_3factor::OrthogonalFactorState::snr_composite_score(&btc_sync);
    assert!(vortex_rs::alpha_3factor::passes_corrective_abc_snapshot_guard_with_sync_c1(
        Side::Bid, &tao_sync, 0.86, btc_al,
    ));
    assert!(vortex_rs::execution_fsm::passes_elliott_macro_coherence_guard_with_sync(
        Side::Bid, &tao_sync, &btc_sync, 0.86,
    ));
    assert!(matches!(
        vortex_rs::alpha_3factor::OrthogonalFactorState::evaluate_entry_signal_v54(
            APPROVED_SPECS[4],
            &tao_sync,
            &btc_sync,
            ScaledBps(720),
            ScaledBps(150),
            50,
            fees,
            0.0,
            0.95,
            0.86,
        ),
        vortex_rs::alpha_3factor::SignalVerdict::AuthorizeMakerEntry { side: Side::Bid, .. }
    ));

    // Adding an opposing Bear OB wall immediately cancels delta_h_sync via (1 - s_opp_ob_wall):
    let mut tao_opp_wall = tao_sync;
    tao_opp_wall.ob_bear_score = BoundedScore::new_clamped(0.88);
    tao_opp_wall.ob_dist_bear_bps = ScaledBps(650);
    assert!(!vortex_rs::alpha_3factor::passes_corrective_abc_snapshot_guard_with_sync_c1(
        Side::Bid,
        &tao_opp_wall,
        0.86,
        btc_al,
    ));

    // All 6 historical CorrectiveAbc range losses (#18, #19, #22/V5.0#3, #23/V5.0#4, #27/V5.0#8, #30/V5.1#3) + ETH #2 stay 100% blocked:
    #[derive(Copy, Clone, Debug)]
    struct HistAbcLoss {
        slot: usize,
        px: i64,
        vol: i64,
        ofi: f64,
        tfi: f64,
        geom: f64,
        btc_c: f64,
        side: Side,
        h_val: f64,
        m_ph: f64,
        bull_pm: u16,
        bear_pm: u16,
        opp_sc: f64,
        opp_dist: i64,
    }
    let hist_abc_losses = [
        // #18 ETH Bid: btc_comp=0.10, H=0.696, macro=+0.45 (weak geom=0.09 + Bear OB wall 55.7bp sc=0.50)
        HistAbcLoss { slot: 1, px: 179_267, vol: 1_725, ofi: 0.57, tfi: 0.93, geom: 0.09, btc_c: 0.10, side: Side::Bid, h_val: 0.696, m_ph: 0.45, bull_pm: 438, bear_pm: 415, opp_sc: 0.50, opp_dist: 5_572 },
        // #19 SUI Ask: btc_comp=-0.55, H=0.748, macro=-0.45, Bull OB wall 96.7bp sc=0.57
        HistAbcLoss { slot: 5, px: 7_316, vol: 1_582, ofi: -0.75, tfi: -0.84, geom: -0.17, btc_c: -0.55, side: Side::Ask, h_val: 0.748, m_ph: -0.45, bull_pm: 522, bear_pm: 478, opp_sc: 0.57, opp_dist: 9_670 },
        // #22 (V5.0 #3) AVAX Ask: btc_comp=-0.21, H=0.637, macro=-0.45, Bull OB 39.1bp sc=0.60
        HistAbcLoss { slot: 3, px: 7_156, vol: 1_873, ofi: -0.49, tfi: -0.95, geom: -0.20, btc_c: -0.21, side: Side::Ask, h_val: 0.637, m_ph: -0.45, bull_pm: 465, bear_pm: 471, opp_sc: 0.60, opp_dist: 3_908 },
        // #23 (V5.0 #4) TAO Bid: btc_comp=+0.36, H=0.729, macro=-0.30, Bear OB 49.8bp sc=0.44
        HistAbcLoss { slot: 4, px: 19_691, vol: 2_428, ofi: 0.65, tfi: 0.86, geom: 0.36, btc_c: 0.36, side: Side::Bid, h_val: 0.729, m_ph: -0.30, bull_pm: 564, bear_pm: 516, opp_sc: 0.44, opp_dist: 4_984 },
        // #27 (V5.0 #8) ETH Bid: btc_comp=+0.43, ofi=0.48, H=0.715, macro=+0.45, Bear OB 63.4bp sc=0.39
        HistAbcLoss { slot: 1, px: 179_552, vol: 1_890, ofi: 0.48, tfi: 0.84, geom: 0.93, btc_c: 0.43, side: Side::Bid, h_val: 0.715, m_ph: 0.45, bull_pm: 443, bear_pm: 441, opp_sc: 0.39, opp_dist: 6_337 },
        // #30 (V5.1 #3) TAO Ask: btc_comp=-0.32, H=0.744, macro=-0.45, Bull OB 88.6bp sc=0.64
        HistAbcLoss { slot: 4, px: 19_038, vol: 3_023, ofi: -0.72, tfi: -0.67, geom: -0.27, btc_c: -0.32, side: Side::Ask, h_val: 0.744, m_ph: -0.45, bull_pm: 476, bear_pm: 548, opp_sc: 0.64, opp_dist: 8_861 },
        // V5.3.2 #2 ETH Ask @ 177485: ofi=-0.22, btc_comp=-0.14
        HistAbcLoss { slot: 1, px: 177_485, vol: 5_800, ofi: -0.22, tfi: -0.62, geom: -0.92, btc_c: -0.14, side: Side::Ask, h_val: 0.700, m_ph: -0.35, bull_pm: 509, bear_pm: 532, opp_sc: 0.67, opp_dist: 1_133 },
    ];
    for c in hist_abc_losses {
        let mut s_loss = mk_vault_snap(c.slot, c.px, c.vol, c.ofi, c.tfi, c.geom, t0);
        s_loss.ob_bull_efficacy_pm = i64::from(c.bull_pm);
        s_loss.ob_bear_efficacy_pm = i64::from(c.bear_pm);
        match c.side {
            Side::Bid => {
                s_loss.ob_bear_score = BoundedScore::new_clamped(c.opp_sc);
                s_loss.ob_dist_bear_bps = ScaledBps(c.opp_dist);
            }
            Side::Ask => {
                s_loss.ob_bull_score = BoundedScore::new_clamped(c.opp_sc);
                s_loss.ob_dist_bull_bps = ScaledBps(c.opp_dist);
            }
        }
        s_loss.set_wave_observables(WaveObservables {
            regime: WaveRegime::CorrectiveAbc,
            climax_active: false,
            hurst: WaveObservables::encode_hurst(c.h_val),
            macro_phase: BoundedScore::new_clamped(c.m_ph),
            oi_impulse: BoundedScore::ZERO,
            liq_imbalance: BoundedScore::ZERO,
            fib_proj_bps: ScaledBps(0),
        });
        let btc_aligned_loss = c.btc_c * c.side.sign_f64();
        let blocked_abc = !vortex_rs::alpha_3factor::passes_corrective_abc_snapshot_guard_with_sync_c1(
            c.side,
            &s_loss,
            0.78,
            btc_aligned_loss,
        );
        let blocked_ev = APPROVED_SPECS[c.slot]
            .evaluate_pilar2_asymmetry_ev_with_flow_sbps(
                LotQty(if c.slot == 1 { 1 } else { 12 }),
                PriceTicks(c.px),
                ScaledBps(c.vol),
                647,
                50,
                c.geom * c.side.sign_f64(),
                c.bull_pm,
                c.bear_pm,
                c.side,
                fees,
                c.ofi * c.side.sign_f64(),
                btc_aligned_loss,
            )
            .is_none();
        assert!(blocked_abc || blocked_ev);
    }

    // =========================================================================
    // (d) Elimination of static 500_000_000 ns scalar in queue SpoofingVeto & !at_l1 favorable shield:
    //     1) TAO Ask @ 19862 at 523 ms when BBO already stepped favorably to asks[0]=19860 (!at_l1 favorable)
    //        does NOT trigger SpoofingVeto even if L1 geometry at the new level flips (+0.55).
    //     2) Red-Team V06 QUEUE_GEOM_FLIP_170MS (resting AT L1 asks[0]==19511, 170 ms, g_in - g_t >= INV_PHI)
    //        STILL triggers SpoofingVeto 100%.
    // =========================================================================
    let mut tao_fav = mk_vault_snap(4, 19_859, 2_800, 0.12, 0.05, 0.55, t0 + 523_000_000);
    tao_fav.bids[0] = BookLevel { price: PriceTicks(19_859), qty: LotQty(450) };
    tao_fav.asks[0] = BookLevel { price: PriceTicks(19_860), qty: LotQty(320) };
    tao_fav.refresh_deep_l1_l5_totals();
    let mut adv_tao_fav = 0_i64;
    assert!(!vortex_rs::exec_daemon::evaluate_elastic_spoof_geom_abort_with_entry_v54(
        Side::Ask,
        PriceTicks(19_862),
        &tao_fav,
        -0.52,
        523_000_000,
        BoundedScore::ZERO,
        0.15,
        1.0,
        50,
        75_000,
        t0 + 523_000_000,
        &mut adv_tao_fav,
        LotQty(240),
        LotQty(180),
    ));

    // Red-Team V06 QUEUE_GEOM_FLIP_170MS at L1 (entry == asks[0] == 19511, g_in = 0.64, g_t = -0.18 -> flip = 0.82 >= INV_PHI):
    let mut tao_v06 = mk_vault_snap(4, 19_510, 2_800, 0.25, 0.15, 0.18, t0 + 170_000_000);
    tao_v06.bids[0] = BookLevel { price: PriceTicks(19_510), qty: LotQty(600) };
    tao_v06.asks[0] = BookLevel { price: PriceTicks(19_511), qty: LotQty(500) };
    tao_v06.refresh_deep_l1_l5_totals();
    let mut adv_v06_l1 = 0_i64;
    assert!(vortex_rs::exec_daemon::evaluate_elastic_spoof_geom_abort_with_entry_v54(
        Side::Ask,
        PriceTicks(19_511),
        &tao_v06,
        -0.64,
        170_000_000,
        BoundedScore::ZERO,
        0.18,
        1.0,
        50,
        75_000,
        t0 + 170_000_000,
        &mut adv_v06_l1,
        LotQty(500),
        LotQty(500),
    ));
}
