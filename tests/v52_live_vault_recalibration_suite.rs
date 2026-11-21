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

    // (c) Boot MFE order (100% hermetic temp-dir fixtures, independent of live data/vault/* state):
    //     1) rehydrate_cold_mfe_seeds_from_scar_vault executed AFTER hydrate_12_slot_mfe_from_path
    //        rescues cold (613) and scratch-sunk (< PHI * fee_rt_mkr, e.g. TAO=594, AVAX=628, SOL=640)
    //        seeds upward while leaving already-healed slots (>= 647) untouched.
    //     2) When all 12 slots in vortex_mfe_plasticity.json are already healed (>= 712), the boot
    //        sequence preserves all 12 slots 1:1 with zero mutations.
    let tmp_v532_dir = std::env::temp_dir().join(format!(
        "vortex_v532_mfe_hermetic_{}_{}",
        std::process::id(),
        t0
    ));
    let _ = std::fs::remove_dir_all(&tmp_v532_dir);
    assert!(std::fs::create_dir_all(&tmp_v532_dir).is_ok());

    let tmp_mfe_sunk = tmp_v532_dir.join("vortex_mfe_plasticity_sunk.json");
    let tmp_scar_jsonl = tmp_v532_dir.join("vortex_governor_scar_memory.jsonl");
    let sunk_and_cold_src: [i64; 12] = [
        vortex_rs::telemetry_hud::DEFAULT_LEARNED_MFE_SCALED_BPS, // 0 BTC  = 613 (cold)
        647,                                                      // 1 ETH  = 647 (warm boundary)
        640,                                                      // 2 SOL  = 640 (sunk < 647)
        628,                                                      // 3 AVAX = 628 (sunk < 647)
        594,                                                      // 4 TAO  = 594 (sunk < 647)
        890,                                                      // 5 SUI  = 890 (healed)
        760,                                                      // 6 ORDI = 760 (healed)
        vortex_rs::telemetry_hud::DEFAULT_LEARNED_MFE_SCALED_BPS, // 7 PENDLE = 613 (cold)
        820,                                                      // 8 WIF  = 820 (healed)
        745,                                                      // 9 LINK = 745 (healed)
        720,                                                      // 10 NEAR = 720 (healed)
        712,                                                      // 11 LTC  = 712 (healed)
    ];
    assert!(vortex_rs::vault_io::save_mfe_plasticity_to_path_atomic(
        &tmp_mfe_sunk,
        t0,
        &sunk_and_cold_src
    )
    .is_ok());
    let scar_fixture = concat!(
        "{\"event\":\"GOV_SCAR_SNAPSHOT\",\"symbol\":\"TAOUSDT\",\"maker_exit\":true,\"mfe_bps\":12.80,\"net_bps\":8.40}\n",
        "{\"event\":\"TRADE_CLOSE\",\"symbol\":\"TAOUSDT\",\"maker_exit\":true,\"mfe_bps\":13.50,\"net_bps\":9.10}\n",
        "{\"event\":\"TRADE_CLOSE\",\"symbol\":\"AVAXUSDT\",\"maker_exit\":true,\"mfe_bps\":11.40,\"net_bps\":7.20}\n",
        "{\"event\":\"GOV_SCAR_SNAPSHOT\",\"symbol\":\"SOLUSDT\",\"maker_exit\":true,\"mfe_bps\":9.60,\"net_bps\":5.30}\n",
        "{\"event\":\"TRADE_CLOSE\",\"symbol\":\"PENDLEUSDT\",\"maker_exit\":true,\"mfe_bps\":8.45,\"net_bps\":4.25}\n",
        "{\"event\":\"TRADE_CLOSE\",\"symbol\":\"ETHUSDT\",\"maker_exit\":true,\"mfe_bps\":14.00,\"net_bps\":9.80}\n"
    );
    assert!(std::fs::write(&tmp_scar_jsonl, scar_fixture).is_ok());

    let mut sim_eng = SlotExecutionEngine::new_12_slot_array();
    SlotExecutionEngine::hydrate_12_slot_mfe_from_path(&mut sim_eng, &tmp_mfe_sunk);
    let before = SlotExecutionEngine::extract_12_slot_mfe_array(&sim_eng);
    assert_eq!(before, sunk_and_cold_src);
    vortex_rs::telemetry_hud::rehydrate_cold_mfe_seeds_from_scar_vault(&mut sim_eng, &tmp_scar_jsonl);
    let after = SlotExecutionEngine::extract_12_slot_mfe_array(&sim_eng);
    let mut rescued_sunk = 0_usize;
    for s_idx in 0..vortex_rs::lob_shm::MAX_ACTIVE_PAIRS {
        let active_fee_rt_mkr = sim_eng[s_idx].active_maker_rt_fee_bps().max(100);
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
    assert!(rescued_sunk >= 4);
    assert!(after[4] > 594 && after[3] > 628 && after[2] > 640 && after[7] > 613);
    assert_eq!(after[1], 647);

    // Production healed-vault invariance: all 12 slots >= 712 remain 100% untouched.
    let tmp_mfe_healed = tmp_v532_dir.join("vortex_mfe_plasticity_healed_712.json");
    let healed_712_src: [i64; 12] = [740, 780, 712, 835, 910, 890, 760, 725, 845, 795, 730, 715];
    assert!(vortex_rs::vault_io::save_mfe_plasticity_to_path_atomic(
        &tmp_mfe_healed,
        t0,
        &healed_712_src
    )
    .is_ok());
    let mut healed_eng = SlotExecutionEngine::new_12_slot_array();
    SlotExecutionEngine::hydrate_12_slot_mfe_from_path(&mut healed_eng, &tmp_mfe_healed);
    vortex_rs::telemetry_hud::rehydrate_cold_mfe_seeds_from_scar_vault(&mut healed_eng, &tmp_scar_jsonl);
    assert_eq!(
        SlotExecutionEngine::extract_12_slot_mfe_array(&healed_eng),
        healed_712_src
    );

    let _ = std::fs::remove_dir_all(&tmp_v532_dir);
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
    // (b) Right-censored learned_mfe_bps non-contraction (offensive Maker win >= round(PHI * fee_rt_mkr):
    //     805 stays 805 on +6.65/+7.10 bps Maker win with 93.7% capture >= INV_PHI), uncapped upward
    //     expansion on higher excursion / markout, and smooth offensive Tranche 1 + Runner scaling.
    // =========================================================================
    let mut eng_link = SlotExecutionEngine::new(9);
    eng_link.learned_mfe_bps = 805;
    eng_link.open_vol_5m_bps = ScaledBps(2_200);
    eng_link.flow_ema_fast = 0.12;
    eng_link.last_closed_mfe_bps = 710;
    eng_link.last_exit_sub_reason = ExitSubReason::LvdbsDeepVacuumSurf;
    eng_link.observe_censored_close_mfe_plasticity(Side::Bid, 665, true);
    assert_eq!(eng_link.learned_mfe_bps, 805);
    eng_link.observe_mfe_sample_uncensored(665, 710, true, false, 400);
    assert_eq!(eng_link.learned_mfe_bps, 805);

    // Post-exit SlotMarkoutRing peak > learned_mfe_bps expands learned_mfe_bps upward without 820 ceiling:
    let mut markout_ring = vortex_rs::markout_tracker::SlotMarkoutRing::new();
    markout_ring.arm_fill_probe(Side::Bid, PriceTicks(9_218), PriceTicks(9_218), TimestampNs(t0));
    markout_ring.step_clock(PriceTicks(9_335), TimestampNs(t0 + 61_000_000_000), 200);
    let uncensored_peak = markout_ring.take_uncensored_fill_peak_sbps().unwrap_or(0);
    assert!(uncensored_peak > 1_200);
    eng_link.observe_mfe_sample_uncensored(665, 710_i64.max(uncensored_peak), true, false, 400);
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

#[test]
fn test_v541_scratch_uncensored_decay_iceberg_multilot_reduceonly_split_and_stagger() {
    record_live_ws_pulse(69_700, 100);
    let fees = FeeSchedule::VIP0_DEFAULT;
    let t0 = 1_795_200_000_000_000_000_i64;
    let w_65 = 6_468_940_634_i64;

    // =========================================================================
    // 1. Seal Trap 1: Sub-offensive scratch (+4.28/+4.34 bps < round(PHI * 400) = 647)
    //    relaxes smoothly when market cools, while offensive win (>= 647) stays immune;
    //    storm-learned MFE modulates down smoothly by sqrt(vol / eq_vol).clamp(INV_PHI, PHI).
    // =========================================================================
    let mut eng_eth_scratch = SlotExecutionEngine::new(1);
    eng_eth_scratch.learned_mfe_bps = 1_200;
    eng_eth_scratch.open_vol_5m_bps = ScaledBps(1_350);
    eng_eth_scratch.flow_ema_fast = -0.05;
    eng_eth_scratch.last_closed_mfe_bps = 434;
    eng_eth_scratch.last_exit_sub_reason = ExitSubReason::StandardStructuralTp;
    eng_eth_scratch.observe_censored_close_mfe_plasticity(Side::Bid, 428, true);
    assert!(eng_eth_scratch.learned_mfe_bps < 1_200);

    let mut eng_off_win = SlotExecutionEngine::new(1);
    eng_off_win.learned_mfe_bps = 920;
    eng_off_win.open_vol_5m_bps = ScaledBps(1_500);
    eng_off_win.flow_ema_fast = 0.10;
    eng_off_win.last_closed_mfe_bps = 720;
    eng_off_win.last_exit_sub_reason = ExitSubReason::StandardStructuralTp;
    eng_off_win.observe_censored_close_mfe_plasticity(Side::Bid, 680, true);
    assert_eq!(eng_off_win.learned_mfe_bps, 920);

    let calm_proj = vortex_rs::types::modulate_learned_mfe_by_diffusive_regime_sbps(
        1_400,
        ScaledBps(850),
        ScaledBps(2_400),
    );
    assert_eq!(calm_proj, 865);
    let storm_proj = vortex_rs::types::modulate_learned_mfe_by_diffusive_regime_sbps(
        700,
        ScaledBps(7_500),
        ScaledBps(2_400),
    );
    assert_eq!(storm_proj, 1_133);

    // Storm structural TP (180_252 = +14.00 bps) relaxes in calm (900 sbps) instead of hanging unreachable:
    let mut calm_snap = mk_vault_snap(1, 180_020, 900, 0.42, 0.44, 0.30, t0 + 15_000_000_000);
    calm_snap.bids[0] = BookLevel { price: PriceTicks(180_020), qty: LotQty(400) };
    calm_snap.asks[0] = BookLevel { price: PriceTicks(180_021), qty: LotQty(350) };
    let calm_ctx = OpenExitContext {
        slot_idx: 1,
        side: Side::Bid,
        avg_price_ticks: PriceTicks(180_000),
        peak_favorable_price: PriceTicks(180_025),
        worst_adverse_price: PriceTicks(179_995),
        initial_lots: LotQty(1),
        open_lots: LotQty(1),
        tranche1_harvested: false,
        tranche1_lots: LotQty(0),
        tranche1_exit_price: PriceTicks(0),
        defensive_floor_locked: false,
        active_exit_price: PriceTicks(180_252),
        structural_tp_price: PriceTicks(180_252),
        resting_exit_price: PriceTicks(180_252),
        sl_trigger_price: PriceTicks(179_400),
        opened_ts: TimestampNs(t0),
        learned_mfe_bps: 1_400,
        learned_t1_permille: 500,
        learned_ir_x100: 25,
        learned_ret_std_bps: 200,
        intrinsic_tau_mkt_s: 180,
        last_eff_sl_ticks: PriceTicks(179_400),
        ratchet_locked_gross_bps: 0,
        last_closed_mfe_bps: 140,
        last_closed_mae_bps: -28,
        active_fees: fees,
        learned_taker_prob_pm: 50,
    };
    let kin_calm = ShadowRunnerKinematics {
        flow_ema_fast: 0.45,
        flow_ema_slow: 0.40,
        in_scar_debt: false,
        chandelier_armed: false,
    };
    let q_calm = vortex_rs::exit_quoter::compute_canonical_exit_quote_omega_v54(
        &calm_ctx,
        kin_calm,
        &calm_snap,
        ScaledBps(5_200),
        None,
        0,
        50_000,
    );
    assert!(q_calm.is_some_and(|q| q.target_price.0 < 180_180));

    // =========================================================================
    // 2. Seal Trap 2: Deep opposing ImpulseWave3 keeps S_band = 0 while accelerating
    //    (ETH #2 mid-whiplash), but reactivates S_3 Maker evacuation during a pause
    //    (aligned_fast > aligned_slow && aligned_fast >= -INV_PHI_CUBE) to rescue at 4.00 bps.
    // =========================================================================
    let eth2_zero_loss_px = vortex_rs::execution_fsm::defensive_zero_loss_maker_exit_price_with_fees(
        Side::Ask,
        PriceTicks(177_485),
        fees,
    );
    let mut eth2_w3 = mk_vault_snap(1, 177_760, 2_980, 0.06, 0.08, 0.20, t0 + 38_000_000_000);
    eth2_w3.bids[0] = BookLevel { price: PriceTicks(177_760), qty: LotQty(3_200) };
    eth2_w3.asks[0] = BookLevel { price: PriceTicks(177_761), qty: LotQty(3_200) };
    eth2_w3.mark_price = PriceTicks(177_760);
    eth2_w3.last_trade_price = PriceTicks(177_760);
    eth2_w3.set_wave_observables(WaveObservables {
        regime: WaveRegime::ImpulseWave3,
        climax_active: false,
        hurst: WaveObservables::encode_hurst(0.72),
        macro_phase: BoundedScore::new_clamped(0.92),
        oi_impulse: BoundedScore::new_clamped(0.30),
        liq_imbalance: BoundedScore::ZERO,
        fib_proj_bps: ScaledBps(4_800),
    });
    let eth2_rescue_ctx = OpenExitContext {
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
    // Accelerating adverse whiplash -> NO Maker bailout:
    let kin_chase = ShadowRunnerKinematics {
        flow_ema_fast: 0.48,
        flow_ema_slow: 0.42,
        in_scar_debt: false,
        chandelier_armed: false,
    };
    let q_chase = vortex_rs::exit_quoter::compute_canonical_exit_quote_omega_v54(
        &eth2_rescue_ctx,
        kin_chase,
        &eth2_w3,
        ScaledBps(2_980),
        Some(-2.6),
        0,
        69_700,
    );
    assert!(q_chase.is_some_and(|q| q.target_price <= eth2_zero_loss_px));

    // Pause in opposing ImpulseWave3 (aligned_fast = -0.06 > aligned_slow = -0.34, >= -INV_PHI_CUBE) -> Maker BBO evacuation!
    let kin_pause = ShadowRunnerKinematics {
        flow_ema_fast: 0.06,
        flow_ema_slow: 0.34,
        in_scar_debt: false,
        chandelier_armed: false,
    };
    let q_pause = vortex_rs::exit_quoter::compute_canonical_exit_quote_omega_v54(
        &eth2_rescue_ctx,
        kin_pause,
        &eth2_w3,
        ScaledBps(2_980),
        Some(-2.6),
        0,
        69_700,
    );
    assert!(q_pause.is_some_and(|q| {
        q.target_price == PriceTicks(177_760) && q.urgent_close_repeg
    }));

    // Diffusive CorrectiveAbc pullback at same depth & pause does NOT panic-bailout:
    let mut eth2_abc = eth2_w3;
    eth2_abc.set_wave_observables(WaveObservables {
        regime: WaveRegime::CorrectiveAbc,
        climax_active: false,
        hurst: WaveObservables::encode_hurst(0.54),
        macro_phase: BoundedScore::new_clamped(0.35),
        oi_impulse: BoundedScore::ZERO,
        liq_imbalance: BoundedScore::ZERO,
        fib_proj_bps: ScaledBps(0),
    });
    let q_abc = vortex_rs::exit_quoter::compute_canonical_exit_quote_omega_v54(
        &eth2_rescue_ctx,
        kin_pause,
        &eth2_abc,
        ScaledBps(2_980),
        Some(-2.6),
        0,
        69_700,
    );
    assert!(q_abc.is_some_and(|q| q.target_price <= eth2_zero_loss_px));

    // =========================================================================
    // 3. Seal Trap 3: Universal C1 Passive Iceberg Absorption Penalty across
    //    multi-lot (SOL, AVAX, SUI, TAO) & 1-lot (ETH), while healthy impulses
    //    (ofi_al >= INV_PHI_SQ) remain 100% intact (penalty == 0.0).
    // =========================================================================
    assert!(
        vortex_rs::alpha_3factor::compute_passive_iceberg_absorption_penalty_c1(0.42, 0.88, -0.25)
            .abs()
            <= f64::EPSILON
    );
    let pen_ice = vortex_rs::alpha_3factor::compute_passive_iceberg_absorption_penalty_c1(
        0.22,
        0.64,
        -0.05,
    );
    assert!(pen_ice > 0.90);

    let mut btc_ice_leader = mk_vault_snap(0, 650_000, 3_000, -0.08, -0.06, -0.05, t0);
    btc_ice_leader.bids[0].qty = LotQty(1_500);
    btc_ice_leader.asks[0].qty = LotQty(1_500);
    btc_ice_leader.refresh_deep_l1_l5_totals();

    let gov_ice = OnlineAdaptiveGovernor::new();
    for &(slot, px, lots) in &[
        (1_usize, 180_000_i64, 1_i64), // ETH 1-lot
        (2_usize, 7_200_i64, 2_i64),   // SOL multi-lot
        (3_usize, 8_150_i64, 16_i64),  // AVAX multi-lot
        (4_usize, 19_500_i64, 6_i64),  // TAO multi-lot
        (5_usize, 7_400_i64, 18_i64),  // SUI multi-lot
    ] {
        // Iceberg ceiling on Bid: ofi=+0.22 (< INV_PHI_SQ), tfi=+0.66 (tfi - ofi = 0.44 > INV_PHI_SQ), btc <= 0.0:
        let mut s_ice = mk_vault_snap(slot, px, 3_600, 0.22, 0.66, 0.85, t0);
        s_ice.bids[0].qty = LotQty(2_400);
        s_ice.asks[0].qty = LotQty(900);
        s_ice.refresh_deep_l1_l5_totals();
        let v_ice = vortex_rs::alpha_3factor::OrthogonalFactorState::evaluate_entry_signal_v54(
            APPROVED_SPECS[slot],
            &s_ice,
            &btc_ice_leader,
            ScaledBps(720),
            ScaledBps(120),
            50,
            fees,
            0.0,
            1.0,
            0.75,
        );
        assert!(!matches!(
            v_ice,
            vortex_rs::alpha_3factor::SignalVerdict::AuthorizeMakerEntry { .. }
        ));
        assert!(gov_ice
            .evaluate_slot_pilar2_ex_ante_ev_with_leader_sbps(
                slot,
                Side::Bid,
                LotQty(lots),
                PriceTicks(px),
                ScaledBps(720),
                &s_ice,
                &btc_ice_leader,
                1.0,
            )
            .is_none());
        assert!(gov_ice
            .authorizes_adaptive_entry_v54(
                slot,
                Side::Bid,
                w_65,
                1_000,
                ScaledBps(720),
                &s_ice,
                &btc_ice_leader,
                0.0,
                1.0,
                0.75,
            )
            .is_err());

        // Healthy impulse (ofi_al = 0.56 >= INV_PHI_SQ) stays 100% intact:
        let mut s_ok = s_ice;
        s_ok.factor_ofi = BoundedScore::new_clamped(0.56);
        let mut btc_ok = btc_ice_leader;
        btc_ok.factor_ofi = BoundedScore::new_clamped(0.45);
        btc_ok.factor_tfi = BoundedScore::new_clamped(0.48);
        btc_ok.factor_geom = BoundedScore::new_clamped(0.35);
        assert!(matches!(
            vortex_rs::alpha_3factor::OrthogonalFactorState::evaluate_entry_signal_v54(
                APPROVED_SPECS[slot],
                &s_ok,
                &btc_ok,
                ScaledBps(720),
                ScaledBps(120),
                50,
                fees,
                0.0,
                1.0,
                0.75,
            ),
            vortex_rs::alpha_3factor::SignalVerdict::AuthorizeMakerEntry { side: Side::Bid, .. }
        ));
        assert!(gov_ice
            .evaluate_slot_pilar2_ex_ante_ev_with_leader_sbps(
                slot,
                Side::Bid,
                LotQty(lots),
                PriceTicks(px),
                ScaledBps(720),
                &s_ok,
                &btc_ok,
                1.0,
            )
            .is_some());
    }

    // =========================================================================
    // 4. Seal Trap 4: Bybit UTA V5 reduceOnly Tranche 1 + Runner split on
    //    deleveraged multi-lot positions ($6..$13.70 USDT, lots >= 2), while
    //    preserving 1-lot orphan (rem_lots == 1) and < $5.25 guards.
    // =========================================================================
    // AVAX 10L @ $7.14 = $7.14 USDT (2 legs of 5L = $3.57 each, valid reduceOnly):
    assert!(APPROVED_SPECS[3].can_split_runner_legs(LotQty(10), PriceTicks(7_140)));
    assert_eq!(
        compute_tranche1_split_lots_core(3, LotQty(10), PriceTicks(7_140), 500),
        Some(LotQty(5))
    );
    // SUI 10L @ $0.72 = $7.20 USDT -> splits 5 + 5:
    assert!(APPROVED_SPECS[5].can_split_runner_legs(LotQty(10), PriceTicks(7_200)));
    assert_eq!(
        compute_tranche1_split_lots_core(5, LotQty(10), PriceTicks(7_200), 500),
        Some(LotQty(5))
    );
    // TAO 4L @ $195.00 = $7.80 USDT -> splits 2 + 2:
    assert!(APPROVED_SPECS[4].can_split_runner_legs(LotQty(4), PriceTicks(19_500)));
    assert_eq!(
        compute_tranche1_split_lots_core(4, LotQty(4), PriceTicks(19_500), 500),
        Some(LotQty(2))
    );
    // SOL 2L @ $45.00 = $9.00 USDT -> splits 1 + 1:
    assert!(APPROVED_SPECS[2].can_split_runner_legs(LotQty(2), PriceTicks(4_500)));
    assert_eq!(
        compute_tranche1_split_lots_core(2, LotQty(2), PriceTicks(4_500), 500),
        Some(LotQty(1))
    );
    // 1-lot orphan (V02) and sub-$5.25 positions never split:
    assert!(!APPROVED_SPECS[5].can_split_runner_legs(LotQty(1), PriceTicks(7_200)));
    assert_eq!(
        compute_tranche1_split_lots_core(5, LotQty(1), PriceTicks(7_200), 500),
        None
    );
    assert!(!APPROVED_SPECS[5].can_split_runner_legs(LotQty(6), PriceTicks(7_200)));
    assert_eq!(
        compute_tranche1_split_lots_core(5, LotQty(6), PriceTicks(7_200), 500),
        None
    );

    // =========================================================================
    // 5. Seal Trap 5: Same-Side Stagger release after harvesting Tranche 1
    //    (30s blocked, 65s pre-T1 below cushion blocked, 65s post-T1 with cur_gross_bps >= 0 admitted).
    // =========================================================================
    let mut stg_engines = SlotExecutionEngine::new_12_slot_array();
    let mut stg_snaps = [SymbolLobSnapshot::new_empty(); vortex_rs::lob_shm::MAX_ACTIVE_PAIRS];
    stg_snaps[5] = mk_vault_snap(5, 7_202, 2_200, 0.45, 0.48, 0.35, t0);
    stg_snaps[5].mark_price = PriceTicks(7_202); // +2.78 bps >= 0 over 7_200, but < req_cushion_bps (~830 sbps)
    assert!(stg_engines[5]
        .adopt_live_open_position(
            Side::Bid,
            PriceTicks(7_200),
            LotQty(10),
            LotQty(10),
            false,
            PriceTicks(7_204),
            PriceTicks(7_199),
            TimestampNs(t0),
            &stg_snaps[5],
        )
        .is_ok());

    // Pre-Tranche 1: blocked at 30s AND blocked at 65s (cur_gross_bps = +277 < req_cushion_bps):
    assert!(!vortex_rs::exec_daemon::can_stage_same_direction_entry(
        3,
        Side::Bid,
        TimestampNs(t0 + 30_000_000_000),
        &stg_engines,
        &stg_snaps,
    ));
    assert!(!vortex_rs::exec_daemon::can_stage_same_direction_entry(
        3,
        Side::Bid,
        TimestampNs(t0 + 65_000_000_000),
        &stg_engines,
        &stg_snaps,
    ));

    // Harvest Tranche 1 on deleveraged 10L SUI ($7.20 -> 5L harvested + 5L runner):
    assert!(stg_engines[5].record_tranche1_harvest(PriceTicks(7_204)));
    assert_eq!(stg_engines[5].tranche1_harvests_count, 1);

    // Post-Tranche 1: 30s STILL blocked by stagger clock, 65s with cur_gross_bps >= 0 ADMITTED!
    assert!(!vortex_rs::exec_daemon::can_stage_same_direction_entry(
        3,
        Side::Bid,
        TimestampNs(t0 + 30_000_000_000),
        &stg_engines,
        &stg_snaps,
    ));
    assert!(vortex_rs::exec_daemon::can_stage_same_direction_entry(
        3,
        Side::Bid,
        TimestampNs(t0 + 65_000_000_000),
        &stg_engines,
        &stg_snaps,
    ));

    // If runner slips underwater (cur_gross_bps < 0) without Chandelier lock, stagger re-engages:
    stg_snaps[5].mark_price = PriceTicks(7_195);
    assert!(!vortex_rs::exec_daemon::can_stage_same_direction_entry(
        3,
        Side::Bid,
        TimestampNs(t0 + 65_000_000_000),
        &stg_engines,
        &stg_snaps,
    ));
}

#[test]
fn test_v542_elastic_throughput_penny_jump_fill_rate_and_5usdt_hard_lock() {
    use std::sync::{mpsc, Arc};
    use vortex_rs::alpha_3factor::{
        compute_corrective_abc_clear_lift_c1, compute_corrective_abc_structural_threat_c1,
        compute_s_opp_ob_wall_c1, passes_corrective_abc_snapshot_guard_with_health_c1,
        BtcLeadLagMemory, EppsBtcCorrelationTracker, OrthogonalFactorState, SignalVerdict,
        INV_PHI, PHI_FOURTH,
    };
    use vortex_rs::bybit_client::InstrumentSpecVerification;
    use vortex_rs::exec_daemon::{
        can_stage_same_direction_entry_v542, compute_validation_epoch_loss_1e8,
        is_safe_halt_standby, step_12_slot_daemon_tick,
    };
    use vortex_rs::exec_gateway_handlers::ExecRuntimeState;
    use vortex_rs::execution_fsm::{
        compute_agile_repeg_cooldown_ns_v542, compute_aligned_entry_propulsion_c1,
        passes_cumulative_repeg_chase_cap_v542, passes_elliott_macro_coherence_guard_with_health,
        select_elastic_entry_maker_price_v542, OrderLifecycleState,
    };
    use vortex_rs::fee_cadence_beta::MonetaryPortfolioBetaGuard;
    use vortex_rs::fifo_sim::{FifoQueueSimulator, SimulatedOrderState, FLIGHT_LATENCY_NS};
    use vortex_rs::forensic_parquet::ParitySyncLedger;
    use vortex_rs::governor_plasticity::{
        compute_epoch_5u_loss_lot_damper_c1, compute_psi_health_c1, DailyShieldPhase,
    };
    use vortex_rs::lob_shm::{MappedShmRegion, MAX_ACTIVE_PAIRS};
    use vortex_rs::replay_walkforward::OmegaDigitalTwinEngine;
    use vortex_rs::telemetry_hud::{
        format_total_loss_shield_5u_plain, rehydrate_shadow_ledger_from_paths,
        SlotForensicEntrySnapshot,
    };
    use vortex_rs::vault_io::{
        spawn_vault_io_worker_with_lock_paths, AtomicLockWatch, VaultIoCommand,
        MAX_VALIDATION_EPOCH_LOSS_1E8,
    };

    record_live_ws_pulse(50_000, 100);
    let fees = FeeSchedule::VIP0_DEFAULT;
    let t0 = 1_795_344_000_000_000_000_i64; // Exact UTC day boundary + offset below

    // =========================================================================
    // (a) M1 + M3: CorrectiveAbc clear-runway unlock (chi_threat == 0), BTC short
    //     lead-lag memory (tau = PHI^4 s), 2-Slot Epps Queue Stagger, AND 100%
    //     blocking of all 6 historical CorrectiveAbc losses + ETH #2.
    // =========================================================================
    let mut btc_neutral = mk_vault_snap(0, 652_000, 2_800, 0.06, 0.08, 0.05, t0);
    btc_neutral.bids[0].qty = LotQty(1_800);
    btc_neutral.asks[0].qty = LotQty(1_750);
    btc_neutral.refresh_deep_l1_l5_totals();

    let mut link_clear = mk_vault_snap(9, 14_250, 1_650, 0.68, 0.72, 0.56, t0);
    link_clear.ob_bull_efficacy_pm = 530;
    link_clear.ob_bear_efficacy_pm = 520;
    link_clear.ob_bull_score = BoundedScore::new_clamped(0.62);
    link_clear.ob_dist_bull_bps = ScaledBps(1_800);
    link_clear.ob_bear_score = BoundedScore::ZERO;
    link_clear.ob_dist_bear_bps = ScaledBps(0);
    link_clear.set_wave_observables(WaveObservables {
        regime: WaveRegime::CorrectiveAbc,
        climax_active: false,
        hurst: WaveObservables::encode_hurst(0.78),
        macro_phase: BoundedScore::new_clamped(0.10),
        oi_impulse: BoundedScore::ZERO,
        liq_imbalance: BoundedScore::ZERO,
        fib_proj_bps: ScaledBps(0),
    });
    let s_opp_clear = compute_s_opp_ob_wall_c1(Side::Bid, &link_clear, 2_400, 50, fees);
    assert!(s_opp_clear.abs() <= 1e-12);
    let chi_clear = compute_corrective_abc_structural_threat_c1(s_opp_clear, 0.10);
    assert!(chi_clear.abs() <= 1e-12);
    let dh_clear = compute_corrective_abc_clear_lift_c1(1.0, 0.68, chi_clear, 0.0);
    assert!(dh_clear > 0.35);

    // At psi_health == 0.0 (V5.4.1 selectivity with low Epps=0.25 & neutral BTC): blocked!
    assert!(!passes_corrective_abc_snapshot_guard_with_health_c1(
        Side::Bid,
        &link_clear,
        0.25,
        0.05,
        0.0,
    ));
    // At psi_health == 1.0 with clear runway (chi_threat == 0.0): unlocked including low-vol breakout!
    assert!(passes_corrective_abc_snapshot_guard_with_health_c1(
        Side::Bid,
        &link_clear,
        0.25,
        0.05,
        1.0,
    ));
    assert!(passes_elliott_macro_coherence_guard_with_health(
        Side::Bid,
        &link_clear,
        &btc_neutral,
        0.25,
        None,
        1.0,
    ));
    assert!(matches!(
        OrthogonalFactorState::evaluate_entry_signal_v542(
            APPROVED_SPECS[9],
            &link_clear,
            &btc_neutral,
            ScaledBps(860),
            ScaledBps(140),
            50,
            fees,
            0.0,
            0.25,
            0.25,
            None,
            1.0,
        ),
        SignalVerdict::AuthorizeMakerEntry { side: Side::Bid, .. }
    ));

    // Short exponential BTC Lead-Lag Memory (tau = PHI^4 s = 6.8541 s) captures lagged altcoin sync:
    assert!((PHI_FOURTH - 6.854_101_966_249_685).abs() < 1e-12);
    let mut btc_mem = BtcLeadLagMemory::new();
    let mut btc_burst = mk_vault_snap(0, 654_000, 3_600, 0.88, 0.92, 0.84, t0);
    btc_burst.bids[0].qty = LotQty(3_400);
    btc_burst.asks[0].qty = LotQty(1_200);
    btc_burst.refresh_deep_l1_l5_totals();
    let ema_t0 = btc_mem.observe_btc_snapshot(&btc_burst);
    assert!(ema_t0 > 0.75);
    let mut btc_pause_3s = btc_neutral;
    btc_pause_3s.ts_local_ns = TimestampNs(t0 + 3_000_000_000);
    let _ = btc_mem.observe_btc_snapshot(&btc_pause_3s);
    let eff_lagged_btc = btc_mem.effective_aligned_impulse(&btc_pause_3s, 1.0, Side::Bid);
    assert!(eff_lagged_btc > 0.50);
    let mut tao_lagged = link_clear;
    tao_lagged.symbol_id = APPROVED_SPECS[4].symbol_id;
    tao_lagged.bids[0].price = PriceTicks(19_600);
    tao_lagged.asks[0].price = PriceTicks(19_601);
    tao_lagged.mark_price = PriceTicks(19_600);
    tao_lagged.set_wave_observables(WaveObservables {
        regime: WaveRegime::CorrectiveAbc,
        climax_active: false,
        hurst: WaveObservables::encode_hurst(0.76),
        macro_phase: BoundedScore::new_clamped(-0.30),
        oi_impulse: BoundedScore::ZERO,
        liq_imbalance: BoundedScore::ZERO,
        fib_proj_bps: ScaledBps(0),
    });
    assert!(passes_corrective_abc_snapshot_guard_with_health_c1(
        Side::Bid,
        &tao_lagged,
        0.86,
        eff_lagged_btc,
        0.40,
    ));

    // All 6 historical CorrectiveAbc losses (#18, #19, #22, #23, #27, #30) + ETH #2 remain 100% blocked even at psi_health == 1.0:
    #[derive(Copy, Clone)]
    struct HistAbcCase {
        slot: usize, px: i64, vol: i64, ofi: f64, tfi: f64, geom: f64, btc_c: f64,
        side: Side, h_val: f64, m_ph: f64, bull_pm: u16, bear_pm: u16, opp_sc: f64, opp_dist: i64,
    }
    let hist_cases = [
        HistAbcCase { slot: 1, px: 179_267, vol: 1_725, ofi: 0.57, tfi: 0.93, geom: 0.09, btc_c: 0.10, side: Side::Bid, h_val: 0.696, m_ph: 0.45, bull_pm: 438, bear_pm: 415, opp_sc: 0.50, opp_dist: 5_572 },
        HistAbcCase { slot: 5, px: 7_316, vol: 1_582, ofi: -0.75, tfi: -0.84, geom: -0.17, btc_c: -0.55, side: Side::Ask, h_val: 0.748, m_ph: -0.45, bull_pm: 522, bear_pm: 478, opp_sc: 0.57, opp_dist: 9_670 },
        HistAbcCase { slot: 3, px: 7_156, vol: 1_873, ofi: -0.49, tfi: -0.95, geom: -0.20, btc_c: -0.21, side: Side::Ask, h_val: 0.637, m_ph: -0.45, bull_pm: 465, bear_pm: 471, opp_sc: 0.60, opp_dist: 3_908 },
        HistAbcCase { slot: 4, px: 19_691, vol: 2_428, ofi: 0.65, tfi: 0.86, geom: 0.36, btc_c: 0.36, side: Side::Bid, h_val: 0.729, m_ph: -0.30, bull_pm: 564, bear_pm: 516, opp_sc: 0.44, opp_dist: 4_984 },
        HistAbcCase { slot: 1, px: 179_552, vol: 1_890, ofi: 0.48, tfi: 0.84, geom: 0.93, btc_c: 0.43, side: Side::Bid, h_val: 0.715, m_ph: 0.45, bull_pm: 443, bear_pm: 441, opp_sc: 0.39, opp_dist: 6_337 },
        HistAbcCase { slot: 4, px: 19_038, vol: 3_023, ofi: -0.72, tfi: -0.67, geom: -0.27, btc_c: -0.32, side: Side::Ask, h_val: 0.744, m_ph: -0.45, bull_pm: 476, bear_pm: 548, opp_sc: 0.64, opp_dist: 8_861 },
        HistAbcCase { slot: 1, px: 177_485, vol: 5_800, ofi: -0.22, tfi: -0.62, geom: -0.92, btc_c: -0.14, side: Side::Ask, h_val: 0.700, m_ph: -0.35, bull_pm: 509, bear_pm: 532, opp_sc: 0.67, opp_dist: 1_133 },
    ];
    for c in hist_cases {
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
        let s_opp_loss = compute_s_opp_ob_wall_c1(c.side, &s_loss, 2_400, 50, fees);
        assert!((s_opp_loss - 1.0).abs() < 1e-9);
        let btc_al = c.btc_c * c.side.sign_f64();
        let blocked_abc = !passes_corrective_abc_snapshot_guard_with_health_c1(
            c.side, &s_loss, 0.82, btc_al, 1.0,
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
                btc_al,
            )
            .is_none();
        assert!(blocked_abc || blocked_ev);
    }

    // M3: 2-Slot Epps Queue Stagger (max 2 RestingEntryQueue on same side, max 1 PlacingEntryMaker):
    let mut m3_engines = SlotExecutionEngine::new_12_slot_array();
    let mut m3_snaps = [SymbolLobSnapshot::new_empty(); MAX_ACTIVE_PAIRS];
    m3_snaps[5] = mk_vault_snap(5, 7_200, 2_600, 0.65, 0.70, 0.50, t0);
    m3_snaps[4] = mk_vault_snap(4, 19_500, 2_800, 0.68, 0.72, 0.55, t0);
    m3_snaps[3] = mk_vault_snap(3, 8_150, 2_600, 0.66, 0.70, 0.52, t0);
    m3_engines[5].state = OrderLifecycleState::RestingEntryQueue {
        side: Side::Bid,
        entry_price: PriceTicks(7_200),
        qty: LotQty(15),
        target_excursion: ScaledBps(700),
        sl_trigger_price: PriceTicks(7_120),
        resting_since_ts: TimestampNs(t0),
    };
    let epps_low = [EppsBtcCorrelationTracker::new(); MAX_ACTIVE_PAIRS]; // cold seed = 0.618 -> 0.618*0.618 = 0.3819 < INV_PHI!
    let mut beta_g = MonetaryPortfolioBetaGuard::new();
    assert!(beta_g.can_open_with_risk_ppm(5, Side::Bid, 2_500, 618));
    beta_g.register_slot_open(5, Side::Bid, 2_500, 618);
    assert!(beta_g.can_open_with_risk_ppm(4, Side::Bid, 2_500, 618));
    assert!(can_stage_same_direction_entry_v542(
        4, Side::Bid, TimestampNs(t0 + 100_000_000), &m3_engines, &m3_snaps, &epps_low, 0.95, 50,
    ));
    // 3rd same-side resting order is strictly blocked (cap == 2):
    m3_engines[4].state = OrderLifecycleState::RestingEntryQueue {
        side: Side::Bid,
        entry_price: PriceTicks(19_500),
        qty: LotQty(1),
        target_excursion: ScaledBps(700),
        sl_trigger_price: PriceTicks(19_300),
        resting_since_ts: TimestampNs(t0),
    };
    assert!(!can_stage_same_direction_entry_v542(
        3, Side::Bid, TimestampNs(t0 + 100_000_000), &m3_engines, &m3_snaps, &epps_low, 0.95, 50,
    ));
    // PlacingEntryMaker on same side blocks staging a second entry on the same tick:
    m3_engines[4].state = OrderLifecycleState::Idle;
    m3_engines[5].state = OrderLifecycleState::PlacingEntryMaker {
        side: Side::Bid,
        entry_price: PriceTicks(7_200),
        qty: LotQty(15),
        target_excursion: ScaledBps(700),
        sl_trigger_price: PriceTicks(7_120),
        submitted_ts: TimestampNs(t0),
    };
    assert!(!can_stage_same_direction_entry_v542(
        4, Side::Bid, TimestampNs(t0 + 5_000_000), &m3_engines, &m3_snaps, &epps_low, 0.95, 50,
    ));

    // =========================================================================
    // (b) M2: Inside-Spread PostOnly Penny-Jump + BetaBinomialFillRate + Agile
    //     Re-Peg + Favorable Miss Cooldown Exemption (V08 intact on toxic abort).
    // =========================================================================
    let mut wide_eth = mk_vault_snap(1, 180_000, 3_600, 0.78, 0.82, 0.65, t0);
    wide_eth.bids[0] = BookLevel { price: PriceTicks(180_000), qty: LotQty(400) };
    wide_eth.asks[0] = BookLevel { price: PriceTicks(180_003), qty: LotQty(400) };
    wide_eth.refresh_deep_l1_l5_totals();
    let prop_bid = compute_aligned_entry_propulsion_c1(Side::Bid, &wide_eth, 0.45);
    assert!(prop_bid > INV_PHI);
    let pj_bid = select_elastic_entry_maker_price_v542(Side::Bid, &wide_eth, 0.95, prop_bid);
    assert_eq!(pj_bid, PriceTicks(180_001));
    assert!(pj_bid > wide_eth.bids[0].price && pj_bid < wide_eth.asks[0].price);

    let mut wide_eth_ask = mk_vault_snap(1, 180_000, 3_600, -0.78, -0.82, -0.65, t0);
    wide_eth_ask.bids[0] = BookLevel { price: PriceTicks(180_000), qty: LotQty(400) };
    wide_eth_ask.asks[0] = BookLevel { price: PriceTicks(180_003), qty: LotQty(400) };
    let prop_ask = compute_aligned_entry_propulsion_c1(Side::Ask, &wide_eth_ask, -0.45);
    let pj_ask = select_elastic_entry_maker_price_v542(Side::Ask, &wide_eth_ask, 0.95, prop_ask);
    assert_eq!(pj_ask, PriceTicks(180_002));
    assert!(pj_ask < wide_eth_ask.asks[0].price && pj_ask > wide_eth_ask.bids[0].price);

    // 1-tick spread never crosses BBO (0 PostOnly rejects):
    let mut tight_eth = wide_eth;
    tight_eth.asks[0] = BookLevel { price: PriceTicks(180_001), qty: LotQty(400) };
    assert_eq!(
        select_elastic_entry_maker_price_v542(Side::Bid, &tight_eth, 0.95, prop_bid),
        PriceTicks(180_000)
    );

    // FIFO Simulator verifies 0 PostOnly rejects, #1 queue priority (queue_ahead == 0), and no phantom L2 wipe:
    let mut fifo = FifoQueueSimulator::new();
    fifo.submit_post_only(Side::Bid, pj_bid, LotQty(1), TimestampNs(t0));
    let mut wide_eth_arrive = wide_eth;
    wide_eth_arrive.ts_local_ns = TimestampNs(t0 + FLIGHT_LATENCY_NS);
    fifo.on_lob_snapshot(&wide_eth_arrive);
    assert!(matches!(
        fifo.state,
        SimulatedOrderState::RestingInFifoQueue { queue_ahead_qty: LotQty(0), .. }
    ));
    assert_eq!(fifo.total_post_only_rejects, 0);
    // Subsequent L2 snapshot with best_bid=180_000 (< 180_001) and best_ask=180_003 (> 180_001) keeps resting cleanly:
    wide_eth_arrive.ts_local_ns = TimestampNs(t0 + FLIGHT_LATENCY_NS + 2_000_000);
    fifo.on_lob_snapshot(&wide_eth_arrive);
    assert!(matches!(
        fifo.state,
        SimulatedOrderState::RestingInFifoQueue { queue_ahead_qty: LotQty(0), .. }
    ));
    // First 1-lot aggressive sell print at 180_001 fills immediately as Maker:
    fifo.on_trade_print(
        TimestampNs(t0 + FLIGHT_LATENCY_NS + 5_000_000),
        Side::Ask,
        pj_bid,
        LotQty(1),
    );
    assert!(matches!(
        fifo.state,
        SimulatedOrderState::FilledMaker { fill_price: PriceTicks(180_001), .. }
    ));

    // BetaBinomialFillRate neutral seed (1000/1000 -> 0.50), GOV_SCAR_SNAPSHOT roundtrip, and agile Re-Peg:
    let mut gov_fill = OnlineAdaptiveGovernor::new();
    assert_eq!(gov_fill.slot_fill_obs_alpha_beta(4), (1_000, 1_000));
    assert!((gov_fill.slot_fill_rate_eta(4) - 0.50).abs() < 1e-12);
    let cd_neutral = compute_agile_repeg_cooldown_ns_v542(50_000, gov_fill.slot_fill_rate_eta(4));

    // Favorable BBO miss raises beta -> lowers eta_fill -> shortens agile Re-Peg cooldown AND sets 0 anti-knife cooldown!
    gov_fill.observe_entry_abort_with_context(
        4,
        Side::Bid,
        PriceTicks(19_500),
        TimestampNs(t0),
        true,
        false,
    );
    gov_fill.observe_favorable_entry_miss(4);
    let tao_dip = mk_vault_snap(4, 19_495, 2_800, 0.55, 0.60, 0.45, t0 + 1_000_000_000);
    assert_eq!(
        gov_fill.audit_scar64_entry_and_sweep_shield(4, Side::Bid, &tao_dip, 0.40),
        Ok(())
    );
    assert!(gov_fill.slot_fill_rate_eta(4) < 0.50);
    let cd_missed = compute_agile_repeg_cooldown_ns_v542(50_000, gov_fill.slot_fill_rate_eta(4));
    assert!(cd_missed < cd_neutral);

    // Passive fills raise alpha -> raises eta_fill:
    for _ in 0..5 {
        gov_fill.observe_passive_entry_fill(2);
    }
    assert!(gov_fill.slot_fill_rate_eta(2) > 0.65);

    // Adverse / toxic abort keeps V08 POST_ABORT_KNIFE_TRAP cooldown 100% active:
    gov_fill.observe_entry_abort_with_context(
        3,
        Side::Bid,
        PriceTicks(8_150),
        TimestampNs(t0),
        false,
        true,
    );
    let avax_dip = mk_vault_snap(3, 8_145, 2_800, 0.55, 0.60, 0.45, t0 + 1_000_000_000);
    assert_eq!(
        gov_fill.audit_scar64_entry_and_sweep_shield(3, Side::Bid, &avax_dip, 0.40),
        Err("VETO_POST_ABORT_ADVERSE_SWEEP")
    );

    // Cumulative Re-Peg chase cap (< INV_PHI_SQ * (target_gross - fee_rt_mkr)):
    // target_gross = 900 sbps, fee_rt_mkr = 400 sbps -> headroom = 500 sbps -> cap = 190.98 sbps (~1.91 bp)
    assert!(passes_cumulative_repeg_chase_cap_v542(
        Side::Bid,
        PriceTicks(100_000),
        PriceTicks(100_015), // +1.50 bps < 1.91 bps
        ScaledBps(900),
        ScaledBps(400),
    ));
    assert!(!passes_cumulative_repeg_chase_cap_v542(
        Side::Bid,
        PriceTicks(100_000),
        PriceTicks(100_022), // +2.20 bps >= 1.91 bps -> blocked to preserve EV > 0!
        ScaledBps(900),
        ScaledBps(400),
    ));

    // Temp dir for isolated persistence checks (never touches data/vault/*):
    let pre_live_halt_lock_exists = std::path::Path::new("data/vault/HALTED_ON_LOSS.lock").exists();
    let tmp_dir = std::env::temp_dir().join(format!(
        "vortex_v542_cert_{}_{}",
        std::process::id(),
        t0
    ));
    let _ = std::fs::remove_dir_all(&tmp_dir);
    assert!(std::fs::create_dir_all(&tmp_dir).is_ok());

    let scar_p = tmp_dir.join("scar_v542.jsonl");
    let loss_p = tmp_dir.join("loss_v542.jsonl");
    let foren_p = tmp_dir.join("foren_v542.jsonl");
    assert!(std::fs::write(
        &scar_p,
        format!("{}\n", gov_fill.to_scar_snapshot_json(TimestampNs(t0 + 1_000_000)))
    )
    .is_ok());
    assert!(std::fs::write(&loss_p, "").is_ok());
    assert!(std::fs::write(&foren_p, "").is_ok());
    let mut gov_rehyd = OnlineAdaptiveGovernor::new();
    let _ = gov_rehyd.rehydrate_from_scar_and_loss_files(&scar_p, &loss_p, Some(&foren_p));
    assert_eq!(gov_rehyd.slot_fill_obs_alpha_beta(2), gov_fill.slot_fill_obs_alpha_beta(2));
    assert_eq!(gov_rehyd.slot_fill_obs_alpha_beta(4), gov_fill.slot_fill_obs_alpha_beta(4));

    // =========================================================================
    // (c) M4: Centrifugal Operational Health Valve psi_health in [0.0, 1.0]
    //     contracts M1..M3 smoothly when toxicity or drawdown rises.
    // =========================================================================
    let psi_healthy = compute_psi_health_c1(0.045, 2.5, 4.0, 0.0, 0.0);
    assert!((psi_healthy - 1.0).abs() < 1e-12);
    let psi_mid = compute_psi_health_c1(0.15, -2.0, 4.0, 0.45, 0.45);
    assert!(psi_mid > 0.0 && psi_mid < 0.50);
    let psi_toxic = compute_psi_health_c1(0.25, 1.0, 4.0, 0.0, 0.0);
    assert!(psi_toxic.abs() <= 1e-12);
    let psi_5u_drawdown = compute_psi_health_c1(0.05, 1.5, 4.0, 0.0, 0.65);
    assert!(psi_5u_drawdown.abs() <= 1e-12);

    // Contracted psi_health immediately shuts off M1 clear-runway lift, M2 Penny-Jump, and M3 2-slot stagger:
    assert!(!passes_corrective_abc_snapshot_guard_with_health_c1(
        Side::Bid,
        &link_clear,
        0.25,
        0.05,
        psi_5u_drawdown,
    ));
    assert_eq!(
        select_elastic_entry_maker_price_v542(Side::Bid, &wide_eth, psi_5u_drawdown, prop_bid),
        PriceTicks(180_000)
    );
    m3_engines[5].state = OrderLifecycleState::RestingEntryQueue {
        side: Side::Bid,
        entry_price: PriceTicks(7_200),
        qty: LotQty(15),
        target_excursion: ScaledBps(700),
        sl_trigger_price: PriceTicks(7_120),
        resting_since_ts: TimestampNs(t0),
    };
    assert!(!can_stage_same_direction_entry_v542(
        4,
        Side::Bid,
        TimestampNs(t0 + 100_000_000),
        &m3_engines,
        &m3_snaps,
        &epps_low,
        psi_5u_drawdown,
        50,
    ));

    // =========================================================================
    // (d) M5: Irreversible -5.00 USDT (MAX_VALIDATION_EPOCH_LOSS_1E8 = 500_000_000)
    //     Hard Shield, temp HALTED_ON_LOSS.lock persistence, 00:00 UTC rollover
    //     immunity, startup rehydration, P0-B custody, and Digital Twin parity.
    // =========================================================================
    assert_eq!(MAX_VALIDATION_EPOCH_LOSS_1E8, 500_000_000);
    assert!(core::mem::size_of::<VaultIoCommand>() <= 512);

    // Pre-limit C1 lot damper: 1.0 below INV_PHI * 5 USDT, smooth contraction in (INV_PHI, 1.0), INV_PHI at 5 USDT:
    assert!((compute_epoch_5u_loss_lot_damper_c1(250_000_000) - 1.0).abs() < 1e-12);
    let d_4u = compute_epoch_5u_loss_lot_damper_c1(400_000_000);
    assert!(d_4u < 1.0 && d_4u > INV_PHI);
    assert!((compute_epoch_5u_loss_lot_damper_c1(500_000_000) - INV_PHI).abs() < 1e-12);

    // MtM + Realized max loss combiner:
    let mut led_mtm = ParitySyncLedger::new(6_500_000_000);
    led_mtm.record_closed_trade_with_sinking_split(-320_000_000, 5_000_000, ScaledBps(-1_600));
    let (mtm_breach_1e8, _) = compute_validation_epoch_loss_1e8(&led_mtm, None, -185_000_000);
    assert_eq!(mtm_breach_1e8, 505_000_000);

    // Ephemeral SHM + temp lock paths + VaultIo worker test:
    let shm_p = MappedShmRegion::ephemeral_self_test_path("v542_5usdt_shield");
    let shm_res = MappedShmRegion::open_or_create(&shm_p, true);
    assert!(shm_res.is_ok());
    let Ok(shm_reg) = shm_res else { return };
    let tmp_halt_lock = tmp_dir.join("HALTED_ON_LOSS.lock");
    let tmp_drain_lock = tmp_dir.join("DRAIN_MODE.lock");
    let lock_watch = Arc::new(AtomicLockWatch::new(false, false));
    let worker_tx = spawn_vault_io_worker_with_lock_paths(
        Arc::clone(&lock_watch),
        tmp_halt_lock.clone(),
        tmp_drain_lock,
    );
    let (tg_tx, tg_rx) = mpsc::sync_channel::<String>(16);
    let (vault_tx, vault_rx) = mpsc::sync_channel::<VaultIoCommand>(32);

    let inst = InstrumentSpecVerification::baseline_from_specs(&APPROVED_SPECS);
    let day1_ns = TimestampNs(t0 + 18 * 3_600_000_000_000);
    let mut rt = ExecRuntimeState::new();
    rt.wallet_1e8 = 6_500_000_000;
    let _ = rt.daily_gov.sync_utc_day(day1_ns, rt.wallet_1e8);

    let mut snaps = [SymbolLobSnapshot::new_empty(); MAX_ACTIVE_PAIRS];
    snaps[0] = mk_vault_snap(0, 652_000, 2_800, 0.15, 0.18, 0.12, day1_ns.0);
    snaps[3] = mk_vault_snap(3, 8_200, 3_000, 0.55, 0.60, 0.45, day1_ns.0);
    snaps[5] = mk_vault_snap(5, 7_203, 2_600, 0.30, 0.32, 0.25, day1_ns.0);

    let mut entry_f = [SlotForensicEntrySnapshot::default(); MAX_ACTIVE_PAIRS];
    entry_f[3].armed_ts_ns = day1_ns.0;
    entry_f[3].geom_in = 0.45;
    entry_f[5].armed_ts_ns = day1_ns.0;
    let mut engines = SlotExecutionEngine::new_12_slot_array();
    // Slot 3 in RestingEntryQueue (must be cancelled immediately on -5.00 USDT breach):
    engines[3].state = OrderLifecycleState::RestingEntryQueue {
        side: Side::Bid,
        entry_price: PriceTicks(8_200),
        qty: LotQty(12),
        target_excursion: ScaledBps(750),
        sl_trigger_price: PriceTicks(8_120),
        resting_since_ts: day1_ns,
    };
    rt.init_q_ahead[3] = LotQty(900);
    rt.cur_q_ahead[3] = LotQty(900);
    rt.last_l1_q_obs[3] = LotQty(900);

    // Slot 5 in PositionOpenMakerExit (must remain under P0-B custody until closed):
    assert!(engines[5]
        .adopt_live_open_position(
            Side::Bid,
            PriceTicks(7_200),
            LotQty(10),
            LotQty(10),
            false,
            PriceTicks(7_212),
            PriceTicks(7_120),
            day1_ns,
            &snaps[5],
        )
        .is_ok());

    let mut gov_d = OnlineAdaptiveGovernor::new();
    let mut ledger = ParitySyncLedger::new(6_500_000_000);
    let mut last_rest = 0_i64;

    // 1) At -4.99999999 USDT (-499_999_999): ARMED, not halted, RestingEntryQueue preserved!
    ledger.record_closed_trade_with_sinking_split(-499_999_999, 10_000_000, ScaledBps(-2_500));
    let (open_pre, pend_pre) = step_12_slot_daemon_tick(
        day1_ns,
        None,
        inst,
        Some(&lock_watch),
        &mut rt,
        &mut last_rest,
        &snaps,
        &mut entry_f,
        &mut engines,
        &mut gov_d,
        &mut ledger,
        shm_reg.arena(),
        &tg_tx,
        &vault_tx,
    );
    assert!(!rt.halt_locked && !lock_watch.is_halt_locked());
    assert_eq!((open_pre, pend_pre), (1, 1));
    assert!(matches!(engines[3].state, OrderLifecycleState::RestingEntryQueue { .. }));

    // 2) Cross exact -5.00000000 USDT (-500_000_000): immediate 0 ns halt, RestingEntryQueue cancelled, P0-B active!
    ledger.record_closed_trade_with_sinking_split(-1, 0, ScaledBps(0));
    assert_eq!(ledger.cumulative_net_pnl_1e8, -500_000_000);
    let tick_breach_ns = TimestampNs(day1_ns.0 + 5_000_000);
    let (open_br, pend_br) = step_12_slot_daemon_tick(
        tick_breach_ns,
        None,
        inst,
        Some(&lock_watch),
        &mut rt,
        &mut last_rest,
        &snaps,
        &mut entry_f,
        &mut engines,
        &mut gov_d,
        &mut ledger,
        shm_reg.arena(),
        &tg_tx,
        &vault_tx,
    );
    assert!(rt.halt_locked && lock_watch.is_halt_locked());
    assert_eq!((open_br, pend_br), (1, 0));
    assert!(matches!(engines[3].state, OrderLifecycleState::Idle));
    assert!(matches!(engines[5].state, OrderLifecycleState::PositionOpenMakerExit { .. }));
    assert!(!is_safe_halt_standby(rt.halt_locked, open_br, pend_br, false));

    // Forward emitted LatchTotalLossHalt command to the real VaultIo worker targeting ephemeral tmp_halt_lock:
    let mut saw_latch_cmd = false;
    while let Ok(cmd) = vault_rx.try_recv() {
        if matches!(cmd, VaultIoCommand::LatchTotalLossHalt { epoch_loss_1e8: 500_000_000, .. }) {
            saw_latch_cmd = true;
            assert!(worker_tx.send(cmd).is_ok());
        }
    }
    assert!(saw_latch_cmd);
    let (ack_tx, ack_rx) = mpsc::sync_channel::<()>(1);
    assert!(worker_tx.send(VaultIoCommand::FlushBarrier(ack_tx)).is_ok());
    assert!(ack_rx.recv_timeout(std::time::Duration::from_secs(2)).is_ok());
    assert!(tmp_halt_lock.exists());
    let lock_body = std::fs::read_to_string(&tmp_halt_lock).unwrap_or_default();
    assert!(
        lock_body.contains("\"event\":\"TOTAL_LOSS_SHIELD_5USDT_HALT\"")
            && lock_body.contains("\"epoch_loss_1e8\":500000000")
    );

    // Verify exact 1 single Telegram alert (zero duplicate spam on subsequent ticks):
    let tg_msg = tg_rx.try_recv().unwrap_or_default();
    assert!(tg_msg.contains("\u{1f534} [VORTEX-OMEGA V5.5 TOTAL LOSS SHIELD -5.00 USDT]"));
    assert!(tg_rx.try_recv().is_err());

    // 3) Advance across 00:00 UTC rollover: DailySessionGovernor wakes up, YET -5.00 USDT halt stays 100% latched!
    rt.daily_gov.phase = DailyShieldPhase::HibernatingLossCap;
    let day2_ns = TimestampNs(day1_ns.0 + 86_400_000_000_000);
    snaps[0].ts_local_ns = day2_ns;
    snaps[3].ts_local_ns = day2_ns;
    snaps[5].ts_local_ns = day2_ns;
    let (open_d2, pend_d2) = step_12_slot_daemon_tick(
        day2_ns,
        None,
        inst,
        Some(&lock_watch),
        &mut rt,
        &mut last_rest,
        &snaps,
        &mut entry_f,
        &mut engines,
        &mut gov_d,
        &mut ledger,
        shm_reg.arena(),
        &tg_tx,
        &vault_tx,
    );
    assert_eq!(rt.daily_gov.phase, DailyShieldPhase::ActiveHunting);
    assert!(rt.halt_locked && lock_watch.is_halt_locked() && tmp_halt_lock.exists());
    assert_eq!((open_d2, pend_d2), (1, 0));
    assert!(tg_rx.try_recv().is_err());

    // 4) Close open position passively under P0-B custody (Tranche 1 + Runner) -> open_count == 0 -> safe halt standby!
    snaps[5].bids[0] = BookLevel { price: PriceTicks(7_212), qty: LotQty(900) };
    snaps[5].asks[0] = BookLevel { price: PriceTicks(7_213), qty: LotQty(900) };
    snaps[5].mark_price = PriceTicks(7_212);
    snaps[5].last_trade_price = PriceTicks(7_213);
    snaps[5].cum_buy_trade_lots = LotQty(5_000);
    snaps[5].ts_local_ns = TimestampNs(day2_ns.0 + 5_000_000);
    let _ = step_12_slot_daemon_tick(
        TimestampNs(day2_ns.0 + 5_000_000),
        None,
        inst,
        Some(&lock_watch),
        &mut rt,
        &mut last_rest,
        &snaps,
        &mut entry_f,
        &mut engines,
        &mut gov_d,
        &mut ledger,
        shm_reg.arena(),
        &tg_tx,
        &vault_tx,
    );
    snaps[5].bids[0] = BookLevel { price: PriceTicks(7_260), qty: LotQty(900) };
    snaps[5].asks[0] = BookLevel { price: PriceTicks(7_261), qty: LotQty(900) };
    snaps[5].mark_price = PriceTicks(7_260);
    snaps[5].last_trade_price = PriceTicks(7_261);
    snaps[5].cum_buy_trade_lots = LotQty(10_000);
    snaps[5].ts_local_ns = TimestampNs(day2_ns.0 + 10_000_000);
    let (open_done, pend_done) = step_12_slot_daemon_tick(
        TimestampNs(day2_ns.0 + 10_000_000),
        None,
        inst,
        Some(&lock_watch),
        &mut rt,
        &mut last_rest,
        &snaps,
        &mut entry_f,
        &mut engines,
        &mut gov_d,
        &mut ledger,
        shm_reg.arena(),
        &tg_tx,
        &vault_tx,
    );
    assert_eq!((open_done, pend_done), (0, 0));
    assert!(is_safe_halt_standby(rt.halt_locked, open_done, pend_done, false));

    // 5) Startup rehydrate_shadow_ledger_from_paths latches halt from cumulative_net_pnl_1e8 <= -500_000_000 OR lock file:
    let foren_breach = tmp_dir.join("foren_5u_breach.jsonl");
    let missing_lock = tmp_dir.join("nonexistent_halt.lock");
    let breach_row = serde_json::json!({
        "event": "TRADE_CLOSE", "schema": "V5.4.2", "ts_ns": day1_ns.0,
        "symbol": "AVAXUSDT", "side": "Bid", "entry_ticks": 8200, "exit_ticks": 8000,
        "net_bps": -250.0, "fee_bps": 7.5, "net_pnl_1e8": -500_000_000_i64, "fee_1e8": 15_000_000_i64,
        "maker_exit": false
    });
    assert!(std::fs::write(&foren_breach, format!("{breach_row}\n")).is_ok());
    let mut led_boot1 = ParitySyncLedger::new(6_500_000_000);
    let mut eng_boot1 = SlotExecutionEngine::new_12_slot_array();
    let mut gov_boot1 = OnlineAdaptiveGovernor::new();
    assert!(rehydrate_shadow_ledger_from_paths(
        &mut led_boot1,
        &mut eng_boot1,
        &mut gov_boot1,
        shm_reg.arena(),
        &scar_p,
        &loss_p,
        &foren_breach,
        &missing_lock,
    ));

    let mut led_boot2 = ParitySyncLedger::new(6_500_000_000);
    let mut eng_boot2 = SlotExecutionEngine::new_12_slot_array();
    let mut gov_boot2 = OnlineAdaptiveGovernor::new();
    assert!(rehydrate_shadow_ledger_from_paths(
        &mut led_boot2,
        &mut eng_boot2,
        &mut gov_boot2,
        shm_reg.arena(),
        &scar_p,
        &loss_p,
        &foren_p,
        &tmp_halt_lock,
    ));

    // 6) Dashboard formatting & 1:1 OmegaDigitalTwinEngine -5.00 USDT parity:
    assert_eq!(
        format_total_loss_shield_5u_plain(125_000_000, false),
        "Escudo Total 5$ : -1.2500 / -5.0000 USDT (25.0%) | Restante: 3.7500 USDT | ARMED"
    );
    assert_eq!(
        format_total_loss_shield_5u_plain(500_000_000, true),
        "Escudo Total 5$ : -5.0000 / -5.0000 USDT (100.0%) | Restante: 0.0000 USDT | HALTED"
    );
    let mut twin = OmegaDigitalTwinEngine::new();
    twin.ledger.record_closed_trade_with_sinking_split(-500_000_000, 5_000_000, ScaledBps(-2_500));
    let par2_row = vortex_rs::forensic_parquet::ColumnarMarketRow::from_lob_snapshot(
        &snaps[0],
        vortex_rs::forensic_parquet::MarketEventKind::L2BookSnapshot,
        Side::Bid,
        PriceTicks(652_000),
        LotQty(1_500),
    );
    twin.ingest_par2_row(&par2_row);
    assert!(twin.halt_locked);

    drop(worker_tx);
    drop(shm_reg);
    let _ = std::fs::remove_file(&shm_p);
    let _ = std::fs::remove_dir_all(&tmp_dir);
    assert_eq!(
        std::path::Path::new("data/vault/HALTED_ON_LOSS.lock").exists(),
        pre_live_halt_lock_exists
    );
}

#[test]
fn test_v55_multilot_priority_3tranche_cascade_freeride_chain_and_antihostage() {
    use std::sync::mpsc;
    use vortex_rs::alpha_3factor::{
        BtcLeadLagMemory, EppsBtcCorrelationTracker, OrthogonalFactorState, SignalVerdict,
        INV_PHI, INV_PHI_CUBE, INV_PHI_SQ, PHI, PHI_CUBE, PHI_SQ,
    };
    use vortex_rs::bybit_client::InstrumentSpecVerification;
    use vortex_rs::exec_daemon::{
        build_monetary_beta_guard_with_epps, can_stage_same_direction_entry_v542,
        compute_exit_sync_schedule, compute_exit_sync_schedule_elastic,
        compute_offensive_entry_utility_score_x1000_v55, compute_vacuum_l2_l3_c1,
        step_12_slot_daemon_tick,
    };
    use vortex_rs::exec_gateway_handlers::ExecRuntimeState;
    use vortex_rs::execution_fsm::OrderLifecycleState;
    use vortex_rs::exit_quoter::{
        compute_anti_hostage_aging_c1, compute_anti_hostage_horizon_s,
    };
    use vortex_rs::forensic_parquet::{MarketEventKind, ParitySyncLedger};
    use vortex_rs::lob_shm::{MappedShmRegion, MAX_ACTIVE_PAIRS};
    use vortex_rs::markout_tracker::SlotMarkoutRing;
    use vortex_rs::replay_walkforward::{OmegaDigitalTwinEngine, TwinReplayEvent};
    use vortex_rs::telemetry_hud::{SlotForensicEntrySnapshot, VaultIoCommand};

    let t0 = 1_802_000_000_000_000_000_i64;
    record_live_ws_pulse(70_000, 100);
    assert!(core::mem::size_of::<VaultIoCommand>() <= 512);

    // =========================================================================
    // (1) O1: Zero-Alloc Multi-Lot Priority Router at 200.0 Hz & Twin Parity
    // =========================================================================
    let u_1lot = compute_offensive_entry_utility_score_x1000_v55(260, LotQty(1), 1.0, 0.20, 0.55);
    let u_8lot = compute_offensive_entry_utility_score_x1000_v55(260, LotQty(8), 1.0, 0.75, 0.75);
    assert!(u_8lot > u_1lot);

    let shm_p = MappedShmRegion::ephemeral_self_test_path("v55_o1_priority_router");
    let shm_res = MappedShmRegion::open_or_create(&shm_p, true);
    assert!(shm_res.is_ok());
    let Ok(shm_reg) = shm_res else { return };
    let (tg_tx, _tg_rx) = mpsc::sync_channel::<String>(32);
    let (vault_tx, _vault_rx) = mpsc::sync_channel::<VaultIoCommand>(64);
    let inst = InstrumentSpecVerification::baseline_from_specs(&APPROVED_SPECS);
    let mut rt = ExecRuntimeState::new();
    rt.wallet_1e8 = 10_000_000_000;
    let _ = rt.daily_gov.sync_utc_day(TimestampNs(t0), rt.wallet_1e8);

    let mut snaps = [SymbolLobSnapshot::new_empty(); MAX_ACTIVE_PAIRS];
    snaps[0] = mk_vault_snap(0, 652_000, 3_200, 0.12, 0.35, 0.25, t0);
    // Slot 1 ETHUSDT (1-lot indivisible @ $22.37/lot) AND Slot 3 AVAXUSDT (divisible multi-lot @ $8.20)
    // receive simultaneous valid bullish impulses on the same 200 Hz tick:
    snaps[1] = mk_vault_snap(1, 223_700, 6_500, 0.82, 0.86, 0.62, t0);
    snaps[3] = mk_vault_snap(3, 8_200, 6_500, 0.82, 0.86, 0.62, t0);
    // Give AVAX L2/L3 thin ask vacuum so vacuum_l2_l3 > 0:
    snaps[3].asks[1] = BookLevel { price: PriceTicks(8_202), qty: LotQty(120) };
    snaps[3].asks[2] = BookLevel { price: PriceTicks(8_203), qty: LotQty(110) };
    snaps[3].refresh_deep_l1_l5_totals();
    assert!(compute_vacuum_l2_l3_c1(Side::Bid, &snaps[3]) > 0.5);

    let mut entry_f = [SlotForensicEntrySnapshot::default(); MAX_ACTIVE_PAIRS];
    let mut engines = SlotExecutionEngine::new_12_slot_array();
    engines[1].learned_mfe_bps = 920;
    engines[3].learned_mfe_bps = 920;
    let mut gov_o1 = OnlineAdaptiveGovernor::new();
    let mut led_o1 = ParitySyncLedger::new(10_000_000_000);
    let mut last_rest_ns = 0_i64;

    let _ = step_12_slot_daemon_tick(
        TimestampNs(t0),
        None,
        inst,
        None,
        &mut rt,
        &mut last_rest_ns,
        &snaps,
        &mut entry_f,
        &mut engines,
        &mut gov_o1,
        &mut led_o1,
        shm_reg.arena(),
        &tg_tx,
        &vault_tx,
    );
    // O1: Divisible multi-lot AVAX (Slot 3) MUST win admission priority over Slot 1 ETH 1-lot!
    assert!(matches!(
        engines[3].state,
        OrderLifecycleState::PlacingEntryMaker { side: Side::Bid, qty, .. } if qty.0 >= 4
    ));
    assert!(matches!(engines[1].state, OrderLifecycleState::Idle));

    // When ETH is the sole valid candidate, O1 NEVER blocks ETH:
    engines[3].state = OrderLifecycleState::Idle;
    rt.slot_rpc_inflight[3] = false;
    snaps[3].sequence_id = 0;
    last_rest_ns = 0;
    let _ = step_12_slot_daemon_tick(
        TimestampNs(t0 + 50_000_000),
        None,
        inst,
        None,
        &mut rt,
        &mut last_rest_ns,
        &snaps,
        &mut entry_f,
        &mut engines,
        &mut gov_o1,
        &mut led_o1,
        shm_reg.arena(),
        &tg_tx,
        &vault_tx,
    );
    assert!(matches!(
        engines[1].state,
        OrderLifecycleState::PlacingEntryMaker { side: Side::Bid, .. }
    ));

    // 1:1 Digital Twin simultaneous slice priority parity:
    let mut twin_o1 = OmegaDigitalTwinEngine::new();
    twin_o1.engines[1].learned_mfe_bps = 920;
    twin_o1.engines[3].learned_mfe_bps = 920;
    let btc_ev = TwinReplayEvent {
        slot_idx: 0,
        kind: MarketEventKind::L2BookSnapshot,
        aggressor_side: Side::Bid,
        trade_price: PriceTicks(652_000),
        trade_lots: LotQty(10),
        snap: snaps[0],
    };
    twin_o1.ingest_event(&btc_ev);
    snaps[3].sequence_id = 500;
    for warm_dt in [10_000_000_i64, 45_000_000, 80_000_000] {
        let mut s1 = snaps[1];
        let mut s3 = snaps[3];
        s1.ts_local_ns = TimestampNs(t0 + warm_dt);
        s3.ts_local_ns = TimestampNs(t0 + warm_dt);
        if warm_dt < 80_000_000 {
            s1.factor_ofi = BoundedScore::new_clamped(0.10);
            s3.factor_ofi = BoundedScore::new_clamped(0.10);
            twin_o1.ingest_event(&TwinReplayEvent {
                slot_idx: 1,
                kind: MarketEventKind::L2BookSnapshot,
                aggressor_side: Side::Bid,
                trade_price: PriceTicks(223_700),
                trade_lots: LotQty(1),
                snap: s1,
            });
            twin_o1.ingest_event(&TwinReplayEvent {
                slot_idx: 3,
                kind: MarketEventKind::L2BookSnapshot,
                aggressor_side: Side::Bid,
                trade_price: PriceTicks(8_200),
                trade_lots: LotQty(1),
                snap: s3,
            });
        } else {
            let winner_slot = twin_o1.ingest_simultaneous_tick_slice(&[
                TwinReplayEvent {
                    slot_idx: 1,
                    kind: MarketEventKind::L2BookSnapshot,
                    aggressor_side: Side::Bid,
                    trade_price: PriceTicks(223_700),
                    trade_lots: LotQty(1),
                    snap: s1,
                },
                TwinReplayEvent {
                    slot_idx: 3,
                    kind: MarketEventKind::L2BookSnapshot,
                    aggressor_side: Side::Bid,
                    trade_price: PriceTicks(8_200),
                    trade_lots: LotQty(1),
                    snap: s3,
                },
            ]);
            assert_eq!(winner_slot, Some(3));
        }
    }

    // =========================================================================
    // (2) O2: BTCUSDT Lead-Lag Ignition on Unified EMA Horizon
    // =========================================================================
    let tau_70ms = BtcLeadLagMemory::unified_horizon_tau_ll_s(70_000);
    let tau_250ms = BtcLeadLagMemory::unified_horizon_tau_ll_s(250_000);
    assert!((tau_70ms - INV_PHI_SQ).abs() < 1e-9);
    assert!((tau_250ms - PHI_SQ * 0.250).abs() < 1e-9);

    let mut ll_mem = BtcLeadLagMemory::new();
    let btc_t0 = mk_vault_snap(0, 650_000, 3_400, 0.25, 0.25, 0.20, t0);
    let sol_t0 = mk_vault_snap(2, 7_500, 4_200, 0.32, 0.34, 0.35, t0);
    let _ = ll_mem.observe_btc_snapshot_with_rtt(&btc_t0, 70_000);
    let _ = ll_mem.observe_slot_snapshot(2, &sol_t0, 70_000);

    // BTC jumps +8.5 bps in 45ms while SOL has not repriced yet:
    let mut btc_jump = mk_vault_snap(0, 650_553, 3_400, 0.72, 0.78, 0.52, t0 + 45_000_000);
    btc_jump.mark_price = PriceTicks(650_553);
    let mut sol_lag = sol_t0;
    sol_lag.ts_local_ns = TimestampNs(t0 + 45_000_000);
    let lag_bps = ll_mem.undiscounted_lead_lag_bps(2, Side::Bid, &sol_lag, &btc_jump, 70_000, 1.15);
    assert!(lag_bps > PHI * 4.0);
    let bonus_o2 = ll_mem.compute_lead_lag_ignition_bonus_c1(
        2,
        Side::Bid,
        &sol_lag,
        &btc_jump,
        70_000,
        1.15,
        0.78,
        1.0,
        FeeSchedule::VIP0_DEFAULT,
    );
    assert!(bonus_o2 > 0.20 && bonus_o2 <= INV_PHI_CUBE + 1e-12);

    // Marginal SOL snapshot fails without lead-lag ignition bonus and arms cleanly WITH bonus:
    let verd_no_bonus = OrthogonalFactorState::evaluate_entry_signal_v55(
        APPROVED_SPECS[2],
        &sol_lag,
        &btc_jump,
        ScaledBps(780),
        ScaledBps(120),
        50,
        FeeSchedule::VIP0_DEFAULT,
        0.0,
        1.15,
        0.78,
        Some(0.75),
        1.0,
        0.0,
    );
    let verd_with_bonus = OrthogonalFactorState::evaluate_entry_signal_v55(
        APPROVED_SPECS[2],
        &sol_lag,
        &btc_jump,
        ScaledBps(780),
        ScaledBps(120),
        50,
        FeeSchedule::VIP0_DEFAULT,
        0.0,
        1.15,
        0.78,
        Some(0.75),
        1.0,
        bonus_o2,
    );
    assert_eq!(verd_no_bonus, SignalVerdict::NeutralNoAction);
    assert!(matches!(
        verd_with_bonus,
        SignalVerdict::AuthorizeMakerEntry { side: Side::Bid, .. }
    ));
    let gov_o2 = OnlineAdaptiveGovernor::new();
    let lots_o2 = gov_o2.authorizes_adaptive_entry_v55(
        2,
        Side::Bid,
        6_500_000_000,
        1_000,
        ScaledBps(780),
        &sol_lag,
        &btc_jump,
        0.0,
        1.15,
        0.78,
        Some(0.75),
        bonus_o2,
    );
    assert!(lots_o2.is_ok());

    // Iceberg trap (high aggressive TFI absorbed by passive wall, weak OFI) OR already-discounted follower move yields strictly 0.0 bonus:
    let mut sol_iceberg = sol_lag;
    sol_iceberg.factor_ofi = BoundedScore::new_clamped(0.05);
    sol_iceberg.factor_tfi = BoundedScore::new_clamped(0.85);
    assert_eq!(
        ll_mem.compute_lead_lag_ignition_bonus_c1(
            2,
            Side::Bid,
            &sol_iceberg,
            &btc_jump,
            70_000,
            1.15,
            0.78,
            1.0,
            FeeSchedule::VIP0_DEFAULT,
        ),
        0.0
    );
    let sol_repriced = mk_vault_snap(2, 7_510, 3_600, 0.55, 0.55, 0.35, t0 + 45_000_000);
    assert_eq!(
        ll_mem.compute_lead_lag_ignition_bonus_c1(
            2,
            Side::Bid,
            &sol_repriced,
            &btc_jump,
            70_000,
            1.15,
            0.78,
            1.0,
            FeeSchedule::VIP0_DEFAULT,
        ),
        0.0
    );

    // =========================================================================
    // (3) O3: Golden 3-Tranche Cascade (T1 + T2 + T3) & cont_ratio_x1000 Learning
    // =========================================================================
    let mut gov_o3 = OnlineAdaptiveGovernor::new();
    assert_eq!(gov_o3.slot_cont_ratio_x1000(3, Side::Bid), 1_000);

    // Short pair (Slot 10 NEAR) converges toward ~618 permille T1 harvest:
    let w_warm = WaveObservables {
        regime: WaveRegime::NeutralWarmup,
        climax_active: false,
        hurst: WaveObservables::encode_hurst(0.55),
        macro_phase: BoundedScore::ZERO,
        oi_impulse: BoundedScore::ZERO,
        liq_imbalance: BoundedScore::ZERO,
        fib_proj_bps: ScaledBps(0),
    };
    for k in 0..8_i64 {
        gov_o3.observe_closed_trade_with_regime_v51(
            10,
            Side::Bid,
            PriceTicks(3_500),
            ScaledBps(80),
            ScaledBps(370),
            true,
            w_warm,
            1.0,
            TimestampNs(t0 + k * 60_000_000_000),
        );
    }
    assert!(gov_o3.slot_cont_ratio_x1000(10, Side::Bid) < 800);
    assert_eq!(gov_o3.learned_tranche1_fraction_permille(10, Side::Bid, 520), 618);

    // Explosive pair (Slot 3 AVAX) updated via +60s SlotMarkoutRing resolution & positive closes -> ~382 permille T1:
    let mut mk_ring = SlotMarkoutRing::new();
    mk_ring.arm_fill_probe(Side::Bid, PriceTicks(8_200), PriceTicks(8_200), TimestampNs(t0));
    for step_s in [1_i64, 2, 6, 16, 61] {
        mk_ring.step_clock(PriceTicks(8_216), TimestampNs(t0 + step_s * 1_000_000_000), 200);
    }
    let resolved_60s = mk_ring.take_resolved_60s_fill_markout();
    assert_eq!(resolved_60s.map(|x| x.0), Some(Side::Bid));
    let fav_60s = resolved_60s.map_or(0, |x| x.1);
    assert!(fav_60s > 1_500);
    for _ in 0..8 {
        let _ = gov_o3.observe_markout_60s_continuation(3, Side::Bid, 1_850, fav_60s, ScaledBps(3_600));
    }
    let cr_avax = gov_o3.slot_cont_ratio_x1000(3, Side::Bid);
    assert!(cr_avax > 1_618 && cr_avax <= 2_618);
    assert_eq!(gov_o3.learned_tranche1_fraction_permille(3, Side::Bid, 1_650), 382);

    // Retrocompatible GOV_SCAR_SNAPSHOT persistence & legacy snapshot fallback:
    let scar_json = gov_o3.to_scar_snapshot_json(TimestampNs(t0 + 600_000_000_000));
    let mut gov_rehyd = OnlineAdaptiveGovernor::new();
    assert!(gov_rehyd.apply_scar_snapshot_json(&scar_json).is_some());
    assert_eq!(gov_rehyd.slot_cont_ratio_x1000(3, Side::Bid), cr_avax);
    assert_eq!(
        gov_rehyd.slot_cont_ratio_x1000(10, Side::Bid),
        gov_o3.slot_cont_ratio_x1000(10, Side::Bid)
    );
    let mut legacy_scar = scar_json;
    if let Some(obj) = legacy_scar.as_object_mut() {
        let _ = obj.remove("cont_ratio_x1000");
    }
    let mut gov_legacy = OnlineAdaptiveGovernor::new();
    assert!(gov_legacy.apply_scar_snapshot_json(&legacy_scar).is_some());
    assert_eq!(gov_legacy.slot_cont_ratio_x1000(3, Side::Bid), 1_000);

    // Full 3-Tranche Cascade (T1 -> T2 -> T3) in SlotExecutionEngine:
    let mut eng_c3 = SlotExecutionEngine::new(3);
    eng_c3.learned_mfe_bps = 1_750;
    eng_c3.sync_governor_for_side(&gov_o3, Side::Bid);
    let mut led_c3 = ParitySyncLedger::new(6_500_000_000);
    let mut snap_c3 = mk_vault_snap(3, 8_200, 3_600, 0.62, 0.66, 0.45, t0);
    snap_c3.set_wave_observables(WaveObservables {
        regime: WaveRegime::ImpulseWave3,
        climax_active: false,
        hurst: WaveObservables::encode_hurst(0.64),
        macro_phase: BoundedScore::new_clamped(0.75),
        oi_impulse: BoundedScore::new_clamped(0.35),
        liq_imbalance: BoundedScore::new_clamped(0.45),
        fib_proj_bps: ScaledBps(2_200),
    });
    assert!(eng_c3
        .try_arm_maker_entry(
            &snap_c3,
            SignalVerdict::AuthorizeMakerEntry {
                side: Side::Bid,
                target_excursion_scaled_bps: ScaledBps(1_750),
            },
            LotQty(10),
        )
        .is_ok());
    assert!(eng_c3.on_maker_entry_filled(TimestampNs(t0)).is_ok());

    // Step 1: T1 partial harvest (~38.2% of 10 -> 4 lots, rem_lots = 6):
    let q_t1_opt = eng_c3.compute_canonical_exit_quote(&snap_c3);
    assert!(q_t1_opt.is_some());
    let Some(q_t1) = q_t1_opt else { return };
    assert_eq!(q_t1.role, ExitOrderRole::Tranche1Harvest50);
    assert!(q_t1.order_lots.0 >= 3 && q_t1.order_lots.0 <= 4);
    snap_c3.ts_local_ns = TimestampNs(t0 + 2_000_000_000);
    snap_c3.bids[0].price = PriceTicks(q_t1.target_price.0 - 1);
    snap_c3.asks[0].price = q_t1.target_price;
    snap_c3.mark_price = q_t1.target_price;
    snap_c3.last_trade_price = q_t1.target_price;
    assert!(eng_c3.step_open_position(&snap_c3, &snaps[0], &gov_o3, &mut led_c3).is_none());
    assert_eq!(eng_c3.tranche1_harvests_count, 1);

    // Step 2: T2 golden intermediate harvest (round(INV_PHI * rem_lots), leaving T3 Shadow-Runner):
    let q_t2_opt = eng_c3.compute_canonical_exit_quote(&snap_c3);
    assert!(q_t2_opt.is_some());
    let Some(q_t2) = q_t2_opt else { return };
    assert_eq!(q_t2.role, ExitOrderRole::Tranche2HarvestPhi);
    assert_eq!(q_t2.sub_reason, ExitSubReason::Tranche2HarvestPhi);
    let rem_before_t2 = match eng_c3.state {
        OrderLifecycleState::PositionOpenMakerExit { open_lots, .. } => open_lots.0,
        _ => 0,
    };
    let expected_t2_lots = ((INV_PHI * (rem_before_t2 as f64)).round() as i64).clamp(1, rem_before_t2 - 1);
    assert_eq!(q_t2.order_lots, LotQty(expected_t2_lots));
    snap_c3.ts_local_ns = TimestampNs(t0 + 5_000_000_000);
    snap_c3.bids[0].price = PriceTicks(q_t2.target_price.0 - 1);
    snap_c3.asks[0].price = q_t2.target_price;
    snap_c3.mark_price = q_t2.target_price;
    snap_c3.last_trade_price = q_t2.target_price;
    snap_c3.set_wave_observables(WaveObservables {
        regime: WaveRegime::ImpulseWave3,
        climax_active: false,
        hurst: WaveObservables::encode_hurst(0.64),
        macro_phase: BoundedScore::new_clamped(0.78),
        oi_impulse: BoundedScore::new_clamped(0.35),
        liq_imbalance: BoundedScore::new_clamped(0.45),
        fib_proj_bps: ScaledBps(850),
    });
    assert!(eng_c3.step_open_position(&snap_c3, &snaps[0], &gov_o3, &mut led_c3).is_none());
    assert!(eng_c3.tranche2_harvested && eng_c3.tranche2_harvests_count == 1);
    assert!(eng_c3.ratchet_locked_gross_bps >= 500);
    assert!(eng_c3.lvdbs_deep_surfs_count >= 1);

    // Step 3: T3 Uncapped Shadow-Runner closes remaining lots at impulse crest -> 3 consolidated legs:
    let q_t3_opt = eng_c3.compute_canonical_exit_quote(&snap_c3);
    assert!(q_t3_opt.is_some());
    let Some(q_t3) = q_t3_opt else { return };
    assert_eq!(q_t3.role, ExitOrderRole::LvdbsDeepSurf50);
    let crest_px = PriceTicks(q_t3.target_price.0.max(8_224));
    snap_c3.ts_local_ns = TimestampNs(t0 + 9_000_000_000);
    snap_c3.bids[0].price = PriceTicks(crest_px.0 - 1);
    snap_c3.asks[0].price = crest_px;
    snap_c3.mark_price = crest_px;
    snap_c3.last_trade_price = crest_px;
    snap_c3.factor_ofi = BoundedScore::new_clamped(-0.15);
    snap_c3.factor_tfi = BoundedScore::new_clamped(-0.15);
    snap_c3.factor_geom = BoundedScore::ZERO;
    snap_c3.set_wave_observables(WaveObservables {
        regime: WaveRegime::ImpulseWave3,
        climax_active: false,
        hurst: WaveObservables::encode_hurst(0.64),
        macro_phase: BoundedScore::new_clamped(0.82),
        oi_impulse: BoundedScore::new_clamped(0.30),
        liq_imbalance: BoundedScore::new_clamped(0.40),
        fib_proj_bps: ScaledBps(10),
    });
    let sum_3leg_opt = eng_c3.step_open_position(&snap_c3, &snaps[0], &gov_o3, &mut led_c3);
    assert!(sum_3leg_opt.is_some());
    let Some(sum_3leg) = sum_3leg_opt else { return };
    assert_eq!(sum_3leg.legs_consolidated, 3);
    assert_eq!(sum_3leg.closed_lots, LotQty(10));
    assert_eq!(eng_c3.last_closed_legs_fee_1e8, sum_3leg.fee_1e8);
    assert!(sum_3leg.net_pnl_1e8 > 0 && led_c3.cumulative_net_pnl_1e8 == sum_3leg.net_pnl_1e8);

    // Reversal after T1 before T2 closes cleanly in 2 legs (preserving V17 invariant):
    let mut eng_2leg = SlotExecutionEngine::new(3);
    eng_2leg.learned_mfe_bps = 1_750;
    eng_2leg.sync_governor_for_side(&gov_o3, Side::Bid);
    let mut snap_2l = mk_vault_snap(3, 8_200, 3_600, 0.62, 0.66, 0.45, t0 + 20_000_000_000);
    assert!(eng_2leg
        .try_arm_maker_entry(
            &snap_2l,
            SignalVerdict::AuthorizeMakerEntry {
                side: Side::Bid,
                target_excursion_scaled_bps: ScaledBps(1_750),
            },
            LotQty(10),
        )
        .is_ok());
    assert!(eng_2leg.on_maker_entry_filled(TimestampNs(t0 + 20_000_000_000)).is_ok());
    let qt1_rev_opt = eng_2leg.compute_canonical_exit_quote(&snap_2l);
    assert!(qt1_rev_opt.is_some());
    let Some(qt1_rev) = qt1_rev_opt else { return };
    assert!(eng_2leg.record_tranche1_harvest(qt1_rev.target_price));
    snap_2l.ts_local_ns = TimestampNs(t0 + 24_000_000_000);
    snap_2l.factor_ofi = BoundedScore::new_clamped(-0.65);
    snap_2l.factor_tfi = BoundedScore::new_clamped(-0.65);
    let sum_2leg_opt = eng_2leg.finalize_open_position_close(
        ClosedTradeKind::MakerPassiveUnwind,
        qt1_rev.target_price,
        snap_2l.ts_local_ns,
        &mut led_c3,
    );
    assert!(sum_2leg_opt.is_some());
    let Some(sum_2leg) = sum_2leg_opt else { return };
    assert_eq!(sum_2leg.legs_consolidated, 2);

    // =========================================================================
    // (4) O4: House-Money Recycling (Free-Ride Risk & Same-Side Stagger Unlock)
    // =========================================================================
    let mut hm_engines = SlotExecutionEngine::new_12_slot_array();
    let mut hm_snaps = [SymbolLobSnapshot::new_empty(); MAX_ACTIVE_PAIRS];
    let hm_epps = [EppsBtcCorrelationTracker::new(); MAX_ACTIVE_PAIRS];
    for &s_i in &[2_usize, 3, 5] {
        hm_snaps[s_i] = mk_vault_snap(s_i, 7_500, 3_200, 0.55, 0.60, 0.40, t0);
        assert!(hm_engines[s_i]
            .adopt_live_open_position(
                Side::Bid,
                PriceTicks(7_500),
                LotQty(10),
                LotQty(10),
                false,
                PriceTicks(7_500),
                PriceTicks(7_500),
                TimestampNs(t0),
                &hm_snaps[s_i],
            )
            .is_ok());
    }
    let guard_full = build_monetary_beta_guard_with_epps(
        6_500_000_000,
        &hm_engines,
        &hm_snaps,
        &hm_epps,
        FeeSchedule::VIP0_DEFAULT,
    );
    assert!(!guard_full.can_open_with_risk_ppm(9, Side::Bid, 8_000, 900));

    // Harvest T1 + ratchet SL to breakeven on slots 2 & 3 -> residual_risk_1e8 == 0 (Free-Ride!):
    for &s_i in &[2_usize, 3] {
        assert!(hm_engines[s_i].record_tranche1_harvest(PriceTicks(7_510)));
        hm_engines[s_i].last_eff_sl_ticks = PriceTicks(7_504);
        assert!(hm_engines[s_i].is_house_money_free_ride());
        assert_eq!(hm_engines[s_i].house_money_residual_risk_1e8_and_s3_scale(), (0, 0.0));
    }
    let guard_freeride = build_monetary_beta_guard_with_epps(
        6_500_000_000,
        &hm_engines,
        &hm_snaps,
        &hm_epps,
        FeeSchedule::VIP0_DEFAULT,
    );
    assert!(guard_freeride.is_slot_free_ride(2) && guard_freeride.is_slot_free_ride(3));
    assert!(!guard_freeride.is_slot_free_ride(5));
    assert!(guard_freeride.total_open_risk_ppm() < guard_full.total_open_risk_ppm() / 2);
    assert!(guard_freeride.can_open_with_risk_ppm(9, Side::Bid, 8_000, 900));

    // Stagger unlock: even with 2 resting entries if converted to Free-Ride open slots, new divisible shot is unlocked;
    // AND hard physical cap <= 6 open slots STILL blocks a 7th slot even when all 6 are Free-Ride:
    for &s_i in &[4_usize, 6, 7] {
        hm_snaps[s_i] = mk_vault_snap(s_i, 7_500, 3_200, 0.55, 0.60, 0.40, t0);
        assert!(hm_engines[s_i]
            .adopt_live_open_position(
                Side::Bid,
                PriceTicks(7_500),
                LotQty(10),
                LotQty(10),
                false,
                PriceTicks(7_500),
                PriceTicks(7_500),
                TimestampNs(t0),
                &hm_snaps[s_i],
            )
            .is_ok());
        assert!(hm_engines[s_i].record_tranche1_harvest(PriceTicks(7_510)));
        hm_engines[s_i].last_eff_sl_ticks = PriceTicks(7_504);
    }
    assert!(hm_engines[5].record_tranche1_harvest(PriceTicks(7_510)));
    hm_engines[5].last_eff_sl_ticks = PriceTicks(7_504);
    let guard_six = build_monetary_beta_guard_with_epps(
        6_500_000_000,
        &hm_engines,
        &hm_snaps,
        &hm_epps,
        FeeSchedule::VIP0_DEFAULT,
    );
    assert_eq!(guard_six.open_counts().2, 6);
    assert_eq!(guard_six.total_open_risk_ppm(), 0);
    assert!(!guard_six.can_open_with_risk_ppm(9, Side::Bid, 5_000, 800));
    assert!(can_stage_same_direction_entry_v542(
        9,
        Side::Bid,
        TimestampNs(t0 + 100_000_000),
        &hm_engines,
        &hm_snaps,
        &hm_epps,
        1.0,
        50,
    ));

    // =========================================================================
    // (5) O5: Momentum Chain Re-Entry Cooldown Compression on Clean Maker Win
    // =========================================================================
    let mut gov_o5 = OnlineAdaptiveGovernor::new();
    let mut eng_o5 = SlotExecutionEngine::new(2);
    let close_ts = TimestampNs(t0 + 100_000_000_000);
    gov_o5.set_slot_momentum_chain_context(2, 0.78, false, 70_000);
    gov_o5.observe_closed_trade_with_regime_v51(
        2,
        Side::Bid,
        PriceTicks(7_500),
        ScaledBps(680),
        ScaledBps(1_200),
        true,
        w_warm,
        1.0,
        close_ts,
    );
    eng_o5.cooldown_until_ns = TimestampNs(close_ts.0 + 3_000_000_000);
    eng_o5.apply_momentum_chain_post_close_cooldown(
        close_ts,
        true,
        ScaledBps(680),
        false,
        gov_o5.psi_health(),
        0.78,
        70_000,
    );
    let expected_settle_ns = (PHI_SQ * 70_000_000.0).round() as i64;
    assert_eq!(
        gov_o5.directional_cooldown_until_ns_for(2, Side::Bid) - close_ts.0,
        expected_settle_ns
    );
    assert_eq!(eng_o5.cooldown_until_ns.0 - close_ts.0, expected_settle_ns);

    // Scratch, Climax, and SL Loss MUST preserve 100% uncompressed cooldowns:
    let close_ts2 = TimestampNs(close_ts.0 + 50_000_000_000);
    let w_climax = WaveObservables {
        regime: WaveRegime::ClimaxExhaustion,
        climax_active: true,
        ..w_warm
    };
    gov_o5.set_slot_momentum_chain_context(5, 0.80, true, 70_000);
    gov_o5.observe_closed_trade_with_regime_v51(
        5,
        Side::Bid,
        PriceTicks(7_200),
        ScaledBps(680),
        ScaledBps(1_200),
        true,
        w_climax,
        1.0,
        close_ts2,
    );
    assert!(gov_o5.directional_cooldown_until_ns_for(5, Side::Bid) - close_ts2.0 > 10_000_000_000);
    gov_o5.observe_closed_trade_at_entry(6, Side::Bid, PriceTicks(2_800), ScaledBps(40), true, close_ts2);
    assert!(gov_o5.directional_cooldown_until_ns_for(6, Side::Bid) - close_ts2.0 > 30_000_000_000);
    gov_o5.observe_closed_trade_at_entry(7, Side::Bid, PriceTicks(14_000), ScaledBps(-1_400), false, close_ts2);
    assert!(gov_o5.directional_cooldown_until_ns_for(7, Side::Bid) - close_ts2.0 >= 250_000_000_000);

    // =========================================================================
    // (6) O6: Sub-Second Crest Snap-In Coupled to ws_rtt_us & ws_quota_rem
    // =========================================================================
    let crest_quote = vortex_rs::exit_quoter::CanonicalExitQuote {
        role: ExitOrderRole::LvdbsDeepSurf50,
        target_price: PriceTicks(7_520),
        order_lots: LotQty(5),
        sub_reason: ExitSubReason::ShadowRunnerCrestHarvest,
        vac_level: 1,
        defensive_lock_active: false,
        urgent_close_repeg: false,
        cur_gross_bps: 1_800,
        wald_pct: 0,
    };
    let (preempt_hi, delay_hi) = compute_exit_sync_schedule_elastic(
        &crest_quote,
        PriceTicks(7_545),
        PriceTicks(7_500),
        70_000,
        100,
    );
    let (_, delay_low_q) = compute_exit_sync_schedule_elastic(
        &crest_quote,
        PriceTicks(7_545),
        PriceTicks(7_500),
        70_000,
        10,
    );
    assert!(preempt_hi);
    assert!((delay_hi - (PHI * 70_000_000.0).round() as i64).abs() <= 2);
    assert!(delay_low_q > delay_hi && delay_low_q < 350_000_000);

    let float_quote = vortex_rs::exit_quoter::CanonicalExitQuote {
        sub_reason: ExitSubReason::LvdbsDeepVacuumSurf,
        urgent_close_repeg: false,
        ..crest_quote
    };
    let (preempt_fl, delay_fl) = compute_exit_sync_schedule_elastic(
        &float_quote,
        PriceTicks(7_528),
        PriceTicks(7_500),
        70_000,
        100,
    );
    let min_fl = (PHI_CUBE * 70_000_000.0).round() as i64;
    let max_fl = ((PHI_CUBE * PHI_SQ) * 70_000_000.0).round() as i64;
    assert!(!preempt_fl && delay_fl >= min_fl && delay_fl <= max_fl);
    let _ = compute_exit_sync_schedule(&float_quote, PriceTicks(7_528), PriceTicks(7_500));

    // =========================================================================
    // (7) O7: Diffusive Anti-Hostage Expiration & 1:1 Digital Twin Non-Regression
    // =========================================================================
    let tau_h0 = compute_anti_hostage_horizon_s(120, 0);
    let tau_h1 = compute_anti_hostage_horizon_s(180, 1);
    assert!(tau_h0 > 390.0 && tau_h0 < 600.0);
    assert!(tau_h1 > 1_100.0 && tau_h1 < 1_300.0);
    assert_eq!(compute_anti_hostage_aging_c1(180.0, 180.0, tau_h0), 0.0);
    assert_eq!(compute_anti_hostage_aging_c1(700.0, 30.0, tau_h0), 0.0);
    assert!(compute_anti_hostage_aging_c1(900.0, 900.0, tau_h0) > 0.99);

    let mut eng_ah = SlotExecutionEngine::new(3);
    eng_ah.intrinsic_tau_mkt_s = 120;
    let mut snap_ah = mk_vault_snap(3, 8_200, 3_600, 0.05, 0.05, 0.02, t0);
    assert!(eng_ah
        .adopt_live_open_position(
            Side::Bid,
            PriceTicks(8_200),
            LotQty(6),
            LotQty(6),
            false,
            PriceTicks(8_201),
            PriceTicks(8_188),
            TimestampNs(t0),
            &snap_ah,
        )
        .is_ok());
    let init_sl = eng_ah.last_eff_sl_ticks;
    // Early life (+150s): zero aging, initial diffusive SL amplitude 100% untouched:
    snap_ah.ts_local_ns = TimestampNs(t0 + 150_000_000_000);
    assert_eq!(eng_ah.current_anti_hostage_aging_c1(snap_ah.ts_local_ns), 0.0);
    assert!(eng_ah.maybe_ratchet_chandelier_or_wald_sl(&snap_ah, &snaps[0]).is_none());
    assert_eq!(eng_ah.last_eff_sl_ticks, init_sl);

    // Multi-minute stagnation (+950s < 8h funding, zero new peak):
    // (1) opportunistic passive Maker exit in [-fee_rt_mkr, +zero_loss_floor], AND
    // (2) monotonic Server MarkPrice SL contraction toward diffusive floor!
    snap_ah.ts_local_ns = TimestampNs(t0 + 950_000_000_000);
    snap_ah.bids[0].price = PriceTicks(8_198);
    snap_ah.asks[0].price = PriceTicks(8_199);
    snap_ah.mark_price = PriceTicks(8_198);
    snap_ah.last_trade_price = PriceTicks(8_198);
    assert!(eng_ah.current_anti_hostage_aging_c1(snap_ah.ts_local_ns) > 0.99);
    let q_aged_opt = eng_ah.compute_canonical_exit_quote(&snap_ah);
    assert!(q_aged_opt.is_some());
    let Some(q_aged) = q_aged_opt else { return };
    assert!(q_aged.urgent_close_repeg && q_aged.target_price == PriceTicks(8_199));
    let aged_sl_opt = eng_ah.maybe_ratchet_chandelier_or_wald_sl(&snap_ah, &snaps[0]);
    assert!(aged_sl_opt.is_some());
    let Some(aged_sl) = aged_sl_opt else { return };
    assert!(aged_sl.0 > init_sl.0 && aged_sl.0 < 8_198);

    // Constitutional 1:1 Digital Twin & SCAR-LOCK 63/63 non-regression check:
    assert_eq!(
        OnlineAdaptiveGovernor::audit_unified_62_losses_and_2160_manifold(),
        (UNIFIED_LOSS_LAB_COUNT, 2_160)
    );
    let twin_rep = run_omega_digital_twin_walk_forward_lab();
    assert_eq!(twin_rep.closed_trades, 3);
    assert_eq!(twin_rep.cum_net_bps_x100, 9_873);
    assert_eq!(twin_rep.hawkes_queue_aborts, 1);
    assert_eq!(twin_rep.post_only_rejects, 0);
    assert_eq!(twin_rep.parity_sync_drift_1e8, 0);

    drop(shm_reg);
    let _ = std::fs::remove_file(&shm_p);
}

#[test]
fn test_v552_live_macro_ob_horizon_tick_hurst_intraday_reversal_and_multilot_qstruct() {
    use core::sync::atomic::Ordering;
    use std::sync::mpsc;
    use vortex_rs::alpha_3factor::{
        compute_corrective_abc_ob_reach_sbps, compute_corrective_abc_structural_threat_c1,
        compute_passive_iceberg_absorption_penalty_c1, compute_s_opp_ob_wall_c1,
        is_opposing_impulse_wave3_entry_hazard_v552, is_opposing_impulse_wave3_hazard_dynamic,
        passes_corrective_abc_snapshot_guard_with_health_c1, OrthogonalFactorState, SignalVerdict,
        INV_PHI, INV_PHI_CUBE, INV_PHI_SQ,
    };
    use vortex_rs::bybit_client::InstrumentSpecVerification;
    use vortex_rs::exec_daemon::{compute_vacuum_l2_l3_c1, step_12_slot_daemon_tick};
    use vortex_rs::exec_gateway_handlers::ExecRuntimeState;
    use vortex_rs::execution_fsm::{
        evaluate_macro_coherence_surface_with_health_c1,
        passes_elliott_macro_coherence_guard_with_health, OrderLifecycleState,
    };
    use vortex_rs::forensic_parquet::ParitySyncLedger;
    use vortex_rs::lob_shm::{MappedShmRegion, MAX_ACTIVE_PAIRS};
    use vortex_rs::telemetry_hud::{SlotForensicEntrySnapshot, VaultIoCommand};
    use vortex_rs::types::{compute_multilot_q_struct_c1, coupled_c1_effective_mfe_raw};

    let t0 = 1_805_000_000_000_000_000_i64;
    let fees = FeeSchedule::VIP0_DEFAULT;
    record_live_ws_pulse(70_000, 100);

    // =========================================================================
    // 1. Physical Bounding of Universal Iceberg Guard (ofi_al < INV_PHI_CUBE vs >= INV_PHI_SQ)
    // =========================================================================
    assert!(
        compute_passive_iceberg_absorption_penalty_c1(0.20, 0.72, -0.10) > 0.90,
        "Genuine passive iceberg absorption (ofi_al < INV_PHI_CUBE) must remain > 0.90"
    );
    assert!(
        compute_passive_iceberg_absorption_penalty_c1(INV_PHI_CUBE, 0.88, -0.20).abs()
            <= f64::EPSILON
    );
    assert!(
        compute_passive_iceberg_absorption_penalty_c1(0.43, 0.93, -0.15).abs() <= f64::EPSILON,
        "SUI live OFI +0.43 / TFI +0.93 (>= INV_PHI_SQ) must have 0.0 iceberg penalty"
    );
    assert!(
        compute_passive_iceberg_absorption_penalty_c1(0.48, 0.86, 0.45).abs() <= f64::EPSILON
    );
    // C1 continuity scan across ofi_al in [0.15, 0.45]:
    let mut prev_ice = compute_passive_iceberg_absorption_penalty_c1(0.15, 0.86, -0.10);
    for step in 1..=300 {
        let x = 0.15 + 0.001 * f64::from(step);
        let cur_ice = compute_passive_iceberg_absorption_penalty_c1(x, 0.86, -0.10);
        assert!((cur_ice - prev_ice).abs() < 0.15);
        prev_ice = cur_ice;
    }

    // =========================================================================
    // 2. Multi-Lot (lots >= 2) q_struct Unlock vs Intact 1-Lot BTC/ETH Selectivity
    // =========================================================================
    let q_1lot = compute_multilot_q_struct_c1(LotQty(1), 0.0, 0.48, 0.86, 0.70);
    assert!(q_1lot.abs() <= f64::EPSILON);
    let q_multilot = compute_multilot_q_struct_c1(LotQty(12), 0.0, 0.48, 0.86, 0.70);
    assert!(q_multilot > INV_PHI_SQ);
    let unlocked_mfe = coupled_c1_effective_mfe_raw(
        ScaledBps(680),
        ScaledBps(1_520),
        BoundedScore::new_clamped(q_multilot),
    );
    assert!(unlocked_mfe > 680);
    // 1-lot ETH with weak geom=0.14 and learned_mfe=680 stays strictly vetoed in Pillar 2:
    assert!(APPROVED_SPECS[1]
        .evaluate_pilar2_asymmetry_ev_with_multilot_flow_sbps(
            LotQty(1),
            PriceTicks(182_000),
            ScaledBps(4_200),
            680,
            50,
            0.14,
            520,
            520,
            Side::Bid,
            fees,
            0.48,
            0.86,
            0.45,
            0.70,
        )
        .is_none());

    // =========================================================================
    // 3. Live Production Snapshot (SUIUSDT slot 5 & SOLUSDT slot 2):
    //    wave_hurst = 0.72, ob_opp @ 95 bps (score = 0.72) + hydrated aligned OB,
    //    geom = 0.14, OFI = +0.48, TFI = +0.86, BTC OFI/TFI = +0.45,
    //    epps_corr = 0.75, learned_mfe = 680 -> MUST ARM ENTRY (Arm > 0)!
    // =========================================================================
    let mut btc_live = mk_vault_snap(0, 652_000, 4_200, 0.45, 0.45, 0.14, t0);
    btc_live.bids[0].qty = LotQty(1_600);
    btc_live.asks[0].qty = LotQty(1_400);
    btc_live.refresh_deep_l1_l5_totals();

    let w_live_abc = WaveObservables {
        regime: WaveRegime::CorrectiveAbc,
        climax_active: false,
        hurst: WaveObservables::encode_hurst(0.72),
        macro_phase: BoundedScore::new_clamped(-0.18),
        oi_impulse: BoundedScore::ZERO,
        liq_imbalance: BoundedScore::ZERO,
        fib_proj_bps: ScaledBps(0),
    };

    for &(slot_idx, px_ticks) in &[(5_usize, 7_350_i64), (2_usize, 7_520_i64)] {
        let mut alt_live = mk_vault_snap(slot_idx, px_ticks, 4_500, 0.48, 0.86, 0.14, t0);
        // Realistic L2/L3 ask vacuum on Bid breakout:
        alt_live.bids[0].qty = LotQty(1_200);
        alt_live.asks[0].qty = LotQty(950);
        alt_live.asks[1] = BookLevel { price: PriceTicks(px_ticks + 2), qty: LotQty(110) };
        alt_live.asks[2] = BookLevel { price: PriceTicks(px_ticks + 3), qty: LotQty(120) };
        alt_live.refresh_deep_l1_l5_totals();
        // Hydrated vortex_tap 25h 5m + 4h 1m OBs: own bull OB @ 28 bps + distant bear OB @ 95 bps (score = 0.72):
        alt_live.ob_bull_score = BoundedScore::new_clamped(0.54);
        alt_live.ob_dist_bull_bps = ScaledBps(2_800);
        alt_live.ob_bear_score = BoundedScore::new_clamped(0.72);
        alt_live.ob_dist_bear_bps = ScaledBps(9_500);
        alt_live.set_wave_observables(w_live_abc);

        let reach_sbps = compute_corrective_abc_ob_reach_sbps(&alt_live, 680, 50, fees);
        assert!((1_800.0..=6_200.0).contains(&reach_sbps));
        let s_opp_live = compute_s_opp_ob_wall_c1(Side::Bid, &alt_live, 0, 50, fees);
        assert!(
            s_opp_live < 0.05,
            "Distant 95 bps opposing OB wall with aligned flow + own OB support must decay to ~0.0"
        );
        let chi_live = compute_corrective_abc_structural_threat_c1(s_opp_live, -0.18);
        assert!(chi_live < INV_PHI);
        let vac_live = compute_vacuum_l2_l3_c1(Side::Bid, &alt_live);
        assert!(vac_live > 0.50);

        assert!(passes_corrective_abc_snapshot_guard_with_health_c1(
            Side::Bid,
            &alt_live,
            0.75,
            0.45,
            1.0,
        ));
        assert!(passes_elliott_macro_coherence_guard_with_health(
            Side::Bid,
            &alt_live,
            &btc_live,
            0.75,
            None,
            1.0,
        ));

        let verdict = OrthogonalFactorState::evaluate_entry_signal_v55(
            APPROVED_SPECS[slot_idx],
            &alt_live,
            &btc_live,
            ScaledBps(680),
            ScaledBps(150),
            65,
            fees,
            0.0,
            0.95,
            0.75,
            None,
            1.0,
            0.0,
        );
        assert!(
            matches!(verdict, SignalVerdict::AuthorizeMakerEntry { side: Side::Bid, .. }),
            "Slot {slot_idx} must authorize Maker Bid entry on live production snapshot"
        );

        let gov_live = OnlineAdaptiveGovernor::new();
        let lots_res = gov_live.authorizes_adaptive_entry_v55(
            slot_idx,
            Side::Bid,
            6_800_000_000,
            1_000,
            ScaledBps(680),
            &alt_live,
            &btc_live,
            0.0,
            0.95,
            0.75,
            None,
            0.0,
        );
        assert!(lots_res.is_ok(), "Governor V5.5 must authorize multi-lot slot {slot_idx}");
        let Ok(alloc_lots) = lots_res else { return };
        assert!(alloc_lots.0 >= 2);

        let ev_opt = gov_live.evaluate_slot_pilar2_ex_ante_ev_with_leader_sbps(
            slot_idx,
            Side::Bid,
            alloc_lots,
            PriceTicks(px_ticks),
            ScaledBps(680),
            &alt_live,
            &btc_live,
            0.95,
        );
        assert!(ev_opt.is_some_and(|ev| ev > 0));

        let mut eng_live = SlotExecutionEngine::new(slot_idx);
        eng_live.learned_mfe_bps = 680;
        let armed_state = eng_live.try_arm_maker_entry_v542(
            &alt_live,
            &btc_live,
            verdict,
            alloc_lots,
            0.75,
            None,
            1.0,
        );
        assert!(matches!(
            armed_state,
            Ok(OrderLifecycleState::PlacingEntryMaker { side: Side::Bid, .. })
        ));
    }

    // End-to-end 200 Hz daemon loop verification (Arm > 0 on SUIUSDT slot 5):
    let shm_p = MappedShmRegion::ephemeral_self_test_path("v552_live_sui_arm");
    let shm_res = MappedShmRegion::open_or_create(&shm_p, true);
    assert!(shm_res.is_ok());
    let Ok(shm_reg) = shm_res else { return };
    let (tg_tx, _tg_rx) = mpsc::sync_channel::<String>(32);
    let (vault_tx, _vault_rx) = mpsc::sync_channel::<VaultIoCommand>(64);
    let inst = InstrumentSpecVerification::baseline_from_specs(&APPROVED_SPECS);
    let mut rt = ExecRuntimeState::new();
    rt.wallet_1e8 = 6_800_000_000;
    let _ = rt.daily_gov.sync_utc_day(TimestampNs(t0), rt.wallet_1e8);
    // Warm Epps tracker on slot 5 across 5s buckets to >= 0.72 correlation:
    let mut own_p = 7_350_i64;
    let mut btc_p = 652_000_i64;
    for k in 0..48_i64 {
        let dir = if k % 2 == 0 { 1_i64 } else { -1_i64 };
        own_p += dir * 22;
        btc_p += dir * 1_950;
        let ts_k = TimestampNs(t0 - (48 - k) * 5_500_000_000);
        rt.epps_trackers[5].observe_5s_pair(
            ts_k,
            false,
            PriceTicks(own_p),
            PriceTicks(btc_p),
        );
    }
    assert!(rt.epps_trackers[5].correlation(false) >= 0.70);

    let mut snaps = [SymbolLobSnapshot::new_empty(); MAX_ACTIVE_PAIRS];
    snaps[0] = btc_live;
    snaps[5] = mk_vault_snap(5, 7_350, 4_500, 0.48, 0.86, 0.14, t0);
    snaps[5].bids[0].qty = LotQty(1_200);
    snaps[5].asks[0].qty = LotQty(950);
    snaps[5].asks[1] = BookLevel { price: PriceTicks(7_352), qty: LotQty(110) };
    snaps[5].asks[2] = BookLevel { price: PriceTicks(7_353), qty: LotQty(120) };
    snaps[5].refresh_deep_l1_l5_totals();
    snaps[5].ob_bull_score = BoundedScore::new_clamped(0.54);
    snaps[5].ob_dist_bull_bps = ScaledBps(2_800);
    snaps[5].ob_bear_score = BoundedScore::new_clamped(0.72);
    snaps[5].ob_dist_bear_bps = ScaledBps(9_500);
    snaps[5].set_wave_observables(w_live_abc);

    let mut entry_f = [SlotForensicEntrySnapshot::default(); MAX_ACTIVE_PAIRS];
    let mut engines = SlotExecutionEngine::new_12_slot_array();
    engines[5].learned_mfe_bps = 680;
    let mut gov_d = OnlineAdaptiveGovernor::new();
    let mut led_d = ParitySyncLedger::new(6_800_000_000);
    let mut last_rest_ns = 0_i64;

    let _ = step_12_slot_daemon_tick(
        TimestampNs(t0),
        None,
        inst,
        None,
        &mut rt,
        &mut last_rest_ns,
        &snaps,
        &mut entry_f,
        &mut engines,
        &mut gov_d,
        &mut led_d,
        shm_reg.arena(),
        &tg_tx,
        &vault_tx,
    );
    assert!(matches!(
        engines[5].state,
        OrderLifecycleState::PlacingEntryMaker { side: Side::Bid, .. }
    ));
    snaps[0].ts_local_ns = TimestampNs(t0 + 250_000_000);
    snaps[5].ts_local_ns = TimestampNs(t0 + 250_000_000);
    let _ = step_12_slot_daemon_tick(
        TimestampNs(t0 + 250_000_000),
        None,
        inst,
        None,
        &mut rt,
        &mut last_rest_ns,
        &snaps,
        &mut entry_f,
        &mut engines,
        &mut gov_d,
        &mut led_d,
        shm_reg.arena(),
        &tg_tx,
        &vault_tx,
    );
    assert!(matches!(
        engines[5].state,
        OrderLifecycleState::RestingEntryQueue { side: Side::Bid, .. }
    ));
    assert!(shm_reg.arena().exec_armed_count.load(Ordering::Relaxed) >= 1);

    // =========================================================================
    // 4. Clean Intraday Reversal vs Stale Opposing ImpulseWave3 (NEW Entry Only,
    //    0ms Resting Queue Abort Intact, Deep Macro/Fib Veto Intact)
    // =========================================================================
    let mut sol_rev = mk_vault_snap(2, 7_520, 4_200, 0.68, 0.82, 0.28, t0);
    sol_rev.asks[1] = BookLevel { price: PriceTicks(7_522), qty: LotQty(110) };
    sol_rev.asks[2] = BookLevel { price: PriceTicks(7_523), qty: LotQty(110) };
    sol_rev.refresh_deep_l1_l5_totals();
    sol_rev.set_wave_observables(WaveObservables {
        regime: WaveRegime::ImpulseWave3,
        climax_active: false,
        hurst: WaveObservables::encode_hurst(0.71),
        macro_phase: BoundedScore::new_clamped(-0.68),
        oi_impulse: BoundedScore::ZERO,
        liq_imbalance: BoundedScore::ZERO,
        fib_proj_bps: ScaledBps(-3_200),
    });
    // 0ms resting-queue hazard detector MUST remain 100% intact (true):
    assert!(is_opposing_impulse_wave3_hazard_dynamic(
        Side::Bid, &sol_rev, 0, 50, fees
    ));
    // Static / contracted-health guard (psi_health <= INV_PHI_SQ) MUST veto:
    assert!(is_opposing_impulse_wave3_entry_hazard_v552(
        Side::Bid,
        &sol_rev,
        0.45,
        INV_PHI_SQ,
        0,
        50,
        fees,
    ));
    assert!(!passes_elliott_macro_coherence_guard(Side::Bid, &sol_rev));
    // Healthy new entry (psi_health = 1.0, OFI >= INV_PHI, TFI >= INV_PHI, BTC >= INV_PHI_SQ) unlocks:
    assert!(!is_opposing_impulse_wave3_entry_hazard_v552(
        Side::Bid, &sol_rev, 0.45, 1.0, 0, 50, fees
    ));
    assert!(
        evaluate_macro_coherence_surface_with_health_c1(Side::Bid, &sol_rev, 0.75, 0.45, 1.0)
            < INV_PHI
    );
    assert!(matches!(
        OrthogonalFactorState::evaluate_entry_signal_v55(
            APPROVED_SPECS[2],
            &sol_rev,
            &btc_live,
            ScaledBps(680),
            ScaledBps(150),
            50,
            fees,
            0.0,
            0.95,
            0.75,
            None,
            1.0,
            0.0,
        ),
        SignalVerdict::AuthorizeMakerEntry { side: Side::Bid, .. }
    ));
    assert!(gov_d
        .authorizes_adaptive_entry_v55(
            2,
            Side::Bid,
            6_800_000_000,
            1_000,
            ScaledBps(680),
            &sol_rev,
            &btc_live,
            0.0,
            0.95,
            0.75,
            None,
            0.0,
        )
        .is_ok());

    // Deep opposing ImpulseWave3 (macro_aligned <= -0.75 OR fib_aligned_bps <= -45.0) MUST veto:
    let mut sol_deep_macro = sol_rev;
    sol_deep_macro.set_wave_observables(WaveObservables {
        macro_phase: BoundedScore::new_clamped(-0.76),
        ..sol_rev.wave_observables()
    });
    assert!(is_opposing_impulse_wave3_entry_hazard_v552(
        Side::Bid,
        &sol_deep_macro,
        0.45,
        1.0,
        0,
        50,
        fees,
    ));
    let mut sol_deep_fib = sol_rev;
    sol_deep_fib.set_wave_observables(WaveObservables {
        fib_proj_bps: ScaledBps(-4_600),
        ..sol_rev.wave_observables()
    });
    assert!(is_opposing_impulse_wave3_entry_hazard_v552(
        Side::Bid,
        &sol_deep_fib,
        0.45,
        1.0,
        0,
        50,
        fees,
    ));

    // =========================================================================
    // 5. Explicit Non-Regression on All 6 CorrectiveAbc Losses (#18, #19, #22,
    //    #23, #27, #30), ETH #2, and ETH #5 (Trade #32 V5.2.1)
    // =========================================================================
    #[derive(Copy, Clone)]
    struct V552AbcLossCase {
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
    let six_abc_losses = [
        V552AbcLossCase { slot: 1, px: 179_267, vol: 1_725, ofi: 0.57, tfi: 0.93, geom: 0.09, btc_c: 0.10, side: Side::Bid, h_val: 0.696, m_ph: 0.45, bull_pm: 438, bear_pm: 415, opp_sc: 0.50, opp_dist: 5_572 },
        V552AbcLossCase { slot: 5, px: 7_316, vol: 1_582, ofi: -0.75, tfi: -0.84, geom: -0.17, btc_c: -0.55, side: Side::Ask, h_val: 0.748, m_ph: -0.45, bull_pm: 522, bear_pm: 478, opp_sc: 0.57, opp_dist: 9_670 },
        V552AbcLossCase { slot: 3, px: 7_156, vol: 1_873, ofi: -0.49, tfi: -0.95, geom: -0.20, btc_c: -0.21, side: Side::Ask, h_val: 0.637, m_ph: -0.45, bull_pm: 465, bear_pm: 471, opp_sc: 0.60, opp_dist: 3_908 },
        V552AbcLossCase { slot: 4, px: 19_691, vol: 2_428, ofi: 0.65, tfi: 0.86, geom: 0.36, btc_c: 0.36, side: Side::Bid, h_val: 0.729, m_ph: -0.30, bull_pm: 564, bear_pm: 516, opp_sc: 0.44, opp_dist: 4_984 },
        V552AbcLossCase { slot: 1, px: 179_552, vol: 1_890, ofi: 0.48, tfi: 0.84, geom: 0.93, btc_c: 0.43, side: Side::Bid, h_val: 0.715, m_ph: 0.45, bull_pm: 443, bear_pm: 441, opp_sc: 0.39, opp_dist: 6_337 },
        V552AbcLossCase { slot: 4, px: 19_038, vol: 3_023, ofi: -0.72, tfi: -0.67, geom: -0.27, btc_c: -0.32, side: Side::Ask, h_val: 0.744, m_ph: -0.45, bull_pm: 476, bear_pm: 548, opp_sc: 0.64, opp_dist: 8_861 },
        V552AbcLossCase { slot: 1, px: 177_485, vol: 5_800, ofi: -0.22, tfi: -0.62, geom: -0.92, btc_c: -0.14, side: Side::Ask, h_val: 0.700, m_ph: -0.35, bull_pm: 509, bear_pm: 532, opp_sc: 0.67, opp_dist: 3_115 },
    ];
    for c in six_abc_losses {
        let mut s_l = mk_vault_snap(c.slot, c.px, c.vol, c.ofi, c.tfi, c.geom, t0);
        s_l.ob_bull_efficacy_pm = i64::from(c.bull_pm);
        s_l.ob_bear_efficacy_pm = i64::from(c.bear_pm);
        match c.side {
            Side::Bid => {
                s_l.ob_bear_score = BoundedScore::new_clamped(c.opp_sc);
                s_l.ob_dist_bear_bps = ScaledBps(c.opp_dist);
            }
            Side::Ask => {
                s_l.ob_bull_score = BoundedScore::new_clamped(c.opp_sc);
                s_l.ob_dist_bull_bps = ScaledBps(c.opp_dist);
            }
        }
        s_l.set_wave_observables(WaveObservables {
            regime: WaveRegime::CorrectiveAbc,
            climax_active: false,
            hurst: WaveObservables::encode_hurst(c.h_val),
            macro_phase: BoundedScore::new_clamped(c.m_ph),
            oi_impulse: BoundedScore::ZERO,
            liq_imbalance: BoundedScore::ZERO,
            fib_proj_bps: ScaledBps(0),
        });
        let btc_l = mk_vault_snap(0, 652_000, 3_200, c.btc_c, c.btc_c, c.btc_c, t0);
        let btc_al = c.btc_c * c.side.sign_f64();
        assert!(
            !passes_corrective_abc_snapshot_guard_with_health_c1(c.side, &s_l, 0.82, btc_al, 1.0),
            "Historical CorrectiveAbc loss on slot {} @ {} sbps must remain 100% blocked",
            c.slot,
            c.opp_dist
        );
        assert!(!matches!(
            OrthogonalFactorState::evaluate_entry_signal_v55(
                APPROVED_SPECS[c.slot],
                &s_l,
                &btc_l,
                ScaledBps(680),
                ScaledBps(150),
                50,
                fees,
                0.0,
                0.95,
                0.82,
                None,
                1.0,
                0.0,
            ),
            SignalVerdict::AuthorizeMakerEntry { .. }
        ));
        assert!(gov_d
            .authorizes_adaptive_entry_v55(
                c.slot,
                c.side,
                6_800_000_000,
                1_000,
                ScaledBps(680),
                &s_l,
                &btc_l,
                0.0,
                0.95,
                0.82,
                None,
                0.0,
            )
            .is_err());
    }

    // ETH #5 (Trade #32 V5.2.1: Bid into low-Hurst bearish ImpulseWave3 -0.90, fib -157.90 bps) stays 100% vetoed:
    let mut eth32 = mk_vault_snap(1, 206_119, 2_822, 0.22, 0.72, 0.91, t0);
    eth32.set_wave_observables(WaveObservables {
        regime: WaveRegime::ImpulseWave3,
        climax_active: false,
        hurst: WaveObservables::encode_hurst(0.320),
        macro_phase: BoundedScore::new_clamped(-0.90),
        oi_impulse: BoundedScore::new_clamped(0.05),
        liq_imbalance: BoundedScore::ZERO,
        fib_proj_bps: ScaledBps(-15_790),
    });
    assert!(is_opposing_impulse_wave3_hazard(Side::Bid, &eth32));
    assert!(is_opposing_impulse_wave3_entry_hazard_v552(
        Side::Bid, &eth32, 0.45, 1.0, 0, 50, fees
    ));
    assert!(!passes_elliott_macro_coherence_guard_with_health(
        Side::Bid, &eth32, &btc_live, 0.85, None, 1.0
    ));
    assert_eq!(
        OrthogonalFactorState::evaluate_entry_signal_v55(
            APPROVED_SPECS[1],
            &eth32,
            &btc_live,
            ScaledBps(715),
            ScaledBps(120),
            48,
            fees,
            0.0,
            0.95,
            0.85,
            None,
            1.0,
            0.0,
        ),
        SignalVerdict::NeutralNoAction
    );
    assert!(gov_d
        .authorizes_adaptive_entry_v55(
            1,
            Side::Bid,
            6_800_000_000,
            1_000,
            ScaledBps(715),
            &eth32,
            &btc_live,
            0.0,
            0.95,
            0.85,
            None,
            0.0,
        )
        .is_err());

    drop(shm_reg);
    let _ = std::fs::remove_file(&shm_p);
}

#[test]
fn test_v553_full_forensic_audit_seals_f01_to_f16() {
    use std::net::{IpAddr, Ipv4Addr};
    use vortex_rs::alpha_3factor::{
        compute_tick_lattice_granularity_eta, is_opposing_impulse_wave3_entry_hazard_v552,
        is_opposing_or_silent_spoof_veto_c1, OrthogonalFactorState, SignalVerdict,
    };
    use vortex_rs::execution_fsm::compute_stagnation_sl_compression_factor;
    use vortex_rs::exit_quoter::ClosedPnlQueryOutcome;
    use vortex_rs::fifo_sim::{FifoQueueSimulator, SimulatedOrderState};
    use vortex_rs::forensic_parquet::ParitySyncLedger;
    use vortex_rs::markout_tracker::SlotMarkoutRing;
    use vortex_rs::types::slot_from_symbol_name;
    use vortex_rs::web_server::{
        is_valid_loopback_origin, VortexWebConfig, VortexWebContext, WebApiError, CSRF_HEADER_NAME,
    };

    let fees = FeeSchedule::VIP0_DEFAULT;
    let avax_idx = slot_from_symbol_name("AVAXUSDT").unwrap_or(3);
    let avax_spec = APPROVED_SPECS[avax_idx];
    let wif_idx = slot_from_symbol_name("WIFUSDT").unwrap_or(8);
    let wif_spec = APPROVED_SPECS[wif_idx];

    // (a) F-01: AVAXUSDT Ask with zeta_tox = 0.47 < 0.618, mfe_bps = 0.0, tfi_out = +0.87 at hold = 41.8s
    // respects the diffusive floor without 82% contraction nor activating exhaustion_unwind_active.
    let stag_factor = compute_stagnation_sl_compression_factor(
        41_800_000_000,
        180_000_000_000,
        1.0,
        0,
        -0.87,
        0.47,
    );
    assert!(stag_factor < 0.01);
    let mut avax_snap = mk_vault_snap(avax_idx, 8_200, 3_600, -0.72, -0.68, -0.64, 10_000_000_000);
    let mut avax_eng = SlotExecutionEngine::new(avax_idx);
    avax_eng.intrinsic_tau_mkt_s = 180;
    avax_eng.learned_mfe_bps = 680;
    assert!(avax_eng
        .try_arm_maker_entry(
            &avax_snap,
            SignalVerdict::AuthorizeMakerEntry {
                side: Side::Ask,
                target_excursion_scaled_bps: ScaledBps(680),
            },
            LotQty(4),
        )
        .is_ok());
    assert!(avax_eng.on_maker_entry_filled(TimestampNs(10_000_000_000)).is_ok());
    let init_sl_dist = avax_eng.last_eff_sl_ticks.0 - 8_201;
    let mut avax_ring = SlotMarkoutRing::new();
    avax_ring.arm_fill_probe(Side::Ask, PriceTicks(8_201), PriceTicks(8_201), TimestampNs(10_000_000_000));
    avax_ring.step_clock(PriceTicks(8_202), TimestampNs(10_300_000_000), 200);
    avax_ring.step_clock(PriceTicks(8_202), TimestampNs(11_200_000_000), 200);
    avax_ring.step_clock(PriceTicks(8_202), TimestampNs(16_000_000_000), 200);
    avax_ring.step_clock(PriceTicks(8_203), TimestampNs(25_500_000_000), 200);
    avax_snap.ts_local_ns = TimestampNs(51_800_000_000);
    avax_snap.bids[0] = BookLevel { price: PriceTicks(8_202), qty: LotQty(450) };
    avax_snap.asks[0] = BookLevel { price: PriceTicks(8_203), qty: LotQty(220) };
    avax_snap.mark_price = PriceTicks(8_202);
    avax_snap.last_trade_price = PriceTicks(8_202);
    avax_snap.factor_ofi = BoundedScore::new_clamped(0.18);
    avax_snap.factor_tfi = BoundedScore::new_clamped(0.87);
    let z_obs = avax_eng.observe_post_fill_toxicity_and_wald_v49(&avax_snap, &avax_snap, 0.25, &avax_ring, 1.0);
    assert!(z_obs < 0.618);
    assert!(!avax_eng.wald_sprt.exhaustion_unwind_active);
    let _ = avax_eng.maybe_ratchet_sl_with_toxicity_v48(&avax_snap, &avax_snap, 0.25, 0.47);
    assert!(!avax_eng.wald_sprt.exhaustion_unwind_active);
    assert!(avax_eng.wald_sprt.contraction_pct < 82);
    let cur_sl_dist = avax_eng.last_eff_sl_ticks.0 - 8_201;
    assert!(cur_sl_dist * 100 > init_sl_dist * 50);

    // (b.1) F-02: Divisible altcoin admission in high volatility (quantum_ev_net_drift_bps / evaluate_pilar2_asymmetry_ev_v553_sbps).
    let hv_vol = ScaledBps(9_600);
    let div_lots = avax_spec.compute_risk_parity_kelly_lots_with_fees(
        6_800_000_000,
        PriceTicks(8_200),
        hv_vol,
        fees.round_trip_maker_exit_bps().raw(),
        45,
        1_000,
        false,
        fees,
    );
    assert!(div_lots.0 >= 1);
    assert!(avax_spec.quantum_ev_net_drift_bps(6_800_000_000, PriceTicks(8_200), hv_vol, ScaledBps(860), 48, fees) > 0.0);
    let ev_hv = avax_spec.evaluate_pilar2_asymmetry_ev_v553_sbps(
        LotQty(div_lots.0.max(2)),
        PriceTicks(8_200),
        hv_vol,
        860,
        48,
        0.78,
        640,
        460,
        Side::Bid,
        fees,
        0.82,
        0.76,
        0.68,
        0.58,
        640,
        ScaledBps(980),
        ScaledBps(1_450),
    );
    assert!(ev_hv.is_some_and(|ev| ev > 0));

    // (b.2) F-08: Wide-tick WIF admission with OFI = +0.78, TFI = 0.00 without NeutralNoAction / SpoofingVeto.
    let btc_bull = mk_vault_snap(0, 680_000, 3_200, 0.72, 0.68, 0.64, 60_000_000_000);
    let mut wif_snap = mk_vault_snap(wif_idx, 1_470, 9_500, 0.78, 0.00, 0.74, 60_000_000_000);
    wif_snap.ob_bull_efficacy_pm = 620;
    wif_snap.ob_bear_efficacy_pm = 480;
    wif_snap.set_wave_observables(WaveObservables {
        regime: WaveRegime::ImpulseWave3,
        climax_active: false,
        hurst: WaveObservables::encode_hurst(0.64),
        macro_phase: BoundedScore::new_clamped(0.62),
        oi_impulse: BoundedScore::new_clamped(0.35),
        liq_imbalance: BoundedScore::ZERO,
        fib_proj_bps: ScaledBps(3_800),
    });
    let eta_wif = compute_tick_lattice_granularity_eta(&wif_snap, fees);
    assert!(eta_wif > 1.62);
    assert!(!is_opposing_or_silent_spoof_veto_c1(0.78, 0.00, eta_wif, 0.90));
    let wif_verdict = OrthogonalFactorState::evaluate_entry_signal_v553(
        wif_spec,
        &wif_snap,
        &btc_bull,
        ScaledBps(3_600),
        ScaledBps(120),
        48,
        fees,
        0.0,
        0.85,
        0.75,
        Some(0.65),
        1.0,
        0.0,
        0.90,
    );
    assert!(matches!(
        wif_verdict,
        SignalVerdict::AuthorizeMakerEntry { side: Side::Bid, .. }
    ));

    // (b.3) F-09: Penny-Jump admission (is_penny_jump = true, SPR >= 2) despite thick rear L1 wall (own_l1_1e8 > max_q_1e8).
    let mut pj_snap = mk_vault_snap(avax_idx, 8_200, 4_200, 0.84, 0.79, 0.72, 62_000_000_000);
    pj_snap.bids[0] = BookLevel { price: PriceTicks(8_200), qty: LotQty(500_000) };
    pj_snap.asks[0] = BookLevel { price: PriceTicks(8_202), qty: LotQty(80) };
    pj_snap.refresh_deep_l1_l5_totals();
    assert!(OnlineAdaptiveGovernor::qualifies_inside_spread_penny_jump_c1(Side::Bid, &pj_snap, 1.0));
    assert!(!avax_spec.passes_l1_queue_depth_and_wall_guard_v553(
        PriceTicks(8_200),
        ScaledBps(4_200),
        150,
        LotQty(500_000),
        LotQty(80),
        pj_snap.deep_bid_lots_l1_l5,
        0.84,
        0.79,
        48,
        fees,
        false,
    ));
    assert!(avax_spec.passes_l1_queue_depth_and_wall_guard_v553(
        PriceTicks(8_201),
        ScaledBps(4_200),
        150,
        LotQty(500_000),
        LotQty(80),
        pj_snap.deep_bid_lots_l1_l5,
        0.84,
        0.79,
        48,
        fees,
        true,
    ));

    // (c) F-03: Zero 0 ms self-cancellation in RestingEntryQueue while OFI/TFI/BTC backing holds, immediate abort once lost.
    let mut w3_rev = mk_vault_snap(avax_idx, 8_200, 3_800, 0.78, 0.74, 0.70, 70_000_000_000);
    w3_rev.set_wave_observables(WaveObservables {
        regime: WaveRegime::ImpulseWave3,
        climax_active: false,
        hurst: WaveObservables::encode_hurst(0.56),
        macro_phase: BoundedScore::new_clamped(-0.62),
        oi_impulse: BoundedScore::new_clamped(-0.10),
        liq_imbalance: BoundedScore::ZERO,
        fib_proj_bps: ScaledBps(-3_200),
    });
    assert!(!is_opposing_impulse_wave3_entry_hazard_v552(
        Side::Bid, &w3_rev, 0.64, 0.92, 3_800, 48, fees
    ));
    w3_rev.factor_ofi = BoundedScore::new_clamped(-0.18);
    w3_rev.factor_tfi = BoundedScore::new_clamped(-0.22);
    assert!(is_opposing_impulse_wave3_entry_hazard_v552(
        Side::Bid, &w3_rev, -0.25, 0.92, 3_800, 48, fees
    ));

    // (d) F-05: Finite contagion cooldown + smooth decay toward 0 when global_dir_debt_bps_x100 crosses threshold with glob_cd == 0, preserving sl_streak.
    let mut gov_f05 = OnlineAdaptiveGovernor::new();
    let mut cum_mat = [[0_i64; 2]; 12];
    cum_mat[1][0] = -3_200;
    let mut sl_mat = [[0_u32; 2]; 12];
    sl_mat[1][0] = 2;
    let scar_f05 = serde_json::json!({
        "event": "GOV_SCAR_SNAPSHOT",
        "ts_ns": 1_700_000_000_000_000_000_i64,
        "cum_net_bps_x100": cum_mat,
        "sl_streak": sl_mat,
        "global_dir_debt_bps_x100": [-12_500_i64, 0_i64],
        "global_dir_cooldown_until_ns": [0_i64, 0_i64],
    });
    assert!(gov_f05.apply_scar_snapshot_json(&scar_f05).is_some());
    assert_eq!(gov_f05.global_dir_cooldown_until_ns_for(Side::Bid), 0);
    let t_f05 = 1_700_000_010_000_000_000_i64;
    assert_eq!(
        gov_f05.audit_historical_entry_veto(1, Side::Bid, 200_000, t_f05),
        Some("VETO_SYMBOL_AND_GLOBAL_DEBT_CONTAGION")
    );
    let armed_cd = gov_f05.global_dir_cooldown_until_ns_for(Side::Bid);
    assert!(armed_cd > t_f05);
    let t_unlocked = armed_cd + 2_400_000_000_000_i64;
    assert_eq!(gov_f05.audit_historical_entry_veto(1, Side::Bid, 200_000, t_unlocked), None);
    assert!(gov_f05.global_dir_debt_bps_x100_for(Side::Bid) > -10_000);
    let t_fully_damped = t_unlocked + 7_200_000_000_000_i64;
    let _ = gov_f05.audit_historical_entry_veto(1, Side::Bid, 200_000, t_fully_damped);
    assert_eq!(gov_f05.global_dir_debt_bps_x100_for(Side::Bid), 0);
    let after_scar = gov_f05.to_scar_snapshot_json(TimestampNs(t_fully_damped));
    assert_eq!(after_scar["sl_streak"][1][0].as_u64(), Some(2));

    // (e) F-06: FifoQueueSimulator::on_lob_snapshot produces ZERO phantom fills when own L1 retreats with SPR > 1 without opposite cross or PublicTrade.
    let mut fifo = FifoQueueSimulator::new();
    fifo.submit_post_only(Side::Bid, PriceTicks(8_200), LotQty(8), TimestampNs(1_000_000_000));
    let mut s_rest = mk_vault_snap(avax_idx, 8_200, 3_200, 0.40, 0.35, 0.30, 1_030_000_000);
    s_rest.bids[0] = BookLevel { price: PriceTicks(8_200), qty: LotQty(400) };
    s_rest.asks[0] = BookLevel { price: PriceTicks(8_203), qty: LotQty(400) };
    fifo.on_lob_snapshot(&s_rest);
    assert!(matches!(fifo.state, SimulatedOrderState::RestingInFifoQueue { .. }));
    let mut s_retreat = s_rest;
    s_retreat.ts_local_ns = TimestampNs(1_050_000_000);
    s_retreat.bids[0] = BookLevel { price: PriceTicks(8_198), qty: LotQty(350) };
    s_retreat.asks[0] = BookLevel { price: PriceTicks(8_203), qty: LotQty(400) };
    fifo.on_lob_snapshot(&s_retreat);
    assert_eq!(fifo.total_maker_fills, 0);
    assert!(!matches!(fifo.state, SimulatedOrderState::FilledMaker { .. }));

    // (f) F-10: Exact T1 Maker + T3 Taker fee attribution on mixed close (PARITY_SYNC Drift == 0).
    let mut eng_f10 = SlotExecutionEngine::new(avax_idx);
    let s_f10 = mk_vault_snap(avax_idx, 8_200, 3_600, 0.75, 0.72, 0.68, 10_000_000_000);
    assert!(eng_f10
        .try_arm_maker_entry(
            &s_f10,
            SignalVerdict::AuthorizeMakerEntry {
                side: Side::Bid,
                target_excursion_scaled_bps: ScaledBps(700),
            },
            LotQty(10),
        )
        .is_ok());
    assert!(eng_f10.on_maker_entry_filled(TimestampNs(10_000_000_000)).is_ok());
    let (net_t1, fee_t1_mkr, _, _) = avax_spec.compute_leg_pnl_and_fee_1e8_with_schedule(
        Side::Bid, LotQty(5), PriceTicks(8_200), PriceTicks(8_212), true, fees,
    );
    let (net_t3, fee_t3_tkr, _, _) = avax_spec.compute_leg_pnl_and_fee_1e8_with_schedule(
        Side::Bid, LotQty(5), PriceTicks(8_200), PriceTicks(8_175), false, fees,
    );
    let (_, fee_t1_if_all_tkr, _, _) = avax_spec.compute_leg_pnl_and_fee_1e8_with_schedule(
        Side::Bid, LotQty(5), PriceTicks(8_200), PriceTicks(8_212), false, fees,
    );
    let qty_half_str = avax_spec.format_lot_qty_str(LotQty(5));
    let (en_w, en_f) = avax_spec.format_price_usdt_parts(PriceTicks(8_200));
    let (t1_w, t1_f) = avax_spec.format_price_usdt_parts(PriceTicks(8_212));
    let (t3_w, t3_f) = avax_spec.format_price_usdt_parts(PriceTicks(8_175));
    let mixed_payload = serde_json::json!({
        "retCode": 0,
        "result": {
            "list": [
                {
                    "symbol": avax_spec.symbol_id.name(),
                    "orderType": "Market",
                    "closedSize": qty_half_str,
                    "avgEntryPrice": format!("{en_w}.{en_f:04}"),
                    "avgExitPrice": format!("{t3_w}.{t3_f:04}"),
                    "closedPnl": format!("{:.8}", (net_t3 as f64) / 100_000_000.0),
                    "updatedTime": "20000"
                },
                {
                    "symbol": avax_spec.symbol_id.name(),
                    "orderType": "Limit",
                    "closedSize": qty_half_str,
                    "avgEntryPrice": format!("{en_w}.{en_f:04}"),
                    "avgExitPrice": format!("{t1_w}.{t1_f:04}"),
                    "closedPnl": format!("{:.8}", (net_t1 as f64) / 100_000_000.0),
                    "updatedTime": "15000"
                }
            ]
        }
    });
    let outcome_f10 = eng_f10.reconcile_closed_pnl_payload(&mixed_payload, 0, false);
    assert!(matches!(outcome_f10, ClosedPnlQueryOutcome::Verified(_)));
    if let ClosedPnlQueryOutcome::Verified(res_f10) = outcome_f10 {
        assert_eq!(res_f10.maker_closed_lots, LotQty(5));
        assert_eq!(res_f10.taker_closed_lots, LotQty(5));
        assert_eq!(res_f10.exact_fee_1e8, Some(fee_t1_mkr + fee_t3_tkr));
        assert!(fee_t1_mkr + fee_t3_tkr < fee_t1_if_all_tkr + fee_t3_tkr);
        let init_eq_1e8 = 6_800_000_000_i64;
        let mut ledger_f10 = ParitySyncLedger::new(init_eq_1e8);
        let applied_opt = eng_f10.apply_verified_close_resolution(res_f10, TimestampNs(20_000_000_000), &mut ledger_f10);
        assert!(applied_opt.is_some());
        if let Some(sum_f10) = applied_opt {
            assert_eq!(sum_f10.fee_1e8, fee_t1_mkr + fee_t3_tkr);
            assert_eq!(sum_f10.net_pnl_1e8, net_t1 + net_t3);
        }
        let expected_exch_eq_1e8 = init_eq_1e8 + net_t1 + net_t3;
        let parity_sync_drift_1e8 = ledger_f10.current_equity_1e8() - expected_exch_eq_1e8;
        assert_eq!(parity_sync_drift_1e8, 0);
    }

    // (g) F-16: Strict authority CSRF rejects http://localhost.evil.tld and accepts http://localhost:8080.
    assert!(!is_valid_loopback_origin("http://localhost.evil.tld"));
    assert!(!is_valid_loopback_origin("http://127.0.0.1.evil.tld"));
    assert!(!is_valid_loopback_origin("http://localhost:0"));
    assert!(!is_valid_loopback_origin("http://localhost:65536"));
    assert!(is_valid_loopback_origin("http://localhost:8080"));
    assert!(is_valid_loopback_origin("http://127.0.0.1:8787/"));
    assert!(is_valid_loopback_origin("http://[::1]:8787"));
    let web_ctx = VortexWebContext::new(VortexWebConfig {
        bind_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        port: 0,
        ..VortexWebConfig::default()
    });
    let evil_hdrs = vec![
        ("Origin".to_string(), "http://localhost.evil.tld".to_string()),
        (CSRF_HEADER_NAME.to_string(), web_ctx.csrf_token.clone()),
    ];
    assert_eq!(web_ctx.verify_csrf_header(&evil_hdrs), Err(WebApiError::CsrfRejected));
    let good_hdrs = vec![
        ("Origin".to_string(), "http://localhost:8080".to_string()),
        (CSRF_HEADER_NAME.to_string(), web_ctx.csrf_token.clone()),
    ];
    assert_eq!(web_ctx.verify_csrf_header(&good_hdrs), Ok(()));
}

#[test]
fn test_v558_deadzone_win_unlock_divisible_t1_rescue_anti_spiral_ewma_and_counterfactual_abort_learning() {
    let fees = vortex_rs::fee_cadence_beta::FeeSchedule::VIP0_DEFAULT;
    let t0 = 1_795_240_000_000_000_000_i64;

    // 1) Dead-zone elimination (+2.68..+3.21 bps): ETH Ask locked at 401 sbps unlocks and amends outward at +2.85 bps!
    let eth_spec = vortex_rs::types::APPROVED_SPECS[1];
    let en_eth = vortex_rs::types::PriceTicks(161_870);
    let z_eth = vortex_rs::execution_fsm::defensive_zero_loss_maker_exit_price_with_fees(
        vortex_rs::types::Side::Ask,
        en_eth,
        fees,
    );
    let mid_285 = vortex_rs::types::PriceTicks(161_824); // +2.84 bps favorable (inside old +2.68..+4.01 dead-zone!)
    let mut eth_snap = mk_vault_snap(1, mid_285.0, 3_200, -0.48, -0.68, -0.35, t0 + 50_000_000_000);
    eth_snap.bids[0].price = mid_285;
    eth_snap.asks[0].price = vortex_rs::types::PriceTicks(mid_285.0 + 1);
    let eth_ctx = vortex_rs::exit_quoter::OpenExitContext {
        slot_idx: 1,
        side: vortex_rs::types::Side::Ask,
        avg_price_ticks: en_eth,
        peak_favorable_price: mid_285,
        worst_adverse_price: vortex_rs::types::PriceTicks(161_942),
        initial_lots: vortex_rs::types::LotQty(1),
        open_lots: vortex_rs::types::LotQty(1),
        tranche1_harvested: false,
        tranche1_lots: vortex_rs::types::LotQty(0),
        tranche1_exit_price: vortex_rs::types::PriceTicks(0),
        defensive_floor_locked: true,
        active_exit_price: z_eth,
        structural_tp_price: vortex_rs::types::PriceTicks(161_700),
        resting_exit_price: z_eth,
        sl_trigger_price: vortex_rs::types::PriceTicks(162_900),
        opened_ts: vortex_rs::types::TimestampNs(t0),
        learned_mfe_bps: 1_050,
        learned_t1_permille: 500,
        learned_ir_x100: 33,
        learned_ret_std_bps: 200,
        intrinsic_tau_mkt_s: 300,
        last_eff_sl_ticks: vortex_rs::types::PriceTicks(162_550),
        ratchet_locked_gross_bps: 0,
        last_closed_mfe_bps: 284,
        last_closed_mae_bps: -444,
        active_fees: fees,
        learned_taker_prob_pm: 57,
    };
    let kin_recov = vortex_rs::exit_quoter::ShadowRunnerKinematics {
        flow_ema_fast: -0.54,
        flow_ema_slow: -0.50,
        in_scar_debt: false,
        chandelier_armed: false,
    };
    let q_eth_opt = vortex_rs::exit_quoter::compute_canonical_exit_quote_omega(&eth_ctx, kin_recov, &eth_snap);
    assert!(q_eth_opt.is_some());
    let Some(q_eth) = q_eth_opt else { return };
    assert!(!q_eth.defensive_lock_active && q_eth.target_price.0 < z_eth.0);
    assert!(vortex_rs::bybit_gateway::should_repeg_resting_exit_tp(
        eth_spec,
        vortex_rs::types::Side::Ask,
        mid_285,
        z_eth,
        vortex_rs::types::LotQty(1),
        q_eth.target_price,
        vortex_rs::types::LotQty(1),
        false,
        284,
        fees,
    ));

    // 2) Divisible rebound (LINK 26L, slow=-0.02, OFI=+0.52) splits Tranche 1 instead of 100% scratch exit:
    let en_link = vortex_rs::types::PriceTicks(8_354);
    let z_link = vortex_rs::execution_fsm::defensive_zero_loss_maker_exit_price_with_fees(
        vortex_rs::types::Side::Bid,
        en_link,
        fees,
    );
    let link_snap = mk_vault_snap(9, 8_350, 3_200, 0.52, 0.20, 0.18, t0 + 90_000_000_000);
    let mut link_ctx = eth_ctx;
    link_ctx.slot_idx = 9;
    link_ctx.side = vortex_rs::types::Side::Bid;
    link_ctx.avg_price_ticks = en_link;
    link_ctx.peak_favorable_price = en_link;
    link_ctx.worst_adverse_price = vortex_rs::types::PriceTicks(8_234);
    link_ctx.initial_lots = vortex_rs::types::LotQty(26);
    link_ctx.open_lots = vortex_rs::types::LotQty(26);
    link_ctx.active_exit_price = z_link;
    link_ctx.resting_exit_price = z_link;
    link_ctx.structural_tp_price = vortex_rs::types::PriceTicks(8_412);
    link_ctx.sl_trigger_price = vortex_rs::types::PriceTicks(8_180);
    link_ctx.last_eff_sl_ticks = vortex_rs::types::PriceTicks(8_210);
    link_ctx.learned_mfe_bps = 686;
    let kin_link = vortex_rs::exit_quoter::ShadowRunnerKinematics {
        flow_ema_fast: 0.28,
        flow_ema_slow: -0.02,
        in_scar_debt: false,
        chandelier_armed: false,
    };
    let q_link_opt = vortex_rs::exit_quoter::compute_canonical_exit_quote_omega(&link_ctx, kin_link, &link_snap);
    assert!(q_link_opt.is_some_and(|q| {
        q.role == vortex_rs::exit_quoter::ExitOrderRole::Tranche1Harvest50
            && q.target_price == z_link
            && q.order_lots.0 < 26
    }));

    // 3) Anti-spiral EWMA on 3 censored scratches + MFE floor >= round(PHI * 400) = 647 (AVAX 676 fix):
    let mut gov = vortex_rs::execution_fsm::OnlineAdaptiveGovernor::new();
    let w_neutral = eth_snap.wave_observables();
    for (k, mfe_sbps) in [(1_i64, 657_i64), (2, 500), (3, 420)] {
        gov.observe_closed_trade_with_regime_v51(
            1,
            vortex_rs::types::Side::Ask,
            en_eth,
            vortex_rs::types::ScaledBps(k),
            vortex_rs::types::ScaledBps(mfe_sbps),
            true,
            w_neutral,
            1.0,
            vortex_rs::types::TimestampNs(t0 + k * 60_000_000_000),
        );
    }
    let (eth_ewma_after_3scr, _, _) = gov.slot_direction_diagnostics(1, vortex_rs::types::Side::Ask);
    assert!(eth_ewma_after_3scr >= 165);
    let mut eng_avax = vortex_rs::execution_fsm::SlotExecutionEngine::new(3);
    eng_avax.learned_mfe_bps = 680;
    eng_avax.open_vol_5m_bps = vortex_rs::types::ScaledBps(1_800);
    eng_avax.last_closed_mfe_bps = 459;
    eng_avax.last_exit_sub_reason = vortex_rs::exit_quoter::ExitSubReason::AdverseScratchEscape;
    eng_avax.observe_censored_close_mfe_plasticity_with_ext(vortex_rs::types::Side::Bid, 459, 0, true);
    assert!(eng_avax.learned_mfe_bps >= 647);

    // 4) Pre-T1 markout keeps cont_ratio == 1000; clean +60s abort counterfactual learns MFE & cont_ratio:
    assert_eq!(
        gov.observe_markout_60s_continuation(3, vortex_rs::types::Side::Bid, 350, 150, vortex_rs::types::ScaledBps(3_600)),
        1_000
    );
    let mut ring = vortex_rs::markout_tracker::SlotMarkoutRing::new();
    for k in 0..6_i64 {
        let ta = t0 + k * 70_000_000_000;
        ring.arm_abort_probe_v49(
            vortex_rs::types::Side::Bid,
            vortex_rs::types::PriceTicks(10_000),
            vortex_rs::lob_shm::LastVetoCode::HawkesStoikovQueueAbort,
            vortex_rs::types::TimestampNs(ta),
            vortex_rs::types::TimestampNs(ta + 1_000_000_000),
            vortex_rs::types::LotQty(200),
            vortex_rs::types::LotQty(40),
        );
        ring.step_clock(vortex_rs::types::PriceTicks(10_002), vortex_rs::types::TimestampNs(ta + 1_300_000_000), 200);
        ring.step_clock(vortex_rs::types::PriceTicks(10_040), vortex_rs::types::TimestampNs(ta + 2_200_000_000), 200);
        ring.step_clock(vortex_rs::types::PriceTicks(10_095), vortex_rs::types::TimestampNs(ta + 6_500_000_000), 200);
        ring.step_clock(vortex_rs::types::PriceTicks(10_140), vortex_rs::types::TimestampNs(ta + 17_000_000_000), 200);
        ring.step_clock(vortex_rs::types::PriceTicks(10_120), vortex_rs::types::TimestampNs(ta + 62_000_000_000), 200);
    }
    let cf_opt = ring.take_counterfactual_abort_learning();
    assert!(cf_opt.is_some());
    if let Some((cf_side, cf_peak, cf_fav60)) = cf_opt {
        let mut eng_cold = vortex_rs::execution_fsm::SlotExecutionEngine::new(6);
        assert_eq!(eng_cold.learned_mfe_bps, 680);
        eng_cold.observe_counterfactual_abort_mfe(cf_peak);
        assert!(eng_cold.learned_mfe_bps > 730);
        let next_cr = gov.observe_counterfactual_abort_continuation_v558(
            6,
            cf_side,
            cf_peak,
            cf_fav60,
            vortex_rs::types::ScaledBps(3_600),
        );
        assert!(next_cr > 1_080);
    }
    assert!(ring.hawkes_abort_precision_patience_mult_c1() > 1.15);
}
#[test]
fn test_v559_ws_partial_t1_runner_preservation_2leg_split_penny_spread_and_c1_t2_unlock() {
    use std::sync::mpsc;
    let fees = vortex_rs::fee_cadence_beta::FeeSchedule::VIP0_DEFAULT;
    let t0 = 1_795_250_000_000_000_000_i64;

    // 1) Live WS Private `WsExitOrderUpdated` partial fill (`T1 = 13/26L`) preserves Runner at structural TP (no T1-price dump & 0 double count):
    let shm_p = vortex_rs::lob_shm::MappedShmRegion::ephemeral_self_test_path("v559_ws_t1_runner");
    let shm_res = vortex_rs::lob_shm::MappedShmRegion::open_or_create(&shm_p, true);
    assert!(shm_res.is_ok());
    let Ok(shm_reg) = shm_res else { return };

    let mut engines = vortex_rs::execution_fsm::SlotExecutionEngine::new_12_slot_array();
    let mut gov = vortex_rs::execution_fsm::OnlineAdaptiveGovernor::new();
    let mut ledger = vortex_rs::forensic_parquet::ParitySyncLedger::new(6_450_060_000);
    let mut rt = vortex_rs::exec_gateway_handlers::ExecRuntimeState::new();
    let entry_f = [vortex_rs::telemetry_hud::SlotForensicEntrySnapshot::default();
        vortex_rs::lob_shm::MAX_ACTIVE_PAIRS];
    let mut snaps = [vortex_rs::lob_shm::SymbolLobSnapshot::new_empty();
        vortex_rs::lob_shm::MAX_ACTIVE_PAIRS];
    snaps[9] = mk_vault_snap(9, 8_358, 4_200, 0.62, 0.70, 0.45, t0 + 12_000_000_000);
    snaps[0] = mk_vault_snap(0, 562_830, 3_600, 0.66, 0.78, 0.40, t0 + 12_000_000_000);

    let v_link = vortex_rs::alpha_3factor::SignalVerdict::AuthorizeMakerEntry {
        side: vortex_rs::types::Side::Bid,
        target_excursion_scaled_bps: vortex_rs::types::ScaledBps(2_400),
    };
    assert!(engines[9]
        .try_arm_maker_entry(&snaps[9], v_link, vortex_rs::types::LotQty(26))
        .is_ok());
    assert!(engines[9]
        .on_maker_entry_filled_verified(
            vortex_rs::types::TimestampNs(t0),
            Some(vortex_rs::types::Side::Bid),
            Some(vortex_rs::types::PriceTicks(8_354)),
            Some(vortex_rs::types::LotQty(26)),
        )
        .is_ok());
    let q_init_opt = engines[9].compute_canonical_exit_quote(&snaps[9]);
    assert!(q_init_opt.is_some());
    let Some(q_init) = q_init_opt else { return };
    assert_eq!(q_init.role, vortex_rs::exit_quoter::ExitOrderRole::Tranche1Harvest50);
    let t1_px = q_init.target_price;
    let t1_lots = q_init.order_lots;
    if let vortex_rs::execution_fsm::OrderLifecycleState::PositionOpenMakerExit {
        resting_exit_price: ref mut rx_p,
        active_exit_price: ref mut ax_p,
        active_exit_lots: ref mut ax_l,
        ..
    } = engines[9].state
    {
        *rx_p = t1_px;
        *ax_p = t1_px;
        *ax_l = t1_lots;
    }

    let (tg_tx, _tg_rx) = mpsc::sync_channel::<String>(8);
    let (vault_tx, _vault_rx) = mpsc::sync_channel::<vortex_rs::vault_io::VaultIoCommand>(16);
    vortex_rs::exec_gateway_handlers::handle_gateway_event(
        vortex_rs::bybit_gateway::GatewayEvent::WsExitOrderUpdated {
            slot_idx: 9,
            is_terminal: true,
            cum_exec_lots: t1_lots,
            resting_price: vortex_rs::types::PriceTicks(0),
        },
        vortex_rs::types::TimestampNs(t0 + 12_000_000_000),
        None,
        &mut rt,
        &snaps,
        &entry_f,
        &mut engines,
        &mut gov,
        &mut ledger,
        shm_reg.arena(),
        &tg_tx,
        &vault_tx,
    );
    assert_eq!(engines[9].tranche1_harvests_count, 1);
    assert!(rt.sl_needs_sync[9]);
    let rem_expected = vortex_rs::types::LotQty(26 - t1_lots.0);
    assert!(matches!(
        engines[9].state,
        vortex_rs::execution_fsm::OrderLifecycleState::PositionOpenMakerExit {
            open_lots,
            tranche1_harvested: true,
            tranche1_exit_price,
            ..
        } if open_lots == rem_expected && tranche1_exit_price == t1_px
    ));
    let q_runner_opt = engines[9].compute_canonical_exit_quote(&snaps[9]);
    assert!(q_runner_opt.is_some_and(|q| q.target_price.0 >= t1_px.0 + 15 && q.order_lots == rem_expected));

    let pnl_after_ws = engines[9].realized_partial_net_1e8;
    vortex_rs::exec_gateway_handlers::handle_gateway_event(
        vortex_rs::bybit_gateway::GatewayEvent::SlotStillOpenSynced {
            slot_idx: 9,
            real_side: vortex_rs::types::Side::Bid,
            verified_avg: Some(vortex_rs::types::PriceTicks(8_354)),
            real_lots: rem_expected,
            active_resting_tp: vortex_rs::types::PriceTicks(8_500),
        },
        vortex_rs::types::TimestampNs(t0 + 12_200_000_000),
        None,
        &mut rt,
        &snaps,
        &entry_f,
        &mut engines,
        &mut gov,
        &mut ledger,
        shm_reg.arena(),
        &tg_tx,
        &vault_tx,
    );
    assert_eq!(engines[9].tranche1_harvests_count, 1);
    assert_eq!(engines[9].realized_partial_net_1e8, pnl_after_ws);

    // 2) Exact 2-leg split feasibility (`TAO 10L`, `SOL 2L`, `LTC 4L`) & `Full <-> Split` gateway repeg (`2L -> 1L`):
    assert!(vortex_rs::exit_quoter::can_split_two_runner_legs(
        4, vortex_rs::types::LotQty(10), vortex_rs::types::PriceTicks(16_920), 500
    ));
    assert!(vortex_rs::exit_quoter::can_split_two_runner_legs(
        2, vortex_rs::types::LotQty(2), vortex_rs::types::PriceTicks(6_605), 500
    ));
    assert!(vortex_rs::exit_quoter::can_split_two_runner_legs(
        11, vortex_rs::types::LotQty(4), vortex_rs::types::PriceTicks(3_855), 500
    ));
    assert!(!vortex_rs::exit_quoter::can_split_two_runner_legs(
        5, vortex_rs::types::LotQty(6), vortex_rs::types::PriceTicks(6_024), 500
    ));
    assert!(vortex_rs::bybit_gateway::should_repeg_resting_exit_tp_with_open_lots(
        vortex_rs::types::APPROVED_SPECS[2],
        vortex_rs::types::Side::Bid,
        vortex_rs::types::PriceTicks(6_610),
        vortex_rs::types::PriceTicks(6_640),
        vortex_rs::types::LotQty(2),
        vortex_rs::types::PriceTicks(6_640),
        vortex_rs::types::LotQty(1),
        false,
        80,
        fees,
        vortex_rs::types::LotQty(2),
    ));
    assert!(!vortex_rs::bybit_gateway::should_repeg_resting_exit_tp_with_open_lots(
        vortex_rs::types::APPROVED_SPECS[3],
        vortex_rs::types::Side::Bid,
        vortex_rs::types::PriceTicks(6_155),
        vortex_rs::types::PriceTicks(6_190),
        vortex_rs::types::LotQty(6),
        vortex_rs::types::PriceTicks(6_190),
        vortex_rs::types::LotQty(5),
        false,
        80,
        fees,
        vortex_rs::types::LotQty(12),
    ));

    // 3) Sustained C1 `T1` expansion, House-Money 5ms flicker immunity & CorrectiveAbc C1 `Tranche 2` unlock:
    let mut tao_abc = mk_vault_snap(4, 16_924, 5_600, 0.66, 0.70, 0.48, t0 + 18_000_000_000);
    tao_abc.set_wave_observables(vortex_rs::wave_elliott::WaveObservables {
        regime: vortex_rs::wave_elliott::WaveRegime::CorrectiveAbc,
        climax_active: false,
        hurst: vortex_rs::wave_elliott::WaveObservables::encode_hurst(0.58),
        macro_phase: vortex_rs::types::BoundedScore::new_clamped(0.35),
        oi_impulse: vortex_rs::types::BoundedScore::ZERO,
        liq_imbalance: vortex_rs::types::BoundedScore::ZERO,
        fib_proj_bps: vortex_rs::types::ScaledBps(0),
    });
    let tao_ctx_t1 = vortex_rs::exit_quoter::OpenExitContext {
        slot_idx: 4,
        side: vortex_rs::types::Side::Bid,
        avg_price_ticks: vortex_rs::types::PriceTicks(16_920),
        peak_favorable_price: vortex_rs::types::PriceTicks(16_925),
        worst_adverse_price: vortex_rs::types::PriceTicks(16_915),
        initial_lots: vortex_rs::types::LotQty(14),
        open_lots: vortex_rs::types::LotQty(14),
        tranche1_harvested: false,
        tranche1_lots: vortex_rs::types::LotQty(0),
        tranche1_exit_price: vortex_rs::types::PriceTicks(0),
        defensive_floor_locked: false,
        active_exit_price: vortex_rs::types::PriceTicks(16_929),
        structural_tp_price: vortex_rs::types::PriceTicks(16_958),
        resting_exit_price: vortex_rs::types::PriceTicks(16_929),
        sl_trigger_price: vortex_rs::types::PriceTicks(16_620),
        opened_ts: vortex_rs::types::TimestampNs(t0),
        learned_mfe_bps: 980,
        learned_t1_permille: 500,
        learned_ir_x100: 35,
        learned_ret_std_bps: 200,
        intrinsic_tau_mkt_s: 240,
        last_eff_sl_ticks: vortex_rs::types::PriceTicks(16_620),
        ratchet_locked_gross_bps: 0,
        last_closed_mfe_bps: 30,
        last_closed_mae_bps: -30,
        active_fees: fees,
        learned_taker_prob_pm: 68,
    };
    let kin_sustained = vortex_rs::exit_quoter::ShadowRunnerKinematics {
        flow_ema_fast: 0.62,
        flow_ema_slow: 0.60,
        in_scar_debt: false,
        chandelier_armed: false,
    };
    let base_win1_tao = vortex_rs::exit_quoter::elastic_offensive_win_maker_exit_price(
        vortex_rs::types::Side::Bid,
        vortex_rs::types::PriceTicks(16_920),
        400,
        tao_abc.realized_vol_5m,
        0.55,
    );
    let q_tao_t1 = vortex_rs::exit_quoter::compute_canonical_exit_quote_omega(
        &tao_ctx_t1,
        kin_sustained,
        &tao_abc,
    );
    assert!(q_tao_t1.is_some_and(|q| {
        q.role == vortex_rs::exit_quoter::ExitOrderRole::Tranche1Harvest50
            && q.target_price.0 > base_win1_tao.0
    }));

    let mut tao_ctx_t2 = tao_ctx_t1;
    tao_ctx_t2.tranche1_harvested = true;
    tao_ctx_t2.tranche1_lots = vortex_rs::types::LotQty(6);
    tao_ctx_t2.tranche1_exit_price = vortex_rs::types::PriceTicks(16_929);
    tao_ctx_t2.open_lots = vortex_rs::types::LotQty(8);
    tao_ctx_t2.learned_mfe_bps = 939;
    let v55_tao = vortex_rs::exit_quoter::CascadeAndAgingContextV55 {
        tranche1_harvests_count: 1,
        tranche2_harvested: false,
        harvested_legs: 1,
        psi_health: 0.92,
        cont_ratio_x1000: 995,
        last_peak_favorable_ts_ns: t0 + 18_000_000_000,
        zeta_tox: 0.05,
        house_money_runner_unlocked: true,
    };
    let q_tao_t2 = vortex_rs::exit_quoter::compute_canonical_exit_quote_omega_v55(
        &tao_ctx_t2,
        kin_sustained,
        v55_tao,
        &tao_abc,
        tao_abc.realized_vol_5m,
        None,
        0,
        70_000,
    );
    assert!(q_tao_t2.is_some_and(|q| q.role == vortex_rs::exit_quoter::ExitOrderRole::Tranche2HarvestPhi));

    let mut flicker_snap = tao_abc;
    flicker_snap.bids[0].price = vortex_rs::types::PriceTicks(16_929);
    flicker_snap.asks[0].price = vortex_rs::types::PriceTicks(16_930);
    flicker_snap.factor_ofi = vortex_rs::types::BoundedScore::new_clamped(-0.25);
    flicker_snap.factor_tfi = vortex_rs::types::BoundedScore::new_clamped(-0.15);
    let mut tao_hm_runner = tao_ctx_t2;
    tao_hm_runner.peak_favorable_price = vortex_rs::types::PriceTicks(16_930);
    tao_hm_runner.ratchet_locked_gross_bps = 540;
    tao_hm_runner.last_closed_mfe_bps = 591;
    let q_hm_hold = vortex_rs::exit_quoter::compute_canonical_exit_quote_omega_v55(
        &tao_hm_runner,
        vortex_rs::exit_quoter::ShadowRunnerKinematics {
            flow_ema_fast: 0.42,
            flow_ema_slow: 0.48,
            in_scar_debt: false,
            chandelier_armed: false,
        },
        v55_tao,
        &flicker_snap,
        tao_abc.realized_vol_5m,
        None,
        0,
        70_000,
    );
    assert!(q_hm_hold.is_some_and(|q| {
        !q.defensive_lock_active
            && q.sub_reason != vortex_rs::exit_quoter::ExitSubReason::ConvexMfeRatchetLock
    }));

    // 4) Wide-tick Penny-Jump spread room (`ORDI SPR=2`) & `EventClock` wide-tick lattice spoof immunity (`WIF`):
    let mut ordi_pj = mk_vault_snap(6, 2_348, 4_200, 0.74, 0.76, 0.52, t0);
    ordi_pj.bids[0].price = vortex_rs::types::PriceTicks(2_348);
    ordi_pj.asks[0].price = vortex_rs::types::PriceTicks(2_350);
    let v_ordi_ok = vortex_rs::alpha_3factor::OrthogonalFactorState::evaluate_entry_signal_v554(
        vortex_rs::types::APPROVED_SPECS[6],
        &ordi_pj,
        &snaps[0],
        vortex_rs::types::ScaledBps(820),
        vortex_rs::types::ScaledBps(200),
        50,
        35,
        200.0,
        fees,
        0.0,
        1.0,
        0.80,
        Some(0.65),
        0.95,
        0.0,
        1.0,
    );
    assert!(matches!(
        v_ordi_ok,
        vortex_rs::alpha_3factor::SignalVerdict::AuthorizeMakerEntry { side: vortex_rs::types::Side::Bid, .. }
    ));
    let v_ordi_low_psi = vortex_rs::alpha_3factor::OrthogonalFactorState::evaluate_entry_signal_v554(
        vortex_rs::types::APPROVED_SPECS[6],
        &ordi_pj,
        &snaps[0],
        vortex_rs::types::ScaledBps(820),
        vortex_rs::types::ScaledBps(200),
        50,
        35,
        200.0,
        fees,
        0.0,
        1.0,
        0.80,
        Some(0.65),
        0.10,
        0.0,
        1.0,
    );
    assert_eq!(v_ordi_low_psi, vortex_rs::alpha_3factor::SignalVerdict::NeutralNoAction);

    let wif_wide = mk_vault_snap(8, 1_138, 4_200, 0.72, 0.02, 0.45, t0 + 1_000_000_000);
    let mut clk_wif = vortex_rs::alpha_3factor::EventClockAlphaModulator::new();
    let _ = clk_wif.step_event_clock_and_spoof_with_snap(&wif_wide, fees);
    let mut wif_wide2 = wif_wide;
    wif_wide2.ts_local_ns = vortex_rs::types::TimestampNs(t0 + 3_000_000_000);
    let _ = clk_wif.step_event_clock_and_spoof_with_snap(&wif_wide2, fees);
    assert!(clk_wif.spoofing_density() < 0.05);

    drop(shm_reg);
}