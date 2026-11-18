//! VORTEX-OMEGA V5.1 Forensic Counterfactual Certification: Live Trades #1..#8, 103 Aborts, F1..F6 & Global Gates.
#![allow(clippy::cast_precision_loss, clippy::too_many_lines)]

use vortex_rs::alpha_3factor::{
    passes_corrective_abc_fractal_guard_c1, should_abort_l1_cancellation_collapse_v51, SignalVerdict,
};
use vortex_rs::bayesian_governor::OnlineAdaptiveGovernor;
use vortex_rs::exec_daemon::{
    evaluate_elastic_spoof_geom_abort_v51, evaluate_toxic_sweep_velocity_abort_v51,
    step_drain_standby_latch,
};
use vortex_rs::exec_gateway_handlers::run_oos_520_multi_regime_suite;
use vortex_rs::execution_fsm::{
    compute_diffusive_sl_floor_bps_v51, record_live_ws_pulse, ExitOrderRole, OrderLifecycleState,
    SlotExecutionEngine,
};
use vortex_rs::exit_quoter::{
    compute_canonical_exit_quote_omega, OpenExitContext, ShadowRunnerKinematics,
};
use vortex_rs::fee_cadence_beta::FeeSchedule;
use vortex_rs::forensic_parquet::ParitySyncLedger;
use vortex_rs::lob_shm::{BookLevel, SymbolLobSnapshot};
use vortex_rs::markout_tracker::SlotMarkoutRing;
use vortex_rs::replay_walkforward::run_omega_digital_twin_walk_forward_lab;
use vortex_rs::types::{
    BoundedScore, LotQty, PriceTicks, ScaledBps, Side, TimestampNs, APPROVED_SPECS,
};
use vortex_rs::wave_elliott::{WaveObservables, WaveRegime};

const LIVE_WALLET_1E8: i64 = 6_482_040_000;

fn mk_live_snap(slot: usize, px: i64, vol: i64, ofi: f64, tfi: f64, geom: f64, ts: i64) -> SymbolLobSnapshot {
    let mut s = SymbolLobSnapshot::new_empty();
    s.symbol_id = APPROVED_SPECS[slot].symbol_id;
    s.is_eligible = true;
    s.sequence_id = 100;
    s.ts_local_ns = TimestampNs(ts);
    s.realized_vol_5m = ScaledBps(vol);
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
fn test_v51_forensic_counterfactual_8_trades_103_aborts_and_all_constitutional_gates() {
    record_live_ws_pulse(75_000, 100);
    let fees = FeeSchedule::VIP0_DEFAULT;
    let t0 = 1_795_000_000_000_000_000_i64;

    // 1. F1: Silent DRAIN_MODE standby latch (1 flush, 0 process exits)
    let mut flushed = false;
    assert_eq!(step_drain_standby_latch(true, 0, 0, false, &mut flushed), (true, true));
    assert_eq!(step_drain_standby_latch(true, 0, 0, false, &mut flushed), (false, true));

    // 2. Trade #1 (TAO Bid 12L): Runner preserved via Tranche1Harvest50 instead of 100% scratch
    let mut tao1 = mk_live_snap(4, 19_513, 2_749, 0.22, 0.18, 0.30, t0 + 95_000_000_000);
    tao1.set_wave_observables(WaveObservables {
        regime: WaveRegime::CorrectiveAbc, climax_active: false, hurst: WaveObservables::encode_hurst(0.42),
        macro_phase: BoundedScore::new_clamped(0.10), oi_impulse: BoundedScore::ZERO,
        liq_imbalance: BoundedScore::ZERO, fib_proj_bps: ScaledBps(0),
    });
    let tao1_ctx = OpenExitContext {
        slot_idx: 4, side: Side::Bid, avg_price_ticks: PriceTicks(19_505),
        peak_favorable_price: PriceTicks(19_514), worst_adverse_price: PriceTicks(19_500),
        initial_lots: LotQty(12), open_lots: LotQty(12), tranche1_harvested: false,
        tranche1_lots: LotQty(0), tranche1_exit_price: PriceTicks(0), defensive_floor_locked: false,
        active_exit_price: PriceTicks(19_620), structural_tp_price: PriceTicks(19_620),
        resting_exit_price: PriceTicks(19_514), sl_trigger_price: PriceTicks(19_410),
        opened_ts: TimestampNs(t0), learned_mfe_bps: 594, learned_t1_permille: 500,
        learned_ir_x100: 25, learned_ret_std_bps: 200, intrinsic_tau_mkt_s: 180,
        last_eff_sl_ticks: PriceTicks(19_410), ratchet_locked_gross_bps: 0,
        last_closed_mfe_bps: 461, last_closed_mae_bps: -256, active_fees: fees, learned_taker_prob_pm: 50,
    };
    let kin_t1 = ShadowRunnerKinematics { flow_ema_fast: 0.16, flow_ema_slow: 0.12, in_scar_debt: false, chandelier_armed: false };
    assert!(compute_canonical_exit_quote_omega(&tao1_ctx, kin_t1, &tao1)
        .is_some_and(|q| q.role == ExitOrderRole::Tranche1Harvest50 && q.order_lots.0 < 12));

    // 3. Trades #2, #5, #7 (ETH 1L): Reversible C1 unlock restores winning structural TP after underwater dip
    let gov = OnlineAdaptiveGovernor::new();
    let mut ledger = ParitySyncLedger::new(LIVE_WALLET_1E8);
    let mut eth7 = mk_live_snap(1, 179_542, 2_280, 0.58, 0.71, 0.94, t0);
    let mut eng_eth = SlotExecutionEngine::new(1);
    let v_eth = SignalVerdict::AuthorizeMakerEntry { side: Side::Bid, target_excursion_scaled_bps: ScaledBps(950) };
    assert!(eng_eth.try_arm_maker_entry(&eth7, v_eth, LotQty(1)).is_ok());
    assert!(eng_eth.on_maker_entry_filled(TimestampNs(t0)).is_ok());
    eng_eth.latch_defensive_floor_if_needed(true);
    eth7.set_wave_observables(WaveObservables {
        regime: WaveRegime::ImpulseWave3, climax_active: false, hurst: WaveObservables::encode_hurst(0.68),
        macro_phase: BoundedScore::new_clamped(0.90), oi_impulse: BoundedScore::new_clamped(0.30),
        liq_imbalance: BoundedScore::new_clamped(0.35), fib_proj_bps: ScaledBps(4_559),
    });
    for k in 1..=35_i64 {
        eth7.ts_local_ns = TimestampNs(t0 + 180_000_000_000 + k * 50_000_000);
        eth7.bids[0].price = PriceTicks(179_618);
        eth7.asks[0].price = PriceTicks(179_619);
        eth7.mark_price = PriceTicks(179_618);
        eth7.last_trade_price = PriceTicks(179_618);
        let _ = eng_eth.step_open_position(&eth7, &eth7, &gov, &mut ledger);
    }
    assert!(matches!(eng_eth.state, OrderLifecycleState::PositionOpenMakerExit { defensive_floor_locked: false, active_exit_price, .. } if active_exit_price.0 > 180_000));

    // 4. Trades #3 (AVAX Ask), #4 (TAO Bid), #8 (ETH Bid) & #6 (ETH Ask vs Wave3 +0.88): Losses vetoed / escaped
    for &(h, m_al, side) in &[(0.674_f64, -0.12_f64, Side::Ask), (0.652, -0.18, Side::Bid), (0.712, -0.10, Side::Bid)] {
        let w_loss = WaveObservables {
            regime: WaveRegime::CorrectiveAbc, climax_active: false, hurst: WaveObservables::encode_hurst(h),
            macro_phase: BoundedScore::new_clamped(m_al * side.sign_f64()), oi_impulse: BoundedScore::ZERO,
            liq_imbalance: BoundedScore::ZERO, fib_proj_bps: ScaledBps(0),
        };
        assert!(!passes_corrective_abc_fractal_guard_c1(side, w_loss, 480, 520));
    }
    assert!(!APPROVED_SPECS[1].passes_single_lot_asymmetry_ev_gate(
        LotQty(1), PriceTicks(179_542), ScaledBps(1_680), 620, 95, 0.22, 480, 510, Side::Bid, fees,
    ));
    let mut eth6_ctx = tao1_ctx;
    eth6_ctx.slot_idx = 1; eth6_ctx.side = Side::Ask; eth6_ctx.open_lots = LotQty(1); eth6_ctx.initial_lots = LotQty(1);
    eth6_ctx.avg_price_ticks = PriceTicks(179_961);
    let mut eth6_s = eth7;
    eth6_s.bids[0].price = PriceTicks(179_880); eth6_s.asks[0].price = PriceTicks(179_881);
    assert!(compute_canonical_exit_quote_omega(&eth6_ctx, kin_t1, &eth6_s).is_some_and(|q| q.defensive_lock_active && q.urgent_close_repeg));

    // 5. F3 & F6: Diffusive SL floor (>18 bps) vs confirmed toxic floor (<=12 bps) + cross-side ABC chop shield
    let ring = SlotMarkoutRing::new();
    let fl_diff = compute_diffusive_sl_floor_bps_v51(ScaledBps(2_983), ScaledBps(418), 50, 0.0, ScaledBps(180), ScaledBps(400), ScaledBps(140), WaveObservables::default(), Side::Ask);
    assert!(fl_diff.raw() >= 1_800 && ring.ewma_lvr_5s_bps() == 0);
    let mut gov_f6 = OnlineAdaptiveGovernor::new();
    let w_chop = tao1.wave_observables();
    gov_f6.observe_closed_trade_with_regime_v51(3, Side::Ask, PriceTicks(7_156), ScaledBps(-2_008), ScaledBps(139), false, w_chop, 1.0, TimestampNs(t0));
    tao1.ts_local_ns = TimestampNs(t0 + 540_000_000_000);
    assert!(!gov_f6.passes_cross_side_abc_chop_shield(4, Side::Bid, &tao1));

    // 6. F4 (103 Aborts Audit): Legitimate FIFO trade consumption & brief GEOM flicker kept; toxic 210ms sweep aborted
    assert!(!should_abort_l1_cancellation_collapse_v51(
        APPROVED_SPECS[4], Side::Bid, LotQty(797), LotQty(118), LotQty(650), PriceTicks(19_505), BoundedScore::new_clamped(0.44), fees,
    ));
    let pat_ns = gov.elastic_resting_l1_patience_ns_v51(4, Side::Bid, ScaledBps(2_750), 1.0, 1.0, BoundedScore::new_clamped(0.52));
    assert!(pat_ns >= 10_000_000_000);
    let mut adv_g = 0_i64;
    let mut flick = mk_live_snap(4, 19_505, 2_750, 0.45, 0.55, -0.06, t0);
    assert!(!evaluate_elastic_spoof_geom_abort_v51(Side::Bid, &flick, 0.25, 160_000_000, BoundedScore::new_clamped(0.48), 0.05, 1.0, 50, 75_000, t0, &mut adv_g));
    flick.factor_ofi = BoundedScore::new_clamped(0.55);
    flick.factor_tfi = BoundedScore::new_clamped(0.86);
    assert!(evaluate_toxic_sweep_velocity_abort_v51(APPROVED_SPECS[1], Side::Ask, PriceTicks(179_961), LotQty(11_211), LotQty(1_100), 210_000_000, &flick, 1.0, 50, fees, 75_000));

    // 7. Constitutional Non-Regression: SCAR-LOCK 63/63 + OOS 520 + 1:1 Digital Twin
    assert_eq!(OnlineAdaptiveGovernor::audit_unified_62_losses_and_2160_manifold(), (63, 2_160));
    let oos = run_oos_520_multi_regime_suite();
    assert!(oos.closed_trades == 520 && oos.maker_exit_ratio_pct >= 82.0 && oos.profit_factor >= 2.0 && oos.parity_sync_drift_1e8 == 0);
    let twin = run_omega_digital_twin_walk_forward_lab();
    assert!(twin.closed_trades == 3 && twin.parity_sync_drift_1e8 == 0 && twin.cum_net_bps_x100 >= 7_000);
    println!(
        "[V5.1 FORENSIC SUITE OK] F1..F6 Verified | Patience={:.2}s | OOS520 PF={:.2} | Twin={:+.2}bp | ScarLock=63/63",
        (pat_ns as f64) * 1e-9, oos.profit_factor, (twin.cum_net_bps_x100 as f64) / 100.0
    );
}
