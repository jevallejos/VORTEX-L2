//! V5.5.4 Canonical Verification: Diffusive Zeta (TAO #4 / AVAX #1), LINK #3 Runner Unlock & SUI Bid Queue Hysteresis.

use vortex_rs::alpha_3factor::{INV_PHI, INV_PHI_FOURTH, INV_PHI_SQ, SignalVerdict};
use vortex_rs::bayesian_governor::OnlineAdaptiveGovernor;
use vortex_rs::exec_daemon::{
    compute_moallemi_yuan_queue_priority_c1, compute_resting_macro_deadband_c1,
    evaluate_resting_repeg_candidate_v554, favorable_bbo_miss_ticks,
    is_resting_headwind_erosion_v554, passes_resting_elliott_macro_hysteresis_c1,
};
use vortex_rs::execution_fsm::{
    ExitOrderRole, OrderLifecycleState, SlotExecutionEngine,
    passes_elliott_macro_coherence_guard_with_health,
};
use vortex_rs::exit_quoter::{
    compute_house_money_sl_floor_ticks, compute_rebound_probability_pi_c1,
    defensive_zero_loss_maker_exit_price_with_fees,
};
use vortex_rs::fee_cadence_beta::FeeSchedule;
use vortex_rs::kinematic_surface::is_headwind_queue_erosion_c1;
use vortex_rs::lob_shm::{BookLevel, SymbolLobSnapshot};
use vortex_rs::markout_tracker::{MARKOUT_MASK_15S, MarkoutCurveBps, SlotMarkoutRing, glosten_milgrom_toxicity_zeta_diffusive};
use vortex_rs::types::{APPROVED_SPECS, BoundedScore, LotQty, PriceTicks, ScaledBps, Side, TimestampNs};
use vortex_rs::wave_elliott::{WaveObservables, WaveRegime};

fn mk_l5_snap(slot: usize, bid_px: i64, vol_sbps: i64, ofi: f64, tfi: f64, geom: f64) -> SymbolLobSnapshot {
    let mut s = SymbolLobSnapshot::new_empty();
    s.symbol_id = APPROVED_SPECS[slot].symbol_id;
    s.is_eligible = true;
    s.sequence_id = 10;
    s.ts_local_ns = TimestampNs(10_000_000_000);
    s.realized_vol_5m = ScaledBps(vol_sbps);
    for lvl in 0..5_usize {
        let step = i64::try_from(lvl).unwrap_or(0);
        s.bids[lvl] = BookLevel { price: PriceTicks(bid_px - step), qty: LotQty(450) };
        s.asks[lvl] = BookLevel { price: PriceTicks(bid_px + 1 + step), qty: LotQty(220) };
    }
    s.refresh_deep_l1_l5_totals();
    s.mark_price = PriceTicks(bid_px);
    s.last_trade_price = PriceTicks(bid_px);
    s.factor_ofi = BoundedScore::new_clamped(ofi);
    s.factor_tfi = BoundedScore::new_clamped(tfi);
    s.factor_geom = BoundedScore::new_clamped(geom);
    s
}

#[test]
fn test_v554_tao4_reversion_vs_avax1_diffusive_zeta_tox() {
    let tao4 = MarkoutCurveBps { samples_bps: [-113, -339, -396, -56], resolved_mask: MARKOUT_MASK_15S, initial_edge_bps: 0 };
    let z_tao4 = glosten_milgrom_toxicity_zeta_diffusive(tao4, ScaledBps(400), ScaledBps(4_539));
    assert!(z_tao4 < INV_PHI);

    let avax1 = MarkoutCurveBps { samples_bps: [-153, -153, -306, -1_226], resolved_mask: MARKOUT_MASK_15S, initial_edge_bps: 0 };
    let z_avax1 = glosten_milgrom_toxicity_zeta_diffusive(avax1, ScaledBps(400), ScaledBps(3_490));
    assert!(z_avax1 > 1.2);

    let mut ring = SlotMarkoutRing::new();
    let t0 = TimestampNs(5_000_000_000);
    ring.arm_fill_probe_with_vol(Side::Bid, PriceTicks(100_000), PriceTicks(100_000), t0, ScaledBps(4_539), ScaledBps(400));
    ring.step_clock(PriceTicks(99_989), TimestampNs(t0.0 + 300_000_000), 200);
    ring.step_clock(PriceTicks(99_966), TimestampNs(t0.0 + 1_100_000_000), 200);
    ring.step_clock(PriceTicks(99_960), TimestampNs(t0.0 + 5_100_000_000), 200);
    ring.step_clock(PriceTicks(99_994), TimestampNs(t0.0 + 15_100_000_000), 200);
    assert!(ring.active_fill_toxicity_zeta_with_vol(t0, ScaledBps(400), ScaledBps(4_539)) < INV_PHI);
}

#[test]
fn test_v554_link3_partial_t1_quantum_floor_and_house_money_runner_unlock() {
    let mut link = mk_l5_snap(9, 10_720, 3_200, 0.62, 0.58, 0.35);
    assert!(compute_rebound_probability_pi_c1(Side::Bid, &link, 0.25) >= INV_PHI_SQ);
    let mut eng = SlotExecutionEngine::new(9);
    eng.learned_mfe_bps = 900;
    let verdict = SignalVerdict::AuthorizeMakerEntry { side: Side::Bid, target_excursion_scaled_bps: ScaledBps(900) };
    assert!(eng.try_arm_maker_entry(&link, verdict, LotQty(16)).is_ok());
    assert!(eng.on_maker_entry_filled(link.ts_local_ns).is_ok());
    eng.latch_defensive_floor_if_needed(true);

    let qf = defensive_zero_loss_maker_exit_price_with_fees(Side::Bid, PriceTicks(10_720), eng.active_fees);
    let q_t1 = eng.compute_canonical_exit_quote(&link);
    assert!(q_t1.is_some_and(|q| q.role == ExitOrderRole::Tranche1Harvest50 && q.target_price == qf && q.order_lots.0 < 16));
    assert!(eng.record_tranche1_harvest(qf));
    assert!(eng.house_money_runner_unlocked);
    let OrderLifecycleState::PositionOpenMakerExit { open_lots, defensive_floor_locked, .. } = eng.state else {
        assert!(eng.house_money_runner_unlocked && !eng.is_house_money_free_ride());
        return;
    };
    assert!(!defensive_floor_locked && open_lots.0 < 16);
    let hm_floor = compute_house_money_sl_floor_ticks(9, Side::Bid, PriceTicks(10_720), open_lots, eng.realized_partial_net_1e8, eng.active_fees);
    assert!(eng.last_eff_sl_ticks.0 > 10_720);
    link.ts_local_ns = TimestampNs(25_000_000_000);
    link.bids[0].price = PriceTicks(10_735);
    link.asks[0].price = PriceTicks(10_736);
    link.mark_price = PriceTicks(10_735);
    link.last_trade_price = PriceTicks(10_735);
    let _ = eng.maybe_ratchet_chandelier_or_wald_sl(&link, &link);
    assert!(eng.last_eff_sl_ticks.0 >= hm_floor.0 && eng.is_house_money_free_ride());
    assert!(eng.compute_canonical_exit_quote(&link).is_some_and(|q| !q.defensive_lock_active && q.target_price.0 > qf.0));
}

#[test]
fn test_v554_sui_bid_moallemi_yuan_priority_hysteresis_and_fav_bbo_miss_repeg() {
    let v_prio = compute_moallemi_yuan_queue_priority_c1(LotQty(120), LotQty(18));
    assert!(v_prio > INV_PHI && compute_resting_macro_deadband_c1(v_prio) > INV_PHI_FOURTH);

    let btc = mk_l5_snap(0, 650_000, 3_600, 0.55, 0.50, 0.45);
    let mut sui_macro = mk_l5_snap(5, 20_000, 4_800, 0.45, 0.38, 0.40);
    sui_macro.set_wave_observables(WaveObservables {
        regime: WaveRegime::NeutralWarmup,
        climax_active: false,
        hurst: WaveObservables::encode_hurst(0.510),
        macro_phase: BoundedScore::new_clamped(-0.252),
        oi_impulse: BoundedScore::ZERO,
        liq_imbalance: BoundedScore::ZERO,
        fib_proj_bps: ScaledBps(0),
    });
    assert!(!passes_elliott_macro_coherence_guard_with_health(Side::Bid, &sui_macro, &btc, 0.40, None, 0.80));
    assert!(passes_resting_elliott_macro_hysteresis_c1(Side::Bid, &sui_macro, &btc, 0.40, None, 0.80, v_prio));

    let sui_hw = mk_l5_snap(5, 20_000, 4_800, 0.30, -0.65, 0.25);
    let raw_hw = is_headwind_queue_erosion_c1(5, 2_500_000_000, 1_500_000_000, LotQty(100), LotQty(20), -0.45, -0.1275);
    assert!(raw_hw);
    assert!(!is_resting_headwind_erosion_v554(5, Side::Bid, 2_500_000_000, 1_500_000_000, LotQty(100), LotQty(20), false, -0.45, -0.1275, &sui_hw, BoundedScore::new_clamped(0.08)));
    assert!(is_resting_headwind_erosion_v554(5, Side::Bid, 2_500_000_000, 1_500_000_000, LotQty(100), LotQty(20), false, -0.45, -0.1275, &sui_hw, BoundedScore::new_clamped(-0.05)));

    let gov = OnlineAdaptiveGovernor::new();
    let sui_repeg = mk_l5_snap(5, 20_002, 4_800, 0.72, 0.74, 0.52);
    assert_eq!(favorable_bbo_miss_ticks(Side::Bid, PriceTicks(20_000), &sui_repeg), 2);
    let ok_repeg = evaluate_resting_repeg_candidate_v554(
        &gov, 5, Side::Bid, 6_500_000_000, LotQty(8), PriceTicks(20_000), PriceTicks(20_000),
        ScaledBps(950), ScaledBps(550), 50, FeeSchedule::VIP0_DEFAULT, &sui_repeg, &btc, 0.0, 0.8, 0.5, None, 0.0, 0.85, 0.20, v_prio,
    );
    assert!(ok_repeg.is_some_and(|(px, ev)| px.0 >= 20_002 && ev > 0));
    let neg_ev = evaluate_resting_repeg_candidate_v554(
        &gov, 5, Side::Bid, 6_500_000_000, LotQty(8), PriceTicks(20_000), PriceTicks(20_000),
        ScaledBps(420), ScaledBps(420), 50, FeeSchedule::VIP0_DEFAULT, &sui_repeg, &btc, 0.0, 0.8, 0.5, None, 0.0, 0.85, 0.20, v_prio,
    );
    assert!(neg_ev.is_none());
}
