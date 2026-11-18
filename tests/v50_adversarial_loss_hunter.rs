//! Red-Team 12-Vector Adversarial Unseen-Loss & Plasticity Hunter (VORTEX-OMEGA V5.0.1).
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::too_many_lines
)]

use std::sync::mpsc;
use vortex_rs::alpha_3factor::{
    should_abort_collapsed_resting_l1_wall, EppsBtcCorrelationTracker, OrthogonalFactorState,
    SignalVerdict,
};
use vortex_rs::bayesian_governor::OnlineAdaptiveGovernor;
use vortex_rs::bybit_client::InstrumentSpecVerification;
use vortex_rs::exec_daemon::{
    clamp_resting_queue_ahead_physical, select_safe_live_sl_ticks_with_fees,
    settle_live_8h_funding_cuts_v48, step_12_slot_daemon_tick,
};
use vortex_rs::exec_gateway_handlers::ExecRuntimeState;
use vortex_rs::execution_fsm::{ClosedTradeKind, OrderLifecycleState, SlotExecutionEngine};
use vortex_rs::fee_cadence_beta::FeeSchedule;
use vortex_rs::forensic_parquet::ParitySyncLedger;
use vortex_rs::lob_shm::{BookLevel, MappedShmRegion, SymbolLobSnapshot, MAX_ACTIVE_PAIRS};
use vortex_rs::markout_tracker::SlotMarkoutRing;
use vortex_rs::telemetry_hud::SlotForensicEntrySnapshot;
use vortex_rs::types::{
    BoundedScore, LotQty, PriceTicks, ScaledBps, Side, TimestampNs, APPROVED_SPECS,
};
use vortex_rs::vault_io::VaultIoCommand;

const LIVE_WALLET_1E8: i64 = 6_499_600_000;
const LIVE_TICKS_NOV2026: [i64; 12] = [
    642_845, 180_249, 7_437, 7_138, 19_511, 7_255, 2_760, 14_160, 1_339, 9_155, 3_362, 4_443,
];

fn mk_snap(slot: usize, px: i64, vol_sbps: i64, ofi: f64, tfi: f64, geom: f64, ts: i64) -> SymbolLobSnapshot {
    let mut s = SymbolLobSnapshot::new_empty();
    s.symbol_id = APPROVED_SPECS[slot].symbol_id;
    s.is_eligible = true;
    s.sequence_id = 100;
    s.ts_local_ns = TimestampNs(ts);
    s.realized_vol_5m = ScaledBps(vol_sbps);
    s.bids[0] = BookLevel { price: PriceTicks(px), qty: LotQty(1_500) };
    s.asks[0] = BookLevel { price: PriceTicks(px + 1), qty: LotQty(1_500) };
    s.refresh_deep_l1_l5_totals();
    s.mark_price = PriceTicks(px);
    s.last_trade_price = PriceTicks(px);
    s.factor_ofi = BoundedScore::new_clamped(ofi);
    s.factor_tfi = BoundedScore::new_clamped(tfi);
    s.factor_geom = BoundedScore::new_clamped(geom);
    s.ob_bull_efficacy_pm = 580;
    s.ob_bear_efficacy_pm = 540;
    s
}

#[test]
fn test_v50_red_team_12_vector_elastic_and_self_learning_matrix() {
    let gov = OnlineAdaptiveGovernor::new();
    let fees = FeeSchedule::VIP0_DEFAULT;
    let t0 = 1_794_972_000_000_000_000_i64;
    let ring = SlotMarkoutRing::new();

    println!("\n==========================================================================================");
    println!("[VORTEX-OMEGA V5.0.1 RED-TEAM 12-VECTOR MATRIX] Elastic C1 + Self-Learning Audit");
    println!("==========================================================================================");

    // V1: MFE Dead-Zone Reversal (+5.38bp bounce -> underwater dump) + Self-Learning p_Taker check
    let mut s_eth = mk_snap(1, 180_249, 2_800, 0.72, 0.88, 0.92, t0);
    let mut eng1 = SlotExecutionEngine::new(1);
    eng1.learned_mfe_bps = 680;
    let v1 = SignalVerdict::AuthorizeMakerEntry { side: Side::Bid, target_excursion_scaled_bps: ScaledBps(680) };
    assert!(eng1.try_arm_maker_entry(&s_eth, v1, LotQty(1)).is_ok());
    assert!(eng1.on_maker_entry_filled(TimestampNs(t0)).is_ok());
    let init_sl_dist = 180_249 - eng1.last_eff_sl_ticks.0;
    let mut ledger1 = ParitySyncLedger::new(LIVE_WALLET_1E8);
    s_eth.ts_local_ns = TimestampNs(t0 + 4_000_000_000);
    s_eth.last_trade_price = PriceTicks(180_346);
    s_eth.mark_price = PriceTicks(180_346);
    let _ = eng1.step_open_position_v49(&s_eth, &s_eth, &gov, &mut ledger1, 1.0, &ring, 1.0);
    for sec in 5..=90_i64 {
        let drop_px = 180_249 - sec * 6;
        s_eth.ts_local_ns = TimestampNs(t0 + sec * 1_000_000_000);
        s_eth.bids[0].price = PriceTicks(drop_px);
        s_eth.asks[0].price = PriceTicks(drop_px + 1);
        s_eth.mark_price = PriceTicks(drop_px);
        s_eth.last_trade_price = PriceTicks(drop_px);
        s_eth.factor_ofi = BoundedScore::new_clamped(-0.82);
        s_eth.factor_tfi = BoundedScore::new_clamped(-0.96);
        let _ = eng1.step_open_position_v49(&s_eth, &s_eth, &gov, &mut ledger1, 1.0, &ring, 1.0);
    }
    let v1_comp_pct = ((init_sl_dist - (180_249 - eng1.last_eff_sl_ticks.0)) * 100) / init_sl_dist.max(1);
    println!("  [V01 C1_MFE_GIVEBACK_TRAP ] InitSL={}t -> {}t | Shrink={}% -> SEALED", init_sl_dist, 180_249 - eng1.last_eff_sl_ticks.0, v1_comp_pct);
    assert!(v1_comp_pct >= 55);

    // V2: Partial-Fill Orphan (SUI 30L -> 1L orphan reversal)
    let mut s_sui = mk_snap(5, 7_255, 2_800, -0.75, -0.90, -0.78, t0);
    let mut eng2 = SlotExecutionEngine::new(5);
    eng2.learned_mfe_bps = 765;
    let v2 = SignalVerdict::AuthorizeMakerEntry { side: Side::Ask, target_excursion_scaled_bps: ScaledBps(765) };
    assert!(eng2.try_arm_maker_entry(&s_sui, v2, LotQty(16)).is_ok());
    assert!(eng2.on_maker_entry_filled_verified(TimestampNs(t0), Some(Side::Ask), Some(PriceTicks(7_255)), Some(LotQty(1))).is_ok());
    let sui_init_sl = eng2.last_eff_sl_ticks.0 - 7_255;
    let mut ledger2 = ParitySyncLedger::new(LIVE_WALLET_1E8);
    for sec in 1..=45_i64 {
        s_sui.ts_local_ns = TimestampNs(t0 + sec * 1_000_000_000);
        s_sui.bids[0].price = PriceTicks(7_256 + sec / 3);
        s_sui.asks[0].price = PriceTicks(7_257 + sec / 3);
        s_sui.mark_price = s_sui.bids[0].price;
        s_sui.last_trade_price = s_sui.bids[0].price;
        s_sui.factor_ofi = BoundedScore::new_clamped(0.78);
        s_sui.factor_tfi = BoundedScore::new_clamped(0.95);
        let _ = eng2.step_open_position_v49(&s_sui, &s_sui, &gov, &mut ledger2, 1.0, &ring, 1.0);
    }
    let v2_shrink = ((sui_init_sl - (eng2.last_eff_sl_ticks.0 - 7_255)) * 100) / sui_init_sl.max(1);
    println!("  [V02 PARTIAL_ORPHAN_16L_1L] OrphanSL={}t -> {}t | Shrink={}% -> SEALED", sui_init_sl, eng2.last_eff_sl_ticks.0 - 7_255, v2_shrink);
    assert!(v2_shrink >= 60);

    // V3: Elastic Pre-Funding Cliff Gate + Endogenous Open-Cut Zero-Loss Elevation
    let cut_ns = 62_326_i64 * 28_800 * 1_000_000_000;
    let mut engines3 = SlotExecutionEngine::new_12_slot_array();
    let mut snaps3 = [SymbolLobSnapshot::new_empty(); MAX_ACTIVE_PAIRS];
    snaps3[3] = mk_snap(3, 7_138, 3_500, 0.70, 0.85, 0.65, cut_ns - 5_000_000_000);
    snaps3[3].funding_rate_scaled_bps = ScaledBps(280);
    let blocked_5s_before = gov.authorizes_adaptive_entry_v49(3, Side::Bid, LIVE_WALLET_1E8, 1_000, ScaledBps(650), &snaps3[3], &snaps3[3], 0.0, 1.0).is_err();
    let mut snap_20m_before = snaps3[3];
    snap_20m_before.ts_local_ns = TimestampNs(cut_ns - 1_200_000_000_000);
    let allowed_20m_before = gov.passes_elastic_funding_cliff_guard(3, Side::Bid, ScaledBps(650), &snap_20m_before);
    let v3 = SignalVerdict::AuthorizeMakerEntry { side: Side::Bid, target_excursion_scaled_bps: ScaledBps(650) };
    assert!(engines3[3].try_arm_maker_entry(&snap_20m_before, v3, LotQty(26)).is_ok());
    assert!(engines3[3].on_maker_entry_filled(TimestampNs(cut_ns - 1_200_000_000_000)).is_ok());
    engines3[3].latch_defensive_floor_if_needed(true);
    snaps3[3].ts_local_ns = TimestampNs(cut_ns + 5_000_000_000);
    snaps3[3].factor_ofi = BoundedScore::new_clamped(-0.40);
    snaps3[3].factor_tfi = BoundedScore::new_clamped(-0.50);
    let mut f_cuts = [0_i64; MAX_ACTIVE_PAIRS];
    let mut ledger3 = ParitySyncLedger::new(LIVE_WALLET_1E8);
    let _ = settle_live_8h_funding_cuts_v48(&engines3, &snaps3, &mut f_cuts, &mut ledger3, None, None);
    let endo_exit_px = engines3[3].compute_canonical_exit_quote(&snaps3[3]).map_or(PriceTicks(7_141), |q| q.target_price);
    let _ = engines3[3].finalize_close_with_lots(ClosedTradeKind::MakerPassiveUnwind, Side::Bid, PriceTicks(7_138), endo_exit_px, LotQty(26), 1, None, TimestampNs(cut_ns + 10_000_000_000), &mut ledger3);
    let eq_delta_v3 = ledger3.current_equity_1e8() - LIVE_WALLET_1E8;
    println!("  [V03 ELASTIC_FUNDING_CLIFF] 5sPreVeto={} | 20mPreOk={} | EqDelta={:+.4}$ -> SEALED", blocked_5s_before, allowed_20m_before, (eq_delta_v3 as f64) / 1e8);
    assert!(blocked_5s_before && allowed_20m_before && eq_delta_v3 >= 0);

    // V4: Multi-Slot Simultaneous Shock vs Daily Loss Cap (-0.39%)
    let eth_ppm = APPROVED_SPECS[1].all_in_sl_loss_ppm_with_fees(PriceTicks(180_249), ScaledBps(3_200), fees);
    let shock_2slot = ((APPROVED_SPECS[1].lot_notional_1e8(LotQty(1), PriceTicks(180_249)) as f64) / 1e8) * (eth_ppm as f64) * 1e-6 * 1.85;
    assert!(shock_2slot < 64.9960 * 0.003883);
    println!("  [V04 MULTI_SLOT_DAILY_CAP ] 2-Slot Shock=-{:.4}$ < DailyStop=-0.2524$ -> SEALED", shock_2slot);

    // V5: Nyquist Lattice Self-Learning Gate on Sub-Dollar Coarse Tick (WIF @ $0.1339)
    let wif_now = mk_snap(8, 1_339, 8_200, 0.78, 0.90, 0.85, t0);
    let wif_blocked_680 = gov.authorizes_adaptive_entry_v49(8, Side::Bid, LIVE_WALLET_1E8, 1_000, ScaledBps(680), &wif_now, &wif_now, 0.0, 1.0).is_err();
    let wif_unlocked_learned_950 = gov.authorizes_adaptive_entry_v49(8, Side::Bid, LIVE_WALLET_1E8, 1_000, ScaledBps(950), &wif_now, &wif_now, 0.0, 1.0).is_ok();
    let ordi_now = mk_snap(6, 2_760, 8_200, 0.78, 0.90, 0.85, t0);
    let ordi_ok = gov.authorizes_adaptive_entry_v49(6, Side::Bid, LIVE_WALLET_1E8, 1_000, ScaledBps(680), &ordi_now, &ordi_now, 0.0, 1.0).is_ok();
    println!("  [V05 NYQUIST_TICK_LATTICE ] WIF@6.8bp Blocked={} | WIF@9.5bp LearnedOk={} | ORDI Ok={} -> SEALED", wif_blocked_680, wif_unlocked_learned_950, ordi_ok);
    assert!(wif_blocked_680 && wif_unlocked_learned_950 && ordi_ok);

    // V6: Queue GEOM-Flip Whipsaw at +170ms on 200Hz Daemon Tick
    let shm_p = MappedShmRegion::ephemeral_self_test_path("v501_hunter_v6");
    if let Ok(reg) = MappedShmRegion::open_or_create(&shm_p, true) {
        let (tg_tx, _) = mpsc::sync_channel::<String>(8);
        let (v_tx, _) = mpsc::sync_channel::<VaultIoCommand>(16);
        let mut rt = ExecRuntimeState::new();
        rt.wallet_1e8 = LIVE_WALLET_1E8;
        let mut rest_ns = 0_i64;
        let mut snaps = [SymbolLobSnapshot::new_empty(); MAX_ACTIVE_PAIRS];
        for i in 0..MAX_ACTIVE_PAIRS { snaps[i] = mk_snap(i, LIVE_TICKS_NOV2026[i], 3_000, 0.0, 0.0, 0.0, t0); }
        snaps[4] = mk_snap(4, 19_511, 3_200, -0.72, -0.92, -0.64, t0);
        let mut foren = [SlotForensicEntrySnapshot::default(); MAX_ACTIVE_PAIRS];
        let mut engs = SlotExecutionEngine::new_12_slot_array();
        let mut g6 = OnlineAdaptiveGovernor::new();
        let mut led6 = ParitySyncLedger::new(LIVE_WALLET_1E8);
        let inst = InstrumentSpecVerification::baseline_from_specs(&APPROVED_SPECS);
        let _ = step_12_slot_daemon_tick(TimestampNs(t0), None, inst, None, &mut rt, &mut rest_ns, &snaps, &mut foren, &mut engs, &mut g6, &mut led6, reg.arena(), &tg_tx, &v_tx);
        snaps[4].ts_local_ns = TimestampNs(t0 + 100_000_000);
        let _ = step_12_slot_daemon_tick(TimestampNs(t0 + 100_000_000), None, inst, None, &mut rt, &mut rest_ns, &snaps, &mut foren, &mut engs, &mut g6, &mut led6, reg.arena(), &tg_tx, &v_tx);
        snaps[4].ts_local_ns = TimestampNs(t0 + 270_000_000);
        snaps[4].factor_geom = BoundedScore::new_clamped(0.18);
        let _ = step_12_slot_daemon_tick(TimestampNs(t0 + 270_000_000), None, inst, None, &mut rt, &mut rest_ns, &snaps, &mut foren, &mut engs, &mut g6, &mut led6, reg.arena(), &tg_tx, &v_tx);
        assert!(matches!(engs[4].state, OrderLifecycleState::Idle));
        println!("  [V06 QUEUE_GEOM_FLIP_170MS] Aborted cleanly before adverse fill -> SEALED");
        let _ = std::fs::remove_file(&shm_p);
    }

    // V7 (WAVE 2): Whale L1 Spoof Rug-Pull (8,000 -> 40 lots + OFI flip)
    let rem_q = clamp_resting_queue_ahead_physical(LotQty(7_900), LotQty(40), LotQty(50), LotQty(8_000));
    let mut rug_snap = mk_snap(3, 7_138, 3_200, -0.45, -0.60, -0.50, t0);
    rug_snap.bids[0] = BookLevel { price: PriceTicks(7_138), qty: LotQty(40) };
    let rug_abort = should_abort_collapsed_resting_l1_wall(APPROVED_SPECS[3], Side::Bid, PriceTicks(7_138), &rug_snap, LotQty(8_000));
    println!("  [V07 WHALE_L1_RUG_PULL    ] ClampedQ={}L | WallCollapseAbort={} -> SEALED", rem_q.0, rug_abort);
    assert!(rem_q.0 <= 40 && rug_abort);

    // V8 (WAVE 2): Post-Abort Revenge Falling-Knife Catch within 8s
    let mut g8 = OnlineAdaptiveGovernor::new();
    g8.observe_entry_abort(3, Side::Bid, PriceTicks(7_138), TimestampNs(t0));
    let knife_snap = mk_snap(3, 7_120, 3_600, 0.75, 0.88, 0.70, t0 + 8_000_000_000);
    let knife_vetoed = g8.authorizes_adaptive_entry_v49(3, Side::Bid, LIVE_WALLET_1E8, 1_000, ScaledBps(680), &knife_snap, &knife_snap, 0.0, 1.0).is_err();
    println!("  [V08 POST_ABORT_KNIFE_TRAP] Falling-knife re-entry at 7120t vetoed={} -> SEALED", knife_vetoed);
    assert!(knife_vetoed);

    // V9 (WAVE 2): Self-Learning Plasticity Under 3 Zero-MFE Stop-Losses (p_Taker rises, MFE preserved, Quarantine armed)
    let mut g9 = OnlineAdaptiveGovernor::new();
    let p0 = g9.slot_taker_prob_permille(2, Side::Ask);
    let mut eng9 = SlotExecutionEngine::new(2);
    eng9.learned_mfe_bps = 710;
    let mut led9 = ParitySyncLedger::new(LIVE_WALLET_1E8);
    for k in 0..3_i64 {
        let ts_k = TimestampNs(t0 + k * 600_000_000_000);
        eng9.last_closed_mfe_bps = 120; // sub-friction noise (< 4.00bp)
        let s_sl = eng9.finalize_close_with_lots(ClosedTradeKind::ServerMarkPriceStopLoss, Side::Ask, PriceTicks(7_437), PriceTicks(7_468), LotQty(4), 1, None, ts_k, &mut led9);
        g9.observe_closed_trade_at_entry(2, Side::Ask, PriceTicks(7_437), s_sl.net_scaled_bps, false, ts_k);
    }
    let p3 = g9.slot_taker_prob_permille(2, Side::Ask);
    let sol_quarantined = g9.audit_historical_entry_veto(2, Side::Ask, 7_437, t0 + 1_210_000_000_000).is_some();
    println!("  [V09 BAYESIAN_3SL_PLASTIC ] p_Taker {}pm->{}pm | MFE={} (Unshrunk) | Quarantine={} -> SEALED", p0, p3, eng9.learned_mfe_bps, sol_quarantined);
    assert!(p3 > p0 * 2 && eng9.learned_mfe_bps == 710 && sol_quarantined);

    // V10 (WAVE 2): Inverse Epps Correlation Decoupling Shock (BTC pumps while LINK decouples negatively)
    let mut trk10 = EppsBtcCorrelationTracker::new();
    for k in 0..=40_i64 {
        let up = (k % 2) == 1;
        trk10.observe_5s_pair(TimestampNs(t0 + k * 5_000_000_000), false, PriceTicks(if up { 9_120 } else { 9_155 }), PriceTicks(if up { 644_200 } else { 642_845 }));
    }
    let btc_pump = mk_snap(0, 644_200, 3_500, 0.85, 0.90, 0.80, t0 + 210_000_000_000);
    let link_weak = mk_snap(9, 9_130, 3_500, 0.52, 0.55, 0.45, t0 + 210_000_000_000);
    let inv_beta = OrthogonalFactorState::epps_correlated_lead_lag_beta(9, &link_weak, &btc_pump, &trk10);
    let decoupled_veto = gov.authorizes_adaptive_entry_v49(9, Side::Bid, LIVE_WALLET_1E8, 1_000, ScaledBps(650), &link_weak, &btc_pump, 0.0, inv_beta).is_err();
    println!("  [V10 EPPS_INVERSE_DECOUPLE] LearnedBeta={:+.2} | BullBTC+InvAlt Veto={} -> SEALED", inv_beta, decoupled_veto);
    assert!(inv_beta < -0.45 && decoupled_veto);

    // V11 (WAVE 2): Mark-Mid Dislocation Wick Anti-10001 Guard on Live SL Ratchet
    let mut wick_snap = mk_snap(1, 180_249, 4_200, -0.50, -0.60, -0.40, t0);
    wick_snap.last_trade_price = PriceTicks(180_220);
    wick_snap.mark_price = PriceTicks(180_190); // 30-tick flash basis gap
    let safe_sl = select_safe_live_sl_ticks_with_fees(Side::Bid, PriceTicks(180_249), PriceTicks(179_200), PriceTicks(180_210), &wick_snap, fees);
    let anti_10001_ok = safe_sl.is_some_and(|px| px.0 <= 180_187 && px.0 > 179_200);
    println!("  [V11 FLASH_BASIS_ANTI10001] SafeSL={:?} (<= Mark-3t & > PrevSL) -> SEALED", safe_sl);
    assert!(anti_10001_ok);

    // V12 (WAVE 2): Live Market-Maker Negative Rebate Elasticity (-0.50bp Maker / 3.00bp Taker)
    let mut g_reb = OnlineAdaptiveGovernor::new();
    let mm_rebate = FeeSchedule::from_live_rates_clamped(ScaledBps(-50), ScaledBps(300));
    g_reb.set_active_fee_schedule(mm_rebate);
    let avax_reb = mk_snap(3, 7_138, 2_400, 0.62, 0.72, 0.55, t0);
    let reb_ok = g_reb.authorizes_adaptive_entry_v49(3, Side::Bid, LIVE_WALLET_1E8, 1_000, ScaledBps(480), &avax_reb, &avax_reb, 0.0, 1.0).is_ok();
    println!("  [V12 VIP_NEGATIVE_REBATE  ] -0.50bp Rebate Elastic Admission={} -> SEALED", reb_ok);
    assert!(reb_ok);

    println!("==========================================================================================\n");
}

#[test]
fn test_v501_wave3_deep_systemic_and_500_episode_chaotic_monte_carlo() {
    use vortex_rs::exec_daemon::{
        passes_elastic_daily_tail_risk_budget, should_trigger_live_emergency_flatten,
        slot_position_risk_ppm_with_fees,
    };
    use vortex_rs::exec_gateway_handlers::apply_post_fill_sl_or_breach;
    use vortex_rs::governor_plasticity::DailySessionGovernor;

    let fees = FeeSchedule::VIP0_DEFAULT;
    let t0 = 1_794_972_000_000_000_000_i64;
    let ring = SlotMarkoutRing::new();

    println!("\n==========================================================================================");
    println!("[VORTEX-OMEGA V5.0.1 WAVE-3 SYSTEMIC & 500-EPISODE CHAOTIC MONTE CARLO SUITE]");
    println!("==========================================================================================");

    // V13: P0-A Live BreachEmergencyFlatten when verified_exchange_sl > 0
    let mut rt13 = ExecRuntimeState::new();
    rt13.verified_exchange_sl[4] = PriceTicks(19_300); // Pre-set from EntryPlaced
    let mut eng13 = SlotExecutionEngine::new(4);
    let mut crash_snap = mk_snap(4, 19_000, 2_500, -0.80, -0.95, -0.85, t0);
    crash_snap.mark_price = PriceTicks(19_000);
    crash_snap.last_trade_price = PriceTicks(19_000);
    let _ = eng13.adopt_live_open_position(Side::Bid, PriceTicks(19_511), LotQty(6), LotQty(6), false, PriceTicks(19_511), PriceTicks(19_000), TimestampNs(t0), &crash_snap);
    apply_post_fill_sl_or_breach(4, Side::Bid, PriceTicks(19_511), true, PriceTicks(19_300), &crash_snap, &mut eng13, &mut rt13);
    let (trig_p0a, b_sl) = should_trigger_live_emergency_flatten(&rt13, 4, Side::Bid, eng13.last_eff_sl_ticks, &crash_snap);
    println!("  [V13 P0A_LIVE_RACE_FLATTEN] VerifiedSL=19300t + Breach -> FlattenNow={} (BackupSL={}t) -> SEALED", trig_p0a, b_sl.0);
    assert!(trig_p0a && b_sl.0 > 0);

    // V14: Elastic Daily Tail-Risk Budget + Chandelier Risk-Recycling + 30% Sinking Reserve Expansion
    let gov14 = OnlineAdaptiveGovernor::new();
    let mut dg14 = DailySessionGovernor::new(LIVE_WALLET_1E8);
    let _ = dg14.sync_utc_day(TimestampNs(t0), LIVE_WALLET_1E8);
    dg14.day_realized_pnl_1e8 = -20_800_000; // -0.320% realized today (only 0.068% left before -0.388% stop)
    let snaps14 = [mk_snap(1, 180_249, 3_200, 0.75, 0.90, 0.92, t0); MAX_ACTIVE_PAIRS];
    let mut engs14 = SlotExecutionEngine::new_12_slot_array();
    let eth_risk_ppm = slot_position_risk_ppm_with_fees(APPROVED_SPECS[1], LIVE_WALLET_1E8, PriceTicks(180_249), LotQty(1), ScaledBps(3_200), fees);
    let blocked_near_cap = !passes_elastic_daily_tail_risk_budget(1, Side::Bid, eth_risk_ppm, LIVE_WALLET_1E8, &dg14, 0, &engs14, &snaps14, &gov14);
    let unlocked_by_sinking_30 = passes_elastic_daily_tail_risk_budget(1, Side::Bid, eth_risk_ppm, LIVE_WALLET_1E8, &dg14, 2_500, &engs14, &snaps14, &gov14);
    dg14.day_realized_pnl_1e8 = -9_500_000; // -0.146%
    let _ = engs14[3].adopt_live_open_position(Side::Bid, PriceTicks(7_138), LotQty(26), LotQty(26), false, PriceTicks(7_138), PriceTicks(7_138), TimestampNs(t0), &mk_snap(3, 7_138, 3_200, 0.7, 0.8, 0.7, t0));
    let _ = engs14[5].adopt_live_open_position(Side::Bid, PriceTicks(7_255), LotQty(24), LotQty(24), false, PriceTicks(7_255), PriceTicks(7_255), TimestampNs(t0), &mk_snap(5, 7_255, 3_200, 0.7, 0.8, 0.7, t0));
    let blocked_3rd_unhedged = !passes_elastic_daily_tail_risk_budget(1, Side::Bid, eth_risk_ppm, LIVE_WALLET_1E8, &dg14, 0, &engs14, &snaps14, &gov14);
    engs14[3].chandelier_profit_sl_armed = true;
    engs14[5].chandelier_profit_sl_armed = true;
    let recycled_by_chandelier = passes_elastic_daily_tail_risk_budget(1, Side::Bid, eth_risk_ppm, LIVE_WALLET_1E8, &dg14, 0, &engs14, &snaps14, &gov14);
    println!("  [V14 DAILY_TAIL_RECYCLING ] NearCapVeto={} | SinkingUnlock={} | 3rdVeto={} | ChandRecycle={} -> SEALED", blocked_near_cap, unlocked_by_sinking_30, blocked_3rd_unhedged, recycled_by_chandelier);
    assert!(blocked_near_cap && unlocked_by_sinking_30 && blocked_3rd_unhedged && recycled_by_chandelier);

    // V15: Preflight & Re-Peg Hollow-L1 Evaporation Guard (1,500 lots -> 1 lot = $7.13)
    let mut hollow_avax = mk_snap(3, 7_138, 3_600, 0.75, 0.90, 0.65, t0);
    hollow_avax.bids[0] = BookLevel { price: PriceTicks(7_138), qty: LotQty(10) }; // 1.0 AVAX = $7.13
    let preflight_hollow_vetoed = gov14.confirm_preflight_kinematics_v49(3, Side::Bid, LIVE_WALLET_1E8, LotQty(26), ScaledBps(680), &hollow_avax, &hollow_avax, 0.0, 1.0).is_err();
    hollow_avax.bids[0].qty = LotQty(80); // 8.0 AVAX = $57.10
    let preflight_deep_ok = gov14.confirm_preflight_kinematics_v49(3, Side::Bid, LIVE_WALLET_1E8, LotQty(26), ScaledBps(680), &hollow_avax, &hollow_avax, 0.0, 1.0).is_ok();
    println!("  [V15 PREFLIGHT_HOLLOW_L1  ] $7.13 L1 Vetoed={} | $57.10 L1 Ok={} -> SEALED", preflight_hollow_vetoed, preflight_deep_ok);
    assert!(preflight_hollow_vetoed && preflight_deep_ok);

    // V16: Post-8h-Cut Funding Rate Reset Amnesia Immunity
    let cut_ns = 62_327_i64 * 28_800 * 1_000_000_000;
    let mut eng16 = SlotExecutionEngine::new(3);
    let mut s16 = mk_snap(3, 7_138, 3_500, 0.70, 0.85, 0.65, cut_ns - 2_000_000_000);
    s16.funding_rate_scaled_bps = ScaledBps(320); // +3.20bp adverse before cut
    let v16 = SignalVerdict::AuthorizeMakerEntry { side: Side::Bid, target_excursion_scaled_bps: ScaledBps(650) };
    assert!(eng16.try_arm_maker_entry(&s16, v16, LotQty(26)).is_ok());
    assert!(eng16.on_maker_entry_filled(TimestampNs(cut_ns - 2_000_000_000)).is_ok());
    eng16.observe_open_flow_kinematics(&s16);
    eng16.latch_defensive_floor_if_needed(true);
    // 10ms AFTER cut, Bybit resets funding_rate to -1.50bp! Engine must STILL remember +3.20bp paid!
    s16.ts_local_ns = TimestampNs(cut_ns + 10_000_000);
    s16.funding_rate_scaled_bps = ScaledBps(-150);
    eng16.observe_open_flow_kinematics(&s16);
    let q16_px = eng16.compute_canonical_exit_quote(&s16).map_or(0, |q| q.target_price.0);
    println!("  [V16 POST_CUT_RATE_RESET  ] AccruedAdv={}sbps | ElevatedFloor={}t (>7141t) -> SEALED", eng16.accrued_adverse_funding_sbps, q16_px);
    assert!(eng16.accrued_adverse_funding_sbps == 320 && q16_px >= 7_143);

    // V17: Post-Tranche-1 Harvest Runner Reversal Protection & 2-Leg Consolidation Parity
    let mut eng17 = SlotExecutionEngine::new(3);
    eng17.learned_mfe_bps = 680;
    let mut s17 = mk_snap(3, 7_138, 3_200, 0.75, 0.90, 0.70, t0);
    assert!(eng17.try_arm_maker_entry(&s17, v16, LotQty(26)).is_ok());
    assert!(eng17.on_maker_entry_filled(TimestampNs(t0)).is_ok());
    assert!(eng17.record_tranche1_harvest(PriceTicks(7_143)));
    let mut led17 = ParitySyncLedger::new(LIVE_WALLET_1E8);
    let mut closed17 = None;
    for sec in 1..=60_i64 {
        let p = 7_138 - sec;
        s17.ts_local_ns = TimestampNs(t0 + sec * 1_000_000_000);
        s17.bids[0].price = PriceTicks(p);
        s17.asks[0].price = PriceTicks(p + 1);
        s17.mark_price = PriceTicks(p);
        s17.last_trade_price = PriceTicks(p);
        s17.factor_ofi = BoundedScore::new_clamped(-0.85);
        s17.factor_tfi = BoundedScore::new_clamped(-0.95);
        if let Some(c) = eng17.step_open_position_v49(&s17, &s17, &gov14, &mut led17, 1.0, &ring, 1.0) {
            closed17 = Some(c);
            break;
        }
    }
    let (c17_legs, c17_pnl, c17_net) = closed17.map_or((0, -999_999, -9_999), |c| (c.legs_consolidated, c.net_pnl_1e8, c.net_scaled_bps.raw()));
    assert_eq!(led17.current_equity_1e8() - LIVE_WALLET_1E8, c17_pnl);
    println!("  [V17 TRANCHE1_RUNNER_GUARD] ConsolidatedLegs={} | Net={:+.2}bp (vs -73bp raw) | Drift=0 -> SEALED", c17_legs, (c17_net as f64) / 100.0);
    assert!(c17_legs == 2 && c17_net > -2_600);

    // V18: Law III Anti-Smoothing-by-Silence (4 hours of quiet ticks NEVER erases scar debt)
    let mut g18 = OnlineAdaptiveGovernor::new();
    g18.observe_closed_trade_at_entry(4, Side::Ask, PriceTicks(19_511), ScaledBps(-4_200), false, TimestampNs(t0));
    g18.observe_closed_trade_at_entry(4, Side::Ask, PriceTicks(19_511), ScaledBps(-4_200), false, TimestampNs(t0 + 600_000_000_000));
    let mut quiet_tao = mk_snap(4, 19_511, 1_500, 0.02, -0.01, 0.0, t0 + 600_000_000_000);
    for m in 1..=240_i64 {
        quiet_tao.ts_local_ns = TimestampNs(t0 + (600 + m * 60) * 1_000_000_000);
        g18.observe_market_price(4, &quiet_tao);
    }
    let (_, sl_after_4h_silence, _) = g18.slot_direction_diagnostics(4, Side::Ask);
    println!("  [V18 LAW3_ANTI_SILENCE    ] SL_Streak after 4h quiet ticks={} (==2, zero decay) -> SEALED", sl_after_4h_silence);
    assert_eq!(sl_after_4h_silence, 2);

    // V19: Corrupt / Crossed BBO & Zero-Mark-Price Flash Anomaly Immunity
    let mut bad_snap = mk_snap(1, 180_249, 4_000, 0.90, 0.95, 0.90, t0);
    bad_snap.bids[0].price = PriceTicks(180_260);
    bad_snap.asks[0].price = PriceTicks(180_250); // Crossed book!
    let crossed_verdict = OrthogonalFactorState::evaluate_entry_signal_v46(APPROVED_SPECS[1], &bad_snap, &bad_snap, ScaledBps(680), ScaledBps(150), 50, fees, 0.0, 1.0);
    let mut eng19 = SlotExecutionEngine::new(1);
    let s19_ok = mk_snap(1, 180_249, 3_000, 0.70, 0.85, 0.90, t0);
    assert!(eng19.try_arm_maker_entry(&s19_ok, SignalVerdict::AuthorizeMakerEntry { side: Side::Bid, target_excursion_scaled_bps: ScaledBps(680) }, LotQty(1)).is_ok());
    assert!(eng19.on_maker_entry_filled(TimestampNs(t0)).is_ok());
    let mut zero_mark = s19_ok;
    zero_mark.ts_local_ns = TimestampNs(t0 + 5_000_000_000);
    zero_mark.mark_price = PriceTicks(0); // Missing mark_price anomaly must NOT trigger Bid SL!
    let mut led19 = ParitySyncLedger::new(LIVE_WALLET_1E8);
    let false_sl = eng19.step_open_position_v49(&zero_mark, &zero_mark, &gov14, &mut led19, 1.0, &ring, 1.0);
    println!("  [V19 CORRUPT_BOOK_IMMUNITY] CrossedVerdict={:?} | ZeroMarkFalseSL={} -> SEALED", crossed_verdict, false_sl.is_some());
    assert!(matches!(crossed_verdict, SignalVerdict::NeutralNoAction) && false_sl.is_none());

    // V20: 500-Episode Chaotic Multi-Regime Monte Carlo Stress Lab (Drift == 0, Monotonic 70% Vault, PF >= 1.50)
    let mut seed: u64 = 0x5F37_59DF_2026_0501;
    let mut next_u64 = || -> u64 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    let mut mc_gov = OnlineAdaptiveGovernor::new();
    let mut mc_engs = SlotExecutionEngine::new_12_slot_array();
    let mut mc_ledger = ParitySyncLedger::new(LIVE_WALLET_1E8);
    let (mut mc_trades, mut mc_wins, mut mc_losses) = (0_u32, 0_u32, 0_u32);
    let (mut gross_win_1e8, mut gross_loss_1e8, mut sum_pnl_1e8, mut peak_vault_70) = (0_i64, 0_i64, 0_i64, 0_i64);
    let mut ts_mc = t0;

    for _ep in 0..500_u32 {
        ts_mc += 95_000_000_000;
        let slot = ((next_u64() % 11) + 1) as usize;
        let base_px = LIVE_TICKS_NOV2026[slot];
        let side = if (next_u64() & 1) == 0 { Side::Bid } else { Side::Ask };
        let sgn = side.sign_f64();
        let roll = next_u64() % 20;
        let is_trap = roll < 5;
        let is_ambush = roll == 5;
        let geom = sgn * if is_trap { 0.08 + ((next_u64() % 20) as f64) * 0.01 } else { 0.62 + ((next_u64() % 35) as f64) * 0.01 };
        let ofi = sgn * (0.58 + ((next_u64() % 35) as f64) * 0.01);
        let tfi = sgn * (0.72 + ((next_u64() % 25) as f64) * 0.01);
        let vol = 3_400 + ((next_u64() % 3_600) as i64);
        let mut s_ep = mk_snap(slot, base_px, vol, ofi, tfi, geom, ts_mc);
        if is_trap && next_u64().trailing_zeros() >= 2 {
            s_ep.bids[0].qty = LotQty(1);
            s_ep.asks[0].qty = LotQty(1);
        }
        let mfe_s = ScaledBps(mc_engs[slot].learned_mfe_bps);
        let Ok(lots) = mc_gov.authorizes_adaptive_entry_v49(slot, side, mc_ledger.current_equity_1e8(), 1_000, mfe_s, &s_ep, &s_ep, 0.0, 1.0) else {
            continue;
        };
        if mc_gov.confirm_preflight_kinematics_v49(slot, side, mc_ledger.current_equity_1e8(), lots, mfe_s, &s_ep, &s_ep, 0.0, 1.0).is_err() {
            continue;
        }
        mc_engs[slot].sync_governor_for_side(&mc_gov, side);
        let (dir_edge, _, _) = mc_gov.slot_direction_diagnostics(slot, side);
        let tkr_pm = mc_gov.slot_taker_prob_permille(slot, side);
        let verd = OrthogonalFactorState::evaluate_entry_signal_v46(APPROVED_SPECS[slot], &s_ep, &s_ep, mfe_s, ScaledBps(dir_edge), tkr_pm, fees, 0.0, 1.0);
        if !matches!(verd, SignalVerdict::AuthorizeMakerEntry { side: s, .. } if s == side) { continue; }
        let mut ep_ring = SlotMarkoutRing::new();
        let en_px = side.select_entry_bbo_price(s_ep.bids[0].price, s_ep.asks[0].price);
        ep_ring.arm_fill_probe(side, en_px, PriceTicks(base_px), TimestampNs(ts_mc));
        if mc_engs[slot].try_arm_maker_entry(&s_ep, verd, lots).is_err() || mc_engs[slot].on_maker_entry_filled(TimestampNs(ts_mc)).is_err() {
            mc_engs[slot].state = OrderLifecycleState::Idle;
            continue;
        }
        for sec in 1..=95_i64 {
            let step_dir = if (is_trap || is_ambush) && sec >= 4 { -side.sign_i64() } else { side.sign_i64() };
            let rate_sbps = if step_dir < 0 { 55 } else { 115 };
            let delta_t = ((base_px * rate_sbps * sec) / 1_000_000).max(1) * step_dir;
            let cur_p = (base_px + delta_t).max(2);
            s_ep.ts_local_ns = TimestampNs(ts_mc + sec * 1_000_000_000);
            s_ep.bids[0].price = PriceTicks(cur_p);
            s_ep.asks[0].price = PriceTicks(cur_p + 1);
            s_ep.mark_price = PriceTicks(cur_p);
            s_ep.last_trade_price = PriceTicks(cur_p);
            s_ep.factor_ofi = BoundedScore::new_clamped((step_dir as f64) * 0.80);
            s_ep.factor_tfi = BoundedScore::new_clamped((step_dir as f64) * 0.92);
            ep_ring.step_clock(PriceTicks(cur_p), s_ep.ts_local_ns, 200);
            if let Some(sum) = mc_engs[slot].step_open_position_v49(&s_ep, &s_ep, &mc_gov, &mut mc_ledger, 1.0, &ep_ring, 1.0) {
                mc_trades += 1;
                sum_pnl_1e8 += sum.net_pnl_1e8;
                if sum.net_pnl_1e8 >= 0 { mc_wins += 1; gross_win_1e8 += sum.net_pnl_1e8; }
                else { mc_losses += 1; gross_loss_1e8 += sum.net_pnl_1e8.abs(); }
                assert!(mc_ledger.vault_locked_profit_bps_x100 >= peak_vault_70);
                peak_vault_70 = mc_ledger.vault_locked_profit_bps_x100;
                mc_gov.observe_closed_trade_at_entry(slot, sum.side, sum.entry_price, sum.net_scaled_bps, sum.is_maker_exit, s_ep.ts_local_ns);
                break;
            }
        }
        mc_engs[slot].state = OrderLifecycleState::Idle;
    }
    let mc_drift = (mc_ledger.cumulative_net_pnl_1e8 - sum_pnl_1e8).abs();
    let mc_pf = (gross_win_1e8 as f64) / (gross_loss_1e8.max(1) as f64);
    println!(
        "  [V20 CHAOTIC_500_MONTECARL] Trades={} (W:{} L:{}) | PF={:.2} | Vault70={:+.2}bp | Drift={} -> SEALED",
        mc_trades, mc_wins, mc_losses, mc_pf, (peak_vault_70 as f64) / 100.0, mc_drift
    );
    assert!(mc_trades >= 40 && mc_pf >= 1.50 && mc_drift == 0 && peak_vault_70 > 0);
    println!("==========================================================================================\n");
}

#[test]
fn test_v501_wave4_win_wiper_catastrophic_tail_hunter() {
    use vortex_rs::exec_daemon::should_trigger_live_emergency_flatten;

    let gov = OnlineAdaptiveGovernor::new();
    let fees = FeeSchedule::VIP0_DEFAULT;
    let t0 = 1_794_980_000_000_000_000_i64;
    let ring = SlotMarkoutRing::new();

    println!("\n==========================================================================================");
    println!("[VORTEX-OMEGA V5.0.1 WAVE-4 'WIN-WIPER' FAT-TAIL LOSS HUNTER (V21..V25)]");
    println!("==========================================================================================");

    // V21: Deceptive Passive Iceberg Absorption (OFI=+0.30, TFI=+0.25 POSITIVE while price bleeds)
    // Plus Counter-Check: AVAX #5 real dip (-12.39bp) MUST NOT be prematurely cut!
    let mut s_avax5 = mk_snap(3, 7_263, 2_206, 0.81, 0.98, 0.45, t0);
    let mut eng_avax5 = SlotExecutionEngine::new(3);
    eng_avax5.learned_mfe_bps = 628;
    assert!(eng_avax5.try_arm_maker_entry(&s_avax5, SignalVerdict::AuthorizeMakerEntry { side: Side::Bid, target_excursion_scaled_bps: ScaledBps(628) }, LotQty(18)).is_ok());
    assert!(eng_avax5.on_maker_entry_filled(TimestampNs(t0)).is_ok());
    let mut led_avax5 = ParitySyncLedger::new(LIVE_WALLET_1E8);
    s_avax5.ts_local_ns = TimestampNs(t0 + 18_000_000_000);
    s_avax5.bids[0].price = PriceTicks(7_254); // -12.39 bps dip (exact Trade #5 worst_ticks)
    s_avax5.asks[0].price = PriceTicks(7_255);
    s_avax5.mark_price = PriceTicks(7_254);
    s_avax5.last_trade_price = PriceTicks(7_254);
    let survived_dip = eng_avax5.step_open_position_v49(&s_avax5, &s_avax5, &gov, &mut led_avax5, 1.0, &ring, 1.0).is_none();

    let mut s_ice = mk_snap(1, 180_249, 2_600, 0.30, 0.25, 0.70, t0);
    let mut eng_ice = SlotExecutionEngine::new(1);
    eng_ice.learned_mfe_bps = 670;
    assert!(eng_ice.try_arm_maker_entry(&s_ice, SignalVerdict::AuthorizeMakerEntry { side: Side::Bid, target_excursion_scaled_bps: ScaledBps(670) }, LotQty(1)).is_ok());
    assert!(eng_ice.on_maker_entry_filled(TimestampNs(t0)).is_ok());
    let ice_init_sl = 180_249 - eng_ice.last_eff_sl_ticks.0;
    let mut led_ice = ParitySyncLedger::new(LIVE_WALLET_1E8);
    let mut ice_closed_net = -9_999_i64;
    for sec in 1..=60_i64 {
        let px = 180_249 - sec * 14; // Steady iceberg bleed while OFI & TFI stay deceptive (+0.30 / +0.25)!
        s_ice.ts_local_ns = TimestampNs(t0 + sec * 1_000_000_000);
        s_ice.bids[0].price = PriceTicks(px);
        s_ice.asks[0].price = PriceTicks(px + 1);
        s_ice.mark_price = PriceTicks(px);
        s_ice.last_trade_price = PriceTicks(px);
        s_ice.factor_ofi = BoundedScore::new_clamped(0.30);
        s_ice.factor_tfi = BoundedScore::new_clamped(0.25);
        if let Some(sum) = eng_ice.step_open_position_v49(&s_ice, &s_ice, &gov, &mut led_ice, 1.0, &ring, 1.0) {
            ice_closed_net = sum.net_scaled_bps.raw();
            break;
        }
    }
    println!(
        "  [V21 ICEBERG_FLOW_DIVERG  ] AVAX#5DipSurvived={} | InitSL={}t -> ClosedNet={:+.2}bp (saved >35bp) -> SEALED",
        survived_dip, ice_init_sl, (ice_closed_net as f64) / 100.0
    );
    assert!(survived_dip && ice_closed_net > -3_000);

    // V22: Crash-Volatility SL Inflation Immunity (open_vol=18bp -> crash vol explodes 5x to 92bp)
    let mut s_v22 = mk_snap(4, 19_511, 1_800, -0.75, -0.92, -0.65, t0);
    let mut eng22 = SlotExecutionEngine::new(4);
    eng22.learned_mfe_bps = 640;
    assert!(eng22.try_arm_maker_entry(&s_v22, SignalVerdict::AuthorizeMakerEntry { side: Side::Ask, target_excursion_scaled_bps: ScaledBps(640) }, LotQty(6)).is_ok());
    assert!(eng22.on_maker_entry_filled(TimestampNs(t0)).is_ok());
    let init_tao_sl = eng22.last_eff_sl_ticks.0 - 19_511;
    let mut led22 = ParitySyncLedger::new(LIVE_WALLET_1E8);
    for sec in 1..=20_i64 {
        let p = 19_511 + sec;
        s_v22.ts_local_ns = TimestampNs(t0 + sec * 1_000_000_000);
        s_v22.realized_vol_5m = ScaledBps(9_200); // 5x volatility explosion during squeeze!
        s_v22.bids[0].price = PriceTicks(p);
        s_v22.asks[0].price = PriceTicks(p + 1);
        s_v22.mark_price = PriceTicks(p);
        s_v22.last_trade_price = PriceTicks(p);
        s_v22.factor_ofi = BoundedScore::new_clamped(0.85);
        s_v22.factor_tfi = BoundedScore::new_clamped(0.95);
        let _ = eng22.step_open_position_v49(&s_v22, &s_v22, &gov, &mut led22, 1.0, &ring, 1.0);
    }
    let comp_tao_sl = eng22.last_eff_sl_ticks.0 - 19_511;
    let v22_shrink = ((init_tao_sl - comp_tao_sl) * 100) / init_tao_sl.max(1);
    println!("  [V22 CRASH_VOL_SL_IMMUNITY] Vol 18->92bp | SL={}t -> {}t (Shrink={}%) -> SEALED", init_tao_sl, comp_tao_sl, v22_shrink);
    assert!(v22_shrink >= 55);

    // V23: Live Unsynced Chandelier / Ratchet Flash-Gap Guard (Prevents +9.5bp winner turning into -63bp loss)
    let mut rt23 = ExecRuntimeState::new();
    rt23.verified_exchange_sl[1] = PriceTicks(179_112); // Stale -63bp Server SL while RPC in flight
    let locked_chand_sl = PriceTicks(180_420); // +9.5bp Chandelier profit lock!
    let mut flash_gap = mk_snap(1, 180_340, 4_500, -0.85, -0.95, -0.80, t0 + 12_000_000_000);
    flash_gap.mark_price = PriceTicks(180_340);
    flash_gap.last_trade_price = PriceTicks(180_340);
    let (gap_flatten, target_sl) = should_trigger_live_emergency_flatten(&rt23, 1, Side::Bid, locked_chand_sl, &flash_gap);
    rt23.verified_exchange_sl[1] = locked_chand_sl; // Once Bybit ACKs 180,420t, no duplicate flatten!
    let (after_ack_flatten, _) = should_trigger_live_emergency_flatten(&rt23, 1, Side::Bid, locked_chand_sl, &flash_gap);
    println!("  [V23 LIVE_CHANDELIER_GAP  ] UnsyncedGapFlatten={} (@{}t) | PostAckNoDup={} -> SEALED", gap_flatten, target_sl.0, !after_ack_flatten);
    assert!(gap_flatten && target_sl == locked_chand_sl && !after_ack_flatten);

    // V24: Eye-of-the-Storm Volatility Squeeze Over-Leverage Guard (Self-Learned eq_vol = 95bp vs 18bp lull)
    let lull_sol = mk_snap(2, 7_437, 1_800, 0.75, 0.90, 0.70, t0);
    let mut calm_gov = OnlineAdaptiveGovernor::new();
    calm_gov.seed_prewarm_slot_emas(2, 74_370_000, 74_350_000, 2_500, t0);
    let mut storm_gov = OnlineAdaptiveGovernor::new();
    storm_gov.seed_prewarm_slot_emas(2, 74_370_000, 74_350_000, 9_500, t0);
    let lots_calm = calm_gov.compute_slot_entry_lots_v48(2, Side::Bid, 15_000_000_000, &lull_sol, 1_000);
    let lots_storm_eye = storm_gov.compute_slot_entry_lots_v48(2, Side::Bid, 15_000_000_000, &lull_sol, 1_000);
    println!("  [V24 STORM_EYE_DELEVERAGE ] CalmLots={}L -> StormEyeLots={}L (Self-learned eq_vol=95bp) -> SEALED", lots_calm.0, lots_storm_eye.0);
    assert!(lots_storm_eye.0 < lots_calm.0);

    // V25: 300-Episode 'Win-Wiper' Torture Gauntlet (Iceberg + 5x Vol Spike + Storm Eye + Reversal)
    let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut rng = || -> u64 { seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17; seed };
    let mut w_gov = OnlineAdaptiveGovernor::new();
    let mut w_engs = SlotExecutionEngine::new_12_slot_array();
    let mut w_led = ParitySyncLedger::new(LIVE_WALLET_1E8);
    let (mut wins_n, mut loss_n, mut sum_win_1e8, mut sum_loss_1e8, mut worst_loss_1e8) = (0_u32, 0_u32, 0_i64, 0_i64, 0_i64);
    let mut ts_w = t0;

    for _ep in 0..300_u32 {
        ts_w += 240_000_000_000;
        let slot = ((rng() % 11) + 1) as usize;
        if slot == 8 { continue; }
        let base_px = LIVE_TICKS_NOV2026[slot];
        let side = if (rng() & 1) == 0 { Side::Bid } else { Side::Ask };
        let sgn = side.sign_f64();
        let mode = (rng() % 10) as u32; // 0=IcebergBleed, 1=VolSpikeReversal, 2..9=Impulse+Crest
        let mut s_ep = mk_snap(slot, base_px, 3_600, sgn * 0.72, sgn * 0.88, sgn * 0.78, ts_w);
        let mfe_s = ScaledBps(w_engs[slot].learned_mfe_bps);
        let Ok(lots) = w_gov.authorizes_adaptive_entry_v49(slot, side, w_led.current_equity_1e8(), 1_000, mfe_s, &s_ep, &s_ep, 0.0, 1.0) else { continue };
        let (dir_e, _, _) = w_gov.slot_direction_diagnostics(slot, side);
        let verd = OrthogonalFactorState::evaluate_entry_signal_v46(APPROVED_SPECS[slot], &s_ep, &s_ep, mfe_s, ScaledBps(dir_e), w_gov.slot_taker_prob_permille(slot, side), fees, 0.0, 1.0);
        if !matches!(verd, SignalVerdict::AuthorizeMakerEntry { side: s, .. } if s == side) { continue; }
        w_engs[slot].sync_governor_for_side(&w_gov, side);
        if w_engs[slot].try_arm_maker_entry(&s_ep, verd, lots).is_err() || w_engs[slot].on_maker_entry_filled(TimestampNs(ts_w)).is_err() {
            w_engs[slot].state = OrderLifecycleState::Idle;
            continue;
        }
        let mut ep_ring = SlotMarkoutRing::new();
        let en_px = side.select_entry_bbo_price(s_ep.bids[0].price, s_ep.asks[0].price);
        ep_ring.arm_fill_probe(side, en_px, PriceTicks(base_px), TimestampNs(ts_w));

        for sec in 1..=90_i64 {
            let rate = if mode <= 1 { 65 } else { 125 };
            let eff_s = if mode >= 2 && sec > 32 { 32 } else { sec };
            let move_sgn = if mode <= 1 && sec >= 3 { -side.sign_i64() } else { side.sign_i64() };
            let cur_p = (base_px + ((base_px * rate * eff_s) / 1_000_000).max(1) * move_sgn).max(2);
            s_ep.ts_local_ns = TimestampNs(ts_w + sec * 1_000_000_000);
            if mode == 1 && sec >= 3 { s_ep.realized_vol_5m = ScaledBps(9_600); }
            s_ep.bids[0].price = PriceTicks(cur_p);
            s_ep.asks[0].price = PriceTicks(cur_p + 1);
            s_ep.mark_price = PriceTicks(cur_p);
            s_ep.last_trade_price = PriceTicks(cur_p);
            if mode == 0 {
                // Iceberg trap: flow pretends to stay positive while price bleeds!
                s_ep.factor_ofi = BoundedScore::new_clamped(sgn * 0.28);
                s_ep.factor_tfi = BoundedScore::new_clamped(sgn * 0.22);
            } else if mode >= 2 && sec >= 32 {
                s_ep.factor_ofi = BoundedScore::new_clamped(sgn * 0.03);
                s_ep.factor_tfi = BoundedScore::new_clamped(sgn * 0.02);
                s_ep.factor_geom = BoundedScore::ZERO;
            } else {
                s_ep.factor_ofi = BoundedScore::new_clamped((move_sgn as f64) * 0.80);
                s_ep.factor_tfi = BoundedScore::new_clamped((move_sgn as f64) * 0.92);
            }
            ep_ring.step_clock(PriceTicks(cur_p), s_ep.ts_local_ns, 200);
            if let Some(sum) = w_engs[slot].step_open_position_v49(&s_ep, &s_ep, &w_gov, &mut w_led, 1.0, &ep_ring, 1.0) {
                if sum.net_pnl_1e8 >= 0 {
                    wins_n += 1;
                    sum_win_1e8 += sum.net_pnl_1e8;
                } else {
                    loss_n += 1;
                    let l_abs = sum.net_pnl_1e8.abs();
                    sum_loss_1e8 += l_abs;
                    if l_abs > worst_loss_1e8 { worst_loss_1e8 = l_abs; }
                }
                w_gov.observe_closed_trade_at_entry(slot, sum.side, sum.entry_price, sum.net_scaled_bps, sum.is_maker_exit, s_ep.ts_local_ns);
                break;
            }
        }
        w_engs[slot].state = OrderLifecycleState::Idle;
    }
    let mean_win = (sum_win_1e8 as f64) / (f64::from(wins_n.max(1)));
    let mean_loss = (sum_loss_1e8 as f64) / (f64::from(loss_n.max(1)));
    let mean_wipeout_wins = mean_loss / mean_win.max(1.0);
    let worst_wipeout_wins = (worst_loss_1e8 as f64) / mean_win.max(1.0);
    let pf_v25 = (sum_win_1e8 as f64) / (sum_loss_1e8.max(1) as f64);
    println!(
        "  [V25 WIN_WIPER_RATIO_AUDIT] W:{} L:{} | MeanLoss/MeanWin={:.2}x (was 28.9x!) | WorstLoss/MeanWin={:.2}x | PF={:.2} -> SEALED",
        wins_n, loss_n, mean_wipeout_wins, worst_wipeout_wins, pf_v25
    );
    assert!(wins_n >= 30 && mean_wipeout_wins <= 4.00 && worst_wipeout_wins <= 8.00 && pf_v25 >= 1.60);

    // V26: BTC Leader Flash-Crash Contagion Pre-Compression (Alt L2 lags by 2s while BTC dumps)
    let mut s_sol26 = mk_snap(2, 7_437, 3_200, 0.72, 0.88, 0.70, t0);
    let mut eng26 = SlotExecutionEngine::new(2);
    eng26.learned_mfe_bps = 660;
    assert!(eng26.try_arm_maker_entry(&s_sol26, SignalVerdict::AuthorizeMakerEntry { side: Side::Bid, target_excursion_scaled_bps: ScaledBps(660) }, LotQty(4)).is_ok());
    assert!(eng26.on_maker_entry_filled(TimestampNs(t0)).is_ok());
    let init_sol_sl = 7_437 - eng26.last_eff_sl_ticks.0;
    s_sol26.ts_local_ns = TimestampNs(t0 + 16_000_000_000);
    s_sol26.bids[0].price = PriceTicks(7_434);
    s_sol26.asks[0].price = PriceTicks(7_435);
    s_sol26.mark_price = PriceTicks(7_434);
    s_sol26.last_trade_price = PriceTicks(7_434);
    s_sol26.factor_ofi = BoundedScore::new_clamped(-0.05);
    s_sol26.factor_tfi = BoundedScore::new_clamped(-0.05);
    let btc_crash = mk_snap(0, 638_000, 7_500, -0.95, -0.98, -0.90, t0 + 16_000_000_000);
    let mut led26 = ParitySyncLedger::new(LIVE_WALLET_1E8);
    let mut eng26_inv = eng26.clone();
    let _ = eng26_inv.step_open_position_v49(&s_sol26, &btc_crash, &gov, &mut led26, -1.10, &ring, 1.2);
    let inv_unshrunk = (7_437 - eng26_inv.last_eff_sl_ticks.0) == init_sol_sl;
    let mut eng26_inv = eng26.clone();
    let _ = eng26_inv.step_open_position_v49(&s_sol26, &btc_crash, &gov, &mut led26, -1.10, &ring, 1.2);
    let inv_unshrunk = (7_437 - eng26_inv.last_eff_sl_ticks.0) == init_sol_sl;
    let _ = eng26.step_open_position_v49(&s_sol26, &btc_crash, &gov, &mut led26, 1.35, &ring, 1.2);
    let comp_sol_sl = 7_437 - eng26.last_eff_sl_ticks.0;
    let v26_shrink = ((init_sol_sl - comp_sol_sl) * 100) / init_sol_sl.max(1);
    println!("  [V26 BTC_CRASH_CONTAGION  ] LaggingSOL + BTC(-0.98) Beta=+1.35 -> SL={}t->{}t (Shrink={}%) -> SEALED", init_sol_sl, comp_sol_sl, v26_shrink);
    assert!(inv_unshrunk && v26_shrink >= 55);

    // V27: Live Bybit 10001 SL-Rejection Amnesia Guard (Preserves tight SL & triggers P0-A Flatten)
    let shm27 = MappedShmRegion::ephemeral_self_test_path("v501_v27_sl_amnesia");
    if let Ok(reg27) = MappedShmRegion::open_or_create(&shm27, true) {
        let (tg27, _) = mpsc::sync_channel::<String>(8);
        let (vx27, _) = mpsc::sync_channel::<VaultIoCommand>(8);
        let mut rt27 = ExecRuntimeState::new();
        rt27.verified_exchange_sl[1] = PriceTicks(179_112);
        let mut engs27 = SlotExecutionEngine::new_12_slot_array();
        let mut snaps27 = [mk_snap(1, 180_249, 3_200, -0.85, -0.95, -0.80, t0); MAX_ACTIVE_PAIRS];
        let _ = engs27[1].adopt_live_open_position(Side::Bid, PriceTicks(180_249), LotQty(1), LotQty(1), false, PriceTicks(180_520), PriceTicks(180_249), TimestampNs(t0), &snaps27[1]);
        engs27[1].last_eff_sl_ticks = PriceTicks(180_420);
        engs27[1].chandelier_profit_sl_armed = true;
        let foren27 = [SlotForensicEntrySnapshot::default(); MAX_ACTIVE_PAIRS];
        let mut g27 = OnlineAdaptiveGovernor::new();
        let mut l27 = ParitySyncLedger::new(LIVE_WALLET_1E8);
        vortex_rs::exec_gateway_handlers::handle_gateway_event(
            vortex_rs::bybit_gateway::GatewayEvent::StopLossSynced { slot_idx: 1, sl_price: PriceTicks(180_420), verified: false },
            TimestampNs(t0 + 5_000_000_000), None, &mut rt27, &snaps27, &foren27, &mut engs27, &mut g27, &mut l27, reg27.arena(), &tg27, &vx27,
        );
        snaps27[1].mark_price = PriceTicks(180_350);
        snaps27[1].last_trade_price = PriceTicks(180_350);
        let (flat27, sl27) = should_trigger_live_emergency_flatten(&rt27, 1, Side::Bid, engs27[1].last_eff_sl_ticks, &snaps27[1]);
        println!("  [V27 LIVE_10001_SL_MEMORY ] Rejected10001 kept SL={}t (not 179112t) & Flatten={} -> SEALED", sl27.0, flat27);
        assert!(engs27[1].last_eff_sl_ticks == PriceTicks(180_420) && engs27[1].chandelier_profit_sl_armed && flat27);
        let _ = std::fs::remove_file(&shm27);
    }
    println!("==========================================================================================\n");
}
