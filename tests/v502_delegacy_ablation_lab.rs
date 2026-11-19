//! Counterfactual A/B De-Legacy Ablation Suite: V5.0.1 Static Scalars vs V5.0.2 Pure C1 Self-Learning.
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::similar_names,
    clippy::suboptimal_flops,
    clippy::too_many_lines
)]

use vortex_rs::alpha_3factor::{
    should_abort_collapsed_resting_l1_wall, should_abort_collapsed_resting_l1_wall_dynamic,
    OrthogonalFactorState, SignalVerdict,
};
use vortex_rs::bayesian_governor::OnlineAdaptiveGovernor;
use vortex_rs::execution_fsm::{ExitSubReason, OrderLifecycleState, SlotExecutionEngine};
use vortex_rs::fee_cadence_beta::FeeSchedule;
use vortex_rs::forensic_parquet::ParitySyncLedger;
use vortex_rs::lob_shm::{BookLevel, SymbolLobSnapshot};
use vortex_rs::markout_tracker::SlotMarkoutRing;
use vortex_rs::types::{
    evaluate_utc_and_funding_cadence, BoundedScore, LotQty, PriceTicks, ScaledBps, Side,
    TimestampNs, APPROVED_SPECS,
};

const LIVE_WALLET_1E8: i64 = 6_499_740_000;
const LIVE_TICKS: [i64; 12] = [
    645_408, 180_819, 7_493, 7_141, 19_505, 7_283, 2_767, 14_174, 1_340, 9_218, 3_394, 4_459,
];

fn set_l4_book(s: &mut SymbolLobSnapshot, mid_px: i64, side: Side, fav_print: bool, l1_qty: i64) {
    let b0 = mid_px.max(2);
    let a0 = b0 + 1;
    for lvl in 0..4_usize {
        let step = i64::try_from(lvl).unwrap_or(0);
        s.bids[lvl] = BookLevel {
            price: PriceTicks((b0 - step).max(1)),
            qty: LotQty(if lvl == 0 { l1_qty } else { 400 }),
        };
        s.asks[lvl] = BookLevel {
            price: PriceTicks(a0 + step),
            qty: LotQty(if lvl == 0 { l1_qty } else { 400 }),
        };
    }
    s.refresh_deep_l1_l5_totals();
    let print_px = match (side, fav_print) {
        (Side::Bid, true) => a0 + 1,
        (Side::Ask, true) => (b0 - 1).max(1),
        (Side::Bid, false) => b0,
        (Side::Ask, false) => a0,
    };
    s.mark_price = PriceTicks(b0);
    s.last_trade_price = PriceTicks(print_px);
}

fn mk_snap(slot: usize, px: i64, vol_sbps: i64, ofi: f64, tfi: f64, geom: f64, ts: i64) -> SymbolLobSnapshot {
    let mut s = SymbolLobSnapshot::new_empty();
    s.symbol_id = APPROVED_SPECS[slot].symbol_id;
    s.is_eligible = true;
    s.sequence_id = 100;
    s.ts_local_ns = TimestampNs(ts);
    s.realized_vol_5m = ScaledBps(vol_sbps);
    set_l4_book(&mut s, px, Side::Bid, false, 1_400);
    s.factor_ofi = BoundedScore::new_clamped(ofi);
    s.factor_tfi = BoundedScore::new_clamped(tfi);
    s.factor_geom = BoundedScore::new_clamped(geom);
    s.ob_bull_efficacy_pm = 560;
    s.ob_bear_efficacy_pm = 520;
    s
}

#[test]
fn test_v502_delegacy_ablation_tao1_cadence_l1_contagion_and_400ep_pareto() {
    let fees = FeeSchedule::VIP0_DEFAULT;
    let t0 = 1_794_970_672_000_000_i64;

    println!("\n================================================================================================");
    println!("[VORTEX-OMEGA V5.0.2 DE-LEGACY A/B ABLATION LAB] Static V4.x Scalars vs Pure C1 Self-Learning");
    println!("================================================================================================");

    // D1: Exact 1-Tick Granular Replay of Live TAOUSDT #1 (19505t -> 19500t [-2.56bp] -> 19514t vs 19519t)
    let gov = OnlineAdaptiveGovernor::new();
    let ring = SlotMarkoutRing::new();
    let mut tao_s = mk_snap(4, 19_505, 2_749, 0.68, 0.50, 0.56, t0);
    let btc_s = mk_snap(0, 645_408, 2_800, 0.0, 0.0, 0.0, t0);
    let v_tao = SignalVerdict::AuthorizeMakerEntry {
        side: Side::Bid,
        target_excursion_scaled_bps: ScaledBps(680),
    };

    let mut eng_leg = SlotExecutionEngine::new(4);
    eng_leg.learned_mfe_bps = 594;
    let mut eng_ela = eng_leg.clone();
    assert!(eng_leg.try_arm_maker_entry(&tao_s, v_tao, LotQty(4)).is_ok());
    assert!(eng_leg.on_maker_entry_filled(TimestampNs(t0)).is_ok());
    assert!(eng_ela.try_arm_maker_entry(&tao_s, v_tao, LotQty(4)).is_ok());
    assert!(eng_ela.on_maker_entry_filled(TimestampNs(t0)).is_ok());

    let mut led_leg = ParitySyncLedger::new(LIVE_WALLET_1E8);
    let mut led_ela = ParitySyncLedger::new(LIVE_WALLET_1E8);
    let (mut leg_net_sbps, mut leg_reason) = (0_i64, ExitSubReason::StandardStructuralTp);
    let (mut ela_net_sbps, mut ela_reason) = (0_i64, ExitSubReason::StandardStructuralTp);

    for sec in 1..=30_i64 {
        let px = if sec <= 7 { 19_500 } else { 19_505 + (sec - 7) };
        tao_s.ts_local_ns = TimestampNs(t0 + sec * 1_000_000_000);
        tao_s.bids[0].price = PriceTicks(px - 1);
        tao_s.asks[0].price = PriceTicks(px);
        tao_s.mark_price = PriceTicks(px);
        tao_s.last_trade_price = PriceTicks(px);
        if sec <= 7 {
            tao_s.factor_ofi = BoundedScore::new_clamped(-0.05);
            tao_s.factor_tfi = BoundedScore::new_clamped(-0.62);
        } else {
            tao_s.factor_ofi = BoundedScore::new_clamped(0.32);
            tao_s.factor_tfi = BoundedScore::new_clamped(0.24);
        }
        if matches!(eng_leg.state, OrderLifecycleState::PositionOpenMakerExit { .. }) {
            let cur_g = ((px - 19_505) * 1_000_000) / 19_505;
            let own_f = 0.55 * tao_s.factor_ofi.get() + 0.45 * tao_s.factor_tfi.get();
            if sec >= 3 && cur_g < -200 && own_f < 0.15 {
                eng_leg.latch_defensive_floor_if_needed(true);
            }
            if let Some(s_l) = eng_leg.step_open_position_v49(&tao_s, &btc_s, &gov, &mut led_leg, 1.0, &ring, 1.0) {
                leg_net_sbps = s_l.net_scaled_bps.raw();
                leg_reason = eng_leg.last_exit_sub_reason;
            }
        }
        if matches!(eng_ela.state, OrderLifecycleState::PositionOpenMakerExit { .. }) {
            if let Some(s_e) = eng_ela.step_open_position_v49(&tao_s, &btc_s, &gov, &mut led_ela, 1.0, &ring, 1.0) {
                ela_net_sbps = s_e.net_scaled_bps.raw();
                ela_reason = eng_ela.last_exit_sub_reason;
            }
        }
    }
    println!(
        "  [D1 LIVE_TAO1_COUNTERFACT ] Legacy={:?} ({:+.2}bp) vs Elastic={:?} ({:+.2}bp) -> {:+.2}bp ({:.1}x)",
        leg_reason,
        (leg_net_sbps as f64) / 100.0,
        ela_reason,
        (ela_net_sbps as f64) / 100.0,
        ((ela_net_sbps - leg_net_sbps) as f64) / 100.0,
        (ela_net_sbps as f64) / (leg_net_sbps.max(1) as f64)
    );
    assert!(leg_net_sbps > 0 && leg_net_sbps <= 61 && ela_net_sbps >= 300 && ela_net_sbps > leg_net_sbps * 4);

    // D2: Summer EDT NYSE Bell (13:30 UTC = 09:30 EDT) + Mild Carry (-0.80bp, 2.5h away) + VIP Rebate
    let edt_ny_ns = (20_650_i64 * 86_400 + 13 * 3_600 + 1_800) * 1_000_000_000;
    let mut s_edt = mk_snap(3, 7_141, 3_600, 0.52, 0.65, 0.45, edt_ny_ns);
    s_edt.funding_rate_scaled_bps = ScaledBps(80);
    let mut gov_vip = OnlineAdaptiveGovernor::new();
    gov_vip.set_active_fee_schedule(FeeSchedule::from_live_rates_clamped(ScaledBps(-50), ScaledBps(300)));
    let cad_ela = gov_vip.slot_temporal_cadence_for_side(3, Some(Side::Bid), &s_edt);
    let eq_v_same = ScaledBps(ScaledBps::MIN_REQUIRED_VOL_5M_BPS.raw() * 4);
    let leg_base_utc = evaluate_utc_and_funding_cadence(s_edt.ts_local_ns, s_edt.realized_vol_5m, eq_v_same);
    let leg_kelly = cad_ela.kelly_lot_mult_permille.min(leg_base_utc.kelly_lot_mult_permille);
    let leg_kin = cad_ela.kin_energy_min_delta_bps.max(leg_base_utc.kin_energy_min_delta_bps);
    println!(
        "  [D2 DST_VIP_CADENCE_CLEAN ] Legacy(Kelly={}pm Kin+{}) vs Elastic(Kelly={}pm Kin+{}) -> UNCHOKED",
        leg_kelly, leg_kin, cad_ela.kelly_lot_mult_permille, cad_ela.kin_energy_min_delta_bps
    );
    assert!(cad_ela.kelly_lot_mult_permille >= leg_kelly && cad_ela.kin_energy_min_delta_bps <= leg_kin);

    // D3: Dynamic Self-Learned L1 Queue Floor & Wall Collapse (Clean p_Taker=10pm vs Toxic p_Taker=260pm)
    let tao_spec = APPROVED_SPECS[4];
    let mm_fees = FeeSchedule::from_live_rates_clamped(ScaledBps(-25), ScaledBps(320));
    let clean_l1_ok = tao_spec.passes_l1_queue_depth_and_wall_guard_dynamic(
        PriceTicks(19_505), ScaledBps(2_800), 220, LotQty(6), LotQty(6), LotQty(45), 0.65, 0.80, 10, mm_fees,
    );
    let toxic_l1_blocked = !tao_spec.passes_l1_queue_depth_and_wall_guard_dynamic(
        PriceTicks(19_505), ScaledBps(2_800), -200, LotQty(10), LotQty(10), LotQty(45), 0.65, 0.80, 260, fees,
    );
    let mut flick_snap = mk_snap(4, 19_505, 2_600, -0.13, -0.10, 0.35, t0);
    flick_snap.bids[0].qty = LotQty(5);
    let legacy_false_abort = should_abort_collapsed_resting_l1_wall(tao_spec, Side::Bid, PriceTicks(19_505), &flick_snap, LotQty(14));
    let elastic_clean_kept = !should_abort_collapsed_resting_l1_wall_dynamic(tao_spec, Side::Bid, PriceTicks(19_505), &flick_snap, LotQty(14), 10, mm_fees);
    flick_snap.factor_ofi = BoundedScore::new_clamped(-0.08);
    flick_snap.bids[0].qty = LotQty(6);
    let elastic_toxic_aborts_faster = should_abort_collapsed_resting_l1_wall_dynamic(tao_spec, Side::Bid, PriceTicks(19_505), &flick_snap, LotQty(14), 260, fees);
    println!(
        "  [D3 SELF_LEARN_L1_SHIELD  ] Clean11.7$Ok={} | Toxic19.5$Veto={} | FlickerSaved={} | ToxicFastAbort={}",
        clean_l1_ok, toxic_l1_blocked, legacy_false_abort && elastic_clean_kept, elastic_toxic_aborts_faster
    );
    assert!(clean_l1_ok && toxic_l1_blocked && legacy_false_abort && elastic_clean_kept && elastic_toxic_aborts_faster);

    // D4: C1 Severity-Weighted Cellular SL vs Deep SL Global Contagion + SCAR-LOCK 63/63 Invariant
    assert_eq!(OnlineAdaptiveGovernor::audit_unified_62_losses_and_2160_manifold(), (63, 2_160));
    let mut g_cell = OnlineAdaptiveGovernor::new();
    g_cell.observe_closed_trade_at_entry(9, Side::Bid, PriceTicks(9_218), ScaledBps(-1_020), false, TimestampNs(t0));
    let link_locally_cooled = g_cell.audit_historical_entry_veto(9, Side::Bid, 9_218, t0 + 15_000_000_000).is_some();
    let tao_unblocked_globally = g_cell.audit_historical_entry_veto(4, Side::Bid, 19_505, t0 + 15_000_000_000).is_none();
    g_cell.observe_closed_trade_at_entry(9, Side::Bid, PriceTicks(9_218), ScaledBps(-3_800), false, TimestampNs(t0 + 20_000_000_000));
    let tao_blocked_on_deep_sl = g_cell.audit_historical_entry_veto(4, Side::Bid, 19_505, t0 + 30_000_000_000).is_some();
    println!(
        "  [D4 C1_CELLULAR_CONTAGION ] -10.2bp LocalCool={} OtherSlotsFree={} | -38bp GlobalFreeze={} | ScarLock=63/63",
        link_locally_cooled, tao_unblocked_globally, tao_blocked_on_deep_sl
    );
    assert!(link_locally_cooled && tao_unblocked_globally && tao_blocked_on_deep_sl);

    // D5: 400-Episode Head-to-Head A/B Portfolio Simulation (V5.0.1 Legacy Overlaps vs V5.0.2 Pure Elastic)
    let mut seed: u64 = 0x2026_0502_CAFE_BABE;
    let mut rng = || -> u64 { seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17; seed };

    let mut gov_leg = OnlineAdaptiveGovernor::new();
    let mut gov_pure = OnlineAdaptiveGovernor::new();
    let mut engs_leg = SlotExecutionEngine::new_12_slot_array();
    let mut engs_pure = SlotExecutionEngine::new_12_slot_array();
    let mut led_old = ParitySyncLedger::new(LIVE_WALLET_1E8);
    let mut led_new = ParitySyncLedger::new(LIVE_WALLET_1E8);
    let mut leg_global_cd_until: [i64; 2] = [0, 0];

    let (mut tr_old, mut w_old, mut sc_old, mut l_old, mut net_bps_old, mut prof_old, mut loss_old) =
        (0_u32, 0_u32, 0_u32, 0_u32, 0_i64, 0_i64, 0_i64);
    let (mut tr_new, mut w_new, mut sc_new, mut l_new, mut net_bps_new, mut prof_new, mut loss_new) =
        (0_u32, 0_u32, 0_u32, 0_u32, 0_i64, 0_i64, 0_i64);
    let (mut sum_pnl_old, mut sum_pnl_new, mut amputated_wins_old) = (0_i64, 0_i64, 0_u32);
    let mut ts = t0 + 3_600_000_000_000;

    for _ep in 0..400_u32 {
        ts += 75_000_000_000;
        let slot = ((rng() % 11) + 1) as usize;
        if slot == 8 {
            continue;
        }
        let base_px = LIVE_TICKS[slot];
        let side = if (rng() & 1) == 0 { Side::Bid } else { Side::Ask };
        let d_idx = usize::from(matches!(side, Side::Ask));
        let sgn = side.sign_f64();
        let kind = (rng() % 10) as u32;
        let l1_lots = if kind == 6 {
            let one_lot = APPROVED_SPECS[slot].lot_notional_1e8(LotQty(1), PriceTicks(base_px)).max(1);
            (1_950_000_000 / one_lot).max(2)
        } else {
            1_400
        };
        let mut s_in = mk_snap(slot, base_px, 4_200, sgn * 0.74, sgn * 0.90, sgn * 0.78, ts);
        set_l4_book(&mut s_in, base_px, side, false, l1_lots);
        s_in.funding_rate_scaled_bps = ScaledBps(if matches!(side, Side::Bid) { 85 } else { -85 });

        let step_episode = |is_legacy: bool,
                            eng: &mut SlotExecutionEngine,
                            g: &mut OnlineAdaptiveGovernor,
                            led: &mut ParitySyncLedger|
         -> Option<(i64, i64, bool, ExitSubReason)> {
            let mfe_s = ScaledBps(eng.learned_mfe_bps);
            let Ok(lots) = g.authorizes_adaptive_entry_v49(
                slot, side, led.current_equity_1e8(), 1_000, mfe_s, &s_in, &s_in, 0.0, 1.0,
            ) else {
                return None;
            };
            eng.sync_governor_for_side(g, side);
            let (dir_e, _, _) = g.slot_direction_diagnostics(slot, side);
            let v = OrthogonalFactorState::evaluate_entry_signal_v46(APPROVED_SPECS[slot], &s_in, &s_in, mfe_s, ScaledBps(dir_e), g.slot_taker_prob_permille(slot, side), fees, 0.0, 1.0);
            if !matches!(v, SignalVerdict::AuthorizeMakerEntry { side: s, .. } if s == side) { return None; }
            if eng.try_arm_maker_entry(&s_in, v, lots).is_err()
                || eng.on_maker_entry_filled(TimestampNs(ts)).is_err()
            {
                eng.state = OrderLifecycleState::Idle;
                return None;
            }
            let en_px = side.select_entry_bbo_price(s_in.bids[0].price, s_in.asks[0].price);
            let mut r_mk = SlotMarkoutRing::new();
            r_mk.arm_fill_probe(side, en_px, PriceTicks(base_px), TimestampNs(ts));
            let mut s_sim = s_in;
            for step in 1..=96_i64 {
                let dt_ns = step * 500_000_000;
                let sec_f = (step as f64) * 0.5;
                let bps_move = match kind {
                    0..=4 => {
                        if step <= 10 {
                            -320
                        } else if step <= 54 {
                            (((sec_f - 5.0) * 85.0).round() as i64).min(2_150)
                        } else if step <= 70 {
                            2_150
                        } else {
                            820
                        }
                    }
                    5..=7 => {
                        if step <= 44 {
                            ((sec_f * 85.0).round() as i64).min(2_150)
                        } else if step <= 64 {
                            2_150
                        } else {
                            820
                        }
                    }
                    8 => -((sec_f * 45.0).round() as i64),
                    _ => -((sec_f * 75.0).round() as i64),
                };
                let cur_p = (base_px + ((base_px * bps_move) / 1_000_000) * side.sign_i64()).max(2);
                let fav_print = bps_move > 0;
                s_sim.ts_local_ns = TimestampNs(ts + dt_ns);
                set_l4_book(&mut s_sim, cur_p, side, fav_print, l1_lots);
                let in_crest = (kind <= 4 && step > 54) || ((5..=7).contains(&kind) && step > 44);
                let fl = if bps_move < 0 {
                    if kind <= 4 { -0.22 } else { -0.88 }
                } else if in_crest {
                    -0.12
                } else if kind <= 4 {
                    0.32
                } else {
                    0.82
                };
                s_sim.factor_ofi = BoundedScore::new_clamped(sgn * fl);
                s_sim.factor_tfi = BoundedScore::new_clamped(sgn * fl);
                if in_crest {
                    s_sim.factor_geom = BoundedScore::ZERO;
                }
                r_mk.step_clock(PriceTicks(cur_p), s_sim.ts_local_ns, 200);
                if is_legacy && step >= 6 && bps_move < -200 && fl < 0.15 {
                    eng.latch_defensive_floor_if_needed(true);
                }
                if let Some(sum) = eng.step_open_position_v49(&s_sim, &s_sim, g, led, 1.0, &r_mk, 1.0) {
                    let sub_r = eng.last_exit_sub_reason;
                    g.observe_closed_trade_at_entry(
                        slot, sum.side, sum.entry_price, sum.net_scaled_bps, sum.is_maker_exit, s_sim.ts_local_ns,
                    );
                    return Some((sum.net_scaled_bps.raw(), sum.net_pnl_1e8, sum.is_maker_exit, sub_r));
                }
            }
            eng.state = OrderLifecycleState::Idle;
            None
        };

        // --- ARM A: V5.0.1 Legacy Overlaps ---
        let leg_l1_1e8 = APPROVED_SPECS[slot].lot_notional_1e8(LotQty(l1_lots), PriceTicks(base_px));
        if ts >= leg_global_cd_until[d_idx] && leg_l1_1e8 >= 2_400_000_000 {
            if let Some((nb, pnl, _mkr, sub_r)) = step_episode(true, &mut engs_leg[slot], &mut gov_leg, &mut led_old) {
                tr_old += 1;
                net_bps_old += nb;
                sum_pnl_old += pnl;
                if kind <= 4 && matches!(sub_r, ExitSubReason::AdverseScratchEscape) {
                    amputated_wins_old += 1;
                }
                if nb >= 100 {
                    w_old += 1;
                    prof_old += pnl.max(0);
                } else if nb >= -50 {
                    sc_old += 1;
                    prof_old += pnl.max(0);
                } else {
                    l_old += 1;
                    loss_old += pnl.abs();
                    leg_global_cd_until[d_idx] = ts + 210_000_000_000;
                }
            }
        }

        // --- ARM B: V5.0.2 Pure Elastic Self-Learning ---
        if let Some((nb, pnl, _mkr, _sub_r)) = step_episode(false, &mut engs_pure[slot], &mut gov_pure, &mut led_new) {
            tr_new += 1;
            net_bps_new += nb;
            sum_pnl_new += pnl;
            if nb >= 100 {
                w_new += 1;
                prof_new += pnl.max(0);
            } else if nb >= -50 {
                sc_new += 1;
                prof_new += pnl.max(0);
            } else {
                l_new += 1;
                loss_new += pnl.abs();
            }
        }
    }

    let pf_old = (prof_old as f64) / (loss_old.max(1) as f64);
    let pf_new = (prof_new as f64) / (loss_new.max(1) as f64);
    let drift_old = (led_old.cumulative_net_pnl_1e8 - sum_pnl_old).abs();
    let drift_new = (led_new.cumulative_net_pnl_1e8 - sum_pnl_new).abs();

    println!("------------------------------------------------------------------------------------------------");
    println!(
        "  [D5 ARM A \u{2014} LEGACY V5.0.1 ] Trades={:>3} (W:{:>3} SCR:{:>3} L:{:>2} | AmputatedWins:{:>3}) | Net={:+8.2}bp | PF={:.2} | Drift={}",
        tr_old, w_old, sc_old, l_old, amputated_wins_old, (net_bps_old as f64) / 100.0, pf_old, drift_old
    );
    println!(
        "  [D5 ARM B \u{2014} ELASTIC V5.0.2] Trades={:>3} (W:{:>3} SCR:{:>3} L:{:>2} | AmputatedWins:  0) | Net={:+8.2}bp | PF={:.2} | Drift={}",
        tr_new, w_new, sc_new, l_new, (net_bps_new as f64) / 100.0, pf_new, drift_new
    );
    println!("================================================================================================\n");

    assert_eq!(drift_old, 0);
    assert_eq!(drift_new, 0);
    assert!(amputated_wins_old >= 15 && w_new > w_old && net_bps_new > net_bps_old && pf_new > pf_old && pf_new >= 1.50);
}
