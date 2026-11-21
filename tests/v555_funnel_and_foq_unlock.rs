//! V5.5.5 Canonical Verification: Elastic Funnel (B-01/B-02/B-04), FOQ Tail Shield (B-03) & TFI Prior (B-05).

use vortex_rs::alpha_3factor::{
    compute_corrective_abc_macro_reversal_relief_c1, compute_passive_iceberg_absorption_penalty_c1,
    OrthogonalFactorState, INV_PHI, INV_PHI_SQ,
};
use vortex_rs::exec_daemon::allows_buried_queue_third_slot_unlock_v555;
use vortex_rs::execution_fsm::{OrderLifecycleState, SlotExecutionEngine};
use vortex_rs::fee_cadence_beta::{FeeSchedule, MonetaryPortfolioBetaGuard};
use vortex_rs::kinematic_surface::{evaluate_moallemi_stoikov_queue_shield_with_beta, HawkesQueueTracker};
use vortex_rs::lob_shm::{BookLevel, SymbolLobSnapshot};
use vortex_rs::types::{
    compute_multilot_q_struct_v555_c1, pilar2_penny_jump_credit_c1, BoundedScore, LotQty, PriceTicks,
    ScaledBps, Side, TimestampNs, APPROVED_SPECS,
};

const SLOT_BTC: usize = 0;
const SLOT_WIF: usize = 8;
const SLOT_LINK: usize = 9;

fn mk_book(slot: usize, bid_px: i64, bid_q: i64, ask_q: i64, ofi: f64, tfi: f64) -> SymbolLobSnapshot {
    let mut s = SymbolLobSnapshot::new_empty();
    s.symbol_id = APPROVED_SPECS[slot].symbol_id;
    s.is_eligible = true;
    s.sequence_id = 10;
    s.ts_local_ns = TimestampNs(10_000_000_000);
    s.realized_vol_5m = ScaledBps(3_500);
    s.bids.iter_mut().zip(s.asks.iter_mut()).zip(0_i64..5).for_each(|((b, a), k)| {
        *b = BookLevel { price: PriceTicks(bid_px - k), qty: LotQty(bid_q) };
        *a = BookLevel { price: PriceTicks(bid_px + 1 + k), qty: LotQty(ask_q) };
    });
    s.refresh_deep_l1_l5_totals();
    s.mark_price = PriceTicks(bid_px);
    s.last_trade_price = PriceTicks(bid_px);
    s.factor_ofi = BoundedScore::new_clamped(ofi);
    s.factor_tfi = BoundedScore::new_clamped(tfi);
    s
}

#[test]
fn test_v555_b01_near_link_corrective_abc_macro_reversal_relief() {
    let psi = 0.90;
    let macro_pen_eff = |ofi: f64, tfi: f64, ob_own: i64, s_wall: f64| -> (f64, f64) {
        let s_ice = compute_passive_iceberg_absorption_penalty_c1(ofi, tfi, 0.0);
        let r_rev = compute_corrective_abc_macro_reversal_relief_c1(psi, ob_own, 300, ofi, tfi, s_wall, s_ice);
        // Lagging macro_phase < -0.45 saturates both macro S_3 penalties to 1.0 => pen = 1 * (1 - r_rev).
        (r_rev, 1.0 - r_rev)
    };
    let (r_near, pen_near) = macro_pen_eff(0.54, 0.75, 700, 0.0);
    let (r_link, pen_link) = macro_pen_eff(0.57, 0.69, 700, 0.0);
    assert!(r_near > 0.45 && pen_near < INV_PHI, "NEAR r_rev={r_near} pen={pen_near}");
    assert!(r_link > r_near && pen_link < INV_PHI, "LINK r_rev={r_link} pen={pen_link}");
    let (r_no_ob, pen_no_ob) = macro_pen_eff(0.54, 0.75, 0, 0.85);
    assert!(r_no_ob.abs() < f64::EPSILON && pen_no_ob >= INV_PHI);
}

#[test]
fn test_v555_b02_b04_preprint_multilot_penny_jump_vs_wif_spr1_and_btc_quantum() {
    let q_pre = compute_multilot_q_struct_v555_c1(LotQty(2), 0.0, 0.72, 0.02, 0.58, 0.05, 0.0);
    let q_legacy = compute_multilot_q_struct_v555_c1(LotQty(2), 0.0, 0.72, 0.02, 0.0, 0.05, 0.0);
    assert!(q_pre > INV_PHI_SQ && q_legacy < INV_PHI_SQ, "q_pre={q_pre} q_legacy={q_legacy}");
    let credit = pilar2_penny_jump_credit_c1(3, 50.0, 100, 0.8, 0.05);
    assert!(credit > 0.0, "credit={credit}");

    let fees = FeeSchedule::VIP0_DEFAULT;
    let link = APPROVED_SPECS[SLOT_LINK];
    let ev = |spoof: f64, spr: i64, fill: f64| {
        link.evaluate_pilar2_asymmetry_ev_v555_sbps(
            LotQty(8), PriceTicks(14_000), ScaledBps(3_500), 900, 50, 0.58, 700, 300, Side::Bid, fees,
            0.72, 0.02, 0.30, 0.0, 1_000, ScaledBps(900), ScaledBps(0), spoof, spr, fill,
        )
    };
    let ev_v555 = ev(0.05, 3, 0.8);
    let ev_v553 = ev(1.0, 1, 0.0);
    assert!(ev_v555.is_some_and(|e| e > 0 && e > ev_v553.unwrap_or(0)), "v555={ev_v555:?} v553={ev_v553:?}");

    let wif = APPROVED_SPECS[SLOT_WIF];
    let wif_px = PriceTicks(1_470);
    let tau_wif = wif.tick_to_price_scaled_bps(wif_px).map_or(0, ScaledBps::raw);
    assert!(pilar2_penny_jump_credit_c1(1, tau_wif as f64, 50, 0.8, 0.05).abs() < f64::EPSILON);
    let wif_ev = |spoof: f64, fill: f64| {
        wif.evaluate_pilar2_asymmetry_ev_v555_sbps(
            LotQty(1), wif_px, ScaledBps(3_500), 700, 50, 0.58, 700, 300, Side::Bid, fees,
            0.72, 0.02, 0.30, 0.0, 1_000, ScaledBps(900), ScaledBps(0), spoof, 1, fill,
        )
    };
    assert_eq!(wif_ev(0.05, 0.8), None);
    assert_eq!(wif_ev(0.05, 0.8), wif_ev(1.0, 0.0));

    let w_65 = 6_513_770_000_i64;
    assert!(!APPROVED_SPECS[SLOT_BTC].passes_quantum_ev_growth_gate(
        w_65, PriceTicks(646_312), ScaledBps(3_069), ScaledBps(568), 46, fees
    ));
}

#[test]
fn test_v555_b03_foq_tail_tip_survives_toxic_inversion_aborts() {
    let btc = SymbolLobSnapshot::new_empty();
    let shield = |ofi: f64, tfi: f64| {
        let snap = mk_book(SLOT_LINK, 14_000, 120, 3_000, ofi, tfi);
        evaluate_moallemi_stoikov_queue_shield_with_beta(
            &mut HawkesQueueTracker::new(), Side::Bid, LotQty(5_000), LotQty(120), LotQty(0), &snap, &btc, 5, 0.0,
        )
    };
    let tip = shield(0.48, 0.35);
    assert!(!tip.abort_now, "tail tip tox={} skew={}", tip.moallemi_toxicity, tip.stoikov_skew);
    let toxic = shield(-0.45, -0.40);
    assert!(toxic.abort_now, "toxic tox={} skew={}", toxic.moallemi_toxicity, toxic.stoikov_skew);
}

#[test]
fn test_v555_b06_buried_two_slot_stagger_third_slot_penny_jump_unlock() {
    let mut engines = SlotExecutionEngine::new_12_slot_array();
    let rest_ts = TimestampNs(9_000_000_000);
    let rest = |px: i64| OrderLifecycleState::RestingEntryQueue { side: Side::Bid, entry_price: PriceTicks(px), qty: LotQty(8), target_excursion: ScaledBps(900), sl_trigger_price: PriceTicks(px - 40), resting_since_ts: rest_ts };
    (engines[3].state, engines[4].state) = (rest(7_000), rest(9_000));
    let mut queues = [(LotQty(0), LotQty(0)); 12];
    (queues[3], queues[4]) = ((LotQty(5_000), LotQty(4_900)), (LotQty(4_000), LotQty(3_950)));
    let mut pj = mk_book(SLOT_LINK, 14_000, 120, 3_000, 0.48, 0.35);
    pj.asks.iter_mut().for_each(|a| a.price = PriceTicks(a.price.0 + 1));
    let (flat, guard) = (mk_book(SLOT_LINK, 14_000, 120, 3_000, 0.48, 0.35), MonetaryPortfolioBetaGuard::new());
    let unlock = |snap: &SymbolLobSnapshot, now: i64| allows_buried_queue_third_slot_unlock_v555(SLOT_LINK, Side::Bid, TimestampNs(now), &engines, &queues, snap, LotQty(8), 0.80, 70_000, &guard, 2_000, 1_000);
    assert!(unlock(&pj, 10_000_000_000) && !unlock(&flat, 10_000_000_000) && !unlock(&pj, 9_100_000_000));
}

#[test]
fn test_v555_b05_wif_deep_book_converged_flow_tfi_prior_unlock() {
    let mut st = OrthogonalFactorState::new();
    let mut snap = mk_book(SLOT_WIF, 1_470, 25_000, 25_000, 0.0, 0.0);
    let (sec, t0) = (1_000_000_000_i64, 1_000_000_000_000_i64);
    snap.ts_local_ns = TimestampNs(t0);
    st.on_l2_book_update(&mut snap);
    // Warm-up: balanced 2 x 67-lot prints per second for 600 s => learned_flow_lots ~ 600 converged.
    (1_i64..=600).for_each(|k| {
        snap.ts_local_ns = TimestampNs(t0 + k * sec);
        st.on_trade_print(Side::Bid, LotQty(67), &mut snap);
        st.on_trade_print(Side::Ask, LotQty(67), &mut snap);
        st.on_l2_book_update(&mut snap);
    });
    assert!(st.spoofing_density() <= 0.05);
    snap.ts_local_ns = TimestampNs(t0 + 630 * sec);
    st.on_trade_print(Side::Bid, LotQty(450), &mut snap);
    let tfi = snap.factor_tfi.get();
    assert!(tfi >= INV_PHI_SQ, "tfi={tfi}");
}
