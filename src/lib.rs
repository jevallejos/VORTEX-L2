//! VORTEX-OMEGA V4.7 Institutional Zero-Jitter Execution Core (24-Submodule Clean-Room Architecture).
#![recursion_limit = "256"]

pub mod alpha_3factor;
pub mod bayesian_governor;
pub mod bybit_client;
pub mod bybit_gateway;
pub mod exec_daemon;
pub mod exec_gateway_handlers;
pub mod execution_fsm;
pub mod exit_quoter;
pub mod fee_cadence_beta;
pub mod fifo_sim;
pub mod forensic_parquet;
pub mod governor_plasticity;
pub mod http_ws_transport;
pub mod kinematic_surface;
pub mod lob_shm;
pub mod markout_tracker;
pub mod order_block;
pub mod replay_walkforward;
pub mod tap_ring;
pub mod telemetry_hud;
pub mod types;
pub mod vault_io;
pub mod wave_elliott;
pub mod ws_protocol;

#[cfg(test)]
mod tests {
    use crate::bayesian_governor::OnlineAdaptiveGovernor;
    use crate::exec_gateway_handlers::{
        run_oos_520_certification_suite, run_oos_520_multi_regime_suite,
    };
    use crate::fee_cadence_beta::FeeSchedule;
    use crate::fifo_sim::{
        run_200k_fifo_benchmark, run_ab_economic_ablation_model, run_quadruple_omega_ablation_suite,
        AblationMode,
    };
    use crate::forensic_parquet::ParitySyncLedger;
    use crate::kinematic_surface::UNIFIED_LOSS_LAB_COUNT;
    use crate::lob_shm::{BookLevel, SymbolLobSnapshot, VORTEX_SHM_VERSION};
    use crate::order_block::OB_PLASTICITY_SCHEMA_TAG;
    use crate::replay_walkforward::{
        run_omega_digital_twin_walk_forward_lab, settle_crossed_funding_boundary,
    };
    use crate::tap_ring::run_isolated_tap_self_test;
    use crate::types::{BoundedScore, LotQty, PriceTicks, ScaledBps, Side, TimestampNs, APPROVED_SPECS};
    use crate::ws_protocol::{Vip0WsRateGovernor, WsMutationOp};

    #[test]
    fn test_phase4_constitutional_3_cli_gates_end_to_end() {
        assert_eq!(VORTEX_SHM_VERSION, 15);

        let tap_gate = run_isolated_tap_self_test(false);
        assert!(tap_gate.is_ok());
        if let Ok((approved, rows, crc)) = tap_gate {
            assert_eq!(approved, 12);
            assert_eq!(rows, 12);
            assert_ne!(crc, 0);
        }

        let (elapsed_us, fills, rejects) = run_200k_fifo_benchmark();
        let ev_per_sec = (200_000_u128 * 1_000_000) / elapsed_us.max(1);
        assert!(ev_per_sec >= 100_000 && fills == 1_000 && rejects == 0);

        let mod_a = run_ab_economic_ablation_model(AblationMode::BaselineFixedL1A);
        let mod_b = run_ab_economic_ablation_model(AblationMode::ElasticMultiTrancheB);
        let mod_ab = run_ab_economic_ablation_model(AblationMode::FullVortexL3AB);
        assert_eq!(mod_a.losses + mod_b.losses + mod_ab.losses, 0);
        assert!(mod_ab.cum_net_bps_x100 > mod_b.cum_net_bps_x100);

        let (oos_trades, maker_ratio, pf, cum_usdt, drift) = run_oos_520_certification_suite();
        assert!(oos_trades == 520 && maker_ratio >= 80.0 && pf >= 1.35 && cum_usdt > 0.0 && drift == 0);

        let (mitigated, manifold) = OnlineAdaptiveGovernor::audit_unified_62_losses_and_2160_manifold();
        assert_eq!((mitigated, manifold), (UNIFIED_LOSS_LAB_COUNT, 2_160));
    }

    #[test]
    fn test_chat6_omega_4arm_twin_and_multi_regime_oos520_gate() {
        assert!(run_quadruple_omega_ablation_suite().pareto_strictly_dominant);
        let twin = run_omega_digital_twin_walk_forward_lab();
        assert!(
            twin.closed_trades == 3
                && twin.hawkes_queue_aborts == 1
                && twin.post_only_rejects == 0
                && twin.parity_sync_drift_1e8 == 0
        );

        let oos = run_oos_520_multi_regime_suite();
        assert!(
            oos.closed_trades == 520
                && oos.maker_exit_ratio_pct >= 82.0
                && oos.profit_factor >= 2.0
                && oos.parity_sync_drift_1e8 == 0
                && oos.utc_funding_vetoes >= 2
                && oos.hawkes_pre_fill_aborts >= 20
                && oos.chandelier_profit_sl_wins >= 20
                && oos.wald_v2_zero_loss_escapes >= 20
                && oos.wald_v2_compressed_sl_closes >= 45
        );
    }

    #[test]
    fn test_omega_v47_zero_loose_ends_end_to_end_gate() {
        assert_eq!(OB_PLASTICITY_SCHEMA_TAG, "OB_PLASTICITY_V47");

        let mut rg = Vip0WsRateGovernor::new();
        rg.on_dispatch(WsMutationOp::Amend, 1_000_000_000);
        rg.on_ack_headers(WsMutationOp::Amend, Some(49), Some(1_500));
        assert!(rg.limit_per_sec(WsMutationOp::Amend) >= 50);

        let mut ledger = ParitySyncLedger::new(6_543_090_000);
        ledger.set_live_vip_fee_rates(ScaledBps(245), ScaledBps(400));
        assert_eq!(ledger.active_maker_fee_bps, ScaledBps(245));

        let cut_ns = 1_794_672_000_000_000_000_i64;
        let mut f_snap = SymbolLobSnapshot::new_empty();
        f_snap.symbol_id = APPROVED_SPECS[0].symbol_id;
        f_snap.ts_local_ns = TimestampNs(cut_ns + 1_000_000_000);
        f_snap.realized_vol_5m = ScaledBps(5_400);
        f_snap.bids[0] = BookLevel { price: PriceTicks(759_000), qty: LotQty(460) };
        f_snap.asks[0] = BookLevel { price: PriceTicks(759_001), qty: LotQty(340) };
        f_snap.refresh_deep_l1_l5_totals();
        f_snap.mark_price = PriceTicks(759_000);
        f_snap.funding_rate_scaled_bps = ScaledBps(100);
        let mut last_cut = 0_i64;
        assert!(settle_crossed_funding_boundary(
            APPROVED_SPECS[0], Side::Ask, LotQty(1), PriceTicks(759_000),
            cut_ns - 5_000_000_000, &f_snap, &mut last_cut, &mut ledger,
        ).is_some());

        let mut gov = OnlineAdaptiveGovernor::new();
        gov.set_active_fee_schedule(FeeSchedule::from_live_rates_clamped(ScaledBps(-50), ScaledBps(325)));
        f_snap.factor_ofi = BoundedScore::new_clamped(-0.66);
        f_snap.factor_tfi = BoundedScore::new_clamped(-0.70);
        f_snap.factor_geom = BoundedScore::new_clamped(-0.35);
        let mut alt = f_snap;
        alt.symbol_id = APPROVED_SPECS[3].symbol_id;
        alt.bids[0] = BookLevel { price: PriceTicks(8_200), qty: LotQty(520) };
        alt.asks[0] = BookLevel { price: PriceTicks(8_201), qty: LotQty(360) };
        alt.refresh_deep_l1_l5_totals();
        alt.factor_ofi = BoundedScore::new_clamped(0.68);
        alt.factor_tfi = BoundedScore::new_clamped(0.72);
        alt.factor_geom = BoundedScore::new_clamped(0.38);
        assert!(gov.authorizes_adaptive_entry_v47(3, Side::Bid, 6_543_090_000, &alt, &f_snap, 0.0, -0.85));
    }
}
