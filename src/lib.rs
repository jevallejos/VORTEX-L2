//! VORTEX-OMEGA V4.6 Institutional 200Hz Maker-First HFT Core (24-Module Zero-Alloc Architecture, SHM v15, PAR2).
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
    use crate::exec_daemon::elastic_resting_queue_timers_ns;
    use crate::exec_gateway_handlers::{
        run_oos_520_certification_suite, run_oos_520_multi_regime_suite, ExecRuntimeState,
    };
    use crate::fee_cadence_beta::FeeSchedule;
    use crate::fifo_sim::{
        run_200k_fifo_benchmark, run_ab_economic_ablation_model, run_quadruple_omega_ablation_suite,
        AblationMode,
    };
    use crate::kinematic_surface::{
        audit_unified_63_losses_and_2160_manifold_with_fees, UNIFIED_LOSS_LAB_COUNT,
    };
    use crate::lob_shm::VORTEX_SHM_VERSION;
    use crate::replay_walkforward::run_omega_digital_twin_walk_forward_lab;
    use crate::tap_ring::run_isolated_tap_self_test;
    use crate::types::{LotQty, PriceTicks, ScaledBps, Side, APPROVED_SPECS};

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
        assert_eq!(oos_trades, 520);
        assert!(maker_ratio >= 80.0 && pf >= 1.35 && cum_usdt > 0.0 && drift == 0);

        let (mitigated, manifold) = OnlineAdaptiveGovernor::audit_unified_62_losses_and_2160_manifold();
        assert_eq!((mitigated, manifold), (UNIFIED_LOSS_LAB_COUNT, 2_160));
    }

    #[test]
    fn test_v46_full_4_islands_vip_rebate_and_multi_regime_oos520_gate() {
        let quad = run_quadruple_omega_ablation_suite();
        assert!(quad.pareto_strictly_dominant);

        let twin = run_omega_digital_twin_walk_forward_lab();
        assert_eq!((twin.closed_trades, twin.hawkes_queue_aborts, twin.post_only_rejects, twin.parity_sync_drift_1e8), (3, 1, 0, 0));

        let oos = run_oos_520_multi_regime_suite();
        assert_eq!(oos.closed_trades, 520);
        assert!(oos.maker_exit_ratio_pct >= 82.0 && oos.profit_factor >= 2.0 && oos.parity_sync_drift_1e8 == 0);

        let rt = ExecRuntimeState::new();
        assert_eq!(rt.epps_trackers.len(), 12);
        assert_eq!(rt.markout_rings.len(), 12);
        let q_t = elastic_resting_queue_timers_ns(5_000_000_000);
        assert_eq!(q_t.repeg_wait_ns, 2_500_000_000);

        let mm_rebate = FeeSchedule::from_live_rates_clamped(ScaledBps(-50), ScaledBps(300));
        assert_eq!(audit_unified_63_losses_and_2160_manifold_with_fees(mm_rebate), (UNIFIED_LOSS_LAB_COUNT, 2_160));
        let (pnl_reb, fee_reb, _, _) = APPROVED_SPECS[0].compute_leg_pnl_and_fee_1e8_with_schedule(
            Side::Bid, LotQty(1), PriceTicks(759_000), PriceTicks(759_600), true, mm_rebate,
        );
        assert!(fee_reb < 0 && pnl_reb > 6_000_000);
    }
}
