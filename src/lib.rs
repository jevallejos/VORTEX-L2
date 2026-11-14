//! VORTEX-OMEGA Institutional Low-Latency Execution Core (18-Module Clean-Room Architecture).
#![recursion_limit = "256"]

pub mod alpha_3factor;
pub mod bayesian_governor;
pub mod bybit_client;
pub mod bybit_gateway;
pub mod exec_daemon;
pub mod exec_gateway_handlers;
pub mod execution_fsm;
pub mod exit_quoter;
pub mod fifo_sim;
pub mod forensic_parquet;
pub mod http_ws_transport;
pub mod kinematic_surface;
pub mod lob_shm;
pub mod replay_walkforward;
pub mod tap_ring;
pub mod telemetry_hud;
pub mod types;
pub mod wave_elliott;
pub mod ws_protocol;

#[cfg(test)]
mod tests {
    use crate::bayesian_governor::OnlineAdaptiveGovernor;
    use crate::exec_gateway_handlers::{
        run_oos_520_certification_suite, run_oos_520_multi_regime_suite,
    };
    use crate::fifo_sim::{
        run_200k_fifo_benchmark, run_ab_economic_ablation_model, run_quadruple_omega_ablation_suite,
        AblationMode,
    };
    use crate::kinematic_surface::UNIFIED_LOSS_LAB_COUNT;
    use crate::lob_shm::VORTEX_SHM_VERSION;
    use crate::replay_walkforward::run_omega_digital_twin_walk_forward_lab;
    use crate::tap_ring::run_isolated_tap_self_test;

    #[test]
    fn test_phase4_constitutional_3_cli_gates_end_to_end() {
        assert_eq!(VORTEX_SHM_VERSION, 13);

        // Gate 1: Ingestor SHM v13 + PAR1 Ring Self-Test (12 institutional slots)
        let tap_gate = run_isolated_tap_self_test(false);
        assert!(tap_gate.is_ok());
        if let Ok((approved, rows, crc)) = tap_gate {
            assert_eq!(approved, 12);
            assert_eq!(rows, 12);
            assert_ne!(crc, 0);
        }

        // Gate 2: Deterministic 200k FIFO Simulator + Legacy A/B/A+B Pareto
        let (elapsed_us, fills, rejects) = run_200k_fifo_benchmark();
        let ev_per_sec = (200_000_u128 * 1_000_000) / elapsed_us.max(1);
        assert!(ev_per_sec >= 100_000);
        assert_eq!(fills, 1_000);
        assert_eq!(rejects, 0);

        let mod_a = run_ab_economic_ablation_model(AblationMode::BaselineFixedL1A);
        let mod_b = run_ab_economic_ablation_model(AblationMode::ElasticMultiTrancheB);
        let mod_ab = run_ab_economic_ablation_model(AblationMode::FullVortexL3AB);
        assert_eq!(mod_a.losses + mod_b.losses + mod_ab.losses, 0);
        assert!(mod_ab.cum_net_bps_x100 > mod_b.cum_net_bps_x100);

        // Gate 3: OOS 520 Certification + 63/63 Scar Loss Lab + 2160 Manifold
        let (oos_trades, maker_ratio, pf, cum_usdt, drift) = run_oos_520_certification_suite();
        assert_eq!(oos_trades, 520);
        assert!(maker_ratio >= 80.0);
        assert!(pf >= 1.35);
        assert!(cum_usdt > 0.0);
        assert_eq!(drift, 0);

        let (mitigated, manifold) =
            OnlineAdaptiveGovernor::audit_unified_62_losses_and_2160_manifold();
        assert_eq!(mitigated, UNIFIED_LOSS_LAB_COUNT);
        assert_eq!(manifold, 2_160);
    }

    #[test]
    fn test_chat6_omega_4arm_twin_and_multi_regime_oos520_gate() {
        let quad = run_quadruple_omega_ablation_suite();
        assert!(quad.pareto_strictly_dominant);

        let twin = run_omega_digital_twin_walk_forward_lab();
        assert_eq!(twin.closed_trades, 3);
        assert_eq!(twin.hawkes_queue_aborts, 1);
        assert_eq!(twin.post_only_rejects, 0);
        assert_eq!(twin.parity_sync_drift_1e8, 0);

        let oos = run_oos_520_multi_regime_suite();
        assert_eq!(oos.closed_trades, 520);
        assert!(oos.maker_exit_ratio_pct >= 82.0);
        assert!(oos.profit_factor >= 2.0);
        assert_eq!(oos.parity_sync_drift_1e8, 0);
        assert!(oos.utc_funding_vetoes >= 2);
        assert!(oos.hawkes_pre_fill_aborts >= 20);
        assert!(oos.chandelier_profit_sl_wins >= 20);
        assert!(oos.wald_v2_zero_loss_escapes >= 20);
        assert!(oos.wald_v2_compressed_sl_closes >= 45);
    }
}
