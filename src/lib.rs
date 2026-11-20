//! VORTEX-OMEGA V5.5 Institutional Zero-Jitter Execution Core (27-Submodule Clean-Room Architecture).
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
pub mod candle_store;
pub mod par2_archiver;
pub mod web_server;
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

#[cfg(test)]
mod omega_v48_constitutional_gate {
    use crate::alpha_3factor::SignalVerdict;
    use crate::execution_fsm::SlotExecutionEngine;
    use crate::fee_cadence_beta::FeeSchedule;
    use crate::governor_plasticity::{DailySessionGovernor, DailyShieldPhase};
    use crate::lob_shm::{BookLevel, SymbolLobSnapshot};
    use crate::markout_tracker::SlotMarkoutRing;
    use crate::types::{BoundedScore, LotQty, PriceTicks, ScaledBps, Side, TimestampNs, APPROVED_SPECS};
    use crate::vault_io::{is_catastrophic_loss_breach, VaultIoCommand};

    #[test]
    fn test_omega_v48_elastic_daily_shield_quantum_ev_and_toxicity_gate() {
        assert!(core::mem::size_of::<VaultIoCommand>() <= 512);
        assert!(!is_catastrophic_loss_breach(-30_211_283, 6_513_770_000));
        assert!(is_catastrophic_loss_breach(-110_000_000, 6_513_770_000));

        let w_65 = 6_513_770_000_i64;
        assert!(!APPROVED_SPECS[0].passes_quantum_ev_growth_gate(
            w_65, PriceTicks(646_312), ScaledBps(3_069), ScaledBps(568), 46, FeeSchedule::VIP0_DEFAULT,
        ));
        assert!(APPROVED_SPECS[3].passes_quantum_ev_growth_gate(
            w_65, PriceTicks(7_263), ScaledBps(2_206), ScaledBps(628), 50, FeeSchedule::VIP0_DEFAULT,
        ));

        let mut dg = DailySessionGovernor::new(w_65);
        let day1 = TimestampNs(1_794_787_200_000_000_000 + 7_200_000_000_000);
        assert_eq!(
            dg.observe_realized_cashflow(day1, 70_000_000, true, true, false, w_65 + 70_000_000, 381, 400, 3_200, 45),
            DailyShieldPhase::ProfitLockArmed
        );
        assert_eq!(
            dg.observe_realized_cashflow(day1, -26_000_000, true, false, true, w_65 + 44_000_000, 381, 400, 3_200, 45),
            DailyShieldPhase::HibernatingProfitLocked
        );
        assert!(dg.sync_utc_day(TimestampNs(day1.0 + 86_400_000_000_000), w_65 + 44_000_000) && !dg.is_hibernating());

        let mut s3 = SymbolLobSnapshot::new_empty();
        s3.symbol_id = APPROVED_SPECS[3].symbol_id;
        s3.ts_local_ns = TimestampNs(10_000_000_000);
        s3.realized_vol_5m = ScaledBps(6_000);
        s3.bids[0] = BookLevel { price: PriceTicks(8_200), qty: LotQty(450) };
        s3.asks[0] = BookLevel { price: PriceTicks(8_201), qty: LotQty(320) };
        s3.mark_price = PriceTicks(8_200);
        s3.last_trade_price = PriceTicks(8_200);
        let mut eng = SlotExecutionEngine::new(3);
        let v = SignalVerdict::AuthorizeMakerEntry { side: Side::Bid, target_excursion_scaled_bps: ScaledBps(680) };
        assert!(eng.try_arm_maker_entry(&s3, v, LotQty(22)).is_ok() && eng.on_maker_entry_filled(TimestampNs(10_000_000_000)).is_ok());
        let init_dist = 8_200 - eng.last_eff_sl_ticks.0;
        let mut ring = SlotMarkoutRing::new();
        ring.arm_fill_probe(Side::Bid, PriceTicks(8_200), PriceTicks(8_200), TimestampNs(10_000_000_000));
        for &(px, t) in &[(8_197_i64, 10_300_000_000_i64), (8_196, 11_200_000_000), (8_192, 16_000_000_000), (8_188, 25_500_000_000)] {
            ring.step_clock(PriceTicks(px), TimestampNs(t), 200);
        }
        s3.ts_local_ns = TimestampNs(25_500_000_000);
        s3.bids[0] = BookLevel { price: PriceTicks(8_188), qty: LotQty(180) };
        s3.asks[0] = BookLevel { price: PriceTicks(8_189), qty: LotQty(490) };
        s3.mark_price = PriceTicks(8_188);
        s3.last_trade_price = PriceTicks(8_188);
        s3.factor_ofi = BoundedScore::new_clamped(-0.55);
        s3.factor_tfi = BoundedScore::new_clamped(-0.62);
        let z = eng.observe_post_fill_toxicity_and_wald_v48(&s3, &s3, 1.0, &ring, 1.2);
        assert!(eng.maybe_ratchet_sl_with_toxicity_v48(&s3, &s3, 1.0, z).is_some());
        assert!((8_200 - eng.last_eff_sl_ticks.0) * 100 <= init_dist * 35);
    }

    #[test]
    fn test_v51_codebase_audit_regressions_end_to_end() {
        use crate::bayesian_governor::OnlineAdaptiveGovernor;
        use crate::governor_plasticity::{DailySessionGovernor, DailyShieldPhase};
        use crate::http_ws_transport::parse_endpoint_url;
        use crate::lob_shm::{BookLevel, MappedShmRegion, SlotTelemetrySnapshot, SymbolLobSnapshot};
        use crate::types::{BoundedScore, LotQty, PriceTicks, ScaledBps, Side, TimestampNs, APPROVED_SPECS};
        use crate::wave_elliott::{WaveObservables, WaveRegime};

        // 1. Fixed-point parser overflow (> i64::MAX ticks/lots) and half-up rounding guards
        let sui = APPROVED_SPECS[5]; // tick_size_1e8 = 10_000
        assert!(sui.parse_price_ticks("999999999999999999.9999").is_err());
        assert_eq!(sui.parse_lot_qty("999999999999999999999"), LotQty(0));
        assert_eq!(sui.parse_lot_qty("-50"), LotQty(0));
        assert_eq!(sui.parse_price_ticks("0.72606"), Ok(PriceTicks(7_261)));

        // 2. DailySessionGovernor monotonic ProfitLockArmed even if eq_vol_bps spikes mid-session
        let w = 6_600_000_000_i64;
        let mut dg = DailySessionGovernor::new(w);
        let t_day = TimestampNs(1_794_787_200_000_000_000 + 3_600_000_000_000);
        assert_eq!(
            dg.observe_realized_cashflow(t_day, 72_000_000, true, true, false, w + 72_000_000, 380, 400, 3_000, 40),
            DailyShieldPhase::ProfitLockArmed
        );
        assert_eq!(
            dg.observe_realized_cashflow(TimestampNs(t_day.0 + 1_000_000_000), 1_000_000, true, true, false, w + 73_000_000, 380, 400, 18_000, 40),
            DailyShieldPhase::ProfitLockArmed
        );

        // 3. OnlineAdaptiveGovernor V5.1 F6 chop fields survive scar JSON roundtrip
        let w_abc = WaveObservables {
            regime: WaveRegime::CorrectiveAbc,
            climax_active: false,
            hurst: WaveObservables::encode_hurst(0.46),
            macro_phase: BoundedScore::new_clamped(0.25),
            oi_impulse: BoundedScore::ZERO,
            liq_imbalance: BoundedScore::ZERO,
            fib_proj_bps: ScaledBps(0),
        };
        let t0 = 1_800_000_000_000_000_000_i64;
        let mut g1 = OnlineAdaptiveGovernor::new();
        g1.observe_closed_trade_with_regime_v51(1, Side::Ask, PriceTicks(179_961), ScaledBps(-1_956), ScaledBps(0), false, w_abc, 1.0, TimestampNs(t0));
        g1.observe_closed_trade_with_regime_v51(1, Side::Bid, PriceTicks(179_542), ScaledBps(-2_431), ScaledBps(0), false, w_abc, 1.0, TimestampNs(t0 + 60_000_000_000));
        let mut eth_snap = SymbolLobSnapshot::new_empty();
        eth_snap.symbol_id = APPROVED_SPECS[1].symbol_id;
        eth_snap.ts_local_ns = TimestampNs(t0 + 180_000_000_000);
        eth_snap.realized_vol_5m = ScaledBps(3_200);
        eth_snap.bids[0] = BookLevel { price: PriceTicks(179_500), qty: LotQty(200) };
        eth_snap.asks[0] = BookLevel { price: PriceTicks(179_501), qty: LotQty(200) };
        eth_snap.set_wave_observables(w_abc);
        assert!(!g1.passes_cross_side_abc_chop_shield(1, Side::Bid, &eth_snap));
        let scar_json = g1.to_scar_snapshot_json(TimestampNs(t0 + 60_000_000_000));
        let mut g2 = OnlineAdaptiveGovernor::new();
        assert!(g2.apply_scar_snapshot_json(&scar_json).is_some());
        assert!(!g2.passes_cross_side_abc_chop_shield(1, Side::Bid, &eth_snap));

        // 4. ReadOnlyShmRegion & SeqLock bool byte sanitization
        let shm_p = MappedShmRegion::ephemeral_self_test_path("v51_ro_san");
        {
            let rw = MappedShmRegion::open_or_create(&shm_p, true);
            assert!(rw.is_ok());
            if let Ok(ref reg) = rw {
                let tel = SlotTelemetrySnapshot {
                    tranche1_harvested: true,
                    ..SlotTelemetrySnapshot::default()
                };
                if let Ok(cell) = reg.arena().telemetry(0) {
                    cell.write_consistent(&tel);
                }
                let ro = MappedShmRegion::open_existing_read_only(&shm_p);
                assert!(ro.is_ok());
                if let Ok(ref ro_reg) = ro {
                    let out = ro_reg.arena().read_telemetry(0, 4);
                    assert!(out.is_ok_and(|t| t.tranche1_harvested));
                }
            }
        }
        let _ = std::fs::remove_file(&shm_p);

        // 5. Endpoint URL parser supports wss://, ws://, and IPv6 bracketed hosts
        let wss = parse_endpoint_url("wss://stream.bybit.com/v5/private");
        assert!(wss.is_some_and(|e| e.use_tls && e.port == 443 && e.base_path == "/v5/private"));
        let v6 = parse_endpoint_url("http://[::1]:8787/api/state");
        assert!(v6.is_some_and(|e| !e.use_tls && e.host == "::1" && e.port == 8787 && e.host_header == "[::1]:8787"));
    }
}
