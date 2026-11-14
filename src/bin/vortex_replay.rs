//! VORTEX-OMEGA Deterministic FIFO Queue Benchmark, 4-Arm Pareto Ablation & 1:1 Digital Twin CLI.

use std::path::Path;
use std::process::ExitCode;

use vortex_rs::replay_walkforward::{
    run_and_print_omega_chat6_twin_certification, run_parquet_walk_forward_on_dir,
};
use vortex_rs::types::{ANSI_GREEN, ANSI_RESET};

fn main() -> ExitCode {
    if !run_and_print_omega_chat6_twin_certification() {
        return ExitCode::FAILURE;
    }
    let args: Vec<String> = std::env::args().collect();
    let dir_arg = args
        .windows(2)
        .find(|w| w[0] == "--dir" || w[0] == "--parquet-dir")
        .map_or("data", |w| w[1].as_str());
    let data_path = Path::new(dir_arg);
    if data_path.exists() {
        let wf = run_parquet_walk_forward_on_dir(data_path, true);
        println!(
            "{}[PAR1 WALK-FORWARD ARCHIVE] Dir: {} | Files: {} | Events: {} | CumNet: {:+.2} bps{}",
            ANSI_GREEN,
            data_path.display(),
            wf.files_count,
            wf.total_events,
            (wf.cum_net_bps_x100 as f64) / 100.0,
            ANSI_RESET
        );
    }
    ExitCode::SUCCESS
}
