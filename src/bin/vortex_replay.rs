//! VORTEX-OMEGA V5.5 Deterministic 4-Arm Pareto, 1:1 PAR2 Digital Twin & Cold `.par2z` Replay CLI.
#![allow(clippy::cast_precision_loss)]

use std::path::Path;
use std::process::ExitCode;

use vortex_rs::replay_walkforward::{
    run_and_print_omega_chat6_twin_certification, run_par2z_archive_walk_forward,
    run_parquet_walk_forward_on_dir,
};

fn main() -> ExitCode {
    let caller_cwd = std::env::current_dir().ok();
    vortex_rs::vault_io::ensure_project_root_cwd_from_exe();
    if !run_and_print_omega_chat6_twin_certification() {
        return ExitCode::FAILURE;
    }
    let args: Vec<String> = std::env::args().collect();
    let mut dir = String::from("data");
    let mut explicit_dir = false;
    let mut archive_dir: Option<String> = None;
    let mut max_segments = 16_384_usize;
    let mut idx = 1_usize;
    while idx < args.len() {
        match args[idx].as_str() {
            "--dir" if idx + 1 < args.len() => {
                dir.clone_from(&args[idx + 1]);
                explicit_dir = true;
                idx += 2;
            }
            "--archive-dir" if idx + 1 < args.len() => {
                archive_dir = Some(args[idx + 1].clone());
                idx += 2;
            }
            "--max-segments" if idx + 1 < args.len() => {
                max_segments = args[idx + 1].parse::<usize>().unwrap_or(16_384).max(1);
                idx += 2;
            }
            _ => idx += 1,
        }
    }
    if let Some(arc) = archive_dir {
        let arc_path = match caller_cwd.as_ref() {
            Some(orig) if Path::new(&arc).is_relative() && (orig.join(&arc).exists() || !Path::new(&arc).exists()) => orig.join(&arc),
            _ => std::path::PathBuf::from(&arc),
        };
        let rep = run_par2z_archive_walk_forward(&arc_path, max_segments, true);
        let cum_bps = (rep.cum_net_bps_x100 as f64) / 100.0;
        println!(
            "[PAR2Z COLD WALK-FORWARD V5.5] Archive: {arc} | Files: {} | Events: {} | CumNet: {cum_bps:+.2} bps",
            rep.files_count, rep.total_events
        );
    } else {
        let dir_path = match caller_cwd.as_ref() {
            Some(orig) if explicit_dir && Path::new(&dir).is_relative() && (orig.join(&dir).exists() || !Path::new(&dir).exists()) => orig.join(&dir),
            _ => std::path::PathBuf::from(&dir),
        };
        let rep = run_parquet_walk_forward_on_dir(&dir_path, true);
        let cum_bps = (rep.cum_net_bps_x100 as f64) / 100.0;
        println!(
            "[PAR2 WALK-FORWARD ARCHIVE V5.5] Dir: {dir} | Files: {} | Events: {} | CumNet: {cum_bps:+.2} bps",
            rep.files_count, rep.total_events
        );
    }
    ExitCode::SUCCESS
}
