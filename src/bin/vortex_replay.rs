//! VORTEX-L3 Replay CLI Wrapper (Delegates FIFO & Walk-Forward Gate to replay_walkforward).

use std::process::ExitCode;

fn main() -> ExitCode {
    vortex_rs::replay_walkforward::run_replay_cli_gate()
}
