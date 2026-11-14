//! VORTEX-L3 Autonomous Execution CLI Wrapper (Delegates 200Hz Core to exec_daemon).

use std::process::ExitCode;

fn main() -> ExitCode {
    vortex_rs::exec_daemon::run_exec_cli_entrypoint()
}
