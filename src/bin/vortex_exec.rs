//! VORTEX-OMEGA V5.5 Autonomous Execution CLI Wrapper (Delegates 200Hz Core to exec_daemon).
use std::process::ExitCode;

fn main() -> ExitCode {
    vortex_rs::vault_io::ensure_project_root_cwd_from_exe();
    vortex_rs::exec_daemon::run_exec_cli_entrypoint()
}
