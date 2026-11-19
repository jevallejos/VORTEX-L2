//! VORTEX-OMEGA V5.4.2 Loopback Observatory & SSE Bridge Daemon (`127.0.0.1:8787`, Zero-Jitter Read-Only SHM v15).

use core::sync::atomic::{AtomicBool, Ordering};
use std::fs;
use std::io::Read;
use std::net::{IpAddr, Ipv4Addr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use vortex_rs::candle_store::{PerpetualCandleStore, Vkl1CandleRecord};
use vortex_rs::types::{ANSI_GREEN, ANSI_RESET, ANSI_SKY_BLUE};
use vortex_rs::web_server::{
    dispatch_http_request, serve_observatory_blocking, stream_sse_5hz_loop, VortexWebConfig,
    VortexWebContext, WebApiError, CSRF_HEADER_NAME, DEFAULT_WEB_PORT,
};

fn arg_value<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
    args.windows(2).find(|w| w[0] == flag).map(|w| w[1].as_str())
}

fn run_web_cli_self_test_gate() -> bool {
    let bad = VortexWebConfig {
        bind_ip: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        ..VortexWebConfig::default()
    };
    if bad.validate_loopback_only() != Err(WebApiError::NonLoopbackBindForbidden) {
        return false;
    }

    let base = std::env::temp_dir().join(format!("vortex_web_cli_gate_{}", std::process::id()));
    let _ = fs::remove_dir_all(&base);
    let cfg = VortexWebConfig {
        bind_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        port: 0,
        shm_path: base.join("offline_shm.mmap"),
        candle_dir: base.join("candles"),
        vault_dir: base.join("vault"),
        data_dir: base.join("data"),
        ui_html_path: PathBuf::from("ui/vortex_observatory.html"),
    };
    let ctx = Arc::new(VortexWebContext::new(cfg));
    let store = PerpetualCandleStore::new(&ctx.cfg.candle_dir);
    let rec = Vkl1CandleRecord {
        slot_idx: 0,
        tf_code: 1,
        open_ts_ns: 1_700_000_100_000_000_000,
        open_ticks: 652_000,
        high_ticks: 653_200,
        low_ticks: 651_800,
        close_ticks: 652_900,
        volume_lots: 85,
        ofi_mean_pm: 510,
        hurst_pm: 610,
        ob_bull_pm: 655,
        ob_bear_pm: 580,
        ..Vkl1CandleRecord::default()
    }
    .seal_with_checksum();

    let mut gate_ok = store.append_or_update(&rec).is_ok();
    let (c_root, _, html) = dispatch_http_request(&ctx, "GET", "/", &[], "");
    let (c_kl, _, kl) = dispatch_http_request(&ctx, "GET", "/api/candles?symbol=BTCUSDT&tf=1m&limit=5", &[], "");
    let (c_clip, _, clip) = dispatch_http_request(&ctx, "GET", "/api/clip/360", &[], "");
    let (c_deny, _, _) = dispatch_http_request(&ctx, "POST", "/api/cmd/drain", &[], "{\"enable\":true}");
    let hdr = vec![(CSRF_HEADER_NAME.to_string(), ctx.csrf_token.clone())];
    let (c_allow, _, _) = dispatch_http_request(&ctx, "POST", "/api/cmd/drain", &hdr, "{\"enable\":false}");
    gate_ok &= c_root == 200 && html.contains(&ctx.csrf_token);
    gate_ok &= c_kl == 200 && kl.contains("\"count\":1");
    gate_ok &= c_clip == 200 && !clip.contains("\u{1b}[");
    gate_ok &= c_deny == 403 && c_allow == 200;

    if let Ok(listener) = TcpListener::bind((IpAddr::V4(Ipv4Addr::LOCALHOST), 0)) {
        if let Ok(addr) = listener.local_addr() {
            let ctx_sse = Arc::clone(&ctx);
            let handle = thread::spawn(move || {
                if let Ok((mut srv_stream, _)) = listener.accept() {
                    let _ = stream_sse_5hz_loop(&ctx_sse, &mut srv_stream, Some(1));
                }
            });
            if let Ok(mut cli) = TcpStream::connect(addr) {
                let _ = cli.set_read_timeout(Some(Duration::from_secs(2)));
                let mut sse_buf = String::new();
                let _ = cli.read_to_string(&mut sse_buf);
                gate_ok &= sse_buf.contains("event: state");
            } else {
                gate_ok = false;
            }
            let _ = handle.join();
        }
    } else {
        gate_ok = false;
    }

    let _ = fs::remove_dir_all(&base);
    gate_ok
}

fn spawn_housekeeping_worker(cfg: VortexWebConfig, shutdown: Arc<AtomicBool>) {
    thread::spawn(move || {
        while !shutdown.load(Ordering::Acquire) {
            let _ = fs::create_dir_all(&cfg.candle_dir);
            let _ = fs::create_dir_all(&cfg.vault_dir);
            let _ = fs::create_dir_all(cfg.data_dir.join("archive"));
            for _ in 0..60 {
                if shutdown.load(Ordering::Acquire) {
                    return;
                }
                thread::sleep(Duration::from_secs(1));
            }
        }
    });
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let daemon_mode = args.iter().any(|a| a == "--daemon" || a == "--serve");
    if !daemon_mode {
        if !run_web_cli_self_test_gate() {
            return ExitCode::FAILURE;
        }
        println!(
            "{ANSI_GREEN}[VORTEX-WEB OMEGA V5.4.2] LOOPBACK 127.0.0.1:{DEFAULT_WEB_PORT}, X-VORTEX-CSRF, VKL1 & 5Hz SSE GATE OK{ANSI_RESET}"
        );
        return ExitCode::SUCCESS;
    }

    let mut cfg = VortexWebConfig::default();
    if let Some(p) = arg_value(&args, "--port").and_then(|s| s.parse::<u16>().ok()) {
        cfg.port = p;
    }
    if let Some(d) = arg_value(&args, "--data-dir") {
        cfg.data_dir = PathBuf::from(d);
    }
    if let Some(v) = arg_value(&args, "--vault-dir") {
        cfg.vault_dir = PathBuf::from(v);
    }
    if let Some(c) = arg_value(&args, "--candle-dir") {
        cfg.candle_dir = PathBuf::from(c);
    }
    if let Some(s) = arg_value(&args, "--shm") {
        cfg.shm_path = PathBuf::from(s);
    }
    if let Some(u) = arg_value(&args, "--ui") {
        cfg.ui_html_path = PathBuf::from(u);
    }

    let shutdown = Arc::new(AtomicBool::new(false));
    spawn_housekeeping_worker(cfg.clone(), Arc::clone(&shutdown));
    let port = cfg.port;
    let ctx = Arc::new(VortexWebContext::new(cfg));
    println!(
        "{ANSI_SKY_BLUE}[VORTEX-WEB OMEGA V5.4.2] Listening on http://127.0.0.1:{port} (Loopback Read-Only SHM v15, 5Hz SSE){ANSI_RESET}"
    );
    match serve_observatory_blocking(ctx, shutdown) {
        Ok(()) => ExitCode::SUCCESS,
        Err(_) => ExitCode::FAILURE,
    }
}
