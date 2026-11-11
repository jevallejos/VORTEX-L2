//! VORTEX-L3 200Hz Autonomous 12-Slot Execution Daemon, Async Gateway & Executive HUD.
#![recursion_limit = "256"]
#![allow(
    clippy::manual_clamp,
    clippy::manual_range_contains,
    clippy::needless_range_loop,
    clippy::too_many_arguments
)]

use core::sync::atomic::Ordering;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::Path;
use std::process::ExitCode;
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread;
use std::time::Duration;

use vortex_rs::alpha_3factor::{OrthogonalFactorState, SignalVerdict};
use vortex_rs::execution_fsm::{
    check_queue_depletion_velocity_abort, defensive_zero_loss_maker_exit_price,
    dynamic_maker_floor_price, parse_verified_avg_price_ticks, sign_bybit_v5_hmac_sha256,
    ClosedPnlQueryOutcome, ClosedTradeKind, ClosedTradeSummary, ExitSubReason,
    OnlineAdaptiveGovernor, OrderLifecycleState, SlotExecutionEngine,
    VerifiedCloseResolution, MAX_CLOSE_RECONCILE_RETRIES,
};
use vortex_rs::forensic_parquet::ParitySyncLedger;
use vortex_rs::lob_shm::{
    MappedShmRegion, SymbolLobSnapshot, VortexShmArena, MAX_ACTIVE_PAIRS,
};
use vortex_rs::types::{
    classify_trade_badge, now_epoch_ns, print_auto_aligned_box, side_from_str,
    slot_from_symbol_name, LotQty, PairSpec, PortfolioBetaGuard, PriceTicks, ScaledBps,
    Side, TimestampNs, ANSI_BLUE, ANSI_DARK_GREEN, ANSI_GRAY, ANSI_GREEN, ANSI_ORANGE,
    ANSI_RED, ANSI_RESET, ANSI_SKY_BLUE, ANSI_YELLOW, APPROVED_SPECS,
};

const DEFAULT_WALLET_EQUITY_1E8: i64 = 6_628_080_000;
const FALLBACK_NOTIONAL_1E8: i64 = 1_922_140_000;
const MAX_HTTP_BODY_BYTES: usize = 2_097_152;
const GATEWAY_CMD_CAP: usize = 64;
const GATEWAY_EVT_CAP: usize = 256;
const TELEGRAM_QUEUE_CAP: usize = 64;

const AI_FORENSIC_VAULT_PATH: &str = "data/vault/vortex_ai_forensic_vault.jsonl";
const RED_LOSS_BOX_PATH: &str = "data/vault/vortex_red_loss_box.jsonl";
const SCAR_MEMORY_VAULT_PATH: &str = "data/vault/vortex_governor_scar_memory.jsonl";
const OPEN_POS_STATE_PATH: &str = "data/vault/vortex_open_positions_state.json";
const DRAIN_LOCK_PATH: &str = "data/vault/DRAIN_MODE.lock";
const HALT_LOCK_PATH: &str = "data/vault/HALTED_ON_LOSS.lock";

fn load_dotenv_map() -> HashMap<String, String> {
    let mut map = HashMap::new();
    for (k, v) in std::env::vars() {
        map.insert(k, v);
    }
    if let Ok(file) = File::open(".env") {
        let reader = BufReader::new(file);
        for line in reader.lines().map_while(Result::ok) {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let body = trimmed.strip_prefix("export ").unwrap_or(trimmed).trim();
            if let Some((k, v)) = body.split_once('=') {
                let clean_k = k.trim().to_string();
                let clean_v = v
                    .trim()
                    .trim_matches(|c: char| c as u32 == 34 || c as u32 == 39)
                    .to_string();
                map.entry(clean_k).or_insert(clean_v);
            }
        }
    }
    for alias in ["TELEGRAM_TOKEN", "BOT_TOKEN", "TG_BOT_TOKEN"] {
        if !map.contains_key("TELEGRAM_BOT_TOKEN") {
            if let Some(val) = map.get(alias).cloned() {
                if !val.is_empty() {
                    map.insert("TELEGRAM_BOT_TOKEN".to_string(), val);
                }
            }
        }
    }
    for alias in ["CHAT_ID", "TG_CHAT_ID"] {
        if !map.contains_key("TELEGRAM_CHAT_ID") {
            if let Some(val) = map.get(alias).cloned() {
                if !val.is_empty() {
                    map.insert("TELEGRAM_CHAT_ID".to_string(), val);
                }
            }
        }
    }
    map
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedEndpointUrl {
    pub use_tls: bool,
    pub host: String,
    pub host_header: String,
    pub port: u16,
    pub base_path: String,
}

#[must_use]
pub fn parse_endpoint_url(raw_url: &str) -> Option<ParsedEndpointUrl> {
    let trimmed = raw_url.trim().trim_end_matches('/');
    let (use_tls, rest, default_port) =
        if let Some(r) = trimmed.strip_prefix("https://") {
            (true, r, 443_u16)
        } else {
            let r = trimmed.strip_prefix("http://")?;
            (false, r, 80_u16)
        };
    let (authority, base_path) = match rest.find('/') {
        Some(idx) => (&rest[..idx], rest[idx..].trim_end_matches('/').to_string()),
        None => (rest, String::new()),
    };
    if authority.is_empty() {
        return None;
    }
    let (host_s, port) = match authority.rsplit_once(':') {
        Some((h, p_str)) => {
            let p = p_str.parse::<u16>().ok()?;
            (h, p)
        }
        None => (authority, default_port),
    };
    if host_s.is_empty() {
        return None;
    }
    let host = host_s.to_string();
    let host_header = if port == default_port {
        host.clone()
    } else {
        format!("{host}:{port}")
    };
    Some(ParsedEndpointUrl {
        use_tls,
        host,
        host_header,
        port,
        base_path,
    })
}

enum HttpTransportStream {
    Plain(TcpStream),
    #[cfg(target_os = "linux")]
    Tls(native_tls::TlsStream<TcpStream>),
}

impl std::fmt::Debug for HttpTransportStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plain(_) => f.write_str("HttpTransportStream::Plain"),
            #[cfg(target_os = "linux")]
            Self::Tls(_) => f.write_str("HttpTransportStream::Tls"),
        }
    }
}

impl HttpTransportStream {
    fn connect(ep: &ParsedEndpointUrl) -> Option<Self> {
        #[cfg(not(target_os = "linux"))]
        if ep.use_tls {
            return None;
        }
        let mut addrs = (ep.host.as_str(), ep.port).to_socket_addrs().ok()?;
        let addr = addrs.next()?;
        let tcp = TcpStream::connect_timeout(&addr, Duration::from_secs(3)).ok()?;
        let _ = tcp.set_nodelay(true);
        let _ = tcp.set_read_timeout(Some(Duration::from_secs(4)));
        let _ = tcp.set_write_timeout(Some(Duration::from_secs(4)));
        #[cfg(target_os = "linux")]
        if ep.use_tls {
            let connector = native_tls::TlsConnector::new().ok()?;
            let tls = connector.connect(&ep.host, tcp).ok()?;
            return Some(Self::Tls(tls));
        }
        Some(Self::Plain(tcp))
    }
}

impl Read for HttpTransportStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(s) => s.read(buf),
            #[cfg(target_os = "linux")]
            Self::Tls(s) => s.read(buf),
        }
    }
}

impl Write for HttpTransportStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(s) => s.write(buf),
            #[cfg(target_os = "linux")]
            Self::Tls(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Plain(s) => s.flush(),
            #[cfg(target_os = "linux")]
            Self::Tls(s) => s.flush(),
        }
    }
}

struct KeepAliveHttpConn {
    endpoint: ParsedEndpointUrl,
    reader: Option<BufReader<HttpTransportStream>>,
    req_buf: Vec<u8>,
    line_buf: String,
    body_buf: Vec<u8>,
}

impl std::fmt::Debug for KeepAliveHttpConn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeepAliveHttpConn")
            .field("endpoint", &self.endpoint)
            .field("connected", &self.reader.is_some())
            .finish_non_exhaustive()
    }
}

impl KeepAliveHttpConn {
    #[must_use]
    fn new(endpoint: ParsedEndpointUrl) -> Self {
        Self {
            endpoint,
            reader: None,
            req_buf: Vec::with_capacity(1_024),
            line_buf: String::with_capacity(256),
            body_buf: Vec::with_capacity(4_096),
        }
    }

    fn send_json_request(
        &mut self,
        method: &str,
        target_path_and_query: &str,
        extra_headers: &[(&str, &str)],
        body: &str,
    ) -> Option<&[u8]> {
        self.req_buf.clear();
        let _ = write!(
            self.req_buf,
            "{} {}{} HTTP/1.1\r\nHost: {}\r\nConnection: keep-alive\r\n",
            method,
            self.endpoint.base_path,
            target_path_and_query,
            self.endpoint.host_header
        );
        let _ = write!(
            self.req_buf,
            "User-Agent: vortex-l3/1.0\r\nAccept: application/json\r\n"
        );
        if !body.is_empty() || method == "POST" {
            let _ = write!(
                self.req_buf,
                "Content-Type: application/json\r\nContent-Length: {}\r\n",
                body.len()
            );
        }
        for &(hk, hv) in extra_headers {
            let _ = write!(self.req_buf, "{hk}: {hv}\r\n");
        }
        self.req_buf.extend_from_slice(b"\r\n");
        if !body.is_empty() {
            self.req_buf.extend_from_slice(body.as_bytes());
        }

        for attempt in 0..2_u8 {
            let was_reused = self.reader.is_some();
            if self.reader.is_none() {
                let stream = HttpTransportStream::connect(&self.endpoint)?;
                self.reader = Some(BufReader::new(stream));
            }
            let rdr = self.reader.as_mut()?;
            if rdr.get_mut().write_all(&self.req_buf).is_err()
                || rdr.get_mut().flush().is_err()
            {
                self.reader = None;
                if attempt == 0 && (was_reused || method == "GET") {
                    continue;
                }
                return None;
            }
            self.line_buf.clear();
            match rdr.read_line(&mut self.line_buf) {
                Ok(n) if n > 0 => {}
                _ => {
                    self.reader = None;
                    if attempt == 0 && (was_reused || method == "GET") {
                        continue;
                    }
                    return None;
                }
            }
            let status_ok = self
                .line_buf
                .split_whitespace()
                .nth(1)
                .and_then(|s| s.parse::<u16>().ok())
                .is_some_and(|code| (200..300).contains(&code));

            let mut content_len: Option<usize> = None;
            let mut is_chunked = false;
            let mut conn_close = false;
            loop {
                self.line_buf.clear();
                let Ok(n) = rdr.read_line(&mut self.line_buf) else {
                    self.reader = None;
                    return None;
                };
                if n == 0 || self.line_buf == "\r\n" || self.line_buf == "\n" {
                    break;
                }
                if let Some((k, v)) = self.line_buf.split_once(':') {
                    let kl = k.trim();
                    let vl = v.trim();
                    if kl.eq_ignore_ascii_case("content-length") {
                        content_len = vl.parse::<usize>().ok();
                    } else if kl.eq_ignore_ascii_case("transfer-encoding")
                        && vl.to_ascii_lowercase().contains("chunked")
                    {
                        is_chunked = true;
                    } else if kl.eq_ignore_ascii_case("connection")
                        && vl.to_ascii_lowercase().contains("close")
                    {
                        conn_close = true;
                    }
                }
            }

            self.body_buf.clear();
            if is_chunked {
                if !Self::read_chunked_into(rdr, &mut self.line_buf, &mut self.body_buf)
                {
                    self.reader = None;
                    return None;
                }
            } else if let Some(clen) = content_len {
                if clen > MAX_HTTP_BODY_BYTES {
                    self.reader = None;
                    return None;
                }
                self.body_buf.resize(clen, 0_u8);
                if clen > 0 && rdr.read_exact(&mut self.body_buf).is_err() {
                    self.reader = None;
                    return None;
                }
            } else {
                if rdr
                    .by_ref()
                    .take(MAX_HTTP_BODY_BYTES as u64)
                    .read_to_end(&mut self.body_buf)
                    .is_err()
                {
                    self.reader = None;
                    return None;
                }
                conn_close = true;
            }

            if conn_close || !status_ok {
                self.reader = None;
            }
            return if status_ok {
                Some(self.body_buf.as_slice())
            } else {
                None
            };
        }
        None
    }

    fn read_chunked_into(
        rdr: &mut BufReader<HttpTransportStream>,
        line_buf: &mut String,
        out: &mut Vec<u8>,
    ) -> bool {
        loop {
            line_buf.clear();
            let Ok(n) = rdr.read_line(line_buf) else {
                return false;
            };
            if n == 0 {
                return false;
            }
            let hex_part = line_buf.trim().split(';').next().unwrap_or("").trim();
            let Ok(chunk_len) = usize::from_str_radix(hex_part, 16) else {
                return false;
            };
            if chunk_len == 0 {
                loop {
                    line_buf.clear();
                    let Ok(tn) = rdr.read_line(line_buf) else {
                        return false;
                    };
                    if tn == 0 || line_buf == "\r\n" || line_buf == "\n" {
                        break;
                    }
                }
                return true;
            }
            if out.len().saturating_add(chunk_len) > MAX_HTTP_BODY_BYTES {
                return false;
            }
            let start = out.len();
            out.resize(start + chunk_len, 0_u8);
            if rdr.read_exact(&mut out[start..]).is_err() {
                return false;
            }
            let mut crlf = [0_u8; 2];
            if rdr.read_exact(&mut crlf).is_err() {
                return false;
            }
        }
    }
}

fn spawn_bounded_telegram_notifier() -> SyncSender<String> {
    let (tx, rx) = mpsc::sync_channel::<String>(TELEGRAM_QUEUE_CAP);
    let env_map = load_dotenv_map();
    let token = env_map.get("TELEGRAM_BOT_TOKEN").cloned().unwrap_or_default();
    let chat_id = env_map.get("TELEGRAM_CHAT_ID").cloned().unwrap_or_default();
    let tg_base = env_map
        .get("TELEGRAM_API_BASE")
        .cloned()
        .unwrap_or_else(|| "https://api.telegram.org".to_string());
    thread::spawn(move || {
        let mut conn_opt = parse_endpoint_url(&tg_base).map(KeepAliveHttpConn::new);
        let path = format!("/bot{}/sendMessage", token);
        while let Ok(msg) = rx.recv() {
            if token.is_empty() || chat_id.is_empty() {
                continue;
            }
            if let Some(ref mut conn) = conn_opt {
                let body = serde_json::json!({
                    "chat_id": chat_id,
                    "text": msg,
                })
                .to_string();
                let _ = conn.send_json_request("POST", &path, &[], &body);
            }
        }
    });
    tx
}

pub struct BybitV5LiveClient {
    api_key: String,
    api_secret: String,
    conn: KeepAliveHttpConn,
}

impl std::fmt::Debug for BybitV5LiveClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BybitV5LiveClient")
            .field("api_key_set", &(!self.api_key.is_empty()))
            .field("api_secret", &"[REDACTED]")
            .field("conn", &self.conn)
            .finish_non_exhaustive()
    }
}

impl BybitV5LiveClient {
    #[must_use]
    pub fn with_base_url(
        api_key: String,
        api_secret: String,
        base_url: &str,
    ) -> Option<Self> {
        if api_key.is_empty() || api_secret.is_empty() {
            return None;
        }
        let parsed = parse_endpoint_url(base_url)?;
        Some(Self {
            api_key,
            api_secret,
            conn: KeepAliveHttpConn::new(parsed),
        })
    }

    #[must_use]
    pub fn from_env_if_enabled(enabled: bool) -> Option<Self> {
        if !enabled {
            return None;
        }
        let env_map = load_dotenv_map();
        let api_key = env_map.get("BYBIT_API_KEY")?.clone();
        let api_secret = env_map.get("BYBIT_API_SECRET")?.clone();
        let base_url = env_map
            .get("BYBIT_REST_URL")
            .cloned()
            .unwrap_or_else(|| "https://api.bybit.com".to_string());
        Self::with_base_url(api_key, api_secret, &base_url)
    }

    fn now_ms_str() -> String {
        let ns = now_epoch_ns().0;
        format!("{}", (ns / 1_000_000).max(1))
    }

    pub fn get_v5(&mut self, endpoint: &str, query: &str) -> Option<serde_json::Value> {
        let ts = Self::now_ms_str();
        let recv_win = "10000";
        let prehash = format!("{}{}{}{}", ts, self.api_key, recv_win, query);
        let sign = sign_bybit_v5_hmac_sha256(&self.api_secret, &prehash);
        let target = if query.is_empty() {
            endpoint.to_string()
        } else {
            format!("{}?{}", endpoint, query)
        };
        let hdrs = [
            ("X-BAPI-API-KEY", self.api_key.as_str()),
            ("X-BAPI-TIMESTAMP", ts.as_str()),
            ("X-BAPI-RECV-WINDOW", recv_win),
            ("X-BAPI-SIGN", sign.as_str()),
        ];
        let raw = self.conn.send_json_request("GET", &target, &hdrs, "")?;
        serde_json::from_slice(raw).ok()
    }

    pub fn post_v5(
        &mut self,
        endpoint: &str,
        body_json: &str,
    ) -> Option<serde_json::Value> {
        let ts = Self::now_ms_str();
        let recv_win = "10000";
        let prehash = format!("{}{}{}{}", ts, self.api_key, recv_win, body_json);
        let sign = sign_bybit_v5_hmac_sha256(&self.api_secret, &prehash);
        let hdrs = [
            ("X-BAPI-API-KEY", self.api_key.as_str()),
            ("X-BAPI-TIMESTAMP", ts.as_str()),
            ("X-BAPI-RECV-WINDOW", recv_win),
            ("X-BAPI-SIGN", sign.as_str()),
        ];
        let raw = self.conn.send_json_request("POST", endpoint, &hdrs, body_json)?;
        serde_json::from_slice(raw).ok()
    }

    pub fn fetch_wallet_and_equity_usdt(&mut self) -> Option<(f64, f64, f64, f64)> {
        let q = "accountType=UNIFIED&coin=USDT";
        let val = self.get_v5("/v5/account/wallet-balance", q)?;
        if val.get("retCode").and_then(|c| c.as_i64()) != Some(0) {
            return None;
        }
        let acct = val.get("result")?.get("list")?.as_array()?.first()?;
        let w_usd = acct
            .get("totalWalletBalance")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let eq_usd = acct
            .get("totalEquity")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(w_usd);
        let av_usd = acct
            .get("totalAvailableBalance")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(eq_usd);
        let up_usd = acct
            .get("totalPerpUPL")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);

        if let Some(coins) = acct.get("coin").and_then(|c| c.as_array()) {
            for c_obj in coins {
                let c_name = c_obj.get("coin").and_then(|v| v.as_str()).unwrap_or("");
                if c_name == "USDT" {
                    let w_c = c_obj
                        .get("walletBalance")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);
                    if w_c > 0.0 {
                        let up_c = c_obj
                            .get("unrealisedPnl")
                            .and_then(|v| v.as_str())
                            .and_then(|s| s.parse::<f64>().ok())
                            .unwrap_or(up_usd);
                        let eq_c = c_obj
                            .get("equity")
                            .and_then(|v| v.as_str())
                            .and_then(|s| s.parse::<f64>().ok())
                            .unwrap_or(w_c + up_c);
                        let pos_im = c_obj
                            .get("totalPositionIM")
                            .and_then(|v| v.as_str())
                            .and_then(|s| s.parse::<f64>().ok())
                            .unwrap_or(0.0);
                        let ord_im = c_obj
                            .get("totalOrderIM")
                            .and_then(|v| v.as_str())
                            .and_then(|s| s.parse::<f64>().ok())
                            .unwrap_or(0.0);
                        let lck = c_obj
                            .get("locked")
                            .and_then(|v| v.as_str())
                            .and_then(|s| s.parse::<f64>().ok())
                            .unwrap_or(0.0);
                        let av_c = (eq_c - pos_im - ord_im - lck).max(0.0);
                        let w_final = w_c.max(w_usd);
                        let eq_final = eq_c.max(eq_usd);
                        let av_final = if av_c > 0.0 { av_c } else { av_usd };
                        return Some((w_final, eq_final, av_final, up_c));
                    }
                }
            }
        }
        if w_usd > 0.0 {
            Some((w_usd, eq_usd, av_usd, up_usd))
        } else {
            None
        }
    }

    pub fn get_resting_exit_order_info(
        &mut self,
        spec: PairSpec,
    ) -> Option<(PriceTicks, LotQty)> {
        let sym = spec.symbol_id.name();
        let q = format!("category=linear&symbol={}", sym);
        let val = self.get_v5("/v5/order/realtime", &q)?;
        if val.get("retCode").and_then(|c| c.as_i64()) != Some(0) {
            return None;
        }
        let list = val
            .get("result")
            .and_then(|r| r.get("list"))
            .and_then(|l| l.as_array())?;
        for item in list {
            let ro = item
                .get("reduceOnly")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if ro {
                let px_s = item.get("price").and_then(|v| v.as_str()).unwrap_or("0");
                let qt_s = item.get("qty").and_then(|v| v.as_str()).unwrap_or("0");
                if let Ok(pt) = spec.parse_price_ticks(px_s) {
                    let lq = spec.parse_lot_qty(qt_s);
                    return Some((pt, lq));
                }
            }
        }
        Some((PriceTicks(0), LotQty(0)))
    }

    pub fn query_open_position_verified(
        &mut self,
        symbol: &str,
    ) -> Option<(bool, String, Side, String)> {
        let q = format!("category=linear&symbol={}", symbol);
        let val = self.get_v5("/v5/position/list", &q)?;
        if val.get("retCode").and_then(|c| c.as_i64()) != Some(0) {
            return None;
        }
        let list = val
            .get("result")
            .and_then(|r| r.get("list"))
            .and_then(|l| l.as_array())?;
        for pos in list {
            let sz_s = pos.get("size").and_then(|v| v.as_str()).unwrap_or("0");
            let side_s = pos.get("side").and_then(|v| v.as_str()).unwrap_or("Buy");
            let avg_s = pos.get("avgPrice").and_then(|v| v.as_str()).unwrap_or("0");
            let Ok(sz_f) = sz_s.parse::<f64>() else {
                return None;
            };
            let side = if side_s == "Sell" { Side::Ask } else { Side::Bid };
            if sz_f > 0.0 {
                return Some((true, sz_s.to_string(), side, avg_s.to_string()));
            }
        }
        Some((false, "0".to_string(), Side::Bid, "0".to_string()))
    }

    pub fn query_closed_pnl_raw(
        &mut self,
        symbol: &str,
        start_time_ms: i64,
    ) -> Option<serde_json::Value> {
        let q = if start_time_ms > 0 {
            format!(
                "category=linear&symbol={}&startTime={}&limit=20",
                symbol, start_time_ms
            )
        } else {
            format!("category=linear&symbol={}&limit=20", symbol)
        };
        self.get_v5("/v5/position/closed-pnl", &q)
    }

    pub fn cancel_all_symbol_orders(&mut self, symbol: &str) -> bool {
        let body = serde_json::json!({
            "category": "linear",
            "symbol": symbol,
            "orderFilter": "Order",
        })
        .to_string();
        self.post_v5("/v5/order/cancel-all", &body)
            .and_then(|v| v.get("retCode")?.as_i64())
            .is_some_and(|rc| rc == 0)
    }

    /// F-06 Fix: Enforces server-side MarkPrice SL and verifies retCode == 0 || 34040.
    pub fn set_position_trading_stop(
        &mut self,
        spec: PairSpec,
        sl_ticks: PriceTicks,
    ) -> bool {
        if sl_ticks.0 <= 0 {
            return false;
        }
        let (sw, sf) = spec.format_price_usdt_parts(sl_ticks);
        let sl_str = format!("{}.{:04}", sw, sf);
        let body = serde_json::json!({
            "category": "linear",
            "symbol": spec.symbol_id.name(),
            "stopLoss": sl_str,
            "slTriggerBy": "MarkPrice",
            "tpslMode": "Full",
            "positionIdx": 0,
        })
        .to_string();
        self.post_v5("/v5/position/trading-stop", &body)
            .and_then(|v| v.get("retCode")?.as_i64())
            .is_some_and(|rc| rc == 0 || rc == 34_040)
    }

    pub fn place_post_only_order(
        &mut self,
        spec: PairSpec,
        order_side: Side,
        price: PriceTicks,
        qty: LotQty,
        sl_price: Option<PriceTicks>,
        reduce_only: bool,
    ) -> i64 {
        let sym = spec.symbol_id.name();
        let side_str = match order_side {
            Side::Bid => "Buy",
            Side::Ask => "Sell",
        };
        let (pw, pf) = spec.format_price_usdt_parts(price);
        let px_str = format!("{}.{:04}", pw, pf);
        let qty_str = spec.format_lot_qty_str(qty);
        let mut payload = serde_json::json!({
            "category": "linear",
            "symbol": sym,
            "side": side_str,
            "orderType": "Limit",
            "qty": qty_str,
            "price": px_str,
            "timeInForce": "PostOnly",
            "reduceOnly": reduce_only,
        });
        if let Some(sl_t) = sl_price {
            if !reduce_only && sl_t.0 > 0 {
                let (sw, sf) = spec.format_price_usdt_parts(sl_t);
                if let Some(obj) = payload.as_object_mut() {
                    obj.insert(
                        "stopLoss".to_string(),
                        serde_json::Value::String(format!("{}.{:04}", sw, sf)),
                    );
                    obj.insert(
                        "slTriggerBy".to_string(),
                        serde_json::Value::String("MarkPrice".to_string()),
                    );
                }
            }
        }
        let Some(resp) = self.post_v5("/v5/order/create", &payload.to_string()) else {
            return -1;
        };
        resp.get("retCode").and_then(|c| c.as_i64()).unwrap_or(-1)
    }
}

#[derive(Copy, Clone, Debug)]
pub enum GatewayCommand {
    SyncWallet,
    PlaceEntry {
        slot_idx: usize,
        side: Side,
        entry_price: PriceTicks,
        qty: LotQty,
        sl_price: PriceTicks,
    },
    RepegEntry {
        slot_idx: usize,
        side: Side,
        new_price: PriceTicks,
        qty: LotQty,
        new_sl_price: PriceTicks,
    },
    AbortEntry {
        slot_idx: usize,
        side: Side,
        aborted_price: PriceTicks,
        sl_hint: PriceTicks,
    },
    PollEntryFill {
        slot_idx: usize,
        sl_hint: PriceTicks,
    },
    EnforceStopLoss {
        slot_idx: usize,
        sl_price: PriceTicks,
    },
    SyncOpenSlot {
        slot_idx: usize,
        side: Side,
        target_px: PriceTicks,
        target_lots: LotQty,
        defensive_lock: bool,
        cur_gross_bps: i64,
        vac_level: usize,
        opened_ts_ns: i64,
        tranche1_harvested: bool,
    },
}

#[derive(Clone, Debug)]
pub enum GatewayEvent {
    WalletUpdated {
        wallet_1e8: i64,
    },
    EntryPlaced {
        slot_idx: usize,
        accepted: bool,
        entry_price: PriceTicks,
        sl_price: PriceTicks,
    },
    EntryRepegged {
        slot_idx: usize,
        accepted: bool,
        new_price: PriceTicks,
        new_sl_price: PriceTicks,
    },
    EntryAborted {
        slot_idx: usize,
        side: Side,
        aborted_price: PriceTicks,
    },
    EntryFilledLive {
        slot_idx: usize,
        real_side: Side,
        verified_avg: Option<PriceTicks>,
        real_lots: LotQty,
        sl_verified: bool,
        tag: &'static str,
    },
    EntryStillResting {
        slot_idx: usize,
    },
    StopLossSynced {
        slot_idx: usize,
        sl_price: PriceTicks,
        verified: bool,
    },
    SlotStillOpenSynced {
        slot_idx: usize,
        real_side: Side,
        verified_avg: Option<PriceTicks>,
        real_lots: LotQty,
        active_resting_tp: PriceTicks,
    },
    SlotClosedDetected {
        slot_idx: usize,
        vac_level: usize,
        closed_pnl_payload: Option<serde_json::Value>,
    },
    SlotSyncSkipped {
        slot_idx: usize,
    },
    WsPositionUpdate {
        slot_idx: usize,
        is_open: bool,
        real_side: Side,
        verified_avg: Option<PriceTicks>,
        real_lots: LotQty,
    },
}

/// Parses Bybit V5 private WS JSON frames (cross-platform testable).
pub fn parse_ws_private_frame(txt: &str, evt_tx: &SyncSender<GatewayEvent>) {
    let Ok(val) = serde_json::from_str::<serde_json::Value>(txt) else {
        return;
    };
    let Some(topic) = val.get("topic").and_then(|t| t.as_str()) else {
        return;
    };
    if topic == "wallet" {
        if let Some(arr) = val.get("data").and_then(|d| d.as_array()) {
            if let Some(first) = arr.first() {
                let w_f = first
                    .get("totalWalletBalance")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                if w_f > 0.0 {
                    let wallet_1e8 = (w_f * 100_000_000.0).round() as i64;
                    let _ = evt_tx.try_send(GatewayEvent::WalletUpdated { wallet_1e8 });
                }
            }
        }
    } else if topic == "position" {
        let Some(arr) = val.get("data").and_then(|d| d.as_array()) else {
            return;
        };
        for item in arr {
            let sym = item.get("symbol").and_then(|v| v.as_str()).unwrap_or("");
            let Some(slot_idx) = slot_from_symbol_name(sym) else {
                continue;
            };
            let spec = APPROVED_SPECS[slot_idx];
            let sz_s = item.get("size").and_then(|v| v.as_str()).unwrap_or("0");
            let Ok(sz_f) = sz_s.parse::<f64>() else {
                continue;
            };
            let side_s = item.get("side").and_then(|v| v.as_str()).unwrap_or("Buy");
            let real_side = if side_s == "Sell" { Side::Ask } else { Side::Bid };
            if sz_f > 0.0 {
                let avg_s = item.get("avgPrice").and_then(|v| v.as_str()).unwrap_or("0");
                let verified_avg =
                    parse_verified_avg_price_ticks(spec, real_side, avg_s).ok();
                let real_lots = spec.parse_lot_qty(sz_s).max(LotQty(1));
                let _ = evt_tx.try_send(GatewayEvent::WsPositionUpdate {
                    slot_idx,
                    is_open: true,
                    real_side,
                    verified_avg,
                    real_lots,
                });
            } else {
                let _ = evt_tx.try_send(GatewayEvent::WsPositionUpdate {
                    slot_idx,
                    is_open: false,
                    real_side,
                    verified_avg: None,
                    real_lots: LotQty(0),
                });
            }
        }
    }
}

fn process_gateway_command(
    client: &mut BybitV5LiveClient,
    cmd: GatewayCommand,
    evt_tx: &SyncSender<GatewayEvent>,
) {
    match cmd {
        GatewayCommand::SyncWallet => {
            if let Some((w_f, _, _, _)) = client.fetch_wallet_and_equity_usdt() {
                if w_f > 0.0 {
                    let wallet_1e8 = (w_f * 100_000_000.0).round() as i64;
                    let _ = evt_tx.try_send(GatewayEvent::WalletUpdated { wallet_1e8 });
                }
            }
        }
        GatewayCommand::PlaceEntry {
            slot_idx,
            side,
            entry_price,
            qty,
            sl_price,
        } => {
            let spec = APPROVED_SPECS[slot_idx];
            let rc = client.place_post_only_order(
                spec,
                side,
                entry_price,
                qty,
                Some(sl_price),
                false,
            );
            let _ = evt_tx.try_send(GatewayEvent::EntryPlaced {
                slot_idx,
                accepted: rc == 0,
                entry_price,
                sl_price,
            });
        }
        GatewayCommand::RepegEntry {
            slot_idx,
            side,
            new_price,
            qty,
            new_sl_price,
        } => {
            let spec = APPROVED_SPECS[slot_idx];
            let sym = spec.symbol_id.name();
            let _ = client.cancel_all_symbol_orders(sym);
            if let Some((true, sz_s, r_side, avg_s)) =
                client.query_open_position_verified(sym)
            {
                let ver_avg = parse_verified_avg_price_ticks(spec, r_side, &avg_s).ok();
                let r_lots = spec.parse_lot_qty(&sz_s).max(LotQty(1));
                let sl_ok = client.set_position_trading_stop(spec, new_sl_price);
                let _ = evt_tx.try_send(GatewayEvent::EntryFilledLive {
                    slot_idx,
                    real_side: r_side,
                    verified_avg: ver_avg,
                    real_lots: r_lots,
                    sl_verified: sl_ok,
                    tag: "MAKER FILL REPEG",
                });
                return;
            }
            let rc = client.place_post_only_order(
                spec,
                side,
                new_price,
                qty,
                Some(new_sl_price),
                false,
            );
            let _ = evt_tx.try_send(GatewayEvent::EntryRepegged {
                slot_idx,
                accepted: rc == 0,
                new_price,
                new_sl_price,
            });
        }
        GatewayCommand::AbortEntry {
            slot_idx,
            side,
            aborted_price,
            sl_hint,
        } => {
            let spec = APPROVED_SPECS[slot_idx];
            let sym = spec.symbol_id.name();
            let _ = client.cancel_all_symbol_orders(sym);
            if let Some((true, sz_s, r_side, avg_s)) =
                client.query_open_position_verified(sym)
            {
                let ver_avg = parse_verified_avg_price_ticks(spec, r_side, &avg_s).ok();
                let r_lots = spec.parse_lot_qty(&sz_s).max(LotQty(1));
                let sl_ok = client.set_position_trading_stop(spec, sl_hint);
                let _ = evt_tx.try_send(GatewayEvent::EntryFilledLive {
                    slot_idx,
                    real_side: r_side,
                    verified_avg: ver_avg,
                    real_lots: r_lots,
                    sl_verified: sl_ok,
                    tag: "MAKER FILL ABORT",
                });
                return;
            }
            let _ = evt_tx.try_send(GatewayEvent::EntryAborted {
                slot_idx,
                side,
                aborted_price,
            });
        }
        GatewayCommand::PollEntryFill { slot_idx, sl_hint } => {
            let spec = APPROVED_SPECS[slot_idx];
            let sym = spec.symbol_id.name();
            match client.query_open_position_verified(sym) {
                Some((true, sz_s, r_side, avg_s)) => {
                    let ver_avg =
                        parse_verified_avg_price_ticks(spec, r_side, &avg_s).ok();
                    let r_lots = spec.parse_lot_qty(&sz_s).max(LotQty(1));
                    let sl_ok = client.set_position_trading_stop(spec, sl_hint);
                    let _ = evt_tx.try_send(GatewayEvent::EntryFilledLive {
                        slot_idx,
                        real_side: r_side,
                        verified_avg: ver_avg,
                        real_lots: r_lots,
                        sl_verified: sl_ok,
                        tag: "MAKER FILL",
                    });
                }
                _ => {
                    let _ = evt_tx.try_send(GatewayEvent::EntryStillResting { slot_idx });
                }
            }
        }
        GatewayCommand::EnforceStopLoss { slot_idx, sl_price } => {
            let spec = APPROVED_SPECS[slot_idx];
            let verified = client.set_position_trading_stop(spec, sl_price);
            let _ = evt_tx.try_send(GatewayEvent::StopLossSynced {
                slot_idx,
                sl_price,
                verified,
            });
        }
        GatewayCommand::SyncOpenSlot {
            slot_idx,
            side,
            target_px,
            target_lots,
            defensive_lock,
            cur_gross_bps,
            vac_level,
            opened_ts_ns,
            tranche1_harvested,
        } => {
            let spec = APPROVED_SPECS[slot_idx];
            let sym = spec.symbol_id.name();
            let Some((mut still_open, mut sz_s, mut r_side, mut avg_s)) =
                client.query_open_position_verified(sym)
            else {
                let _ = evt_tx.try_send(GatewayEvent::SlotSyncSkipped { slot_idx });
                return;
            };
            if !still_open && cur_gross_bps < 0 {
                match client.query_open_position_verified(sym) {
                    Some((open2, sz2, s2, a2)) => {
                        still_open = open2;
                        sz_s = sz2;
                        r_side = s2;
                        avg_s = a2;
                    }
                    None => {
                        let _ =
                            evt_tx.try_send(GatewayEvent::SlotSyncSkipped { slot_idx });
                        return;
                    }
                }
            }
            if !still_open {
                let _ = client.cancel_all_symbol_orders(sym);
                let opened_ms = (opened_ts_ns / 1_000_000).max(0);
                let start_ms = if tranche1_harvested {
                    opened_ms.saturating_sub(86_400_000)
                } else {
                    opened_ms.saturating_sub(5_000)
                };
                let closed_pnl_payload = client.query_closed_pnl_raw(sym, start_ms);
                let _ = evt_tx.try_send(GatewayEvent::SlotClosedDetected {
                    slot_idx,
                    vac_level,
                    closed_pnl_payload,
                });
                return;
            }
            let verified_avg = parse_verified_avg_price_ticks(spec, r_side, &avg_s).ok();
            let real_lots = spec.parse_lot_qty(&sz_s).max(LotQty(1));
            let Some((resting_tp, resting_qty)) = client.get_resting_exit_order_info(spec)
            else {
                let _ = evt_tx.try_send(GatewayEvent::SlotSyncSkipped { slot_idx });
                return;
            };
            let eff_lots = LotQty(target_lots.0.min(real_lots.0).max(1));
            let tp_diff = (target_px.0 - resting_tp.0).abs();
            let moving_closer = match side {
                Side::Bid => resting_tp.0 == 0 || target_px.0 < resting_tp.0,
                Side::Ask => resting_tp.0 == 0 || target_px.0 > resting_tp.0,
            };
            let should_repeg = resting_tp.0 == 0
                || resting_qty != eff_lots
                || (moving_closer && tp_diff >= 1 && (defensive_lock || tp_diff >= 2))
                || (!moving_closer && cur_gross_bps < 200 && tp_diff >= 4);
            let active_resting_tp = if should_repeg {
                let _ = client.cancel_all_symbol_orders(sym);
                let rc = client.place_post_only_order(
                    spec,
                    side.opposite(),
                    target_px,
                    eff_lots,
                    None,
                    true,
                );
                if rc == 0 { target_px } else { PriceTicks(0) }
            } else {
                resting_tp
            };
            let _ = evt_tx.try_send(GatewayEvent::SlotStillOpenSynced {
                slot_idx,
                real_side: r_side,
                verified_avg,
                real_lots,
                active_resting_tp,
            });
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn spawn_private_ws_listener(_evt_tx: SyncSender<GatewayEvent>) {}

#[cfg(target_os = "linux")]
fn spawn_private_ws_listener(evt_tx: SyncSender<GatewayEvent>) {
    let env_map = load_dotenv_map();
    let api_key = env_map.get("BYBIT_API_KEY").cloned().unwrap_or_default();
    let api_secret = env_map.get("BYBIT_API_SECRET").cloned().unwrap_or_default();
    if api_key.is_empty() || api_secret.is_empty() {
        return;
    }
    let ws_url = env_map
        .get("BYBIT_PRIVATE_WS_URL")
        .cloned()
        .unwrap_or_else(|| "wss://stream.bybit.com/v5/private".to_string());
    thread::spawn(move || loop {
        if let Ok((mut socket, _)) = tungstenite::connect(ws_url.as_str()) {
            let tcp_opt = match socket.get_mut() {
                tungstenite::stream::MaybeTlsStream::Plain(s) => Some(s),
                tungstenite::stream::MaybeTlsStream::NativeTls(tls) => Some(tls.get_mut()),
                _ => None,
            };
            if let Some(tcp) = tcp_opt {
                let _ = tcp.set_nodelay(true);
                let _ = tcp.set_read_timeout(Some(Duration::from_secs(10)));
                let _ = tcp.set_write_timeout(Some(Duration::from_secs(5)));
            }
            let expires_ms = (now_epoch_ns().0 / 1_000_000).max(1) + 10_000;
            let sig = sign_bybit_v5_hmac_sha256(
                &api_secret,
                &format!("GET/realtime{}", expires_ms),
            );
            let auth_msg = serde_json::json!({
                "op": "auth",
                "args": [api_key, expires_ms, sig],
            })
            .to_string();
            let sub_msg = "{\"op\":\"subscribe\",\"args\":[\"position\",\"wallet\"]}"
                .to_string();
            if socket.send(tungstenite::Message::Text(auth_msg)).is_err()
                || socket.send(tungstenite::Message::Text(sub_msg)).is_err()
            {
                thread::sleep(Duration::from_millis(500));
                continue;
            }
            let mut last_ping_ns = now_epoch_ns().0;
            while let Ok(msg) = socket.read() {
                let now_ns = now_epoch_ns().0;
                if now_ns.saturating_sub(last_ping_ns) >= 15_000_000_000 {
                    if socket
                        .send(tungstenite::Message::Text("{\"op\":\"ping\"}".to_string()))
                        .is_err()
                    {
                        break;
                    }
                    last_ping_ns = now_ns;
                }
                match msg {
                    tungstenite::Message::Text(t) => {
                        parse_ws_private_frame(&t, &evt_tx);
                    }
                    tungstenite::Message::Ping(p) => {
                        let _ = socket.send(tungstenite::Message::Pong(p));
                    }
                    tungstenite::Message::Close(_) => break,
                    _ => {}
                }
            }
        }
        thread::sleep(Duration::from_millis(500));
    });
}

#[derive(Debug)]
pub struct BybitAsyncGateway {
    cmd_tx: SyncSender<GatewayCommand>,
    evt_rx: Receiver<GatewayEvent>,
}

impl BybitAsyncGateway {
    #[must_use]
    pub fn spawn_if_enabled(enabled: bool) -> Option<Self> {
        let mut client = BybitV5LiveClient::from_env_if_enabled(enabled)?;
        let (cmd_tx, cmd_rx) = mpsc::sync_channel::<GatewayCommand>(GATEWAY_CMD_CAP);
        let (evt_tx, evt_rx) = mpsc::sync_channel::<GatewayEvent>(GATEWAY_EVT_CAP);
        let evt_ws = evt_tx.clone();
        thread::spawn(move || {
            while let Ok(cmd) = cmd_rx.recv() {
                process_gateway_command(&mut client, cmd, &evt_tx);
            }
        });
        spawn_private_ws_listener(evt_ws);
        Some(Self { cmd_tx, evt_rx })
    }

    #[inline]
    pub fn try_send(&self, cmd: GatewayCommand) -> bool {
        self.cmd_tx.try_send(cmd).is_ok()
    }

    #[inline]
    pub fn try_recv(&self) -> Option<GatewayEvent> {
        self.evt_rx.try_recv().ok()
    }
}
fn save_open_positions_state(engines: &[SlotExecutionEngine; MAX_ACTIVE_PAIRS]) {
    let _ = fs::create_dir_all("data/vault");
    let mut arr = Vec::new();
    for eng in engines {
        if let OrderLifecycleState::PositionOpenMakerExit {
            side,
            entry_price,
            avg_price_ticks,
            peak_favorable_price,
            worst_adverse_price,
            initial_lots,
            open_lots,
            tranche1_harvested,
            tranche1_lots,
            tranche1_exit_price,
            defensive_floor_locked,
            structural_tp_price,
            resting_exit_price,
            sl_trigger_price,
            opened_ts,
            ..
        } = eng.state
        {
            arr.push(serde_json::json!({
                "slot_idx": eng.slot_idx,
                "side": if matches!(side, Side::Ask) { "Ask" } else { "Bid" },
                "entry_ticks": entry_price.0,
                "avg_ticks": avg_price_ticks.0,
                "peak_ticks": peak_favorable_price.0,
                "worst_ticks": worst_adverse_price.0,
                "initial_lots": initial_lots.0,
                "open_lots": open_lots.0,
                "tranche1_harvested": tranche1_harvested,
                "tranche1_lots": tranche1_lots.0,
                "tranche1_exit_ticks": tranche1_exit_price.0,
                "defensive_floor_locked": defensive_floor_locked,
                "structural_tp_ticks": structural_tp_price.0,
                "tp_ticks": resting_exit_price.0,
                "sl_ticks": sl_trigger_price.0,
                "opened_ts_ns": opened_ts.0,
            }));
        }
    }
    if let Ok(txt) = serde_json::to_string_pretty(&arr) {
        let _ = fs::write(OPEN_POS_STATE_PATH, txt);
    }
}

fn load_saved_slot_state(slot_idx: usize) -> Option<serde_json::Value> {
    let txt = fs::read_to_string(OPEN_POS_STATE_PATH).ok()?;
    let arr = serde_json::from_str::<Vec<serde_json::Value>>(&txt).ok()?;
    arr.into_iter()
        .find(|v| v.get("slot_idx").and_then(|s| s.as_u64()) == Some(slot_idx as u64))
}

fn append_governor_scar_snapshot(gov: &OnlineAdaptiveGovernor, ts_ns: TimestampNs) {
    let _ = fs::create_dir_all("data/vault");
    let snap_line = gov.to_scar_snapshot_json(ts_ns).to_string();
    if let Ok(mut f) = OpenOptions::new()
        .create(true)
        .append(true)
        .open(SCAR_MEMORY_VAULT_PATH)
    {
        let _ = writeln!(f, "{}", snap_line);
    }
}

fn extract_vault_row_pnl_and_fee_1e8(
    v: &serde_json::Value,
    s_idx_opt: Option<usize>,
    side: Side,
    net_sc: ScaledBps,
    fee_sc: ScaledBps,
) -> (i64, i64, PriceTicks) {
    let en_t = PriceTicks(v.get("entry_ticks").and_then(|n| n.as_i64()).unwrap_or(0));
    let ex_t = PriceTicks(v.get("exit_ticks").and_then(|n| n.as_i64()).unwrap_or(0));
    if let (Some(pnl), Some(fee)) = (
        v.get("net_pnl_1e8").and_then(|n| n.as_i64()),
        v.get("fee_1e8").and_then(|n| n.as_i64()),
    ) {
        return (pnl, fee, en_t);
    }
    if let Some(s_idx) = s_idx_opt {
        if en_t.0 > 0 && ex_t.0 > 0 {
            let spec = APPROVED_SPECS[s_idx];
            let lots_raw = v.get("closed_lots").and_then(|n| n.as_i64()).unwrap_or(0);
            let lots = if lots_raw > 0 {
                LotQty(lots_raw)
            } else {
                spec.compute_compounding_kelly_lots(
                    DEFAULT_WALLET_EQUITY_1E8,
                    en_t,
                    200,
                    false,
                )
            };
            let (pnl_1e8, fee_1e8, _, _) =
                spec.compute_leg_pnl_and_fee_1e8(side, lots, en_t, ex_t, fee_sc);
            return (pnl_1e8, fee_1e8, en_t);
        }
    }
    let pnl_fb = (FALLBACK_NOTIONAL_1E8.saturating_mul(net_sc.raw())) / 1_000_000;
    let fee_fb = (FALLBACK_NOTIONAL_1E8.saturating_mul(fee_sc.raw())) / 1_000_000;
    (pnl_fb, fee_fb, en_t)
}

fn pad_visible_right(s: &str, target_w: usize) -> String {
    let w = vortex_rs::types::visible_terminal_width(s);
    let pad = target_w.saturating_sub(w);
    format!("{}{}", s, " ".repeat(pad))
}

fn fmt_3col(c1: String, c2: String, c3: String) -> String {
    format!(
        "   \u{2022} {} \u{2502} {} \u{2502} {}",
        pad_visible_right(&c1, 58),
        pad_visible_right(&c2, 41),
        c3
    )
}

fn format_telegram_close_message(
    spec: PairSpec,
    summary: &ClosedTradeSummary,
    sub_reason: ExitSubReason,
    trade_num: u64,
    ledger: &ParitySyncLedger,
) -> String {
    let sym_name = spec.symbol_id.name();
    let net_bps_f = (summary.net_scaled_bps.raw() as f64) / 100.0;
    let gross_f = (summary.gross_scaled_bps.raw() as f64) / 100.0;
    let fee_f = (summary.fee_scaled_bps.raw() as f64) / 100.0;
    let (en_w, en_f) = spec.format_price_usdt_parts(summary.entry_price);
    let (ex_w, ex_f) = spec.format_price_usdt_parts(summary.exit_price);
    let pnl_usdt = (summary.net_pnl_1e8 as f64) / 100_000_000.0;
    let qty_s = spec.format_lot_qty_str(summary.closed_lots);
    let is_partial = matches!(sub_reason, ExitSubReason::Tranche1Harvest50);
    let (_, icon, lbl) = classify_trade_badge(net_bps_f, is_partial);
    format!(
        concat!(
            "{} [VORTEX-L3 {} #{} \u{2014} {:?}]\n",
            "- Ganancia : {:+.2}% @10x | {:+.4} USDT\n",
            "- Par y Lado : {} ({:?}) qty={} ({}L) | Maker: {}\n",
            "- Precios    : {}.{:04} -> {}.{:04} USDT ({}t -> {}t)\n",
            "- Balance    : Bruto: {:+.2} bps | Fee: -{:.2} bps | ",
            "Neto: {:+.2} bps\n",
            "- Boveda 70/30: Reserva 30%: {:+.2} bps | Blindado 70%: {:+.2} bps"
        ),
        icon,
        lbl.trim(),
        trade_num,
        sub_reason,
        net_bps_f / 10.0,
        pnl_usdt,
        sym_name,
        summary.side,
        qty_s,
        summary.legs_consolidated,
        summary.is_maker_exit,
        en_w,
        en_f,
        ex_w,
        ex_f,
        summary.entry_price.0,
        summary.exit_price.0,
        gross_f,
        fee_f,
        net_bps_f,
        (ledger.sinking_fund_reserve_bps_x100 as f64) / 100.0,
        (ledger.vault_locked_profit_bps_x100 as f64) / 100.0
    )
}

fn format_position_duration(secs: u64) -> String {
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3_600 {
        format!("{}m {:02}s", secs / 60, secs % 60)
    } else {
        let h = secs / 3_600;
        let m = (secs % 3_600) / 60;
        let s = secs % 60;
        format!("{}h {:02}m {:02}s", h, m, s)
    }
}

/// F-09 Fix: Tear-free HUD using SlotExecTelemetry::read_consistent_or_default.
fn render_live_hud(arena: &VortexShmArena) {
    let now_ns = now_epoch_ns().0.max(0) as u64;
    let hb_ns = arena.heartbeat_ns.load(Ordering::Acquire);
    let hb_age_ms = if hb_ns > 0 && now_ns >= hb_ns {
        (now_ns - hb_ns) / 1_000_000
    } else {
        0
    };
    let drain_active = Path::new(DRAIN_LOCK_PATH).exists();
    let sec_title = vec![format!(
        concat!(
            "\u{1f4e1} [VORTEX-L3 LIVE HUD] SHM v{}: /dev/shm/vortex_lob.mmap ",
            "\u{2502} Heartbeat Age: {} ms"
        ),
        arena.version, hb_age_ms
    )];
    let sec_hdr = vec![concat!(
        "SLOT  \u{2502} SYMBOL     \u{2502}      PyG (UPL) \u{2502} POSITION TIME \u{2502} ",
        "      SEQ_ID \u{2502}     BEST_BID \u{2502}     BEST_ASK \u{2502} SPR_T \u{2502} ",
        " X_OFI \u{2502}  X_TFI \u{2502} X_GEOM \u{2502} VERDICT           "
    )
    .to_string()];

    let mut sec_rows = Vec::with_capacity(MAX_ACTIVE_PAIRS);
    let mut open_slots_count = 0_usize;
    let mut queued_slots_count = 0_usize;
    let btc_snap = arena
        .slot(0)
        .and_then(|s| s.read_snapshot(4))
        .unwrap_or_else(|_| SymbolLobSnapshot::new_empty());

    for s_idx in 0..MAX_ACTIVE_PAIRS {
        let spec = APPROVED_SPECS[s_idx];
        let snap = arena
            .slot(s_idx)
            .and_then(|s| s.read_snapshot(4))
            .unwrap_or_else(|_| SymbolLobSnapshot::new_empty());
        let tel_snap = arena
            .telemetry(s_idx)
            .map(|t| t.read_consistent_or_default(4))
            .unwrap_or_default();
        let tel_state = tel_snap.state_code;
        let is_slot_open = (3..=5).contains(&tel_state);
        let is_slot_queued = tel_state == 1 || tel_state == 2;
        let mut pyg_col_cell = format!("{}{:>14}{}", ANSI_GRAY, "-", ANSI_RESET);
        let mut pos_time_cell = format!("{}{:>13}{}", ANSI_GRAY, "-", ANSI_RESET);

        if is_slot_open {
            open_slots_count += 1;
            let is_ask = tel_snap.side_code == 1;
            let avg_t = tel_snap.avg_price_ticks;
            let op_l = tel_snap.open_lots;
            let cur_t = if snap.mark_price.0 > 0 {
                snap.mark_price.0
            } else if snap.bids[0].price.0 > 0 && snap.asks[0].price.0 > 0 {
                i64::midpoint(snap.bids[0].price.0, snap.asks[0].price.0)
            } else {
                avg_t
            };
            let diff_t = if is_ask { avg_t - cur_t } else { cur_t - avg_t };
            let unit_usdt = ((spec.tick_size_1e8 as f64) / 100_000_000.0)
                * ((spec.lot_step_1e8 as f64) / 100_000_000.0);
            let upl_usdt = (diff_t as f64) * (op_l as f64) * unit_usdt;
            let upl_pct = ((diff_t as f64) / (avg_t.max(1) as f64)) * 1_000.0;
            let pyg_raw = format!("{:+.2}({:+.1}%)", upl_usdt, upl_pct);
            let gross_bps_live = (diff_t.saturating_mul(1_000_000)) / avg_t.max(1);
            let pyg_col = if gross_bps_live >= 440 {
                ANSI_GREEN
            } else if diff_t >= 0 {
                ANSI_YELLOW
            } else {
                ANSI_RED
            };
            pyg_col_cell = format!("{}{:>14}{}", pyg_col, pyg_raw, ANSI_RESET);
            let saved_open_ns = load_saved_slot_state(s_idx)
                .and_then(|v| v.get("opened_ts_ns")?.as_u64())
                .unwrap_or(0);
            let open_ns = if saved_open_ns > 0 {
                saved_open_ns
            } else {
                tel_snap.resting_since_ns
            };
            let hold_secs = if open_ns > 0 && now_ns >= open_ns {
                (now_ns - open_ns) / 1_000_000_000
            } else {
                0
            };
            let dur_str = format_position_duration(hold_secs);
            pos_time_cell = format!("{}{:>13}{}", ANSI_YELLOW, dur_str, ANSI_RESET);
        } else if is_slot_queued {
            queued_slots_count += 1;
            pyg_col_cell = format!("{}{:>14}{}", ANSI_SKY_BLUE, "IN_QUEUE_L1", ANSI_RESET);
            let q_ns = tel_snap.resting_since_ns;
            let q_secs = if q_ns > 0 && now_ns >= q_ns {
                (now_ns - q_ns) / 1_000_000_000
            } else {
                0
            };
            let q_str = format!("Q: {}s", q_secs);
            pos_time_cell = format!("{}{:>13}{}", ANSI_SKY_BLUE, q_str, ANSI_RESET);
        }

        let slot_col = if is_slot_open {
            format!("{}\u{1f7e1} {:2}{}", ANSI_YELLOW, s_idx, ANSI_RESET)
        } else if is_slot_queued {
            format!("{}\u{1fa75} {:2}{}", ANSI_SKY_BLUE, s_idx, ANSI_RESET)
        } else {
            format!("   {:2}", s_idx)
        };
        let (bw, bf) = spec.format_price_usdt_parts(snap.bids[0].price);
        let (aw, af) = spec.format_price_usdt_parts(snap.asks[0].price);
        let spr = (snap.asks[0].price.0 - snap.bids[0].price.0).max(0);
        let verdict_str = match OrthogonalFactorState::evaluate_entry_signal_with_leader(
            spec, &snap, &btc_snap,
        ) {
            SignalVerdict::SpoofingVeto => {
                format!("{}\u{1f7e0} SPOOF_VETO     {}", ANSI_ORANGE, ANSI_RESET)
            }
            SignalVerdict::AuthorizeMakerEntry { side: Side::Bid, .. } => {
                format!("{}\u{1f7e1} AUTH_LONG_MKR  {}", ANSI_YELLOW, ANSI_RESET)
            }
            SignalVerdict::AuthorizeMakerEntry { side: Side::Ask, .. } => {
                format!("{}\u{1f7e1} AUTH_SHORT_MKR {}", ANSI_YELLOW, ANSI_RESET)
            }
            SignalVerdict::NeutralNoAction => {
                format!("{}\u{26aa} NEUTRAL_WAIT   {}", ANSI_GRAY, ANSI_RESET)
            }
        };

        sec_rows.push(format!(
            concat!(
                "{} \u{2502} {:<10} \u{2502} {} \u{2502} {} \u{2502} {:>12} \u{2502} ",
                "{:>7}.{:04} \u{2502} {:>7}.{:04} \u{2502} {:>5} \u{2502} ",
                "{:>+6.2} \u{2502} {:>+6.2} \u{2502} {:>+6.2} \u{2502} {}"
            ),
            slot_col,
            spec.symbol_id.name(),
            pyg_col_cell,
            pos_time_cell,
            snap.sequence_id,
            bw,
            bf,
            aw,
            af,
            spr,
            snap.factor_ofi.get(),
            snap.factor_tfi.get(),
            snap.factor_geom.get(),
            verdict_str
        ));
    }

    let state_banner = if drain_active {
        format!(
            "{}\u{1f7e2} DRAINING_OPEN_SESSIONS ({} open){}",
            ANSI_GREEN, open_slots_count, ANSI_RESET
        )
    } else if open_slots_count > 0 || queued_slots_count > 0 {
        format!(
            "{}\u{1f7e1} ACTIVE_MULTI_SLOT ({} open / {} queued){}",
            ANSI_YELLOW, open_slots_count, queued_slots_count, ANSI_RESET
        )
    } else {
        format!("{}\u{26aa} IDLE (SCANNING 12 SLOTS){}", ANSI_GRAY, ANSI_RESET)
    };

    let sec_footer = vec![format!(
        concat!(
            "[EXEC FSM LIVE] State: {} \u{2502} [FLOW] ",
            "\u{1f7e1}Arm:{:<3} \u{1f7e0}Abr:{:<3} \u{1f7e1}Fil:{:<3} \u{1f7e2}Cls:{:<3}"
        ),
        pad_visible_right(&state_banner, 45),
        arena.exec_armed_count.load(Ordering::Acquire),
        arena.exec_abort_count.load(Ordering::Acquire),
        arena.exec_fill_count.load(Ordering::Acquire),
        arena.exec_closed_count.load(Ordering::Acquire)
    )];
    print_auto_aligned_box(&[sec_title, sec_hdr, sec_rows, sec_footer]);
}

/// F-05 Fix: Executive Dashboard using exact executed lot PnL & monotonic 70% Vault.
fn render_trades_and_executive_dashboard() {
    let mut rows_out = Vec::new();
    let mut total = 0_usize;
    let mut wins = 0_usize;
    let mut partials = 0_usize;
    let mut scratches = 0_usize;
    let mut losses = 0_usize;
    let mut maker_closes = 0_usize;
    let mut cum_gross = 0.0_f64;
    let mut cum_fee = 0.0_f64;
    let mut cum_net = 0.0_f64;
    let mut sum_mfe = 0.0_f64;
    let mut sum_mae = 0.0_f64;
    let mut sum_hold_s = 0.0_f64;
    let mut dash_ledger = ParitySyncLedger::new(DEFAULT_WALLET_EQUITY_1E8);

    if let Ok(file) = File::open(AI_FORENSIC_VAULT_PATH) {
        let reader = BufReader::new(file);
        for line in reader.lines().map_while(Result::ok) {
            let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) else {
                continue;
            };
            if v.get("event").and_then(|e| e.as_str()) != Some("TRADE_CLOSE") {
                continue;
            }
            total += 1;
            let sym = v.get("symbol").and_then(|s| s.as_str()).unwrap_or("UNKNOWN");
            let side_s = v.get("side").and_then(|s| s.as_str()).unwrap_or("Bid");
            let en_t = v.get("entry_ticks").and_then(|n| n.as_i64()).unwrap_or(0);
            let ex_t = v.get("exit_ticks").and_then(|n| n.as_i64()).unwrap_or(0);
            let g_bps = v.get("gross_bps").and_then(|n| n.as_f64()).unwrap_or(0.0);
            let f_bps = v.get("fee_bps").and_then(|n| n.as_f64()).unwrap_or(4.0);
            let n_bps = v.get("net_bps").and_then(|n| n.as_f64()).unwrap_or(0.0);
            let mfe = v.get("mfe_bps").and_then(|n| n.as_f64()).unwrap_or(g_bps);
            let mae = v.get("mae_bps").and_then(|n| n.as_f64()).unwrap_or(0.0);
            let hold_ms = v.get("hold_ms").and_then(|n| n.as_i64()).unwrap_or(0);
            let sub_r = v
                .get("exit_sub_reason")
                .and_then(|s| s.as_str())
                .unwrap_or("MakerExit");
            let mkr = v.get("maker_exit").and_then(|b| b.as_bool()).unwrap_or(true);
            if mkr {
                maker_closes += 1;
            }
            let is_partial =
                sub_r.contains("Tranche1Harvest50") || sub_r.contains("Partial");
            if is_partial {
                partials += 1;
            } else if n_bps >= 1.00 {
                wins += 1;
            } else if n_bps >= -0.50 {
                scratches += 1;
            } else {
                losses += 1;
            }
            cum_gross += g_bps;
            cum_fee += f_bps;
            cum_net += n_bps;
            sum_mfe += mfe;
            sum_mae += mae;
            sum_hold_s += (hold_ms as f64) / 1_000.0;

            let net_sc = ScaledBps((n_bps * 100.0).round() as i64);
            let fee_sc = ScaledBps((f_bps * 100.0).round() as i64);
            let s_opt = slot_from_symbol_name(sym);
            let side = side_from_str(side_s);
            let (pnl_1e8, fee_1e8, _) =
                extract_vault_row_pnl_and_fee_1e8(&v, s_opt, side, net_sc, fee_sc);
            dash_ledger.record_closed_trade_with_sinking_split(pnl_1e8, fee_1e8, net_sc);

            let (col, icon, lbl) = classify_trade_badge(n_bps, is_partial);
            rows_out.push(format!(
                concat!(
                    "{}{} [TRADE #{:<3} {}] {:<10} {:<3} \u{2502} {:>7}t->{:<7}t \u{2502} ",
                    "G:{:>+6.2} F:{:>4.2} N:{:>+6.2}bp ({:>+5.2}%@10x) \u{2502} ",
                    "MFE:{:>+6.2} MAE:{:>+6.2} \u{2502} {:>4}s \u{2502} {:<21} (Mkr={}){}"
                ),
                col,
                icon,
                total,
                lbl,
                sym,
                side_s,
                en_t,
                ex_t,
                g_bps,
                f_bps,
                n_bps,
                n_bps / 10.0,
                mfe,
                mae,
                hold_ms / 1_000,
                sub_r,
                mkr,
                ANSI_RESET
            ));
        }
    }
    if rows_out.is_empty() {
        rows_out.push(
            "  \u{26aa} [LEDGER EMPTY] Waiting for first closed trade in VORTEX-L3..."
                .to_string(),
        );
    }

    let mut live_client = BybitV5LiveClient::from_env_if_enabled(true);
    let fallback_w = (DEFAULT_WALLET_EQUITY_1E8 as f64) / 100_000_000.0;
    let (w_usdt, eq_usdt, av_usdt, upl_usdt) = live_client
        .as_mut()
        .and_then(BybitV5LiveClient::fetch_wallet_and_equity_usdt)
        .unwrap_or((fallback_w, fallback_w, fallback_w, 0.0));

    let eff_notional_usdt = (w_usdt * 0.29).max(18.0);
    let bps_to_usdt = eff_notional_usdt / 10_000.0;
    let reserve_bps = (dash_ledger.sinking_fund_reserve_bps_x100 as f64) / 100.0;
    let locked_bps = (dash_ledger.vault_locked_profit_bps_x100 as f64) / 100.0;
    let net_usdt = (dash_ledger.cumulative_net_pnl_1e8 as f64) / 100_000_000.0;
    let fee_usdt = (dash_ledger.cumulative_fees_1e8 as f64) / 100_000_000.0;
    let gross_usdt = net_usdt + fee_usdt;

    let trade_goal: usize = if total < 25 {
        25
    } else if total < 50 {
        50
    } else if total < 100 {
        100
    } else if total < 250 {
        250
    } else if total < 500 {
        500
    } else if total < 1_000 {
        1_000
    } else {
        ((total / 500) + 1) * 500
    };
    let prog_25 = total.min(trade_goal);
    let pct_25 = ((prog_25 as f64) / (trade_goal as f64)) * 100.0;
    let bars_25 = ((pct_25 / 5.0).round() as usize).min(20);
    let bar_25_str = format!(
        "{}{}",
        "\u{2588}".repeat(bars_25),
        "\u{2591}".repeat(20 - bars_25)
    );
    let rem_25 = trade_goal.saturating_sub(prog_25);
    let goal_lbl = format!("Meta {} Tiros", trade_goal);
    let faltan_lbl = format!("Faltan Meta {}", trade_goal);
    let mkr_pct = if total > 0 {
        (maker_closes as f64 * 100.0) / (total as f64)
    } else {
        100.0
    };

    let goal_anchor_usdt = w_usdt.max(eq_usdt);
    let goal_pct = ((goal_anchor_usdt / 95.0) * 100.0).clamp(0.0, 100.0);
    let filled_bars = ((goal_pct / 5.0).round() as usize).min(20);
    let bar_95 = format!(
        "{}{}",
        "\u{2588}".repeat(filled_bars),
        "\u{2591}".repeat(20 - filled_bars)
    );
    let rem_95 = (95.0 - goal_anchor_usdt).max(0.0);

    let sec_summary = vec![format!(
        concat!(
            "{}\u{1f7e2} [LEDGER SUMMARY] Total: {} (\u{1f7e2} Wins: {} \u{2502} ",
            "{}\u{1f332} Partials: {}{} \u{2502} {}\u{1fa75} Scratches: {}{} \u{2502} ",
            "{}\u{1f534} Losses: {}{}) \u{2502} CumNet: {:+.2} bps ({:+.4} USDT){}"
        ),
        ANSI_GREEN,
        total,
        wins,
        ANSI_DARK_GREEN,
        partials,
        ANSI_GREEN,
        ANSI_SKY_BLUE,
        scratches,
        ANSI_GREEN,
        ANSI_RED,
        losses,
        ANSI_GREEN,
        cum_net,
        net_usdt,
        ANSI_RESET
    )];

    let sec_dash_hdr = vec![format!(
        concat!(
            "{}\u{1f48e} VORTEX-L3 EXECUTIVE DASHBOARD \u{2014} ",
            "CAPITAL REAL (BYBIT UTA V5 LIVE) & DATOS CONSOLIDADOS{}"
        ),
        ANSI_BLUE, ANSI_RESET
    )];
    let upl_col = if upl_usdt < 0.0 { ANSI_RED } else { ANSI_GREEN };

    let sec_wallet = vec![
        "\u{1f4b0} [1. DINERO REAL EN BILLETERA Y GESTION DE MARGEN (USDT)]".to_string(),
        fmt_3col(
            format!(
                "{}Saldo Billetera (Wallet) : {:>10.4} USDT{}",
                ANSI_GREEN, w_usdt, ANSI_RESET
            ),
            format!(
                "{}Equity Total Vivo     : {:>10.4} USDT{}",
                ANSI_YELLOW, eq_usdt, ANSI_RESET
            ),
            "Estado Cuenta  : ACTIVA (10x)".to_string(),
        ),
        fmt_3col(
            format!("Margen Disponible Libre  : {:>10.4} USDT", av_usdt),
            format!(
                "{}PnL Abierto (UPL)     : {:>+10.4} USDT{}",
                upl_col, upl_usdt, ANSI_RESET
            ),
            "Beta Cap       : 3B / 3A (Max 6)".to_string(),
        ),
        fmt_3col(
            format!("Nocional Compuesto (10x) : {:>10.4} USDT", eff_notional_usdt),
            format!("Margen Base / Tiro    : {:>10.4} USDT", eff_notional_usdt * 0.10),
            "Fraccion Kelly : 29.0% Wallet".to_string(),
        ),
    ];

    let denom_t = (total.max(1)) as f64;
    let sec_perf = vec![
        "\u{1f4c8} [2. PROGRESO DE RACHA Y RENDIMIENTO CONSOLIDADO DE SESION]"
            .to_string(),
        fmt_3col(
            format!(
                "{:<15}: [{}] {:>3}/{} ({:>5.1}%)",
                goal_lbl, bar_25_str, prog_25, trade_goal, pct_25
            ),
            format!("{:<22}: {:>10} tiros", faltan_lbl, rem_25),
            format!("Salidas Maker  : {:>6.1}%", mkr_pct),
        ),
        fmt_3col(
            format!(
                "Meta 95 USDT   : [{}] {:>5.2}/95 ({:>4.1}%)",
                bar_95, goal_anchor_usdt, goal_pct
            ),
            format!("Faltan Meta 95        : {:>10.4} USDT", rem_95),
            "Anclaje Meta   : Wallet Floor".to_string(),
        ),
        fmt_3col(
            format!("Balance (bps)  : Bruto: {:>+10.2} bps", cum_gross),
            format!("Comisiones (Fees)     : {:>+10.2} bps", -cum_fee),
            format!(
                "Neto Sesion    : {:>+7.2} bps ({:+.2}% @10x)",
                cum_net,
                cum_net / 10.0
            ),
        ),
        fmt_3col(
            format!("Balance (USDT) : Bruto: {:>+10.4} USDT", gross_usdt),
            format!("Comisiones (USDT)     : {:>+10.4} USDT", -fee_usdt),
            format!("Neto Exacto    : {:>+7.4} USDT", net_usdt),
        ),
        fmt_3col(
            format!(
                "Fondo Saneam.  : Reserva(30%): {:>+6.2} bps (+{:.4}$)",
                reserve_bps,
                reserve_bps * bps_to_usdt
            ),
            format!("Blindado Boveda (70%) : {:>+10.2} bps", locked_bps),
            format!("Blindado USDT  : +{:>.4} USDT", locked_bps * bps_to_usdt),
        ),
        fmt_3col(
            format!("Excursiones    : MFE Medio: {:>+8.2} bps", sum_mfe / denom_t),
            format!("MAE Medio             : {:>+10.2} bps", sum_mae / denom_t),
            format!("Custodia Media : {:>6.1} s", sum_hold_s / denom_t),
        ),
        format!(
            concat!(
                "   \u{2022} Conteo Tiros   : Total: {:<3} \u{2502} ",
                "{}\u{1f7e2} WIN: {:<3} ({:>5.1}%){} \u{2502} ",
                "{}\u{1f332} PART: {:<3} ({:>5.1}%){} \u{2502} ",
                "{}\u{1fa75} SCR: {:<3} ({:>5.1}%){} \u{2502} ",
                "{}\u{1f534} LOSS: {:<3} ({:>5.1}%){}"
            ),
            total,
            ANSI_GREEN,
            wins,
            (wins as f64 * 100.0) / denom_t,
            ANSI_RESET,
            ANSI_DARK_GREEN,
            partials,
            (partials as f64 * 100.0) / denom_t,
            ANSI_RESET,
            ANSI_SKY_BLUE,
            scratches,
            (scratches as f64 * 100.0) / denom_t,
            ANSI_RESET,
            ANSI_RED,
            losses,
            (losses as f64 * 100.0) / denom_t,
            ANSI_RESET
        ),
    ];
    print_auto_aligned_box(&[rows_out, sec_summary, sec_dash_hdr, sec_wallet, sec_perf]);
}
fn verify_ws_private_parser_self_check() -> bool {
    let (tx, rx) = mpsc::sync_channel::<GatewayEvent>(4);
    let w_frame =
        "{\"topic\":\"wallet\",\"data\":[{\"totalWalletBalance\":\"66.2808\"}]}";
    let p_frame = concat!(
        "{\"topic\":\"position\",\"data\":[{\"symbol\":\"AVAXUSDT\",",
        "\"side\":\"Buy\",\"size\":\"2.6\",\"avgPrice\":\"8.204\"}]}"
    );
    parse_ws_private_frame(w_frame, &tx);
    parse_ws_private_frame(p_frame, &tx);
    let ok_w = matches!(
        rx.try_recv(),
        Ok(GatewayEvent::WalletUpdated { wallet_1e8 }) if wallet_1e8 == 6_628_080_000
    );
    let ok_p = matches!(
        rx.try_recv(),
        Ok(GatewayEvent::WsPositionUpdate {
            slot_idx: 3,
            is_open: true,
            real_side: Side::Bid,
            verified_avg: Some(PriceTicks(8_204)),
            real_lots: LotQty(26),
        })
    );
    ok_w && ok_p
}

pub fn run_oos_520_certification_suite() -> (u64, f64, f64, f64, i64) {
    use vortex_rs::lob_shm::BookLevel;
    use vortex_rs::types::BoundedScore;

    if !verify_ws_private_parser_self_check() {
        return (0, 0.0, 0.0, 0.0, 1);
    }

    let mut engines = SlotExecutionEngine::new_12_slot_array();
    let mut gov = OnlineAdaptiveGovernor::new();
    let mut ledger = ParitySyncLedger::new(DEFAULT_WALLET_EQUITY_1E8);
    let sample_px: [i64; MAX_ACTIVE_PAIRS] = [
        759_000, 223_700, 8_930, 8_170, 22_400, 8_510,
        3_130, 16_140, 1_650, 10_720, 3_690, 5_176,
    ];

    let mut ts = 1_700_000_000_000_000_000_i64;
    let mut closed_total = 0_u64;
    let mut maker_total = 0_u64;
    let mut profit_1e8 = 0_i64;
    let mut loss_1e8 = 0_i64;
    let mut sum_trade_pnl_1e8 = 0_i64;
    let mut step = 0_usize;

    while closed_total < 520 && step < 6_000 {
        let s_idx = step % MAX_ACTIVE_PAIRS;
        step += 1;
        ts += 3_600_000_000_000;
        let spec = APPROVED_SPECS[s_idx];
        let base_px = sample_px[s_idx];

        let mut btc_snap = SymbolLobSnapshot::new_empty();
        btc_snap.symbol_id = APPROVED_SPECS[0].symbol_id;
        btc_snap.is_eligible = true;
        btc_snap.ts_local_ns = TimestampNs(ts);
        btc_snap.realized_vol_5m = ScaledBps(8_200);
        btc_snap.bids[0] = BookLevel { price: PriceTicks(759_000), qty: LotQty(450) };
        btc_snap.asks[0] = BookLevel { price: PriceTicks(759_001), qty: LotQty(350) };
        btc_snap.mark_price = PriceTicks(759_000);
        btc_snap.last_trade_price = PriceTicks(759_000);
        btc_snap.factor_ofi = BoundedScore::new_clamped(0.86);
        btc_snap.factor_tfi = BoundedScore::new_clamped(0.88);
        btc_snap.factor_geom = BoundedScore::new_clamped(0.58);

        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = spec.symbol_id;
        snap.is_eligible = true;
        snap.ts_local_ns = TimestampNs(ts);
        snap.realized_vol_5m = ScaledBps(8_200);
        snap.bids[0] = BookLevel { price: PriceTicks(base_px), qty: LotQty(450) };
        snap.asks[0] = BookLevel { price: PriceTicks(base_px + 1), qty: LotQty(350) };
        snap.mark_price = PriceTicks(base_px);
        snap.last_trade_price = PriceTicks(base_px);
        snap.factor_ofi = BoundedScore::new_clamped(0.86);
        snap.factor_tfi = BoundedScore::new_clamped(0.88);
        snap.factor_geom = BoundedScore::new_clamped(0.58);

        let verdict = OrthogonalFactorState::evaluate_entry_signal_with_leader(
            spec, &snap, &btc_snap,
        );
        let SignalVerdict::AuthorizeMakerEntry { side, .. } = verdict else {
            continue;
        };
        let mut beta_guard = PortfolioBetaGuard::new();
        for eng in &engines {
            if let Some(act_s) = eng.active_side() {
                beta_guard.observe_active_side(act_s);
            }
        }
        let eq_1e8 = ledger.current_equity_1e8();
        if !beta_guard.can_admit_side(side)
            || !gov.authorizes_adaptive_entry_with_wallet(
                s_idx, side, eq_1e8, &snap, &btc_snap,
            )
        {
            continue;
        }
        let (edge_bps, _, _) = gov.slot_direction_diagnostics(s_idx, side);
        let in_debt = gov.is_in_scar_debt(s_idx, side);
        let en_px = side.select_entry_bbo_price(snap.bids[0].price, snap.asks[0].price);
        let lots = spec.compute_compounding_kelly_lots(eq_1e8, en_px, edge_bps, in_debt);
        if engines[s_idx].try_arm_maker_entry(&snap, verdict, lots).is_err() {
            continue;
        }
        let _ = engines[s_idx].on_maker_entry_filled(TimestampNs(ts + 25_000_000));
        snap.ts_local_ns = TimestampNs(ts + 12_000_000_000);

        if closed_total % 85 == 42 {
            let sl_px = engines[s_idx].last_eff_sl_ticks.max(PriceTicks(1));
            snap.mark_price = sl_px;
            snap.last_trade_price = sl_px;
        } else {
            let q_fl = defensive_zero_loss_maker_exit_price(side, en_px);
            snap.bids[0] = BookLevel { price: PriceTicks(q_fl.0 - 1), qty: LotQty(450) };
            snap.asks[0] = BookLevel { price: q_fl, qty: LotQty(200) };
            snap.mark_price = q_fl;
            snap.last_trade_price = q_fl;
            if s_idx >= 1 && lots.0 >= 2 {
                let _ = engines[s_idx]
                    .step_open_position(&snap, &btc_snap, &gov, &mut ledger);
            }
            snap.ts_local_ns = TimestampNs(ts + 25_000_000_000);
            let step_t = ((base_px * 18) / 10_000).max(5);
            for lvl in 0..4 {
                let ask_p = q_fl.0 + 1 + (lvl as i64) * step_t;
                snap.bids[lvl] = BookLevel {
                    price: PriceTicks(q_fl.0 - (lvl as i64)),
                    qty: LotQty(380),
                };
                snap.asks[lvl] = BookLevel { price: PriceTicks(ask_p), qty: LotQty(160) };
            }
            snap.last_trade_price = snap.asks[3].price;
            snap.mark_price = snap.asks[3].price;
        }

        if let Some(summary) =
            engines[s_idx].step_open_position(&snap, &btc_snap, &gov, &mut ledger)
        {
            closed_total += 1;
            if summary.is_maker_exit {
                maker_total += 1;
            }
            if summary.net_pnl_1e8 >= 0 {
                profit_1e8 += summary.net_pnl_1e8;
            } else {
                loss_1e8 += summary.net_pnl_1e8.abs();
            }
            sum_trade_pnl_1e8 += summary.net_pnl_1e8;
            gov.observe_closed_trade_at_entry(
                s_idx,
                summary.side,
                summary.entry_price,
                summary.net_scaled_bps,
                summary.is_maker_exit,
                snap.ts_local_ns,
            );
        }
    }

    let maker_ratio = (maker_total as f64 * 100.0) / (closed_total.max(1) as f64);
    let pf = (profit_1e8 as f64) / (loss_1e8.max(1) as f64);
    let cum_usdt = (ledger.cumulative_net_pnl_1e8 as f64) / 100_000_000.0;
    let drift = (ledger.cumulative_net_pnl_1e8 - sum_trade_pnl_1e8).abs();
    (closed_total, maker_ratio, pf, cum_usdt, drift)
}

/// F-05 & F-07 Fix: Rehydrates 100% of vault trades with real timestamps + scar memory.
fn rehydrate_shadow_ledger(
    ledger: &mut ParitySyncLedger,
    engines: &mut [SlotExecutionEngine; MAX_ACTIVE_PAIRS],
    gov: &mut OnlineAdaptiveGovernor,
    arena: &VortexShmArena,
) {
    let mut scar_snap_ts_ns = 0_i64;
    if let Ok(s_file) = File::open(SCAR_MEMORY_VAULT_PATH) {
        let reader = BufReader::new(s_file);
        let mut latest_scar: Option<serde_json::Value> = None;
        for line in reader.lines().map_while(Result::ok) {
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(&line) {
                if val.get("event").and_then(|v| v.as_str()) == Some("GOV_SCAR_SNAPSHOT") {
                    latest_scar = Some(val);
                }
            }
        }
        if let Some(ref sc_val) = latest_scar {
            if let Some(ts_restored) = gov.apply_scar_snapshot_json(sc_val) {
                scar_snap_ts_ns = ts_restored.0;
            }
        }
    }

    let mut l3_vault_count = 0_u64;
    if let Ok(file) = File::open(AI_FORENSIC_VAULT_PATH) {
        let reader = BufReader::new(file);
        for line in reader.lines().map_while(Result::ok) {
            let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) else {
                continue;
            };
            if v.get("event").and_then(|e| e.as_str()) != Some("TRADE_CLOSE") {
                continue;
            }
            l3_vault_count += 1;
            let sym = v.get("symbol").and_then(|s| s.as_str()).unwrap_or("");
            let side_s = v.get("side").and_then(|s| s.as_str()).unwrap_or("Bid");
            let net_f = v.get("net_bps").and_then(|n| n.as_f64()).unwrap_or(0.0);
            let fee_f = v.get("fee_bps").and_then(|n| n.as_f64()).unwrap_or(4.0);
            let mkr = v.get("maker_exit").and_then(|b| b.as_bool()).unwrap_or(true);
            let mfe_f = v.get("mfe_bps").and_then(|n| n.as_f64()).unwrap_or(0.0);
            let row_ts_ns = v.get("ts_ns").and_then(|n| n.as_i64()).unwrap_or(0);

            let side = side_from_str(side_s);
            let net_sc = ScaledBps((net_f * 100.0).round() as i64);
            let fee_sc = ScaledBps((fee_f * 100.0).round() as i64);
            let s_opt = slot_from_symbol_name(sym);
            let (pnl_1e8, fee_1e8, en_t) =
                extract_vault_row_pnl_and_fee_1e8(&v, s_opt, side, net_sc, fee_sc);
            ledger.record_closed_trade_with_sinking_split(pnl_1e8, fee_1e8, net_sc);

            if let Some(s_idx) = s_opt {
                if scar_snap_ts_ns <= 0 || row_ts_ns > scar_snap_ts_ns {
                    gov.observe_closed_trade_at_entry(
                        s_idx,
                        side,
                        en_t,
                        net_sc,
                        mkr,
                        TimestampNs(row_ts_ns),
                    );
                }
                let mfe_sc = (mfe_f * 100.0).round() as i64;
                if mfe_sc >= 500 {
                    engines[s_idx].observe_mfe_sample(mfe_sc);
                }
            }
        }
    }

    ledger.closed_trades_count = l3_vault_count;
    arena.exec_closed_count.store(l3_vault_count, Ordering::Release);
    println!(
        concat!(
            "{}\u{1f7e2} [JSONL REHYDRATE] Restored {} VORTEX-L3 trades ",
            "(Reserva 30%: {:+.2} bps | Blindado 70%: {:+.2} bps | ScarTs: {}){}"
        ),
        ANSI_GREEN,
        l3_vault_count,
        (ledger.sinking_fund_reserve_bps_x100 as f64) / 100.0,
        (ledger.vault_locked_profit_bps_x100 as f64) / 100.0,
        scar_snap_ts_ns,
        ANSI_RESET
    );
}

#[derive(Copy, Clone, Debug, Default)]
struct SlotForensicEntrySnapshot {
    armed_ts_ns: i64,
    init_q_lots: i64,
    ofi_in: f64,
    tfi_in: f64,
    geom_in: f64,
    snr_in: f64,
    vol5m_in: f64,
    spread_in: i64,
    btc_comp_in: f64,
    gov_ewma_bps: f64,
    win_streak: u32,
    sl_streak: u32,
    drift_bps: f64,
    ext_bps: f64,
    kin_accel_net_bps: f64,
    kin_energy_in_bps: f64,
    req_kin_energy_in_bps: f64,
}

#[must_use]
pub fn promote_verified_fill_state(
    eng: &mut SlotExecutionEngine,
    fill_ts: TimestampNs,
    verified_side: Option<Side>,
    verified_avg: Option<PriceTicks>,
    verified_lots: Option<LotQty>,
) -> Option<(Side, PriceTicks, LotQty)> {
    let _ = eng.on_maker_entry_filled_verified(
        fill_ts,
        verified_side,
        verified_avg,
        verified_lots,
    );
    if let OrderLifecycleState::PositionOpenMakerExit {
        side,
        avg_price_ticks,
        open_lots,
        ..
    } = eng.state
    {
        Some((side, avg_price_ticks, open_lots))
    } else {
        None
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum LiveCloseStepAction {
    AwaitNextPoll { next_poll_delay_ns: i64 },
    FinalizeVerified(VerifiedCloseResolution),
}

#[must_use]
pub fn resolve_live_closed_position_step(
    eng: &mut SlotExecutionEngine,
    snap: &SymbolLobSnapshot,
    closed_pnl_payload: Option<&serde_json::Value>,
    vac_level_hint: usize,
) -> Option<LiveCloseStepAction> {
    if !matches!(eng.state, OrderLifecycleState::PositionOpenMakerExit { .. }) {
        return None;
    }
    let attempt = eng.close_reconcile_retries.saturating_add(1);
    let allow_partial = attempt >= MAX_CLOSE_RECONCILE_RETRIES;
    let outcome = match closed_pnl_payload {
        Some(payload) => {
            eng.reconcile_closed_pnl_payload(payload, vac_level_hint, allow_partial)
        }
        None => ClosedPnlQueryOutcome::TransportOrApiError,
    };
    match outcome {
        ClosedPnlQueryOutcome::Verified(res) => {
            eng.close_reconcile_retries = 0;
            Some(LiveCloseStepAction::FinalizeVerified(res))
        }
        ClosedPnlQueryOutcome::PendingIndexerRetry
        | ClosedPnlQueryOutcome::TransportOrApiError => {
            if attempt < MAX_CLOSE_RECONCILE_RETRIES {
                eng.close_reconcile_retries = attempt;
                Some(LiveCloseStepAction::AwaitNextPoll {
                    next_poll_delay_ns: 250_000_000,
                })
            } else {
                eng.close_reconcile_retries = 0;
                eng.resolve_fallback_unverified_close(snap, vac_level_hint)
                    .map(LiveCloseStepAction::FinalizeVerified)
            }
        }
    }
}

fn record_and_notify_entry_fill(
    s_idx: usize,
    spec: PairSpec,
    engines: &mut [SlotExecutionEngine; MAX_ACTIVE_PAIRS],
    entry_f: &SlotForensicEntrySnapshot,
    arena: &VortexShmArena,
    tg_tx: &SyncSender<String>,
    now_ns: TimestampNs,
    eff_side: Side,
    eff_entry_px: PriceTicks,
    eff_qty: LotQty,
    tag: &str,
) {
    arena.exec_fill_count.fetch_add(1, Ordering::Relaxed);
    engines[s_idx].sync_to_shm_telemetry(arena);
    save_open_positions_state(engines);

    let sym_name = spec.symbol_id.name();
    let (en_w, en_f) = spec.format_price_usdt_parts(eff_entry_px);
    let win_fl_t = dynamic_maker_floor_price(eff_side, eff_entry_px, ScaledBps(520));
    let (tp_w, tp_f) = spec.format_price_usdt_parts(win_fl_t);
    let (sl_w, sl_f) = spec.format_price_usdt_parts(engines[s_idx].last_eff_sl_ticks);
    let q_ms = (now_ns.0.saturating_sub(entry_f.armed_ts_ns)).max(0) / 1_000_000;
    let qty_s = spec.format_lot_qty_str(eff_qty);

    println!(
        concat!(
            "{}\u{1f7e1} [{}] {} {:?} qty={} filled passively @ ",
            "{}.{:04} USDT ({}t | WIN_TP: {}.{:04} | SL: {}.{:04} | ",
            "Wait: {}ms){}"
        ),
        ANSI_YELLOW,
        tag,
        sym_name,
        eff_side,
        qty_s,
        en_w,
        en_f,
        eff_entry_px.0,
        tp_w,
        tp_f,
        sl_w,
        sl_f,
        q_ms,
        ANSI_RESET
    );
    let _ = tg_tx.try_send(format!(
        concat!(
            "\u{1f7e1} [VORTEX-L3 {}]\n",
            "\u{2022} Par y Lado : {} ({:?}) | Lote: {}\n",
            "\u{2022} Entrada L1 : {}.{:04} USDT ({}t) | Cola: {} ms\n",
            "\u{2022} Techos/SL  : WIN Floor: {}.{:04} USDT | ",
            "SL: {}.{:04} USDT\n",
            "\u{2022} Microestr. : OFI:{:+.2} | TFI:{:+.2} | ",
            "GEOM:{:+.2} | SNR:{:+.2} | Vol5m:{:.0}bp"
        ),
        tag,
        sym_name,
        eff_side,
        qty_s,
        en_w,
        en_f,
        eff_entry_px.0,
        q_ms,
        tp_w,
        tp_f,
        sl_w,
        sl_f,
        entry_f.ofi_in,
        entry_f.tfi_in,
        entry_f.geom_in,
        entry_f.snr_in,
        entry_f.vol5m_in
    ));
}

fn append_closed_trade_logs(
    slot_idx: usize,
    summary: &ClosedTradeSummary,
    eng: &SlotExecutionEngine,
    entry_f: &SlotForensicEntrySnapshot,
    exit_snap: &SymbolLobSnapshot,
    ledger: &ParitySyncLedger,
    gov: &OnlineAdaptiveGovernor,
    ts_ns: TimestampNs,
) {
    let _ = fs::create_dir_all("data/vault");
    let spec = APPROVED_SPECS[slot_idx];
    let sym = spec.symbol_id.name();
    let side_str = if matches!(summary.side, Side::Ask) { "Ask" } else { "Bid" };
    let kind_str = match summary.kind {
        ClosedTradeKind::MakerPassiveUnwind => "MakerPassiveUnwind",
        ClosedTradeKind::ServerMarkPriceStopLoss => "ServerMarkPriceStopLoss",
    };
    let net_bps = (summary.net_scaled_bps.raw() as f64) / 100.0;
    let gross_bps = (summary.gross_scaled_bps.raw() as f64) / 100.0;
    let fee_bps = (summary.fee_scaled_bps.raw() as f64) / 100.0;
    let fee_drag_pct = if gross_bps > 0.01 {
        (fee_bps / gross_bps) * 100.0
    } else {
        100.0
    };
    let mfe_bps = ((eng.last_closed_mfe_bps as f64) / 100.0).max(gross_bps);
    let mae_bps = (eng.last_closed_mae_bps as f64) / 100.0;
    let mfe_capture_pct = if mfe_bps > 0.01 {
        ((gross_bps / mfe_bps) * 100.0).clamp(0.0, 100.0)
    } else {
        0.0
    };
    let hold_ms = (ts_ns.0.saturating_sub(eng.last_opened_ts.0)).max(0) / 1_000_000;
    let q_wait_ms = if entry_f.armed_ts_ns > 0 && eng.last_opened_ts.0 >= entry_f.armed_ts_ns
    {
        (eng.last_opened_ts.0 - entry_f.armed_ts_ns) / 1_000_000
    } else {
        0
    };
    let q_floor = defensive_zero_loss_maker_exit_price(summary.side, summary.entry_price);

    let vault_line = serde_json::json!({
        "event": "TRADE_CLOSE",
        "trade_id": ledger.closed_trades_count,
        "armed_ts_ns": entry_f.armed_ts_ns,
        "opened_ts_ns": eng.last_opened_ts.0,
        "ts_ns": ts_ns.0,
        "queue_wait_ms": q_wait_ms,
        "hold_ms": hold_ms,
        "symbol": sym,
        "slot_idx": slot_idx,
        "side": side_str,
        "closed_lots": summary.closed_lots.0,
        "legs_consolidated": summary.legs_consolidated,
        "entry_ticks": summary.entry_price.0,
        "exit_ticks": summary.exit_price.0,
        "floor_ticks": q_floor.0,
        "quantum_floor_ticks": q_floor.0,
        "gross_bps": gross_bps,
        "fee_bps": fee_bps,
        "net_bps": net_bps,
        "fee_drag_pct": (fee_drag_pct * 10.0).round() / 10.0,
        "net_pnl_1e8": summary.net_pnl_1e8,
        "fee_1e8": summary.fee_1e8,
        "cum_net_pnl_1e8": ledger.cumulative_net_pnl_1e8,
        "kind": kind_str,
        "exit_sub_reason": format!("{:?}", eng.last_exit_sub_reason),
        "maker_exit": summary.is_maker_exit,
        "peak_ticks": eng.last_closed_peak_ticks.0,
        "mfe_bps": mfe_bps,
        "worst_ticks": eng.last_closed_worst_ticks.0,
        "mae_bps": mae_bps,
        "mfe_capture_pct": (mfe_capture_pct * 10.0).round() / 10.0,
        "init_q_lots": entry_f.init_q_lots,
        "ofi_in": (entry_f.ofi_in * 100.0).round() / 100.0,
        "tfi_in": (entry_f.tfi_in * 100.0).round() / 100.0,
        "geom_in": (entry_f.geom_in * 100.0).round() / 100.0,
        "snr_in": (entry_f.snr_in * 100.0).round() / 100.0,
        "vol5m_in": entry_f.vol5m_in,
        "spread_in": entry_f.spread_in,
        "btc_comp_in": (entry_f.btc_comp_in * 100.0).round() / 100.0,
        "ofi_out": (exit_snap.factor_ofi.get() * 100.0).round() / 100.0,
        "tfi_out": (exit_snap.factor_tfi.get() * 100.0).round() / 100.0,
        "geom_out": (exit_snap.factor_geom.get() * 100.0).round() / 100.0,
        "vol5m_out": (exit_snap.realized_vol_5m.raw() as f64) / 100.0,
        "slot_mfe_bps": (eng.learned_mfe_bps as f64) / 100.0,
        "gov_ewma_bps": entry_f.gov_ewma_bps,
        "win_streak": entry_f.win_streak,
        "sl_streak": entry_f.sl_streak,
        "drift_bps": entry_f.drift_bps,
        "ext_bps": entry_f.ext_bps,
        "kin_accel_net_bps": entry_f.kin_accel_net_bps,
        "kin_energy_in_bps": entry_f.kin_energy_in_bps,
        "req_kin_energy_in_bps": entry_f.req_kin_energy_in_bps,
        "exit_peg_attempts": eng.exit_peg_attempts,
        "wald_contraction_pct": eng.last_closed_wald_pct,
        "lvdbs_surfs": eng.lvdbs_deep_surfs_count,
    })
    .to_string();

    if let Ok(mut f) = OpenOptions::new()
        .create(true)
        .append(true)
        .open(AI_FORENSIC_VAULT_PATH)
    {
        let _ = writeln!(f, "{}", vault_line);
    }
    append_governor_scar_snapshot(gov, ts_ns);
    if net_bps < -0.50 {
        if let Ok(mut f) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(RED_LOSS_BOX_PATH)
        {
            let _ = writeln!(f, "{}", vault_line);
        }
        let _ = fs::write(HALT_LOCK_PATH, &vault_line);
    }
}
fn finalize_and_emit_live_close(
    s_idx: usize,
    resolution: VerifiedCloseResolution,
    now_ns: TimestampNs,
    cached_snaps: &[SymbolLobSnapshot; MAX_ACTIVE_PAIRS],
    entry_forensics: &[SlotForensicEntrySnapshot; MAX_ACTIVE_PAIRS],
    engines: &mut [SlotExecutionEngine; MAX_ACTIVE_PAIRS],
    gov: &mut OnlineAdaptiveGovernor,
    ledger: &mut ParitySyncLedger,
    arena: &VortexShmArena,
    tg_tx: &SyncSender<String>,
) {
    let spec = APPROVED_SPECS[s_idx];
    let sym_name = spec.symbol_id.name();
    let hold_s =
        (now_ns.0.saturating_sub(engines[s_idx].last_opened_ts.0)).max(0) / 1_000_000_000;
    let Some(summary) =
        engines[s_idx].apply_verified_close_resolution(resolution, now_ns, ledger)
    else {
        return;
    };
    gov.observe_closed_trade_at_entry(
        s_idx,
        summary.side,
        summary.entry_price,
        summary.net_scaled_bps,
        summary.is_maker_exit,
        now_ns,
    );
    arena.exec_closed_count.fetch_add(1, Ordering::Relaxed);
    append_closed_trade_logs(
        s_idx,
        &summary,
        &engines[s_idx],
        &entry_forensics[s_idx],
        &cached_snaps[s_idx],
        ledger,
        gov,
        now_ns,
    );
    engines[s_idx].sync_to_shm_telemetry(arena);
    save_open_positions_state(engines);

    let net_f = (summary.net_scaled_bps.raw() as f64) / 100.0;
    let (en_w, en_f) = spec.format_price_usdt_parts(summary.entry_price);
    let (ex_w, ex_f) = spec.format_price_usdt_parts(summary.exit_price);
    let pnl_usdt = (summary.net_pnl_1e8 as f64) / 100_000_000.0;
    let lots_s = spec.format_lot_qty_str(summary.closed_lots);
    let is_part = matches!(
        engines[s_idx].last_exit_sub_reason,
        ExitSubReason::Tranche1Harvest50
    );
    let (col, icon, lbl) = classify_trade_badge(net_f, is_part);
    println!(
        concat!(
            "{}{} [TRADE CLOSE \u{2014} {}] {} {:?} qty={} ({}L) | ",
            "{}.{:04} -> {}.{:04} | Net: {:+.2} bps ({:+.4} USDT) | Hold: {}s{}"
        ),
        col,
        icon,
        lbl.trim(),
        sym_name,
        summary.side,
        lots_s,
        summary.legs_consolidated,
        en_w,
        en_f,
        ex_w,
        ex_f,
        net_f,
        pnl_usdt,
        hold_s,
        ANSI_RESET
    );
    let _ = tg_tx.try_send(format_telegram_close_message(
        spec,
        &summary,
        engines[s_idx].last_exit_sub_reason,
        ledger.closed_trades_count,
        ledger,
    ));
}

fn handle_gateway_event(
    evt: GatewayEvent,
    now_ns: TimestampNs,
    wallet_1e8: &mut i64,
    slot_rpc_inflight: &mut [bool; MAX_ACTIVE_PAIRS],
    sl_needs_sync: &mut [bool; MAX_ACTIVE_PAIRS],
    init_q_ahead: &mut [LotQty; MAX_ACTIVE_PAIRS],
    cur_q_ahead: &mut [LotQty; MAX_ACTIVE_PAIRS],
    cached_snaps: &[SymbolLobSnapshot; MAX_ACTIVE_PAIRS],
    entry_forensics: &[SlotForensicEntrySnapshot; MAX_ACTIVE_PAIRS],
    engines: &mut [SlotExecutionEngine; MAX_ACTIVE_PAIRS],
    gov: &mut OnlineAdaptiveGovernor,
    ledger: &mut ParitySyncLedger,
    arena: &VortexShmArena,
    tg_tx: &SyncSender<String>,
) {
    match evt {
        GatewayEvent::WalletUpdated { wallet_1e8: w } => {
            if w > 0 {
                *wallet_1e8 = w;
            }
        }
        GatewayEvent::EntryPlaced {
            slot_idx,
            accepted,
            entry_price,
            sl_price,
        } => {
            slot_rpc_inflight[slot_idx] = false;
            let OrderLifecycleState::PlacingEntryMaker { side, qty, .. } =
                engines[slot_idx].state
            else {
                return;
            };
            if accepted {
                let _ = engines[slot_idx].on_entry_order_resting(now_ns);
                arena.exec_armed_count.fetch_add(1, Ordering::Relaxed);
                engines[slot_idx].sync_to_shm_telemetry(arena);
                let spec = APPROVED_SPECS[slot_idx];
                let snap = cached_snaps[slot_idx];
                let (ew_p, ef_p) = spec.format_price_usdt_parts(entry_price);
                let (sw_p, sf_p) = spec.format_price_usdt_parts(sl_price);
                println!(
                    concat!(
                        "{}\u{1f7e1} [BYBIT V5 LIVE ORDER SENT] {} {:?} qty={} @ ",
                        "{}.{:04} USDT (SL: {}.{:04} | Q_L1: {} lots | Seq: {} | ",
                        "OFI: {:+.2} | TFI: {:+.2} | GEOM: {:+.2}){}"
                    ),
                    ANSI_YELLOW,
                    spec.symbol_id.name(),
                    side,
                    spec.format_lot_qty_str(qty),
                    ew_p,
                    ef_p,
                    sw_p,
                    sf_p,
                    init_q_ahead[slot_idx].0,
                    snap.sequence_id,
                    snap.factor_ofi.get(),
                    snap.factor_tfi.get(),
                    snap.factor_geom.get(),
                    ANSI_RESET
                );
            } else {
                engines[slot_idx].state = OrderLifecycleState::Idle;
                engines[slot_idx].sync_to_shm_telemetry(arena);
            }
        }
        GatewayEvent::EntryRepegged {
            slot_idx,
            accepted,
            new_price,
            new_sl_price,
        } => {
            slot_rpc_inflight[slot_idx] = false;
            let OrderLifecycleState::RestingEntryQueue { side, .. } =
                engines[slot_idx].state
            else {
                return;
            };
            if accepted {
                engines[slot_idx].apply_resting_repeg(new_price, new_sl_price, now_ns);
                let q_new = match side {
                    Side::Bid => cached_snaps[slot_idx].bids[0].qty,
                    Side::Ask => cached_snaps[slot_idx].asks[0].qty,
                };
                init_q_ahead[slot_idx] = q_new;
                cur_q_ahead[slot_idx] = q_new;
                engines[slot_idx].sync_to_shm_telemetry(arena);
            } else {
                engines[slot_idx].state = OrderLifecycleState::Idle;
                engines[slot_idx].sync_to_shm_telemetry(arena);
            }
        }
        GatewayEvent::EntryAborted {
            slot_idx,
            side,
            aborted_price,
        } => {
            slot_rpc_inflight[slot_idx] = false;
            if matches!(
                engines[slot_idx].state,
                OrderLifecycleState::PlacingEntryMaker { .. }
                    | OrderLifecycleState::RestingEntryQueue { .. }
            ) {
                gov.observe_entry_abort(slot_idx, side, aborted_price, now_ns);
                engines[slot_idx].state = OrderLifecycleState::Idle;
                arena.exec_abort_count.fetch_add(1, Ordering::Relaxed);
                engines[slot_idx].sync_to_shm_telemetry(arena);
            }
        }
        GatewayEvent::EntryStillResting { slot_idx } => {
            slot_rpc_inflight[slot_idx] = false;
        }
        GatewayEvent::EntryFilledLive {
            slot_idx,
            real_side,
            verified_avg,
            real_lots,
            sl_verified,
            tag,
        } => {
            slot_rpc_inflight[slot_idx] = false;
            if !matches!(
                engines[slot_idx].state,
                OrderLifecycleState::PlacingEntryMaker { .. }
                    | OrderLifecycleState::RestingEntryQueue { .. }
            ) {
                return;
            }
            gov.clear_abort_streak(slot_idx, real_side);
            let prev_sl = engines[slot_idx].last_eff_sl_ticks;
            if let Some((eff_s, eff_px, eff_q)) = promote_verified_fill_state(
                &mut engines[slot_idx],
                now_ns,
                Some(real_side),
                verified_avg,
                Some(real_lots),
            ) {
                if let Ok(valid_sl) = SlotExecutionEngine::compute_fill_or_adopt_sl_ticks(
                    eff_s,
                    eff_px,
                    &cached_snaps[slot_idx],
                ) {
                    engines[slot_idx].last_eff_sl_ticks = valid_sl;
                    if let OrderLifecycleState::PositionOpenMakerExit {
                        sl_trigger_price: ref mut sl_p,
                        ..
                    } = engines[slot_idx].state
                    {
                        *sl_p = valid_sl;
                    }
                }
                if !sl_verified || engines[slot_idx].last_eff_sl_ticks != prev_sl {
                    sl_needs_sync[slot_idx] = true;
                }
                let spec = APPROVED_SPECS[slot_idx];
                record_and_notify_entry_fill(
                    slot_idx,
                    spec,
                    engines,
                    &entry_forensics[slot_idx],
                    arena,
                    tg_tx,
                    now_ns,
                    eff_s,
                    eff_px,
                    eff_q,
                    tag,
                );
            }
        }
        GatewayEvent::StopLossSynced {
            slot_idx,
            sl_price,
            verified,
        } => {
            slot_rpc_inflight[slot_idx] = false;
            if verified {
                sl_needs_sync[slot_idx] = false;
                engines[slot_idx].last_eff_sl_ticks = sl_price;
                if let OrderLifecycleState::PositionOpenMakerExit {
                    sl_trigger_price: ref mut sl_p,
                    ..
                } = engines[slot_idx].state
                {
                    *sl_p = sl_price;
                }
                engines[slot_idx].sync_to_shm_telemetry(arena);
            } else {
                sl_needs_sync[slot_idx] = true;
            }
        }
        GatewayEvent::SlotSyncSkipped { slot_idx } => {
            slot_rpc_inflight[slot_idx] = false;
        }
        GatewayEvent::SlotStillOpenSynced {
            slot_idx,
            real_side,
            verified_avg,
            real_lots,
            active_resting_tp,
        } => {
            slot_rpc_inflight[slot_idx] = false;
            let OrderLifecycleState::PositionOpenMakerExit {
                side,
                initial_lots,
                tranche1_harvested,
                resting_exit_price: prev_rest_tp,
                opened_ts,
                ..
            } = engines[slot_idx].state
            else {
                return;
            };
            if real_side == side {
                if let Some(v_avg) = verified_avg {
                    engines[slot_idx].sync_open_avg_price_ticks(v_avg);
                }
            }
            let hold_s = (now_ns.0.saturating_sub(opened_ts.0)).max(0) / 1_000_000_000;
            let eff_init = initial_lots.max(real_lots);
            let mut t1_done = tranche1_harvested;
            if !t1_done
                && prev_rest_tp.0 > 0
                && eff_init.0 >= 2
                && real_lots.0 < eff_init.0
                && hold_s >= 1
            {
                t1_done = true;
                engines[slot_idx].tranche1_harvests_count =
                    engines[slot_idx].tranche1_harvests_count.saturating_add(1);
            }
            if active_resting_tp.0 > 0 && active_resting_tp != prev_rest_tp {
                engines[slot_idx].exit_peg_attempts =
                    engines[slot_idx].exit_peg_attempts.saturating_add(1);
            }
            if let OrderLifecycleState::PositionOpenMakerExit {
                initial_lots: ref mut in_l,
                open_lots: ref mut op_l,
                tranche1_harvested: ref mut t1_h,
                tranche1_lots: ref mut t1_l,
                tranche1_exit_price: ref mut t1_px,
                resting_exit_price: ref mut rx_p,
                ..
            } = engines[slot_idx].state
            {
                *in_l = eff_init;
                *op_l = real_lots;
                if t1_done && !*t1_h {
                    *t1_h = true;
                    *t1_l = LotQty((eff_init.0 - real_lots.0).max(1));
                    if prev_rest_tp.0 > 0 {
                        *t1_px = prev_rest_tp;
                    }
                }
                *rx_p = active_resting_tp;
            }
            save_open_positions_state(engines);
            engines[slot_idx].sync_to_shm_telemetry(arena);
        }
        GatewayEvent::SlotClosedDetected {
            slot_idx,
            vac_level,
            closed_pnl_payload,
        } => {
            slot_rpc_inflight[slot_idx] = false;
            sl_needs_sync[slot_idx] = false;
            let Some(step_action) = resolve_live_closed_position_step(
                &mut engines[slot_idx],
                &cached_snaps[slot_idx],
                closed_pnl_payload.as_ref(),
                vac_level,
            ) else {
                engines[slot_idx].sync_to_shm_telemetry(arena);
                return;
            };
            match step_action {
                LiveCloseStepAction::AwaitNextPoll { next_poll_delay_ns } => {
                    engines[slot_idx].next_peg_allowed_ts_ns =
                        now_ns.0 + next_poll_delay_ns;
                    engines[slot_idx].sync_to_shm_telemetry(arena);
                }
                LiveCloseStepAction::FinalizeVerified(res) => {
                    finalize_and_emit_live_close(
                        slot_idx,
                        res,
                        now_ns,
                        cached_snaps,
                        entry_forensics,
                        engines,
                        gov,
                        ledger,
                        arena,
                        tg_tx,
                    );
                }
            }
        }
        GatewayEvent::WsPositionUpdate {
            slot_idx,
            is_open,
            real_side,
            verified_avg,
            real_lots,
        } => {
            if is_open {
                if matches!(
                    engines[slot_idx].state,
                    OrderLifecycleState::PlacingEntryMaker { .. }
                        | OrderLifecycleState::RestingEntryQueue { .. }
                ) {
                    slot_rpc_inflight[slot_idx] = false;
                    gov.clear_abort_streak(slot_idx, real_side);
                    if let Some((eff_s, eff_px, eff_q)) = promote_verified_fill_state(
                        &mut engines[slot_idx],
                        now_ns,
                        Some(real_side),
                        verified_avg,
                        Some(real_lots),
                    ) {
                        if let Ok(valid_sl) =
                            SlotExecutionEngine::compute_fill_or_adopt_sl_ticks(
                                eff_s,
                                eff_px,
                                &cached_snaps[slot_idx],
                            )
                        {
                            engines[slot_idx].last_eff_sl_ticks = valid_sl;
                            if let OrderLifecycleState::PositionOpenMakerExit {
                                sl_trigger_price: ref mut sl_p,
                                ..
                            } = engines[slot_idx].state
                            {
                                *sl_p = valid_sl;
                            }
                        }
                        sl_needs_sync[slot_idx] = true;
                        let spec = APPROVED_SPECS[slot_idx];
                        record_and_notify_entry_fill(
                            slot_idx,
                            spec,
                            engines,
                            &entry_forensics[slot_idx],
                            arena,
                            tg_tx,
                            now_ns,
                            eff_s,
                            eff_px,
                            eff_q,
                            "MAKER FILL WS",
                        );
                    }
                } else if let OrderLifecycleState::PositionOpenMakerExit {
                    side,
                    open_lots,
                    ..
                } = engines[slot_idx].state
                {
                    if real_side == side {
                        if let Some(v_avg) = verified_avg {
                            engines[slot_idx].sync_open_avg_price_ticks(v_avg);
                        }
                    }
                    if real_lots.0 > 0 && real_lots.0 < open_lots.0 {
                        engines[slot_idx].next_peg_allowed_ts_ns = 0;
                    }
                }
            } else if matches!(
                engines[slot_idx].state,
                OrderLifecycleState::PositionOpenMakerExit { .. }
            ) {
                engines[slot_idx].next_peg_allowed_ts_ns = 0;
            }
        }
    }
}

fn bootstrap_live_adopt_if_enabled(
    live_capital: bool,
    arena: &VortexShmArena,
    wallet_1e8: &mut i64,
    sl_needs_sync: &mut [bool; MAX_ACTIVE_PAIRS],
    entry_forensics: &mut [SlotForensicEntrySnapshot; MAX_ACTIVE_PAIRS],
    engines: &mut [SlotExecutionEngine; MAX_ACTIVE_PAIRS],
) {
    let Some(mut boot_client) = BybitV5LiveClient::from_env_if_enabled(live_capital) else {
        return;
    };
    if let Some((w_f, _, _, _)) = boot_client.fetch_wallet_and_equity_usdt() {
        if w_f > 0.0 {
            *wallet_1e8 = (w_f * 100_000_000.0).round() as i64;
        }
    }
    for (s_idx, spec) in APPROVED_SPECS.iter().enumerate() {
        let sym_name = spec.symbol_id.name();
        let Some((true, sz_s, side_p, avg_s)) =
            boot_client.query_open_position_verified(sym_name)
        else {
            continue;
        };
        let Ok(bybit_avg_t) = parse_verified_avg_price_ticks(*spec, side_p, &avg_s) else {
            continue;
        };
        let lots = spec.parse_lot_qty(&sz_s).max(LotQty(1));
        let now_t = now_epoch_ns();
        let saved = load_saved_slot_state(s_idx);
        let init_l = saved
            .as_ref()
            .and_then(|v| v.get("initial_lots")?.as_i64())
            .map_or(lots, LotQty);
        let t1_done = saved
            .as_ref()
            .and_then(|v| v.get("tranche1_harvested")?.as_bool())
            .unwrap_or(false);
        let def_locked = saved
            .as_ref()
            .and_then(|v| v.get("defensive_floor_locked")?.as_bool())
            .unwrap_or(false);
        let t1_ex_saved = saved
            .as_ref()
            .and_then(|v| v.get("tranche1_exit_ticks")?.as_i64())
            .unwrap_or(0);
        let opened_ns = saved
            .as_ref()
            .and_then(|v| v.get("opened_ts_ns")?.as_i64())
            .map_or(now_t, TimestampNs);
        let peak_t = saved
            .as_ref()
            .and_then(|v| v.get("peak_ticks")?.as_i64())
            .filter(|&p| p > 0)
            .map_or(bybit_avg_t, PriceTicks);
        let worst_t = saved
            .as_ref()
            .and_then(|v| v.get("worst_ticks")?.as_i64())
            .filter(|&w| w > 0)
            .map_or(bybit_avg_t, PriceTicks);
        let snap = arena
            .slot(s_idx)
            .and_then(|s| s.read_snapshot(4))
            .unwrap_or_else(|_| SymbolLobSnapshot::new_empty());

        if let Ok(sl_t) = engines[s_idx].adopt_live_open_position(
            side_p,
            bybit_avg_t,
            init_l,
            lots,
            t1_done,
            peak_t,
            worst_t,
            opened_ns,
            &snap,
        ) {
            engines[s_idx].latch_defensive_floor_if_needed(def_locked);
            if t1_ex_saved > 0 {
                if let OrderLifecycleState::PositionOpenMakerExit {
                    tranche1_exit_price: ref mut t1_p,
                    ..
                } = engines[s_idx].state
                {
                    *t1_p = PriceTicks(t1_ex_saved);
                }
            }
            let sl_ok = boot_client.set_position_trading_stop(*spec, sl_t);
            sl_needs_sync[s_idx] = !sl_ok;
            entry_forensics[s_idx].armed_ts_ns = opened_ns.0;
            entry_forensics[s_idx].init_q_lots = init_l.0;
            engines[s_idx].sync_to_shm_telemetry(arena);
            println!(
                concat!(
                    "{}\u{1f7e2} [VORTEX-L3 LIVE ADOPT] Slot {:2} {} {:?} ",
                    "qty={} @ {}t (SL={}t ok={} T1={}){}"
                ),
                ANSI_GREEN,
                s_idx,
                sym_name,
                side_p,
                sz_s,
                bybit_avg_t.0,
                sl_t.0,
                sl_ok,
                t1_done,
                ANSI_RESET
            );
        }
    }
    save_open_positions_state(engines);
}
fn run_autonomous_12_slot_daemon(arena: &VortexShmArena, live_capital: bool) -> ExitCode {
    let tg_tx = spawn_bounded_telegram_notifier();
    let mut engines = SlotExecutionEngine::new_12_slot_array();
    let mut gov = OnlineAdaptiveGovernor::new();
    let mut ledger = ParitySyncLedger::new(DEFAULT_WALLET_EQUITY_1E8);
    let mut cached_snaps = [SymbolLobSnapshot::new_empty(); MAX_ACTIVE_PAIRS];
    let mut entry_forensics = [SlotForensicEntrySnapshot::default(); MAX_ACTIVE_PAIRS];
    let mut last_seq = [0_u64; MAX_ACTIVE_PAIRS];
    let mut init_q_ahead = [LotQty(0); MAX_ACTIVE_PAIRS];
    let mut cur_q_ahead = [LotQty(0); MAX_ACTIVE_PAIRS];
    let mut last_l1_q_obs = [LotQty(0); MAX_ACTIVE_PAIRS];
    let mut last_opp_trades = [LotQty(0); MAX_ACTIVE_PAIRS];
    let mut slot_rpc_inflight = [false; MAX_ACTIVE_PAIRS];
    let mut sl_needs_sync = [false; MAX_ACTIVE_PAIRS];
    let mut sl_retry_after_ns = [0_i64; MAX_ACTIVE_PAIRS];
    let mut wallet_1e8 = DEFAULT_WALLET_EQUITY_1E8;
    let mut last_wallet_sync_ns = 0_i64;
    let mut last_rest_dispatch_ns = 0_i64;

    rehydrate_shadow_ledger(&mut ledger, &mut engines, &mut gov, arena);
    bootstrap_live_adopt_if_enabled(
        live_capital,
        arena,
        &mut wallet_1e8,
        &mut sl_needs_sync,
        &mut entry_forensics,
        &mut engines,
    );
    let gateway = BybitAsyncGateway::spawn_if_enabled(live_capital);

    println!(
        concat!(
            "{}\u{1f7e2} [VORTEX-L3 200.0 Hz ARMED] 12 Slot Engines + Async TLS/WS ",
            "Gateway + SSOT LVDBS L2..L4 + BetaCap 3B/3A active.{}"
        ),
        ANSI_GREEN, ANSI_RESET
    );

    loop {
        thread::sleep(Duration::from_millis(5));
        let now_ns = now_epoch_ns();

        for s_idx in 0..MAX_ACTIVE_PAIRS {
            if let Ok(slot_ref) = arena.slot(s_idx) {
                if let Ok(snap) = slot_ref.read_snapshot(4) {
                    cached_snaps[s_idx] = snap;
                    if snap.sequence_id != last_seq[s_idx] {
                        last_seq[s_idx] = snap.sequence_id;
                        gov.observe_market_price(s_idx, &snap);
                    }
                }
            }
        }

        if let Some(ref gw) = gateway {
            while let Some(evt) = gw.try_recv() {
                handle_gateway_event(
                    evt,
                    now_ns,
                    &mut wallet_1e8,
                    &mut slot_rpc_inflight,
                    &mut sl_needs_sync,
                    &mut init_q_ahead,
                    &mut cur_q_ahead,
                    &cached_snaps,
                    &entry_forensics,
                    &mut engines,
                    &mut gov,
                    &mut ledger,
                    arena,
                    &tg_tx,
                );
            }
        }

        if Path::new(HALT_LOCK_PATH).exists() {
            thread::sleep(Duration::from_millis(250));
            continue;
        }
        let drain_active = Path::new(DRAIN_LOCK_PATH).exists();

        if now_ns.0.saturating_sub(last_wallet_sync_ns) >= 30_000_000_000 {
            last_wallet_sync_ns = now_ns.0;
            if let Some(ref gw) = gateway {
                let _ = gw.try_send(GatewayCommand::SyncWallet);
            }
        }

        let btc_snap = cached_snaps[0];
        let mut open_count = 0_usize;

        for s_idx in 0..MAX_ACTIVE_PAIRS {
            let spec = APPROVED_SPECS[s_idx];
            let snap = cached_snaps[s_idx];
            if snap.sequence_id == 0 {
                continue;
            }

            match engines[s_idx].state {
                OrderLifecycleState::Idle => {}
                OrderLifecycleState::PlacingEntryMaker {
                    side,
                    qty,
                    submitted_ts,
                    ..
                } => {
                    if slot_rpc_inflight[s_idx] {
                        continue;
                    }
                    if drain_active {
                        engines[s_idx].state = OrderLifecycleState::Idle;
                        engines[s_idx].sync_to_shm_telemetry(arena);
                        continue;
                    }
                    let wait_ns =
                        engines[s_idx].elastic_preflight_window_ns(snap.realized_vol_5m);
                    if now_ns.0.saturating_sub(submitted_ts.0) < wait_ns {
                        continue;
                    }
                    let sig_ok = matches!(
                        OrthogonalFactorState::evaluate_entry_signal_with_leader(
                            spec, &snap, &btc_snap
                        ),
                        SignalVerdict::AuthorizeMakerEntry { side: s_now, .. }
                            if s_now == side
                    );
                    if !sig_ok {
                        engines[s_idx].state = OrderLifecycleState::Idle;
                        engines[s_idx].sync_to_shm_telemetry(arena);
                        continue;
                    }
                    let Ok((synced_entry_px, synced_sl_px)) =
                        engines[s_idx].sync_preflight_l1_entry(&snap, 8)
                    else {
                        engines[s_idx].state = OrderLifecycleState::Idle;
                        engines[s_idx].sync_to_shm_telemetry(arena);
                        continue;
                    };

                    let (q_l1, opp_cum) = match side {
                        Side::Bid => (snap.bids[0].qty, snap.cum_sell_trade_lots),
                        Side::Ask => (snap.asks[0].qty, snap.cum_buy_trade_lots),
                    };
                    let d_sign_i = side.sign_i64();
                    let (dr_b, ex_b) = gov.price_drift_and_extension_scaled_bps(s_idx, &snap);
                    let (ewma_b, sl_st, win_st) = gov.slot_direction_diagnostics(s_idx, side);
                    let signed_gm = snap.factor_geom.get() * side.sign_f64();
                    let snr_v = OrthogonalFactorState::snr_composite_score(&snap);
                    let btc_c = OrthogonalFactorState::snr_composite_score(&btc_snap);
                    let (e_kin, e_min) = gov
                        .evaluate_kinematic_energy_surface(
                            s_idx,
                            side,
                            dr_b,
                            ex_b,
                            signed_gm,
                            snr_v.abs(),
                        )
                        .unwrap_or(((ex_b - dr_b) * d_sign_i, 125));

                    entry_forensics[s_idx] = SlotForensicEntrySnapshot {
                        armed_ts_ns: now_ns.0,
                        init_q_lots: q_l1.0,
                        ofi_in: snap.factor_ofi.get(),
                        tfi_in: snap.factor_tfi.get(),
                        geom_in: snap.factor_geom.get(),
                        snr_in: snr_v,
                        vol5m_in: (snap.realized_vol_5m.raw() as f64) / 100.0,
                        spread_in: (snap.asks[0].price.0 - snap.bids[0].price.0).max(1),
                        btc_comp_in: btc_c,
                        gov_ewma_bps: (ewma_b as f64) / 100.0,
                        win_streak: win_st,
                        sl_streak: sl_st,
                        drift_bps: (dr_b as f64) / 100.0,
                        ext_bps: (ex_b as f64) / 100.0,
                        kin_accel_net_bps: (((ex_b - dr_b) * d_sign_i) as f64) / 100.0,
                        kin_energy_in_bps: (e_kin as f64) / 100.0,
                        req_kin_energy_in_bps: (e_min as f64) / 100.0,
                    };
                    init_q_ahead[s_idx] = q_l1;
                    cur_q_ahead[s_idx] = q_l1;
                    last_l1_q_obs[s_idx] = q_l1;
                    last_opp_trades[s_idx] = opp_cum;

                    if let Some(ref gw) = gateway {
                        if gw.try_send(GatewayCommand::PlaceEntry {
                            slot_idx: s_idx,
                            side,
                            entry_price: synced_entry_px,
                            qty,
                            sl_price: synced_sl_px,
                        }) {
                            slot_rpc_inflight[s_idx] = true;
                        }
                    } else {
                        let _ = engines[s_idx].on_entry_order_resting(now_ns);
                        arena.exec_armed_count.fetch_add(1, Ordering::Relaxed);
                        engines[s_idx].sync_to_shm_telemetry(arena);
                    }
                }
                OrderLifecycleState::RestingEntryQueue {
                    side,
                    entry_price,
                    qty,
                    sl_trigger_price,
                    resting_since_ts,
                    ..
                } => {
                    let (at_l1, live_l1_q, cur_opp_cum) = match side {
                        Side::Bid => (
                            snap.bids[0].price == entry_price,
                            snap.bids[0].qty,
                            snap.cum_sell_trade_lots,
                        ),
                        Side::Ask => (
                            snap.asks[0].price == entry_price,
                            snap.asks[0].qty,
                            snap.cum_buy_trade_lots,
                        ),
                    };
                    let tape_delta =
                        (cur_opp_cum.0 - last_opp_trades[s_idx].0).max(0);
                    last_opp_trades[s_idx] = cur_opp_cum;
                    if tape_delta > 0 {
                        let hits_px = match side {
                            Side::Bid => {
                                snap.last_trade_price.0 > 0
                                    && snap.last_trade_price.0 <= entry_price.0
                            }
                            Side::Ask => snap.last_trade_price.0 >= entry_price.0,
                        };
                        if hits_px {
                            cur_q_ahead[s_idx] =
                                LotQty((cur_q_ahead[s_idx].0 - tape_delta).max(0));
                        }
                    }
                    if at_l1 {
                        if last_l1_q_obs[s_idx].0 > 0
                            && live_l1_q.0 < last_l1_q_obs[s_idx].0
                        {
                            let drop_25 = (last_l1_q_obs[s_idx].0 - live_l1_q.0) / 4;
                            if cur_q_ahead[s_idx].0 > 1 && drop_25 > 0 {
                                cur_q_ahead[s_idx] =
                                    LotQty((cur_q_ahead[s_idx].0 - drop_25).max(1));
                            }
                        }
                        last_l1_q_obs[s_idx] = live_l1_q;
                    } else {
                        last_l1_q_obs[s_idx] = LotQty(0);
                    }
                    if let Ok(tel) = arena.telemetry(s_idx) {
                        tel.store_queue_ahead_lots(cur_q_ahead[s_idx]);
                    }
                    if slot_rpc_inflight[s_idx] {
                        continue;
                    }

                    let resting_elapsed_ns = now_ns.0.saturating_sub(resting_since_ts.0);
                    let vol_q = snap.realized_vol_5m.raw().clamp(4_000, 12_500);
                    let queue_timeout_ns = ((15_000_000_000_i64 * 6_500) / vol_q)
                        .clamp(10_000_000_000, 20_000_000_000);
                    let flow_imp = (0.55 * snap.factor_ofi.get()
                        + 0.45 * snap.factor_tfi.get())
                        * side.sign_f64();
                    let dq_abort = (resting_elapsed_ns >= 1_500_000_000
                        && at_l1
                        && check_queue_depletion_velocity_abort(
                            side,
                            init_q_ahead[s_idx],
                            cur_q_ahead[s_idx],
                            &snap,
                        ))
                        || flow_imp <= -0.48;

                    if drain_active || resting_elapsed_ns >= queue_timeout_ns || dq_abort {
                        if let Some(ref gw) = gateway {
                            if gw.try_send(GatewayCommand::AbortEntry {
                                slot_idx: s_idx,
                                side,
                                aborted_price: entry_price,
                                sl_hint: sl_trigger_price,
                            }) {
                                slot_rpc_inflight[s_idx] = true;
                            }
                        } else {
                            gov.observe_entry_abort(s_idx, side, entry_price, now_ns);
                            engines[s_idx].state = OrderLifecycleState::Idle;
                            arena.exec_abort_count.fetch_add(1, Ordering::Relaxed);
                            engines[s_idx].sync_to_shm_telemetry(arena);
                        }
                        continue;
                    }

                    if !at_l1 && resting_elapsed_ns >= 2_500_000_000 {
                        let new_l1 =
                            side.select_entry_bbo_price(snap.bids[0].price, snap.asks[0].price);
                        let moved_away = match side {
                            Side::Bid => {
                                new_l1.0 > entry_price.0 && (new_l1.0 - entry_price.0) <= 15
                            }
                            Side::Ask => {
                                new_l1.0 < entry_price.0 && (entry_price.0 - new_l1.0) <= 15
                            }
                        };
                        let sig_ok = matches!(
                            OrthogonalFactorState::evaluate_entry_signal_with_leader(
                                spec, &snap, &btc_snap
                            ),
                            SignalVerdict::AuthorizeMakerEntry { side: s_now, .. }
                                if s_now == side
                        );
                        if moved_away
                            && sig_ok
                            && now_ns.0 >= engines[s_idx].next_peg_allowed_ts_ns
                        {
                            if let Ok(repeg_sl) =
                                engines[s_idx].compute_repeg_sl_trigger(&snap, new_l1)
                            {
                                engines[s_idx].next_peg_allowed_ts_ns =
                                    now_ns.0 + 1_500_000_000;
                                if let Some(ref gw) = gateway {
                                    if gw.try_send(GatewayCommand::RepegEntry {
                                        slot_idx: s_idx,
                                        side,
                                        new_price: new_l1,
                                        qty,
                                        new_sl_price: repeg_sl,
                                    }) {
                                        slot_rpc_inflight[s_idx] = true;
                                        continue;
                                    }
                                } else {
                                    engines[s_idx].apply_resting_repeg(
                                        new_l1, repeg_sl, now_ns,
                                    );
                                    init_q_ahead[s_idx] = live_l1_q;
                                    cur_q_ahead[s_idx] = live_l1_q;
                                    engines[s_idx].sync_to_shm_telemetry(arena);
                                    continue;
                                }
                            }
                        }
                    }

                    if let Some(ref gw) = gateway {
                        if now_ns.0 >= engines[s_idx].next_peg_allowed_ts_ns
                            && now_ns.0.saturating_sub(last_rest_dispatch_ns) >= 55_000_000
                        {
                            last_rest_dispatch_ns = now_ns.0;
                            engines[s_idx].next_peg_allowed_ts_ns =
                                now_ns.0 + 1_200_000_000;
                            if gw.try_send(GatewayCommand::PollEntryFill {
                                slot_idx: s_idx,
                                sl_hint: sl_trigger_price,
                            }) {
                                slot_rpc_inflight[s_idx] = true;
                            }
                        }
                    } else {
                        let bbo_crossed = match side {
                            Side::Bid => {
                                snap.asks[0].price.0 > 0
                                    && snap.asks[0].price.0 <= entry_price.0
                            }
                            Side::Ask => snap.bids[0].price.0 >= entry_price.0,
                        };
                        if cur_q_ahead[s_idx].0 <= 0 || bbo_crossed {
                            gov.clear_abort_streak(s_idx, side);
                            if let Some((eff_s, eff_px, eff_q)) =
                                promote_verified_fill_state(
                                    &mut engines[s_idx],
                                    now_ns,
                                    None,
                                    None,
                                    None,
                                )
                            {
                                record_and_notify_entry_fill(
                                    s_idx,
                                    spec,
                                    &mut engines,
                                    &entry_forensics[s_idx],
                                    arena,
                                    &tg_tx,
                                    now_ns,
                                    eff_s,
                                    eff_px,
                                    eff_q,
                                    "SHADOW MAKER FILL",
                                );
                            }
                        }
                    }
                }
                OrderLifecycleState::PositionOpenMakerExit {
                    side,
                    avg_price_ticks,
                    mut peak_favorable_price,
                    mut worst_adverse_price,
                    tranche1_harvested,
                    sl_trigger_price,
                    opened_ts,
                    ..
                } => {
                    open_count += 1;
                    let cur_bbo = match side {
                        Side::Bid => snap.asks[0].price,
                        Side::Ask => snap.bids[0].price,
                    };
                    let ref_px = if snap.last_trade_price.0 > 0 {
                        snap.last_trade_price
                    } else {
                        cur_bbo
                    };
                    match side {
                        Side::Bid => {
                            if ref_px.0 > peak_favorable_price.0 {
                                peak_favorable_price = ref_px;
                            }
                            if ref_px.0 > 0 && ref_px.0 < worst_adverse_price.0 {
                                worst_adverse_price = ref_px;
                            }
                        }
                        Side::Ask => {
                            if ref_px.0 > 0 && ref_px.0 < peak_favorable_price.0 {
                                peak_favorable_price = ref_px;
                            }
                            if ref_px.0 > worst_adverse_price.0 {
                                worst_adverse_price = ref_px;
                            }
                        }
                    }
                    if let OrderLifecycleState::PositionOpenMakerExit {
                        peak_favorable_price: ref mut pk,
                        worst_adverse_price: ref mut wr,
                        ..
                    } = engines[s_idx].state
                    {
                        *pk = peak_favorable_price;
                        *wr = worst_adverse_price;
                    }
                    let avg_denom = avg_price_ticks.0.max(1);
                    let pk_d =
                        (peak_favorable_price.0 - avg_price_ticks.0) * side.sign_i64();
                    let wr_d =
                        (worst_adverse_price.0 - avg_price_ticks.0) * side.sign_i64();
                    engines[s_idx].last_closed_mfe_bps =
                        (pk_d.saturating_mul(1_000_000)) / avg_denom;
                    engines[s_idx].last_closed_mae_bps =
                        (wr_d.saturating_mul(1_000_000)) / avg_denom;
                    engines[s_idx].last_closed_peak_ticks = peak_favorable_price;
                    engines[s_idx].last_closed_worst_ticks = worst_adverse_price;
                    engines[s_idx].last_eff_sl_ticks =
                        engines[s_idx].last_eff_sl_ticks.max(sl_trigger_price);

                    if let Some(ref gw) = gateway {
                        let Some(quote) = engines[s_idx].compute_canonical_exit_quote(&snap)
                        else {
                            engines[s_idx].sync_to_shm_telemetry(arena);
                            continue;
                        };
                        engines[s_idx].last_closed_wald_pct = quote.wald_pct;
                        engines[s_idx]
                            .latch_defensive_floor_if_needed(quote.defensive_lock_active);
                        if let OrderLifecycleState::PositionOpenMakerExit {
                            active_exit_role: ref mut ax_r,
                            active_exit_price: ref mut ax_p,
                            active_exit_lots: ref mut ax_l,
                            ..
                        } = engines[s_idx].state
                        {
                            *ax_r = quote.role;
                            *ax_p = quote.target_price;
                            *ax_l = quote.order_lots;
                        }

                        if sl_needs_sync[s_idx]
                            && !slot_rpc_inflight[s_idx]
                            && now_ns.0 >= sl_retry_after_ns[s_idx]
                        {
                            let eff_sl = engines[s_idx].last_eff_sl_ticks;
                            if gw.try_send(GatewayCommand::EnforceStopLoss {
                                slot_idx: s_idx,
                                sl_price: eff_sl,
                            }) {
                                slot_rpc_inflight[s_idx] = true;
                                sl_retry_after_ns[s_idx] = now_ns.0 + 1_000_000_000;
                            }
                            engines[s_idx].sync_to_shm_telemetry(arena);
                            continue;
                        }

                        let rest_ready = !slot_rpc_inflight[s_idx]
                            && now_ns.0 >= engines[s_idx].next_peg_allowed_ts_ns
                            && now_ns.0.saturating_sub(last_rest_dispatch_ns) >= 55_000_000;
                        if rest_ready {
                            last_rest_dispatch_ns = now_ns.0;
                            engines[s_idx].next_peg_allowed_ts_ns =
                                now_ns.0 + 1_350_000_000;
                            if gw.try_send(GatewayCommand::SyncOpenSlot {
                                slot_idx: s_idx,
                                side,
                                target_px: quote.target_price,
                                target_lots: quote.order_lots,
                                defensive_lock: quote.defensive_lock_active,
                                cur_gross_bps: quote.cur_gross_bps,
                                vac_level: quote.vac_level,
                                opened_ts_ns: opened_ts.0,
                                tranche1_harvested,
                            }) {
                                slot_rpc_inflight[s_idx] = true;
                            }
                        }
                        engines[s_idx].sync_to_shm_telemetry(arena);
                        continue;
                    }

                    if let Some(summary) = engines[s_idx].step_open_position(
                        &snap, &btc_snap, &gov, &mut ledger,
                    ) {
                        gov.observe_closed_trade_at_entry(
                            s_idx,
                            summary.side,
                            summary.entry_price,
                            summary.net_scaled_bps,
                            summary.is_maker_exit,
                            now_ns,
                        );
                        arena.exec_closed_count.fetch_add(1, Ordering::Relaxed);
                        append_closed_trade_logs(
                            s_idx,
                            &summary,
                            &engines[s_idx],
                            &entry_forensics[s_idx],
                            &snap,
                            &ledger,
                            &gov,
                            now_ns,
                        );
                        save_open_positions_state(&engines);
                        let _ = tg_tx.try_send(format_telegram_close_message(
                            spec,
                            &summary,
                            engines[s_idx].last_exit_sub_reason,
                            ledger.closed_trades_count,
                            &ledger,
                        ));
                    }
                    engines[s_idx].sync_to_shm_telemetry(arena);
                }
            }
        }

        let any_inflight = slot_rpc_inflight.iter().any(|&b| b);
        if drain_active && open_count == 0 && !any_inflight {
            let _ = tg_tx.try_send(
                concat!(
                    "\u{1f7e2} [VORTEX-L3 DRAIN COMPLETE] ",
                    "0 open positions verified (Wallet == Equity)."
                )
                .to_string(),
            );
            println!(
                concat!(
                    "{}\u{1f7e2} [DRAIN_COMPLETE_STOPPED] ",
                    "0 open positions verified. Exiting cleanly.{}"
                ),
                ANSI_GREEN, ANSI_RESET
            );
            return ExitCode::SUCCESS;
        }

        let mut beta_guard = PortfolioBetaGuard::new();
        for eng in &engines {
            if let Some(act_side) = eng.active_side() {
                beta_guard.observe_active_side(act_side);
            }
        }
        if !drain_active && beta_guard.total_active() < 6 {
            let mut armed_this_tick = 0_usize;
            for s_idx in 0..MAX_ACTIVE_PAIRS {
                if beta_guard.total_active() >= 6 || armed_this_tick >= 2 {
                    break;
                }
                if !matches!(engines[s_idx].state, OrderLifecycleState::Idle)
                    || slot_rpc_inflight[s_idx]
                {
                    continue;
                }
                let spec = APPROVED_SPECS[s_idx];
                let snap = cached_snaps[s_idx];
                if snap.sequence_id == 0 {
                    continue;
                }
                let verdict = OrthogonalFactorState::evaluate_entry_signal_with_leader(
                    spec, &snap, &btc_snap,
                );
                if let SignalVerdict::AuthorizeMakerEntry { side, .. } = verdict {
                    if !beta_guard.can_admit_side(side) {
                        continue;
                    }
                    if gov.authorizes_adaptive_entry_with_wallet(
                        s_idx, side, wallet_1e8, &snap, &btc_snap,
                    ) {
                        let (edge_bps, _, _) = gov.slot_direction_diagnostics(s_idx, side);
                        let in_debt = gov.is_in_scar_debt(s_idx, side);
                        let entry_bbo = side.select_entry_bbo_price(
                            snap.bids[0].price,
                            snap.asks[0].price,
                        );
                        let lots = spec.compute_compounding_kelly_lots(
                            wallet_1e8, entry_bbo, edge_bps, in_debt,
                        );
                        if engines[s_idx]
                            .try_arm_maker_entry(&snap, verdict, lots)
                            .is_ok()
                            && matches!(
                                engines[s_idx].state,
                                OrderLifecycleState::PlacingEntryMaker { .. }
                            )
                        {
                            let _ = beta_guard.try_admit(side);
                            engines[s_idx].sync_to_shm_telemetry(arena);
                            armed_this_tick += 1;
                        }
                    }
                }
            }
        }
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let is_live_hud = args.iter().any(|a| a == "--live");
    let is_trades = args.iter().any(|a| a == "--trades");
    let is_daemon = args.iter().any(|a| a == "--daemon");
    let is_live_cap = args.iter().any(|a| a == "--live-capital");
    let shm_path = MappedShmRegion::default_shm_path();

    if is_live_hud {
        match MappedShmRegion::open_existing_read_only(&shm_path) {
            Ok(reg) => render_live_hud(reg.arena()),
            Err(_) => {
                println!(
                    concat!(
                        "{}\u{26aa} [VORTEX-L3 LIVE HUD] SHM inactive ({}) ",
                        "\u{2014} daemons stopped.{}"
                    ),
                    ANSI_GRAY,
                    shm_path.display(),
                    ANSI_RESET
                );
            }
        }
        return ExitCode::SUCCESS;
    }
    if is_trades {
        render_trades_and_executive_dashboard();
        return ExitCode::SUCCESS;
    }

    let (oos_trades, maker_ratio, pf, cum_usdt, drift) = run_oos_520_certification_suite();
    let sep = "=".repeat(80);
    println!("{}", sep);
    println!("[VORTEX-EXEC V3.0] 12-SLOT EXECUTION FSM, PARITY LEDGER & OOS GATE VERIFIER");
    println!("{}", sep);
    println!(
        "  {}\u{1f7e2} -> OOS Closed Trades Verified : {} (Gate >= 500){}",
        ANSI_GREEN, oos_trades, ANSI_RESET
    );
    println!(
        "  {}\u{1f7e2} -> Maker Exit Ratio           : {:.2}% (Gate >= 80.00%){}",
        ANSI_GREEN, maker_ratio, ANSI_RESET
    );
    println!(
        "  {}\u{1f7e2} -> Net Profit Factor (PostFee): {:.2} (Gate >= 1.35){}",
        ANSI_GREEN, pf, ANSI_RESET
    );
    println!(
        concat!(
            "  {}\u{1f7e2} -> Cumulative Net PnL (OOS)   : ",
            "+ {:.4} USDT | PARITY_SYNC Drift: {} units{}"
        ),
        ANSI_GREEN, cum_usdt, drift, ANSI_RESET
    );
    println!("{}", sep);

    if oos_trades < 500 || maker_ratio < 80.0 || pf < 1.35 || drift != 0 {
        return ExitCode::FAILURE;
    }
    if is_daemon {
        let Ok(reg) = MappedShmRegion::open_or_create(&shm_path, false) else {
            return ExitCode::FAILURE;
        };
        return run_autonomous_12_slot_daemon(reg.arena(), is_live_cap);
    }
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use vortex_rs::lob_shm::BookLevel;

    fn drain_http_request(rdr: &mut BufReader<TcpStream>) -> bool {
        let mut clen = 0_usize;
        loop {
            let mut line = String::new();
            match rdr.read_line(&mut line) {
                Ok(0) | Err(_) => return false,
                Ok(_) => {
                    if line == "\r\n" || line == "\n" {
                        break;
                    }
                    if let Some((k, v)) = line.split_once(':') {
                        if k.trim().eq_ignore_ascii_case("content-length") {
                            clen = v.trim().parse::<usize>().unwrap_or(0);
                        }
                    }
                }
            }
        }
        if clen > 0 {
            let mut body = vec![0_u8; clen];
            if rdr.read_exact(&mut body).is_err() {
                return false;
            }
        }
        true
    }

    #[test]
    fn test_oos_520_certification_gate_and_zero_drift() {
        let (trades, maker_pct, pf, cum_usdt, drift) = run_oos_520_certification_suite();
        assert_eq!(trades, 520);
        assert!(maker_pct >= 80.0);
        assert!(pf >= 1.35);
        assert!(cum_usdt > 0.0);
        assert_eq!(drift, 0);
    }

    #[test]
    fn test_12_slot_autonomous_array_and_portfolio_beta_cap() {
        let engines = SlotExecutionEngine::new_12_slot_array();
        assert_eq!(engines.len(), 12);
        for (idx, eng) in engines.iter().enumerate() {
            assert_eq!(eng.slot_idx, idx);
            assert_eq!(eng.active_side(), None);
        }
        let mut guard = PortfolioBetaGuard::new();
        for _ in 0..3 {
            assert!(guard.try_admit(Side::Bid));
            assert!(guard.try_admit(Side::Ask));
        }
        assert!(!guard.can_admit_side(Side::Bid));
        assert!(!guard.can_admit_side(Side::Ask));
    }

    #[test]
    fn test_f02_f03_verified_fill_and_closed_pnl_retry_reconciliation() {
        let spec = APPROVED_SPECS[3];
        let mut eng = SlotExecutionEngine::new(3);
        let mut ledger = ParitySyncLedger::new(DEFAULT_WALLET_EQUITY_1E8);
        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = spec.symbol_id;
        snap.realized_vol_5m = ScaledBps(7_000);
        snap.ts_local_ns = TimestampNs(1_700_000_010_000_000_000);
        snap.bids[0] = BookLevel { price: PriceTicks(8_200), qty: LotQty(300) };
        snap.asks[0] = BookLevel { price: PriceTicks(8_201), qty: LotQty(250) };
        snap.mark_price = PriceTicks(8_200);
        snap.last_trade_price = PriceTicks(8_200);

        let verdict = SignalVerdict::AuthorizeMakerEntry {
            side: Side::Bid,
            target_excursion_scaled_bps: ScaledBps(650),
        };
        assert!(eng.try_arm_maker_entry(&snap, verdict, LotQty(26)).is_ok());
        let avg_v = parse_verified_avg_price_ticks(spec, Side::Bid, "8.200");
        assert_eq!(avg_v, Ok(PriceTicks(8_200)));
        assert_eq!(
            promote_verified_fill_state(
                &mut eng,
                TimestampNs(1_700_000_010_500_000_000),
                Some(Side::Bid),
                avg_v.ok(),
                Some(LotQty(26)),
            ),
            Some((Side::Bid, PriceTicks(8_200), LotQty(26)))
        );

        let empty_json = serde_json::json!({ "retCode": 0, "result": { "list": [] } });
        assert_eq!(
            resolve_live_closed_position_step(&mut eng, &snap, Some(&empty_json), 2),
            Some(LiveCloseStepAction::AwaitNextPoll {
                next_poll_delay_ns: 250_000_000,
            })
        );
        let verified_json = serde_json::json!({
            "retCode": 0,
            "result": {
                "list": [{
                    "symbol": "AVAXUSDT",
                    "side": "Sell",
                    "orderType": "Limit",
                    "execType": "Trade",
                    "closedSize": "2.6",
                    "avgEntryPrice": "8.200",
                    "avgExitPrice": "8.208",
                    "closedPnl": "0.01240000",
                    "updatedTime": "1700000012000"
                }]
            }
        });
        let step2 =
            resolve_live_closed_position_step(&mut eng, &snap, Some(&verified_json), 2);
        if let Some(LiveCloseStepAction::FinalizeVerified(res)) = step2 {
            let closed = eng.apply_verified_close_resolution(
                res,
                TimestampNs(1_700_000_012_500_000_000),
                &mut ledger,
            );
            assert!(closed.is_some());
            if let Some(s) = closed {
                assert!(s.is_maker_exit);
                assert_eq!(s.net_pnl_1e8, 1_240_000);
            }
        } else {
            assert_eq!(eng.slot_idx, 999);
        }
    }

    #[test]
    fn test_f05_f07_vault_exact_lot_pnl_and_scar_rehydration() {
        let mut ledger = ParitySyncLedger::new(DEFAULT_WALLET_EQUITY_1E8);
        let row_v = serde_json::json!({
            "entry_ticks": 759_000,
            "exit_ticks": 759_600,
            "closed_lots": 1
        });
        let (pnl, fee, en_t) = extract_vault_row_pnl_and_fee_1e8(
            &row_v,
            Some(0),
            Side::Bid,
            ScaledBps(390),
            ScaledBps(400),
        );
        assert_eq!(en_t, PriceTicks(759_000));
        assert_eq!(fee, 3_037_200);
        assert_eq!(pnl, 2_962_800);
        ledger.record_closed_trade_with_sinking_split(pnl, fee, ScaledBps(390));
        let locked_before = ledger.vault_locked_profit_bps_x100;
        ledger.record_closed_trade_with_sinking_split(-900_000, 500_000, ScaledBps(-900));
        assert_eq!(ledger.vault_locked_profit_bps_x100, locked_before);
    }

    #[test]
    fn test_f06_f08_keepalive_http_loopback_34040_sl_and_chunked_wallet() {
        let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
            return;
        };
        let Ok(addr) = listener.local_addr() else {
            return;
        };
        let srv = thread::spawn(move || {
            let Ok((mut writer, _)) = listener.accept() else {
                return;
            };
            let Ok(reader_sock) = writer.try_clone() else {
                return;
            };
            let mut rdr = BufReader::new(reader_sock);
            if drain_http_request(&mut rdr) {
                let b1 = "{\"retCode\":34040,\"retMsg\":\"not modified\"}";
                let r1 = format!(
                    concat!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n",
                        "Connection: keep-alive\r\n\r\n{}"
                    ),
                    b1.len(),
                    b1
                );
                let _ = writer.write_all(r1.as_bytes());
                let _ = writer.flush();
            }
            if drain_http_request(&mut rdr) {
                let b2 = concat!(
                    "{\"retCode\":0,\"result\":{\"list\":[{",
                    "\"totalWalletBalance\":\"66.2808\",",
                    "\"totalEquity\":\"66.2808\",",
                    "\"totalAvailableBalance\":\"66.2808\",",
                    "\"totalPerpUPL\":\"0\"}]}}"
                );
                let r2 = format!(
                    concat!(
                        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n",
                        "Connection: close\r\n\r\n{:X}\r\n{}\r\n0\r\n\r\n"
                    ),
                    b2.len(),
                    b2
                );
                let _ = writer.write_all(r2.as_bytes());
                let _ = writer.flush();
            }
        });

        let base = format!("http://127.0.0.1:{}", addr.port());
        let client_opt = BybitV5LiveClient::with_base_url(
            "k_test".to_string(),
            "s_test".to_string(),
            &base,
        );
        assert!(client_opt.is_some());
        if let Some(mut client) = client_opt {
            assert!(
                client.set_position_trading_stop(APPROVED_SPECS[3], PriceTicks(8_090))
            );
            let w_opt = client.fetch_wallet_and_equity_usdt();
            assert!(w_opt.is_some());
            if let Some((w, eq, av, _)) = w_opt {
                assert!((w - 66.2808).abs() < 1e-6);
                assert!((eq - 66.2808).abs() < 1e-6);
                assert!((av - 66.2808).abs() < 1e-6);
            }
        }
        let _ = srv.join();
    }
}
