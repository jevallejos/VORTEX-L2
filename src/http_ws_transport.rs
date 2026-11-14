//! Keep-Alive HTTP/1.1 TLS Transport, WS Trade Tunnel, Indexer Lag Estimator & Env Loader.

#![allow(
    clippy::manual_clamp,
    clippy::manual_range_contains,
    clippy::too_many_arguments
)]

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use crate::execution_fsm::{
    format_ws_trade_req_id, Vip0WsRateGovernor, WsMutationOp, WsOrderLatticeState,
    MAX_CLOSE_RECONCILE_RETRIES,
};
#[cfg(target_os = "linux")]
use crate::execution_fsm::sign_bybit_v5_hmac_sha256;
use crate::lob_shm::MAX_ACTIVE_PAIRS;
use crate::types::{now_epoch_ns, LotQty, PriceTicks};
#[cfg(target_os = "linux")]
use crate::types::{ANSI_GREEN, ANSI_RESET};

pub const MAX_HTTP_BODY_BYTES: usize = 2_097_152;
pub const SEED_INDEXER_LAG_NS: i64 = 350_000_000;

/// Online asymmetric Kalman-EWMA estimator of Bybit /v5/position/closed-pnl indexer lag.
#[derive(Copy, Clone, Debug)]
pub struct OnlineIndexerLagEstimator {
    ewma_lag_ns: i64,
}

impl Default for OnlineIndexerLagEstimator {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl OnlineIndexerLagEstimator {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            ewma_lag_ns: SEED_INDEXER_LAG_NS,
        }
    }

    #[inline]
    #[must_use]
    pub const fn current_lag_ns(self) -> i64 {
        self.ewma_lag_ns
    }

    #[inline]
    pub fn observe_closed_pnl_payload(
        &mut self,
        payload: &serde_json::Value,
        observed_at_ns: i64,
    ) {
        if observed_at_ns <= 0 {
            return;
        }
        let Some(first) = payload
            .get("result")
            .and_then(|r| r.get("list"))
            .and_then(|l| l.as_array())
            .and_then(|a| a.first())
        else {
            return;
        };
        let upd_ms = first
            .get("updatedTime")
            .or_else(|| first.get("createdTime"))
            .and_then(|v| {
                v.as_i64()
                    .or_else(|| v.as_str().and_then(|s| s.trim().parse::<i64>().ok()))
            })
            .unwrap_or(0);
        if upd_ms <= 0 {
            return;
        }
        let upd_ns = upd_ms.saturating_mul(1_000_000);
        let raw_lag = observed_at_ns.saturating_sub(upd_ns).clamp(120_000_000, 2_000_000_000);
        let next = if raw_lag > self.ewma_lag_ns {
            (self.ewma_lag_ns * 65 + raw_lag * 35) / 100
        } else {
            (self.ewma_lag_ns * 88 + raw_lag * 12) / 100
        };
        self.ewma_lag_ns = next.clamp(120_000_000, 1_500_000_000);
    }

    #[inline]
    pub fn observe_empty_retry(&mut self) {
        self.ewma_lag_ns = ((self.ewma_lag_ns * 112) / 100).clamp(150_000_000, 1_500_000_000);
    }

    #[inline]
    #[must_use]
    pub fn adaptive_poll_delay_ns(self, attempt: u8) -> i64 {
        let mult = i64::from(attempt.clamp(1, MAX_CLOSE_RECONCILE_RETRIES));
        self.ewma_lag_ns
            .saturating_mul(mult)
            .clamp(150_000_000, 2_400_000_000)
    }
}

#[must_use]
pub fn load_dotenv_map() -> HashMap<String, String> {
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
    let (use_tls, rest, default_port) = if let Some(r) = trimmed.strip_prefix("https://") {
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

pub struct KeepAliveHttpConn {
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
    pub fn new(endpoint: ParsedEndpointUrl) -> Self {
        Self {
            endpoint,
            reader: None,
            req_buf: Vec::with_capacity(1_024),
            line_buf: String::with_capacity(256),
            body_buf: Vec::with_capacity(4_096),
        }
    }

    pub fn send_json_request(
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
                if !Self::read_chunked_into(rdr, &mut self.line_buf, &mut self.body_buf) {
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

#[must_use]
pub fn boot_session_hash32() -> u32 {
    static BOOT_H: std::sync::OnceLock<u32> = std::sync::OnceLock::new();
    *BOOT_H.get_or_init(|| {
        let ns = now_epoch_ns().0 as u64;
        let pid = u64::from(std::process::id());
        let mixed = ns.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ (pid << 16);
        ((mixed >> 32) as u32) ^ (mixed as u32) ^ 0x5658_4C33
    })
}

#[derive(Clone, Debug)]
pub struct SlotWsOrderTracker {
    pub cycle_id: u32,
    pub entry_sub_idx: u16,
    pub entry_link_id: String,
    pub entry_lattice: WsOrderLatticeState,
    pub exit_sub_idx: u16,
    pub exit_link_id: String,
    pub exit_lattice: WsOrderLatticeState,
}

impl Default for SlotWsOrderTracker {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl SlotWsOrderTracker {
    #[must_use]
    pub fn new() -> Self {
        Self {
            cycle_id: 1,
            entry_sub_idx: 0,
            entry_link_id: String::new(),
            entry_lattice: WsOrderLatticeState::new_dispatched(PriceTicks(0), LotQty(0)),
            exit_sub_idx: 0,
            exit_link_id: String::new(),
            exit_lattice: WsOrderLatticeState::new_dispatched(PriceTicks(0), LotQty(0)),
        }
    }

    pub fn advance_cycle(&mut self) {
        self.cycle_id = self.cycle_id.wrapping_add(1).max(1);
        self.entry_sub_idx = 0;
        self.entry_link_id.clear();
        self.entry_lattice = WsOrderLatticeState::new_dispatched(PriceTicks(0), LotQty(0));
        self.exit_sub_idx = 0;
        self.exit_link_id.clear();
        self.exit_lattice = WsOrderLatticeState::new_dispatched(PriceTicks(0), LotQty(0));
    }
}

#[must_use]
pub fn global_ws_trackers() -> &'static std::sync::Mutex<[SlotWsOrderTracker; MAX_ACTIVE_PAIRS]> {
    static TRACKERS: std::sync::OnceLock<
        std::sync::Mutex<[SlotWsOrderTracker; MAX_ACTIVE_PAIRS]>,
    > = std::sync::OnceLock::new();
    TRACKERS.get_or_init(|| {
        std::sync::Mutex::new(std::array::from_fn(|_| SlotWsOrderTracker::new()))
    })
}

#[must_use]
pub fn build_ws_trade_envelope_json(
    req_id: &str,
    ts_ms: i64,
    recv_window_ms: u32,
    op_name: &str,
    arg_obj: &serde_json::Value,
) -> String {
    serde_json::json!({
        "reqId": req_id,
        "header": {
            "X-BAPI-TIMESTAMP": ts_ms.to_string(),
            "X-BAPI-RECV-WINDOW": recv_window_ms.to_string(),
        },
        "op": op_name,
        "args": [arg_obj],
    })
    .to_string()
}

#[must_use]
pub fn parse_ws_trade_ack_frame(
    txt: &str,
    expected_req_id: &str,
) -> Option<(i64, Option<i64>, Option<i64>, bool)> {
    let val = serde_json::from_str::<serde_json::Value>(txt).ok()?;
    if val.get("op").and_then(|o| o.as_str()) == Some("pong")
        || val.get("ret_msg").and_then(|m| m.as_str()) == Some("pong")
        || val.get("retMsg").and_then(|m| m.as_str()) == Some("pong")
    {
        return None;
    }
    let rc = val.get("retCode").and_then(|v| v.as_i64())?;
    if rc == 10_019 {
        return Some((rc, None, None, true));
    }
    let ack_req = val.get("reqId").and_then(|v| v.as_str()).unwrap_or("");
    if !ack_req.is_empty() && !expected_req_id.is_empty() && ack_req != expected_req_id {
        return None;
    }
    let hdr = val.get("header");
    let lim_status = hdr
        .and_then(|h| h.get("X-Bapi-Limit-Status"))
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<i64>().ok());
    let rst_ms = hdr
        .and_then(|h| h.get("X-Bapi-Limit-Reset-Timestamp"))
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<i64>().ok());
    Some((rc, lim_status, rst_ms, false))
}

#[cfg(target_os = "linux")]
type WsTradeStream =
    tungstenite::WebSocket<tungstenite::stream::MaybeTlsStream<std::net::TcpStream>>;

pub struct WsTradeTunnel {
    enabled: bool,
    ws_url: String,
    req_seq: u32,
    rate_gov: Vip0WsRateGovernor,
    #[cfg(target_os = "linux")]
    socket: Option<WsTradeStream>,
    #[cfg(target_os = "linux")]
    last_ping_ns: i64,
    #[cfg(target_os = "linux")]
    next_reconnect_ns: i64,
}

impl std::fmt::Debug for WsTradeTunnel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WsTradeTunnel")
            .field("enabled", &self.enabled)
            .field("ws_url", &self.ws_url)
            .field("req_seq", &self.req_seq)
            .finish_non_exhaustive()
    }
}

impl WsTradeTunnel {
    #[must_use]
    pub const fn new_disabled() -> Self {
        Self {
            enabled: false,
            ws_url: String::new(),
            req_seq: 0,
            rate_gov: Vip0WsRateGovernor::new(),
            #[cfg(target_os = "linux")]
            socket: None,
            #[cfg(target_os = "linux")]
            last_ping_ns: 0,
            #[cfg(target_os = "linux")]
            next_reconnect_ns: 0,
        }
    }

    #[inline]
    #[must_use]
    pub const fn authorizes_mutation(
        &self,
        op: WsMutationOp,
        is_defensive_or_abort: bool,
        now_ns: i64,
    ) -> bool {
        self.rate_gov
            .authorizes_mutation(op, is_defensive_or_abort, now_ns)
    }

    pub fn enable_with_url(&mut self, ws_url: String, api_key: &str, api_secret: &str) {
        self.enabled = cfg!(target_os = "linux");
        self.ws_url = ws_url;
        self.maintain_heartbeat(api_key, api_secret);
    }

    #[cfg(not(target_os = "linux"))]
    pub fn maintain_heartbeat(&mut self, _api_key: &str, _api_secret: &str) {
        if !self.enabled {
            self.req_seq = 0;
        }
    }

    #[cfg(target_os = "linux")]
    fn set_stream_read_timeout(sock: &mut WsTradeStream, millis: u64) {
        let tcp_opt = match sock.get_mut() {
            tungstenite::stream::MaybeTlsStream::Plain(s) => Some(s),
            tungstenite::stream::MaybeTlsStream::NativeTls(tls) => Some(tls.get_mut()),
            _ => None,
        };
        if let Some(tcp) = tcp_opt {
            let _ = tcp.set_read_timeout(Some(Duration::from_millis(millis)));
        }
    }

    #[cfg(target_os = "linux")]
    fn ensure_connected(&mut self, api_key: &str, api_secret: &str) -> bool {
        if self.socket.is_some() {
            return true;
        }
        let now_ns = now_epoch_ns().0;
        if !self.enabled || now_ns < self.next_reconnect_ns {
            return false;
        }
        self.next_reconnect_ns = now_ns.saturating_add(2_000_000_000);
        let Ok((mut sock, _)) = tungstenite::connect(self.ws_url.as_str()) else {
            return false;
        };
        let tcp_opt = match sock.get_mut() {
            tungstenite::stream::MaybeTlsStream::Plain(s) => Some(s),
            tungstenite::stream::MaybeTlsStream::NativeTls(tls) => Some(tls.get_mut()),
            _ => None,
        };
        if let Some(tcp) = tcp_opt {
            let _ = tcp.set_nodelay(true);
            let _ = tcp.set_read_timeout(Some(Duration::from_millis(2_200)));
            let _ = tcp.set_write_timeout(Some(Duration::from_millis(2_000)));
        }
        let exp_ms = (now_ns / 1_000_000).max(1) + 10_000;
        let sig = sign_bybit_v5_hmac_sha256(api_secret, &format!("GET/realtime{exp_ms}"));
        let auth_msg = serde_json::json!({
            "op": "auth",
            "args": [api_key, exp_ms, sig],
        })
        .to_string();
        if sock.send(tungstenite::Message::Text(auth_msg)).is_err() {
            return false;
        }
        if let Ok(tungstenite::Message::Text(reply)) = sock.read() {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&reply) {
                let ok_auth = v.get("retCode").and_then(serde_json::Value::as_i64) == Some(0)
                    || v.get("success").and_then(serde_json::Value::as_bool) == Some(true);
                if ok_auth {
                    println!(
                        concat!(
                            "{}\u{1f7e2} [BYBIT V5 WS TRADE TUNNEL] ",
                            "Persistent {} armed (Atomic Amend).{}"
                        ),
                        ANSI_GREEN, self.ws_url, ANSI_RESET
                    );
                    self.last_ping_ns = now_ns;
                    self.socket = Some(sock);
                    return true;
                }
            }
        }
        false
    }

    #[cfg(target_os = "linux")]
    pub fn maintain_heartbeat(&mut self, api_key: &str, api_secret: &str) {
        if !self.enabled || !self.ensure_connected(api_key, api_secret) {
            return;
        }
        let now_ns = now_epoch_ns().0;
        if now_ns.saturating_sub(self.last_ping_ns) < 12_000_000_000 {
            return;
        }
        let Some(mut sock) = self.socket.take() else {
            return;
        };
        if sock
            .send(tungstenite::Message::Text("{\"op\":\"ping\"}".to_string()))
            .is_err()
        {
            return;
        }
        let mut closed = false;
        Self::set_stream_read_timeout(&mut sock, 25);
        for _ in 0..4 {
            match sock.read() {
                Ok(tungstenite::Message::Ping(p)) => {
                    let _ = sock.send(tungstenite::Message::Pong(p));
                }
                Ok(tungstenite::Message::Close(_)) => {
                    closed = true;
                    break;
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
        Self::set_stream_read_timeout(&mut sock, 2_200);
        if !closed {
            self.last_ping_ns = now_ns;
            self.socket = Some(sock);
        }
    }

    #[cfg(not(target_os = "linux"))]
    pub fn execute_ws_op(
        &mut self,
        _api_key: &str,
        _api_secret: &str,
        op: WsMutationOp,
        op_name: &str,
        recv_window_ms: u32,
        arg_obj: &serde_json::Value,
    ) -> Option<i64> {
        let req_id = format_ws_trade_req_id(boot_session_hash32(), self.req_seq);
        let frame = build_ws_trade_envelope_json(&req_id, 1, recv_window_ms, op_name, arg_obj);
        if !self.enabled {
            let _ = parse_ws_trade_ack_frame(&frame, &req_id);
            return None;
        }
        self.req_seq = self.req_seq.wrapping_add(1);
        let (rc, lim, rst, _) = parse_ws_trade_ack_frame(&frame, &req_id)?;
        self.rate_gov.on_ack_headers(op, lim, rst);
        Some(rc)
    }

    #[cfg(target_os = "linux")]
    pub fn execute_ws_op(
        &mut self,
        api_key: &str,
        api_secret: &str,
        op: WsMutationOp,
        op_name: &str,
        recv_window_ms: u32,
        arg_obj: &serde_json::Value,
    ) -> Option<i64> {
        if !self.ensure_connected(api_key, api_secret) {
            return None;
        }
        let mut sock = self.socket.take()?;
        let now_ns = now_epoch_ns().0;
        self.req_seq = self.req_seq.wrapping_add(1);
        let req_id = format_ws_trade_req_id(boot_session_hash32(), self.req_seq);
        let ts_ms = (now_ns / 1_000_000).max(1);
        let frame = build_ws_trade_envelope_json(&req_id, ts_ms, recv_window_ms, op_name, arg_obj);
        self.rate_gov.on_dispatch(op, now_ns);
        if sock.send(tungstenite::Message::Text(frame)).is_err() {
            self.rate_gov.on_ack_headers(op, None, None);
            return None;
        }
        self.last_ping_ns = now_ns;
        for _ in 0..64 {
            match sock.read() {
                Ok(tungstenite::Message::Text(txt)) => {
                    if let Some((rc, lim, rst, is_maint)) = parse_ws_trade_ack_frame(&txt, &req_id)
                    {
                        self.rate_gov.on_ack_headers(op, lim, rst);
                        if is_maint {
                            return None;
                        }
                        self.socket = Some(sock);
                        return Some(rc);
                    }
                }
                Ok(tungstenite::Message::Ping(p)) => {
                    let _ = sock.send(tungstenite::Message::Pong(p));
                }
                Ok(tungstenite::Message::Close(_)) | Err(_) => break,
                Ok(_) => {}
            }
        }
        self.rate_gov.on_ack_headers(op, None, None);
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::thread;

    #[test]
    fn test_endpoint_parser_indexer_lag_ws_ack_and_chunked_keepalive() {
        let ep = parse_endpoint_url("https://api.bybit.com/v5/");
        assert!(ep.is_some_and(|u| u.use_tls && u.port == 443 && u.base_path == "/v5"));

        let mut lag = OnlineIndexerLagEstimator::new();
        let sample_p = serde_json::json!({
            "result": { "list": [{ "updatedTime": "1700000000000" }] }
        });
        lag.observe_closed_pnl_payload(&sample_p, 1_700_000_000_600_000_000);
        lag.observe_empty_retry();
        assert!(lag.current_lag_ns() >= 350_000_000);
        assert!(lag.adaptive_poll_delay_ns(2) >= 700_000_000);

        let arg = serde_json::json!({"symbol": "AVAXUSDT"});
        let env_s =
            build_ws_trade_envelope_json("r-abcd1234-00000007", 1_763_000_000_123, 10_000, "order.amend", &arg);
        assert!(env_s.contains("\"op\":\"order.amend\""));
        let ack_json = concat!(
            "{\"reqId\":\"r-abcd1234-00000007\",\"retCode\":0,\"op\":\"order.amend\",",
            "\"header\":{\"X-Bapi-Limit-Status\":\"8\",",
            "\"X-Bapi-Limit-Reset-Timestamp\":\"1763000001000\"}}"
        );
        assert_eq!(
            parse_ws_trade_ack_frame(ack_json, "r-abcd1234-00000007"),
            Some((0, Some(8), Some(1_763_000_001_000), false))
        );

        let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
            return;
        };
        let Ok(addr) = listener.local_addr() else {
            return;
        };
        let srv = thread::spawn(move || {
            let Ok((mut sock, _)) = listener.accept() else {
                return;
            };
            let mut rdr_buf = [0_u8; 512];
            let _ = sock.read(&mut rdr_buf);
            let body = "{\"retCode\":0}";
            let resp = format!(
                concat!(
                    "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n",
                    "Connection: close\r\n\r\n{:X}\r\n{}\r\n0\r\n\r\n"
                ),
                body.len(),
                body
            );
            let _ = sock.write_all(resp.as_bytes());
            let _ = sock.flush();
        });
        if let Some(parsed) = parse_endpoint_url(&format!("http://127.0.0.1:{}", addr.port())) {
            let mut conn = KeepAliveHttpConn::new(parsed);
            let out = conn.send_json_request("GET", "/v5/account", &[], "");
            assert_eq!(out, Some(b"{\"retCode\":0}".as_slice()));
        }
        let _ = srv.join();
    }
}
