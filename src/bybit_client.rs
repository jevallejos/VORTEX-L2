//! Authenticated Bybit UTA V5 REST/WS Client & P0-A Emergency Market IOC Flattener.

#![allow(
    clippy::manual_clamp,
    clippy::manual_range_contains,
    clippy::too_many_arguments
)]

use crate::execution_fsm::{
    asymmetric_recv_window_ms, sign_bybit_v5_hmac_sha256, WsMutationOp,
};
use crate::http_ws_transport::{
    load_dotenv_map, parse_endpoint_url, KeepAliveHttpConn, WsTradeTunnel,
};
use crate::types::{
    now_epoch_ns, LotQty, PairSpec, PriceTicks, Side, ANSI_ORANGE, ANSI_RED, ANSI_RESET,
};

pub struct BybitV5LiveClient {
    api_key: String,
    api_secret: String,
    conn: KeepAliveHttpConn,
    ws_trade: WsTradeTunnel,
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
            ws_trade: WsTradeTunnel::new_disabled(),
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

    pub fn post_v5(&mut self, endpoint: &str, body_json: &str) -> Option<serde_json::Value> {
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

    pub fn has_resting_entry_order(&mut self, spec: PairSpec) -> Option<bool> {
        let sym = spec.symbol_id.name();
        let q = format!("category=linear&symbol={sym}");
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
            if !ro {
                return Some(true);
            }
        }
        Some(false)
    }

    pub fn cancel_single_order_id(&mut self, symbol: &str, order_id: &str) -> bool {
        if order_id.is_empty() {
            return false;
        }
        let body = serde_json::json!({
            "category": "linear",
            "symbol": symbol,
            "orderId": order_id,
        })
        .to_string();
        self.post_v5("/v5/order/cancel", &body)
            .and_then(|v| v.get("retCode")?.as_i64())
            .is_some_and(|rc| rc == 0 || rc == 110_001)
    }

    pub fn cancel_non_reduce_only_orders(&mut self, spec: PairSpec) -> usize {
        let sym = spec.symbol_id.name();
        let q = format!("category=linear&symbol={sym}");
        let Some(val) = self.get_v5("/v5/order/realtime", &q) else {
            return 0;
        };
        if val.get("retCode").and_then(|c| c.as_i64()) != Some(0) {
            return 0;
        }
        let Some(list) = val
            .get("result")
            .and_then(|r| r.get("list"))
            .and_then(|l| l.as_array())
        else {
            return 0;
        };
        let mut ids_to_cancel = Vec::new();
        for item in list {
            let ro = item
                .get("reduceOnly")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if !ro {
                if let Some(oid) = item.get("orderId").and_then(|v| v.as_str()) {
                    if !oid.is_empty() {
                        ids_to_cancel.push(oid.to_string());
                    }
                }
            }
        }
        let mut cancelled = 0_usize;
        for oid in &ids_to_cancel {
            if self.cancel_single_order_id(sym, oid) {
                cancelled += 1;
            }
        }
        cancelled
    }

    pub fn inspect_exit_and_purge_residual_entries(
        &mut self,
        spec: PairSpec,
    ) -> Option<(PriceTicks, LotQty)> {
        let sym = spec.symbol_id.name();
        let q = format!("category=linear&symbol={sym}");
        let val = self.get_v5("/v5/order/realtime", &q)?;
        if val.get("retCode").and_then(|c| c.as_i64()) != Some(0) {
            return None;
        }
        let list = val
            .get("result")
            .and_then(|r| r.get("list"))
            .and_then(|l| l.as_array())?;
        let mut exit_info = (PriceTicks(0), LotQty(0));
        let mut orphan_entry_ids = Vec::new();
        for item in list {
            let ro = item
                .get("reduceOnly")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if ro {
                if exit_info.0 .0 == 0 {
                    let px_s = item.get("price").and_then(|v| v.as_str()).unwrap_or("0");
                    let qt_s = item
                        .get("leavesQty")
                        .filter(|v| {
                            v.as_str().is_some_and(|s| {
                                s.bytes().any(|b| b.is_ascii_digit() && b != b'0')
                            })
                        })
                        .or_else(|| item.get("qty"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("0");
                    if let Ok(pt) = spec.parse_price_ticks(px_s) {
                        let lq = spec.parse_lot_qty(qt_s);
                        exit_info = (pt, lq);
                    }
                }
            } else if let Some(oid) = item.get("orderId").and_then(|v| v.as_str()) {
                if !oid.is_empty() {
                    orphan_entry_ids.push(oid.to_string());
                }
            }
        }
        for oid in &orphan_entry_ids {
            let _ = self.cancel_single_order_id(sym, oid);
        }
        Some(exit_info)
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

    pub fn enable_ws_trade_from_env(&mut self) {
        let env_map = load_dotenv_map();
        let ws_url = env_map
            .get("BYBIT_TRADE_WS_URL")
            .cloned()
            .unwrap_or_else(|| "wss://stream.bybit.com/v5/trade".to_string());
        let k = self.api_key.clone();
        let s = self.api_secret.clone();
        self.ws_trade.enable_with_url(ws_url, &k, &s);
    }

    pub fn maintain_ws_trade_heartbeat(&mut self) {
        let k = self.api_key.clone();
        let s = self.api_secret.clone();
        self.ws_trade.maintain_heartbeat(&k, &s);
    }

    #[must_use]
    pub fn authorizes_ws_mutation(
        &self,
        op: WsMutationOp,
        is_defensive_or_abort: bool,
    ) -> bool {
        self.ws_trade
            .authorizes_mutation(op, is_defensive_or_abort, now_epoch_ns().0)
    }

    pub fn place_order_ws_or_rest(
        &mut self,
        spec: PairSpec,
        order_side: Side,
        price: PriceTicks,
        qty: LotQty,
        sl_price: Option<PriceTicks>,
        reduce_only: bool,
        order_link_id: &str,
        is_entry_create: bool,
    ) -> i64 {
        if order_link_id.is_empty() {
            return self.place_post_only_order(
                spec,
                order_side,
                price,
                qty,
                sl_price,
                reduce_only,
            );
        }
        let sym = spec.symbol_id.name();
        let side_str = match order_side {
            Side::Bid => "Buy",
            Side::Ask => "Sell",
        };
        let px_str = spec.format_price_ticks_str(price);
        let qty_str = spec.format_lot_qty_str(qty);
        let mut arg_obj = serde_json::json!({
            "category": "linear",
            "symbol": sym,
            "side": side_str,
            "orderType": "Limit",
            "qty": qty_str,
            "price": px_str,
            "timeInForce": "PostOnly",
            "reduceOnly": reduce_only,
            "positionIdx": 0,
            "orderLinkId": order_link_id,
        });
        if let Some(sl_t) = sl_price {
            if !reduce_only && sl_t.0 > 0 {
                let sl_exact = spec.format_price_ticks_str(sl_t);
                if let Some(obj) = arg_obj.as_object_mut() {
                    obj.insert("stopLoss".to_string(), serde_json::Value::String(sl_exact));
                    obj.insert(
                        "slTriggerBy".to_string(),
                        serde_json::Value::String("MarkPrice".to_string()),
                    );
                    obj.insert(
                        "tpslMode".to_string(),
                        serde_json::Value::String("Full".to_string()),
                    );
                }
            }
        }
        let recv_ms = asymmetric_recv_window_ms(is_entry_create);
        let k = self.api_key.clone();
        let s = self.api_secret.clone();
        if let Some(ws_rc) = self.ws_trade.execute_ws_op(
            &k,
            &s,
            WsMutationOp::Create,
            "order.create",
            recv_ms,
            &arg_obj,
        ) {
            return if ws_rc == 110_072 { 0 } else { ws_rc };
        }
        let body = arg_obj.to_string();
        let rc = self
            .post_v5("/v5/order/create", &body)
            .and_then(|v| v.get("retCode")?.as_i64())
            .unwrap_or(-1);
        if rc == 110_072 { 0 } else { rc }
    }

    pub fn amend_order_ws_or_rest(
        &mut self,
        spec: PairSpec,
        order_link_id: &str,
        new_price: PriceTicks,
        new_total_qty: LotQty,
        new_sl_price: Option<PriceTicks>,
    ) -> i64 {
        if order_link_id.is_empty() || new_price.0 <= 0 || new_total_qty.0 <= 0 {
            return -1;
        }
        let mut arg_obj = serde_json::json!({
            "category": "linear",
            "symbol": spec.symbol_id.name(),
            "orderLinkId": order_link_id,
            "qty": spec.format_lot_qty_str(new_total_qty),
            "price": spec.format_price_ticks_str(new_price),
        });
        if let Some(sl_t) = new_sl_price {
            if sl_t.0 > 0 {
                let sl_exact = spec.format_price_ticks_str(sl_t);
                if let Some(obj) = arg_obj.as_object_mut() {
                    obj.insert("stopLoss".to_string(), serde_json::Value::String(sl_exact));
                    obj.insert(
                        "slTriggerBy".to_string(),
                        serde_json::Value::String("MarkPrice".to_string()),
                    );
                }
            }
        }
        let recv_ms = asymmetric_recv_window_ms(false);
        let k = self.api_key.clone();
        let s = self.api_secret.clone();
        if let Some(ws_rc) = self.ws_trade.execute_ws_op(
            &k,
            &s,
            WsMutationOp::Amend,
            "order.amend",
            recv_ms,
            &arg_obj,
        ) {
            return if ws_rc == 34_040 { 0 } else { ws_rc };
        }
        let body = arg_obj.to_string();
        let rc = self
            .post_v5("/v5/order/amend", &body)
            .and_then(|v| v.get("retCode")?.as_i64())
            .unwrap_or(-1);
        if rc == 34_040 { 0 } else { rc }
    }

    pub fn cancel_order_ws_or_rest(&mut self, spec: PairSpec, order_link_id: &str) -> i64 {
        if order_link_id.is_empty() {
            return -1;
        }
        let arg_obj = serde_json::json!({
            "category": "linear",
            "symbol": spec.symbol_id.name(),
            "orderLinkId": order_link_id,
        });
        let recv_ms = asymmetric_recv_window_ms(false);
        let k = self.api_key.clone();
        let s = self.api_secret.clone();
        if let Some(ws_rc) = self.ws_trade.execute_ws_op(
            &k,
            &s,
            WsMutationOp::Cancel,
            "order.cancel",
            recv_ms,
            &arg_obj,
        ) {
            return ws_rc;
        }
        let body = arg_obj.to_string();
        self.post_v5("/v5/order/cancel", &body)
            .and_then(|v| v.get("retCode")?.as_i64())
            .unwrap_or(-1)
    }

    /// P0-A Law V: Deterministic last-resort Market IOC reduceOnly flattener over WS + REST fallback.
    pub fn flatten_position_market_ioc(
        &mut self,
        spec: PairSpec,
        open_side: Side,
        qty: LotQty,
        order_link_id: &str,
    ) -> i64 {
        if qty.0 <= 0 {
            return -1;
        }
        let sym = spec.symbol_id.name();
        let close_side_str = match open_side.opposite() {
            Side::Bid => "Buy",
            Side::Ask => "Sell",
        };
        let qty_str = spec.format_lot_qty_str(qty);
        let mut arg_obj = serde_json::json!({
            "category": "linear",
            "symbol": sym,
            "side": close_side_str,
            "orderType": "Market",
            "qty": qty_str,
            "timeInForce": "IOC",
            "reduceOnly": true,
            "positionIdx": 0,
        });
        if !order_link_id.is_empty() {
            if let Some(obj) = arg_obj.as_object_mut() {
                obj.insert(
                    "orderLinkId".to_string(),
                    serde_json::Value::String(order_link_id.to_string()),
                );
            }
            let recv_ms = asymmetric_recv_window_ms(false);
            let k = self.api_key.clone();
            let s = self.api_secret.clone();
            if let Some(ws_rc) = self.ws_trade.execute_ws_op(
                &k,
                &s,
                WsMutationOp::Create,
                "order.create",
                recv_ms,
                &arg_obj,
            ) {
                if ws_rc == 0 || ws_rc == 110_017 || ws_rc == 110_072 {
                    return 0;
                }
            }
        }
        let body = arg_obj.to_string();
        let Some(resp) = self.post_v5("/v5/order/create", &body) else {
            return -1;
        };
        let rc = resp.get("retCode").and_then(|c| c.as_i64()).unwrap_or(-1);
        if rc == 0 || rc == 110_017 || rc == 110_072 {
            0
        } else {
            let rmsg = resp.get("retMsg").and_then(|m| m.as_str()).unwrap_or("unknown");
            println!(
                "{}\u{1f534} [BYBIT V5 EMERGENCY FLATTEN REJECTED] {} {} qty={} (rc={} msg={}){}",
                ANSI_RED, sym, close_side_str, qty_str, rc, rmsg, ANSI_RESET
            );
            rc
        }
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

    pub fn set_position_trading_stop(&mut self, spec: PairSpec, sl_ticks: PriceTicks) -> bool {
        if sl_ticks.0 <= 0 {
            return false;
        }
        let sl_str = spec.format_price_ticks_str(sl_ticks);
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
        let px_str = spec.format_price_ticks_str(price);
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
                let sl_exact = spec.format_price_ticks_str(sl_t);
                if let Some(obj) = payload.as_object_mut() {
                    obj.insert("stopLoss".to_string(), serde_json::Value::String(sl_exact));
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
        let rc = resp.get("retCode").and_then(|c| c.as_i64()).unwrap_or(-1);
        if rc != 0 && rc != 110_017 {
            let rmsg = resp.get("retMsg").and_then(|m| m.as_str()).unwrap_or("unknown");
            println!(
                "{}\u{1f7e0} [BYBIT V5 ORDER REJECTED] {} {} qty={} @ {} (ro={} rc={} msg={}){}",
                ANSI_ORANGE, sym, side_str, qty_str, px_str, reduce_only, rc, rmsg, ANSI_RESET
            );
        }
        rc
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::APPROVED_SPECS;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::thread;

    fn read_http_body(rdr: &mut BufReader<TcpStream>) -> Option<String> {
        let mut clen = 0_usize;
        loop {
            let mut line = String::new();
            match rdr.read_line(&mut line) {
                Ok(0) | Err(_) => return None,
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
        let mut body = vec![0_u8; clen];
        if clen > 0 && rdr.read_exact(&mut body).is_err() {
            return None;
        }
        String::from_utf8(body).ok()
    }

    #[test]
    fn test_bybit_client_34040_sl_residual_purge_and_p0a_market_ioc_flatten() {
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
            if read_http_body(&mut rdr).is_some() {
                let b1 = "{\"retCode\":34040,\"retMsg\":\"not modified\"}";
                let r1 = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{}",
                    b1.len(),
                    b1
                );
                let _ = writer.write_all(r1.as_bytes());
                let _ = writer.flush();
            }
            if let Some(flatten_req) = read_http_body(&mut rdr) {
                assert!(flatten_req.contains("\"orderType\":\"Market\""));
                assert!(flatten_req.contains("\"timeInForce\":\"IOC\""));
                assert!(flatten_req.contains("\"reduceOnly\":true"));
                assert!(flatten_req.contains("\"side\":\"Buy\""));
                let b2 = "{\"retCode\":0,\"retMsg\":\"OK\"}";
                let r2 = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    b2.len(),
                    b2
                );
                let _ = writer.write_all(r2.as_bytes());
                let _ = writer.flush();
            }
        });
        let base = format!("http://127.0.0.1:{}", addr.port());
        if let Some(mut client) =
            BybitV5LiveClient::with_base_url("k_test".to_string(), "s_test".to_string(), &base)
        {
            assert!(client.set_position_trading_stop(APPROVED_SPECS[3], PriceTicks(8_090)));
            let rc_flat = client.flatten_position_market_ioc(
                APPROVED_SPECS[3],
                Side::Ask,
                LotQty(26),
                "vx-p0a-flat-s03",
            );
            assert_eq!(rc_flat, 0);
        }
        let _ = srv.join();
    }
}
