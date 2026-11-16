#!/usr/bin/env python3
"""VORTEX-OMEGA V4.8 Persistent 20s/300s EMA & 5m Parkinson Volatility Prewarmer (3x HTTP Retry)."""
import json
import math
import os
import time
import urllib.request
from pathlib import Path

SPECS = [
    ("BTCUSDT", 10_000_000),
    ("ETHUSDT", 1_000_000),
    ("SOLUSDT", 1_000_000),
    ("AVAXUSDT", 100_000),
    ("TAOUSDT", 1_000_000),
    ("SUIUSDT", 10_000),
    ("ORDIUSDT", 100_000),
    ("PENDLEUSDT", 10_000),
    ("WIFUSDT", 10_000),
    ("LINKUSDT", 100_000),
    ("NEARUSDT", 100_000),
    ("LTCUSDT", 1_000_000),
]
PARKINSON_K = 0.600_561_20 * math.sqrt(5.0)


def decimal_to_ticks(val_str: str, tick_size_1e8: int) -> int:
    s = val_str.strip()
    if "." in s:
        w_s, f_s = s.split(".", 1)
        f_s = (f_s + "00000000")[:8]
    else:
        w_s, f_s = s, "00000000"
    total_1e8 = int(w_s or "0") * 100_000_000 + int(f_s)
    return max(1, (total_1e8 + tick_size_1e8 // 2) // tick_size_1e8)


def fetch_1m_klines(symbol: str, limit: int = 90) -> list:
    url = f"https://api.bybit.com/v5/market/kline?category=linear&symbol={symbol}&interval=1&limit={limit}"
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "vortex-omega-prewarm/4.8"})
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            rows = data.get("result", {}).get("list", [])
            if rows:
                return list(reversed(rows))
        except Exception:
            if attempt < 2:
                time.sleep(0.30 * (attempt + 1))
    return []


def main() -> int:
    repo_root = Path(__file__).resolve().parent.parent
    out_path = repo_root / "data" / "vault" / "vortex_ema_prewarm.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    fast_arr = [0] * 12
    slow_arr = [0] * 12
    vol_arr = [4_800] * 12
    hydrated = 0
    a_fast = 1.0 - math.exp(-60.0 / 20.0)
    a_slow = 1.0 - math.exp(-60.0 / 300.0)

    for idx, (sym, tick_1e8) in enumerate(SPECS):
        rows = fetch_1m_klines(sym, 90)
        if not rows:
            continue
        f_px = 0.0
        s_px = 0.0
        hl_samples = []
        for r in rows:
            hi_t = decimal_to_ticks(r[2], tick_1e8)
            lo_t = decimal_to_ticks(r[3], tick_1e8)
            cl_t = decimal_to_ticks(r[4], tick_1e8)
            px_1e4 = float(cl_t * 10_000)
            if f_px <= 0.0:
                f_px = px_1e4
                s_px = px_1e4
            else:
                f_px += a_fast * (px_1e4 - f_px)
                s_px += a_slow * (px_1e4 - s_px)
            hl_bps = ((max(0, hi_t - lo_t) * 1_000_000) / max(1, cl_t)) * PARKINSON_K
            hl_samples.append(hl_bps)
        fast_arr[idx] = int(round(f_px))
        slow_arr[idx] = int(round(s_px))
        recent_vol = sum(hl_samples[-15:]) / max(1, len(hl_samples[-15:]))
        vol_arr[idx] = max(2_500, min(24_000, int(round(recent_vol))))
        hydrated += 1

    if hydrated == 0:
        return 0
    now_ns = time.time_ns()
    payload = {
        "event": "EMA_PREWARM_V43",
        "ts_ns": now_ns,
        "ewma_fast_px_1e4": fast_arr,
        "ewma_slow_px_1e4": slow_arr,
        "eq_vol_5m_bps": vol_arr,
    }
    tmp_path = out_path.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    os.replace(tmp_path, out_path)
    print(f"[PREWARM V4.8 OK] {hydrated}/12 pares hidratados (90m 1m klines) -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
