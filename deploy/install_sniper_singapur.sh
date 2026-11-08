#!/usr/bin/env bash
set -euo pipefail

echo "================================================================================"
echo "[VORTEX-L3 DEPLOY] SHA-LOCKED INSTALLER FOR SNIPER-SINGAPUR (UBUNTU x86_64)"
echo "================================================================================"

VORTEX_DIR="/home/ubuntu/vortex-rs"
BIN_DIR="${VORTEX_DIR}/bin"
DATA_DIR="${VORTEX_DIR}/data"
VAULT_DIR="${DATA_DIR}/vault"
RELEASE_BASE="https://github.com/jevallejos/VORTEX-L2/releases/download/latest-linux-x86_64"
RELEASE_URL="${RELEASE_BASE}/vortex-linux-x86_64.tar.gz"
SHA_URL="${RELEASE_BASE}/COMMIT_SHA"

mkdir -p "${BIN_DIR}" "${DATA_DIR}" "${VAULT_DIR}"
cd "${VORTEX_DIR}"

TARGET_SHA=$(git rev-parse HEAD)
SHORT_TARGET=$(echo "${TARGET_SHA}" | cut -c1-7)
echo "[SHA LOCK] Target Git HEAD: ${TARGET_SHA}"

VERIFIED=0
for attempt in $(seq 1 36); do
    REMOTE_SHA=$(curl -fsSL --max-time 5 "${SHA_URL}" 2>/dev/null | tr -d '[:space:]' || true)
    if [ "${REMOTE_SHA}" = "${TARGET_SHA}" ]; then
        echo "[OK] Verified exact binary SHA match (${TARGET_SHA}) on attempt #${attempt} (0% CPU jitter)."
        VERIFIED=1
        break
    fi
    SHORT_REMOTE=$(echo "${REMOTE_SHA}" | cut -c1-7)
    echo "  [WAIT ${attempt}/36] Release binary SHA (${SHORT_REMOTE:-none}) != HEAD (${SHORT_TARGET}). GitHub Actions building... waiting 5s"
    sleep 5
done

if [ "${VERIFIED}" -ne 1 ]; then
    echo "[ABORT] Binary SHA verification timed out. Refusing unverified deploy."
    exit 1
fi

curl -fsSL --retry 3 --retry-delay 2 "${RELEASE_URL}" -o "/tmp/vortex-linux-x86_64.tar.gz"
sudo systemctl stop vortex-exec.service vortex-tap.service 2>/dev/null || true
rm -f "${VAULT_DIR}/DRAIN_MODE.lock" "${VAULT_DIR}/HALTED_ON_LOSS.lock"
tar -xzf "/tmp/vortex-linux-x86_64.tar.gz" -C "${BIN_DIR}"
rm -f "/tmp/vortex-linux-x86_64.tar.gz"
chmod +x "${BIN_DIR}/vortex_tap" "${BIN_DIR}/vortex_replay" "${BIN_DIR}/vortex_exec"

sudo tee /etc/systemd/system/vortex-tap.service > /dev/null <<EOF
[Unit]
Description=VORTEX-L3 Bybit UTA V5 12-Pair Public L2 + Trade Tape Seqlock Daemon (SHM v12)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=${VORTEX_DIR}
EnvironmentFile=-${VORTEX_DIR}/.env
ExecStart=${BIN_DIR}/vortex_tap --daemon
Restart=always
RestartSec=2
Nice=-10
OOMScoreAdjust=-800
LimitMEMLOCK=infinity

[Install]
WantedBy=multi-user.target
EOF

sudo tee /etc/systemd/system/vortex-exec.service > /dev/null <<EOF
[Unit]
Description=VORTEX-L3 200Hz Autonomous 12-Slot Execution Daemon (ZMPQR + LVDBS)
After=vortex-tap.service
Requires=vortex-tap.service

[Service]
Type=simple
User=ubuntu
WorkingDirectory=${VORTEX_DIR}
EnvironmentFile=-${VORTEX_DIR}/.env
ExecStart=${BIN_DIR}/vortex_exec --daemon --live-capital
Restart=always
RestartSec=2
Nice=-15
OOMScoreAdjust=-800
LimitMEMLOCK=infinity

[Install]
WantedBy=multi-user.target
EOF

sudo tee /usr/local/bin/vortex > /dev/null << 'VORTEX_CLI_EOF'
#!/usr/bin/env bash
set -euo pipefail

VORTEX_DIR="/home/ubuntu/vortex-rs"
BIN_DIR="/home/ubuntu/vortex-rs/bin"

case "${1:-live}" in
    360)
        echo "=== [1. VORTEX-L3 LIVE HUD (SHM v12)] ==="
        cd "${VORTEX_DIR}" && "${BIN_DIR}/vortex_exec" --live
        echo "=== [2. BYBIT V5 LIVE ORDERS & QUEUE TELEMETRY] ==="
        journalctl -u vortex-exec.service -n 35 --no-pager | grep -E "VORTEX-L3|BYBIT V5|QUEUE|FILL|CLOSE|ADOPT|REHYDRATE|DRAIN" || true
        echo "=== [3. CLOSED TRADES LEDGER & EXECUTIVE DASHBOARD] ==="
        cd "${VORTEX_DIR}" && "${BIN_DIR}/vortex_exec" --trades
        echo "=== [4. WALK-FORWARD PAR1 REPLAY & A/B/A+B ECONOMIC LAB] ==="
        cd "${VORTEX_DIR}" && "${BIN_DIR}/vortex_replay"
        echo "=== [5. AI FORENSIC VAULT (last 15 records)] ==="
        tail -n 15 "${VORTEX_DIR}/data/vault/vortex_ai_forensic_vault.jsonl" 2>/dev/null || true
        echo "=== [6. RED LOSS FORENSIC BOX] ==="
        cat "${VORTEX_DIR}/data/vault/vortex_red_loss_box.jsonl" 2>/dev/null || true
        ;;
    drain|drenar)
        mkdir -p "${VORTEX_DIR}/data/vault"
        touch "${VORTEX_DIR}/data/vault/DRAIN_MODE.lock"
        echo "🟢 [MODO DRENAJE ACTIVADO] Custodiando en verde las posiciones abiertas hasta 0..."
        sleep 1
        cd "${VORTEX_DIR}" && "${BIN_DIR}/vortex_exec" --live
        ;;
    normal|resume|undrain)
        rm -f "${VORTEX_DIR}/data/vault/DRAIN_MODE.lock" "${VORTEX_DIR}/data/vault/HALTED_ON_LOSS.lock"
        if ! systemctl is-active --quiet vortex-exec.service; then
            sudo systemctl start vortex-exec.service
            sleep 1
        fi
        echo "🟢 [MODO NORMAL RESTAURADO] 12/12 motores autonomos activos en vivo."
        cd "${VORTEX_DIR}" && "${BIN_DIR}/vortex_exec" --live
        ;;
    live|-l)
        cd "${VORTEX_DIR}" && "${BIN_DIR}/vortex_exec" --live
        ;;
    trades|-t)
        cd "${VORTEX_DIR}" && "${BIN_DIR}/vortex_exec" --trades
        ;;
    replay|-r)
        cd "${VORTEX_DIR}" && "${BIN_DIR}/vortex_replay"
        ;;
    status|-s)
        systemctl status vortex-tap.service vortex-exec.service --no-pager
        ;;
    all)
        cd "${VORTEX_DIR}"
        "${BIN_DIR}/vortex_tap"
        "${BIN_DIR}/vortex_replay"
        "${BIN_DIR}/vortex_exec"
        ;;
    *)
        echo "Usage: vortex {360|drain|normal|live|trades|replay|status|all}"
        exit 1
        ;;
esac
VORTEX_CLI_EOF

sudo chmod +x /usr/local/bin/vortex
sudo systemctl daemon-reload
sudo systemctl enable vortex-tap.service vortex-exec.service >/dev/null 2>&1 || true
sudo systemctl restart vortex-tap.service
sleep 2
sudo systemctl restart vortex-exec.service
sleep 1
echo "[OK] VORTEX-L3 Daemons active (Commit SHA Verified)."
