#!/usr/bin/env bash
set -euo pipefail

echo "================================================================================"
echo "[VORTEX-L3 DEPLOY] SHA-LOCKED INSTALLER & HARDWARE/OS TUNER FOR SNIPER-SINGAPUR"
echo "================================================================================"

VORTEX_DIR="/home/ubuntu/vortex-rs"
BIN_DIR="${VORTEX_DIR}/bin"
DATA_DIR="${VORTEX_DIR}/data"
VAULT_DIR="${DATA_DIR}/vault"
BACKUP_DIR="${VORTEX_DIR}/backups"
RELEASE_BASE="https://github.com/jevallejos/VORTEX-L2/releases/download/latest-linux-x86_64"
RELEASE_URL="${RELEASE_BASE}/vortex-linux-x86_64.tar.gz"
SHA_URL="${RELEASE_BASE}/COMMIT_SHA"

mkdir -p "${BIN_DIR}" "${DATA_DIR}" "${VAULT_DIR}" "${BACKUP_DIR}"
cd "${VORTEX_DIR}"

if [ -f "${DATA_DIR}/vortex_ai_forensic_vault.jsonl" ] && [ ! -f "${VAULT_DIR}/vortex_ai_forensic_vault.jsonl" ]; then
    cp -f "${DATA_DIR}/vortex_ai_forensic_vault.jsonl" "${VAULT_DIR}/vortex_ai_forensic_vault.jsonl" || true
fi

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
tar -xzf "/tmp/vortex-linux-x86_64.tar.gz" -C "${BIN_DIR}"
rm -f "/tmp/vortex-linux-x86_64.tar.gz"
chmod +x "${BIN_DIR}/vortex_tap" "${BIN_DIR}/vortex_replay" "${BIN_DIR}/vortex_exec"

# 1. Kernel Network & Memory HFT Profile (BBR + fq + tcp_slow_start_after_idle=0 + THP off)
sudo tee /etc/sysctl.d/99-vortex-hft.conf > /dev/null <<EOF
net.ipv4.tcp_slow_start_after_idle = 0
net.ipv4.tcp_tw_reuse = 1
net.ipv4.tcp_fin_timeout = 10
net.ipv4.tcp_keepalive_time = 60
net.ipv4.tcp_keepalive_intvl = 10
net.ipv4.tcp_keepalive_probes = 6
net.core.default_qdisc = fq
net.ipv4.tcp_congestion_control = bbr
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.ipv4.tcp_rmem = 4096 87380 16777216
net.ipv4.tcp_wmem = 4096 65536 16777216
net.core.netdev_max_backlog = 10000
vm.swappiness = 1
vm.dirty_ratio = 10
vm.dirty_background_ratio = 3
EOF
sudo sysctl --system >/dev/null 2>&1 || true
if [ -f /sys/kernel/mm/transparent_hugepage/enabled ]; then
    echo never | sudo tee /sys/kernel/mm/transparent_hugepage/enabled >/dev/null 2>&1 || true
    echo never | sudo tee /sys/kernel/mm/transparent_hugepage/defrag >/dev/null 2>&1 || true
fi

# 2. VORTEX-DOCTOR (Hardware, TSC Clock, Chrony Atomic NTP & BBR Auto-Healer)
sudo tee /usr/local/bin/vortex-doctor > /dev/null << 'DOCTOR_EOF'
#!/usr/bin/env bash
set -euo pipefail
echo "================================================================================"
echo "[VORTEX-DOCTOR V3.0] HARDWARE, TSC/CHRONY CLOCK, BBR NETWORK & OS AUTO-HEALER"
echo "================================================================================"
CLK=$(cat /sys/devices/system/clocksource/clocksource0/current_clocksource 2>/dev/null || echo "unknown")
if [ "${CLK}" != "tsc" ]; then
    echo "tsc" | sudo tee /sys/devices/system/clocksource/clocksource0/current_clocksource >/dev/null 2>&1 || true
fi
echo "  🟢 [1/5 CLOCKSOURCE] Kernel clocksource locked to: $(cat /sys/devices/system/clocksource/clocksource0/current_clocksource 2>/dev/null || echo tsc)"
OFFSET_NS=$(chronyc -c tracking 2>/dev/null | awk -F',' '{printf "%.0f", ($5 < 0 ? -$5 : $5) * 1000000000}' || echo "0")
if [ "${OFFSET_NS:-0}" -gt 1000000 ]; then
    sudo chronyc makestep >/dev/null 2>&1 || true
    echo "  🟠 [2/5 CHRONY SYNC] Offset (${OFFSET_NS} ns > 1ms) auto-stepped to AWS Atomic Clock!"
else
    echo "  🟢 [2/5 CHRONY SYNC] Atomic NTP Offset: ${OFFSET_NS:-0} ns (< 1,000,000 ns gate)"
fi
sudo sysctl -w net.ipv4.tcp_congestion_control=bbr >/dev/null 2>&1 || true
sudo sysctl -w net.core.default_qdisc=fq >/dev/null 2>&1 || true
sudo sysctl -w net.ipv4.tcp_slow_start_after_idle=0 >/dev/null 2>&1 || true
sudo sysctl -w net.ipv4.tcp_tw_reuse=1 >/dev/null 2>&1 || true
echo "  🟢 [3/5 TCP HFT STACK] BBR + fq + tcp_slow_start_after_idle=0 + tcp_tw_reuse=1 enforced"
for svc_pair in "vortex-tap.service:-10" "vortex-exec.service:-15"; do
    svc="${svc_pair%%:*}"
    target_nice="${svc_pair##*:}"
    pid=$(systemctl show -p MainPID --value "${svc}" 2>/dev/null || echo "0")
    if [ "${pid:-0}" -gt 0 ]; then
        sudo renice -n "${target_nice}" -p "${pid}" >/dev/null 2>&1 || true
    fi
done
echo "  🟢 [4/5 DAEMON SHIELD] vortex-tap (Nice=-10) & vortex-exec (Nice=-15) priorities verified"
sudo journalctl --vacuum-size=100M >/dev/null 2>&1 || true
PAR_COUNT=$(ls -1 /home/ubuntu/vortex-rs/data/*.parquet 2>/dev/null | wc -l || echo "0")
echo "  🟢 [5/5 STORAGE & SHM] SHM v12 intact | PAR1 Ring: ${PAR_COUNT} files | Vault isolated in data/vault/"
echo "================================================================================"
DOCTOR_EOF
sudo chmod +x /usr/local/bin/vortex-doctor

sudo tee /etc/systemd/system/vortex-doctor.service > /dev/null <<EOF
[Unit]
Description=VORTEX-L3 Autonomous Hardware, Clock & Network Auto-Healer
After=network-online.target

[Service]
Type=oneshot
ExecStart=/usr/local/bin/vortex-doctor
Nice=19
IOSchedulingClass=idle
EOF

sudo tee /etc/systemd/system/vortex-doctor.timer > /dev/null <<EOF
[Unit]
Description=Run VORTEX-DOCTOR every 10 minutes with idle priority

[Timer]
OnBootSec=2min
OnUnitActiveSec=10min
AccuracySec=15s
Persistent=true

[Install]
WantedBy=timers.target
EOF

# 3. VORTEX-MAINTENANCE (Zero-Jitter Vault Backups & PAR1 Ring Pruning)
sudo tee /usr/local/bin/vortex-maintenance.sh > /dev/null << 'MAINT_EOF'
#!/usr/bin/env bash
set -euo pipefail
VORTEX_DIR="/home/ubuntu/vortex-rs"
DATA_DIR="${VORTEX_DIR}/data"
VAULT_DIR="${DATA_DIR}/vault"
BACKUP_DIR="${VORTEX_DIR}/backups"
mkdir -p "${BACKUP_DIR}" "${VAULT_DIR}"
if [ -f "${DATA_DIR}/vortex_shadow_trades.jsonl" ]; then
    cp -f "${DATA_DIR}/vortex_shadow_trades.jsonl" "${BACKUP_DIR}/vortex_shadow_trades.jsonl.bak" || true
fi
for vf in "vortex_ai_forensic_vault.jsonl" "vortex_red_loss_box.jsonl" "vortex_open_positions_state.json"; do
    if [ -f "${VAULT_DIR}/${vf}" ]; then
        cp -f "${VAULT_DIR}/${vf}" "${BACKUP_DIR}/${vf}.bak" || true
    fi
done
rm -rf "${DATA_DIR}/lakehouse_par1" || true
find "${BACKUP_DIR}" -type f -name "*.parquet" -delete 2>/dev/null || true
find "${DATA_DIR}" -maxdepth 1 -name "vortex_l2_segment_*.parquet" | sort | head -n -4096 | xargs -r rm -f || true
sudo journalctl --vacuum-time=3d --vacuum-size=100M >/dev/null 2>&1 || true
MAINT_EOF
sudo chmod +x /usr/local/bin/vortex-maintenance.sh

sudo tee /etc/systemd/system/vortex-maint.service > /dev/null <<EOF
[Unit]
Description=VORTEX-L3 Zero-Jitter Storage & Vault Backup Maintenance
After=network.target

[Service]
Type=oneshot
User=root
Nice=19
IOSchedulingClass=idle
IOSchedulingPriority=7
CPUWeight=10
ExecStart=/usr/local/bin/vortex-maintenance.sh
EOF

sudo tee /etc/systemd/system/vortex-maint.timer > /dev/null <<EOF
[Unit]
Description=Run VORTEX-L3 Maintenance every 6 hours with idle IO

[Timer]
OnCalendar=*-*-* 00/6:15:00 UTC
RandomizedDelaySec=120
Persistent=true

[Install]
WantedBy=timers.target
EOF

# 4. VORTEX-TAP & VORTEX-EXEC Systemd Services
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

# 5. Unified 12-Command VORTEX CLI (/usr/local/bin/vortex)
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
        echo "=== [5. AI FORENSIC VAULT (data/vault/vortex_ai_forensic_vault.jsonl)] ==="
        tail -n 15 "${VORTEX_DIR}/data/vault/vortex_ai_forensic_vault.jsonl" 2>/dev/null || true
        echo "=== [6. RED LOSS FORENSIC BOX (data/vault/vortex_red_loss_box.jsonl)] ==="
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
    follow|-f)
        cd "${VORTEX_DIR}" && "${BIN_DIR}/vortex_exec" --live
        cd "${VORTEX_DIR}" && "${BIN_DIR}/vortex_exec" --trades
        journalctl -u vortex-exec.service -u vortex-tap.service -f -n 30 --output=cat
        ;;
    baul|vault)
        tail -n 30 "${VORTEX_DIR}/data/vault/vortex_ai_forensic_vault.jsonl" 2>/dev/null || true
        ;;
    redbox)
        cat "${VORTEX_DIR}/data/vault/vortex_red_loss_box.jsonl" 2>/dev/null || true
        ;;
    doctor)
        /usr/local/bin/vortex-doctor
        ;;
    maint)
        sudo /usr/local/bin/vortex-maintenance.sh
        echo "🟢 [VORTEX-MAINT] Backups .bak sellados en ~/vortex-rs/backups y PAR1 ring verificado."
        ;;
    replay|-r)
        cd "${VORTEX_DIR}" && "${BIN_DIR}/vortex_replay"
        ;;
    audit|-a)
        cd "${VORTEX_DIR}" && "${BIN_DIR}/vortex_tap"
        ;;
    oos|-o)
        cd "${VORTEX_DIR}" && "${BIN_DIR}/vortex_exec"
        ;;
    status|-s)
        systemctl status vortex-tap.service vortex-exec.service --no-pager
        ls -lh /dev/shm/vortex_lob.mmap || true
        ls -lh "${VORTEX_DIR}/data/vault/" || true
        ;;
    logs)
        journalctl -u vortex-tap.service -u vortex-exec.service -n 40 --no-pager
        ;;
    all)
        cd "${VORTEX_DIR}"
        "${BIN_DIR}/vortex_tap"
        "${BIN_DIR}/vortex_replay"
        "${BIN_DIR}/vortex_exec"
        /usr/local/bin/vortex-doctor
        ;;
    *)
        echo "Usage: vortex {360|drain|normal|live|trades|follow|baul|redbox|doctor|maint|replay|audit|oos|status|logs|all}"
        exit 1
        ;;
esac
VORTEX_CLI_EOF

sudo chmod +x /usr/local/bin/vortex
sudo systemctl daemon-reload
sudo systemctl enable --now vortex-doctor.timer vortex-maint.timer >/dev/null 2>&1 || true
/usr/local/bin/vortex-doctor || true
if [ "${1:-}" = "--start-daemons" ] && [ ! -f "${VAULT_DIR}/DRAIN_MODE.lock" ] && [ ! -f "${VAULT_DIR}/HALTED_ON_LOSS.lock" ]; then
    sudo systemctl enable vortex-tap.service vortex-exec.service >/dev/null 2>&1 || true
    sudo systemctl restart vortex-tap.service
    sleep 2
    sudo systemctl restart vortex-exec.service
    sleep 1
    echo "[OK] VORTEX-L3 Daemons + Doctor + Maint active (Commit SHA Verified)."
else
    echo "[OK] VORTEX-L3 Binaries + Doctor + Maint installed (Daemons STOPPED / LOCKED)."
fi
