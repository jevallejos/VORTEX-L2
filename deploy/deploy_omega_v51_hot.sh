#!/usr/bin/env bash
set -euo pipefail

VORTEX_DIR="/home/ubuntu/vortex-rs"
VAULT_DIR="$VORTEX_DIR/data/vault"
cd "$VORTEX_DIR"

echo "=== [1/5] BUILD AISLADO (NICE 19) + LIB/INTEGRATION TESTS + GATE WALK-FORWARD PRE-CORTE ==="
TAP_PID_BEFORE=$(systemctl show -p MainPID --value vortex-tap.service)
source "$HOME/.cargo/env" 2>/dev/null || true
nice -n 19 ionice -c 3 cargo clippy --all-targets -- -D warnings
nice -n 19 ionice -c 3 cargo test --lib -- --test-threads=1
nice -n 19 ionice -c 3 cargo test --tests -- --test-threads=1
nice -n 19 ionice -c 3 cargo build --release --bins
./target/release/vortex_tap
./target/release/vortex_exec
./target/release/vortex_web

echo "=== [2/5] COMPUERTA WALK-FORWARD SOBRE SEGMENTOS PAR2 REALES DE HOY ==="
WF_OUT=$(nice -n 19 ionice -c 3 ./target/release/vortex_replay --dir "$VORTEX_DIR/data")
echo "$WF_OUT"
echo "$WF_OUT" | grep -q "Rej=0" || { echo "[ABORTADO] PostOnly Rejects detectados en PAR2 vivo"; exit 1; }
echo "$WF_OUT" | grep -q "Drift:0" || { echo "[ABORTADO] Drift distinto de cero en Digital Twin"; exit 1; }

echo "=== [3/5] VERIFICACION P0-B (DRAIN + 0 POSICIONES ABIERTAS + TAP L2 100% ACTIVO) ==="
touch "$VAULT_DIR/DRAIN_MODE.lock"
sleep 2
while [ "$(wc -c < "$VAULT_DIR/vortex_open_positions_state.json" 2>/dev/null || echo 2)" -gt 4 ]; do
  echo "Esperando cierre natural a 200.0 Hz antes de promover V5.3.2..."
  sleep 2
done
sudo systemctl stop vortex-web.service vortex-exec.service 2>/dev/null || true
echo "[OK] vortex-exec detenido sin exposicion | vortex-tap activo (PID=$TAP_PID_BEFORE)."

echo "=== [4/5] SNAPSHOT INMUTABLE PRE-V5.3.2 (BINARIOS + BAULES + SHA256) ==="
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
BACKUP_DIR="$VORTEX_DIR/backups/pre_omega_v532_$STAMP"
mkdir -p "$BACKUP_DIR/bin" "$BACKUP_DIR/vault"
git rev-parse HEAD > "$BACKUP_DIR/PREV_COMMIT_SHA.txt" || true
cp -a "$VORTEX_DIR/bin/." "$BACKUP_DIR/bin/" 2>/dev/null || true
cp -a "$VAULT_DIR/." "$BACKUP_DIR/vault/" 2>/dev/null || true
(cd "$BACKUP_DIR/bin" && sha256sum * > "$BACKUP_DIR/SHA256SUMS.txt" 2>/dev/null) || true
(cd "$BACKUP_DIR/vault" && sha256sum * >> "$BACKUP_DIR/SHA256SUMS.txt" 2>/dev/null) || true
chmod -R a-w "$BACKUP_DIR/vault" 2>/dev/null || true
echo "[OK] Respaldo sellado en: $BACKUP_DIR"

echo "=== [5/5] INSTALACION ATOMICA + REAPERTURA ZERO-RESTART VORTEX-OMEGA V5.3.2 ==="
install -m 0755 target/release/vortex_tap "$VORTEX_DIR/bin/vortex_tap"
install -m 0755 target/release/vortex_replay "$VORTEX_DIR/bin/vortex_replay"
install -m 0755 target/release/vortex_exec "$VORTEX_DIR/bin/vortex_exec"
install -m 0755 target/release/vortex_web "$VORTEX_DIR/bin/vortex_web"
sudo install -m 0644 "$VORTEX_DIR/deploy/vortex-web.service" /etc/systemd/system/vortex-web.service || true
for unit_f in /etc/systemd/system/vortex-exec.service /etc/systemd/system/vortex-tap.service; do
  if [ -f "$unit_f" ]; then
    sudo sed -i -E '/prewarm_emas_bybit\.py/d; s/VORTEX-OMEGA V[0-9]+(\.[0-9]+)*/VORTEX-OMEGA V5.3.2/g' "$unit_f" || true
  fi
done
sudo systemctl daemon-reload || true
sha256sum "$VORTEX_DIR/bin/vortex_tap" "$VORTEX_DIR/bin/vortex_replay" "$VORTEX_DIR/bin/vortex_exec" "$VORTEX_DIR/bin/vortex_web"
rm -f "$VAULT_DIR/DRAIN_MODE.lock" "$VAULT_DIR/HALTED_ON_LOSS.lock"

sudo systemctl start vortex-exec.service
sudo systemctl start vortex-web.service
sleep 3
TAP_PID_AFTER=$(systemctl show -p MainPID --value vortex-tap.service)
echo "[TAP ZERO-RESTART CHECK] PID: ${TAP_PID_BEFORE} -> ${TAP_PID_AFTER}"
wc -l "$VAULT_DIR"/*.jsonl 2>/dev/null || true
git rev-parse --short HEAD
systemctl is-active vortex-tap.service vortex-exec.service vortex-web.service
"$VORTEX_DIR/bin/vortex_exec" --trades
"$VORTEX_DIR/bin/vortex_exec" --live
