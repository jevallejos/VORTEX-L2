#!/usr/bin/env bash
# VORTEX-OMEGA V4.9 — Deferred Production Promotion Script (sniper-singapur)
# LAW IX GUARD: Execute ONLY after completing >= 25/25 closed trades in V4.8 and fast-forwarding main.
set -uo pipefail

VORTEX_DIR="/home/ubuntu/vortex-rs"
VAULT_DIR="$VORTEX_DIR/data/vault"
FORENSIC_JSONL="$VAULT_DIR/vortex_ai_forensic_vault.jsonl"
cd "$VORTEX_DIR" || exit 1
mkdir -p "$VAULT_DIR" "$VORTEX_DIR/bin" "$VORTEX_DIR/backups" "$VORTEX_DIR/data/candles" "$VORTEX_DIR/data/archive"

echo "=== [0/6] COMPUERTA LEY IX: VERIFICACION DE MUESTRA ESTACIONARIA (>= 25/25 TIROS) ==="
CLOSED_COUNT=0
if [ -f "$FORENSIC_JSONL" ]; then
  CLOSED_COUNT=$(awk '/"event":"EPOCH_RESET"/{c=0; next} /"event":"TRADE_CLOSE"/{c++} END{print c+0}' "$FORENSIC_JSONL" 2>/dev/null || echo 0)
fi
if [ "$CLOSED_COUNT" -lt 25 ] && [ "${1:-}" != "--force-override-law9" ]; then
  echo "[BLOQUEADO POR LEY IX] Muestra actual en V4.8: ${CLOSED_COUNT}/25 tiros cerrados."
  echo "Mantener sniper-singapur congelado en V4.8 hasta >= 25/25 antes de promover V4.9."
  exit 1
fi
echo "[OK] Muestra estacionaria verificada (${CLOSED_COUNT}/25 tiros)."

echo "=== [1/6] RESGUARDO P0-B: DRAIN_MODE CON 0 POSICIONES ABIERTAS ==="
touch "$VAULT_DIR/DRAIN_MODE.lock"
sleep 1
while [ "$(wc -c < "$VAULT_DIR/vortex_open_positions_state.json" 2>/dev/null || echo 2)" -gt 4 ]; do
  echo "Posicion viva detectada en custodia; esperando cierre natural a 200.0 Hz..."
  sleep 2
done

sudo systemctl stop vortex-web.service vortex-exec.service vortex-tap.service 2>/dev/null || true
sleep 1
pkill -TERM -f "/home/ubuntu/vortex-rs/bin/vortex_" 2>/dev/null || true
sleep 1
if pgrep -f "/home/ubuntu/vortex-rs/bin/vortex_" >/dev/null 2>&1; then
  echo "[ERROR] Procesos vortex siguen vivos; abortando corte por seguridad."
  exit 1
fi
echo "[OK] 0 posiciones abiertas verificadas y demonios detenidos limpiamente."

echo "=== [2/6] SNAPSHOT INMUTABLE PRE-OMEGA V4.9 (BINARIOS + VAULT + SHA256) ==="
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
BACKUP_DIR="$VORTEX_DIR/backups/pre_omega_v49_$STAMP"
mkdir -p "$BACKUP_DIR/bin" "$BACKUP_DIR/vault"
git rev-parse HEAD > "$BACKUP_DIR/PREV_COMMIT_SHA.txt" 2>/dev/null || true
cp -a "$VORTEX_DIR/bin/." "$BACKUP_DIR/bin/" 2>/dev/null || true
cp -a "$VAULT_DIR/." "$BACKUP_DIR/vault/" 2>/dev/null || true
(cd "$BACKUP_DIR/bin" && sha256sum * > "$BACKUP_DIR/SHA256SUMS.txt" 2>/dev/null) || true
chmod -R a-w "$BACKUP_DIR/vault" 2>/dev/null || true
echo "[OK] Respaldo inmutable sellado en: $BACKUP_DIR"

echo "=== [3/6] GIT PULL ORIGIN/MAIN + CLIPPY + 72 TESTS (/dev/shm) + BUILD 4 BINARIOS LTO FAT ==="
git fetch origin main || exit 1
git checkout main 2>/dev/null || true
git reset --hard origin/main || exit 1
chmod +x "$VORTEX_DIR/deploy/prewarm_emas_bybit.py" "$VORTEX_DIR/deploy/install_sniper_singapur.sh" "$VORTEX_DIR/deploy/deploy_omega_v49_observatory.sh"
source "$HOME/.cargo/env" 2>/dev/null || true
nice -n 19 ionice -c 3 cargo clippy --all-targets -- -D warnings || exit 1
nice -n 19 ionice -c 3 cargo test --lib -- --test-threads=1 || exit 1
nice -n 19 ionice -c 3 cargo test --tests -- --test-threads=1 || exit 1
nice -n 19 ionice -c 3 cargo build --release --bins || exit 1

echo "=== [4/6] CERTIFICACION DE LAS 4 COMPUERTAS CLI Y UNIDADES SYSTEMD V4.9 ==="
./target/release/vortex_tap || exit 1
mkdir -p /tmp/vortex_empty_par2_gate
./target/release/vortex_replay --dir /tmp/vortex_empty_par2_gate || exit 1
rmdir /tmp/vortex_empty_par2_gate 2>/dev/null || true
./target/release/vortex_exec || exit 1
./target/release/vortex_web || exit 1

install -m 0755 target/release/vortex_tap "$VORTEX_DIR/bin/vortex_tap" || exit 1
install -m 0755 target/release/vortex_replay "$VORTEX_DIR/bin/vortex_replay" || exit 1
install -m 0755 target/release/vortex_exec "$VORTEX_DIR/bin/vortex_exec" || exit 1
install -m 0755 target/release/vortex_web "$VORTEX_DIR/bin/vortex_web" || exit 1
sha256sum "$VORTEX_DIR/bin/vortex_tap" "$VORTEX_DIR/bin/vortex_replay" "$VORTEX_DIR/bin/vortex_exec" "$VORTEX_DIR/bin/vortex_web"

sudo tee /etc/systemd/system/vortex-tap.service > /dev/null <<UNIT_TAP
[Unit]
Description=VORTEX-OMEGA V4.9 Bybit UTA V5 12-Pair L2 + PAR2Z + VKL1 Daemon (SHM v15)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=${VORTEX_DIR}
EnvironmentFile=-${VORTEX_DIR}/.env
ExecStart=${VORTEX_DIR}/bin/vortex_tap --daemon
Restart=always
RestartSec=2
Nice=-10
OOMScoreAdjust=-800
LimitMEMLOCK=infinity

[Install]
WantedBy=multi-user.target
UNIT_TAP

sudo tee /etc/systemd/system/vortex-exec.service > /dev/null <<UNIT_EXEC
[Unit]
Description=VORTEX-OMEGA V4.9 200Hz Zero-Jitter 12-Slot Execution Daemon (SHM v15)
After=vortex-tap.service
Requires=vortex-tap.service

[Service]
Type=simple
User=ubuntu
WorkingDirectory=${VORTEX_DIR}
EnvironmentFile=-${VORTEX_DIR}/.env
ExecStartPre=-/usr/bin/python3 ${VORTEX_DIR}/deploy/prewarm_emas_bybit.py
ExecStart=${VORTEX_DIR}/bin/vortex_exec --daemon --live-capital
Restart=always
RestartSec=2
Nice=-15
OOMScoreAdjust=-800
LimitMEMLOCK=infinity

[Install]
WantedBy=multi-user.target
UNIT_EXEC

sudo install -m 0644 "$VORTEX_DIR/deploy/vortex-web.service" /etc/systemd/system/vortex-web.service || exit 1
if [ -f /usr/local/bin/vortex ]; then
  sudo sed -i 's/OMEGA V4\.8/OMEGA V4.9/g; s/OMEGA V4\.7/OMEGA V4.9/g' /usr/local/bin/vortex || true
fi
sudo systemctl daemon-reload
sudo systemctl enable vortex-tap.service vortex-exec.service vortex-web.service >/dev/null 2>&1 || true

echo "=== [5/6] RECREACION LIMPIA SHM v15 + ARRANQUE 3 DEMONIOS ==="
rm -f /dev/shm/vortex_lob.mmap
rm -f "$VAULT_DIR/DRAIN_MODE.lock" "$VAULT_DIR/HALTED_ON_LOSS.lock"

sudo systemctl start vortex-tap.service || exit 1
echo "Hidratando SHM v15 + VKL1 (1s..1d) + Elliott C1 + [OB+, OB-]..."
sleep 7
ls -lh /dev/shm/vortex_lob.mmap || exit 1

echo "=== [6/6] ENCENDIENDO CUSTODIO 200.0 Hz Y OBSERVATORIO 127.0.0.1:8787 (Nice=19) ==="
sudo systemctl start vortex-exec.service || exit 1
sudo systemctl start vortex-web.service || exit 1
sleep 2
systemctl is-active vortex-tap.service vortex-exec.service vortex-web.service
curl -fsS http://127.0.0.1:8787/api/state >/dev/null && echo "[OK] Loopback Observatory HTTP 127.0.0.1:8787 ONLINE"
"$VORTEX_DIR/bin/vortex_exec" --trades
"$VORTEX_DIR/bin/vortex_exec" --live
echo "[TUNEL SSH WINDOWS] Ejecutar en PowerShell: ssh -N -L 8787:127.0.0.1:8787 sniper-singapur"
