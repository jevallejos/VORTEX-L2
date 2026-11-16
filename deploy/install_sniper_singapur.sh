#!/usr/bin/env bash
set -euo pipefail

cd /home/ubuntu/vortex-rs
mkdir -p /home/ubuntu/vortex-rs/bin /home/ubuntu/vortex-rs/data/vault /home/ubuntu/vortex-rs/backups
chmod +x /home/ubuntu/vortex-rs/deploy/prewarm_emas_bybit.py

nice -n 19 ionice -c 3 cargo clippy --all-targets -- -D warnings
nice -n 19 ionice -c 3 cargo test --lib -- --test-threads=1
nice -n 19 ionice -c 3 cargo build --release --bins

./target/release/vortex_tap
mkdir -p /tmp/vortex_empty_par2_gate
./target/release/vortex_replay --dir /tmp/vortex_empty_par2_gate
rmdir /tmp/vortex_empty_par2_gate
./target/release/vortex_exec

install -m 0755 target/release/vortex_tap /home/ubuntu/vortex-rs/bin/vortex_tap
install -m 0755 target/release/vortex_replay /home/ubuntu/vortex-rs/bin/vortex_replay
install -m 0755 target/release/vortex_exec /home/ubuntu/vortex-rs/bin/vortex_exec

sudo tee /etc/systemd/system/vortex-tap.service > /dev/null <<UNIT_TAP
[Unit]
Description=VORTEX-OMEGA V4.7 Bybit UTA V5 12-Pair L2 + PAR2 + OB+- Daemon (SHM v15)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/vortex-rs
EnvironmentFile=-/home/ubuntu/vortex-rs/.env
ExecStart=/home/ubuntu/vortex-rs/bin/vortex_tap --daemon
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
Description=VORTEX-OMEGA V4.7 200Hz Autonomous 12-Slot Execution Daemon (SHM v15)
After=vortex-tap.service
Requires=vortex-tap.service

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/vortex-rs
EnvironmentFile=-/home/ubuntu/vortex-rs/.env
ExecStartPre=-/usr/bin/python3 /home/ubuntu/vortex-rs/deploy/prewarm_emas_bybit.py
ExecStart=/home/ubuntu/vortex-rs/bin/vortex_exec --daemon --live-capital
Restart=always
RestartSec=2
Nice=-15
OOMScoreAdjust=-800
LimitMEMLOCK=infinity

[Install]
WantedBy=multi-user.target
UNIT_EXEC

sudo systemctl daemon-reload
echo "[OK] VORTEX-OMEGA V4.7 (SHM v15 + PAR2) instalado en /home/ubuntu/vortex-rs/bin."
