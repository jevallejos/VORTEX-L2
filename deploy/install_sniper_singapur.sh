#!/usr/bin/env bash
set -euo pipefail

cd /home/ubuntu/vortex-rs
mkdir -p /home/ubuntu/vortex-rs/bin /home/ubuntu/vortex-rs/data/vault /home/ubuntu/vortex-rs/data/candles /home/ubuntu/vortex-rs/data/archive /home/ubuntu/vortex-rs/backups
chmod +x /home/ubuntu/vortex-rs/deploy/prewarm_emas_bybit.py

nice -n 19 ionice -c 3 cargo clippy --all-targets -- -D warnings
nice -n 19 ionice -c 3 cargo test --lib -- --test-threads=1
nice -n 19 ionice -c 3 cargo test --tests -- --test-threads=1
nice -n 19 ionice -c 3 cargo build --release --bins

./target/release/vortex_tap
mkdir -p /tmp/vortex_empty_par2_gate
./target/release/vortex_replay --dir /tmp/vortex_empty_par2_gate
rmdir /tmp/vortex_empty_par2_gate
./target/release/vortex_exec
./target/release/vortex_web

install -m 0755 target/release/vortex_tap /home/ubuntu/vortex-rs/bin/vortex_tap
install -m 0755 target/release/vortex_replay /home/ubuntu/vortex-rs/bin/vortex_replay
install -m 0755 target/release/vortex_exec /home/ubuntu/vortex-rs/bin/vortex_exec
install -m 0755 target/release/vortex_web /home/ubuntu/vortex-rs/bin/vortex_web
sudo install -m 0644 /home/ubuntu/vortex-rs/deploy/vortex-web.service /etc/systemd/system/vortex-web.service
for unit_f in /etc/systemd/system/vortex-exec.service /etc/systemd/system/vortex-tap.service; do
  if [ -f "$unit_f" ]; then
    sudo sed -i -E '/prewarm_emas_bybit\.py/d; s/VORTEX-OMEGA V[0-9]+(\.[0-9]+)*/VORTEX-OMEGA V5.5/g' "$unit_f" || true
  fi
done
sudo systemctl daemon-reload
echo "[OK] VORTEX-OMEGA V5.5 (4 Binarios LTO Fat + SHM v15 + VKL1 + PAR2Z + Web 127.0.0.1:8787) instalado."
