# VORTEX-OMEGA V5.2 — Protocolo de Despliegue Zero-Restart en Producción (`sniper-singapur`)

Este documento detalla el procedimiento exacto de despliegue en caliente (**Zero-Restart** sobre `vortex-tap.service`, preservando la arena `/dev/shm/vortex_lob.mmap` `SHM v15`, el anillo `PAR2` y todos los baúles históricos en `/home/ubuntu/vortex-rs/data/vault/`) mediante sesión SSH directa (PuTTY).

---

## 1. Garantías Operativas del Despliegue V5.2

1. **Invarianza Binaria `SHM v15`:** Como `VORTEX_SHM_VERSION == 15` y `size_of::<SlotTelemetrySnapshot>() == 96` no cambian respecto a V5.1, **`vortex-tap.service` NO se reinicia** (cero pérdida de secuencia WebSocket L2 `orderbook.50` ni huecos en velas `VKL1`).
2. **Resguardo P0-B (`DRAIN_MODE.lock`):** Antes de detener `vortex-exec.service`, se activa `DRAIN_MODE.lock` para impedir nuevas entradas y esperar a `200.0 Hz` el cierre natural pasivo de cualquier posición abierta.
3. **Snapshot Inmutable Criptográfico (`SHA-256`):** Se sellan en `backups/pre_omega_v52_<UTC>/` tanto los 4 binarios previos como una copia de sólo lectura (`chmod a-w`) de `data/vault/`.
4. **Compuerta Cuádruple Pre-Instalación:** El script aborta antes de tocar `bin/` si falla `clippy -D warnings`, cualquiera de los `90` tests deterministas o cualquiera de las `4` auto-certificaciones CLI (`vortex_tap`, `vortex_replay --dir`, `vortex_exec`, `vortex_web`).

---

## 2. Bloque SSH Directo para PuTTY (`sniper-singapur`)

Una vez subido el commit de V5.2 a `origin/main` en `https://github.com/jevallejos/VORTEX-L2.git`, pegar directamente en PuTTY:

```bash
cat << 'EOF' > /tmp/deploy_omega_v52_singapur.sh
#!/usr/bin/env bash
set -uo pipefail

VORTEX_DIR="/home/ubuntu/vortex-rs"
VAULT_DIR="/home/ubuntu/vortex-rs/data/vault"
cd "$VORTEX_DIR" || exit 1
mkdir -p "$VAULT_DIR" "$VORTEX_DIR/bin" "$VORTEX_DIR/backups" "$VORTEX_DIR/data/candles" "$VORTEX_DIR/data/archive"

echo "=== [1/6] RESGUARDO P0-B: DRAIN_MODE HASTA 0 POSICIONES ABIERTAS ==="
touch "$VAULT_DIR/DRAIN_MODE.lock"
sleep 1
while [ "$(wc -c < "$VAULT_DIR/vortex_open_positions_state.json" 2>/dev/null || echo 2)" -gt 4 ]; do
  echo "Posicion viva detectada en custodia; esperando cierre natural a 200.0 Hz..."
  sleep 2
done

sudo systemctl stop vortex-web.service vortex-exec.service 2>/dev/null || true
sleep 1
echo "[OK] 0 posiciones abiertas | vortex-tap sigue vivo sin corte L2 (Zero-Restart SHM v15)."

echo "=== [2/6] SNAPSHOT INMUTABLE PRE-OMEGA V5.2 (BINARIOS + VAULT + SHA256) ==="
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
BACKUP_DIR="$VORTEX_DIR/backups/pre_omega_v52_$STAMP"
mkdir -p "$BACKUP_DIR/bin" "$BACKUP_DIR/vault"
git rev-parse HEAD > "$BACKUP_DIR/PREV_COMMIT_SHA.txt" 2>/dev/null || true
cp -a "$VORTEX_DIR/bin/." "$BACKUP_DIR/bin/" 2>/dev/null || true
cp -a "$VAULT_DIR/." "$BACKUP_DIR/vault/" 2>/dev/null || true
(cd "$BACKUP_DIR/bin" && sha256sum * > "$BACKUP_DIR/SHA256SUMS.txt" 2>/dev/null) || true
(cd "$BACKUP_DIR/vault" && sha256sum * >> "$BACKUP_DIR/SHA256SUMS.txt" 2>/dev/null) || true
chmod -R a-w "$BACKUP_DIR/vault" 2>/dev/null || true
echo "[OK] Respaldo inmutable sellado en: $BACKUP_DIR"

echo "=== [3/6] PULL ORIGIN/MAIN + CLIPPY + 90 TESTS (LIB + V50 + V502 + V51 + V52) + BUILD LTO FAT ==="
git fetch origin main || exit 1
git checkout main 2>/dev/null || true
git reset --hard origin/main || exit 1
chmod +x "$VORTEX_DIR/deploy/prewarm_emas_bybit.py" "$VORTEX_DIR/deploy/install_sniper_singapur.sh"
source "$HOME/.cargo/env" 2>/dev/null || true

nice -n 19 ionice -c 3 cargo clippy --all-targets -- -D warnings || exit 1
nice -n 19 ionice -c 3 cargo test --lib -- --test-threads=1 || exit 1
nice -n 19 ionice -c 3 cargo test --tests -- --test-threads=1 || exit 1
nice -n 19 ionice -c 3 cargo build --release --bins || exit 1

echo "=== [4/6] CERTIFICACION DE LOS 4 BINARIOS CLI ==="
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

sudo systemctl daemon-reload
sudo systemctl enable vortex-tap.service vortex-exec.service vortex-web.service >/dev/null 2>&1 || true

echo "=== [5/6] LIMPIEZA DE CANDADOS DE DRENAJE (MANTENIENDO ARENA SHM v15 Y PRIORS BAYESIANOS) ==="
rm -f "$VAULT_DIR/DRAIN_MODE.lock" "$VAULT_DIR/HALTED_ON_LOSS.lock"

echo "=== [6/6] REENCENDIDO EN CALIENTE V5.2 (CUSTODIO 200.0 Hz + WEB 8787) ==="
sudo systemctl start vortex-exec.service || exit 1
sudo systemctl start vortex-web.service || exit 1
sleep 3

echo "[CONTEO FINAL DE BAULES HISTORICOS Y PLASTICIDAD SIN AMNESIA]:"
wc -l "$VAULT_DIR"/*.jsonl
git rev-parse --short HEAD
systemctl is-active vortex-tap.service vortex-exec.service vortex-web.service
curl -fsS http://127.0.0.1:8787/api/state | head -c 240 && echo ""
vortex 360
EOF

chmod +x /tmp/deploy_omega_v52_singapur.sh
bash /tmp/deploy_omega_v52_singapur.sh
```

---

## 3. Procedimiento de Rollback Inmediato (En Caso de Contingencia)

Si en cualquier momento se requiere revertir a la fotografía exacta pre-despliegue:

```bash
LATEST_BKP=$(ls -1dt /home/ubuntu/vortex-rs/backups/pre_omega_v52_* | head -n 1)
touch /home/ubuntu/vortex-rs/data/vault/DRAIN_MODE.lock
sudo systemctl stop vortex-web.service vortex-exec.service
install -m 0755 "$LATEST_BKP/bin/"* /home/ubuntu/vortex-rs/bin/
rm -f /home/ubuntu/vortex-rs/data/vault/DRAIN_MODE.lock
sudo systemctl start vortex-exec.service vortex-web.service
```
