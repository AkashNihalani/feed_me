#!/usr/bin/env bash
set -euo pipefail

FEEDME_HOME="${FEEDME_HOME:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
FEEDME_USER="${FEEDME_USER:-$(id -un)}"
FEEDME_GROUP="${FEEDME_GROUP:-$(id -gn)}"
FEEDME_ENV_FILE="${FEEDME_ENV_FILE:-$FEEDME_HOME/apps/worker/.env}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
START_SERVICES="${START_SERVICES:-1}"

if [[ ! -f "$FEEDME_HOME/apps/worker/requirements.txt" ]]; then
  echo "Could not find apps/worker/requirements.txt under FEEDME_HOME=$FEEDME_HOME" >&2
  exit 1
fi

if [[ ! -f "$FEEDME_ENV_FILE" ]]; then
  echo "Missing worker env file: $FEEDME_ENV_FILE" >&2
  echo "Create it from infra/.env.worker.example or point FEEDME_ENV_FILE at the production env file." >&2
  exit 1
fi

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "Python not found: $PYTHON_BIN" >&2
  exit 1
fi

if ! command -v ffmpeg >/dev/null 2>&1; then
  echo "ffmpeg is required for worker media previews. Install it before starting the services." >&2
  exit 1
fi

"$PYTHON_BIN" -m venv "$FEEDME_HOME/.venv"
"$FEEDME_HOME/.venv/bin/python" -m pip install --upgrade pip
"$FEEDME_HOME/.venv/bin/python" -m pip install -r "$FEEDME_HOME/apps/worker/requirements.txt"

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

render_unit() {
  local mode="$1"
  local name="$2"
  local syslog="$3"
  cat >"$tmpdir/$name.service" <<UNIT
[Unit]
Description=$name
After=network-online.target
Wants=network-online.target
StartLimitIntervalSec=300
StartLimitBurst=10

[Service]
Type=simple
User=$FEEDME_USER
Group=$FEEDME_GROUP
WorkingDirectory=$FEEDME_HOME
Environment=PYTHONUNBUFFERED=1
EnvironmentFile=$FEEDME_ENV_FILE
ExecStart=$FEEDME_HOME/.venv/bin/python -m apps.worker.app.cli --mode $mode
Restart=always
RestartSec=10
TimeoutStopSec=30
KillSignal=SIGINT
SyslogIdentifier=$syslog

[Install]
WantedBy=multi-user.target
UNIT
}

render_unit "worker" "feedme-worker" "feedme-worker"
render_unit "fingerprint_reels_worker" "feedme-fingerprint-worker" "feedme-fingerprint-worker"

sudo install -m 0644 "$tmpdir/feedme-worker.service" /etc/systemd/system/feedme-worker.service
sudo install -m 0644 "$tmpdir/feedme-fingerprint-worker.service" /etc/systemd/system/feedme-fingerprint-worker.service
sudo systemctl daemon-reload
sudo systemctl enable feedme-worker.service feedme-fingerprint-worker.service

if [[ "$START_SERVICES" == "1" ]]; then
  sudo systemctl restart feedme-worker.service feedme-fingerprint-worker.service
fi

sudo systemctl --no-pager --full status feedme-worker.service feedme-fingerprint-worker.service || true
