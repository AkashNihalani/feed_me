#!/usr/bin/env bash
set -euo pipefail

FEEDME_HOME="${FEEDME_HOME:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
BRANCH="${BRANCH:-main}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

cd "$FEEDME_HOME"

git fetch origin "$BRANCH"
git checkout "$BRANCH"
git pull --ff-only origin "$BRANCH"

if [[ ! -x "$FEEDME_HOME/.venv/bin/python" ]]; then
  "$PYTHON_BIN" -m venv "$FEEDME_HOME/.venv"
fi

"$FEEDME_HOME/.venv/bin/python" -m pip install -r "$FEEDME_HOME/apps/worker/requirements.txt"

sudo systemctl restart feedme-worker.service feedme-fingerprint-worker.service
sudo systemctl --no-pager --full status feedme-worker.service feedme-fingerprint-worker.service
