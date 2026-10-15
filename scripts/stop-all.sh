#!/usr/bin/env bash
# 停止 scripts/start-all.sh 启动的进程。
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
for f in "$ROOT"/.run/*.pid; do
  [ -e "$f" ] || continue
  pid="$(cat "$f")"
  pkill -P "$pid" 2>/dev/null; kill "$pid" 2>/dev/null && echo "stopped $(basename "$f" .pid)"
  rm -f "$f"
done
