#!/usr/bin/env bash
# 一键启动 mock-gateway / mock-agent / backend / frontend(dev)，日志与 pid 写到 .run/。
#
#   scripts/start-all.sh            # 安装依赖（首次）+ 构建 + 启动
#   scripts/start-all.sh --fresh    # 先清空数据库再启动（mock-gateway 是纯内存的，重启后需配套清库）
#   scripts/start-all.sh --no-frontend
#
# 前置：Node 22+；PostgreSQL 可用（docker compose up -d 即可，见 README）。
# 端口/地址均可用环境变量覆盖：DATABASE_URL / BACKEND_PORT / GATEWAY_PORT / AGENT_PORT。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RUN="$ROOT/.run"
mkdir -p "$RUN"

export DATABASE_URL="${DATABASE_URL:-postgres://exam:exam@127.0.0.1:15433/exam}"
BACKEND_PORT="${BACKEND_PORT:-3001}"
GATEWAY_PORT="${GATEWAY_PORT:-3101}"
AGENT_PORT="${AGENT_PORT:-3102}"
FRESH=0; FRONTEND=1
for a in "$@"; do
  case "$a" in
    --fresh) FRESH=1 ;;
    --no-frontend) FRONTEND=0 ;;
    *) echo "未知参数 $a"; exit 2 ;;
  esac
done

"$ROOT/scripts/stop-all.sh" >/dev/null 2>&1 || true

for d in mock-gateway mock-agent backend frontend; do
  [ "$d" = frontend ] && [ "$FRONTEND" = 0 ] && continue
  if [ ! -d "$ROOT/$d/node_modules" ]; then
    echo "· npm ci ($d)"; (cd "$ROOT/$d" && npm ci --no-audit --no-fund >/dev/null)
  fi
done
echo "· build mock-agent / backend"
(cd "$ROOT/mock-agent" && npm run build >/dev/null)
(cd "$ROOT/backend" && npm run build >/dev/null)

# 等数据库可连
cd "$ROOT/backend"
for i in $(seq 1 30); do
  node -e "const{Client}=require('pg');const c=new Client({connectionString:process.env.DATABASE_URL});c.connect().then(()=>c.end()).then(()=>process.exit(0),()=>process.exit(1))" && break
  [ "$i" = 30 ] && { echo "✗ 连不上数据库 $DATABASE_URL（先 docker compose up -d）"; exit 1; }
  sleep 1
done
if [ "$FRESH" = 1 ]; then
  echo "· 清空数据库 schema public"
  node -e "const{Client}=require('pg');const c=new Client({connectionString:process.env.DATABASE_URL});c.connect().then(()=>c.query('DROP SCHEMA public CASCADE; CREATE SCHEMA public')).then(()=>c.end())"
fi

start() { # name dir cmd...（exec 使 pid 文件里就是服务进程本身；输出全部进日志，不占用调用方的 stdout）
  local name="$1" dir="$2"; shift 2
  (cd "$dir" && exec nohup "$@") </dev/null >"$RUN/$name.log" 2>&1 &
  echo $! >"$RUN/$name.pid"
}
start mock-gateway "$ROOT/mock-gateway" env PORT="$GATEWAY_PORT" node server.js
start mock-agent "$ROOT/mock-agent" env PORT="$AGENT_PORT" node dist/index.js
start backend "$ROOT/backend" env PORT="$BACKEND_PORT" DATABASE_URL="$DATABASE_URL" \
  GATEWAY_URL="http://127.0.0.1:$GATEWAY_PORT" AGENT_URL="http://127.0.0.1:$AGENT_PORT" node dist/index.js
[ "$FRONTEND" = 1 ] && start frontend "$ROOT/frontend" node node_modules/vite/bin/vite.js --port 5173 --strictPort --host 127.0.0.1

for i in $(seq 1 30); do
  curl -sf "http://127.0.0.1:$BACKEND_PORT/api/health" >/dev/null && break
  [ "$i" = 30 ] && { echo "✗ backend 未就绪，见 .run/backend.log"; tail -20 "$RUN/backend.log"; exit 1; }
  sleep 1
done
echo "✓ 已启动（日志 .run/*.log，停止：scripts/stop-all.sh）"
echo "  backend       http://127.0.0.1:$BACKEND_PORT   $(curl -s "http://127.0.0.1:$BACKEND_PORT/api/health")"
echo "  mock-gateway  http://127.0.0.1:$GATEWAY_PORT"
echo "  mock-agent    http://127.0.0.1:$AGENT_PORT"
[ "$FRONTEND" = 1 ] && echo "  控制台         http://127.0.0.1:5173   （admin/admin，viewer/viewer）"
echo "  一键验收：     node verify/run-all.mjs"
