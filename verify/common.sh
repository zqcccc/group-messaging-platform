#!/bin/bash
# S5-S8 verify common helpers (backend :3001, gateway :3101, agent :3102)
BACKEND="${BACKEND:-http://127.0.0.1:3001}"
GW="${GW:-http://127.0.0.1:3101}"
AG="${AG:-http://127.0.0.1:3102}"
GROUP="${GROUP:-236331d5-d4b4-48de-905f-0e79803570f9}"
GGID="${GGID:-g-3b8da0f8}"
login() {
  TOKEN=$(curl -s -X POST "$BACKEND/api/auth/login" -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' | python3 -c "import sys,json;print(json.load(sys.stdin)['accessToken'])")
  export TOKEN
}
gwctl() { curl -s -X POST "$GW/__control" -H 'Content-Type: application/json' -d "$1"; echo; }
agctl() { curl -s -X POST "$AG/__control" -H 'Content-Type: application/json' -d "$1"; echo; }
reset_controls() {
  gwctl '{"duplicateEvents":false,"outOfOrder":false,"forceSend504All":false,"send504Lands":true,"force503":false,"failNextLeave":false,"decisions":{"accounts":{"acc-1":null,"acc-2":null,"acc-3":null,"acc-4":null}}}' >/dev/null
  agctl '{"reset":true}' >/dev/null
}
gw_events_since() { curl -s --max-time 5 "$GW/events?since=$1"; }
gw_stats_events() { curl -s "$GW/__control" | python3 -c "import sys,json;print(json.load(sys.stdin)['stats']['events'])"; }
