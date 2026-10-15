#!/bin/bash
# S8: sequence precheck — step 3 placeholder without value.
# Pass: 422 error.code=UNRESOLVED_PLACEHOLDER, error.stepIndex=3,
# error.key=<name>, and gateway receives no message.
cd "$(dirname "$0")"
# shellcheck source=common.sh
source ./common.sh
login
TS=$(date +%s); MISSING="nope_$TS"
LOG="evidence-s8.log"; : > "$LOG"
note() { echo "$@" | tee -a "$LOG"; }
note "=== S8 sequence precheck ts=$TS missing_key=$MISSING ==="

reset_controls
SINCE=$(gw_stats_events)
note "gw_since(eventId)=$SINCE"

SEQ=$(curl -s -X POST "$BACKEND/api/sequences" -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"name":"s8-'$TS'","steps":[{"index":1,"accountRole":"admin","text":"hi {greet}","delaySeconds":1},{"index":2,"accountRole":"member","text":"mid {greet}","delaySeconds":1},{"index":3,"accountRole":"admin","text":"need {'$MISSING'} here","delaySeconds":1}]}')
note "POST /api/sequences -> $SEQ"
SEQID=$(echo "$SEQ" | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])")
note "sequenceId=$SEQID"
note "curl -X POST $BACKEND/api/sequences {name,steps[1..3]}"

RESP=$(curl -s -w '\nHTTP:%{http_code}' -X POST "$BACKEND/api/groups/$GROUP/sequence-runs" -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"sequenceId":"'$SEQID'","vars":{"greet":"hello"},"stepVars":{}}')
BODY=$(echo "$RESP" | sed '$d'); CODE=$(echo "$RESP" | tail -1 | sed 's/HTTP://')
note "POST /api/groups/$GROUP/sequence-runs {sequenceId,vars:{greet}} -> $CODE $BODY"
echo "$BODY" | python3 -c "import sys,json;d=json.load(sys.stdin);e=d.get('error',{});print('code=%s stepIndex=%s key=%s requestId=%s' % (e.get('code'),e.get('stepIndex'),e.get('key'),e.get('requestId')))" | tee -a "$LOG"

sleep 3
gw_events_since "$SINCE" > "/tmp/s8_events_$TS.txt"
GWNEW=$(python3 - <<'EOF' "/tmp/s8_events_$TS.txt"
import sys, json
n = 0
for line in open(sys.argv[1], encoding='utf-8', errors='replace'):
    line = line.strip()
    if line.startswith('data:'):
        try: d = json.loads(line[5:].strip())
        except Exception: continue
        if d.get('type') in ('message','message_sent'): n += 1
print(n)
EOF
)
note "gateway message/message_sent events since start: $GWNEW"
note "curl '$GW/events?since=$SINCE' (max-time 5)"

ACTIVE=$(curl -s "$BACKEND/api/groups/$GROUP" -H "Authorization: Bearer $TOKEN" | python3 -c "import sys,json;print(json.load(sys.stdin)['activeSequenceRunId'])")
note "group activeSequenceRunId=$ACTIVE (expect null)"

python3 - "$CODE" "$BODY" "$MISSING" "$GWNEW" "$ACTIVE" <<'EOF' | tee -a "$LOG"
import sys, json
code, body, missing, gwnew, active = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]), sys.argv[5]
e = json.loads(body).get('error', {})
ok = []
ok.append(('HTTP 422', code == '422'))
ok.append(('error.code=UNRESOLVED_PLACEHOLDER', e.get('code') == 'UNRESOLVED_PLACEHOLDER'))
ok.append(('error.stepIndex=3', e.get('stepIndex') == 3))
ok.append(('error.key=<missing>', e.get('key') == missing))
ok.append(('gateway got no message', gwnew == 0))
ok.append(('no running sequence left', active in ('None', 'null', '')))
for name, v in ok: print(('PASS' if v else 'FAIL'), '-', name)
print('S8_RESULT=' + ('pass' if all(v for _,v in ok) else 'fail'))
EOF
