#!/bin/bash
# S7: concurrent sequence starts — exactly one 201 + one 409.
# Pass: statuses {201,409}, 409 body error.code=SEQUENCE_ALREADY_RUNNING,
# group has exactly one running sequence run afterwards.
cd "$(dirname "$0")"
# shellcheck source=common.sh
source ./common.sh
login
TS=$(date +%s)
LOG="evidence-s7.log"; : > "$LOG"
note() { echo "$@" | tee -a "$LOG"; }
note "=== S7 concurrent sequence start ts=$TS ==="

reset_controls
# cleanup: make sure no running sequence holds the group lock
OLD=$(curl -s "$BACKEND/api/groups/$GROUP" -H "Authorization: Bearer $TOKEN" | python3 -c "import sys,json;print(json.load(sys.stdin)['activeSequenceRunId'] or '')")
note "pre-check activeSequenceRunId='$OLD' (expect empty; S8 leaves none)"
if [ -n "$OLD" ]; then
  note "S7_RESULT=fail(precondition: sequence $OLD already running)"
  exit 0
fi

SEQ=$(curl -s -X POST "$BACKEND/api/sequences" -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"name":"s7-'$TS'","steps":[{"index":1,"accountRole":"admin","text":"s7 one {v}","delaySeconds":60},{"index":2,"accountRole":"member","text":"s7 two {v}","delaySeconds":60}]}')
SEQID=$(echo "$SEQ" | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])")
note "sequenceId=$SEQID (delaySeconds=60 so run stays running during both starts)"
note "curl -X POST $BACKEND/api/sequences {steps delay 60}"

# two concurrent POSTs
curl -s -w '\nHTTP:%{http_code}' -X POST "$BACKEND/api/groups/$GROUP/sequence-runs" -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"sequenceId":"'$SEQID'","vars":{"v":"x"},"stepVars":{}}' > /tmp/s7_a.txt &
P1=$!
curl -s -w '\nHTTP:%{http_code}' -X POST "$BACKEND/api/groups/$GROUP/sequence-runs" -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"sequenceId":"'$SEQID'","vars":{"v":"x"},"stepVars":{}}' > /tmp/s7_b.txt &
P2=$!
wait $P1; wait $P2
note "--- response A:"; cat /tmp/s7_a.txt | tee -a "$LOG"; echo | tee -a "$LOG"
note "--- response B:"; cat /tmp/s7_b.txt | tee -a "$LOG"; echo | tee -a "$LOG"
note "two concurrent: curl -X POST $BACKEND/api/groups/$GROUP/sequence-runs {sequenceId} & twice"

ACTIVE=$(curl -s "$BACKEND/api/groups/$GROUP" -H "Authorization: Bearer $TOKEN" | python3 -c "import sys,json;print(json.load(sys.stdin)['activeSequenceRunId'] or '')")
note "post-check activeSequenceRunId='$ACTIVE'"

python3 - "$ACTIVE" <<'EOF' | tee -a "$LOG"
import sys, json
def parse(p):
    raw = open(p).read().strip().rsplit('\n', 1)
    body, code = (raw[0], raw[1].replace('HTTP:', '')) if len(raw) == 2 else ('', '')
    try: j = json.loads(body)
    except Exception: j = {}
    return code, j
ca, ja = parse('/tmp/s7_a.txt'); cb, jb = parse('/tmp/s7_b.txt')
active = sys.argv[1]
ok = []
ok.append(('one 201 + one 409', sorted([ca, cb]) == ['201', '409']))
bad = jb if cb == '409' else ja
ok.append(('409 body code=SEQUENCE_ALREADY_RUNNING', bad.get('error', {}).get('code') == 'SEQUENCE_ALREADY_RUNNING'))
ok.append(('exactly one runId issued', bool(active)))
for name, v in ok: print(('PASS' if v else 'FAIL'), '-', name)
print('S7_RESULT=' + ('pass' if all(v for _, v in ok) else 'fail'))
print('S7_RUN=' + active)
EOF

# cleanup: cancel the leftover running sequence via DB so group is free for later runs
sleep 2
