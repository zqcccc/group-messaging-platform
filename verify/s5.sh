#!/bin/bash
# S5: Agent retry with same idempotency_key.
# Setup: gateway forceSend504All=true (first send 504, lands <2s), agent script
# [send-message(key K), same-key-retry, finish].
# Pass: run finished/final; 2nd send_message returns sent WITHOUT audit
# (auditVerdict null); gateway shows exactly ONE message_sent for K.
cd "$(dirname "$0")"
# shellcheck source=common.sh
source ./common.sh
login
TS=$(date +%s); KEY="k-s5-$TS"; SENDER="ext-s5-$TS"
LOG="evidence-s5.log"; : > "$LOG"
note() { echo "$@" | tee -a "$LOG"; }
note "=== S5 same-key retry ts=$TS key=$KEY ==="

reset_controls
note "--- step1: reset gateway+agent control plane (curl below)"
note "curl -X POST $GW/__control {duplicateEvents:false,forceSend504All:false,...}; curl -X POST $AG/__control {reset:true}"
gwctl '{"forceSend504All":true,"send504Lands":true}' | tee -a "$LOG"
note "curl -X POST $GW/__control {\"forceSend504All\":true,\"send504Lands\":true}"
agctl "{\"script\":[{\"name\":\"send-message\",\"input\":{\"text\":\"S5 hello $TS\",\"idempotency_key\":\"$KEY\"}},\"same-key-retry\",\"finish\"]}" | tee -a "$LOG"
note "curl -X POST $AG/__control {script:[send-message(key=$KEY), same-key-retry, finish]}"

SINCE=$(gw_stats_events)
note "gw_since(eventId)=$SINCE"
BEFORE=$(curl -s "$BACKEND/api/groups/$GROUP/agent-runs" -H "Authorization: Bearer $TOKEN" | python3 -c "import sys,json;print(' '.join(r['id'] for r in json.load(sys.stdin)))")
gwctl "{\"externalMessage\":{\"groupId\":\"$GGID\",\"senderPlatformUserId\":\"$SENDER\",\"text\":\"trigger s5 $TS\"}}" | tee -a "$LOG"
note "curl -X POST $GW/__control {externalMessage:{groupId:$GGID,sender:$SENDER}} -> trigger agent run"

RUN=""
for _ in $(seq 1 15); do
  sleep 1
  RUN=$(curl -s "$BACKEND/api/groups/$GROUP/agent-runs" -H "Authorization: Bearer $TOKEN" | python3 -c "
import sys,json
rs=json.load(sys.stdin); before=set('''$BEFORE'''.split())
for r in rs:
  if r['id'] not in before:
    print(r['id']); break")
  [ -n "$RUN" ] && break
done
note "RUN=$RUN"
if [ -z "$RUN" ]; then
  note "FAIL: no new agent run observed within 15s"
  note "S5_RESULT=fail(no-run)"
  gwctl '{"forceSend504All":false}' >/dev/null; agctl '{"reset":true}' >/dev/null
  exit 0
fi

DETAIL=""
for _ in $(seq 1 45); do
  sleep 2
  DETAIL=$(curl -s "$BACKEND/api/agent-runs/$RUN" -H "Authorization: Bearer $TOKEN")
  ST=$(echo "$DETAIL" | python3 -c "import sys,json;print(json.load(sys.stdin)['status'])")
  [ "$ST" != "running" ] && break
done
echo "$DETAIL" > "/tmp/s5_detail_$TS.json"
note "--- GET /api/agent-runs/$RUN (curl -H auth) steps:"
echo "$DETAIL" | python3 -c "
import sys,json
d=json.load(sys.stdin)
print('status=%s endReason=%s nsteps=%d' % (d['status'], d['endReason'], len(d['steps'])))
for i,s in enumerate(d['steps']):
  print('step%d kind=%s name=%s isError=%s errorCode=%s audit=%s rawLen=%d summary=%s' % (
    i, s.get('kind'), s.get('name'), s.get('isError'), s.get('errorCode'),
    s.get('auditVerdict'), len(s.get('rawResponse') or ''), (s.get('resultSummary') or '')[:110]))" | tee -a "$LOG"

gw_events_since "$SINCE" > "/tmp/s5_events_$TS.txt"
GWCOUNT=$(python3 - "$KEY" <<'EOF'
import sys, json
key = sys.argv[1]
n = 0; mids = set()
for line in open(sys.argv[2], encoding='utf-8', errors='replace'):
    line = line.strip()
    if line.startswith('data:'):
        try: d = json.loads(line[5:].strip())
        except Exception: continue
        if d.get('type') == 'message_sent' and d.get('clientMsgId') == key:
            n += 1; mids.add(d.get('msgId'))
print('%d|%s' % (n, ','.join(sorted(mids))))
EOF
"/tmp/s5_events_$TS.txt")
note "gateway message_sent count for $KEY: $GWCOUNT"
note "curl '$GW/events?since=$SINCE' (max-time 5) -> filter data.*message_sent clientMsgId=$KEY"

TL=$(curl -s "$BACKEND/api/groups/$GROUP/messages?limit=200" -H "Authorization: Bearer $TOKEN" | KEY="$KEY" python3 -c "
import sys,json,os
items=json.load(sys.stdin)['items']
hits=[m for m in items if m.get('clientMsgId')==os.environ['KEY']]
print('%d|%s' % (len(hits), hits[0].get('deliveryStatus') if hits else None))")
note "timeline rows for $KEY: $TL (count|deliveryStatus)"
note "curl '$BACKEND/api/groups/$GROUP/messages?limit=200'"

python3 - "$KEY" "$GWCOUNT" "$TL" <<'EOF' | tee -a "$LOG"
import sys, json
key = sys.argv[1]
gwn, _ = sys.argv[2].split('|', 1)
tln, tlds = sys.argv[3].split('|', 1)
d = json.load(open([f for f in __import__('glob').glob('/tmp/s5_detail_*.json')][-1]))
ok = []
ok.append(('run finished/final', d['status']=='finished' and d['endReason']=='final'))
st = d['steps']
ok.append(('3 steps send/send/finish', len(st)==3 and st[0].get('name')=='send_message' and st[1].get('name')=='send_message' and st[2].get('name')=='finish'))
ok.append(('1st send audited pass', st[0].get('auditVerdict')=='pass' and 'deliveryStatus=sent' in (st[0].get('resultSummary') or '')))
ok.append(('2nd same-key returns sent, no audit', 'deliveryStatus=sent' in (st[1].get('resultSummary') or '') and st[1].get('auditVerdict') is None and st[1].get('isError') is False))
ok.append(('gateway exactly one message_sent', int(gwn)==1))
ok.append(('timeline one row sent', int(tln)==1 and tlds=='sent'))
for name, v in ok: print(('PASS' if v else 'FAIL'), '-', name)
print('S5_RESULT=' + ('pass' if all(v for _,v in ok) else 'fail'))
EOF

gwctl '{"forceSend504All":false}' >/dev/null; agctl '{"reset":true}' >/dev/null
note "--- cleanup: forceSend504All=false, agent reset"
