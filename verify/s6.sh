#!/bin/bash
# S6: Agent bad responses. Script [bad-json, unknown-tool, finish].
# Pass: run ends final/budget_exhausted/protocol_errors; backend alive;
# every step has kind + rawResponse.
cd "$(dirname "$0")"
# shellcheck source=common.sh
source ./common.sh
login
TS=$(date +%s); SENDER="ext-s6-$TS"
LOG="evidence-s6.log"; : > "$LOG"
note() { echo "$@" | tee -a "$LOG"; }
note "=== S6 bad agent responses ts=$TS ==="

reset_controls
note "--- step1: reset gateway+agent control plane"
agctl '{"script":["bad-json","unknown-tool","finish"]}' | tee -a "$LOG"
note "curl -X POST $AG/__control {script:[bad-json, unknown-tool, finish]}"

BEFORE=$(curl -s "$BACKEND/api/groups/$GROUP/agent-runs" -H "Authorization: Bearer $TOKEN" | python3 -c "import sys,json;print(' '.join(r['id'] for r in json.load(sys.stdin)))")
gwctl "{\"externalMessage\":{\"groupId\":\"$GGID\",\"senderPlatformUserId\":\"$SENDER\",\"text\":\"trigger s6 $TS\"}}" | tee -a "$LOG"
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
  note "S6_RESULT=fail(no-run)"
  agctl '{"reset":true}' >/dev/null
  exit 0
fi

DETAIL=""
for _ in $(seq 1 45); do
  sleep 2
  DETAIL=$(curl -s "$BACKEND/api/agent-runs/$RUN" -H "Authorization: Bearer $TOKEN")
  ST=$(echo "$DETAIL" | python3 -c "import sys,json;print(json.load(sys.stdin)['status'])")
  [ "$ST" != "running" ] && break
done
echo "$DETAIL" > "/tmp/s6_detail_$TS.json"
note "--- GET /api/agent-runs/$RUN steps:"
echo "$DETAIL" | python3 -c "
import sys,json
d=json.load(sys.stdin)
print('status=%s endReason=%s nsteps=%d' % (d['status'], d['endReason'], len(d['steps'])))
for i,s in enumerate(d['steps']):
  print('step%d kind=%s name=%s isError=%s errorCode=%s rawLen=%d summary=%s' % (
    i, s.get('kind'), s.get('name'), s.get('isError'), s.get('errorCode'),
    len(s.get('rawResponse') or ''), (s.get('resultSummary') or '')[:110]))" | tee -a "$LOG"

HEALTH=$(curl -s "$BACKEND/api/health")
note "GET /api/health -> $HEALTH"

python3 - "$HEALTH" <<'EOF' | tee -a "$LOG"
import sys, json, glob
health = json.loads(sys.argv[1])
d = json.load(open(sorted(glob.glob('/tmp/s6_detail_*.json'))[-1]))
ok = []
ok.append(('endReason in {final,budget_exhausted,protocol_errors}',
           (d['status'], d['endReason']) in [('finished','final'),('failed','budget_exhausted'),('failed','protocol_errors')]))
steps = d['steps']
ok.append(('>=3 steps', len(steps) >= 3))
ok.append(('every step has kind+rawResponse', all(s.get('kind') and s.get('rawResponse') for s in steps)))
codes = [s.get('errorCode') for s in steps]
ok.append(('has BAD_JSON protocol_error step', 'BAD_JSON' in codes))
ok.append(('has UNKNOWN_TOOL error step', 'UNKNOWN_TOOL' in codes))
ok.append(('backend alive', health.get('ok') is True))
for name, v in ok: print(('PASS' if v else 'FAIL'), '-', name)
print('S6_RESULT=' + ('pass' if all(v for _,v in ok) else 'fail'))
EOF

agctl '{"reset":true}' >/dev/null
note "--- cleanup: agent reset"
