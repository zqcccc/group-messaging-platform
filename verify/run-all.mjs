#!/usr/bin/env node
// 一键验收：自建账号/群 → 逐条跑 S1–S8 与若干补充用例 → 终端打印结果表，并写 verify/REPORT.md。
// 零依赖（Node 22+ 自带 fetch / WebSocket）。前置：mock-gateway、mock-agent、backend 已启动。
//
//   node verify/run-all.mjs            # 全部
//   node verify/run-all.mjs S4 X3      # 只跑指定用例
//
// 地址可用环境变量覆盖：BACKEND / GW / AG（默认 :3001 / :3101 / :3102）。
import { writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const BACKEND = process.env.BACKEND ?? 'http://127.0.0.1:3001';
const GW = process.env.GW ?? 'http://127.0.0.1:3101';
const AG = process.env.AG ?? 'http://127.0.0.1:3102';
const HERE = dirname(fileURLToPath(import.meta.url));
const ACCOUNTS = ['acc-1', 'acc-2', 'acc-3', 'acc-4'];

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const uniq = () => `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`;

// ---------- HTTP ----------
async function http(base, method, path, body, token) {
  const headers = { 'content-type': 'application/json' };
  if (token) headers.authorization = `Bearer ${token}`;
  const res = await fetch(base + path, { method, headers, body: body === undefined ? undefined : JSON.stringify(body) });
  const text = await res.text();
  let json = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = text;
  }
  return { status: res.status, body: json };
}
const api = (method, path, body, token = ctx.token) => http(BACKEND, method, path, body, token);
const gw = (patch) => http(GW, 'POST', '/__control', patch);
const ag = (patch) => http(AG, 'POST', '/__control', patch);

async function waitFor(fn, timeoutMs, intervalMs = 200) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const v = await fn();
    if (v) return v;
    if (Date.now() >= deadline) return null;
    await sleep(intervalMs);
  }
}

// 网关事件历史（SSE 补拉，读到 maxMs 为止）。
async function gwEvents(since, maxMs = 1500) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), maxMs);
  let buf = '';
  try {
    const res = await fetch(`${GW}/events?since=${since}`, { signal: ctrl.signal });
    const reader = res.body.getReader();
    const dec = new TextDecoder();
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      buf += dec.decode(value, { stream: true });
    }
  } catch {
    // 读到超时为止。
  } finally {
    clearTimeout(t);
  }
  return buf
    .split('\n')
    .filter((l) => l.startsWith('data:'))
    .map((l) => {
      try {
        return JSON.parse(l.slice(5).trim());
      } catch {
        return null;
      }
    })
    .filter(Boolean);
}
async function gwEventCursor() {
  const r = await http(GW, 'GET', '/__control');
  return r.body?.stats?.events ?? 0;
}

const resetControls = async () => {
  await gw({
    duplicateEvents: false,
    outOfOrder: false,
    forceSend504All: false,
    forceSend504Accounts: [],
    send504Lands: true,
    force503: false,
    byClientId503: false,
    failNextLeave: false,
    sendDelayMs: [50, 2000],
    acceptDelayMs: [0, 300],
  });
  await ag({ reset: true });
};

// ---------- 业务小工具 ----------
const ctx = { token: null, viewerToken: null, group: null, ggid: null };

async function timeline(limit = 200) {
  const r = await api('GET', `/api/groups/${ctx.group}/messages?limit=${limit}`);
  return r.body?.items ?? [];
}
async function runIds() {
  const r = await api('GET', `/api/groups/${ctx.group}/agent-runs`);
  return new Set((Array.isArray(r.body) ? r.body : r.body?.items ?? []).map((x) => x.id));
}
async function triggerAgentRun(text) {
  const before = await runIds();
  await gw({ externalMessage: { groupId: ctx.ggid, senderPlatformUserId: `ext-${uniq()}`, text } });
  const runId = await waitFor(async () => [...(await runIds())].find((id) => !before.has(id)), 15000, 300);
  if (!runId) return { runId: null, run: null };
  const run = await waitFor(async () => {
    const r = await api('GET', `/api/agent-runs/${runId}`);
    return r.body?.status && r.body.status !== 'running' ? r.body : null;
  }, 70000, 500);
  return { runId, run };
}
async function accountStatus(id) {
  const r = await api('GET', '/api/accounts');
  return r.body.find((a) => a.id === id)?.status;
}
async function ensureOnline(id) {
  const st = await accountStatus(id);
  if (st === 'online') return;
  if (st === 'rate_limited') {
    await waitFor(async () => (await accountStatus(id)) === 'online', 70000, 500);
    return;
  }
  await api('POST', `/api/accounts/${id}/connect`);
}

// WS 监听：认证后收集事件帧。
function openWs(token) {
  const url = BACKEND.replace(/^http/, 'ws') + '/ws';
  const frames = [];
  const ws = new WebSocket(url);
  const ready = new Promise((resolve, reject) => {
    ws.addEventListener('open', () => ws.send(JSON.stringify({ type: 'auth', accessToken: token })));
    ws.addEventListener('message', (ev) => {
      const f = JSON.parse(String(ev.data));
      if (f.type === 'auth') return f.success ? resolve() : reject(new Error('ws auth failed'));
      frames.push(f);
    });
    ws.addEventListener('error', () => reject(new Error('ws error')));
  });
  return { ws, frames, ready };
}

// ---------- 用例 ----------
// 每个用例返回 checks: [{ name, ok, detail }]。
const cases = [];
const def = (id, title, req, fn) => cases.push({ id, title, req, fn });

def('S1', '受理与发出', '2.4 S1 / A2', async () => {
  await gw({ sendDelayMs: [2500, 2500] }); // 拉长 202 → message_sent 的间隔，便于观察 accepted
  const t0 = Date.now();
  const r = await api('POST', `/api/groups/${ctx.group}/send`, { accountId: 'acc-2', text: `S1 ${uniq()}` });
  const elapsed = Date.now() - t0;
  const cmid = r.body?.clientMsgId;
  const seen = new Set();
  await waitFor(async () => {
    const row = (await timeline()).find((m) => m.clientMsgId === cmid);
    if (row) seen.add(row.deliveryStatus);
    return row?.deliveryStatus === 'sent';
  }, 10000, 100);
  const rows = (await timeline()).filter((m) => m.clientMsgId === cmid);
  return [
    { name: 'POST send → 202 { clientMsgId }', ok: r.status === 202 && !!cmid, detail: `HTTP ${r.status}` },
    { name: '202 不等 message_sent（< 1.5s 返回）', ok: elapsed < 1500, detail: `${elapsed}ms` },
    { name: 'message_sent 之前观察到 accepted', ok: seen.has('accepted'), detail: [...seen].join(' → ') },
    { name: '最终 sent，且时间线只有一行', ok: rows.length === 1 && rows[0].deliveryStatus === 'sent', detail: `rows=${rows.length}` },
  ];
});

def('S2', '事件重复', '2.4 S2 / A2 / A5-1', async () => {
  await gw({ duplicateEvents: true });
  const text = `S2 dup ${uniq()}`;
  const { runId, run } = await triggerAgentRun(text);
  await sleep(1500);
  const rows = (await timeline()).filter((m) => m.text === text);
  const runsAfter = await runIds();
  await sleep(2000);
  const runsLater = await runIds();
  return [
    { name: '每个事件推两次，时间线该消息只有 1 行', ok: rows.length === 1, detail: `rows=${rows.length}` },
    { name: '触发且只触发了 1 个 agent run', ok: !!runId && runsLater.size === runsAfter.size, detail: `run=${runId?.slice(0, 8)} status=${run?.status}` },
  ];
});

def('S3', '自己的消息回流', '2.4 S3 / A2', async () => {
  const before = await runIds();
  const r = await api('POST', `/api/groups/${ctx.group}/send`, { accountId: 'acc-3', text: `S3 own ${uniq()}` });
  const cmid = r.body?.clientMsgId;
  const row = await waitFor(async () => {
    const m = (await timeline()).find((x) => x.clientMsgId === cmid);
    return m?.deliveryStatus === 'sent' && m.msgId ? m : null;
  }, 10000);
  await sleep(2000);
  const after = await runIds();
  return [
    { name: '回流的自发消息 isOwn=true', ok: row?.isOwn === true, detail: `msgId=${row?.msgId}` },
    { name: '没有产生新的 agent run', ok: after.size === before.size, detail: `runs ${before.size} → ${after.size}` },
  ];
});

def('S4', '限流', '2.4 S4 / A1 / A2', async () => {
  const acc = 'acc-3';
  const secs = 4;
  await gw({ rateLimit: { accountId: acc, seconds: secs } });
  const since = await gwEventCursor();
  const r1 = await api('POST', `/api/groups/${ctx.group}/send`, { accountId: acc, text: `S4 first ${uniq()}` });
  const limited = await waitFor(async () => {
    const a = (await api('GET', '/api/accounts')).body.find((x) => x.id === acc);
    return a?.status === 'rate_limited' ? a : null;
  }, 5000);
  const r2 = await api('POST', `/api/groups/${ctx.group}/send`, { accountId: acc, text: `S4 second ${uniq()}` });
  const queuedRow = (await timeline()).find((m) => m.clientMsgId === r2.body?.clientMsgId);
  // 窗口内网关不应再收到该账号的 send：若收到，网关会重置计时（窗口被拉长），这里用"到期按时恢复"间接验证。
  const back = await waitFor(async () => (await accountStatus(acc)) === 'online', (secs + 6) * 1000, 300);
  const sentBoth = await waitFor(async () => {
    const tl = await timeline();
    const a = tl.find((m) => m.clientMsgId === r1.body?.clientMsgId);
    const b = tl.find((m) => m.clientMsgId === r2.body?.clientMsgId);
    return a?.deliveryStatus === 'sent' && b?.deliveryStatus === 'sent' ? [a, b] : null;
  }, 15000, 300);
  const sentEvents = (await gwEvents(since)).filter((e) => e.type === 'message_sent' && e.accountId === acc);
  const order = sentEvents.map((e) => e.clientMsgId);
  const inOrder = order.indexOf(r1.body?.clientMsgId) > -1 && order.indexOf(r1.body?.clientMsgId) < order.indexOf(r2.body?.clientMsgId);
  return [
    { name: '429 后账号 → rate_limited，rateLimitedUntil 有值', ok: !!limited?.rateLimitedUntil, detail: limited?.rateLimitedUntil ?? '-' },
    { name: '限流期内发送照常 202、保持 queued', ok: r2.status === 202 && queuedRow?.deliveryStatus === 'queued', detail: `HTTP ${r2.status} ${queuedRow?.deliveryStatus}` },
    { name: `到期（${secs}s）自动回到 online`, ok: !!back, detail: back ? 'online' : await accountStatus(acc) },
    { name: '排队的消息到期后按原顺序发出', ok: !!sentBoth && inOrder, detail: order.map((x) => x.slice(0, 8)).join(' → ') },
  ];
});

def('S5', 'Agent 重试同一个 key', '2.4 S5 / A2 / A5-7', async () => {
  const key = `k-s5-${uniq()}`;
  await gw({ forceSend504All: true, send504Lands: true });
  await ag({ script: [{ name: 'send-message', input: { text: `S5 hello ${key}`, idempotency_key: key } }, 'same-key-retry', 'finish'] });
  const since = await gwEventCursor();
  const { run } = await triggerAgentRun(`trigger ${key}`);
  await gw({ forceSend504All: false });
  const st = run?.steps ?? [];
  const sends = (await gwEvents(since)).filter((e) => e.type === 'message_sent' && String(e.clientMsgId).endsWith(`:${key}`));
  return [
    { name: 'run finished / final', ok: run?.status === 'finished' && run?.endReason === 'final', detail: `${run?.status}/${run?.endReason}` },
    { name: '第 1 次 send 经审计后 sent', ok: st[0]?.auditVerdict === 'pass' && /deliveryStatus=sent/.test(st[0]?.resultSummary ?? ''), detail: st[0]?.resultSummary },
    { name: '同 key 第 2 次：不审计，返回 sent', ok: st[1]?.name === 'send_message' && st[1]?.auditVerdict === null && /deliveryStatus=sent/.test(st[1]?.resultSummary ?? ''), detail: st[1]?.resultSummary },
    { name: '网关里恰好一条消息', ok: sends.length === 1, detail: `message_sent=${sends.length}` },
  ];
});

def('S6', 'Agent 坏响应', '2.4 S6 / A5-3', async () => {
  await ag({ script: ['bad-json', 'unknown-tool', 'finish'] });
  const { run } = await triggerAgentRun(`trigger S6 ${uniq()}`);
  const st = run?.steps ?? [];
  const health = await http(BACKEND, 'GET', '/api/health');
  return [
    { name: 'run 以 final / budget_exhausted / protocol_errors 结束', ok: ['final', 'budget_exhausted', 'protocol_errors'].includes(run?.endReason), detail: `${run?.status}/${run?.endReason}` },
    { name: '坏 JSON → protocol_error(BAD_JSON)', ok: st.some((s) => s.kind === 'protocol_error' && s.errorCode === 'BAD_JSON'), detail: st.map((s) => `${s.kind}:${s.errorCode ?? s.name}`).join(', ') },
    { name: '未知工具 → UNKNOWN_TOOL 错误结果', ok: st.some((s) => s.errorCode === 'UNKNOWN_TOOL') },
    { name: '每一步都有 kind 和 rawResponse', ok: st.length > 0 && st.every((s) => s.kind && typeof s.rawResponse === 'string') },
    { name: '服务未崩溃', ok: health.status === 200 },
  ];
});

def('S7', '序列并发启动', '2.4 S7 / B1', async () => {
  const seq = await api('POST', '/api/sequences', {
    name: `s7-${uniq()}`,
    steps: [
      { index: 1, accountRole: 'admin', text: 'S7 one {v}', delaySeconds: 3 },
      { index: 2, accountRole: 'member', text: 'S7 two {v}', delaySeconds: 1 },
    ],
  });
  const start = () => api('POST', `/api/groups/${ctx.group}/sequence-runs`, { sequenceId: seq.body.id, vars: { v: 'x' }, stepVars: {} });
  const [a, b] = await Promise.all([start(), start()]);
  const codes = [a.status, b.status].sort();
  const loser = a.status === 409 ? a : b;
  const winner = a.status === 201 ? a : b;
  const done = await waitFor(async () => {
    const r = await api('GET', `/api/sequence-runs/${winner.body?.runId}`);
    return r.body?.status && r.body.status !== 'running' ? r.body : null;
  }, 30000, 500);
  const list = await api('GET', '/api/sequences');
  return [
    { name: 'GET /api/sequences 能列出刚建的序列', ok: Array.isArray(list.body) && list.body.some((x) => x.id === seq.body.id) },
    { name: '并发两次：恰好一个 201、一个 409', ok: codes[0] === 201 && codes[1] === 409, detail: codes.join(' + ') },
    { name: '409 的 error.code = SEQUENCE_ALREADY_RUNNING', ok: loser.body?.error?.code === 'SEQUENCE_ALREADY_RUNNING' },
    { name: '获胜的运行正常跑完（finished）', ok: done?.status === 'finished', detail: `${done?.status} steps=${done?.steps?.map((s) => s.status).join(',')}` },
  ];
});

def('S8', '序列预检', '2.4 S8 / B1', async () => {
  const missing = `nope_${uniq()}`;
  const seq = await api('POST', '/api/sequences', {
    name: `s8-${uniq()}`,
    steps: [
      { index: 1, accountRole: 'admin', text: 'hi {greet}', delaySeconds: 1 },
      { index: 2, accountRole: 'member', text: 'mid {greet}', delaySeconds: 1 },
      { index: 3, accountRole: 'admin', text: `need {${missing}}`, delaySeconds: 1 },
    ],
  });
  const since = await gwEventCursor();
  const r = await api('POST', `/api/groups/${ctx.group}/sequence-runs`, { sequenceId: seq.body.id, vars: { greet: 'hello' }, stepVars: {} });
  await sleep(2500);
  const sent = (await gwEvents(since)).filter((e) => e.type === 'message_sent' || e.type === 'message');
  const g = await api('GET', `/api/groups/${ctx.group}`);
  const e = r.body?.error ?? {};
  return [
    { name: 'HTTP 422 + code=UNRESOLVED_PLACEHOLDER', ok: r.status === 422 && e.code === 'UNRESOLVED_PLACEHOLDER', detail: `HTTP ${r.status} ${e.code}` },
    { name: 'error.stepIndex = 3，error.key = 占位符名', ok: e.stepIndex === 3 && e.key === missing, detail: `stepIndex=${e.stepIndex} key=${e.key}` },
    { name: '网关收不到任何消息', ok: sent.length === 0, detail: `events=${sent.length}` },
    { name: '不留下运行中的记录', ok: g.body?.activeSequenceRunId === null },
  ];
});

def('X1', '鉴权与错误体', 'A0 / 2.3', async () => {
  const noToken = await api('GET', '/api/accounts', undefined, null);
  const viewerWrite = await api('POST', '/api/accounts/acc-1/transition', { to: 'disconnected', expectedFrom: 'online' }, ctx.viewerToken);
  const viewerRead = await api('GET', '/api/accounts', undefined, ctx.viewerToken);
  const health = await http(BACKEND, 'GET', '/api/health');
  return [
    { name: '无 token → 401 UNAUTHORIZED（带 requestId）', ok: noToken.status === 401 && noToken.body?.error?.code === 'UNAUTHORIZED' && !!noToken.body.error.requestId },
    { name: 'viewer 写操作 → 403 FORBIDDEN', ok: viewerWrite.status === 403 && viewerWrite.body?.error?.code === 'FORBIDDEN' },
    { name: 'viewer 可读', ok: viewerRead.status === 200 },
    { name: 'GET /api/health → { ok, schemaVersion }', ok: health.body?.ok === true && Number.isInteger(health.body?.schemaVersion), detail: JSON.stringify(health.body) },
  ];
});

def('X2', '账号状态机 + WS 推送', 'A1 / A4', async () => {
  const acc = 'acc-4';
  const { ws, frames, ready } = openWs(ctx.token);
  await ready;
  const nf = await api('POST', '/api/accounts/acc-nope/transition', { to: 'online', expectedFrom: 'idle' });
  const bad = await api('POST', `/api/accounts/${acc}/transition`, { to: 'disconnected' });
  const illegal = await api('POST', `/api/accounts/${acc}/transition`, { to: 'online', expectedFrom: 'online' });
  const cas = await api('POST', `/api/accounts/${acc}/transition`, { to: 'online', expectedFrom: 'disconnected' });
  const [a, b] = await Promise.all([
    api('POST', `/api/accounts/${acc}/transition`, { to: 'disconnected', expectedFrom: 'online' }),
    api('POST', `/api/accounts/${acc}/transition`, { to: 'disconnected', expectedFrom: 'online' }),
  ]);
  const reconnect = await api('POST', `/api/accounts/${acc}/connect`);
  await waitFor(() => frames.filter((f) => f.type === 'account_status_changed' && f.payload.accountId === acc).length >= 2, 3000);
  ws.close();
  const evs = frames.filter((f) => f.type === 'account_status_changed' && f.payload.accountId === acc).map((f) => `${f.payload.from}→${f.payload.to}`);
  const seqs = frames.map((f) => f.seq);
  return [
    { name: '账号不存在 → 404 ACCOUNT_NOT_FOUND', ok: nf.status === 404 && nf.body?.error?.code === 'ACCOUNT_NOT_FOUND' },
    { name: '缺 expectedFrom → 400 VALIDATION_ERROR', ok: bad.status === 400 && bad.body?.error?.code === 'VALIDATION_ERROR' },
    { name: '同状态 online→online → 409 ILLEGAL_TRANSITION', ok: illegal.status === 409 && illegal.body?.error?.code === 'ILLEGAL_TRANSITION' },
    { name: 'expectedFrom 与当前不符 → 409 CAS_CONFLICT', ok: cas.status === 409 && cas.body?.error?.code === 'CAS_CONFLICT' },
    { name: '并发两次同一转移：一个 200、一个 CAS_CONFLICT', ok: [a.status, b.status].sort().join() === '200,409', detail: `${a.status} + ${b.status}` },
    { name: '重连 → online', ok: reconnect.body?.status === 'online' },
    { name: 'WS 收到 account_status_changed（离线、重连各一条）', ok: evs.includes('online→disconnected') && evs.includes('disconnected→online'), detail: evs.join(', ') || '无' },
    { name: 'WS 帧 seq 单调递增', ok: seqs.every((s, i) => i === 0 || s > seqs[i - 1]) },
  ];
});

def('X3', '不同 run 复用同一个 idempotency_key', 'A5-7', async () => {
  // 幂等只在同一个 run 内成立：两个 run 用同一个 key 时两条消息都要发出。
  const key = `k-x3-${uniq()}`;
  const texts = [`X3 first ${key}`, `X3 second ${key}`];
  const since = await gwEventCursor();
  const runs = [];
  for (const text of texts) {
    await ag({ script: [{ name: 'send-message', input: { text, idempotency_key: key } }, 'finish'] });
    runs.push((await triggerAgentRun(`trigger ${text}`)).run);
  }
  const landed = (await gwEvents(since)).filter((e) => e.type === 'message' && texts.includes(e.text)).map((e) => e.text);
  return [
    { name: '两个 run 都 finished', ok: runs.every((r) => r?.status === 'finished'), detail: runs.map((r) => r?.status).join(', ') },
    { name: '两条消息都真正到达网关', ok: texts.every((t) => landed.includes(t)), detail: `landed=${landed.length}/2` },
  ];
});

def('X4', '504 期间 by-client-id 不可用', 'A2', async () => {
  // 504 且消息没有落地，同时 by-client-id 查询 503：无法确认就必须保持 unknown（不能记 failed、不能重发）；
  // 查询恢复后确认没发出 → 同 clientMsgId 重发一次 → sent。
  await gw({ forceSend504All: true, send504Lands: false, byClientId503: true });
  const since = await gwEventCursor();
  const r = await api('POST', `/api/groups/${ctx.group}/send`, { accountId: 'acc-2', text: `X4 ${uniq()}` });
  const cmid = r.body?.clientMsgId;
  const seen = new Set();
  await waitFor(async () => {
    const row = (await timeline()).find((m) => m.clientMsgId === cmid);
    if (row) seen.add(row.deliveryStatus);
    return false;
  }, 5000, 150);
  const during = (await timeline()).find((m) => m.clientMsgId === cmid)?.deliveryStatus;
  await gw({ forceSend504All: false, byClientId503: false });
  const final = await waitFor(async () => {
    const row = (await timeline()).find((m) => m.clientMsgId === cmid);
    return row?.deliveryStatus === 'sent' ? row : null;
  }, 8000, 200);
  const sends = (await gwEvents(since)).filter((e) => e.type === 'message_sent' && e.clientMsgId === cmid);
  return [
    { name: '查询不可用的 5 秒内一直是 unknown（不记 failed）', ok: during === 'unknown' && !seen.has('failed'), detail: [...seen].join(' → ') },
    { name: '查询恢复后确认没发出 → 重发一次 → sent', ok: !!final, detail: final?.deliveryStatus ?? (await timeline()).find((m) => m.clientMsgId === cmid)?.deliveryStatus },
    { name: '网关里恰好 1 条', ok: sends.length === 1, detail: `message_sent=${sends.length}` },
  ];
});

// ---------- 主流程 ----------
async function preflight() {
  const targets = [
    ['backend', `${BACKEND}/api/health`],
    ['mock-gateway', `${GW}/__control`],
    ['mock-agent', `${AG}/__control`],
  ];
  for (const [name, url] of targets) {
    try {
      const r = await fetch(url);
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
    } catch (e) {
      console.error(`✗ ${name} 不可达（${url}）：${e.message}\n  先按 README「快速开始」启动全部服务。`);
      process.exit(2);
    }
  }
}

async function bootstrap() {
  const login = async (u) => (await http(BACKEND, 'POST', '/api/auth/login', { username: u, password: u })).body?.accessToken;
  ctx.token = await login('admin');
  ctx.viewerToken = await login('viewer');
  if (!ctx.token || !ctx.viewerToken) throw new Error('登录失败（admin/admin、viewer/viewer）');
  await resetControls();
  for (const id of ACCOUNTS) {
    const st = await accountStatus(id);
    if (st === 'suspended' || st === 'session_expired') throw new Error(`账号 ${id} 已是终态 ${st}，请用全新数据库重跑`);
    await ensureOnline(id);
  }
  const job = await api('POST', '/api/groups', { creatorAccountId: 'acc-1', memberAccountIds: ['acc-2', 'acc-3', 'acc-4'] });
  const jr = await waitFor(async () => {
    const r = await api('GET', `/api/jobs/${job.body.jobId}`);
    return r.body?.status !== 'running' ? r.body : null;
  }, 20000);
  if (jr?.status !== 'finished') throw new Error(`建群 job 未完成：${JSON.stringify(jr)}`);
  const groups = (await api('GET', '/api/groups')).body;
  const g = groups.find((x) => x.creatorAccountId === 'acc-1' && x.status === 'active' && x.members.length === 4 && !seenGroups.has(x.id));
  ctx.group = g.id;
  ctx.ggid = g.gatewayGroupId;
  await api('PATCH', `/api/groups/${ctx.group}`, { agentEnabled: true });
  return g;
}
let seenGroups = new Set();

async function main() {
  const only = new Set(process.argv.slice(2).map((s) => s.toUpperCase()));
  const selected = only.size ? cases.filter((c) => only.has(c.id)) : cases;
  await preflight();
  ctx.token = (await http(BACKEND, 'POST', '/api/auth/login', { username: 'admin', password: 'admin' })).body?.accessToken;
  seenGroups = new Set(((await api('GET', '/api/groups')).body ?? []).map((g) => g.id));
  const g = await bootstrap();
  const roles = g.members.map((m) => `${m.accountId}:${m.role}`).join(' ');
  console.log(`准备就绪：新建群 ${ctx.group}（网关 ${ctx.ggid}）成员 ${roles}，agentEnabled=true\n`);

  const results = [];
  for (const c of selected) {
    process.stdout.write(`▶ ${c.id} ${c.title} … `);
    let checks;
    try {
      await resetControls();
      for (const id of ACCOUNTS) await ensureOnline(id);
      checks = await c.fn();
    } catch (e) {
      checks = [{ name: '用例执行异常', ok: false, detail: e instanceof Error ? e.message : String(e) }];
    }
    const ok = checks.every((x) => x.ok);
    results.push({ ...c, checks, ok });
    console.log(ok ? 'PASS' : 'FAIL');
    for (const x of checks) console.log(`    ${x.ok ? '✓' : '✗'} ${x.name}${x.detail ? `  (${x.detail})` : ''}`);
  }
  await resetControls();

  const passed = results.filter((r) => r.ok).length;
  console.log(`\n结果：${passed}/${results.length} 通过`);
  const md = [
    '# 验收结果（自动生成）',
    '',
    `> 由 \`node verify/run-all.mjs\` 生成于 ${new Date().toISOString()}；群 \`${ctx.group}\`。重新运行即可复现。`,
    '',
    '| 用例 | 场景 | 对应需求 | 结果 |',
    '| ---- | ---- | -------- | ---- |',
    ...results.map((r) => `| ${r.id} | ${r.title} | ${r.req} | ${r.ok ? '✅ PASS' : '❌ FAIL'} |`),
    '',
    '## 明细',
    '',
    ...results.flatMap((r) => [
      `### ${r.id} ${r.title}`,
      '',
      ...r.checks.map((x) => `- ${x.ok ? '✅' : '❌'} ${x.name}${x.detail ? ` — \`${String(x.detail).replace(/`/g, "'")}\`` : ''}`),
      '',
    ]),
  ].join('\n');
  if (!only.size) writeFileSync(join(HERE, 'REPORT.md'), md);
  process.exit(passed === results.length ? 0 : 1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
