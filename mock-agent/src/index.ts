// mock-agent：Agent 服务模拟，对应 test.md §2.2。
// 无运行时依赖，仅用 Node 内置 http；默认监听 3102（PORT 环境变量可覆盖）。
import * as http from 'http';
import { randomUUID } from 'crypto';

const PORT = Number(process.env.PORT) || 3102;

// ---------- 类型 ----------
type JsonRecord = Record<string, unknown>;

interface ScriptObject {
  name: string;
  delayMs?: number;
  input?: JsonRecord;
  text?: string;
}

type ScriptItem = string | ScriptObject;

interface Session {
  count: number; // 已返回的轮数（默认剧本按此推进）
  queue: ScriptItem[]; // 该 run 的剧本队列（用尽后回落到默认推进）
  lastToolUseId: string | null; // 上一轮 tool_use.id（供 duplicate-id 剧本复用）
}

interface TurnResponse {
  stop_reason: string;
  content: JsonRecord[];
}

// ---------- 工具契约（§2.2 表格：工具名 → 全部入参） ----------
const EXPECTED_TOOLS: Record<string, string[]> = {
  get_recent_messages: ['limit'],
  send_message: ['text', 'idempotency_key'],
  kick_user: ['platform_user_id', 'reason'],
  finish: ['summary'],
};

// 会话：按 runId 记录轮次与剧本队列。
const sessions = new Map<string, Session>();
const runQueues = new Map<string, ScriptItem[]>(); // /__control 给指定 runId 预设的队列
let defaultQueue: ScriptItem[] = []; // /__control 设的全局默认队列
let toolSeq = 0; // tool_use.id 全局自增，保证正常情况下不重复

// 审计行为：pass | fail | 500 | bad-body | slow | hang
let auditMode = 'pass';
let auditDelayMs = 8000;

// ---------- 小工具 ----------
function sleep(ms: number): Promise<void> {
  const { promise, resolve } = Promise.withResolvers<void>();
  setTimeout(resolve, ms);
  return promise;
}

function readBody(req: http.IncomingMessage, limit = 1 << 20): Promise<string> {
  const { promise, resolve, reject } = Promise.withResolvers<string>();
  const chunks: Buffer[] = [];
  let size = 0;
  req.on('data', (c: Buffer) => {
    size += c.length;
    if (size > limit) {
      reject(new Error('body too large'));
      req.destroy();
      return;
    }
    chunks.push(c);
  });
  req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
  req.on('error', reject);
  return promise;
}

function sendJson(res: http.ServerResponse, status: number, obj: unknown): void {
  const body = JSON.stringify(obj);
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8' });
  res.end(body);
}

function sendText(res: http.ServerResponse, status: number, text: string): void {
  res.writeHead(status, { 'content-type': 'text/plain; charset=utf-8' });
  res.end(text);
}
function errBody(code: string, message: string): JsonRecord {
  return { error: { code, message, requestId: randomUUID() } };
}

// 校验 tools：必须恰好 4 个、名字齐全、input_schema 为合法 JSON Schema
// 且 required 覆盖全部入参；不合则返回原因串，合法返回 null。
function validateTools(tools: unknown): string | null {
  if (!Array.isArray(tools) || tools.length !== 4) return 'tools 必须恰好是 4 个';
  const seen = new Set<string>();
  for (const t of tools) {
    if (typeof t !== 'object' || t === null || Array.isArray(t)) return 'tool 条目必须是对象';
    const rec = t as JsonRecord;
    const name = rec.name;
    if (typeof name !== 'string' || !(name in EXPECTED_TOOLS)) return `未知工具: ${String(name)}`;
    if (seen.has(name)) return `工具重复: ${name}`;
    seen.add(name);
    const schema = rec.input_schema;
    if (typeof schema !== 'object' || schema === null || Array.isArray(schema)) {
      return `${name} 的 input_schema 不是对象`;
    }
    const srec = schema as JsonRecord;
    if (srec.type !== 'object') {
      return `${name} 的 input_schema 不是合法 JSON Schema（需要 type=object）`;
    }
    const props = srec.properties;
    const required = srec.required;
    if (typeof props !== 'object' || props === null || Array.isArray(props)) {
      return `${name} 的 input_schema 缺少 properties`;
    }
    if (!Array.isArray(required)) return `${name} 的 input_schema 缺少 required`;
    const prec = props as JsonRecord;
    const params = EXPECTED_TOOLS[name];
    if (params) {
      for (const p of params) {
        if (!(p in prec)) return `${name} 的 input_schema.properties 缺少入参 ${p}`;
        if (!required.includes(p)) return `${name} 的 input_schema.required 未覆盖入参 ${p}`;
      }
    }
  }
  return null;
}

function toolUse(id: string, name: string, input: JsonRecord): TurnResponse {
  return { stop_reason: 'tool_use', content: [{ type: 'tool_use', id, name, input }] };
}

function endTurn(text: string): TurnResponse {
  return { stop_reason: 'end_turn', content: [{ type: 'text', text }] };
}

// 从 messages 历史里找最近一次 send_message 的 idempotency_key（供同 key 重试剧本）。
function lastSendKey(messages: unknown): string | null {
  if (!Array.isArray(messages)) return null;
  let key: string | null = null;
  for (const m of messages) {
    if (typeof m !== 'object' || m === null) continue;
    const msg = m as JsonRecord;
    if (msg.role !== 'assistant' || !Array.isArray(msg.content)) continue;
    for (const b of msg.content) {
      if (typeof b !== 'object' || b === null) continue;
      const blk = b as JsonRecord;
      if (blk.type !== 'tool_use' || blk.name !== 'send_message') continue;
      if (typeof blk.input !== 'object' || blk.input === null || Array.isArray(blk.input)) continue;
      const inRec = blk.input as JsonRecord;
      if (typeof inRec.idempotency_key === 'string') key = inRec.idempotency_key;
    }
  }
  return key;
}

// 把 /__control 传来的 script 数组规范化为 ScriptItem[]；不合法返回 null。
function normalizeScript(v: unknown): ScriptItem[] | null {
  if (!Array.isArray(v)) return null;
  const out: ScriptItem[] = [];
  for (const item of v) {
    if (typeof item === 'string') {
      out.push(item);
      continue;
    }
    if (typeof item !== 'object' || item === null || Array.isArray(item)) return null;
    const rec = item as JsonRecord;
    if (typeof rec.name !== 'string') return null;
    const norm: ScriptObject = { name: rec.name };
    if (typeof rec.delayMs === 'number') norm.delayMs = rec.delayMs;
    if (typeof rec.text === 'string') norm.text = rec.text;
    if (typeof rec.input === 'object' && rec.input !== null && !Array.isArray(rec.input)) {
      norm.input = rec.input as JsonRecord;
    }
    out.push(norm);
  }
  return out;
}

// 生成合法 tools（/​/__control 自检展示用）；description 固定占位。
function sampleTools(): JsonRecord[] {
  return Object.entries(EXPECTED_TOOLS).map(([name, params]) => ({
    name,
    description: `mock ${name}`,
    input_schema: {
      type: 'object',
      properties: Object.fromEntries(params.map((p) => [p, { type: 'string' }])),
      required: params,
    },
  }));
}

// ---------- /agent/turn ----------
async function handleTurn(raw: string, res: http.ServerResponse): Promise<void> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    sendJson(res, 400, errBody('INVALID_REQUEST', '请求体不是合法 JSON'));
    return;
  }
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
    sendJson(res, 400, errBody('INVALID_REQUEST', '请求体必须是 JSON 对象'));
    return;
  }
  const body = parsed as JsonRecord;
  if (typeof body.runId !== 'string' || body.runId.length === 0) {
    sendJson(res, 400, errBody('INVALID_REQUEST', '缺少 runId'));
    return;
  }
  const toolsProblem = validateTools(body.tools);
  if (toolsProblem) {
    sendJson(res, 400, errBody('TOOLS_INVALID', toolsProblem));
    return;
  }

  const runId: string = body.runId;
  let session = sessions.get(runId);
  if (!session) {
    const preset = runQueues.get(runId);
    if (preset) runQueues.delete(runId);
    session = { count: 0, queue: preset ? [...preset] : [...defaultQueue], lastToolUseId: null };
    sessions.set(runId, session);
  }

  // 取剧本头；loop-get 为粘性剧本（不消耗，一直重复同样调用）。
  const item: ScriptItem | undefined = session.queue[0];
  const name = typeof item === 'string' ? item : item?.name;
  if (item !== undefined && name !== 'loop-get') session.queue.shift();
  // 剧本对象附带的参数就地解析（内部已规范化形状，直接取用）。
  let delayMs = 0;
  let text: string | undefined;
  let input: JsonRecord = {};
  if (item !== undefined && typeof item !== 'string') {
    if (typeof item.delayMs === 'number') delayMs = item.delayMs;
    if (typeof item.text === 'string') text = item.text;
    if (item.input !== undefined) input = item.input;
  }

  // 延迟：对象形式的 delayMs，或 slow[:ms] 后缀。
  if (name === 'slow') delayMs = delayMs || 8000;
  else if (typeof name === 'string' && name.startsWith('slow:')) {
    delayMs = delayMs || Number(name.slice(5)) || 8000;
  }
  if (delayMs > 0) await sleep(delayMs);

  session.count += 1;
  const n = session.count;
  const newId = (): string => `tu_${++toolSeq}`;

  // 无剧本时的默认推进：首轮 get_recent_messages，次轮 send_message(key k-1)，再 finish。
  if (item === undefined) {
    if (n === 1) {
      const id = newId();
      session.lastToolUseId = id;
      sendJson(res, 200, toolUse(id, 'get_recent_messages', { limit: 10 }));
      return;
    }
    if (n === 2) {
      const id = newId();
      session.lastToolUseId = id;
      sendJson(res, 200, toolUse(id, 'send_message', { text: '你好，我是群助手。', idempotency_key: 'k-1' }));
      return;
    }
    if (n === 3) {
      const id = newId();
      session.lastToolUseId = id;
      sendJson(res, 200, toolUse(id, 'finish', { summary: '已查看近期消息并回复，本轮结束。' }));
      return;
    }
    sendJson(res, 200, endTurn('done'));
    return;
  }

  switch (name) {
    case 'bad-json':
      // 非法 JSON 体（后端应记 BAD_JSON 协议错误）。
      sendText(res, 200, 'not-json{{{body broken');
      return;
    case 'markdown-fence':
    case 'fence':
      // JSON 外套 markdown 围栏 + 前后文字，同样算不合法。
      sendText(
        res,
        200,
        '好的，这是我的回复：\n```json\n{"stop_reason":"end_turn","content":[{"type":"text","text":"done"}]}\n```\n请继续。',
      );
      return;
    case 'unknown-tool': {
      const id = newId();
      session.lastToolUseId = id;
      sendJson(res, 200, toolUse(id, 'nonexistent_tool_xyz', {}));
      return;
    }
    case 'duplicate-id': {
      // 复用上一轮的 tool_use.id（后端应记 DUPLICATE_TOOL_USE_ID 协议错误）。
      const id = session.lastToolUseId ?? 'tu_dup_0';
      sendJson(res, 200, toolUse(id, 'get_recent_messages', { limit: 10 }));
      return;
    }
    case 'same-key-retry': {
      // 用历史里最近一次 send_message 的 key 再调一次（新 id）。
      const id = newId();
      session.lastToolUseId = id;
      const key = lastSendKey(body.messages) ?? 'k-1';
      sendJson(res, 200, toolUse(id, 'send_message', { text: '你好，我是群助手。', idempotency_key: key }));
      return;
    }
    case 'loop-get': {
      const id = newId();
      session.lastToolUseId = id;
      sendJson(res, 200, toolUse(id, 'get_recent_messages', { limit: 10 }));
      return;
    }
    case 'limit-huge': {
      const id = newId();
      session.lastToolUseId = id;
      sendJson(res, 200, toolUse(id, 'get_recent_messages', { limit: 100000 }));
      return;
    }
    case 'slow':
    case 'get-recent': {
      const id = newId();
      session.lastToolUseId = id;
      const limit = typeof input.limit === 'number' ? input.limit : 10;
      sendJson(res, 200, toolUse(id, 'get_recent_messages', { limit }));
      return;
    }
    case 'send-message': {
      const id = newId();
      session.lastToolUseId = id;
      const msgText = typeof input.text === 'string' ? input.text : '你好，我是群助手。';
      const key = typeof input.idempotency_key === 'string' ? input.idempotency_key : 'k-1';
      sendJson(res, 200, toolUse(id, 'send_message', { text: msgText, idempotency_key: key }));
      return;
    }
    case 'kick-user': {
      const id = newId();
      session.lastToolUseId = id;
      const target = typeof input.platform_user_id === 'string' ? input.platform_user_id : 'u-external-1';
      const reason = typeof input.reason === 'string' ? input.reason : 'mock kick';
      sendJson(res, 200, toolUse(id, 'kick_user', { platform_user_id: target, reason }));
      return;
    }
    case 'finish': {
      const id = newId();
      session.lastToolUseId = id;
      sendJson(res, 200, toolUse(id, 'finish', { summary: text ?? 'mock 运行结束。' }));
      return;
    }
    case 'end-turn':
    case 'end_turn':
      sendJson(res, 200, endTurn(text ?? 'ok'));
      return;
    default:
      sendJson(res, 500, errBody('UNKNOWN_SCRIPT', `未知剧本: ${String(name)}`));
      return;
  }
}

// ---------- /agent/audit ----------
async function handleAudit(raw: string, res: http.ServerResponse): Promise<void> {
  try {
    JSON.parse(raw);
  } catch {
    sendJson(res, 400, errBody('INVALID_REQUEST', '请求体不是合法 JSON'));
    return;
  }
  switch (auditMode) {
    case 'fail':
      sendJson(res, 200, { verdict: 'fail', reason: 'mock 审计拒绝' });
      return;
    case '500':
      sendJson(res, 500, errBody('AUDIT_MOCK_ERROR', 'mock 审计 500'));
      return;
    case 'bad-body':
      sendText(res, 200, 'ok-but-not-json{{{');
      return;
    case 'slow':
      await sleep(auditDelayMs);
      sendJson(res, 200, { verdict: 'pass', reason: 'mock 审计通过（慢响应）' });
      return;
    case 'hang':
      // 生效后不返回，由调用方超时；连接保持 10 分钟后兜底关闭。
      await sleep(600_000);
      sendJson(res, 200, { verdict: 'pass', reason: 'mock 审计通过' });
      return;
    default:
      sendJson(res, 200, { verdict: 'pass', reason: 'mock 审计通过' });
      return;
  }
}

// ---------- /__control ----------
// POST body（字段均可选）：
//   { runId?, script?: (string|{name,delayMs?,input?,text?})[] }  // 有 runId 则只对该 run，无则设全局默认
//   { audit?: 'pass'|'fail'|'500'|'bad-body'|'slow'|'hang'|{mode,delayMs?}, auditDelayMs? }
//   { reset?: true, runId? }  // 有 runId 只清该 run，否则全清并恢复默认
function handleControl(raw: string, res: http.ServerResponse, method: string): void {
  if (method === 'GET') {
    const sessionView: Record<string, JsonRecord> = {};
    for (const [k, v] of sessions) {
      sessionView[k] = { count: v.count, queue: v.queue, lastToolUseId: v.lastToolUseId };
    }
    const queues: Record<string, ScriptItem[]> = {};
    for (const [k, v] of runQueues) queues[k] = v;
    sendJson(res, 200, {
      defaultScript: defaultQueue,
      runQueues: queues,
      sessions: sessionView,
      audit: { mode: auditMode, delayMs: auditDelayMs },
      sampleTools: sampleTools(),
    });
    return;
  }
  let parsed: unknown;
  try {
    parsed = raw.length === 0 ? {} : JSON.parse(raw);
  } catch {
    sendJson(res, 400, errBody('INVALID_REQUEST', 'control 请求体不是合法 JSON'));
    return;
  }
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
    sendJson(res, 400, errBody('INVALID_REQUEST', 'control 请求体必须是 JSON 对象'));
    return;
  }
  const body = parsed as JsonRecord;
  if (body.reset === true) {
    if (typeof body.runId === 'string') {
      sessions.delete(body.runId);
      runQueues.delete(body.runId);
    } else {
      sessions.clear();
      runQueues.clear();
      defaultQueue = [];
      auditMode = 'pass';
      auditDelayMs = 8000;
    }
    sendJson(res, 200, { ok: true });
    return;
  }
  if (body.script !== undefined) {
    const norm = normalizeScript(body.script);
    if (!norm) {
      sendJson(res, 400, errBody('INVALID_REQUEST', 'script 必须是字符串或 {name,…} 对象的数组'));
      return;
    }
    if (typeof body.runId === 'string') {
      runQueues.set(body.runId, [...norm]);
      const s = sessions.get(body.runId);
      if (s) {
        // 运行中改剧本：替换队列并从头按序返回。
        s.queue = [...norm];
        s.count = 0;
      }
    } else {
      defaultQueue = [...norm];
    }
  }
  const a = body.audit;
  if (a !== undefined) {
    if (typeof a === 'string') {
      auditMode = a;
      if (typeof body.auditDelayMs === 'number') auditDelayMs = body.auditDelayMs;
    } else if (typeof a === 'object' && a !== null && !Array.isArray(a)) {
      const arec = a as JsonRecord;
      if (typeof arec.mode === 'string') auditMode = arec.mode;
      if (typeof arec.delayMs === 'number') auditDelayMs = arec.delayMs;
    } else {
      sendJson(res, 400, errBody('INVALID_REQUEST', 'audit 必须是字符串或对象'));
      return;
    }
  }
  sendJson(res, 200, { ok: true, audit: { mode: auditMode, delayMs: auditDelayMs } });
}

// ---------- 路由 ----------
const server = http.createServer((req, res) => {
  (async () => {
    const url = new URL(req.url || '/', 'http://x');
    const path = url.pathname;
    const method = (req.method || 'GET').toUpperCase();
    if (method === 'GET' && path === '/health') {
      sendJson(res, 200, { ok: true });
      return;
    }
    if (method === 'POST' && path === '/agent/turn') {
      await handleTurn(await readBody(req), res);
      return;
    }
    if (method === 'POST' && path === '/agent/audit') {
      await handleAudit(await readBody(req), res);
      return;
    }
    if (path === '/__control' && (method === 'POST' || method === 'GET')) {
      handleControl(method === 'GET' ? '' : await readBody(req), res, method);
      return;
    }
    sendJson(res, 404, errBody('NOT_FOUND', `未知路径 ${path}`));
  })().catch((e: unknown) => {
    if (!res.writableEnded) {
      const msg = e instanceof Error ? e.message : String(e);
      sendJson(res, 500, errBody('INTERNAL', msg));
    }
  });
});

server.listen(PORT, () => {
  console.log(`mock-agent listening on :${PORT}`);
});
