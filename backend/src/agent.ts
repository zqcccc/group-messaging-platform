// Agent 接入（A5 全条 + S5/S6）：触发建 run、主循环、工具执行、审计、恢复 + 查询路由。
// 出站发送走 send.ts 的 createOutbound（与操作员发送、序列共用一条出站流水线）。
import { Request, Response, Router } from 'express';
import { v4 as uuidv4 } from 'uuid';
import { requireAuth } from './auth';
import { getPool } from './db';
import { GatewayError, gwKick, gwMembers } from './gateway';
import { applyTerminal, createOutbound, markGroupUnreachable, pollLanding } from './send';
import { asyncHandler, sendError } from './util';
import { emit } from './ws';

// ---------- 常量 ----------

const MAX_STEPS = 12; // 上限 12 步（含结束步）
const WALL_BUDGET_MS = 60000; // run 创建起 60 秒（含审计耗时；重启后按 active_ms 累计，停机不计）
const TURN_TIMEOUT_MS = 12000; // /agent/turn 每轮 12s 超时（10–15s 可配区间内）
const AUDIT_TIMEOUT_MS = 10000; // 单次审计超时
const AUDIT_MAX_ATTEMPTS = 3; // 拿不到明确结论时同一次工具调用最多尝试 3 次
const CONFIRM_SUPPLEMENT_MS = 3000; // createOutbound 返回 accepted/unknown 后的补充确认
const RAW_RESPONSE_LIMIT = 2048; // rawResponse 截断到 2KB
const TOOL_CONTENT_LIMIT = 8192; // 单个 tool_result content ≤ 8KB
const RECENT_TEXT_LIMIT = 500; // 单条 text 超 500 字截断
const SUMMARY_LIMIT = 200; // resultSummary ≤ 200 字

// ---------- 类型 ----------

export interface InboundTrigger {
  msgId: string;
  senderPlatformUserId: string;
  text: string;
  sentAt: string;
}

interface StepRec {
  kind: string;
  toolUseId: string | null;
  name: string | null;
  input: unknown;
  resultSummary: string | null;
  isError: boolean;
  errorCode: string | null;
  auditVerdict: string | null;
  rawResponse: string | null;
}

interface AgentRunState {
  id: string;
  groupId: string;
  status: string;
  history: unknown[];
  steps: StepRec[];
  consec: number;
  usedKeys: Record<string, string>;
  activeMs: number;
  stepCount: number;
  deadlineAt: string | null;
}

interface GroupInfo {
  id: string;
  gatewayGroupId: string;
  status: string;
  agentEnabled: boolean;
  autoKickEnabled: boolean;
}

// 同进程 runLoop 重入 guard（同 runId 只跑一个循环；跨进程靠 step_count CAS 仲裁）。
const activeLoops = new Set<string>();

// ---------- 小工具 ----------

function sleep(ms: number): Promise<void> {
  return new Promise<void>((resolve) => setTimeout(resolve, ms));
}

function agentBase(): string {
  return (process.env.AGENT_URL ?? '').replace(/\/+$/, '');
}

function asArray(v: unknown): unknown[] {
  return Array.isArray(v) ? v : [];
}

// jsonb 对象收敛为 string 值表（used_idempotency_keys 的值均为 clientMsgId）。
function asStrMap(v: unknown): Record<string, string> {
  if (typeof v !== 'object' || v === null || Array.isArray(v)) return {};
  const out: Record<string, string> = {};
  for (const [k, val] of Object.entries(v)) {
    if (typeof val === 'string') out[k] = val;
  }
  return out;
}

function capChars(s: string, n: number): string {
  const chars = Array.from(s);
  return chars.length > n ? `${chars.slice(0, n).join('')}…` : s;
}

function truncBytes(s: string, n: number): string {
  if (Buffer.byteLength(s, 'utf8') <= n) return s;
  let lo = 0;
  let hi = s.length;
  while (lo < hi) {
    const mid = Math.floor((lo + hi + 1) / 2);
    if (Buffer.byteLength(s.slice(0, mid), 'utf8') <= n) lo = mid;
    else hi = mid - 1;
  }
  return s.slice(0, lo);
}

function isUniqueViolation(e: unknown): boolean {
  return typeof e === 'object' && e !== null && (e as { code?: unknown }).code === '23505';
}

function isoOf(v: Date | string): string {
  return v instanceof Date ? v.toISOString() : String(v);
}

// ---------- DB 存取 ----------

async function loadRun(runId: string): Promise<AgentRunState | null> {
  const { rows } = await getPool().query<{
    id: string;
    group_id: string;
    status: string;
    history: unknown;
    steps: unknown;
    consec_protocol_errors: number;
    used_idempotency_keys: unknown;
    active_ms: number;
    step_count: number;
    deadline_at: Date | string | null;
  }>(
    `SELECT id, group_id, status, history, steps, consec_protocol_errors,
            used_idempotency_keys, active_ms, step_count, deadline_at
     FROM agent_runs WHERE id = $1`,
    [runId],
  );
  if (rows.length === 0) return null;
  const r = rows[0];
  return {
    id: r.id,
    groupId: r.group_id,
    status: r.status,
    history: asArray(r.history),
    steps: asArray(r.steps) as StepRec[],
    consec: Number(r.consec_protocol_errors ?? 0),
    usedKeys: asStrMap(r.used_idempotency_keys),
    activeMs: Number(r.active_ms ?? 0),
    stepCount: Number(r.step_count ?? 0),
    deadlineAt: r.deadline_at == null ? null : isoOf(r.deadline_at),
  };
}

async function loadGroup(groupIdOrGateway: string): Promise<GroupInfo | null> {
  const { rows } = await getPool().query<{
    id: string;
    gateway_group_id: string;
    status: string;
    agent_enabled: boolean;
    auto_kick_enabled: boolean;
  }>(
    `SELECT id, gateway_group_id, status, agent_enabled, auto_kick_enabled
     FROM groups WHERE id::text = $1 OR gateway_group_id = $1 LIMIT 1`,
    [groupIdOrGateway],
  );
  if (rows.length === 0) return null;
  const r = rows[0];
  return {
    id: r.id,
    gatewayGroupId: r.gateway_group_id,
    status: r.status,
    agentEnabled: r.agent_enabled === true,
    autoKickEnabled: r.auto_kick_enabled === true,
  };
}

async function loadOwnPlatformUserIds(): Promise<string[]> {
  const { rows } = await getPool().query<{ platform_user_id: string | null }>(
    `SELECT platform_user_id FROM accounts WHERE platform_user_id IS NOT NULL`,
  );
  const out: string[] = [];
  for (const r of rows) {
    if (typeof r.platform_user_id === 'string' && r.platform_user_id.length > 0) out.push(r.platform_user_id);
  }
  return out;
}

async function emitAgentRun(runId: string, groupId: string, status: string, endReason: string | null): Promise<void> {
  try {
    await emit('agent_run', { runId, groupId, status, endReason });
  } catch {
    // 事件落库失败不影响主流程（WS broadcaster 会补推 ws_events 行；落库都失败则只打日志）。
  }
}

// ---------- 触发 ----------

// sse.ts 入库后调用此函数：
// onInboundMessage(groupId, { msgId, senderPlatformUserId, text, sentAt })。
// groupId 兼容内部 UUID 与网关 groupId 两种传参。
export function onInboundMessage(
  groupId: string,
  msg: { msgId: string; senderPlatformUserId: string; text: string; sentAt: string },
): void {
  void handleInbound(groupId, msg).catch((e: unknown) => {
    console.error(`[agent] 入站触发失败 group=${groupId}:`, e instanceof Error ? e.message : e);
  });
}

async function handleInbound(
  groupId: string,
  msg: { msgId: string; senderPlatformUserId: string; text: string; sentAt: string },
): Promise<void> {
  const pool = getPool();
  // 发送者是服务账号（自己消息回流）直接忽略，不触发。
  const own = await loadOwnPlatformUserIds();
  if (own.includes(msg.senderPlatformUserId)) return;
  const group = await loadGroup(groupId);
  if (!group) return;
  // 未开 agent 或群不可写（unreachable）不再触发。
  if (!group.agentEnabled || group.status !== 'active') return;
  const trigger: InboundTrigger = { msgId: msg.msgId, senderPlatformUserId: msg.senderPlatformUserId, text: msg.text, sentAt: msg.sentAt };
  const runId = uuidv4();
  // 同一群同一时刻至多一个 running（多实例也成立，靠部分唯一索引仲裁）：
  // INSERT 抢到即新 run；冲突则把消息追加到已在运行的 run 的 pending。
  for (let attempt = 0; attempt < 3; attempt += 1) {
    try {
      await pool.query(
        `INSERT INTO agent_runs(id, group_id, status, history, steps, pending_triggers,
                                consec_protocol_errors, used_idempotency_keys, active_ms, deadline_at, step_count)
         VALUES ($1, $2, 'running', '[]', '[]', $3::jsonb, 0, '{}', 0, now() + interval '60 seconds', 0)`,
        [runId, group.id, JSON.stringify([trigger])],
      );
      await emitAgentRun(runId, group.id, 'running', null);
      void runLoop(runId);
      return;
    } catch (e) {
      if (!isUniqueViolation(e)) throw e;
    }
    // 冲突：追加到当前 running run 的待处理（只追加仍在 running 的行；已结束则重试 INSERT）。
    const appended = await pool.query(
      `UPDATE agent_runs SET pending_triggers = pending_triggers || $2::jsonb
       WHERE group_id = $1 AND status = 'running'`,
      [group.id, JSON.stringify([trigger])],
    );
    if ((appended.rowCount ?? 0) > 0) return;
    // running 的 run 在两者之间结束了：退避后重试 INSERT。
    await sleep(50);
  }
}

// ---------- /agent/turn 调用与校验 ----------

function agentTools(): unknown[] {
  return [
    {
      name: 'get_recent_messages',
      description: '读取本群最近的入站消息（含触发消息与 run 期间新到消息），按 sentAt 升序返回',
      input_schema: {
        type: 'object',
        properties: { limit: { type: 'integer', minimum: 1, maximum: 50 } },
        required: ['limit'],
      },
    },
    {
      name: 'send_message',
      description: '以服务账号身份向本群发送一条文本消息',
      input_schema: {
        type: 'object',
        properties: { text: { type: 'string' }, idempotency_key: { type: 'string' } },
        required: ['text', 'idempotency_key'],
      },
    },
    {
      name: 'kick_user',
      description: '将指定用户移出本群（需要群开启 autoKick 且执行账号为群主或管理员）',
      input_schema: {
        type: 'object',
        properties: { platform_user_id: { type: 'string' }, reason: { type: 'string' } },
        required: ['platform_user_id', 'reason'],
      },
    },
    {
      name: 'finish',
      description: '结束本轮运行并给出总结',
      input_schema: {
        type: 'object',
        properties: { summary: { type: 'string' } },
        required: ['summary'],
      },
    },
  ];
}

async function postTurn(
  runId: string,
  messages: unknown[],
): Promise<{ status: number | null; body: string | null; timeout: boolean }> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), TURN_TIMEOUT_MS);
  try {
    const res = await fetch(`${agentBase()}/agent/turn`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ runId, tools: agentTools(), messages }),
      signal: ctrl.signal,
    });
    const text = await res.text().catch(() => '');
    return { status: res.status, body: text, timeout: false };
  } catch {
    // 超时后才到的响应被丢弃（AbortController 已取消请求）。
    return { status: null, body: null, timeout: true };
  } finally {
    clearTimeout(timer);
  }
}

type TurnParse =
  | { kind: 'tool_use'; id: string; name: string; input: unknown; raw: string }
  | { kind: 'end_turn'; text: string; raw: string }
  | { kind: 'bad'; hint: string; raw: string | null };

function validateTurn(status: number | null, body: string | null): TurnParse {
  if (status === null || body === null) return { kind: 'bad', hint: 'turn 请求失败（网络错误或超时）', raw: null };
  if (status < 200 || status >= 300) return { kind: 'bad', hint: `turn 返回非 2xx（status=${status}）`, raw: body };
  let parsed: unknown;
  try {
    parsed = JSON.parse(body);
  } catch {
    return { kind: 'bad', hint: '响应体不是合法 JSON（含 markdown 围栏或前后夹文字）', raw: body };
  }
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
    return { kind: 'bad', hint: '响应体不是 JSON 对象', raw: body };
  }
  if (!('stop_reason' in parsed) || typeof parsed.stop_reason !== 'string') {
    return { kind: 'bad', hint: '缺少 stop_reason', raw: body };
  }
  if (!('content' in parsed) || !Array.isArray(parsed.content) || parsed.content.length !== 1) {
    const n = 'content' in parsed && Array.isArray(parsed.content) ? parsed.content.length : -1;
    return { kind: 'bad', hint: `content 块数不等于 1（实际 ${n === -1 ? '非数组' : n}）`, raw: body };
  }
  const block: unknown = parsed.content[0];
  if (typeof block !== 'object' || block === null || Array.isArray(block)) {
    return { kind: 'bad', hint: 'content 块不是对象', raw: body };
  }
  if (parsed.stop_reason === 'tool_use') {
    if (!('type' in block) || block.type !== 'tool_use') {
      return { kind: 'bad', hint: 'stop_reason 与块类型不一致（tool_use 期望 tool_use 块）', raw: body };
    }
    if (!('id' in block) || typeof block.id !== 'string' || block.id.length === 0) {
      return { kind: 'bad', hint: 'tool_use 块缺少 id', raw: body };
    }
    if (!('name' in block) || typeof block.name !== 'string' || block.name.length === 0) {
      return { kind: 'bad', hint: 'tool_use 块缺少 name', raw: body };
    }
    if (!('input' in block) || typeof block.input !== 'object' || block.input === null || Array.isArray(block.input)) {
      return { kind: 'bad', hint: 'tool_use 块 input 不是对象', raw: body };
    }
    return { kind: 'tool_use', id: block.id, name: block.name, input: block.input, raw: body };
  }
  if (parsed.stop_reason === 'end_turn') {
    if (!('type' in block) || block.type !== 'text') {
      return { kind: 'bad', hint: 'stop_reason 与块类型不一致（end_turn 期望 text 块）', raw: body };
    }
    if (!('text' in block) || typeof block.text !== 'string') {
      return { kind: 'bad', hint: 'end_turn 的 text 块缺少 text', raw: body };
    }
    return { kind: 'end_turn', text: block.text, raw: body };
  }
  return { kind: 'bad', hint: `未知 stop_reason: ${parsed.stop_reason}`, raw: body };
}

// 入参校验：未知工具 / 入参不合 schema（返回码；正常记步，不算协议错误）。
function validateToolInput(name: string, input: unknown): 'UNKNOWN_TOOL' | 'INVALID_INPUT' | null {
  if (typeof input !== 'object' || input === null || Array.isArray(input)) return 'INVALID_INPUT';
  if (!('limit' in input) && name === 'get_recent_messages') return 'INVALID_INPUT';
  switch (name) {
    case 'get_recent_messages': {
      const rec = input as { limit?: unknown };
      return typeof rec.limit === 'number' && Number.isFinite(rec.limit) ? null : 'INVALID_INPUT';
    }
    case 'send_message': {
      const rec = input as { text?: unknown; idempotency_key?: unknown };
      if (typeof rec.text !== 'string' || rec.text.length === 0) return 'INVALID_INPUT';
      if (typeof rec.idempotency_key !== 'string' || rec.idempotency_key.length === 0) return 'INVALID_INPUT';
      return null;
    }
    case 'kick_user': {
      const rec = input as { platform_user_id?: unknown; reason?: unknown };
      if (typeof rec.platform_user_id !== 'string' || rec.platform_user_id.length === 0) return 'INVALID_INPUT';
      if (typeof rec.reason !== 'string') return 'INVALID_INPUT';
      return null;
    }
    case 'finish': {
      const rec = input as { summary?: unknown };
      return typeof rec.summary === 'string' ? null : 'INVALID_INPUT';
    }
    default:
      return 'UNKNOWN_TOOL';
  }
}

// ---------- 审计 ----------

async function auditOnce(text: string, groupId: string): Promise<'pass' | 'fail' | 'unknown'> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), AUDIT_TIMEOUT_MS);
  try {
    const res = await fetch(`${agentBase()}/agent/audit`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ text, groupId }),
      signal: ctrl.signal,
    });
    if (!res.ok) return 'unknown';
    const data: unknown = await res.json().catch(() => null);
    if (typeof data !== 'object' || data === null || Array.isArray(data)) return 'unknown';
    if (!('verdict' in data)) return 'unknown';
    if (data.verdict === 'pass') return 'pass';
    if (data.verdict === 'fail') return 'fail';
    return 'unknown';
  } catch {
    return 'unknown';
  } finally {
    clearTimeout(timer);
  }
}

// 同一次工具调用最多尝试 3 次（耗时计入 60 秒；单次失败不返回给 agent、不计步）。
async function auditWithRetry(text: string, groupId: string): Promise<'pass' | 'fail' | 'blocked'> {
  for (let i = 0; i < AUDIT_MAX_ATTEMPTS; i += 1) {
    const r = await auditOnce(text, groupId);
    if (r === 'pass') return 'pass';
    if (r === 'fail') return 'fail';
  }
  return 'blocked';
}

// ---------- history 扫描（恢复用） ----------

interface PastToolUse {
  id: string;
  name: string;
  input: Record<string, unknown>;
}

function listAssistantToolUses(history: unknown[]): PastToolUse[] {
  const out: PastToolUse[] = [];
  for (const m of history) {
    if (typeof m !== 'object' || m === null || Array.isArray(m)) continue;
    if (!('role' in m) || m.role !== 'assistant') continue;
    if (!('content' in m) || !Array.isArray(m.content)) continue;
    for (const b of m.content) {
      if (typeof b !== 'object' || b === null || Array.isArray(b)) continue;
      if (!('type' in b) || b.type !== 'tool_use') continue;
      if (!('id' in b) || typeof b.id !== 'string') continue;
      if (!('name' in b) || typeof b.name !== 'string') continue;
      // input 由本进程写入时恒为对象；历史坏数据按空对象处理。
      const holder = b as { input?: unknown };
      const input =
        typeof holder.input === 'object' && holder.input !== null && !Array.isArray(holder.input)
          ? (holder.input as Record<string, unknown>)
          : {};
      out.push({ id: b.id, name: b.name, input });
    }
  }
  return out;
}

function hasToolResult(history: unknown[], toolUseId: string): boolean {
  for (const m of history) {
    if (typeof m !== 'object' || m === null || Array.isArray(m)) continue;
    if (!('role' in m) || m.role !== 'user') continue;
    if (!('content' in m) || !Array.isArray(m.content)) continue;
    for (const b of m.content) {
      if (typeof b !== 'object' || b === null || Array.isArray(b)) continue;
      if ('type' in b && b.type === 'tool_result' && 'tool_use_id' in b && b.tool_use_id === toolUseId) return true;
    }
  }
  return false;
}

// 崩溃前已发出 tool_use 但结果没落库：网关可能已生效，不能盲目重发。
function hasUnresultedSend(history: unknown[], key: string): boolean {
  return listAssistantToolUses(history).some(
    (u) => u.name === 'send_message' && u.input.idempotency_key === key && !hasToolResult(history, u.id),
  );
}

function hasUnresultedKick(history: unknown[], target: string): boolean {
  return listAssistantToolUses(history).some(
    (u) => u.name === 'kick_user' && u.input.platform_user_id === target && !hasToolResult(history, u.id),
  );
}

// ---------- 工具执行 ----------

type AuditVerdict = 'pass' | 'fail' | null;

interface ToolExecOk {
  ok: true;
  content: Record<string, unknown>;
  summary: string;
  auditVerdict: AuditVerdict;
}

interface ToolExecErr {
  ok: false;
  code: string;
  message: string;
  auditVerdict: AuditVerdict;
}

type ToolExec = ToolExecOk | ToolExecErr;

function errResult(code: string, message: string, auditVerdict: AuditVerdict): ToolExecErr {
  return { ok: false, code, message, auditVerdict };
}

async function execGetRecent(groupId: string, limitRaw: unknown): Promise<ToolExec> {
  const limitNum = typeof limitRaw === 'number' ? Math.floor(limitRaw) : Number.NaN;
  if (!Number.isFinite(limitNum) || limitNum < 1) return errResult('INVALID_INPUT', 'limit 必须为 ≥1 的数字', null);
  const eff = Math.min(limitNum, 50);
  const pool = getPool();
  const total = await pool.query<{ c: string }>(`SELECT COUNT(*) AS c FROM inbound_messages WHERE group_id = $1`, [
    groupId,
  ]);
  const totalCount = Number(total.rows[0]?.c ?? 0);
  const { rows } = await pool.query<{
    msg_id: string;
    sender_platform_user_id: string;
    is_own: boolean;
    text: string;
    sent_at: Date | string;
  }>(
    `SELECT msg_id, sender_platform_user_id, is_own, text, sent_at
     FROM inbound_messages WHERE group_id = $1
     ORDER BY sent_at DESC, msg_id DESC LIMIT $2`,
    [groupId, eff],
  );
  let topTruncated = totalCount > eff;
  const items = [...rows].reverse().map((r) => {
    const text = typeof r.text === 'string' ? r.text : '';
    const cut = Array.from(text).length > RECENT_TEXT_LIMIT;
    if (cut) topTruncated = true;
    return {
      msgId: r.msg_id,
      senderPlatformUserId: r.sender_platform_user_id,
      isOwn: r.is_own === true,
      text: cut ? `${Array.from(text).slice(0, RECENT_TEXT_LIMIT).join('')}…` : text,
      sentAt: isoOf(r.sent_at),
      truncated: cut,
    };
  });
  // 单个 tool_result content ≤ 8KB：超出时丢弃最旧的消息并置 truncated。
  let content = JSON.stringify({ messages: items, truncated: topTruncated });
  while (Buffer.byteLength(content, 'utf8') > TOOL_CONTENT_LIMIT && items.length > 1) {
    items.shift();
    topTruncated = true;
    content = JSON.stringify({ messages: items, truncated: topTruncated });
  }
  if (Buffer.byteLength(content, 'utf8') > TOOL_CONTENT_LIMIT && items.length === 1) {
    const only = items[0];
    const head = Buffer.byteLength(JSON.stringify({ messages: [], truncated: true }), 'utf8');
    only.text = truncBytes(only.text, Math.max(0, TOOL_CONTENT_LIMIT - head - 256));
    only.truncated = true;
    content = JSON.stringify({ messages: items, truncated: true });
  }
  return {
    ok: true,
    content: JSON.parse(content) as Record<string, unknown>,
    summary: `get_recent_messages ok: 返回 ${items.length} 条，truncated=${topTruncated}`,
    auditVerdict: null,
  };
}

async function readOutboundByClientMsgId(
  clientMsgId: string,
): Promise<{ deliveryStatus: string; failCode: string | null } | null> {
  const { rows } = await getPool().query<{ delivery_status: string; fail_code: string | null }>(
    `SELECT delivery_status, fail_code FROM outbound_messages WHERE client_msg_id = $1`,
    [clientMsgId],
  );
  if (rows.length === 0) return null;
  return { deliveryStatus: rows[0].delivery_status, failCode: rows[0].fail_code ?? null };
}

// 出站行状态 → tool_result；accepted/unknown 时做一次补充确认并尽量把 sent 落库。
async function mapOutboundStatus(
  groupId: string,
  gatewayGroupId: string,
  clientMsgId: string,
  elapsedMs: number,
  audited: boolean,
): Promise<ToolExec> {
  const verdict: AuditVerdict = audited ? 'pass' : null;
  const remain = Math.max(0, CONFIRM_SUPPLEMENT_MS - elapsedMs);
  let row = await readOutboundByClientMsgId(clientMsgId);
  if ((!row || ['sending', 'accepted', 'unknown'].includes(row.deliveryStatus)) && remain > 0) {
    try {
      const landed = await pollLanding(gatewayGroupId, clientMsgId, Math.min(remain, CONFIRM_SUPPLEMENT_MS));
      if (landed) {
        await getPool()
          .query(
            `UPDATE outbound_messages SET delivery_status = 'sent', gateway_msg_id = $2, gateway_sent_at = $3
             WHERE client_msg_id = $1 AND delivery_status IN ('queued', 'sending', 'accepted', 'unknown')`,
            [clientMsgId, landed.msgId, landed.sentAt],
          )
          .catch(() => undefined);
        return {
          ok: true,
          content: { clientMsgId, deliveryStatus: 'sent' },
          summary: `send_message ok: clientMsgId=${clientMsgId} deliveryStatus=sent`,
          auditVerdict: verdict,
        };
      }
    } catch {
      // 确认失败则按库里状态继续判定。
    }
    row = await readOutboundByClientMsgId(clientMsgId);
  }
  const status = row?.deliveryStatus ?? 'unknown';
  if (status === 'sent' || status === 'accepted') {
    return {
      ok: true,
      content: { clientMsgId, deliveryStatus: status },
      summary: `send_message ok: clientMsgId=${clientMsgId} deliveryStatus=${status}`,
      auditVerdict: verdict,
    };
  }
  if (status === 'failed') {
    if (row?.failCode === 'GROUP_WRITE_FORBIDDEN') {
      await markGroupUnreachable(groupId).catch(() => undefined);
      return errResult('GROUP_UNREACHABLE', '群不可写（GROUP_WRITE_FORBIDDEN）', verdict);
    }
    return errResult('SEND_FAILED', `消息发送失败（${row?.failCode ?? 'UNKNOWN'}）`, verdict);
  }
  if (status === 'cancelled') return errResult('SEND_FAILED', '发送账号中途进入终态，消息已取消', verdict);
  if (status === 'queued') return errResult('SEND_FAILED', '账号限流中，消息仍在排队', verdict);
  return errResult('SEND_TIMEOUT', '5 秒内无法确认是否发出（SEND_TIMEOUT）', verdict);
}

async function pickSendAccount(groupId: string): Promise<string | null> {
  const { rows } = await getPool().query<{ account_id: string }>(
    `SELECT gm.account_id FROM group_members gm JOIN accounts a ON a.id = gm.account_id
     WHERE gm.group_id = $1 AND a.status = 'online' ORDER BY gm.account_id LIMIT 1`,
    [groupId],
  );
  if (rows.length === 0) return null;
  return rows[0].account_id;
}

async function pickKickAccount(groupId: string): Promise<string | null> {
  const { rows } = await getPool().query<{ account_id: string }>(
    `SELECT gm.account_id FROM group_members gm JOIN accounts a ON a.id = gm.account_id
     WHERE gm.group_id = $1 AND a.status = 'online' AND gm.role IN ('creator', 'admin')
     ORDER BY gm.account_id LIMIT 1`,
    [groupId],
  );
  if (rows.length === 0) return null;
  return rows[0].account_id;
}

// agent 出站 clientMsgId：run 级命名空间 + idempotency_key。
function agentClientMsgId(runId: string, key: string): string {
  return `${runId}:${key}`;
}

async function execSend(
  runId: string,
  groupId: string,
  gatewayGroupId: string,
  key: string,
  text: string,
  usedKeys: Record<string, string>,
  history: unknown[],
): Promise<ToolExec | { blocked: true }> {
  const started = Date.now();
  // clientMsgId 按 run 命名空间派生：幂等只在同一个 run 内成立（A5-7），
  // 不同 run 复用同一个 key 不能互相吞消息；确定性派生保证重启后仍能查回同一行。
  const cmid = agentClientMsgId(runId, key);
  // 幂等：同 run 同 key 第二次直接查出站行返当前状态（不发送不审计）。
  // AUDIT_REJECTED / POLICY_DENIED 拒绝的不记 key（execSend 只在通过后才记）。
  const knownClientMsgId = usedKeys[key];
  if (typeof knownClientMsgId === 'string' && knownClientMsgId.length > 0) {
    return mapOutboundStatus(groupId, gatewayGroupId, knownClientMsgId, Date.now() - started, false);
  }
  // 重启恢复：history 显示本 run 曾尝试过该 key（结果没落库），先查出站行，不盲目重发。
  if (hasUnresultedSend(history, key)) {
    const row = await readOutboundByClientMsgId(cmid);
    if (row) {
      usedKeys[key] = cmid;
      return mapOutboundStatus(groupId, gatewayGroupId, cmid, Date.now() - started, false);
    }
  }
  // 审计：text 原文；只有合法 JSON 且 verdict 恰为 pass 才执行。
  const audit = await auditWithRetry(text, groupId);
  if (audit === 'fail') return errResult('AUDIT_REJECTED', '审计未通过（AUDIT_REJECTED），消息不执行', 'fail');
  if (audit === 'blocked') return { blocked: true };
  // 执行账号：online 的群成员。
  const accountId = await pickSendAccount(groupId);
  if (!accountId) return errResult('NO_AVAILABLE_ACCOUNT', '群内没有 online 的服务账号可用', 'pass');
  // run 内派生的 clientMsgId 作出站幂等键；确认预算 5 秒。
  try {
    const result = await createOutbound(groupId, accountId, text, { idempotencyKey: cmid });
    usedKeys[key] = result.clientMsgId;
    return mapOutboundStatus(groupId, gatewayGroupId, result.clientMsgId, Date.now() - started, true);
  } catch (e) {
    // createOutbound 抛错：终态/不可写按码映射；疑似 clientMsgId 冲突则读已存在行判定。
    const code = e instanceof GatewayError ? e.code : undefined;
    if (code === 'GROUP_WRITE_FORBIDDEN') {
      await markGroupUnreachable(groupId).catch(() => undefined);
      return errResult('GROUP_UNREACHABLE', '群不可写（GROUP_WRITE_FORBIDDEN）', 'pass');
    }
    if (code === 'ACCOUNT_SUSPENDED' || code === 'SESSION_EXPIRED' || code === 'ACCOUNT_TERMINAL') {
      return errResult('SEND_FAILED', '发送账号中途进入终态', 'pass');
    }
    const row = await readOutboundByClientMsgId(cmid).catch(() => null);
    if (row) {
      usedKeys[key] = cmid;
      return mapOutboundStatus(groupId, gatewayGroupId, cmid, Date.now() - started, true);
    }
    return errResult('SEND_FAILED', `消息发送失败（${code ?? 'OUTBOUND_ERROR'}）`, 'pass');
  }
}

async function execKick(
  groupId: string,
  gatewayGroupId: string,
  target: string,
  reason: string,
  usedKeys: Record<string, string>,
  history: unknown[],
): Promise<ToolExec | { blocked: true }> {
  // 群 autoKickEnabled=false 直接 POLICY_DENIED（不审计不执行，不占 key）。
  const group = await loadGroup(groupId);
  if (!group || !group.autoKickEnabled) return errResult('POLICY_DENIED', '群未开启 autoKick，kick 不执行', null);
  // 恢复：已 kick 不再执行。
  if (usedKeys[`kick:${target}`]) {
    return {
      ok: true,
      content: { kicked: true },
      summary: `kick_user ok: ${target}（恢复期跳过重复执行）`,
      auditVerdict: null,
    };
  }
  // 恢复：崩溃前 kick 的 tool_use 有记录但结果没落库——查成员列表收敛，
  // 目标已不在群内视为已踢，不再执行。
  if (hasUnresultedKick(history, target)) {
    try {
      const members = await gwMembers(gatewayGroupId);
      if (!members.includes(target)) {
        usedKeys[`kick:${target}`] = '1';
        return {
          ok: true,
          content: { kicked: true },
          summary: `kick_user ok: ${target} 已不在成员列表（恢复期收敛确认）`,
          auditVerdict: null,
        };
      }
    } catch {
      // 成员列表不可用则继续走正常 kick 流程。
    }
  }
  // 审计 text 为 JSON.stringify({ action:'kick', platform_user_id, reason })。
  const auditText = JSON.stringify({ action: 'kick', platform_user_id: target, reason });
  const audit = await auditWithRetry(auditText, groupId);
  if (audit === 'fail') return errResult('AUDIT_REJECTED', '审计未通过（AUDIT_REJECTED），kick 不执行', 'fail');
  if (audit === 'blocked') return { blocked: true };
  // 执行账号：online 且 role ∈ {creator, admin}。
  const byAccountId = await pickKickAccount(groupId);
  if (!byAccountId) return errResult('NO_AVAILABLE_ACCOUNT', '群内没有可执行 kick 的 online 管理员账号', 'pass');
  try {
    await gwKick(gatewayGroupId, byAccountId, target);
    usedKeys[`kick:${target}`] = '1';
    return { ok: true, content: { kicked: true }, summary: `kick_user ok: 已移除 ${target}`, auditVerdict: 'pass' };
  } catch (e) {
    const code = e instanceof GatewayError ? e.code : undefined;
    // 504/未知结果查 members 收敛判定（网关保证 2 秒内收敛）。
    if (code === 'NETWORK_TIMEOUT' || code === 'GATEWAY_UNREACHABLE' || code === 'RETRYABLE') {
      const converged = await waitKickConverged(gatewayGroupId, target, 2500);
      if (converged === true) {
        usedKeys[`kick:${target}`] = '1';
        return {
          ok: true,
          content: { kicked: true },
          summary: `kick_user ok: ${target} 已不在成员列表（504 收敛确认）`,
          auditVerdict: 'pass',
        };
      }
      if (converged === false) return errResult('SEND_FAILED', 'kick 结果未知且目标仍在群内', 'pass');
      return errResult('SEND_FAILED', 'kick 结果未知（成员列表不可用）', 'pass');
    }
    // OWNER_LEFT / NO_PERMISSION 透传；账号、群状态不变。
    if (code === 'OWNER_LEFT' || code === 'NO_PERMISSION') return errResult(code, `kick 失败（${code}）`, 'pass');
    if (code === 'ACCOUNT_SUSPENDED' || code === 'SESSION_EXPIRED') {
      await applyTerminal(byAccountId, code === 'ACCOUNT_SUSPENDED' ? 'suspended' : 'session_expired').catch(
        () => undefined,
      );
      return errResult('SEND_FAILED', '执行账号中途进入终态', 'pass');
    }
    return errResult('SEND_FAILED', `kick 失败（${code ?? 'KICK_ERROR'}）`, 'pass');
  }
}

// kick 504 后收敛判定：目标不在成员列表即视为已移除；到期仍在即未移除；列表不可用返回 null。
async function waitKickConverged(gatewayGroupId: string, target: string, timeoutMs: number): Promise<boolean | null> {
  const deadline = Date.now() + timeoutMs;
  let reachableOnce = false;
  for (;;) {
    try {
      const members = await gwMembers(gatewayGroupId);
      reachableOnce = true;
      if (!members.includes(target)) return true;
    } catch {
      // 成员列表抖动时继续等。
    }
    if (Date.now() >= deadline) return reachableOnce ? false : null;
    await sleep(300);
  }
}

// ---------- 主循环 ----------

export async function runLoop(runId: string): Promise<void> {
  if (activeLoops.has(runId)) return;
  activeLoops.add(runId);
  try {
    await driveRun(runId);
  } catch (e) {
    console.error(`[agent] run ${runId} 主循环异常:`, e instanceof Error ? e.message : e);
    try {
      // 异常退出也尽量落终态：沿用库里已存的 history/steps，不用空值覆盖。
      const cur = await loadRun(runId).catch(() => null);
      if (cur && cur.status === 'running') {
        await endRun(
          runId, 'failed', 'protocol_errors', `run 主循环内部异常：${e instanceof Error ? e.message : '未知错误'}`,
          cur.history, cur.steps, cur.consec, cur.usedKeys, cur.activeMs, cur.stepCount,
        );
      }
    } catch {
      // 结束落库也失败则只打日志（重启恢复会接管）。
    }
  } finally {
    activeLoops.delete(runId);
  }
}

// 开跑时原子消费 pending_triggers（行锁保证与入站追加不丢消息）。
async function consumeTriggers(runId: string): Promise<InboundTrigger[]> {
  const client = await getPool().connect();
  try {
    await client.query('BEGIN');
    const { rows } = await client.query<{ pending_triggers: unknown }>(
      `SELECT pending_triggers FROM agent_runs WHERE id = $1 AND status = 'running' FOR UPDATE`,
      [runId],
    );
    if (rows.length === 0) {
      await client.query('ROLLBACK');
      return [];
    }
    const pending = asArray(rows[0].pending_triggers) as InboundTrigger[];
    await client.query(`UPDATE agent_runs SET pending_triggers = '[]' WHERE id = $1`, [runId]);
    await client.query('COMMIT');
    return pending;
  } catch (e) {
    try {
      await client.query('ROLLBACK');
    } catch {
      // 回滚失败忽略。
    }
    throw e;
  } finally {
    client.release();
  }
}

// 首条触发上下文落库：不计步（一步 = 一次 /agent/turn 往返），只在 history 仍为空时写入，
// 并发 double-run 时只有一个成功，失败方重载对方已写的内容。
async function saveInitialContext(runId: string, history: unknown[]): Promise<unknown[] | null> {
  const r = await getPool().query(
    `UPDATE agent_runs SET history = $2::jsonb WHERE id = $1 AND status = 'running' AND history = '[]'::jsonb`,
    [runId, JSON.stringify(history)],
  );
  if ((r.rowCount ?? 0) > 0) return history;
  const cur = await loadRun(runId);
  if (!cur || cur.status !== 'running') return null;
  return cur.history;
}

// 每步（含协议错误步）落库：history/steps/used_keys/consec/active_ms/step_count。
// 用 step_count 做 CAS，返回 false 表示 run 已被结束或被别的循环推进，调用方直接停。
async function saveProgress(args: {
  runId: string;
  expectStepCount: number;
  history: unknown[];
  steps: StepRec[];
  consec: number;
  usedKeys: Record<string, string>;
  activeMs: number;
}): Promise<boolean> {
  const r = await getPool().query(
    `UPDATE agent_runs
     SET history = $2::jsonb, steps = $3::jsonb, consec_protocol_errors = $4,
         used_idempotency_keys = $5::jsonb, active_ms = $6, step_count = $7
     WHERE id = $1 AND status = 'running' AND step_count = $8`,
    [
      args.runId,
      JSON.stringify(args.history),
      JSON.stringify(args.steps),
      args.consec,
      JSON.stringify(args.usedKeys),
      args.activeMs,
      args.expectStepCount + 1,
      args.expectStepCount,
    ],
  );
  return (r.rowCount ?? 0) > 0;
}

function toolResultBlock(toolUseId: string, content: unknown, isError: boolean): Record<string, unknown> {
  const contentStr = typeof content === 'string' ? content : JSON.stringify(content);
  const capped =
    Buffer.byteLength(contentStr, 'utf8') > TOOL_CONTENT_LIMIT ? truncBytes(contentStr, TOOL_CONTENT_LIMIT) : contentStr;
  const blk: Record<string, unknown> = { type: 'tool_result', tool_use_id: toolUseId, content: capped };
  if (isError) blk.is_error = true;
  return blk;
}

async function driveRun(runId: string): Promise<void> {
  let run = await loadRun(runId);
  if (!run || run.status !== 'running') return;
  const groupId = run.groupId;
  // 触发消息：开跑时全量消费 pending（run 进行期间新到的留给下一次 run）。
  const initialPending = await consumeTriggers(runId);
  run = (await loadRun(runId)) ?? run;
  let history: unknown[] = [...run.history];
  const steps: StepRec[] = [...run.steps];
  let consec = run.consec;
  let activeMs = run.activeMs;
  let stepCount = run.stepCount;
  const usedKeys: Record<string, string> = { ...run.usedKeys };
  const seenIds = new Set<string>();
  for (const u of listAssistantToolUses(history)) seenIds.add(u.id);

  if (history.length === 0) {
    const group = await loadGroup(groupId);
    if (!group) {
      await endRun(runId, 'cancelled', 'cancelled', '群不存在，run 取消', history, steps, consec, usedKeys, activeMs, stepCount);
      return;
    }
    const ownIds = await loadOwnPlatformUserIds().catch(() => [] as string[]);
    const sorted = [...initialPending].sort((a, b) => (a.sentAt < b.sentAt ? -1 : a.sentAt > b.sentAt ? 1 : 0));
    history.push({
      role: 'user',
      content: [
        {
          type: 'text',
          text: JSON.stringify({
            groupId,
            triggerMessages: sorted,
            policy: { autoKickEnabled: group.autoKickEnabled },
            ownPlatformUserIds: ownIds,
          }),
        },
      ],
    });
    const saved = await saveInitialContext(runId, history);
    if (saved === null) return;
    history = [...saved];
  } else if (initialPending.length > 0) {
    // 恢复续跑：pending 已在 consumeTriggers 里取回但本轮 history 已有上下文，
    // 这些消息留给下一次 run（放回 pending，顺序不重要，下次按 sentAt 排序）。
    await getPool()
      .query(
        `UPDATE agent_runs SET pending_triggers = pending_triggers || $2::jsonb WHERE id = $1 AND status = 'running'`,
        [runId, JSON.stringify(initialPending)],
      )
      .catch(() => undefined);
  }

  for (;;) {
    run = await loadRun(runId);
    if (!run || run.status !== 'running') return;
    stepCount = run.stepCount;
    // 上限 12 步（含结束步）。
    if (stepCount >= MAX_STEPS) {
      await endRun(runId, 'failed', 'budget_exhausted', `步数达到上限（${MAX_STEPS} 步）`, history, steps, consec, usedKeys, activeMs, stepCount);
      return;
    }
    // 墙钟 60s（deadline_at；active_ms 跨重启累计，停机不计）。
    if (activeMs >= WALL_BUDGET_MS || (run.deadlineAt !== null && Date.now() >= Date.parse(run.deadlineAt))) {
      await endRun(runId, 'failed', 'wall_clock', 'run 超过 60 秒墙钟预算', history, steps, consec, usedKeys, activeMs, stepCount);
      return;
    }
    // 外部状态变化：群 unreachable 或 agentEnabled 关，当前步后 cancelled（步间即上一步后）。
    const live = await loadGroup(groupId).catch(() => null);
    if (!live || live.status !== 'active' || !live.agentEnabled) {
      await endRun(runId, 'cancelled', 'cancelled', '群不可写或 agent 已关闭，run 取消', history, steps, consec, usedKeys, activeMs, stepCount);
      return;
    }

    const stepStart = Date.now();
    const turn = await postTurn(runId, history);
    if (turn.timeout) {
      activeMs += Date.now() - stepStart;
      history.push({
        role: 'user',
        content: [{ type: 'text', text: `PROTOCOL_ERROR TURN_TIMEOUT: agent turn 调用 ${TURN_TIMEOUT_MS}ms 超时` }],
      });
      steps.push({
        kind: 'protocol_error', toolUseId: null, name: null, input: null,
        resultSummary: capChars(`PROTOCOL_ERROR TURN_TIMEOUT: agent turn 调用超时（${TURN_TIMEOUT_MS}ms）`, SUMMARY_LIMIT),
        isError: true, errorCode: 'TURN_TIMEOUT', auditVerdict: null, rawResponse: null,
      });
      consec += 1;
      const saved = await saveProgress({ runId, expectStepCount: stepCount, history, steps, consec, usedKeys, activeMs });
      if (!saved) return;
      stepCount += 1;
      if (consec >= 3) {
        await endRun(runId, 'failed', 'protocol_errors', '连续 3 次协议错误', history, steps, consec, usedKeys, activeMs, stepCount);
        return;
      }
      continue;
    }

    const parsed = validateTurn(turn.status, turn.body);
    const rawStored = turn.body === null ? null : truncBytes(turn.body, RAW_RESPONSE_LIMIT);
    if (parsed.kind === 'bad') {
      // BAD_JSON：不追加 assistant 块，追加 user text 块 `PROTOCOL_ERROR <code>: <一句话>`。
      activeMs += Date.now() - stepStart;
      history.push({ role: 'user', content: [{ type: 'text', text: `PROTOCOL_ERROR BAD_JSON: ${parsed.hint}` }] });
      steps.push({
        kind: 'protocol_error', toolUseId: null, name: null, input: null,
        resultSummary: capChars(`PROTOCOL_ERROR BAD_JSON: ${parsed.hint}`, SUMMARY_LIMIT),
        isError: true, errorCode: 'BAD_JSON', auditVerdict: null, rawResponse: rawStored,
      });
      consec += 1;
      const saved = await saveProgress({ runId, expectStepCount: stepCount, history, steps, consec, usedKeys, activeMs });
      if (!saved) return;
      stepCount += 1;
      if (consec >= 3) {
        await endRun(runId, 'failed', 'protocol_errors', '连续 3 次协议错误', history, steps, consec, usedKeys, activeMs, stepCount);
        return;
      }
      continue;
    }

    if (parsed.kind === 'end_turn') {
      activeMs += Date.now() - stepStart;
      history.push({ role: 'assistant', content: [{ type: 'text', text: parsed.text }] });
      steps.push({
        kind: 'final', toolUseId: null, name: null, input: null,
        resultSummary: capChars(`end_turn: ${parsed.text}`, SUMMARY_LIMIT),
        isError: false, errorCode: null, auditVerdict: null, rawResponse: rawStored,
      });
      consec = 0;
      const saved = await saveProgress({ runId, expectStepCount: stepCount, history, steps, consec, usedKeys, activeMs });
      if (!saved) return;
      stepCount += 1;
      // end_turn 同样结束：text 存为 summary，不发到群里。
      await endRun(runId, 'finished', 'final', parsed.text, history, steps, consec, usedKeys, activeMs, stepCount);
      return;
    }

    // tool_use：重复 id 先按协议错误处理。
    if (seenIds.has(parsed.id)) {
      activeMs += Date.now() - stepStart;
      history.push({
        role: 'user',
        content: [{ type: 'text', text: `PROTOCOL_ERROR DUPLICATE_TOOL_USE_ID: tool_use.id 重复（${parsed.id}）` }],
      });
      steps.push({
        kind: 'protocol_error', toolUseId: null, name: null, input: null,
        resultSummary: capChars(`PROTOCOL_ERROR DUPLICATE_TOOL_USE_ID: tool_use.id ${parsed.id} 重复`, SUMMARY_LIMIT),
        isError: true, errorCode: 'DUPLICATE_TOOL_USE_ID', auditVerdict: null, rawResponse: rawStored,
      });
      consec += 1;
      const saved = await saveProgress({ runId, expectStepCount: stepCount, history, steps, consec, usedKeys, activeMs });
      if (!saved) return;
      stepCount += 1;
      if (consec >= 3) {
        await endRun(runId, 'failed', 'protocol_errors', '连续 3 次协议错误', history, steps, consec, usedKeys, activeMs, stepCount);
        return;
      }
      continue;
    }
    seenIds.add(parsed.id);
    // 任何合法 tool_use 响应清零协议错误计数（含 UNKNOWN_TOOL / INVALID_INPUT）。
    consec = 0;
    history.push({ role: 'assistant', content: [{ type: 'tool_use', id: parsed.id, name: parsed.name, input: parsed.input }] });

    const inputProblem = validateToolInput(parsed.name, parsed.input);
    if (inputProblem !== null) {
      // 未知工具 / 入参不合 schema：正常追加 assistant 块（上已追加），再追加 is_error tool_result。
      activeMs += Date.now() - stepStart;
      const msg = inputProblem === 'UNKNOWN_TOOL' ? `未知工具: ${parsed.name}` : `工具入参不符合 input_schema（${parsed.name}）`;
      history.push({ role: 'user', content: [toolResultBlock(parsed.id, { code: inputProblem, message: msg }, true)] });
      steps.push({
        kind: 'tool_use', toolUseId: parsed.id, name: parsed.name, input: parsed.input,
        resultSummary: capChars(`${parsed.name} err ${inputProblem}: ${msg}`, SUMMARY_LIMIT),
        isError: true, errorCode: inputProblem, auditVerdict: null, rawResponse: rawStored,
      });
      const saved = await saveProgress({ runId, expectStepCount: stepCount, history, steps, consec, usedKeys, activeMs });
      if (!saved) return;
      stepCount += 1;
      continue;
    }

    const input = parsed.input as Record<string, unknown>;
    if (parsed.name === 'finish') {
      activeMs += Date.now() - stepStart;
      const summary = String(input.summary);
      history.push({ role: 'user', content: [toolResultBlock(parsed.id, { ok: true }, false)] });
      steps.push({
        kind: 'final', toolUseId: parsed.id, name: parsed.name, input: parsed.input,
        resultSummary: capChars(`finish: ${summary}`, SUMMARY_LIMIT),
        isError: false, errorCode: null, auditVerdict: null, rawResponse: rawStored,
      });
      const saved = await saveProgress({ runId, expectStepCount: stepCount, history, steps, consec, usedKeys, activeMs });
      if (!saved) return;
      stepCount += 1;
      await endRun(runId, 'finished', 'final', summary, history, steps, consec, usedKeys, activeMs, stepCount);
      return;
    }

    // 真正执行的工具：get_recent_messages / send_message / kick_user。
    let exec: ToolExec | { blocked: true };
    try {
      if (parsed.name === 'get_recent_messages') {
        exec = await execGetRecent(groupId, input.limit);
      } else if (parsed.name === 'send_message') {
        const group = await loadGroup(groupId);
        if (!group) {
          await endRun(runId, 'cancelled', 'cancelled', '群不存在，run 取消', history, steps, consec, usedKeys, activeMs, stepCount);
          return;
        }
        exec = await execSend(runId, groupId, group.gatewayGroupId, String(input.idempotency_key), String(input.text), usedKeys, history);
      } else {
        const group = await loadGroup(groupId);
        if (!group) {
          await endRun(runId, 'cancelled', 'cancelled', '群不存在，run 取消', history, steps, consec, usedKeys, activeMs, stepCount);
          return;
        }
        exec = await execKick(groupId, group.gatewayGroupId, String(input.platform_user_id), String(input.reason), usedKeys, history);
      }
    } catch (e) {
      exec = { ok: false, code: 'SEND_FAILED', message: `工具执行内部异常：${e instanceof Error ? e.message : '未知错误'}`, auditVerdict: null };
    }
    activeMs += Date.now() - stepStart;

    // 审计 3 次无果：run blocked，endReason=audit_blocked，该工具不执行，推事件通知。
    if (!('ok' in exec)) {
      const saved = await saveProgress({ runId, expectStepCount: stepCount, history, steps, consec, usedKeys, activeMs });
      if (!saved) return;
      stepCount += 1;
      await endRun(runId, 'blocked', 'audit_blocked', '审计 3 次均拿不到明确结论', history, steps, consec, usedKeys, activeMs, stepCount);
      return;
    }

    if (exec.ok) {
      history.push({ role: 'user', content: [toolResultBlock(parsed.id, exec.content, false)] });
      steps.push({
        kind: 'tool_use', toolUseId: parsed.id, name: parsed.name, input: parsed.input,
        resultSummary: capChars(exec.summary, SUMMARY_LIMIT),
        isError: false, errorCode: null, auditVerdict: exec.auditVerdict, rawResponse: rawStored,
      });
    } else {
      history.push({ role: 'user', content: [toolResultBlock(parsed.id, { code: exec.code, message: exec.message }, true)] });
      steps.push({
        kind: 'tool_use', toolUseId: parsed.id, name: parsed.name, input: parsed.input,
        resultSummary: capChars(`${parsed.name} err ${exec.code}: ${exec.message}`, SUMMARY_LIMIT),
        isError: true, errorCode: exec.code, auditVerdict: exec.auditVerdict, rawResponse: rawStored,
      });
    }
    const saved = await saveProgress({ runId, expectStepCount: stepCount, history, steps, consec, usedKeys, activeMs });
    if (!saved) return;
    stepCount += 1;
  }
}

// run 结束：写 status/endReason/summary，推 WS 事件；
// 结束时有 pending（finished/failed/blocked，非 cancelled）立即建下一次 run 全量 triggerMessages。
async function endRun(
  runId: string,
  status: 'finished' | 'failed' | 'blocked' | 'cancelled',
  endReason: string,
  summary: string,
  history: unknown[] = [],
  steps: StepRec[] = [],
  consec = 0,
  usedKeys: Record<string, string> = {},
  activeMs = 0,
  stepCount = 0,
): Promise<void> {
  const pool = getPool();
  const ended = await pool.query<{ group_id: string; pending_triggers: unknown }>(
    `UPDATE agent_runs
     SET status = $2, end_reason = $3, summary = $4, history = $5::jsonb, steps = $6::jsonb,
         consec_protocol_errors = $7, used_idempotency_keys = $8::jsonb, active_ms = $9, step_count = $10
     WHERE id = $1 AND status = 'running'
     RETURNING group_id, pending_triggers`,
    [runId, status, endReason, summary, JSON.stringify(history), JSON.stringify(steps), consec, JSON.stringify(usedKeys), activeMs, stepCount],
  );
  if ((ended.rowCount ?? 0) === 0) return; // 已被结束（别的循环/恢复），不再重复通知。
  const groupId = ended.rows[0].group_id;
  const pending = asArray(ended.rows[0].pending_triggers) as InboundTrigger[];
  await emitAgentRun(runId, groupId, status, endReason);
  // cancelled 不链式建 run；finished/failed/blocked 有 pending 则立即建下一次。
  if ((status === 'finished' || status === 'failed' || status === 'blocked') && pending.length > 0) {
    const nextId = uuidv4();
    try {
      await pool.query(
        `INSERT INTO agent_runs(id, group_id, status, history, steps, pending_triggers,
                                consec_protocol_errors, used_idempotency_keys, active_ms, deadline_at, step_count)
         VALUES ($1, $2, 'running', '[]', '[]', $3::jsonb, 0, '{}', 0, now() + interval '60 seconds', 0)`,
        [nextId, groupId, JSON.stringify(pending)],
      );
      await emitAgentRun(nextId, groupId, 'running', null);
      void runLoop(nextId);
    } catch (e) {
      if (!isUniqueViolation(e)) {
        console.error(`[agent] 链式建 run 失败 group=${groupId}:`, e instanceof Error ? e.message : e);
        return;
      }
      // 并发已建出新的 running run：把 pending 并过去，不丢消息。
      await pool
        .query(
          `UPDATE agent_runs SET pending_triggers = pending_triggers || $2::jsonb
           WHERE group_id = $1 AND status = 'running'`,
          [groupId, JSON.stringify(pending)],
        )
        .catch(() => undefined);
    }
  }
}

// ---------- 恢复 ----------

// 服务在 run 进行中任意时刻重启，run 都从中断处继续（同一个 runId）：
// deadline=now+(60s-active_ms)，停机时间不计；已 sent 的 send 不重发（used keys/出站行），已 kick 不再执行。
export async function resumeAgentRuns(): Promise<void> {
  const pool = getPool();
  const { rows } = await pool.query<{ id: string; active_ms: number }>(
    `SELECT id, active_ms FROM agent_runs WHERE status = 'running'`,
  );
  for (const r of rows) {
    const runId = r.id;
    try {
      const active = Number(r.active_ms ?? 0);
      if (active >= WALL_BUDGET_MS) {
        const cur = await loadRun(runId);
        if (cur && cur.status === 'running') {
          await endRun(runId, 'failed', 'wall_clock', '重启时已耗尽 60 秒预算', cur.history, cur.steps, cur.consec, cur.usedKeys, cur.activeMs, cur.stepCount);
        }
        continue;
      }
      // 重启 deadline=now+(60s-active_ms)，停机时间不计。
      await pool.query(
        `UPDATE agent_runs SET deadline_at = now() + (GREATEST(0, 60000 - active_ms) || ' milliseconds')::interval
         WHERE id = $1 AND status = 'running'`,
        [runId],
      );
      void runLoop(runId);
    } catch (e) {
      console.error(`[agent] 恢复 run ${runId} 失败:`, e instanceof Error ? e.message : e);
    }
  }
}

// ---------- 路由 ----------

export const agentRouter: Router = Router();

function toPublicStep(s: StepRec): Record<string, unknown> {
  return {
    kind: s.kind,
    toolUseId: s.toolUseId ?? null,
    name: s.name ?? null,
    input: s.input ?? null,
    resultSummary: s.resultSummary ?? null,
    isError: s.isError === true,
    errorCode: s.errorCode ?? null,
    auditVerdict: s.auditVerdict ?? null,
    rawResponse: s.rawResponse ?? null,
  };
}

// GET /api/agent-runs/:id 全文（status≠running 时 endReason 有值）。
agentRouter.get(
  '/agent-runs/:id',
  requireAuth,
  asyncHandler(async (req: Request, res: Response) => {
    const raw = req.params.id as unknown;
    const id = Array.isArray(raw) ? raw[0] : raw;
    if (typeof id !== 'string' || id.length === 0) {
      sendError(req, res, 404, 'NOT_FOUND', 'run 不存在');
      return;
    }
    const { rows } = await getPool().query<{
      id: string;
      group_id: string;
      status: string;
      end_reason: string | null;
      summary: string | null;
      steps: unknown;
    }>(`SELECT id, group_id, status, end_reason, summary, steps FROM agent_runs WHERE id = $1`, [id]);
    if (rows.length === 0) {
      sendError(req, res, 404, 'NOT_FOUND', 'run 不存在');
      return;
    }
    const r = rows[0];
    res.json({
      id: r.id,
      groupId: r.group_id,
      status: r.status,
      endReason: r.end_reason ?? null,
      summary: r.summary ?? null,
      steps: (asArray(r.steps) as StepRec[]).map(toPublicStep),
    });
  }),
);

// GET /api/groups/:id/agent-runs 最近 20（可不含 steps，这里省略 steps）。
agentRouter.get(
  '/groups/:id/agent-runs',
  requireAuth,
  asyncHandler(async (req: Request, res: Response) => {
    const raw = req.params.id as unknown;
    const groupId = Array.isArray(raw) ? raw[0] : raw;
    if (typeof groupId !== 'string' || groupId.length === 0) {
      sendError(req, res, 404, 'NOT_FOUND', '群不存在');
      return;
    }
    const g = await getPool().query(`SELECT 1 FROM groups WHERE id = $1`, [groupId]);
    if ((g.rowCount ?? 0) === 0) {
      sendError(req, res, 404, 'NOT_FOUND', '群不存在');
      return;
    }
    const { rows } = await getPool().query<{
      id: string;
      group_id: string;
      status: string;
      end_reason: string | null;
      summary: string | null;
    }>(
      `SELECT id, group_id, status, end_reason, summary FROM agent_runs
       WHERE group_id = $1 ORDER BY run_started_at DESC LIMIT 20`,
      [groupId],
    );
    res.json(
      rows.map((r) => ({
        id: r.id,
        groupId: r.group_id,
        status: r.status,
        endReason: r.end_reason ?? null,
        summary: r.summary ?? null,
      })),
    );
  }),
);
