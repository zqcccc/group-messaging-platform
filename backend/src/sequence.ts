// 定时序列：序列存储、启动预检（vars/stepVars 取值与占位符校验）、排期执行与重启恢复。
// 出站发送走本文件 deliverMessage（先落 outbound 行再调网关，含 504 确认与一次重发仲裁）。
import { Request, Response, Router } from 'express';
import { v4 as uuidv4 } from 'uuid';
import { applyAccountTransition } from './accounts';
import { requireAdmin, requireAuth } from './auth';
import { getPool } from './db';
import { GatewayError, gwByClientId, gwSend } from './gateway';
import { asyncHandler, sendError } from './util';
// 状态与事件常量（字面量统一收口，避免散落）。
const ACCOUNT_ONLINE = 'online';
const ACCOUNT_RATE_LIMITED = 'rate_limited';
const ACCOUNT_SUSPENDED = 'suspended';
const ACCOUNT_SESSION_EXPIRED = 'session_expired';
const GROUP_UNREACHABLE = 'unreachable';
const EVT_ACCOUNT_STATUS = 'account_status_changed';
const EVT_GROUP_UPDATED = 'group_updated';
const EVT_MESSAGE = 'message';
const EVT_SEQUENCE_RUN = 'sequence_run';

// ---------- 小工具 ----------

function sleep(ms: number): Promise<void> {
  return new Promise<void>((resolve) => setTimeout(resolve, ms));
}

function isoNow(): string {
  return new Date().toISOString();
}

// ws_events 先入库（WS 服务端按 seq 推送，保证单调）。
async function emitWs(type: string, payload: Record<string, unknown>): Promise<void> {
  try {
    await getPool().query('INSERT INTO ws_events(type, payload) VALUES ($1, $2)', [type, JSON.stringify(payload)]);
  } catch {
    // 事件落库失败不影响主流程。
  }
}

// 自发消息及其投递状态变化即时推前端（时间线按 clientMsgId 合行）。
async function emitOwnMessage(groupId: string, clientMsgId: string): Promise<void> {
  await emitWs(EVT_MESSAGE, { groupId, msgId: clientMsgId, isOwn: true });
}

function routeParam(req: Request, name: string): string | null {
  const raw = (req.params as Record<string, unknown>)[name];
  const v = Array.isArray(raw) ? raw[0] : raw;
  return typeof v === 'string' && v.length > 0 ? v : null;
}

function isUniqueViolation(e: unknown): boolean {
  return typeof e === 'object' && e !== null && (e as { code?: unknown }).code === '23505';
}

// ---------- 占位符与取值 ----------

// 文本里的 {key}，key 匹配 [A-Za-z0-9_]+。
function extractKeys(text: string): string[] {
  const out: string[] = [];
  const re = /\{([A-Za-z0-9_]+)\}/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(text)) !== null) out.push(m[1]);
  return out;
}

type StrMap = Record<string, string>;

// 生效取值：base = vars（空串视为未提供）；某步在 stepVars 里给了非空值，
// 从这一步起后续步骤都用新值，直到更晚的步骤再次给值；stepVars 空串表示不改。
// sources 标每个 key 的来源：default（vars）或 step:<最初给出它的步骤 index>。
function effectiveVars(
  vars: StrMap,
  stepVars: Record<string, StrMap>,
  upto: number,
): { values: StrMap; sources: Record<string, string> } {
  const values: StrMap = {};
  const sources: Record<string, string> = {};
  for (const [k, v] of Object.entries(vars)) {
    if (typeof v !== 'string' || v === '') continue;
    values[k] = v;
    sources[k] = 'default';
  }
  const firstStep: Record<string, number> = {};
  for (let j = 1; j <= upto; j++) {
    const ov = stepVars[String(j)];
    if (!ov || typeof ov !== 'object') continue;
    for (const [k, v] of Object.entries(ov)) {
      if (typeof v !== 'string' || v === '') continue;
      values[k] = v;
      if (!(k in firstStep)) firstStep[k] = j;
      sources[k] = `step:${firstStep[k]}`;
    }
  }
  return { values, sources };
}

function renderText(text: string, values: StrMap): string {
  return text.replace(/\{([A-Za-z0-9_]+)\}/g, (m, k: string) => (k in values ? values[k] : m));
}

// ---------- 账号/群状态副作用 ----------

// 429 限流：账号进 rate_limited，到期自动回 online（已不是 rate_limited 则不转移）。
export async function applyRateLimited(accountId: string, retryAfterSeconds: number): Promise<void> {
  const pool = getPool();
  const cur = await pool.query('SELECT status FROM accounts WHERE id = $1', [accountId]);
  if (cur.rows.length === 0) return;
  const from = String(cur.rows[0].status);
  if (from === ACCOUNT_SUSPENDED || from === ACCOUNT_SESSION_EXPIRED) return;
  const secs = Number.isFinite(retryAfterSeconds) && retryAfterSeconds > 0 ? Math.ceil(retryAfterSeconds) : 60;
  const upd = await pool.query(
    `UPDATE accounts SET status = 'rate_limited', rate_limited_until = now() + ($2 || ' seconds')::interval
     WHERE id = $1 AND status NOT IN ('suspended', 'session_expired') RETURNING rate_limited_until`,
    [accountId, String(secs)],
  );
  const untilRaw = upd.rows[0]?.rate_limited_until as Date | string | null | undefined;
  const rateLimitedUntil =
    untilRaw instanceof Date ? untilRaw.toISOString() : untilRaw != null ? String(untilRaw) : null;
  await emitWs(EVT_ACCOUNT_STATUS, { accountId, from, to: ACCOUNT_RATE_LIMITED, rateLimitedUntil });
  // 到期恢复：只在仍是 rate_limited 且窗口已过时转回 online。
  setTimeout(() => {
    void (async () => {
      try {
        const r = await getPool().query(
          `UPDATE accounts SET status = 'online', rate_limited_until = NULL
           WHERE id = $1 AND status = 'rate_limited' AND rate_limited_until <= now()`,
          [accountId],
        );
        if ((r.rowCount ?? 0) > 0) {
          await emitWs(EVT_ACCOUNT_STATUS, {
            accountId,
            from: ACCOUNT_RATE_LIMITED,
            to: ACCOUNT_ONLINE,
            rateLimitedUntil: null,
          });
          // 到期后按原顺序发出该账号排队中的消息（动态加载 send 模块，避免循环依赖）。
          try {
            // eslint-disable-next-line @typescript-eslint/no-require-imports
            const send = require('./send.js') as {
              flushQueuedForAccount?: (accountId: string) => Promise<void>;
            };
            if (typeof send.flushQueuedForAccount === 'function') await send.flushQueuedForAccount(accountId);
          } catch {
            // flush 失败不抛，靠下次发送/重启恢复继续推进。
          }
        }
      } catch {
        // 恢复失败不抛，靠下次发送/轮询继续推进。
      }
    })();
  }, secs * 1000 + 500);
}

// 终态：复用 accounts.ts 的 CAS 转移（含清成员、取消排队发送、account_terminal 事件，同事务）。
// 重复进入同一终态静默忽略。
export async function applyTerminal(accountId: string, to: 'suspended' | 'session_expired'): Promise<void> {
  const pool = getPool();
  const cur = await pool.query('SELECT status FROM accounts WHERE id = $1', [accountId]);
  if (cur.rows.length === 0) return;
  const from = String(cur.rows[0].status);
  if (from === to || from === ACCOUNT_SUSPENDED || from === ACCOUNT_SESSION_EXPIRED) return;
  await applyAccountTransition(pool, accountId, to, from);
}

// 群不可写：群置 unreachable（账号状态不变）。
export async function markGroupUnreachable(groupId: string): Promise<void> {
  const r = await getPool().query(`UPDATE groups SET status = 'unreachable' WHERE id = $1 AND status = 'active'`, [groupId]);
  if ((r.rowCount ?? 0) > 0) {
    await emitWs(EVT_GROUP_UPDATED, { groupId, status: GROUP_UNREACHABLE });
  }
}

// ---------- 出站发送（含确认与重发仲裁，供序列与 agent 共用） ----------

export interface DeliveryOutcome {
  kind: 'sent' | 'rate_limited' | 'terminal' | 'group_forbidden' | 'failed' | 'retry_later' | 'timeout';
  clientMsgId: string;
  msgId?: string;
  sentAt?: string;
  code?: string;
  retryAfter?: number;
  terminal?: string;
}

// 落地确认：by-client-id 轮询为主（message_sent 落地即有），outbound 行 sent 为辅（sse/流水线已确认）。
// 返回网关的 msgId/sentAt（"发出"时刻以此为准）。
export async function pollLanding(
  gatewayGroupId: string,
  clientMsgId: string,
  timeoutMs: number,
): Promise<{ msgId: string; sentAt: string } | null> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    try {
      const hit = await gwByClientId(gatewayGroupId, clientMsgId);
      if (hit) return hit;
    } catch {
      // 网关抖动时继续轮询。
    }
    try {
      const r = await getPool().query(
        `SELECT delivery_status, gateway_msg_id, gateway_sent_at FROM outbound_messages WHERE client_msg_id = $1`,
        [clientMsgId],
      );
      const row = r.rows[0] as
        | { delivery_status: string; gateway_msg_id: string | null; gateway_sent_at: Date | string | null }
        | undefined;
      if (row && row.delivery_status === 'sent' && row.gateway_msg_id) {
        const sentAt =
          row.gateway_sent_at instanceof Date ? row.gateway_sent_at.toISOString() : String(row.gateway_sent_at ?? isoNow());
        return { msgId: row.gateway_msg_id, sentAt };
      }
    } catch {
      // 忽略，继续轮询。
    }
    if (Date.now() >= deadline) return null;
    await sleep(200);
  }
}

async function markRowSent(clientMsgId: string, msgId: string, sentAt: string): Promise<void> {
  await getPool().query(
    `UPDATE outbound_messages SET delivery_status = 'sent', gateway_msg_id = $2, gateway_sent_at = $3
     WHERE client_msg_id = $1`,
    [clientMsgId, msgId, sentAt],
  );
}

async function markRowFailed(clientMsgId: string, code: string): Promise<void> {
  await getPool().query(
    `UPDATE outbound_messages SET delivery_status = 'failed', fail_code = $2 WHERE client_msg_id = $1`,
    [clientMsgId, code],
  );
}

// 同一 clientMsgId 最多重发一次：retry_count 原子占用，抢到才发。
async function claimResend(clientMsgId: string): Promise<boolean> {
  const r = await getPool().query(
    `UPDATE outbound_messages SET retry_count = retry_count + 1 WHERE client_msg_id = $1 AND retry_count < 1`,
    [clientMsgId],
  );
  return (r.rowCount ?? 0) > 0;
}

// 发送一条文本：先落 outbound 行（queued，网关发出而库里无记录不可能），再调网关。
// 202 → accepted，确认落地后 sent；504 → unknown，2 秒窗口确认，未发出且可重发时同 clientMsgId 重发一次。
export async function deliverMessage(args: {
  groupId: string;
  gatewayGroupId: string;
  accountId: string;
  text: string;
  clientMsgId?: string;
  confirmMs?: number;
}): Promise<DeliveryOutcome> {
  const pool = getPool();
  const confirmMs = args.confirmMs ?? 12000;
  const clientMsgId = args.clientMsgId ?? uuidv4();
  if (!args.clientMsgId) {
    await pool.query(
      `INSERT INTO outbound_messages(id, group_id, account_id, client_msg_id, text, delivery_status)
       VALUES ($1, $2, $3, $4, $5, 'queued')`,
      [uuidv4(), args.groupId, args.accountId, clientMsgId, args.text],
    );
  }
  // 入队即推（新插入与调用方预插入都覆盖），前端即时见到自己发出的消息。
  await emitWs(EVT_MESSAGE, { groupId: args.groupId, msgId: clientMsgId, isOwn: true });
  const accepted = async (): Promise<void> => {
    await pool.query(
      `UPDATE outbound_messages SET delivery_status = 'accepted', accepted_at = now() WHERE client_msg_id = $1`,
      [clientMsgId],
    );
    await emitOwnMessage(args.groupId, clientMsgId);
  };
  try {
    await gwSend(args.gatewayGroupId, args.accountId, clientMsgId, args.text);
    await accepted();
  } catch (e) {
    const code = e instanceof GatewayError ? e.code : 'GATEWAY_UNREACHABLE';
    const retryAfter = e instanceof GatewayError ? e.retryAfterSeconds : undefined;
    if (code === 'RATE_LIMITED') {
      // 限流：行保持 queued，到期后流水线按顺序发出；调用方顺延。
      await applyRateLimited(args.accountId, retryAfter ?? 60);
      return { kind: 'rate_limited', clientMsgId, retryAfter: retryAfter ?? 60 };
    }
    if (code === 'NETWORK_TIMEOUT') {
      await pool.query(`UPDATE outbound_messages SET delivery_status = 'unknown' WHERE client_msg_id = $1`, [
        clientMsgId,
      ]);
      await emitOwnMessage(args.groupId, clientMsgId);
      // 2 秒窗口确认（网关保证 504 后 2 秒内收敛）。
      const landed = await pollLanding(args.gatewayGroupId, clientMsgId, 2500);
      if (landed) {
        await markRowSent(clientMsgId, landed.msgId, landed.sentAt);
        await emitOwnMessage(args.groupId, clientMsgId);
        return { kind: 'sent', clientMsgId, msgId: landed.msgId, sentAt: landed.sentAt };
      }
      // 确认未发出且可重发：同 clientMsgId 重发一次。
      if (await claimResend(clientMsgId)) {
        return await deliverResend(args.groupId, args.gatewayGroupId, args.accountId, args.text, clientMsgId, confirmMs);
      }
      await markRowFailed(clientMsgId, 'NETWORK_TIMEOUT');
      await emitOwnMessage(args.groupId, clientMsgId);
      return { kind: 'failed', clientMsgId, code: 'NETWORK_TIMEOUT' };
    }
    if (code === 'ACCOUNT_SUSPENDED' || code === 'SESSION_EXPIRED') {
      const terminal = code === 'ACCOUNT_SUSPENDED' ? 'suspended' : 'session_expired';
      await applyTerminal(args.accountId, terminal);
      await pool.query(
        `UPDATE outbound_messages SET delivery_status = 'cancelled', fail_code = 'ACCOUNT_TERMINAL'
         WHERE client_msg_id = $1`,
        [clientMsgId],
      );
      await emitOwnMessage(args.groupId, clientMsgId);
      return { kind: 'terminal', clientMsgId, terminal };
    }
    if (code === 'GROUP_WRITE_FORBIDDEN') {
      await markRowFailed(clientMsgId, code);
      await emitOwnMessage(args.groupId, clientMsgId);
      await markGroupUnreachable(args.groupId);
      return { kind: 'group_forbidden', clientMsgId, code };
    }
    if (code === 'SENDER_NOT_IN_GROUP' || code === 'ACCOUNT_OFFLINE') {
      await markRowFailed(clientMsgId, code);
      await emitOwnMessage(args.groupId, clientMsgId);
      return { kind: 'failed', clientMsgId, code };
    }
    // 503 RETRYABLE / 不可达等：行保持 queued，调用方稍后重试。
    return { kind: 'retry_later', clientMsgId, code };
  }
  // 202 受理后确认落地（序列无 5 秒限制，可等久一点）。
  const landed = await pollLanding(args.gatewayGroupId, clientMsgId, confirmMs);
  if (landed) {
    await markRowSent(clientMsgId, landed.msgId, landed.sentAt);
    await emitOwnMessage(args.groupId, clientMsgId);
    return { kind: 'sent', clientMsgId, msgId: landed.msgId, sentAt: landed.sentAt };
  }
  return { kind: 'timeout', clientMsgId };
}

// 同 clientMsgId 的重发（retry_count 已原子占用，这里只发一次，不再递归重发）。
async function deliverResend(
  groupId: string,
  gatewayGroupId: string,
  accountId: string,
  text: string,
  clientMsgId: string,
  confirmMs: number,
): Promise<DeliveryOutcome> {
  const pool = getPool();
  try {
    await gwSend(gatewayGroupId, accountId, clientMsgId, text);
    await pool.query(
      `UPDATE outbound_messages SET delivery_status = 'accepted', accepted_at = now() WHERE client_msg_id = $1`,
      [clientMsgId],
    );
    await emitOwnMessage(groupId, clientMsgId);
  } catch (e) {
    const code = e instanceof GatewayError ? e.code : 'GATEWAY_UNREACHABLE';
    if (code === 'RATE_LIMITED') {
      const retryAfter = (e as GatewayError).retryAfterSeconds ?? 60;
      await applyRateLimited(accountId, retryAfter);
      return { kind: 'rate_limited', clientMsgId, retryAfter };
    }
    if (code === 'ACCOUNT_SUSPENDED' || code === 'SESSION_EXPIRED') {
      const terminal = code === 'ACCOUNT_SUSPENDED' ? 'suspended' : 'session_expired';
      await applyTerminal(accountId, terminal);
      await pool.query(
        `UPDATE outbound_messages SET delivery_status = 'cancelled', fail_code = 'ACCOUNT_TERMINAL'
         WHERE client_msg_id = $1`,
        [clientMsgId],
      );
      await emitOwnMessage(groupId, clientMsgId);
      return { kind: 'terminal', clientMsgId, terminal };
    }
    if (code === 'GROUP_WRITE_FORBIDDEN') {
      await markRowFailed(clientMsgId, code);
      await emitOwnMessage(groupId, clientMsgId);
      await markGroupUnreachable(groupId);
      return { kind: 'group_forbidden', clientMsgId, code };
    }
    if (code === 'SENDER_NOT_IN_GROUP' || code === 'ACCOUNT_OFFLINE') {
      await markRowFailed(clientMsgId, code);
      await emitOwnMessage(groupId, clientMsgId);
      return { kind: 'failed', clientMsgId, code };
    }
    if (code === 'NETWORK_TIMEOUT') {
      await markRowFailed(clientMsgId, 'NETWORK_TIMEOUT');
      await emitOwnMessage(groupId, clientMsgId);
      return { kind: 'failed', clientMsgId, code: 'NETWORK_TIMEOUT' };
    }
    return { kind: 'retry_later', clientMsgId, code };
  }
  const landed = await pollLanding(gatewayGroupId, clientMsgId, confirmMs);
  if (landed) {
    await markRowSent(clientMsgId, landed.msgId, landed.sentAt);
    await emitOwnMessage(groupId, clientMsgId);
    return { kind: 'sent', clientMsgId, msgId: landed.msgId, sentAt: landed.sentAt };
  }
  return { kind: 'timeout', clientMsgId };
}

// ---------- 序列步骤状态 ----------

interface SeqStepDef {
  index: number;
  accountRole: string;
  text: string;
  delaySeconds: number;
}

interface SeqStepState extends SeqStepDef {
  resolvedText: string;
  resolvedVars: StrMap;
  varSources: Record<string, string>;
  status: 'pending' | 'accepted' | 'sent' | 'skipped' | 'failed';
  scheduledAt: string | null;
  sentAt: string | null;
  clientMsgId: string | null;
  accountId: string | null;
  failCode: string | null;
  attempts: number;
}

interface SeqRunRow {
  id: string;
  group_id: string;
  sequence_id: string;
  status: string;
  current_step_index: number;
  steps: SeqStepState[];
}

// ---------- 排期定时器 ----------

const seqTimers = new Map<string, NodeJS.Timeout>();

function clearSeqTimer(runId: string): void {
  const t = seqTimers.get(runId);
  if (t) {
    clearTimeout(t);
    seqTimers.delete(runId);
  }
}

function scheduleSeqStep(runId: string, stepIndex: number, atMs: number): void {
  clearSeqTimer(runId);
  const delay = Math.max(0, atMs - Date.now());
  const t = setTimeout(() => {
    seqTimers.delete(runId);
    void fireSequenceStep(runId, stepIndex);
  }, delay);
  seqTimers.set(runId, t);
}

// ---------- DB 存取 ----------

async function loadSeqRun(runId: string): Promise<SeqRunRow | null> {
  const r = await getPool().query(
    `SELECT id, group_id, sequence_id, status, current_step_index, steps FROM sequence_runs WHERE id = $1`,
    [runId],
  );
  if (r.rows.length === 0) return null;
  const row = r.rows[0] as {
    id: string;
    group_id: string;
    sequence_id: string;
    status: string;
    current_step_index: number;
    steps: SeqStepState[];
  };
  return {
    id: row.id,
    group_id: row.group_id,
    sequence_id: row.sequence_id,
    status: row.status,
    current_step_index: Number(row.current_step_index ?? 0),
    steps: Array.isArray(row.steps) ? row.steps : [],
  };
}

async function saveSeqRun(runId: string, steps: SeqStepState[], extra?: { status?: string; currentStepIndex?: number }): Promise<void> {
  const sets: string[] = ['steps = $2'];
  const values: unknown[] = [runId, JSON.stringify(steps)];
  if (extra?.status !== undefined) {
    sets.push(`status = $${values.length + 1}`);
    values.push(extra.status);
  }
  if (extra?.currentStepIndex !== undefined) {
    sets.push(`current_step_index = $${values.length + 1}`);
    values.push(extra.currentStepIndex);
  }
  await getPool().query(`UPDATE sequence_runs SET ${sets.join(', ')} WHERE id = $1`, values);
}

async function emitSeqRun(runId: string, groupId: string, status: string, currentStepIndex: number): Promise<void> {
  await emitWs(EVT_SEQUENCE_RUN, { runId, groupId, status, currentStepIndex });
}

// ---------- 账号选择 ----------

type PickResult = { kind: 'ok'; accountId: string } | { kind: 'wait'; untilMs: number } | { kind: 'none' };

// admin：群里 role ∈ {creator, admin} 且 online 的账号（优先 admin）；
// member：role = member 且 online，按 accountId 字典序首个。
// 没有匹配时该步 skipped；rate_limited 的账号不算没有，该步顺延到限流结束后。
async function pickAccount(groupId: string, accountRole: string): Promise<PickResult> {
  const r = await getPool().query(
    `SELECT m.account_id, m.role, a.status, a.rate_limited_until
     FROM group_members m JOIN accounts a ON a.id = m.account_id
     WHERE m.group_id = $1`,
    [groupId],
  );
  const rows = r.rows as Array<{
    account_id: string;
    role: string;
    status: string;
    rate_limited_until: Date | string | null;
  }>;
  const matchRole = (role: string): boolean =>
    accountRole === 'admin' ? role === 'creator' || role === 'admin' : role === 'member';
  const cands = rows.filter((x) => matchRole(String(x.role)));
  const online = cands.filter((x) => x.status === 'online');
  if (online.length > 0) {
    online.sort((a, b) => {
      if (accountRole === 'admin') {
        const ra = a.role === 'admin' ? 0 : 1;
        const rb = b.role === 'admin' ? 0 : 1;
        if (ra !== rb) return ra - rb;
      }
      return a.account_id < b.account_id ? -1 : a.account_id > b.account_id ? 1 : 0;
    });
    return { kind: 'ok', accountId: online[0].account_id };
  }
  // 无 online：限流窗口已过的视为可用（网关为准，发一次便知）。
  const now = Date.now();
  const limited = cands.filter((x) => x.status === 'rate_limited');
  const expired = limited.filter((x) => {
    if (!x.rate_limited_until) return true;
    const t = x.rate_limited_until instanceof Date ? x.rate_limited_until.getTime() : Date.parse(String(x.rate_limited_until));
    return !Number.isFinite(t) || t <= now + 1000;
  });
  if (expired.length > 0) {
    expired.sort((a, b) => (a.account_id < b.account_id ? -1 : a.account_id > b.account_id ? 1 : 0));
    return { kind: 'ok', accountId: expired[0].account_id };
  }
  if (limited.length > 0) {
    let earliest = Number.POSITIVE_INFINITY;
    for (const x of limited) {
      if (!x.rate_limited_until) {
        earliest = now;
        break;
      }
      const t = x.rate_limited_until instanceof Date ? x.rate_limited_until.getTime() : Date.parse(String(x.rate_limited_until));
      if (Number.isFinite(t) && t < earliest) earliest = t;
    }
    if (earliest !== Number.POSITIVE_INFINITY) return { kind: 'wait', untilMs: earliest + 800 };
  }
  return { kind: 'none' };
}

// ---------- 步骤执行 ----------

// 崩溃前已开始发送（clientMsgId 已落步骤）但未确认的步骤：先确认，不盲目重发。
// 同 clientMsgId 的重发走 deliverMessage 的 retry_count 仲裁（最多一次）。
async function confirmExistingStep(
  groupId: string,
  gatewayGroupId: string,
  accountId: string,
  text: string,
  clientMsgId: string,
): Promise<DeliveryOutcome> {
  const landed = await pollLanding(gatewayGroupId, clientMsgId, 3000);
  if (landed) {
    await markRowSent(clientMsgId, landed.msgId, landed.sentAt);
    return { kind: 'sent', clientMsgId, msgId: landed.msgId, sentAt: landed.sentAt };
  }
  if (await claimResend(clientMsgId)) {
    return await deliverResend(groupId, gatewayGroupId, accountId, text, clientMsgId, 12000);
  }
  const r = await getPool().query(`SELECT delivery_status FROM outbound_messages WHERE client_msg_id = $1`, [clientMsgId]);
  const st = r.rows[0] ? String(r.rows[0].delivery_status) : '';
  if (st === 'failed' || st === 'cancelled') {
    return { kind: 'failed', clientMsgId, code: 'PREVIOUS_ATTEMPT_FAILED' };
  }
  return { kind: 'retry_later', clientMsgId, code: 'CONFIRM_PENDING' };
}

export async function fireSequenceStep(runId: string, stepIndex: number): Promise<void> {
  const run = await loadSeqRun(runId);
  if (!run || run.status !== 'running') return;
  const step = run.steps.find((s) => s.index === stepIndex);
  if (!step || step.status !== 'pending') {
    // 该步已处理（重复触发/恢复期重叠），直接推进下一步。
    await driveNext(runId);
    return;
  }
  const g = await getPool().query(`SELECT id, gateway_group_id, status FROM groups WHERE id = $1`, [run.group_id]);
  if (g.rows.length === 0 || String(g.rows[0].status) !== 'active') {
    await stopSeqRun(runId, run.group_id, run.current_step_index);
    return;
  }
  const gatewayGroupId = String(g.rows[0].gateway_group_id);
  await saveSeqRun(runId, run.steps, { currentStepIndex: stepIndex });

  const picked = await pickAccount(run.group_id, step.accountRole);
  if (picked.kind === 'none') {
    // 无匹配账号：skipped，视为在跳过时刻"发出"，进度照常推进。
    step.status = 'skipped';
    step.sentAt = isoNow();
    await saveSeqRun(runId, run.steps);
    await emitSeqRun(runId, run.group_id, 'running', stepIndex);
    await driveNext(runId);
    return;
  }
  if (picked.kind === 'wait') {
    // 限流顺延：到限流结束后重发，不跳过。
    step.scheduledAt = new Date(picked.untilMs).toISOString();
    await saveSeqRun(runId, run.steps);
    scheduleSeqStep(runId, stepIndex, picked.untilMs);
    return;
  }

  step.accountId = picked.accountId;
  let outcome: DeliveryOutcome;
  if (step.clientMsgId) {
    outcome = await confirmExistingStep(run.group_id, gatewayGroupId, picked.accountId, step.resolvedText, step.clientMsgId);
  } else {
    const clientMsgId = uuidv4();
    step.clientMsgId = clientMsgId;
    await saveSeqRun(runId, run.steps);
    outcome = await deliverMessage({
      groupId: run.group_id,
      gatewayGroupId,
      accountId: picked.accountId,
      text: step.resolvedText,
      clientMsgId,
      confirmMs: 12000,
    });
  }
  await applyStepOutcome(runId, run.group_id, stepIndex, outcome);
}

async function applyStepOutcome(runId: string, groupId: string, stepIndex: number, outcome: DeliveryOutcome): Promise<void> {
  const run = await loadSeqRun(runId);
  if (!run || run.status !== 'running') return;
  const step = run.steps.find((s) => s.index === stepIndex);
  if (!step) return;
  switch (outcome.kind) {
    case 'sent': {
      step.status = 'sent';
      step.sentAt = outcome.sentAt ?? isoNow();
      step.failCode = null;
      await saveSeqRun(runId, run.steps);
      await emitSeqRun(runId, groupId, 'running', stepIndex);
      await driveNext(runId);
      return;
    }
    case 'terminal': {
      // 终态账号：该步 skipped。
      step.status = 'skipped';
      step.sentAt = isoNow();
      await saveSeqRun(runId, run.steps);
      await emitSeqRun(runId, groupId, 'running', stepIndex);
      await driveNext(runId);
      return;
    }
    case 'group_forbidden': {
      await stopSeqRun(runId, groupId, stepIndex);
      return;
    }
    case 'rate_limited': {
      // 顺延到限流结束后，不跳过。
      const at = Date.now() + (outcome.retryAfter ?? 60) * 1000 + 800;
      step.scheduledAt = new Date(at).toISOString();
      await saveSeqRun(runId, run.steps);
      scheduleSeqStep(runId, stepIndex, at);
      return;
    }
    case 'retry_later': {
      step.attempts = (step.attempts ?? 0) + 1;
      if (step.attempts > 5) {
        step.status = 'failed';
        step.failCode = outcome.code ?? 'RETRYABLE';
        await saveSeqRun(runId, run.steps);
        await emitSeqRun(runId, groupId, 'running', stepIndex);
        await driveNext(runId);
        return;
      }
      const at = Date.now() + 5000;
      step.scheduledAt = new Date(at).toISOString();
      await saveSeqRun(runId, run.steps);
      scheduleSeqStep(runId, stepIndex, at);
      return;
    }
    case 'failed':
    default: {
      step.status = 'failed';
      step.failCode = outcome.code ?? 'SEND_FAILED';
      await saveSeqRun(runId, run.steps);
      await emitSeqRun(runId, groupId, 'running', stepIndex);
      await driveNext(runId);
      return;
    }
  }
}

// 上一步"发出"（sent 的 message_sent 时刻 / skipped 的跳过时刻）后 delaySeconds 发下一步。
async function driveNext(runId: string): Promise<void> {
  const run = await loadSeqRun(runId);
  if (!run || run.status !== 'running') return;
  const ordered = [...run.steps].sort((a, b) => a.index - b.index);
  const next = ordered.find((s) => s.status === 'pending');
  if (!next) {
    const failed = ordered.some((s) => s.status === 'failed');
    const status = failed ? 'failed' : 'finished';
    await saveSeqRun(runId, run.steps, { status });
    await emitSeqRun(runId, run.group_id, status, run.current_step_index);
    return;
  }
  // 前一步的发出时刻：已完成步骤中 index 最大的 sentAt。
  let prevMoment = Date.now();
  for (const s of ordered) {
    if (s.index < next.index && s.sentAt) {
      const t = Date.parse(s.sentAt);
      if (Number.isFinite(t) && t > prevMoment - 86400000) prevMoment = Math.max(prevMoment, t);
    }
  }
  const at = Math.max(Date.now(), prevMoment + next.delaySeconds * 1000);
  next.scheduledAt = new Date(at).toISOString();
  await saveSeqRun(runId, run.steps, { currentStepIndex: next.index });
  scheduleSeqStep(runId, next.index, at);
}

async function stopSeqRun(runId: string, groupId: string, currentStepIndex: number): Promise<void> {
  clearSeqTimer(runId);
  await saveSeqRun(runId, (await loadSeqRun(runId))?.steps ?? [], { status: 'stopped' });
  await emitSeqRun(runId, groupId, 'stopped', currentStepIndex);
}

// 重启恢复：只重排最早一个已过期的待发步骤（重启时刻 + 该步 delaySeconds），
// 后续步骤仍按"前一步发出后"排期，不一次性全部发出。
export async function resumeSequenceRuns(): Promise<void> {
  // 限流恢复兜底：窗口已过但仍是 rate_limited 的账号转回 online。
  try {
    const r = await getPool().query(
      `UPDATE accounts SET status = 'online', rate_limited_until = NULL
       WHERE status = 'rate_limited' AND rate_limited_until <= now()`,
    );
    void r;
  } catch {
    // 忽略，发送路径会继续处理。
  }
  const { rows } = await getPool().query(`SELECT id FROM sequence_runs WHERE status = 'running'`);
  for (const row of rows as Array<{ id: string }>) {
    try {
      const run = await loadSeqRun(row.id);
      if (!run || run.status !== 'running') continue;
      const ordered = [...run.steps].sort((a, b) => a.index - b.index);
      const pending = ordered.filter((s) => s.status === 'pending');
      if (pending.length === 0) {
        await driveNext(run.id);
        continue;
      }
      const first = pending[0];
      // 后续待发步骤的旧排期作废，等前一步发出后再排。
      let touched = false;
      for (const s of pending.slice(1)) {
        if (s.scheduledAt !== null) {
          s.scheduledAt = null;
          touched = true;
        }
      }
      const scheduled = first.scheduledAt ? Date.parse(first.scheduledAt) : Number.NaN;
      if (!Number.isFinite(scheduled) || scheduled <= Date.now()) {
        const at = Date.now() + first.delaySeconds * 1000;
        first.scheduledAt = new Date(at).toISOString();
        touched = true;
        await saveSeqRun(run.id, run.steps, { currentStepIndex: first.index });
        scheduleSeqStep(run.id, first.index, at);
      } else {
        if (touched) await saveSeqRun(run.id, run.steps);
        await saveSeqRun(run.id, (await loadSeqRun(run.id))?.steps ?? [], { currentStepIndex: first.index });
        scheduleSeqStep(run.id, first.index, scheduled);
      }
    } catch {
      // 单个 run 恢复失败不影响其他 run。
    }
  }
}

// ---------- 路由 ----------

export const sequenceRouter: Router = Router();

// POST /api/sequences { name, steps } → { id }。
sequenceRouter.post(
  '/sequences',
  requireAuth,
  requireAdmin,
  asyncHandler(async (req: Request, res: Response) => {
    const body = (req.body ?? {}) as Record<string, unknown>;
    if (typeof body.name !== 'string' || body.name.length === 0 || !Array.isArray(body.steps) || body.steps.length === 0) {
      sendError(req, res, 400, 'VALIDATION_ERROR', 'name 与非空 steps[] 必填');
      return;
    }
    const defs: SeqStepDef[] = [];
    const seen = new Set<number>();
    for (const raw of body.steps) {
      if (typeof raw !== 'object' || raw === null) {
        sendError(req, res, 400, 'VALIDATION_ERROR', 'step 必须为对象');
        return;
      }
      const s = raw as Record<string, unknown>;
      if (
        typeof s.index !== 'number' ||
        !Number.isInteger(s.index) ||
        s.index <= 0 ||
        (s.accountRole !== 'admin' && s.accountRole !== 'member') ||
        typeof s.text !== 'string' ||
        typeof s.delaySeconds !== 'number' ||
        !Number.isFinite(s.delaySeconds) ||
        s.delaySeconds < 0
      ) {
        sendError(req, res, 400, 'VALIDATION_ERROR', 'step 需含 index(正整数)、accountRole(admin|member)、text、delaySeconds(≥0)');
        return;
      }
      if (seen.has(s.index)) {
        sendError(req, res, 400, 'VALIDATION_ERROR', `step index 重复: ${s.index}`);
        return;
      }
      seen.add(s.index);
      defs.push({ index: s.index, accountRole: s.accountRole, text: s.text, delaySeconds: s.delaySeconds });
    }
    defs.sort((a, b) => a.index - b.index);
    const id = uuidv4();
    await getPool().query(`INSERT INTO sequences(id, name, steps) VALUES ($1, $2, $3)`, [
      id,
      body.name,
      JSON.stringify(defs),
    ]);
    res.json({ id });
  }),
);
// GET /api/sequences → [{ id, name, steps }]。
sequenceRouter.get(
  '/sequences',
  requireAuth,
  asyncHandler(async (_req: Request, res: Response) => {
    const r = await getPool().query<{ id: string; name: string; steps: unknown }>(
      'SELECT id, name, steps FROM sequences ORDER BY id',
    );
    res.json(
      r.rows.map((row) => {
        let steps: SeqStepDef[] = [];
        if (Array.isArray(row.steps)) {
          steps = row.steps as SeqStepDef[];
        } else if (typeof row.steps === 'string') {
          try {
            const parsed: unknown = JSON.parse(row.steps);
            if (Array.isArray(parsed)) steps = parsed as SeqStepDef[];
          } catch {
            steps = [];
          }
        }
        return { id: row.id, name: row.name, steps };
      }),
    );
  }),
);

function toStrMap(v: unknown): StrMap | null {
  if (typeof v !== 'object' || v === null || Array.isArray(v)) return null;
  const out: StrMap = {};
  for (const [k, val] of Object.entries(v as Record<string, unknown>)) {
    if (typeof val !== 'string') return null;
    out[k] = val;
  }
  return out;
}

// POST /api/groups/:id/sequence-runs { sequenceId, vars, stepVars } → 201 { runId }。
sequenceRouter.post(
  '/groups/:id/sequence-runs',
  requireAuth,
  requireAdmin,
  asyncHandler(async (req: Request, res: Response) => {
    const groupId = routeParam(req, 'id');
    if (!groupId) {
      sendError(req, res, 400, 'VALIDATION_ERROR', '群 id 非法');
      return;
    }
    const body = (req.body ?? {}) as Record<string, unknown>;
    if (typeof body.sequenceId !== 'string' || body.sequenceId.length === 0) {
      sendError(req, res, 400, 'VALIDATION_ERROR', 'sequenceId 必填');
      return;
    }
    const vars = body.vars === undefined ? {} : toStrMap(body.vars);
    if (vars === null) {
      sendError(req, res, 400, 'VALIDATION_ERROR', 'vars 必须为 string→string 映射');
      return;
    }
    let stepVars: Record<string, StrMap> = {};
    if (body.stepVars !== undefined) {
      if (typeof body.stepVars !== 'object' || body.stepVars === null || Array.isArray(body.stepVars)) {
        sendError(req, res, 400, 'VALIDATION_ERROR', 'stepVars 必须为步 index→映射');
        return;
      }
      for (const [k, v] of Object.entries(body.stepVars as Record<string, unknown>)) {
        const m = toStrMap(v);
        if (m === null) {
          sendError(req, res, 400, 'VALIDATION_ERROR', `stepVars[${k}] 必须为 string→string 映射`);
          return;
        }
        stepVars[k] = m;
      }
    }
    const pool = getPool();
    const g = await pool.query(`SELECT id, status FROM groups WHERE id = $1`, [groupId]);
    if (g.rows.length === 0) {
      sendError(req, res, 404, 'GROUP_NOT_FOUND', `群 ${groupId} 不存在`);
      return;
    }
    const seq = await pool.query(`SELECT id, steps FROM sequences WHERE id = $1`, [body.sequenceId]);
    if (seq.rows.length === 0) {
      sendError(req, res, 404, 'SEQUENCE_NOT_FOUND', `序列 ${String(body.sequenceId)} 不存在`);
      return;
    }
    const defs = (seq.rows[0].steps ?? []) as SeqStepDef[];
    if (!Array.isArray(defs) || defs.length === 0) {
      sendError(req, res, 400, 'VALIDATION_ERROR', '序列步骤为空');
      return;
    }
    // ① 预检所有步骤：任一步骤有未解析 key → 422，不留 running 记录、不碰网关。
    const states: SeqStepState[] = [];
    for (const def of [...defs].sort((a, b) => a.index - b.index)) {
      const { values, sources } = effectiveVars(vars, stepVars, def.index);
      const keys = extractKeys(def.text);
      const resolvedVars: StrMap = {};
      const varSources: Record<string, string> = {};
      for (const k of keys) {
        if (!(k in values)) {
          sendError(req, res, 422, 'UNRESOLVED_PLACEHOLDER', `第 ${def.index} 步占位符 {${k}} 无取值`, {
            stepIndex: def.index,
            key: k,
          });
          return;
        }
        resolvedVars[k] = values[k];
        varSources[k] = sources[k];
      }
      states.push({
        ...def,
        resolvedText: renderText(def.text, values),
        resolvedVars,
        varSources,
        status: 'pending',
        scheduledAt: null,
        sentAt: null,
        clientMsgId: null,
        accountId: null,
        failCode: null,
        attempts: 0,
      });
    }
    // 排期：第 1 步启动后 delaySeconds 发。
    const now = Date.now();
    states.sort((a, b) => a.index - b.index);
    states[0].scheduledAt = new Date(now + states[0].delaySeconds * 1000).toISOString();
    // ② 同群 running 唯一：DB 唯一索引 + 事务，并发下恰好一个 201。
    const runId = uuidv4();
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        `INSERT INTO sequence_runs(id, group_id, sequence_id, status, current_step_index, steps, vars, step_vars)
         VALUES ($1, $2, $3, 'running', $4, $5, $6, $7)`,
        [runId, groupId, String(body.sequenceId), states[0].index, JSON.stringify(states), JSON.stringify(vars), JSON.stringify(stepVars)],
      );
      await client.query('COMMIT');
    } catch (e) {
      try {
        await client.query('ROLLBACK');
      } catch {
        // 忽略。
      }
      if (isUniqueViolation(e)) {
        sendError(req, res, 409, 'SEQUENCE_ALREADY_RUNNING', `群 ${groupId} 已有运行中的序列`);
        return;
      }
      throw e;
    } finally {
      client.release();
    }
    await emitSeqRun(runId, groupId, 'running', states[0].index);
    scheduleSeqStep(runId, states[0].index, now + states[0].delaySeconds * 1000);
    res.status(201).json({ runId });
  }),
);

// GET /api/sequence-runs/:id → { status, currentStepIndex, steps[] }。
sequenceRouter.get(
  '/sequence-runs/:id',
  requireAuth,
  asyncHandler(async (req: Request, res: Response) => {
    const runId = routeParam(req, 'id');
    if (!runId) {
      sendError(req, res, 400, 'VALIDATION_ERROR', 'run id 非法');
      return;
    }
    const run = await loadSeqRun(runId);
    if (!run) {
      sendError(req, res, 404, 'SEQUENCE_RUN_NOT_FOUND', `序列运行 ${runId} 不存在`);
      return;
    }
    res.json({
      id: run.id,
      groupId: run.group_id,
      sequenceId: run.sequence_id,
      status: run.status,
      currentStepIndex: run.current_step_index,
      steps: [...run.steps]
        .sort((a, b) => a.index - b.index)
        .map((s) => ({
          index: s.index,
          status: s.status,
          scheduledAt: s.scheduledAt,
          sentAt: s.sentAt,
          clientMsgId: s.clientMsgId,
          resolvedVars: s.resolvedVars,
          varSources: s.varSources,
        })),
    });
  }),
);
