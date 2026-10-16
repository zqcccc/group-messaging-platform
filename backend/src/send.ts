// 出站发送：操作员发送、序列、agent 共用的唯一出站流水线。
// 出站行状态：queued → sending → accepted → sent | failed | unknown | cancelled。
// sending 是内部状态（已占用、正在调网关 send、结果尚未落库），对外按 queued 展示；
// 崩溃重启后停在 sending 的行按 504 同等处理（确认后再决定是否重发），不会一直卡在 queued。
import { v4 as uuidv4 } from 'uuid';
import { applyAccountTransition } from './accounts';
import { getPool } from './db';
import { GatewayError, gwByClientId, gwSend } from './gateway';
import { emit } from './ws';

function sleep(ms: number): Promise<void> {
  return new Promise<void>((resolve) => setTimeout(resolve, ms));
}

async function emitSafe(type: string, payload: Record<string, unknown>): Promise<void> {
  try {
    await emit(type, payload);
  } catch {
    // 事件落库失败不影响主流程。
  }
}

// 对外 deliveryStatus：内部的 sending 展示为 queued（契约里没有 sending）。
export function publicDeliveryStatus(status: string): string {
  return status === 'sending' ? 'queued' : status;
}

// ---------- 账号/群状态副作用 ----------

// 429 限流：账号进 rate_limited，到期自动回 online（已不是 rate_limited 则不转移）。
export async function applyRateLimited(accountId: string, retryAfterSeconds: number): Promise<void> {
  const pool = getPool();
  const cur = await pool.query('SELECT status FROM accounts WHERE id = $1', [accountId]);
  if (cur.rows.length === 0) return;
  const from = String(cur.rows[0].status);
  if (from === 'suspended' || from === 'session_expired') return;
  const secs = Number.isFinite(retryAfterSeconds) && retryAfterSeconds > 0 ? Math.ceil(retryAfterSeconds) : 60;
  // 转移表上只有 online → rate_limited；已是 rate_limited 时只刷新 rateLimitedUntil（不算转移，不推事件）。
  const upd = await pool.query(
    `UPDATE accounts SET status = 'rate_limited', rate_limited_until = now() + ($2 || ' seconds')::interval
     WHERE id = $1 AND status IN ('online', 'rate_limited')`,
    [accountId, String(secs)],
  );
  if ((upd.rowCount ?? 0) === 0) return;
  if (from !== 'rate_limited') await emitSafe('account_status_changed', { accountId, from, to: 'rate_limited' });
  scheduleRateLimitRecovery(accountId, secs * 1000);
}

// 限流到期恢复：只在仍是 rate_limited 且窗口已过时转回 online，再按原顺序 flush 排队消息。
// 重启后由 resumeOutbound 对窗口未过的账号重新挂上（定时器不随进程存活）。
export function scheduleRateLimitRecovery(accountId: string, delayMs: number): void {
  setTimeout(() => {
    void (async () => {
      try {
        const r = await getPool().query(
          `UPDATE accounts SET status = 'online', rate_limited_until = NULL
           WHERE id = $1 AND status = 'rate_limited' AND rate_limited_until <= now()`,
          [accountId],
        );
        if ((r.rowCount ?? 0) > 0) {
          await emitSafe('account_status_changed', { accountId, from: 'rate_limited', to: 'online' });
          await flushQueuedForAccount(accountId);
        }
      } catch {
        // 恢复失败不抛，靠下次发送/重启恢复继续推进。
      }
    })();
  }, Math.max(0, delayMs) + 500);
}

// 终态：复用 accounts.ts 的 CAS 转移（含清成员、取消排队发送、序列步骤 skipped、account_terminal 事件，同事务）。
// 重复进入同一终态静默忽略。
export async function applyTerminal(accountId: string, to: 'suspended' | 'session_expired'): Promise<void> {
  const pool = getPool();
  const cur = await pool.query('SELECT status FROM accounts WHERE id = $1', [accountId]);
  if (cur.rows.length === 0) return;
  const from = String(cur.rows[0].status);
  if (from === to || from === 'suspended' || from === 'session_expired') return;
  await applyAccountTransition(pool, accountId, to, from);
}

// 群不可写：群置 unreachable（账号状态不变）。
export async function markGroupUnreachable(groupId: string): Promise<void> {
  await getPool().query(`UPDATE groups SET status = 'unreachable' WHERE id = $1 AND status = 'active'`, [groupId]);
}

// ---------- 出站行读写 ----------

export interface DeliveryOutcome {
  // pending：这一行已在别处发送中（sending/accepted/unknown），本次调用没有碰网关，结果以行状态为准。
  kind: 'sent' | 'rate_limited' | 'terminal' | 'group_forbidden' | 'failed' | 'retry_later' | 'timeout' | 'pending';
  clientMsgId: string;
  msgId?: string;
  sentAt?: string;
  code?: string;
  retryAfter?: number;
  terminal?: string;
}

interface OutboundRow {
  delivery_status: string;
  fail_code: string | null;
  account_id: string;
  text: string;
  gateway_msg_id: string | null;
  gateway_sent_at: Date | null;
}

export async function readOutboundRow(clientMsgId: string): Promise<OutboundRow | null> {
  const r = await getPool().query<OutboundRow>(
    `SELECT delivery_status, fail_code, account_id, text, gateway_msg_id, gateway_sent_at
     FROM outbound_messages WHERE client_msg_id = $1`,
    [clientMsgId],
  );
  return r.rows[0] ?? null;
}

// 行的当前状态 → DeliveryOutcome（不碰网关）。
export function outcomeOfRow(clientMsgId: string, row: OutboundRow | null): DeliveryOutcome {
  const st = row?.delivery_status;
  if (st === 'sent' && row?.gateway_msg_id) {
    return {
      kind: 'sent',
      clientMsgId,
      msgId: row.gateway_msg_id,
      sentAt: (row.gateway_sent_at ?? new Date()).toISOString(),
    };
  }
  if (st === 'cancelled') return { kind: 'terminal', clientMsgId };
  if (st === 'failed') {
    const code = row?.fail_code ?? 'SEND_FAILED';
    return { kind: code === 'GROUP_WRITE_FORBIDDEN' ? 'group_forbidden' : 'failed', clientMsgId, code };
  }
  if (st === 'sending' || st === 'accepted' || st === 'unknown' || st === 'sent') return { kind: 'pending', clientMsgId };
  return { kind: 'retry_later', clientMsgId, code: 'NOT_CLAIMED' };
}

export async function markRowSent(clientMsgId: string, msgId: string, sentAt: string): Promise<void> {
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

// 202 受理：只从 sending/unknown 前进，不把事件流先一步置好的 sent 改回 accepted。
async function markRowAccepted(clientMsgId: string): Promise<void> {
  await getPool().query(
    `UPDATE outbound_messages SET delivery_status = 'accepted', accepted_at = now()
     WHERE client_msg_id = $1 AND delivery_status IN ('sending', 'unknown')`,
    [clientMsgId],
  );
}

// 网关明确没收下（429/503 等）：sending 退回 queued，之后照常重发。
async function releaseClaim(clientMsgId: string): Promise<void> {
  await getPool().query(
    `UPDATE outbound_messages SET delivery_status = 'queued' WHERE client_msg_id = $1 AND delivery_status = 'sending'`,
    [clientMsgId],
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

// 限流窗口内不碰网关（等待期内再发会重置网关计时）；返回剩余秒数，未限流返回 null。
async function rateLimitRemaining(accountId: string): Promise<number | null> {
  const acc = await getPool().query<{ status: string; rate_limited_until: Date | null }>(
    'SELECT status, rate_limited_until FROM accounts WHERE id = $1',
    [accountId],
  );
  const row = acc.rows[0];
  if (!row || row.status !== 'rate_limited') return null;
  if (row.rate_limited_until === null) return 60;
  const left = new Date(row.rate_limited_until).getTime() - Date.now();
  return left > 0 ? Math.ceil(left / 1000) : null;
}

// ---------- 落地确认 ----------

// 落地确认：by-client-id 轮询为主（message_sent 落地即有），outbound 行 sent 为辅（sse 已确认）。
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
      const row = await readOutboundRow(clientMsgId);
      if (row && row.delivery_status === 'sent' && row.gateway_msg_id) {
        return { msgId: row.gateway_msg_id, sentAt: (row.gateway_sent_at ?? new Date()).toISOString() };
      }
    } catch {
      // 忽略，继续轮询。
    }
    if (Date.now() >= deadline) return null;
    await sleep(200);
  }
}

// 504 之后的确认：by-client-id 命中 → 已发出；504 起超过 2 秒仍明确 404 → 确认没发出；
// 查询本身不可用（503/网络错误）不算 404，期间保持 unknown（A2：未确认没发出之前不能重发）。
async function confirmAfterTimeout(
  gatewayGroupId: string,
  clientMsgId: string,
  timeoutAt: number,
  maxWaitMs: number,
): Promise<{ msgId: string; sentAt: string } | 'absent' | 'inconclusive'> {
  const deadline = Date.now() + maxWaitMs;
  for (;;) {
    try {
      const hit = await gwByClientId(gatewayGroupId, clientMsgId);
      if (hit) return hit;
      if (Date.now() - timeoutAt >= 2000) return 'absent';
    } catch {
      // 查询不可用：结论未知，继续等。
    }
    // 事件流已先一步收到 message_sent（出站行被 sse 置 sent）同样算已发出。
    try {
      const row = await readOutboundRow(clientMsgId);
      if (row && row.delivery_status === 'sent' && row.gateway_msg_id) {
        return { msgId: row.gateway_msg_id, sentAt: (row.gateway_sent_at ?? new Date()).toISOString() };
      }
    } catch {
      // 忽略，继续轮询。
    }
    if (Date.now() >= deadline) return 'inconclusive';
    await sleep(200);
  }
}

interface SendCtx {
  groupId: string;
  gatewayGroupId: string;
  accountId: string;
  text: string;
  clientMsgId: string;
  confirmMs: number;
}

// unknown 行收敛：落地 → sent；确认没发出 → 有额度则同 clientMsgId 重发一次，否则 failed(NETWORK_TIMEOUT)；
// 查询不可用 → 保持 unknown 返回 timeout，并在后台继续确认（恢复后 2 秒内定状态）。
async function settleUnknown(ctx: SendCtx, timeoutAt: number, maxWaitMs: number): Promise<DeliveryOutcome> {
  const r = await confirmAfterTimeout(ctx.gatewayGroupId, ctx.clientMsgId, timeoutAt, maxWaitMs);
  if (r === 'inconclusive') {
    if (maxWaitMs < 600000) {
      void settleUnknown(ctx, timeoutAt, 600000).catch((e: unknown) => {
        console.error(`[send] unknown 后台确认失败 clientMsgId=${ctx.clientMsgId}:`, e instanceof Error ? e.message : e);
      });
    }
    return { kind: 'timeout', clientMsgId: ctx.clientMsgId };
  }
  if (r !== 'absent') {
    await markRowSent(ctx.clientMsgId, r.msgId, r.sentAt);
    return { kind: 'sent', clientMsgId: ctx.clientMsgId, msgId: r.msgId, sentAt: r.sentAt };
  }
  if (await claimResend(ctx.clientMsgId)) return await sendToGateway(ctx, true);
  await markRowFailed(ctx.clientMsgId, 'NETWORK_TIMEOUT');
  return { kind: 'failed', clientMsgId: ctx.clientMsgId, code: 'NETWORK_TIMEOUT' };
}

// 调一次网关 send 并按 A2 收敛行状态。调用前这一行必须已被本次调用占用：
// 首发为 sending（queued 抢占所得），重发为 unknown（retry_count 抢占所得）。
async function sendToGateway(ctx: SendCtx, isResend: boolean): Promise<DeliveryOutcome> {
  const pool = getPool();
  const { clientMsgId } = ctx;
  try {
    await gwSend(ctx.gatewayGroupId, ctx.accountId, clientMsgId, ctx.text);
  } catch (e) {
    const code = e instanceof GatewayError ? e.code : 'GATEWAY_UNREACHABLE';
    if (code === 'NETWORK_TIMEOUT') {
      await pool.query(`UPDATE outbound_messages SET delivery_status = 'unknown' WHERE client_msg_id = $1`, [
        clientMsgId,
      ]);
      return await settleUnknown(ctx, Date.now(), Math.max(2500, Math.min(ctx.confirmMs, 5000)));
    }
    if (code === 'ACCOUNT_SUSPENDED' || code === 'SESSION_EXPIRED') {
      const terminal = code === 'ACCOUNT_SUSPENDED' ? 'suspended' : 'session_expired';
      await applyTerminal(ctx.accountId, terminal);
      await pool.query(
        `UPDATE outbound_messages SET delivery_status = 'cancelled', fail_code = 'ACCOUNT_TERMINAL'
         WHERE client_msg_id = $1`,
        [clientMsgId],
      );
      return { kind: 'terminal', clientMsgId, terminal };
    }
    if (code === 'GROUP_WRITE_FORBIDDEN') {
      await markRowFailed(clientMsgId, code);
      await markGroupUnreachable(ctx.groupId);
      return { kind: 'group_forbidden', clientMsgId, code };
    }
    if (code === 'SENDER_NOT_IN_GROUP' || code === 'ACCOUNT_OFFLINE') {
      await markRowFailed(clientMsgId, code);
      return { kind: 'failed', clientMsgId, code };
    }
    if (code === 'RATE_LIMITED') {
      const retryAfter = e instanceof GatewayError && e.retryAfterSeconds !== undefined ? e.retryAfterSeconds : 60;
      await applyRateLimited(ctx.accountId, retryAfter);
      if (!isResend) {
        // 网关没收下：行退回 queued，到期后流水线按顺序发出；调用方顺延。
        await releaseClaim(clientMsgId);
        return { kind: 'rate_limited', clientMsgId, retryAfter };
      }
    }
    if (isResend) {
      // 唯一一次重发也没被受理 → 确定没有发出。
      await markRowFailed(clientMsgId, 'NETWORK_TIMEOUT');
      return { kind: 'failed', clientMsgId, code: 'NETWORK_TIMEOUT' };
    }
    // 503 RETRYABLE 等：网关没收下，行退回 queued，调用方稍后重试。
    await releaseClaim(clientMsgId);
    return { kind: 'retry_later', clientMsgId, code };
  }
  await markRowAccepted(clientMsgId);
  const landed = await pollLanding(ctx.gatewayGroupId, clientMsgId, ctx.confirmMs);
  if (landed) {
    await markRowSent(clientMsgId, landed.msgId, landed.sentAt);
    return { kind: 'sent', clientMsgId, msgId: landed.msgId, sentAt: landed.sentAt };
  }
  return { kind: 'timeout', clientMsgId };
}

// 发送一条出站行：行不存在先落 queued（网关发出了、库里一定有记录），
// 再 queued → sending 原子占用，抢到才调网关——同一出站行在网关里只会对应一条消息。
// 抢不到（已在别处发送 / 已终结）不碰网关，按行的当前状态返回。
export async function deliverMessage(args: {
  groupId: string;
  gatewayGroupId: string;
  accountId: string;
  text: string;
  clientMsgId?: string;
  confirmMs?: number;
}): Promise<DeliveryOutcome> {
  const pool = getPool();
  const clientMsgId = args.clientMsgId ?? uuidv4();
  await pool.query(
    `INSERT INTO outbound_messages(id, group_id, account_id, client_msg_id, text, delivery_status)
     VALUES ($1, $2, $3, $4, $5, 'queued') ON CONFLICT (client_msg_id) DO NOTHING`,
    [uuidv4(), args.groupId, args.accountId, clientMsgId, args.text],
  );
  // 行上的账号与文本为准（重启/顺延后重新执行时调用方可能已换了账号）。
  const row = await readOutboundRow(clientMsgId);
  const accountId = row?.account_id ?? args.accountId;
  const retryAfter = await rateLimitRemaining(accountId);
  if (retryAfter !== null) return { kind: 'rate_limited', clientMsgId, retryAfter };
  const claimed = await pool.query(
    `UPDATE outbound_messages SET delivery_status = 'sending' WHERE client_msg_id = $1 AND delivery_status = 'queued'`,
    [clientMsgId],
  );
  if ((claimed.rowCount ?? 0) === 0) return outcomeOfRow(clientMsgId, await readOutboundRow(clientMsgId));
  return await sendToGateway(
    {
      groupId: args.groupId,
      gatewayGroupId: args.gatewayGroupId,
      accountId,
      text: row?.text ?? args.text,
      clientMsgId,
      confirmMs: args.confirmMs ?? 12000,
    },
    false,
  );
}

// ---------- 操作员 / agent 入口 ----------

export interface CreateOutboundOptions {
  idempotencyKey?: string;
  // true：落行后立即返回（queued），网关调用与落地确认在后台进行（操作员发送接口用，202 不等 message_sent）。
  detach?: boolean;
}

export interface OutboundResult {
  clientMsgId: string;
  deliveryStatus: string;
}

// deliverMessage outcome → 对外 deliveryStatus（pending 由调用方再读行状态）。
const STATUS_BY_OUTCOME: Record<string, string> = {
  sent: 'sent',
  rate_limited: 'queued',
  terminal: 'cancelled',
  group_forbidden: 'failed',
  failed: 'failed',
  retry_later: 'queued',
  timeout: 'accepted',
  pending: 'queued',
};

export async function createOutbound(
  groupId: string,
  accountId: string,
  text: string,
  opts?: CreateOutboundOptions,
): Promise<OutboundResult> {
  const pool = getPool();
  const clientMsgId = opts?.idempotencyKey ?? uuidv4();
  const g = await pool.query<{ gateway_group_id: string }>('SELECT gateway_group_id FROM groups WHERE id = $1', [
    groupId,
  ]);
  if (g.rows.length === 0) {
    const missing: Error & { code?: string } = new Error(`群 ${groupId} 不存在`);
    missing.code = 'GROUP_NOT_FOUND';
    throw missing;
  }
  // 先落行；同 key 重入时冲突忽略，且不再调网关（网关不按 clientMsgId 去重，重发会多出一条），直接返回当前状态。
  const inserted = await pool.query(
    `INSERT INTO outbound_messages(id, group_id, account_id, client_msg_id, text, delivery_status)
     VALUES ($1, $2, $3, $4, $5, 'queued') ON CONFLICT (client_msg_id) DO NOTHING`,
    [uuidv4(), groupId, accountId, clientMsgId, text],
  );
  if ((inserted.rowCount ?? 0) === 0) {
    const cur = await readOutboundRow(clientMsgId);
    return { clientMsgId, deliveryStatus: publicDeliveryStatus(cur?.delivery_status ?? 'unknown') };
  }
  const delivery = deliverMessage({
    groupId,
    gatewayGroupId: g.rows[0].gateway_group_id,
    accountId,
    text,
    clientMsgId,
    confirmMs: opts?.idempotencyKey ? 5000 : 12000,
  });
  if (opts?.detach) {
    // 网关暂不可用：行已退回 queued，后台按序补发。
    delivery.then(
      (outcome) => {
        if (outcome.kind === 'retry_later') scheduleRetryFlush(accountId, 1);
      },
      (e: unknown) => {
        console.error(`[send] 后台投递异常 clientMsgId=${clientMsgId}:`, e instanceof Error ? e.message : e);
      },
    );
    return { clientMsgId, deliveryStatus: 'queued' };
  }
  const outcome = await delivery;
  if (outcome.kind === 'retry_later') {
    // 同步调用方（agent）已经拿到失败结果，可能换新 key 重试：这一行就此记 failed，免得之后被补发成重复消息。
    await pool.query(
      `UPDATE outbound_messages SET delivery_status = 'failed', fail_code = $2
       WHERE client_msg_id = $1 AND delivery_status = 'queued'`,
      [clientMsgId, outcome.code ?? 'RETRYABLE'],
    );
    const cur = await readOutboundRow(clientMsgId);
    return { clientMsgId, deliveryStatus: publicDeliveryStatus(cur?.delivery_status ?? 'failed') };
  }
  return { clientMsgId, deliveryStatus: STATUS_BY_OUTCOME[outcome.kind] ?? 'queued' };
}

// ---------- 排队消息与重启恢复 ----------

// 网关暂不可用（503 等）时的补发：每 5 秒按序再 flush 一次，约 1 分钟后放弃（重启恢复会再接上）。
const RETRY_FLUSH_MS = 5000;
const RETRY_FLUSH_MAX = 12;
const flushing = new Set<string>();

function scheduleRetryFlush(accountId: string, attempt: number): void {
  if (attempt > RETRY_FLUSH_MAX) return;
  setTimeout(() => {
    void flushQueuedForAccount(accountId, false, attempt).catch(() => undefined);
  }, RETRY_FLUSH_MS);
}

// 按原顺序发出该账号排队中的消息（created_at 升序，一次一条，遇限流即停）。
// 同一账号同时只跑一个 flush（并发 flush 可能打乱顺序）。
// onlyUnowned：只发不属于运行中序列的行（重启时用，序列步骤由序列恢复按排期重发）。
export async function flushQueuedForAccount(accountId: string, onlyUnowned = false, attempt = 0): Promise<void> {
  if (flushing.has(accountId)) return;
  flushing.add(accountId);
  try {
    await flushLoop(accountId, onlyUnowned, attempt);
  } finally {
    flushing.delete(accountId);
  }
}

async function flushLoop(accountId: string, onlyUnowned: boolean, attempt: number): Promise<void> {
  const pool = getPool();
  const skipped = new Set<string>();
  for (;;) {
    const r = await pool.query<{ client_msg_id: string; group_id: string; gateway_group_id: string; text: string }>(
      `SELECT ob.client_msg_id, ob.group_id, g.gateway_group_id, ob.text
       FROM outbound_messages ob JOIN groups g ON g.id = ob.group_id
       WHERE ob.account_id = $1 AND ob.delivery_status = 'queued' AND NOT (ob.client_msg_id = ANY($2))
         AND (NOT $3 OR NOT EXISTS (
           SELECT 1 FROM sequence_runs sr, jsonb_array_elements(sr.steps) st
           WHERE sr.status = 'running' AND st->>'clientMsgId' = ob.client_msg_id))
       ORDER BY ob.created_at ASC LIMIT 1`,
      [accountId, [...skipped], onlyUnowned],
    );
    if (r.rows.length === 0) return;
    const row = r.rows[0];
    let outcome: DeliveryOutcome;
    try {
      outcome = await deliverMessage({
        groupId: row.group_id,
        gatewayGroupId: row.gateway_group_id,
        accountId,
        text: row.text,
        clientMsgId: row.client_msg_id,
        confirmMs: 12000,
      });
    } catch {
      return; // 异常即停，下次发送/恢复再推。
    }
    if (outcome.kind === 'retry_later') {
      scheduleRetryFlush(accountId, attempt + 1);
      return;
    }
    if (outcome.kind === 'rate_limited' || outcome.kind === 'timeout') return;
    if (outcome.kind === 'terminal' || outcome.kind === 'group_forbidden') return;
    // pending：别处已抢走这一行，跳过它继续下一条。
    if (outcome.kind === 'pending') skipped.add(row.client_msg_id);
  }
}

// 重启恢复：
// 1) 限流窗口已过 → online 并按序补发；未过的重新挂到期定时器。
// 2) 停在 sending 的行（崩溃时网关可能已收下、也可能没有）→ 按 504 处理：unknown 后确认，没发出才重发一次。
//    崩溃时那次 send 可能还在等 202（一两秒）+ message_sent（≤2 秒），所以确认窗口比 504 的 2 秒放宽到 5 秒。
// 3) unknown 行短窗确认，落地置 sent，重发额度用尽 → failed，否则同 clientMsgId 重发一次。
// 4) online 账号崩溃前落行但从未调过网关的 queued 行 → 按序补发（运行中序列的步骤除外）。
export async function resumeOutbound(): Promise<void> {
  const pool = getPool();
  const expired = await pool.query<{ id: string }>(
    `UPDATE accounts SET status = 'online', rate_limited_until = NULL
     WHERE status = 'rate_limited' AND rate_limited_until <= now() RETURNING id`,
  );
  for (const row of expired.rows) {
    await emitSafe('account_status_changed', { accountId: row.id, from: 'rate_limited', to: 'online' });
  }
  const pendingLimited = await pool.query<{ id: string; until: Date }>(
    `SELECT id, rate_limited_until AS until FROM accounts
     WHERE status = 'rate_limited' AND rate_limited_until > now()`,
  );
  for (const row of pendingLimited.rows) {
    scheduleRateLimitRecovery(row.id, new Date(row.until).getTime() - Date.now());
  }
  const inflight = await pool.query<{ client_msg_id: string }>(
    `UPDATE outbound_messages SET delivery_status = 'unknown' WHERE delivery_status = 'sending' RETURNING client_msg_id`,
  );
  const wasSending = new Set(inflight.rows.map((r) => r.client_msg_id));
  const unk = await pool.query<{
    client_msg_id: string;
    group_id: string;
    gateway_group_id: string;
    account_id: string;
    text: string;
  }>(
    `SELECT ob.client_msg_id, ob.group_id, g.gateway_group_id, ob.account_id, ob.text
     FROM outbound_messages ob JOIN groups g ON g.id = ob.group_id
     WHERE ob.delivery_status = 'unknown'`,
  );
  const restartAt = Date.now();
  for (const row of unk.rows) {
    // 与运行时同一套收敛逻辑；504 行以重启时刻起算 2 秒窗口，崩溃时在途的行起算点后推 3 秒（共 5 秒）。
    const grace = wasSending.has(row.client_msg_id) ? 3000 : 0;
    void settleUnknown(
      {
        groupId: row.group_id,
        gatewayGroupId: row.gateway_group_id,
        accountId: row.account_id,
        text: row.text,
        clientMsgId: row.client_msg_id,
        confirmMs: 5000,
      },
      restartAt + grace,
      5000 + grace,
    ).catch(() => {
      // 保持 unknown，下次恢复再定。
    });
  }
  const queued = await pool.query<{ account_id: string }>(
    `SELECT DISTINCT ob.account_id FROM outbound_messages ob JOIN accounts a ON a.id = ob.account_id
     WHERE ob.delivery_status = 'queued' AND a.status = 'online'`,
  );
  const expiredIds = new Set(expired.rows.map((r) => r.id));
  for (const row of queued.rows) {
    // 刚从限流恢复的账号：排队消息本就该在到期后发出，序列步骤一并发。
    void flushQueuedForAccount(row.account_id, !expiredIds.has(row.account_id)).catch(() => {
      // 失败不抛，靠下次发送/恢复继续推进。
    });
  }
}
