// 统一出站入口，供 groups 路由与 agent 共用。
// groups.ts 经动态 require 取 createOutbound（三参调用兼容）；
// 实际发送复用 sequence.ts deliverMessage（含 504 确认与一次重发仲裁）。
import { v4 as uuidv4 } from 'uuid';
import { getPool } from './db';
import { deliverMessage, pollLanding } from './sequence';
import { emit } from './ws';

export interface CreateOutboundOptions {
  idempotencyKey?: string;
}

export interface OutboundResult {
  clientMsgId: string;
  deliveryStatus: string;
}

// deliverMessage outcome → 对外 deliveryStatus（静态映射表）。
const STATUS_BY_OUTCOME: Record<string, string> = {
  sent: 'sent',
  rate_limited: 'queued',
  terminal: 'cancelled',
  group_forbidden: 'failed',
  failed: 'failed',
  retry_later: 'queued',
  timeout: 'accepted',
};

export async function createOutbound(
  groupId: string,
  accountId: string,
  text: string,
  opts?: CreateOutboundOptions,
): Promise<OutboundResult> {
  const pool = getPool();
  const key = opts && typeof opts.idempotencyKey === 'string' ? opts.idempotencyKey : undefined;
  const clientMsgId = key ? key : uuidv4();
  // 先落行（网关发出了、库里一定有记录）；同 key 重入时冲突忽略。
  await pool.query(
    `INSERT INTO outbound_messages(id, group_id, account_id, client_msg_id, text, delivery_status)
     VALUES ($1, $2, $3, $4, $5, 'queued') ON CONFLICT (client_msg_id) DO NOTHING`,
    [uuidv4(), groupId, accountId, clientMsgId, text],
  );
  const g = await pool.query<{ gateway_group_id: string }>('SELECT gateway_group_id FROM groups WHERE id = $1', [
    groupId,
  ]);
  if (g.rows.length === 0) {
    const missing: Error & { code?: string } = new Error(`群 ${groupId} 不存在`);
    missing.code = 'GROUP_NOT_FOUND';
    throw missing;
  }
  const gatewayGroupId = g.rows[0].gateway_group_id;
  // 前置：限流窗口内不碰网关（等待期内再发会重置网关计时），直接返回 queued。
  // disconnect/leave 不走此函数，不受影响。
  const acc = await pool.query<{ status: string; rate_limited_until: Date | string | null }>(
    'SELECT status, rate_limited_until FROM accounts WHERE id = $1',
    [accountId],
  );
  if (acc.rows.length > 0 && acc.rows[0].status === 'rate_limited') {
    const until = acc.rows[0].rate_limited_until;
    if (until === null || new Date(until).getTime() > Date.now()) {
      return { clientMsgId, deliveryStatus: 'queued' };
    }
  }
  const outcome = await deliverMessage({
    groupId,
    gatewayGroupId,
    accountId,
    text,
    clientMsgId,
    confirmMs: opts ? 5000 : 12000,
  });
  return { clientMsgId, deliveryStatus: STATUS_BY_OUTCOME[outcome.kind] ?? 'queued' };
}

// 重启恢复：限流窗口已过 → online；unknown 行短窗确认，落地置 sent，
// 重试额度用尽 → failed，否则同 key 重发一次。
export async function resumeOutbound(): Promise<void> {
  const pool = getPool();
  const expired = await pool.query<{ id: string }>(
    `UPDATE accounts SET status = 'online', rate_limited_until = NULL
     WHERE status = 'rate_limited' AND rate_limited_until <= now() RETURNING id`,
  );
  for (const row of expired.rows) {
    try {
      await emit('account_status_changed', { accountId: row.id, from: 'rate_limited', to: 'online' });
    } catch {
      // 事件落库失败不影响恢复本身。
    }
    // 恢复后把排队消息按序发出（失败不抛，靠下次发送继续推进）。
    try {
      await flushQueuedForAccount(row.id);
    } catch {
      // 忽略。
    }
  }
  const unk = await pool.query<{
    client_msg_id: string;
    group_id: string;
    gateway_group_id: string;
    account_id: string;
    text: string;
    retry_count: number;
  }>(
    `SELECT ob.client_msg_id, ob.group_id, g.gateway_group_id, ob.account_id, ob.text, ob.retry_count
     FROM outbound_messages ob JOIN groups g ON g.id = ob.group_id
     WHERE ob.delivery_status = 'unknown'`,
  );
  for (const row of unk.rows) {
    // 短窗确认：pollLanding 内部轮询网关 by-client-id（落地即有）。
    let landed: { msgId: string; sentAt: string } | null = null;
    try {
      landed = await pollLanding(row.gateway_group_id, row.client_msg_id, 2000);
    } catch {
      continue; // 确认失败：保持 unknown，下次恢复再定。
    }
    if (landed) {
      await pool.query(
        `UPDATE outbound_messages SET delivery_status = 'sent', gateway_msg_id = $2, gateway_sent_at = $3
         WHERE client_msg_id = $1`,
        [row.client_msg_id, landed.msgId, landed.sentAt],
      );
      continue;
    }
    if (row.retry_count >= 1) {
      await pool.query(
        `UPDATE outbound_messages SET delivery_status = 'failed', fail_code = 'NETWORK_TIMEOUT'
         WHERE client_msg_id = $1`,
        [row.client_msg_id],
      );
      continue;
    }
    // 同 key 重发一次：走 deliverMessage 全流程（含各错误码收敛与 504 重发仲裁）。
    try {
      const outcome = await deliverMessage({
        groupId: row.group_id,
        gatewayGroupId: row.gateway_group_id,
        accountId: row.account_id,
        text: row.text,
        clientMsgId: row.client_msg_id,
        confirmMs: 5000,
      });
      if (outcome.kind === 'rate_limited') {
        await pool.query(`UPDATE outbound_messages SET delivery_status = 'queued' WHERE client_msg_id = $1`, [
          row.client_msg_id,
        ]);
      }
    } catch {
      // 重发异常：保持 unknown，下次恢复再定。
    }
  }
}

// 限流到期后按原顺序发出该账号排队中的消息（created_at 升序，一次一条，遇限流即停）。
export async function flushQueuedForAccount(accountId: string): Promise<void> {
  const pool = getPool();
  for (;;) {
    const r = await pool.query<{
      client_msg_id: string;
      group_id: string;
      gateway_group_id: string;
      text: string;
    }>(
      `SELECT ob.client_msg_id, ob.group_id, g.gateway_group_id, ob.text
       FROM outbound_messages ob JOIN groups g ON g.id = ob.group_id
       WHERE ob.account_id = $1 AND ob.delivery_status = 'queued'
       ORDER BY ob.created_at ASC LIMIT 1`,
      [accountId],
    );
    if (r.rows.length === 0) return;
    const row = r.rows[0];
    let outcome: { kind: string };
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
    if (outcome.kind === 'rate_limited' || outcome.kind === 'retry_later' || outcome.kind === 'timeout') return;
    if (outcome.kind === 'terminal' || outcome.kind === 'group_forbidden') return;
  }
}
