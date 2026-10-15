// 统一出站入口，供 groups 路由与 agent 共用。
// groups.ts 经动态 require 取 createOutbound（三参调用兼容）；
// 实际发送复用 sequence.ts deliverMessage（含 504 确认与一次重发仲裁）。
import { v4 as uuidv4 } from 'uuid';
import { getPool } from './db';
import { deliverMessage, scheduleRateLimitRecovery, settleUnknown } from './sequence';
import { emit } from './ws';

export interface CreateOutboundOptions {
  idempotencyKey?: string;
  // true：落行后立即返回（queued），网关调用与落地确认在后台进行（操作员发送接口用，202 不等 message_sent）。
  detach?: boolean;
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
  const inserted = await pool.query(
    `INSERT INTO outbound_messages(id, group_id, account_id, client_msg_id, text, delivery_status)
     VALUES ($1, $2, $3, $4, $5, 'queued') ON CONFLICT (client_msg_id) DO NOTHING`,
    [uuidv4(), groupId, accountId, clientMsgId, text],
  );
  if ((inserted.rowCount ?? 0) === 0) {
    // 同 clientMsgId 的出站行已存在：不再调网关（网关不按 clientMsgId 去重，重发会多出一条），
    // 直接返回该行当前状态。
    const cur = await pool.query<{ delivery_status: string }>(
      'SELECT delivery_status FROM outbound_messages WHERE client_msg_id = $1',
      [clientMsgId],
    );
    return { clientMsgId, deliveryStatus: cur.rows[0]?.delivery_status ?? 'unknown' };
  }
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
  const delivery = deliverMessage({
    groupId,
    gatewayGroupId,
    accountId,
    text,
    clientMsgId,
    confirmMs: opts?.idempotencyKey ? 5000 : 12000,
  });
  if (opts?.detach) {
    delivery.catch((e: unknown) => {
      console.error(`[send] 后台投递异常 clientMsgId=${clientMsgId}:`, e instanceof Error ? e.message : e);
    });
    return { clientMsgId, deliveryStatus: 'queued' };
  }
  const outcome = await delivery;
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
  // 窗口未过的限流账号：重新挂到期恢复定时器（进程重启后原定时器已丢失）。
  const pendingLimited = await pool.query<{ id: string; until: Date }>(
    `SELECT id, rate_limited_until AS until FROM accounts
     WHERE status = 'rate_limited' AND rate_limited_until > now()`,
  );
  for (const row of pendingLimited.rows) {
    scheduleRateLimitRecovery(row.id, new Date(row.until).getTime() - Date.now());
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
    // 与运行时同一套收敛逻辑：查询不可用时保持 unknown（后台继续确认），确认没发出才重发/记 failed。
    void settleUnknown(
      {
        groupId: row.group_id,
        gatewayGroupId: row.gateway_group_id,
        accountId: row.account_id,
        text: row.text,
        clientMsgId: row.client_msg_id,
        confirmMs: 5000,
      },
      Date.now(),
      5000,
    )
      .then(async (outcome) => {
        if (outcome.kind === 'rate_limited') {
          await pool.query(`UPDATE outbound_messages SET delivery_status = 'queued' WHERE client_msg_id = $1`, [
            row.client_msg_id,
          ]);
        }
      })
      .catch(() => {
        // 保持 unknown，下次恢复再定。
      });
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
