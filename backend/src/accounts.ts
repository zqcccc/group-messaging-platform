// 账号面：列表查询、网关 connect、操作员手动转移（含 A1 转移表、CAS、终态副作用、网关联动）。
import { Request, Response, Router } from 'express';
import { Pool } from 'pg';
import { requireAdmin, requireAuth } from './auth';
import { getPool } from './db';
import { asyncHandler, sendError } from './util';

// A1 状态转移表（CONTRACT 唯一事实源，代码内常量；终态 suspended/session_expired 无出边）。
export const ACCOUNT_TRANSITIONS: Record<string, Record<string, true>> = {
  idle: { online: true, suspended: true, session_expired: true },
  online: { idle: true, rate_limited: true, disconnected: true, suspended: true, session_expired: true },
  rate_limited: { online: true, disconnected: true, suspended: true, session_expired: true },
  disconnected: { idle: true, online: true, suspended: true, session_expired: true },
  suspended: {},
  session_expired: {},
};

export const TERMINAL_STATUSES: Record<string, true> = { suspended: true, session_expired: true };

export type TransitionError = 'ACCOUNT_NOT_FOUND' | 'ILLEGAL_TRANSITION' | 'CAS_CONFLICT';

// 通用状态变更（含终态副作用），后续 send/sse 模块可直接复用：
// 合法性按转移表校验（表外含自转一律 ILLEGAL）；并发用一条带状态条件的 UPDATE 做 CAS；
// 进入终态时清群成员、取消排队发送、记 ws_events，三者与状态变更同事务，要么都生效要么都不生效。
// 说明：序列步骤的 skipped 由 sequence 模块按账号终态自行推导（步骤 JSON 结构归它所有）。
export async function applyAccountTransition(
  pool: Pool,
  id: string,
  to: string,
  expectedFrom: string,
): Promise<{ ok: true; status: string } | { ok: false; error: TransitionError }> {
  const exists = await pool.query('SELECT 1 FROM accounts WHERE id = $1', [id]);
  if (exists.rows.length === 0) return { ok: false, error: 'ACCOUNT_NOT_FOUND' };
  const allowed = ACCOUNT_TRANSITIONS[expectedFrom];
  if (!allowed || !allowed[to]) return { ok: false, error: 'ILLEGAL_TRANSITION' };
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const updated = await client.query(
      'UPDATE accounts SET status = $2, version = version + 1 WHERE id = $1 AND status = $3',
      [id, to, expectedFrom],
    );
    if ((updated.rowCount ?? 0) === 0) {
      // 账号存在但当前状态已不是 expectedFrom：并发下另一个变更先赢了。
      await client.query('ROLLBACK');
      return { ok: false, error: 'CAS_CONFLICT' };
    }
    // 状态事件与状态同事务落库（A1：推给前端的状态事件必须对应已保存的状态），由 WS broadcaster 推送。
    await client.query('INSERT INTO ws_events(type, payload) VALUES ($1, $2)', [
      'account_status_changed',
      { accountId: id, from: expectedFrom, to },
    ]);
    if (TERMINAL_STATUSES[to]) {
      await client.query('DELETE FROM group_members WHERE account_id = $1', [id]);
      await client.query(
        `UPDATE outbound_messages SET delivery_status = 'cancelled', fail_code = 'ACCOUNT_TERMINAL'
         WHERE account_id = $1 AND delivery_status IN ('queued', 'accepted', 'unknown')`,
        [id],
      );
      await client.query('INSERT INTO ws_events(type, payload) VALUES ($1, $2)', [
        'account_terminal',
        { accountId: id, status: to },
      ]);
    }
    await client.query('COMMIT');
    return { ok: true, status: to };
  } catch (e) {
    try {
      await client.query('ROLLBACK');
    } catch {
      // 回滚失败也忽略，上层按 500 处理。
    }
    throw e;
  } finally {
    client.release();
  }
}

// 网关 POST 最小封装（完整客户端归 src/gateway.ts 所有，这里只为避免文件冲突先内联两个调用）。
async function gatewayPost(path: string, body: unknown): Promise<{ status: number; data: unknown }> {
  const base = (process.env.GATEWAY_URL ?? '').replace(/\/+$/, '');
  if (!base) throw new Error('缺少环境变量 GATEWAY_URL');
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), 10000);
  try {
    const res = await fetch(base + path, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body ?? {}),
      signal: ctrl.signal,
    });
    const data: unknown = await res.json().catch(() => null);
    return { status: res.status, data };
  } finally {
    clearTimeout(timer);
  }
}

// 网关错误码：兼容 { code } 与 { error: { code } } 两种形状。
function gatewayCode(data: unknown): string | undefined {
  if (typeof data !== 'object' || data === null) return undefined;
  const d = data as Record<string, unknown>;
  if (typeof d.code === 'string') return d.code;
  const inner = d.error as Record<string, unknown> | undefined;
  if (inner && typeof inner.code === 'string') return inner.code;
  return undefined;
}

export const accountsRouter: Router = Router();

// GET /api/accounts → [{ id, status, platformUserId, rateLimitedUntil }]（viewer 可读）。
accountsRouter.get(
  '/',
  requireAuth,
  asyncHandler(async (_req: Request, res: Response) => {
    const { rows } = await getPool().query(
      'SELECT id, status, platform_user_id, rate_limited_until FROM accounts ORDER BY id',
    );
    res.json(
      rows.map((r) => ({
        id: r.id as string,
        status: r.status as string,
        platformUserId: (r.platform_user_id as string | null) ?? null,
        rateLimitedUntil:
          r.rate_limited_until != null ? (r.rate_limited_until as Date).toISOString() : null,
      })),
    );
  }),
);

// POST /api/accounts/:id/connect：调网关 connect，存 platformUserId，idle/disconnected/online → online。
// 网关不可达或返回未知错误 → 502 但进程不崩；网关报终态则本地同步进入终态（含副作用）。
accountsRouter.post(
  '/:id/connect',
  requireAuth,
  requireAdmin,
  asyncHandler(async (req: Request, res: Response) => {
    const rawId = req.params.id;
    const id = Array.isArray(rawId) ? rawId[0] ?? '' : rawId;
    const cur = await getPool().query('SELECT id, status FROM accounts WHERE id = $1', [id]);
    if (cur.rows.length === 0) {
      sendError(req, res, 404, 'ACCOUNT_NOT_FOUND', '账号不存在');
      return;
    }
    const status = cur.rows[0].status as string;
    if (TERMINAL_STATUSES[status] || (status !== 'idle' && status !== 'disconnected' && status !== 'online')) {
      sendError(req, res, 409, 'ILLEGAL_TRANSITION', `状态 ${status} 不允许 connect`);
      return;
    }
    let gw: { status: number; data: unknown };
    try {
      gw = await gatewayPost(`/accounts/${encodeURIComponent(id)}/connect`, {});
    } catch {
      sendError(req, res, 502, 'GATEWAY_UNAVAILABLE', '网关不可用');
      return;
    }
    const code = gatewayCode(gw.data);
    if (code === 'SESSION_EXPIRED' || (code === undefined && gw.status === 401)) {
      await applyAccountTransition(getPool(), id, 'session_expired', status);
      sendError(req, res, 401, 'SESSION_EXPIRED', '账号会话已失效');
      return;
    }
    if (code === 'ACCOUNT_SUSPENDED' || (code === undefined && gw.status === 403)) {
      await applyAccountTransition(getPool(), id, 'suspended', status);
      sendError(req, res, 403, 'ACCOUNT_SUSPENDED', '账号已被停用');
      return;
    }
    if (gw.status < 200 || gw.status >= 300) {
      sendError(req, res, 502, 'GATEWAY_UNAVAILABLE', `网关返回 ${gw.status}`);
      return;
    }
    const platformUserId = (gw.data as { platformUserId?: unknown } | null)?.platformUserId;
    if (typeof platformUserId !== 'string' || !platformUserId) {
      sendError(req, res, 502, 'GATEWAY_UNAVAILABLE', '网关响应缺少 platformUserId');
      return;
    }
    // CAS：只在状态仍是 connect 前读到的值时生效，并与 account_status_changed 事件同事务落库。
    const client = await getPool().connect();
    try {
      await client.query('BEGIN');
      const upd = await client.query(
        `UPDATE accounts SET platform_user_id = $2, status = 'online', version = version + 1
         WHERE id = $1 AND status = $3`,
        [id, platformUserId, status],
      );
      if ((upd.rowCount ?? 0) === 0) {
        await client.query('ROLLBACK');
        sendError(req, res, 409, 'CAS_CONFLICT', `账号状态在 connect 期间已变化（原为 ${status}）`);
        return;
      }
      if (status !== 'online') {
        await client.query('INSERT INTO ws_events(type, payload) VALUES ($1, $2)', [
          'account_status_changed',
          { accountId: id, from: status, to: 'online' },
        ]);
      }
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw e;
    } finally {
      client.release();
    }
    res.json({ status: 'online', platformUserId });
  }),
);

// POST /api/accounts/:id/transition { to, expectedFrom }：操作员手动标记状态。
// 标为 disconnected/idle 时顺带调网关 disconnect（尽力而为：失败仍以本地标记为准，
// 唯独网关明确报终态时升级为终态）。
accountsRouter.post(
  '/:id/transition',
  requireAuth,
  requireAdmin,
  asyncHandler(async (req: Request, res: Response) => {
    const rawId = req.params.id;
    const id = Array.isArray(rawId) ? rawId[0] ?? '' : rawId;
    const body = req.body as { to?: unknown; expectedFrom?: unknown };
    if (typeof body?.to !== 'string' || !body.to || typeof body?.expectedFrom !== 'string' || !body.expectedFrom) {
      sendError(req, res, 400, 'VALIDATION_ERROR', 'to 与 expectedFrom 必填');
      return;
    }
    const r = await applyAccountTransition(getPool(), id, body.to, body.expectedFrom);
    if (!r.ok) {
      if (r.error === 'ACCOUNT_NOT_FOUND') {
        sendError(req, res, 404, 'ACCOUNT_NOT_FOUND', '账号不存在');
        return;
      }
      if (r.error === 'ILLEGAL_TRANSITION') {
        sendError(req, res, 409, 'ILLEGAL_TRANSITION', `${body.expectedFrom} → ${body.to} 不在转移表上`);
        return;
      }
      sendError(req, res, 409, 'CAS_CONFLICT', `账号当前状态已不是 ${body.expectedFrom}`);
      return;
    }
    let status = r.status;
    if (status === 'idle' || status === 'disconnected') {
      try {
        const gw = await gatewayPost(`/accounts/${encodeURIComponent(id)}/disconnect`, {});
        const code = gatewayCode(gw.data);
        const terminal =
          code === 'SESSION_EXPIRED' ? 'session_expired' : code === 'ACCOUNT_SUSPENDED' ? 'suspended' : null;
        if (terminal) {
          const t2 = await applyAccountTransition(getPool(), id, terminal, status);
          if (t2.ok) status = t2.status;
        }
      } catch {
        // 网关不可达时仍以本地标记为准，不回滚。
      }
    }
    res.json({ status });
  }),
);
