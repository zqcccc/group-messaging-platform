// 消息时间线：合并 outbound（自发消息，queued 起可见）与 inbound，
// 按 sentAt 倒序 + (clientMsgId/msgId) 辅助排序做稳定游标分页；同一 clientMsgId 只出一行。
import { Request, Response, Router } from 'express';
import { requireAuth } from './auth';
import { getPool } from './db';
import { publicDeliveryStatus } from './send';
import { asyncHandler, sendError } from './util';

interface TimelineItem {
  msgId: string;
  clientMsgId: string | null;
  senderPlatformUserId: string;
  isOwn: boolean;
  text: string;
  sentAt: string;
  deliveryStatus: string | null;
  failCode: string | null;
}

interface CursorPayload {
  t: string;
  id: string;
}

interface OutboundRow {
  account_id: string;
  client_msg_id: string;
  text: string;
  delivery_status: string;
  fail_code: string | null;
  gateway_msg_id: string | null;
  gateway_sent_at: Date | null;
  created_at: Date;
}

interface InboundRow {
  msg_id: string;
  sender_platform_user_id: string;
  text: string;
  sent_at: Date;
  is_own: boolean;
  client_msg_id: string | null;
}

// 游标 id：自发消息用 clientMsgId（发出前后不变），外部消息用 msgId（天然不变）。
function cursorIdOf(item: TimelineItem): string {
  return item.clientMsgId ?? item.msgId;
}

function encodeCursor(item: TimelineItem): string {
  const payload: CursorPayload = { t: item.sentAt, id: cursorIdOf(item) };
  return Buffer.from(JSON.stringify(payload), 'utf8').toString('base64url');
}

// before 解码：缺省返回 undefined（第一页）；非法返回 null（调用方 400）。
function decodeCursor(raw: unknown): CursorPayload | undefined | null {
  if (raw === undefined) return undefined;
  const text = Array.isArray(raw) ? raw[0] : raw;
  if (typeof text !== 'string' || text.length === 0) return null;
  try {
    const parsed: unknown = JSON.parse(Buffer.from(text, 'base64url').toString('utf8'));
    if (typeof parsed !== 'object' || parsed === null) return null;
    if (!('t' in parsed) || !('id' in parsed)) return null;
    if (typeof parsed.t !== 'string' || typeof parsed.id !== 'string') return null;
    if (parsed.t.length === 0 || parsed.id.length === 0) return null;
    return { t: parsed.t, id: parsed.id };
  } catch {
    return null;
  }
}

// limit 解析：缺省 50；非 1–200 整数返回 null（调用方 400）。
function parseLimit(raw: unknown): number | null {
  if (raw === undefined) return 50;
  const text = Array.isArray(raw) ? raw[0] : raw;
  if (typeof text !== 'string') return null;
  const n = Number(text);
  if (!Number.isInteger(n) || n < 1 || n > 200) return null;
  return n;
}

// 倒序比较：sentAt 降序，相同则游标 id 降序（与游标截断方向一致，保证往返无重复遗漏）。
function compareDesc(a: TimelineItem, b: TimelineItem): number {
  if (a.sentAt !== b.sentAt) return a.sentAt < b.sentAt ? 1 : -1;
  const aid = cursorIdOf(a);
  const bid = cursorIdOf(b);
  if (aid !== bid) return aid < bid ? 1 : -1;
  return 0;
}

export const messagesRouter: Router = Router();

// GET /api/groups/:id/messages?before=<cursor>&limit=50 → { items, nextCursor }。
messagesRouter.get(
  '/:id/messages',
  requireAuth,
  asyncHandler(async (req: Request, res: Response) => {
    const groupId: unknown = req.params.id;
    if (typeof groupId !== 'string') {
      sendError(req, res, 400, 'VALIDATION_ERROR', '群 id 非法');
      return;
    }
    const limit = parseLimit(req.query.limit);
    if (limit === null) {
      sendError(req, res, 400, 'VALIDATION_ERROR', 'limit 须为 1–200 的整数');
      return;
    }
    const before = decodeCursor(req.query.before);
    if (before === null) {
      sendError(req, res, 400, 'VALIDATION_ERROR', 'before 游标非法');
      return;
    }
    const pool = getPool();
    const g = await pool.query<{ id: string }>('SELECT id FROM groups WHERE id = $1', [groupId]);
    if (g.rows.length === 0) {
      sendError(req, res, 404, 'GROUP_NOT_FOUND', `群 ${groupId} 不存在`);
      return;
    }
    const outbound = await pool.query<OutboundRow>(
      `SELECT account_id, client_msg_id, text, delivery_status, fail_code,
              gateway_msg_id, gateway_sent_at, created_at
       FROM outbound_messages WHERE group_id = $1`,
      [groupId],
    );
    const inbound = await pool.query<InboundRow>(
      `SELECT msg_id, sender_platform_user_id, text, sent_at, is_own, client_msg_id
       FROM inbound_messages WHERE group_id = $1`,
      [groupId],
    );
    // 自发送方 platformUserId：从 accounts 表取，缺失时按网关命名规则兜底。
    const accountIds = [...new Set(outbound.rows.map((r) => r.account_id))];
    const senderOf = new Map<string, string>();
    if (accountIds.length > 0) {
      const accs = await pool.query<{ id: string; platform_user_id: string | null }>(
        'SELECT id, platform_user_id FROM accounts WHERE id = ANY($1)',
        [accountIds],
      );
      for (const r of accs.rows) senderOf.set(r.id, r.platform_user_id ?? `pu-${r.id}`);
    }

    // 按 clientMsgId 建回流入站索引：自发消息的回流只出一行（合到 outbound 行里）。
    const inboundByClient = new Map<string, InboundRow>();
    for (const r of inbound.rows) {
      if (r.client_msg_id !== null && !inboundByClient.has(r.client_msg_id)) {
        inboundByClient.set(r.client_msg_id, r);
      }
    }
    const consumed = new Set<string>();
    const items: TimelineItem[] = [];
    for (const o of outbound.rows) {
      const echo = inboundByClient.get(o.client_msg_id);
      if (echo !== undefined) consumed.add(o.client_msg_id);
      // sentAt：未发出用受理时刻（created_at），发出后换网关值。
      const sentAt =
        o.gateway_sent_at !== null
          ? o.gateway_sent_at.toISOString()
          : (echo?.sent_at.toISOString() ?? o.created_at.toISOString());
      const needsCode = o.delivery_status === 'failed' || o.delivery_status === 'cancelled';
      items.push({
        msgId: o.gateway_msg_id ?? echo?.msg_id ?? o.client_msg_id,
        clientMsgId: o.client_msg_id,
        senderPlatformUserId: senderOf.get(o.account_id) ?? `pu-${o.account_id}`,
        isOwn: true,
        text: o.text,
        sentAt,
        deliveryStatus: publicDeliveryStatus(o.delivery_status),
        // failed/cancelled 必有 failCode：库里缺失时兜底 UNKNOWN（输出契约优先）。
        failCode: o.fail_code ?? (needsCode ? 'UNKNOWN' : null),
      });
    }
    for (const r of inbound.rows) {
      if (r.client_msg_id !== null && consumed.has(r.client_msg_id)) continue;
      items.push({
        msgId: r.msg_id,
        clientMsgId: r.client_msg_id,
        senderPlatformUserId: r.sender_platform_user_id,
        isOwn: r.is_own,
        text: r.text,
        sentAt: r.sent_at.toISOString(),
        // deliveryStatus 仅对自己的消息有意义。
        deliveryStatus: null,
        failCode: null,
      });
    }
    items.sort(compareDesc);
    // 游标截断：只要严格早于 (t, id) 的行；新写入的消息只会排在前面，不影响“加载更早”。
    const scoped =
      before === undefined
        ? items
        : items.filter((it) => it.sentAt < before.t || (it.sentAt === before.t && cursorIdOf(it) < before.id));
    const page = scoped.slice(0, limit);
    res.json({
      items: page,
      nextCursor: scoped.length > page.length ? encodeCursor(page[page.length - 1]) : null,
    });
  }),
);
