// 网关 SSE 消费：长连接拉取、去重、断线补拉。
// groups.ts 经动态 require 取 subscribeMemberEvents（成员等待的提前唤醒）；
// 本文件绝不 import groups.ts，避免循环依赖。
import { applyAccountTransition } from './accounts';
import { getPool } from './db';
import { applyRateLimited, applyTerminal, markGroupUnreachable } from './sequence';
import { emit, emitInconsistency } from './ws';

// groups.ts 动态 require 的成员事件形状（调用方按宽形状接收）。
export interface MemberEvent {
  gatewayGroupId: string;
  platformUserId: string;
  kind: 'joined' | 'left';
}

type MemberCb = (ev: MemberEvent) => void;

// 存活订阅集合（动态增删，用 Set）。
const memberSubs = new Set<MemberCb>();

// 成员事件订阅：处理 member_joined/left 时同步调用全部订阅者；返回取消订阅函数。
export function subscribeMemberEvents(cb: MemberCb): () => void {
  memberSubs.add(cb);
  return () => {
    memberSubs.delete(cb);
  };
}

// 同步通知订阅者（单个订阅者抛错不中断事件落库）。
function notifyMembers(ev: MemberEvent): void {
  for (const cb of [...memberSubs]) {
    try {
      cb(ev);
    } catch {
      // 订阅者异常直接丢弃，事件本身已/将落库。
    }
  }
}

async function readCursor(): Promise<number> {
  const pool = getPool();
  await pool.query('INSERT INTO gateway_cursor(id, last_event_id) VALUES (1, 0) ON CONFLICT (id) DO NOTHING');
  const r = await pool.query<{ last_event_id: string }>('SELECT last_event_id FROM gateway_cursor WHERE id = 1');
  if (r.rows.length === 0) return 0;
  const v = Number(r.rows[0].last_event_id);
  return Number.isFinite(v) && v >= 0 ? Math.floor(v) : 0;
}

// 只向前推进（max 语义，乱序到达的旧事件不回拨）。
async function advanceCursor(eventId: number): Promise<void> {
  await getPool().query('UPDATE gateway_cursor SET last_event_id = GREATEST(last_event_id, $1) WHERE id = 1', [
    eventId,
  ]);
}

interface GatewayEvent {
  id: number;
  type: string;
  data: Record<string, unknown>;
}

// 服务账号的 platformUserId 集合（isOwn 判定用；动态结果，用 Set）。
async function serviceAccountIds(): Promise<Set<string>> {
  const r = await getPool().query<{ platform_user_id: string | null }>(
    'SELECT platform_user_id FROM accounts WHERE platform_user_id IS NOT NULL',
  );
  const out = new Set<string>();
  for (const row of r.rows) {
    if (row.platform_user_id) out.add(row.platform_user_id);
  }
  return out;
}

// 网关群 id 反查内部群 id（未知群返回 null）。
async function groupIdByGateway(gatewayGroupId: string): Promise<string | null> {
  const r = await getPool().query<{ id: string }>('SELECT id FROM groups WHERE gateway_group_id = $1', [
    gatewayGroupId,
  ]);
  return r.rows.length > 0 ? r.rows[0].id : null;
}

// agent 触发函数签名（动态模块无法静态校验，调用前已做 typeof 检查）。
type OnInboundMessage = (
  groupId: string,
  msg: { msgId: string; senderPlatformUserId: string; text: string; sentAt: string },
) => Promise<unknown> | unknown;

// 自发回流的出站关联（顶层函数，供 handleMessage 调用）。
// 两种到达顺序都要合为一行：
// message_sent 先到时行上已有 gateway_msg_id=msgId，直接找回 clientMsgId；
// message 先到时行尚未确认，按发送方 FIFO 关联最早未确认行并置 sent。
async function linkOwnOutbound(
  groupId: string,
  sender: string,
  msgId: string,
  sentAt: string,
): Promise<string | null> {
  const pool = getPool();
  const byMsg = await pool.query<{ client_msg_id: string }>(
    'SELECT client_msg_id FROM outbound_messages WHERE gateway_msg_id = $1',
    [msgId],
  );
  if (byMsg.rows.length > 0) return byMsg.rows[0].client_msg_id;
  const cand = await pool.query<{ client_msg_id: string }>(
    `SELECT client_msg_id FROM outbound_messages
     WHERE group_id = $1 AND account_id IN (SELECT id FROM accounts WHERE platform_user_id = $2)
       AND gateway_msg_id IS NULL AND delivery_status IN ('queued', 'accepted', 'unknown')
     ORDER BY created_at ASC LIMIT 1`,
    [groupId, sender],
  );
  if (cand.rows.length === 0) return null;
  const clientMsgId = cand.rows[0].client_msg_id;
  await pool.query(
    `UPDATE outbound_messages SET delivery_status = 'sent', gateway_msg_id = $2, gateway_sent_at = $3
     WHERE client_msg_id = $1`,
    [clientMsgId, msgId, sentAt],
  );
  return clientMsgId;
}

// message：未知群记 inconsistency 跳过；(groupId,msgId) 幂等入库；
// 自发回流关联出站行（置 sent）且不触发 agent；非 own 触发 agent 并推 WS。
async function handleMessage(d: Record<string, unknown>): Promise<void> {
  const pool = getPool();
  const gatewayGroupId = typeof d.groupId === 'string' ? d.groupId : undefined;
  const msgId = typeof d.msgId === 'string' ? d.msgId : undefined;
  const sender = typeof d.senderPlatformUserId === 'string' ? d.senderPlatformUserId : undefined;
  const text = typeof d.text === 'string' ? d.text : '';
  const sentAt = typeof d.sentAt === 'string' ? d.sentAt : undefined;
  if (!gatewayGroupId || !msgId || !sender || !sentAt) {
    await emitInconsistency('MALFORMED_EVENT', gatewayGroupId ?? '-', 'message 事件缺字段，已跳过');
    return;
  }
  const groupId = await groupIdByGateway(gatewayGroupId);
  if (!groupId) {
    await emitInconsistency('UNKNOWN_GROUP', gatewayGroupId, `未知群的消息 ${msgId}，已跳过`);
    return;
  }
  const isOwn = (await serviceAccountIds()).has(sender);
  const clientMsgId = isOwn ? await linkOwnOutbound(groupId, sender, msgId, sentAt) : null;
  // client_msg_id 回填：时间线把回流与出站行合并为一行。
  const ins = await pool.query(
    `INSERT INTO inbound_messages(group_id, msg_id, sender_platform_user_id, text, sent_at, is_own, client_msg_id)
     VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (group_id, msg_id) DO NOTHING`,
    [groupId, msgId, sender, text, sentAt, isOwn, clientMsgId],
  );
  if ((ins.rowCount ?? 0) === 0) {
    // 重复投递：inbound 已有。若本次找回了 clientMsgId 而入库行没有，回填对齐。
    if (clientMsgId) {
      await pool.query(
        'UPDATE inbound_messages SET client_msg_id = $2 WHERE group_id = $1 AND msg_id = $3 AND client_msg_id IS NULL',
        [groupId, clientMsgId, msgId],
      );
    }
    return; // 不再触发、不再推送。
  }
  if (isOwn) {
    await emit('message', { groupId, msgId, isOwn: true });
    return;
  }
  // 非 own：触发 agent（agent.ts 未落地时跳过），再推 WS。
  try {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const mod: unknown = require('./agent.js');
    if (typeof mod === 'object' && mod !== null && 'onInboundMessage' in mod) {
      const fn: unknown = mod.onInboundMessage;
      if (typeof fn === 'function') {
        // fn 已做 typeof 检查；签名按 agent.ts 约定断言（动态模块无法静态校验）。
        const onInbound = fn as OnInboundMessage;
        await onInbound(groupId, { msgId, senderPlatformUserId: sender, text, sentAt });
      }
    }
  } catch {
    // agent 未落地或触发失败：消息已入库，仅跳过触发。
  }
  await emit('message', { groupId, msgId, isOwn: false });
}

// message_sent：按 clientMsgId 置 sent；行不存在记 inconsistency。
async function handleMessageSent(d: Record<string, unknown>): Promise<void> {
  const clientMsgId = typeof d.clientMsgId === 'string' ? d.clientMsgId : undefined;
  const msgId = typeof d.msgId === 'string' ? d.msgId : undefined;
  const sentAt = typeof d.sentAt === 'string' ? d.sentAt : undefined;
  if (!clientMsgId || !msgId || !sentAt) {
    await emitInconsistency('MALFORMED_EVENT', clientMsgId ?? '-', 'message_sent 事件缺字段，已跳过');
    return;
  }
  const r = await getPool().query(
    `UPDATE outbound_messages SET delivery_status = 'sent', gateway_msg_id = $2, gateway_sent_at = $3
     WHERE client_msg_id = $1`,
    [clientMsgId, msgId, sentAt],
  );
  if ((r.rowCount ?? 0) === 0) {
    await emitInconsistency('OUTBOUND_NOT_FOUND', clientMsgId, `message_sent 无对应出站行 ${clientMsgId}`);
  }
}

// message_failed：按 A2 表收敛（终态/禁言/限流分别进对应副作用，其余记 failed）。
async function handleMessageFailed(d: Record<string, unknown>): Promise<void> {
  const pool = getPool();
  const clientMsgId = typeof d.clientMsgId === 'string' ? d.clientMsgId : undefined;
  const code = typeof d.code === 'string' ? d.code : 'UNKNOWN';
  if (!clientMsgId) {
    await emitInconsistency('MALFORMED_EVENT', '-', 'message_failed 事件缺 clientMsgId，已跳过');
    return;
  }
  const row = await pool.query<{ account_id: string; group_id: string }>(
    'SELECT account_id, group_id FROM outbound_messages WHERE client_msg_id = $1',
    [clientMsgId],
  );
  if (row.rows.length === 0) {
    await emitInconsistency('OUTBOUND_NOT_FOUND', clientMsgId, `message_failed 无对应出站行 ${clientMsgId}`);
    return;
  }
  const accountId = row.rows[0].account_id;
  const groupId = row.rows[0].group_id;
  if (code === 'ACCOUNT_SUSPENDED' || code === 'SESSION_EXPIRED') {
    // 终态副作用（含清成员、取消排队发送）由 applyTerminal 内复用，重复进入静默忽略。
    await applyTerminal(accountId, code === 'ACCOUNT_SUSPENDED' ? 'suspended' : 'session_expired');
    return;
  }
  if (code === 'GROUP_WRITE_FORBIDDEN') {
    await pool.query(
      `UPDATE outbound_messages SET delivery_status = 'failed', fail_code = $2 WHERE client_msg_id = $1`,
      [clientMsgId, code],
    );
    await markGroupUnreachable(groupId);
    return;
  }
  if (code === 'RATE_LIMITED') {
    const retryAfter =
      typeof d.retryAfterSeconds === 'number' && Number.isFinite(d.retryAfterSeconds) ? d.retryAfterSeconds : 60;
    await applyRateLimited(accountId, retryAfter);
    return; // 行保持 queued，到期后按原顺序发出。
  }
  await pool.query(
    `UPDATE outbound_messages SET delivery_status = 'failed', fail_code = $2 WHERE client_msg_id = $1`,
    [clientMsgId, code],
  );
}

// member_joined：服务账号落成员行（role 缺省 member，冲突忽略）；外部用户忽略。
async function handleMemberJoined(d: Record<string, unknown>): Promise<void> {
  const gatewayGroupId = typeof d.groupId === 'string' ? d.groupId : undefined;
  const platformUserId = typeof d.platformUserId === 'string' ? d.platformUserId : undefined;
  if (!gatewayGroupId || !platformUserId) return;
  notifyMembers({ gatewayGroupId, platformUserId, kind: 'joined' });
  const groupId = await groupIdByGateway(gatewayGroupId);
  if (!groupId) return; // 非本后端建的群，忽略。
  const acc = await getPool().query<{ id: string }>('SELECT id FROM accounts WHERE platform_user_id = $1', [
    platformUserId,
  ]);
  if (acc.rows.length === 0) return; // 外部用户，忽略。
  await getPool().query(
    `INSERT INTO group_members(group_id, account_id, platform_user_id, role)
     VALUES ($1, $2, $3, 'member') ON CONFLICT (group_id, account_id) DO NOTHING`,
    [groupId, acc.rows[0].id, platformUserId],
  );
}

// member_left：删对应服务账号成员行（外部用户本就没有行，自然无操作）。
async function handleMemberLeft(d: Record<string, unknown>): Promise<void> {
  const gatewayGroupId = typeof d.groupId === 'string' ? d.groupId : undefined;
  const platformUserId = typeof d.platformUserId === 'string' ? d.platformUserId : undefined;
  if (!gatewayGroupId || !platformUserId) return;
  notifyMembers({ gatewayGroupId, platformUserId, kind: 'left' });
  const groupId = await groupIdByGateway(gatewayGroupId);
  if (!groupId) return;
  await getPool().query('DELETE FROM group_members WHERE group_id = $1 AND platform_user_id = $2', [
    groupId,
    platformUserId,
  ]);
}

// account_status：相同状态静默忽略，否则走转移（含终态副作用）。
async function handleAccountStatus(d: Record<string, unknown>): Promise<void> {
  const accountId = typeof d.accountId === 'string' ? d.accountId : undefined;
  const to = typeof d.status === 'string' ? d.status : undefined;
  if (!accountId || (to !== 'suspended' && to !== 'session_expired')) return;
  const pool = getPool();
  const cur = await pool.query<{ status: string }>('SELECT status FROM accounts WHERE id = $1', [accountId]);
  if (cur.rows.length === 0) {
    await emitInconsistency('UNKNOWN_ACCOUNT', accountId, `account_status 账号不存在 ${accountId}`);
    return;
  }
  const from = cur.rows[0].status;
  if (from === to || from === 'suspended' || from === 'session_expired') return;
  const r = await applyAccountTransition(pool, accountId, to, from);
  if (!r.ok) {
    await emitInconsistency('ACCOUNT_TRANSITION_FAILED', accountId, `${from}→${to} 失败：${r.error}`);
  }
}

// 单事件分发：占坑去重 → 处理 → 成功推进 cursor；处理失败记 inconsistency 且不推进，
// 但不断开事件流（后继事件照常处理；cursor 冻结期间的重连会补拉重处理）。
async function dispatch(ev: GatewayEvent): Promise<boolean> {
  const pool = getPool();
  let claimed = false;
  try {
    const ins = await pool.query('INSERT INTO processed_gateway_events(event_id) VALUES ($1) ON CONFLICT DO NOTHING', [
      ev.id,
    ]);
    if ((ins.rowCount ?? 0) === 0) {
      await advanceCursor(ev.id); // 冲突跳过处理（崩溃前已占坑未推进时顺手补推进）。
      return true;
    }
    claimed = true;
    switch (ev.type) {
      case 'message':
        await handleMessage(ev.data);
        break;
      case 'message_sent':
        await handleMessageSent(ev.data);
        break;
      case 'message_failed':
        await handleMessageFailed(ev.data);
        break;
      case 'member_joined':
        await handleMemberJoined(ev.data);
        break;
      case 'member_left':
        await handleMemberLeft(ev.data);
        break;
      case 'account_status':
        await handleAccountStatus(ev.data);
        break;
      default:
        break; // 未知类型：占坑 + 推进，避免卡住流。
    }
  } catch (e) {
    // DB 写入失败：删占坑（不丢 event），记 inconsistency，不推进 cursor。
    if (claimed) {
      try {
        await pool.query('DELETE FROM processed_gateway_events WHERE event_id = $1', [ev.id]);
      } catch {
        // 删占坑也失败：重连补拉时靠 ON CONFLICT 跳过即可。
      }
    }
    await emitInconsistency('EVENT_PROCESS_FAILED', String(ev.id), e instanceof Error ? e.message : '网关事件处理失败');
    return false;
  }
  try {
    await advanceCursor(ev.id);
  } catch (e) {
    await emitInconsistency(
      'EVENT_PROCESS_FAILED',
      String(ev.id),
      e instanceof Error ? e.message : 'cursor 推进失败',
    );
    return false;
  }
  return true;
}

// 一次连接：GET /events?since=cursor，自解析 id:/event:/data: 帧；
// 正常 EOF 返回，连续 10 次处理失败抛错触发重连补拉。
async function runOnce(): Promise<void> {
  const base = (process.env.GATEWAY_URL ?? '').replace(/\/+$/, '');
  if (!base) throw new Error('缺少环境变量 GATEWAY_URL');
  const since = await readCursor();
  const ctrl = new AbortController();
  const res = await fetch(`${base}/events?since=${since}`, {
    headers: { Accept: 'text/event-stream' },
    signal: ctrl.signal,
  });
  if (!res.ok || !res.body) throw new Error(`SSE 连接失败：${res.status}`);
  // 空闲看门狗：60s 无字节主动断开重连（网关心跳 15s，正常不会触发）。
  let idleTimer: NodeJS.Timeout | undefined;
  const bump = (): void => {
    clearTimeout(idleTimer);
    idleTimer = setTimeout(() => ctrl.abort(), 60000);
  };
  bump();
  try {
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';
    let fieldId = '';
    let fieldEvent = '';
    let fieldData = '';
    let consecFail = 0;
    for (;;) {
      const { done: end, value } = await reader.read();
      if (end) break;
      bump();
      buf += decoder.decode(value, { stream: true });
      for (;;) {
        const sep = buf.indexOf('\n\n');
        if (sep < 0) break;
        const raw = buf.slice(0, sep);
        buf = buf.slice(sep + 2);
        for (const line of raw.split('\n')) {
          const l = line.endsWith('\r') ? line.slice(0, -1) : line;
          if (l.startsWith(':')) continue; // 心跳注释
          const colon = l.indexOf(':');
          if (colon < 0) continue;
          const k = l.slice(0, colon);
          let v = l.slice(colon + 1);
          if (v.startsWith(' ')) v = v.slice(1);
          if (k === 'id') fieldId = v;
          else if (k === 'event') fieldEvent = v;
          else if (k === 'data') fieldData = v;
        }
        if (fieldData !== '' || fieldEvent !== '' || fieldId !== '') {
          const idNum = Number(fieldId);
          // data 为网关 JSON 边界，整体断言后逐字段校验（命名断言，非内联成员访问）。
          let data: Record<string, unknown> = {};
          try {
            const parsed: unknown = JSON.parse(fieldData);
            if (typeof parsed === 'object' && parsed !== null) data = parsed as Record<string, unknown>;
          } catch {
            // 脏帧忽略，不计失败。
          }
          const eidRaw: unknown = data.eventId;
          const eid = typeof eidRaw === 'number' && Number.isInteger(eidRaw) ? eidRaw : idNum;
          const etypeRaw: unknown = data.type;
          const etype = typeof etypeRaw === 'string' && etypeRaw ? etypeRaw : fieldEvent;
          if (Number.isInteger(eid) && etype) {
            const ok = await dispatch({ id: eid, type: etype, data });
            consecFail = ok ? 0 : consecFail + 1;
            if (consecFail >= 10) throw new Error('连续事件处理失败，断开重连补拉');
          }
        }
        fieldId = '';
        fieldEvent = '';
        fieldData = '';
      }
    }
  } catch (e) {
    if (e instanceof Error && e.name === 'AbortError') return; // 看门狗/关闭：正常重连。
    throw e;
  } finally {
    clearTimeout(idleTimer);
    ctrl.abort();
  }
}

let running = false;

// 启动 SSE 消费（幂等）：断线指数退避重连，重连时以 cursor 补拉。
export function startSse(): void {
  if (running) return;
  running = true;
  void (async () => {
    let backoffMs = 1000;
    for (;;) {
      try {
        await runOnce();
        backoffMs = 1000;
        await new Promise<void>((resolve) => setTimeout(resolve, 1000)); // 服务端正常关闭：稍歇再连，避免热循环。
      } catch (e) {
        console.error('[sse] 连接断开，将重连：', e instanceof Error ? e.message : e);
        await new Promise<void>((resolve) => setTimeout(resolve, backoffMs));
        backoffMs = Math.min(backoffMs * 2, 30000);
      }
    }
  })();
}
