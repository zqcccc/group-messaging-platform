// mock-gateway：消息网关模拟服务（PORT=3101）
// 实现 test.md §2.1 全部端点与行为开关，供后端联调/演示使用。
// 说明：纯内存实现，重启后状态清空；事件 eventId 全局递增并保留全历史。
'use strict';

const express = require('express');
const { v4: uuidv4 } = require('uuid');

const PORT = Number(process.env.PORT || 3101);

// ---------------- 工具 ----------------

// [min,max] | number | undefined → 实际延迟毫秒
function resolveDelay(cfg, dflt) {
  if (cfg === undefined || cfg === null) cfg = dflt;
  if (typeof cfg === 'number') return Math.max(0, cfg);
  if (Array.isArray(cfg)) {
    const [a, b] = cfg;
    return Math.max(0, Math.floor(a + Math.random() * (b - a)));
  }
  return 0;
}

// 环境变量解析："500" → 500；"100,1500" → [100,1500]
function parseDelayEnv(v) {
  if (!v) return undefined;
  if (v.includes(',')) {
    const [a, b] = v.split(',').map(Number);
    if (Number.isFinite(a) && Number.isFinite(b)) return [a, b];
    return undefined;
  }
  const n = Number(v);
  return Number.isFinite(n) ? n : undefined;
}

function err(res, status, code, message, extra) {
  return res
    .status(status)
    .json({ error: { code, message, requestId: uuidv4(), ...(extra || {}) } });
}

// ---------------- 状态 ----------------

// 事件历史：{ eventId, type, data }，eventId 全局单调递增，保留全历史
let nextEventId = 0;
const eventHistory = [];
// SSE 长连接集合
const sseClients = new Set();
// 账号：id -> { id, platformUserId, online, terminal, rateLimitedUntil, rateLimitSeconds }
const accounts = new Map();
// 群：groupId -> { ownerAccountId, ownerLeft, writable, members: Map(pu -> {accountId, platformUserId, external}), admins: Set(pu), invites: Map(link -> {readyAt, expiresAt}) }
const groups = new Map();
// 消息：clientMsgId -> [rec...]（不去重，append，最早的在前）；msgId -> rec
const byClient = new Map();
const byMsgId = new Map();
// 媒体：id -> Buffer
const mediaStore = new Map();

// ---------------- 行为开关（环境变量或 POST /__control） ----------------
function defaultControl() {
  return {
    duplicateEvents: process.env.DUPLICATE_EVENTS === '1', // at-least-once：同一事件推两次（两个 eventId，内容相同）
    outOfOrder: process.env.OUT_OF_ORDER === '1', // 相邻事件乱序（ live 推送延迟 0-1000ms，历史保持有序）
    dropJoin: process.env.DROP_JOIN === '1', // join 受理后永不推 member_joined（账号并未入群）
    dropJoinAccounts: new Set(), // 指定账号 join 永不入群
    decisions: { accounts: {}, groups: {} }, // per account/group 强制错误，见下
    force503: false, // 所有端点 503
    byClientId503: false, // 仅 by-client-id 查询 503（验证"查询不可用期间保持 unknown、不重发"）
    forceSend504All: false, // 所有 send 走 504 未知结果路径
    forceSend504Accounts: new Set(), // 指定账号 send 走 504
    send504Lands: true, // 504 后消息是否在 2s 内落地（false 则永不落地）
    forceKick504: false, // kick 返回 504（2s 内收敛）
    failNextLeave: false, // 下一次 leave 返回 500（一次性）
    failSendOnce: [], // [{ accountId?, groupId?, code }] 下一次投递转 message_failed（一次性）
    kickDelayMs: parseDelayEnv(process.env.KICK_DELAY_MS) ?? [1000, 5000], // kick 响应延迟 1-5s
    joinDelayMs: parseDelayEnv(process.env.JOIN_DELAY_MS) ?? [100, 1500], // member_joined 延迟
    sendDelayMs: parseDelayEnv(process.env.SEND_DELAY_MS) ?? [50, 2000], // message_sent 延迟
    acceptDelayMs: parseDelayEnv(process.env.ACCEPT_DELAY_MS) ?? [0, 300], // 202 响应延迟（模拟一两秒可用 control 调大）
    inviteReadyMs: 'random', // number | [min,max] | 'random'(50% 为 0，否则 1-3s)
    inviteTtlMs: null, // 邀请链接存活毫秒，null 不过期
  };
}
let ctl = defaultControl();

// decisions 允许的 code：
//   账号：RATE_LIMITED(+retryAfterSeconds) / ACCOUNT_SUSPENDED / SESSION_EXPIRED /
//         SENDER_NOT_IN_GROUP(占位，账号级忽略) / ACCOUNT_OFFLINE / NETWORK_TIMEOUT / 503
//   群组：GROUP_WRITE_FORBIDDEN / SENDER_NOT_IN_GROUP / NETWORK_TIMEOUT / 503
function resetState() {
  nextEventId = 0;
  eventHistory.length = 0;
  accounts.clear();
  groups.clear();
  byClient.clear();
  byMsgId.clear();
  mediaStore.clear();
  ctl = defaultControl();
}

// ---------------- 事件推送 ----------------
function frameOf(e) {
  return `id: ${e.eventId}\nevent: ${e.type}\ndata: ${JSON.stringify(e.data)}\n\n`;
}

function broadcast(e) {
  const frame = frameOf(e);
  for (const res of sseClients) {
    const write = () => {
      try {
        res.write(frame);
      } catch {
        sseClients.delete(res);
      }
    };
    // 乱序开关：live 推送随机延迟 0-1000ms，相邻事件可能乱序到达；历史保持有序
    if (ctl.outOfOrder) setTimeout(write, Math.floor(Math.random() * 1000));
    else write();
  }
}

// 推一个事件：分配 eventId、记历史、广播。duplicateEvents 开启时同一内容推两次（两个 eventId）。
function pushEvent(type, payload) {
  const make = () => {
    const eventId = ++nextEventId;
    const e = { eventId, type, data: { eventId, type, ...payload } };
    eventHistory.push(e);
    return e;
  };
  broadcast(make());
  if (ctl.duplicateEvents) broadcast(make());
}

// ---------------- 账号/群辅助 ----------------
function accountOf(id, create) {
  let a = accounts.get(id);
  // 同一 accountId 每次 connect 返回同一 platformUserId：pu-<id>
  if (!a && create) {
    a = {
      id,
      platformUserId: `pu-${id}`,
      online: false,
      terminal: null, // 'suspended' | 'session_expired'
      rateLimitedUntil: 0,
      rateLimitSeconds: 0,
    };
    accounts.set(id, a);
  }
  return a;
}

function terminalError(a) {
  if (!a || !a.terminal) return null;
  if (a.terminal === 'suspended')
    return { status: 403, code: 'ACCOUNT_SUSPENDED', message: 'account suspended by platform' };
  return { status: 401, code: 'SESSION_EXPIRED', message: 'account session expired' };
}

// 进入终态：下线 + 移出所有群（推 member_left，owner 退群则标记 ownerLeft）
function setTerminal(a, status) {
  a.terminal = status;
  a.online = false;
  for (const [gid, g] of groups) {
    if (g.members.has(a.platformUserId)) {
      g.members.delete(a.platformUserId);
      g.admins.delete(a.platformUserId);
      if (g.ownerAccountId === a.id) g.ownerLeft = true;
      pushEvent('member_left', { groupId: gid, platformUserId: a.platformUserId });
    }
  }
}

// 账号级 decisions，返回 null 表示放行，否则返回 { status, code, message, extra?, timeout? }
function checkAccountDecision(a) {
  const d = ctl.decisions.accounts[a.id];
  if (!d) return null;
  switch (d.code) {
    case 'ACCOUNT_SUSPENDED':
      setTerminal(a, 'suspended');
      return { status: 403, code: 'ACCOUNT_SUSPENDED', message: 'account suspended by platform' };
    case 'SESSION_EXPIRED':
      setTerminal(a, 'session_expired');
      return { status: 401, code: 'SESSION_EXPIRED', message: 'account session expired' };
    case 'RATE_LIMITED': {
      const s = d.retryAfterSeconds ?? 60;
      a.rateLimitSeconds = s;
      a.rateLimitedUntil = Date.now() + s * 1000;
      return { status: 429, code: 'RATE_LIMITED', message: 'rate limited, retry later', extra: { retryAfterSeconds: s } };
    }
    case 'ACCOUNT_OFFLINE':
      return { status: 409, code: 'ACCOUNT_OFFLINE', message: 'account offline' };
    case 'NETWORK_TIMEOUT':
      return { timeout: true };
    case '503':
      return { status: 503, code: 'SERVICE_UNAVAILABLE', message: 'gateway temporarily unavailable' };
    default:
      return null;
  }
}

// 限流窗口内再次 send：同样 429 且计时重置
function checkRateWindow(a) {
  if (Date.now() < a.rateLimitedUntil) {
    const s = a.rateLimitSeconds || 60;
    a.rateLimitedUntil = Date.now() + s * 1000;
    const remain = Math.ceil((a.rateLimitedUntil - Date.now()) / 1000);
    return { status: 429, code: 'RATE_LIMITED', message: 'rate limited, retry later', extra: { retryAfterSeconds: remain } };
  }
  return null;
}

// 群级 decisions（send 路径），返回 null 表示放行
function checkGroupDecision(gid) {
  const d = ctl.decisions.groups[gid];
  if (!d) return null;
  switch (d.code) {
    case 'GROUP_WRITE_FORBIDDEN':
      return { status: 403, code: 'GROUP_WRITE_FORBIDDEN', message: 'group is not writable' };
    case 'SENDER_NOT_IN_GROUP':
      return { status: 403, code: 'SENDER_NOT_IN_GROUP', message: 'sender not in group' };
    case 'NETWORK_TIMEOUT':
      return { timeout: true };
    case '503':
      return { status: 503, code: 'SERVICE_UNAVAILABLE', message: 'gateway temporarily unavailable' };
    default:
      return null;
  }
}

// 消息落地：记两份索引 + 先推 message_sent 再推 message（含自己发出的回流）
function landMessage({ groupId, senderPlatformUserId, accountId, clientMsgId, text, mediaUrl }) {
  const msgId = `m-${uuidv4().slice(0, 8)}`;
  const sentAt = new Date().toISOString();
  const rec = { msgId, sentAt, groupId, accountId, clientMsgId, text };
  byMsgId.set(msgId, rec);
  if (!byClient.has(clientMsgId)) byClient.set(clientMsgId, []);
  byClient.get(clientMsgId).push(rec); // 不按 clientMsgId 去重
  pushEvent('message_sent', { groupId, accountId, clientMsgId, msgId, sentAt });
  const md = { groupId, msgId, senderPlatformUserId, text, sentAt };
  if (mediaUrl) md.mediaUrl = mediaUrl;
  pushEvent('message', md);
  return rec;
}

// 一次性 failSend 消费：匹配 accountId/groupId（指定了才比对）
function consumeFailSend(accountId, groupId) {
  const i = ctl.failSendOnce.findIndex(
    (f) => (!f.accountId || f.accountId === accountId) && (!f.groupId || f.groupId === groupId),
  );
  if (i < 0) return null;
  return ctl.failSendOnce.splice(i, 1)[0].code;
}

// ---------------- app ----------------
const app = express();
app.use(express.json());

app.get('/health', (_req, res) => res.json({ ok: true }));

// ---------- 账号 ----------
app.post('/accounts/:id/connect', (req, res) => {
  if (ctl.force503) return err(res, 503, 'SERVICE_UNAVAILABLE', 'gateway temporarily unavailable');
  const a = accountOf(req.params.id, true);
  const t = terminalError(a);
  if (t) return err(res, t.status, t.code, t.message);
  a.online = true;
  a.rateLimitedUntil = 0;
  return res.json({ platformUserId: a.platformUserId });
});

app.post('/accounts/:id/disconnect', (req, res) => {
  if (ctl.force503) return err(res, 503, 'SERVICE_UNAVAILABLE', 'gateway temporarily unavailable');
  const a = accountOf(req.params.id, true);
  const t = terminalError(a);
  if (t) return err(res, t.status, t.code, t.message);
  a.online = false;
  return res.json({});
});

// ---------- 建群 ----------
app.post('/groups', (req, res) => {
  if (ctl.force503) return err(res, 503, 'SERVICE_UNAVAILABLE', 'gateway temporarily unavailable');
  const { creatorAccountId } = req.body || {};
  if (!creatorAccountId) return err(res, 400, 'INVALID_INPUT', 'creatorAccountId is required');
  const a = accountOf(creatorAccountId, true);
  const t = terminalError(a);
  if (t) return err(res, t.status, t.code, t.message);
  const dec = checkAccountDecision(a);
  if (dec) {
    if (dec.timeout) return err(res, 504, 'NETWORK_TIMEOUT', 'network timeout');
    return err(res, dec.status, dec.code, dec.message, dec.extra);
  }
  if (!a.online) return err(res, 409, 'ACCOUNT_OFFLINE', 'creator account offline');
  // 创建者建群成功即为成员（群主），不推 member_joined
  const groupId = `g-${uuidv4().slice(0, 8)}`;
  groups.set(groupId, {
    ownerAccountId: creatorAccountId,
    ownerLeft: false,
    writable: true,
    members: new Map([[a.platformUserId, { accountId: creatorAccountId, platformUserId: a.platformUserId }]]),
    admins: new Set(),
    invites: new Map(),
  });
  return res.json({ groupId });
});

// ---------- 邀请链接 ----------
function makeInvite(g) {
  let readyAfterMs;
  const cfg = ctl.inviteReadyMs;
  if (typeof cfg === 'number') readyAfterMs = cfg;
  else if (Array.isArray(cfg)) readyAfterMs = Math.floor(cfg[0] + Math.random() * (cfg[1] - cfg[0]));
  else readyAfterMs = Math.random() < 0.5 ? 0 : Math.floor(1000 + Math.random() * 2000); // 0 或几秒
  const inviteLink = `inv-${uuidv4().slice(0, 8)}`;
  const now = Date.now();
  g.invites.set(inviteLink, {
    readyAt: now + readyAfterMs,
    expiresAt: ctl.inviteTtlMs ? now + ctl.inviteTtlMs : 0,
  });
  return { inviteLink, readyAfterMs };
}

app.post('/groups/:id/invite', (req, res) => {
  if (ctl.force503) return err(res, 503, 'SERVICE_UNAVAILABLE', 'gateway temporarily unavailable');
  const g = groups.get(req.params.id);
  if (!g) return err(res, 404, 'GROUP_NOT_FOUND', 'group not found');
  if (!g.writable) return err(res, 403, 'GROUP_WRITE_FORBIDDEN', 'group is not writable');
  return res.json(makeInvite(g));
});

// ---------- 入群 ----------
app.post('/groups/:id/join', (req, res) => {
  if (ctl.force503) return err(res, 503, 'SERVICE_UNAVAILABLE', 'gateway temporarily unavailable');
  const g = groups.get(req.params.id);
  if (!g) return err(res, 404, 'GROUP_NOT_FOUND', 'group not found');
  const { accountId, inviteLink } = req.body || {};
  if (!accountId) return err(res, 400, 'INVALID_INPUT', 'accountId is required');
  const a = accountOf(accountId, true);
  const t = terminalError(a);
  if (t) return err(res, t.status, t.code, t.message);
  const dec = checkAccountDecision(a);
  if (dec) {
    if (dec.timeout) return err(res, 504, 'NETWORK_TIMEOUT', 'network timeout');
    return err(res, dec.status, dec.code, dec.message, dec.extra);
  }
  if (!a.online) return err(res, 409, 'ACCOUNT_OFFLINE', 'account offline');
  // 链接校验：未知/过期 → 410；未到 readyAt → 409
  const inv = inviteLink && g.invites.get(inviteLink);
  const now = Date.now();
  if (!inv || (inv.expiresAt && now > inv.expiresAt)) {
    if (inv) g.invites.delete(inviteLink);
    return err(res, 410, 'INVITE_EXPIRED', 'invite link expired, request a new one');
  }
  if (now < inv.readyAt) return err(res, 409, 'INVITE_NOT_READY', 'invite link not ready yet');
  if (g.members.has(a.platformUserId)) return err(res, 409, 'ALREADY_MEMBER', 'already a member');
  res.status(202).json({ accepted: true });
  // 202 受理后 100-1500ms 推 member_joined；dropJoin 时永不推（账号并未入群）
  const noJoin = ctl.dropJoin || ctl.dropJoinAccounts.has(accountId);
  if (noJoin) return;
  setTimeout(() => {
    const gg = groups.get(req.params.id);
    if (!gg || gg.members.has(a.platformUserId)) return;
    // 成员列表先变，事件随后推
    gg.members.set(a.platformUserId, { accountId, platformUserId: a.platformUserId });
    pushEvent('member_joined', { groupId: req.params.id, platformUserId: a.platformUserId });
  }, resolveDelay(ctl.joinDelayMs, [100, 1500]));
});

// ---------- 提升管理员 ----------
app.post('/groups/:id/promote', (req, res) => {
  if (ctl.force503) return err(res, 503, 'SERVICE_UNAVAILABLE', 'gateway temporarily unavailable');
  const g = groups.get(req.params.id);
  if (!g) return err(res, 404, 'GROUP_NOT_FOUND', 'group not found');
  const { byAccountId, accountId } = req.body || {};
  if (!byAccountId || !accountId) return err(res, 400, 'INVALID_INPUT', 'byAccountId and accountId are required');
  const by = accountOf(byAccountId, true);
  const tb = terminalError(by);
  if (tb) return err(res, tb.status, tb.code, tb.message);
  if (!by.online) return err(res, 409, 'ACCOUNT_OFFLINE', 'account offline');
  if (g.ownerLeft || !g.members.has(`pu-${g.ownerAccountId}`))
    return err(res, 409, 'OWNER_LEFT', 'group owner has left');
  if (byAccountId !== g.ownerAccountId) return err(res, 403, 'NO_PERMISSION', 'only group owner can promote');
  const targetPu = `pu-${accountId}`;
  if (!g.members.has(targetPu))
    return err(res, 409, 'NOT_MEMBER_YET', 'target has not joined yet');
  g.admins.add(targetPu);
  return res.json({});
});

// ---------- 移出成员 ----------
app.post('/groups/:id/kick', (req, res) => {
  if (ctl.force503) return err(res, 503, 'SERVICE_UNAVAILABLE', 'gateway temporarily unavailable');
  const g = groups.get(req.params.id);
  if (!g) return err(res, 404, 'GROUP_NOT_FOUND', 'group not found');
  const { byAccountId, targetPlatformUserId } = req.body || {};
  if (!byAccountId || !targetPlatformUserId)
    return err(res, 400, 'INVALID_INPUT', 'byAccountId and targetPlatformUserId are required');
  const by = accountOf(byAccountId, true);
  const tb = terminalError(by);
  if (tb) return err(res, tb.status, tb.code, tb.message);
  if (!by.online) return err(res, 409, 'ACCOUNT_OFFLINE', 'account offline');
  if (g.ownerLeft || !g.members.has(`pu-${g.ownerAccountId}`))
    return err(res, 409, 'OWNER_LEFT', 'group owner has left');
  const byMember = [...g.members.values()].find((m) => m.accountId === byAccountId);
  const byPu = `pu-${byAccountId}`;
  if (!byMember || (byAccountId !== g.ownerAccountId && !g.admins.has(byPu)))
    return err(res, 403, 'NO_PERMISSION', 'no permission to kick');
  if (!g.members.has(targetPlatformUserId))
    return err(res, 404, 'NOT_MEMBER', 'target is not a member');
  // kick 响应 1-5s；目标在 200 返回前已从成员列表移除，随后推 member_left
  const finish = () => {
    const gg = groups.get(req.params.id);
    if (!gg) return;
    gg.members.delete(targetPlatformUserId);
    gg.admins.delete(targetPlatformUserId);
    if (targetPlatformUserId === `pu-${gg.ownerAccountId}`) gg.ownerLeft = true;
    pushEvent('member_left', { groupId: req.params.id, platformUserId: targetPlatformUserId });
  };
  if (ctl.forceKick504) {
    // 504 未知结果：2s 内收敛（成员列表可查），随后推 member_left；先回 504
    setTimeout(finish, Math.floor(Math.random() * 2000));
    setTimeout(() => err(res, 504, 'NETWORK_TIMEOUT', 'network timeout'), 500);
    return;
  }
  setTimeout(() => {
    finish();
    res.json({ kicked: true });
  }, resolveDelay(ctl.kickDelayMs, [1000, 5000]));
});

// ---------- 退群 ----------
app.post('/groups/:id/leave', (req, res) => {
  if (ctl.force503) return err(res, 503, 'SERVICE_UNAVAILABLE', 'gateway temporarily unavailable');
  const g = groups.get(req.params.id);
  if (!g) return err(res, 404, 'GROUP_NOT_FOUND', 'group not found');
  const { accountId } = req.body || {};
  if (!accountId) return err(res, 400, 'INVALID_INPUT', 'accountId is required');
  const a = accountOf(accountId, true);
  const t = terminalError(a);
  if (t) return err(res, t.status, t.code, t.message);
  if (!a.online) return err(res, 409, 'ACCOUNT_OFFLINE', 'account offline');
  if (ctl.failNextLeave) {
    ctl.failNextLeave = false;
    return err(res, 500, 'LEAVE_FAILED', 'leave failed, not left');
  }
  if (g.members.has(a.platformUserId)) {
    g.members.delete(a.platformUserId);
    g.admins.delete(a.platformUserId);
    if (g.ownerAccountId === accountId) g.ownerLeft = true;
    pushEvent('member_left', { groupId: req.params.id, platformUserId: a.platformUserId });
  }
  return res.json({});
});

// ---------- 成员列表 ----------
app.get('/groups/:id/members', (req, res) => {
  if (ctl.force503) return err(res, 503, 'SERVICE_UNAVAILABLE', 'gateway temporarily unavailable');
  const g = groups.get(req.params.id);
  if (!g) return err(res, 404, 'GROUP_NOT_FOUND', 'group not found');
  return res.json([...g.members.keys()].map((platformUserId) => ({ platformUserId })));
});

// ---------- 发消息 ----------
app.post('/groups/:id/send', (req, res) => {
  if (ctl.force503) return err(res, 503, 'SERVICE_UNAVAILABLE', 'gateway temporarily unavailable');
  const gid = req.params.id;
  const g = groups.get(gid);
  if (!g) return err(res, 404, 'GROUP_NOT_FOUND', 'group not found');
  const { accountId, clientMsgId, text } = req.body || {};
  if (!accountId || !clientMsgId || typeof text !== 'string')
    return err(res, 400, 'INVALID_INPUT', 'accountId, clientMsgId and text are required');
  const a = accountOf(accountId, true);
  const t = terminalError(a);
  if (t) return err(res, t.status, t.code, t.message);
  const dec = checkAccountDecision(a);
  if (dec) {
    if (dec.timeout) return respondSendTimeout();
    return err(res, dec.status, dec.code, dec.message, dec.extra);
  }
  const rw = checkRateWindow(a);
  if (rw) return err(res, rw.status, rw.code, rw.message, rw.extra);
  if (!a.online) return err(res, 409, 'ACCOUNT_OFFLINE', 'account offline');
  if (!g.members.has(a.platformUserId))
    return err(res, 403, 'SENDER_NOT_IN_GROUP', 'sender not in group');
  const gd = checkGroupDecision(gid);
  if (gd) {
    if (gd.timeout) return respondSendTimeout();
    return err(res, gd.status, gd.code, gd.message, gd.extra);
  }
  if (!g.writable) return err(res, 403, 'GROUP_WRITE_FORBIDDEN', 'group is not writable');
  if (ctl.forceSend504All || ctl.forceSend504Accounts.has(accountId)) return respondSendTimeout();

  // 504 未知结果路径：消息可能已接收；接收了则 2s 内落地并推 message_sent，
  // 2s 后 by-client-id 仍 404 即可确定没有发出
  function respondSendTimeout() {
    setTimeout(() => err(res, 504, 'NETWORK_TIMEOUT', 'network timeout'), 300);
    if (ctl.send504Lands) {
      setTimeout(() => {
        if (!groups.has(gid)) return;
        landMessage({ groupId: gid, senderPlatformUserId: a.platformUserId, accountId, clientMsgId, text });
      }, Math.floor(500 + Math.random() * 1500));
    }
  }

  // 202 受理（本身可能延迟一两秒），50-2000ms 后推 message_sent 或 message_failed
  setTimeout(() => {
    try {
      res.status(202).json({ accepted: true });
    } catch {
      return;
    }
    setTimeout(() => {
      const gg = groups.get(gid);
      const aa = accountOf(accountId);
      const tt = terminalError(aa);
      if (!gg || !gg.writable) {
        pushEvent('message_failed', { groupId: gid, clientMsgId, code: 'GROUP_WRITE_FORBIDDEN' });
        return;
      }
      if (tt) {
        pushEvent('message_failed', { groupId: gid, clientMsgId, code: tt.code });
        return;
      }
      const failCode = consumeFailSend(accountId, gid);
      if (failCode) {
        pushEvent('message_failed', { groupId: gid, clientMsgId, code: failCode });
        return;
      }
      landMessage({ groupId: gid, senderPlatformUserId: a.platformUserId, accountId, clientMsgId, text });
    }, resolveDelay(ctl.sendDelayMs, [50, 2000]));
  }, resolveDelay(ctl.acceptDelayMs, [0, 300]));
});

// ---------- 按 clientMsgId 查询 ----------
app.get('/groups/:id/messages/by-client-id/:cid', (req, res) => {
  if (ctl.force503 || ctl.byClientId503) return err(res, 503, 'SERVICE_UNAVAILABLE', 'gateway temporarily unavailable');
  if (!groups.has(req.params.id)) return err(res, 404, 'GROUP_NOT_FOUND', 'group not found');
  const list = byClient.get(req.params.cid);
  if (!list || list.length === 0) return err(res, 404, 'MESSAGE_NOT_FOUND', 'message not landed yet');
  // 同一 clientMsgId 多条时返回最早的一条
  const first = list[0];
  return res.json({ msgId: first.msgId, sentAt: first.sentAt });
});

// ---------- 媒体 ----------
app.get('/media/:id', (req, res) => {
  if (ctl.force503) return err(res, 503, 'SERVICE_UNAVAILABLE', 'gateway temporarily unavailable');
  const buf = mediaStore.get(req.params.id);
  if (!buf) return err(res, 404, 'MEDIA_EXPIRED', 'media expired or not found');
  res.type('application/octet-stream').send(buf);
});

// ---------- 事件流 SSE ----------
// 帧格式：id: <eventId> / event: <type> / data: <JSON> 三行；data 里带 eventId 与 type。
// since 独占（返回 eventId > since 的事件）；不带 since 从当前时刻开始。
app.get('/events', (req, res) => {
  if (ctl.force503) return err(res, 503, 'SERVICE_UNAVAILABLE', 'gateway temporarily unavailable');
  let startId;
  if (req.query.since === undefined) {
    startId = nextEventId;
  } else {
    startId = Number(req.query.since);
    if (!Number.isInteger(startId) || startId < 0)
      return err(res, 400, 'INVALID_INPUT', 'since must be a non-negative integer');
  }
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
    'X-Accel-Buffering': 'no',
  });
  res.write(': connected\n\n');
  // 补拉全历史（保留全部，支持断线重连）
  for (const e of eventHistory) {
    if (e.eventId > startId) res.write(frameOf(e));
  }
  sseClients.add(res);
  const hb = setInterval(() => {
    try {
      res.write(': hb\n\n');
    } catch {
      clearInterval(hb);
      sseClients.delete(res);
    }
  }, 15000);
  req.on('close', () => {
    clearInterval(hb);
    sseClients.delete(res);
  });
});

// ---------------- 测试控制面 ----------------
// POST /__control：行为开关与故障注入（字段全可选，patch 语义）：
// {
//   duplicateEvents, outOfOrder, dropJoin: bool,
//   dropJoinAccounts: ["acc-1"], forceSend504Accounts: ["acc-1"],
//   decisions: { accounts: { "acc-1": { code, retryAfterSeconds? } | null },
//                groups: { "<gid>": { code } | null } },
//   rateLimit: { accountId, seconds },            // 立即限流该账号
//   failSend: { accountId?, groupId?, code },     // 下一次投递转 message_failed（一次性）
//   forceSend504All, forceKick504, send504Lands, force503, byClientId503, failNextLeave: bool,
//   kickDelayMs, joinDelayMs, sendDelayMs, acceptDelayMs: number | [min,max],
//   inviteReadyMs: number | [min,max] | "random", inviteTtlMs: number | null,
//   setWritable: { groupId, writable },           // 群禁言/解散模拟
//   accountStatus: { accountId, status },         // 推 account_status 并移出所有群
//   externalJoin: { groupId, platformUserId },    // 外部用户入群
//   externalLeave: { groupId, platformUserId },   // 外部用户退群
//   externalMessage: { groupId, senderPlatformUserId, text, withMedia? }, // 外部消息（含 mediaUrl）
//   expireMedia: "<id>", expireInvite: "<link>",  // 使媒体/链接过期
//   redeliver: { accountId, groupId? }            // 补投：新 eventId + 原 msgId/sentAt 重推 message
// }
app.post('/__control', (req, res) => {
  const b = req.body || {};
  const setBool = (k) => {
    if (k in b) ctl[k] = !!b[k];
  };
  setBool('duplicateEvents');
  setBool('outOfOrder');
  setBool('dropJoin');
  setBool('force503');
  setBool('byClientId503');
  setBool('forceSend504All');
  setBool('forceKick504');
  setBool('send504Lands');
  setBool('failNextLeave');
  for (const k of ['kickDelayMs', 'joinDelayMs', 'sendDelayMs', 'acceptDelayMs', 'inviteReadyMs', 'inviteTtlMs']) {
    if (k in b) ctl[k] = b[k];
  }
  if (Array.isArray(b.dropJoinAccounts)) ctl.dropJoinAccounts = new Set(b.dropJoinAccounts);
  if (Array.isArray(b.forceSend504Accounts)) ctl.forceSend504Accounts = new Set(b.forceSend504Accounts);
  if (b.decisions) {
    for (const scope of ['accounts', 'groups']) {
      if (b.decisions[scope]) {
        for (const [k, v] of Object.entries(b.decisions[scope])) {
          if (v === null) delete ctl.decisions[scope][k];
          else ctl.decisions[scope][k] = v;
        }
      }
    }
  }
  if (b.rateLimit) {
    const a = accountOf(b.rateLimit.accountId, true);
    const s = b.rateLimit.seconds ?? 60;
    a.rateLimitSeconds = s;
    a.rateLimitedUntil = Date.now() + s * 1000;
  }
  if (b.failSend) ctl.failSendOnce.push(b.failSend);
  if (b.setWritable) {
    const g = groups.get(b.setWritable.groupId);
    if (g) g.writable = !!b.setWritable.writable;
  }
  if (b.accountStatus) {
    const a = accountOf(b.accountStatus.accountId, true);
    const st = b.accountStatus.status;
    if (st === 'suspended' || st === 'session_expired') {
      setTerminal(a, st);
      pushEvent('account_status', { accountId: a.id, status: st });
    }
  }
  if (b.externalJoin) {
    const g = groups.get(b.externalJoin.groupId);
    if (g && !g.members.has(b.externalJoin.platformUserId)) {
      g.members.set(b.externalJoin.platformUserId, { accountId: null, platformUserId: b.externalJoin.platformUserId, external: true });
      pushEvent('member_joined', { groupId: b.externalJoin.groupId, platformUserId: b.externalJoin.platformUserId });
    }
  }
  if (b.externalLeave) {
    const g = groups.get(b.externalLeave.groupId);
    if (g && g.members.has(b.externalLeave.platformUserId)) {
      g.members.delete(b.externalLeave.platformUserId);
      pushEvent('member_left', { groupId: b.externalLeave.groupId, platformUserId: b.externalLeave.platformUserId });
    }
  }
  if (b.externalMessage) {
    const m = b.externalMessage;
    if (groups.has(m.groupId)) {
      let mediaUrl;
      if (m.withMedia) {
        const mid = `med-${uuidv4().slice(0, 8)}`;
        mediaStore.set(mid, Buffer.from(`media-bytes-for-${mid}`));
        mediaUrl = `http://127.0.0.1:${PORT}/media/${mid}`;
      }
      landMessage({
        groupId: m.groupId,
        senderPlatformUserId: m.senderPlatformUserId || 'ext-user',
        accountId: null,
        clientMsgId: `ext-${uuidv4().slice(0, 8)}`,
        text: m.text || 'hello',
        mediaUrl,
      });
    }
  }
  if (b.expireMedia) mediaStore.delete(b.expireMedia);
  if (b.expireInvite) {
    for (const g of groups.values()) g.invites.delete(b.expireInvite);
  }
  // 补投：离线账号之前发过的消息以新 eventId + 原 msgId/sentAt 重推（sentAt 可能比已收消息早任意时长）
  if (b.redeliver) {
    for (const rec of byMsgId.values()) {
      if (rec.accountId !== b.redeliver.accountId) continue;
      if (b.redeliver.groupId && rec.groupId !== b.redeliver.groupId) continue;
      pushEvent('message', {
        groupId: rec.groupId,
        msgId: rec.msgId,
        senderPlatformUserId: `pu-${rec.accountId}`,
        text: rec.text,
        sentAt: rec.sentAt,
      });
    }
  }
  return res.json({ ok: true, control: publicControl() });
});

function publicControl() {
  return {
    ...ctl,
    dropJoinAccounts: [...ctl.dropJoinAccounts],
    forceSend504Accounts: [...ctl.forceSend504Accounts],
  };
}

app.get('/__control', (_req, res) => {
  res.json({ control: publicControl(), stats: { accounts: accounts.size, groups: groups.size, events: nextEventId } });
});

// 一键清空（测试隔离用）
app.post('/__reset', (_req, res) => {
  resetState();
  res.json({ ok: true });
});

app.use((req, res) => err(res, 404, 'NOT_FOUND', `no such endpoint: ${req.method} ${req.path}`));

const server = app.listen(PORT, () => {
  console.log(`[mock-gateway] listening on :${PORT}`);
});
process.on('SIGTERM', () => server.close(() => process.exit(0)));
