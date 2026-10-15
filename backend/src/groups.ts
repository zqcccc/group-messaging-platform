// 群组面：建群 job、leave-all job、jobs 查询、群查询/PATCH、群内 send。
// 网关调用走本文件最小客户端（与 gateway.ts 同错误形状，落地后可整体替换 gwFetch 一层）。
// 成员事件等待以轮询网关 members 为主；sse.ts 落地后顺带订阅做提前唤醒（动态加载，不存在即走纯轮询）。
import { Request, Response, Router } from 'express';
import { v4 as uuidv4 } from 'uuid';
import { requireAdmin, requireAuth } from './auth';
import { getPool } from './db';
import { asyncHandler, sendError } from './util';

// ---------- 网关最小客户端 ----------

// 网关失败（HTTP 非 2xx 或不可达），code 取自 { error: { code } } / { code }。
class GwError extends Error {
  httpStatus: number;
  code: string;
  retryAfterSeconds?: number;
  constructor(httpStatus: number, code: string, message: string, retryAfterSeconds?: number) {
    super(message);
    this.httpStatus = httpStatus;
    this.code = code;
    this.retryAfterSeconds = retryAfterSeconds;
  }
}

// 网关错误码：兼容 { error: { code } } 与 { code } 两种形状。
function gwBodyCode(data: unknown): string | undefined {
  if (typeof data !== 'object' || data === null) return undefined;
  if ('code' in data && typeof data.code === 'string') return data.code;
  if ('error' in data && typeof data.error === 'object' && data.error !== null) {
    const inner = data.error;
    if ('code' in inner && typeof inner.code === 'string') return inner.code;
  }
  return undefined;
}

// 429 携带的 retryAfterSeconds（在 { error: {...} } 里）。
function readRetryAfterSeconds(data: unknown): number | undefined {
  if (typeof data !== 'object' || data === null || !('error' in data)) return undefined;
  const inner = data.error;
  if (typeof inner !== 'object' || inner === null || !('retryAfterSeconds' in inner)) return undefined;
  return typeof inner.retryAfterSeconds === 'number' ? inner.retryAfterSeconds : undefined;
}

// 网关请求：默认 15s 超时；非 2xx 抛 GwError；连不上抛 code=GATEWAY_UNREACHABLE 的 GwError。
async function gwFetch(method: string, path: string, body?: unknown, timeoutMs = 15000): Promise<unknown> {
  const base = (process.env.GATEWAY_URL ?? '').replace(/\/+$/, '');
  if (!base) throw new GwError(0, 'GATEWAY_NOT_CONFIGURED', '缺少环境变量 GATEWAY_URL');
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(base + path, {
      method,
      headers: { 'content-type': 'application/json' },
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: ctrl.signal,
    });
    const data: unknown = await res.json().catch(() => null);
    if (!res.ok) {
      const code = gwBodyCode(data) ?? `HTTP_${res.status}`;
      throw new GwError(res.status, code, `网关请求失败 ${method} ${path}: ${code}`, readRetryAfterSeconds(data));
    }
    return data;
  } catch (e) {
    if (e instanceof GwError) throw e;
    const detail = e instanceof Error ? e.message : '未知错误';
    throw new GwError(0, 'GATEWAY_UNREACHABLE', `网关不可达 ${method} ${path}: ${detail}`);
  } finally {
    clearTimeout(timer);
  }
}

// 建群响应提取 groupId（形状不对抛 BAD_GATEWAY_RESPONSE，调用方记 step=create）。
function parseGroupId(data: unknown): string {
  if (typeof data === 'object' && data !== null && 'groupId' in data) {
    if (typeof data.groupId === 'string' && data.groupId.length > 0) return data.groupId;
  }
  throw new GwError(200, 'BAD_GATEWAY_RESPONSE', '建群响应缺 groupId');
}

// 邀请响应提取链接与就绪延迟。
function parseInvite(data: unknown): { inviteLink: string; readyAfterMs: number } {
  if (typeof data === 'object' && data !== null && 'inviteLink' in data) {
    if (typeof data.inviteLink === 'string' && data.inviteLink.length > 0) {
      const raw = 'readyAfterMs' in data ? data.readyAfterMs : 0;
      return { inviteLink: data.inviteLink, readyAfterMs: typeof raw === 'number' && raw > 0 ? raw : 0 };
    }
  }
  throw new GwError(200, 'BAD_GATEWAY_RESPONSE', '邀请响应缺 inviteLink');
}

// 成员列表提取 platformUserId 数组（非数组抛 BAD_GATEWAY_RESPONSE，单条坏数据跳过）。
function parseMembers(data: unknown): string[] {
  if (!Array.isArray(data)) throw new GwError(200, 'BAD_GATEWAY_RESPONSE', '成员列表响应非法');
  const out: string[] = [];
  for (const item of data) {
    if (typeof item === 'object' && item !== null && 'platformUserId' in item) {
      if (typeof item.platformUserId === 'string') out.push(item.platformUserId);
    }
  }
  return out;
}

async function gwCreateGroup(creatorAccountId: string): Promise<string> {
  return parseGroupId(await gwFetch('POST', '/groups', { creatorAccountId }));
}

async function gwInvite(gatewayGroupId: string): Promise<{ inviteLink: string; readyAfterMs: number }> {
  return parseInvite(await gwFetch('POST', `/groups/${gatewayGroupId}/invite`, {}));
}

async function gwJoin(gatewayGroupId: string, accountId: string, inviteLink: string): Promise<void> {
  await gwFetch('POST', `/groups/${gatewayGroupId}/join`, { accountId, inviteLink });
}

async function gwPromote(gatewayGroupId: string, byAccountId: string, accountId: string): Promise<void> {
  await gwFetch('POST', `/groups/${gatewayGroupId}/promote`, { byAccountId, accountId });
}

async function gwLeave(gatewayGroupId: string, accountId: string): Promise<void> {
  await gwFetch('POST', `/groups/${gatewayGroupId}/leave`, { accountId });
}

async function gwMembers(gatewayGroupId: string): Promise<string[]> {
  return parseMembers(await gwFetch('GET', `/groups/${gatewayGroupId}/members`));
}

function sleep(ms: number): Promise<void> {
  return new Promise<void>((resolve) => setTimeout(resolve, ms));
}

// catch 捕获的未知错误转网关业务码（GwError 与 gateway.ts 的 GatewayError 都有 code 字段）。
function thrownCode(e: unknown): string | undefined {
  if (e instanceof GwError) return e.code;
  if (typeof e === 'object' && e !== null && 'code' in e && typeof e.code === 'string') return e.code;
  return undefined;
}

// ---------- 可选的外部模块（存在即用，不存在走本地兜底，保证随时可构建运行） ----------

// sse 成员事件（与 sse.ts 导出形状一致）。
interface SseMemberEvent {
  gatewayGroupId: string;
  platformUserId: string;
  kind: string;
}

type SseSubscribeFn = (fn: (ev: SseMemberEvent) => void) => () => void;

// sse.ts 动态加载：文件不存在时抛错，调用方捕获后走纯轮询（保持本模块独立可构建）。
// eslint-disable-next-line @typescript-eslint/no-require-imports
function loadSseModule(): unknown {
  return require('./sse.js');
}

// 已确认为函数的 subscribeMemberEvents 转为内部签名（动态模块形参无法再做 schema 校验）。
function asSseSubscribe(mod: object): SseSubscribeFn | null {
  if (!('subscribeMemberEvents' in mod) || typeof mod.subscribeMemberEvents !== 'function') return null;
  const subscribe = mod.subscribeMemberEvents as unknown as SseSubscribeFn;
  return subscribe;
}

// sse 订阅转内部唤醒回调；加载失败/签名不符返回 null，调用方走纯轮询。
function subscribeMemberWakeup(
  cb: (gatewayGroupId: string, platformUserId: string, kind: 'joined' | 'left') => void,
): (() => void) | null {
  let mod: unknown;
  try {
    mod = loadSseModule();
  } catch {
    return null;
  }
  if (typeof mod !== 'object' || mod === null) return null;
  const subscribe = asSseSubscribe(mod);
  if (!subscribe) return null;
  try {
    const off: unknown = subscribe((ev) => {
      if (
        typeof ev.gatewayGroupId === 'string' &&
        typeof ev.platformUserId === 'string' &&
        (ev.kind === 'joined' || ev.kind === 'left')
      ) {
        cb(ev.gatewayGroupId, ev.platformUserId, ev.kind);
      }
    });
    if (typeof off !== 'function') return null;
    const unsub = off as unknown as () => void;
    return unsub;
  } catch {
    return null;
  }
}

// 等网关成员出现/消失：轮询 members 为主（网关先改列表后推事件，轮询最可靠），
// sse 订阅命中则提前复核一次；超时返回 false，由调用方记 JOIN_TIMEOUT。
async function waitForGatewayMember(
  gatewayGroupId: string,
  platformUserId: string,
  present: boolean,
  timeoutMs: number,
): Promise<boolean> {
  let hinted = false;
  const unsub = subscribeMemberWakeup((gid, pu, kind) => {
    if (gid === gatewayGroupId && pu === platformUserId && (kind === 'joined') === present) hinted = true;
  });
  try {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      try {
        const members = await gwMembers(gatewayGroupId);
        if (members.includes(platformUserId) === present) return true;
      } catch {
        // 网关抖动时继续等，超时了再判失败。
      }
      if (hinted) {
        hinted = false;
        try {
          const members = await gwMembers(gatewayGroupId);
          if (members.includes(platformUserId) === present) return true;
        } catch {
          // 忽略，继续轮询。
        }
      }
      if (Date.now() >= deadline) return false;
      await sleep(250);
    }
  } finally {
    try {
      unsub?.();
    } catch {
      // 取消订阅失败不影响结果。
    }
  }
}

// ---------- jobs 存取 ----------

async function createJob(kind: string, payload: Record<string, unknown>): Promise<string> {
  const id = uuidv4();
  await getPool().query('INSERT INTO jobs(id, kind, status, errors, payload) VALUES ($1, $2, $3, $4, $5)', [
    id,
    kind,
    'running',
    JSON.stringify([]),
    JSON.stringify(payload),
  ]);
  return id;
}

async function jobAddError(jobId: string, step: string, code: string): Promise<void> {
  await getPool().query('UPDATE jobs SET errors = errors || $2::jsonb WHERE id = $1', [
    jobId,
    JSON.stringify([{ step, code }]),
  ]);
}

async function jobPatchPayload(jobId: string, patch: Record<string, unknown>): Promise<void> {
  await getPool().query(
    "UPDATE jobs SET payload = COALESCE(payload, '{}'::jsonb) || $2::jsonb WHERE id = $1",
    [jobId, JSON.stringify(patch)],
  );
}

async function jobFinish(jobId: string, status: 'finished' | 'failed'): Promise<void> {
  await getPool().query('UPDATE jobs SET status = $2 WHERE id = $1', [jobId, status]);
}

interface JobErrorItem {
  step: string;
  code: string;
}

// jobs.errors 列（jsonb）转公开形状，坏数据直接丢弃。
function toJobErrors(value: unknown): JobErrorItem[] {
  if (!Array.isArray(value)) return [];
  const out: JobErrorItem[] = [];
  for (const item of value) {
    if (typeof item !== 'object' || item === null) continue;
    if (!('step' in item) || !('code' in item)) continue;
    if (typeof item.step === 'string' && typeof item.code === 'string') out.push({ step: item.step, code: item.code });
  }
  return out;
}

// ---------- 建群后台 job ----------

// 建群流程：网关建群 → 邀请链接 → 逐成员 join（INVITE_NOT_READY 等待重试；
// INVITE_EXPIRED 重申请一次；ALREADY_MEMBER 视为成功）→ 等 member_joined（10s 超时记
// JOIN_TIMEOUT）→ promote memberAccountIds[0]（调用总数 ≤2，NOT_MEMBER_YET 最多重试 1 次）。
// 成功后写 groups + creator 成员行（creator）；成员行在各自 member_joined 确认后写入。
async function runCreateGroupJob(
  jobId: string,
  groupId: string,
  creatorAccountId: string,
  memberAccountIds: string[],
  platformUserIds: Map<string, string>,
): Promise<void> {
  const fail = async (step: string, code: string): Promise<void> => {
    await jobAddError(jobId, step, code);
    await jobFinish(jobId, 'failed');
  };
  try {
    // step=create：网关建群。
    let gatewayGroupId: string;
    try {
      gatewayGroupId = await gwCreateGroup(creatorAccountId);
    } catch (e) {
      await fail('create', thrownCode(e) ?? 'CREATE_FAILED');
      return;
    }
    await jobPatchPayload(jobId, { gatewayGroupId });
    // 群行先落库：group_members.group_id 有外键指向 groups(id)，成员行必须在群行之后写入。
    await getPool().query(
      `INSERT INTO groups(id, gateway_group_id, status, creator_account_id, agent_enabled, auto_kick_enabled)
       VALUES ($1, $2, 'active', $3, FALSE, FALSE)
       ON CONFLICT (id) DO UPDATE SET gateway_group_id = EXCLUDED.gateway_group_id`,
      [groupId, gatewayGroupId, creatorAccountId],
    );

    // step=invite：申请邀请链接。
    let invite: { inviteLink: string; readyAfterMs: number };
    try {
      invite = await gwInvite(gatewayGroupId);
    } catch (e) {
      await fail('invite', thrownCode(e) ?? 'INVITE_FAILED');
      return;
    }
    let expiredRounds = 0;

    // 逐成员 join。
    for (const accountId of memberAccountIds) {
      const platformUserId = platformUserIds.get(accountId) ?? `pu-${accountId}`;
      let joined = false;
      let notReadyAttempts = 0;
      for (;;) {
        try {
          await gwJoin(gatewayGroupId, accountId, invite.inviteLink);
          break; // 202 受理，下面等 member_joined。
        } catch (e) {
          const code = thrownCode(e) ?? 'JOIN_FAILED';
          if (code === 'ALREADY_MEMBER') {
            joined = true; // 视为成功，直接走 promote/写行。
            break;
          }
          if (code === 'INVITE_NOT_READY') {
            notReadyAttempts += 1;
            if (notReadyAttempts > 10) {
              await fail(`join:${accountId}`, 'JOIN_TIMEOUT');
              return;
            }
            await sleep(Math.max(invite.readyAfterMs, 500));
            continue;
          }
          if (code === 'INVITE_EXPIRED') {
            // 重申请链接后重试一次，群和账号状态都不变。
            expiredRounds += 1;
            if (expiredRounds > 1) {
              await fail(`join:${accountId}`, 'INVITE_EXPIRED');
              return;
            }
            try {
              invite = await gwInvite(gatewayGroupId);
            } catch (ie) {
              await fail('invite', thrownCode(ie) ?? 'INVITE_FAILED');
              return;
            }
            continue;
          }
          await fail(`join:${accountId}`, code);
          return;
        }
      }
      if (!joined) {
        // 等 member_joined，最多 10s；超时 job failed，code=JOIN_TIMEOUT。
        const seen = await waitForGatewayMember(gatewayGroupId, platformUserId, true, 10000);
        if (!seen) {
          await fail(`join:${accountId}`, 'JOIN_TIMEOUT');
          return;
        }
      }
      // 其他成员在 member_joined 确认后写入；upsert 保证行存在且角色正确
      //（creator 行永不降级；sse 消费者若先写了行，这里把角色扶正）。
      const role = accountId === memberAccountIds[0] ? 'admin' : 'member';
      await getPool().query(
        `INSERT INTO group_members(group_id, account_id, platform_user_id, role)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (group_id, account_id) DO UPDATE SET
           platform_user_id = EXCLUDED.platform_user_id,
           role = CASE WHEN group_members.role = 'creator' THEN 'creator' ELSE EXCLUDED.role END`,
        [groupId, accountId, platformUserId, role],
      );
    }

    // step=promote：把 memberAccountIds[0] 提升为管理员，调用总数 ≤2。
    const first = memberAccountIds[0];
    let promoted = false;
    let lastCode = 'PROMOTE_FAILED';
    for (let attempt = 0; attempt < 2; attempt += 1) {
      try {
        await gwPromote(gatewayGroupId, creatorAccountId, first);
        promoted = true;
        break;
      } catch (e) {
        lastCode = thrownCode(e) ?? 'PROMOTE_FAILED';
        // NOT_MEMBER_YET 则最多重试 1 次；其他错误直接失败。
        if (lastCode !== 'NOT_MEMBER_YET' || attempt === 1) break;
        await sleep(1000);
      }
    }
    if (!promoted) {
      await fail('promote', lastCode);
      return;
    }

    // 成功：群行已在 job 开始时写入，这里只补 creator 成员行，job finished。
    const creatorPu = platformUserIds.get(creatorAccountId) ?? `pu-${creatorAccountId}`;
    await getPool().query(
      `INSERT INTO group_members(group_id, account_id, platform_user_id, role)
       VALUES ($1, $2, $3, 'creator')
       ON CONFLICT (group_id, account_id) DO UPDATE SET platform_user_id = EXCLUDED.platform_user_id, role = 'creator'`,
      [groupId, creatorAccountId, creatorPu],
    );
    await jobFinish(jobId, 'finished');
  } catch (e) {
    // 兜底：job 永不卡在 running；记录未知错误后 failed。
    try {
      await fail('create', thrownCode(e) ?? 'INTERNAL_ERROR');
    } catch {
      // 记录失败也忽略，job 状态由后续排查处理。
    }
  }
}

// ---------- leave-all 后台 job ----------

// DB 成员与网关成员一致性校验：DB 有而网关无、网关有（已知账号）而 DB 无都算不一致。
// 网关里未知外部用户的 platformUserId 忽略（非服务账号进出由事件流处理）。
async function checkMembersConsistency(groupId: string, gatewayGroupId: string): Promise<string | null> {
  const pool = getPool();
  const dbRows = await pool.query<{ account_id: string; platform_user_id: string }>(
    'SELECT account_id, platform_user_id FROM group_members WHERE group_id = $1',
    [groupId],
  );
  let gatewayMembers: string[];
  try {
    gatewayMembers = await gwMembers(gatewayGroupId);
  } catch {
    return 'gateway-members-unreadable';
  }
  const gatewaySet = new Set(gatewayMembers);
  const problems: string[] = [];
  const dbPus = new Set<string>();
  for (const r of dbRows.rows) {
    dbPus.add(r.platform_user_id);
    if (!gatewaySet.has(r.platform_user_id)) problems.push(`db-extra:${r.account_id}`);
  }
  if (gatewayMembers.length > 0) {
    const known = await pool.query<{ id: string; platform_user_id: string }>(
      'SELECT id, platform_user_id FROM accounts WHERE platform_user_id = ANY($1)',
      [gatewayMembers],
    );
    const accountOfPu = new Map<string, string>();
    for (const r of known.rows) accountOfPu.set(r.platform_user_id, r.id);
    for (const pu of gatewayMembers) {
      const accountId = accountOfPu.get(pu);
      if (accountId !== undefined && !dbPus.has(pu)) problems.push(`gateway-extra:${accountId}`);
    }
  }
  return problems.length > 0 ? problems.join(',') : null;
}

async function emitInconsistency(kind: string, ref: string, message: string): Promise<void> {
  await getPool().query('INSERT INTO ws_events(type, payload) VALUES ($1, $2)', [
    'inconsistency',
    { kind, ref, message },
  ]);
}

// leave-all：非群主先退（失败记 errors 继续、群主不退、job failed，失败账号两边仍是成员），
// 群主最后退；完成后做 DB 与网关成员一致性校验，不一致记 inconsistency。
async function runLeaveAllJob(jobId: string, groupId: string, gatewayGroupId: string): Promise<void> {
  try {
    const pool = getPool();
    const g = await pool.query<{ creator_account_id: string }>('SELECT creator_account_id FROM groups WHERE id = $1', [
      groupId,
    ]);
    if (g.rows.length === 0) {
      await jobAddError(jobId, 'leave:group', 'GROUP_NOT_FOUND');
      await jobFinish(jobId, 'failed');
      return;
    }
    const creatorAccountId = g.rows[0].creator_account_id;
    const m = await pool.query<{ account_id: string }>(
      'SELECT account_id FROM group_members WHERE group_id = $1 ORDER BY account_id',
      [groupId],
    );
    const others = m.rows.map((r) => r.account_id).filter((id) => id !== creatorAccountId);
    let failed = false;
    for (const accountId of others) {
      try {
        await gwLeave(gatewayGroupId, accountId);
        await pool.query('DELETE FROM group_members WHERE group_id = $1 AND account_id = $2', [groupId, accountId]);
      } catch (e) {
        // 非群主失败：记 errors 继续退剩下的人，群主不退，失败账号两边仍是成员。
        failed = true;
        await jobAddError(jobId, `leave:${accountId}`, thrownCode(e) ?? 'LEAVE_FAILED');
      }
    }
    if (!failed) {
      try {
        await gwLeave(gatewayGroupId, creatorAccountId);
        await pool.query('DELETE FROM group_members WHERE group_id = $1 AND account_id = $2', [
          groupId,
          creatorAccountId,
        ]);
        // leave-all 完成后群 left、members=[]。
        await pool.query(`UPDATE groups SET status = 'left' WHERE id = $1`, [groupId]);
      } catch (e) {
        failed = true;
        await jobAddError(jobId, `leave:${creatorAccountId}`, thrownCode(e) ?? 'LEAVE_FAILED');
      }
    }
    // 完成后做一致性校验，不一致记 inconsistency（推 ws_events + 落 job payload，不翻转状态）。
    const inconsistency = await checkMembersConsistency(groupId, gatewayGroupId);
    if (inconsistency) {
      await emitInconsistency('members_mismatch', groupId, `群 ${groupId} 成员不一致: ${inconsistency}`);
      await jobPatchPayload(jobId, { inconsistency });
    }
    await jobFinish(jobId, failed ? 'failed' : 'finished');
  } catch (e) {
    try {
      await jobAddError(jobId, 'leave:group', thrownCode(e) ?? 'INTERNAL_ERROR');
      await jobFinish(jobId, 'failed');
    } catch {
      // 忽略二次失败。
    }
  }
}

// ---------- 群查询拼装 ----------

// 群详情：members 含 role；activeSequenceRunId/activeAgentRunId 只在 running 时非空。
async function loadGroupView(groupId: string): Promise<Record<string, unknown> | null> {
  const pool = getPool();
  const g = await pool.query<{
    id: string;
    gateway_group_id: string;
    status: string;
    creator_account_id: string;
    agent_enabled: boolean;
    auto_kick_enabled: boolean;
  }>(
    `SELECT id, gateway_group_id, status, creator_account_id, agent_enabled, auto_kick_enabled
     FROM groups WHERE id = $1`,
    [groupId],
  );
  if (g.rows.length === 0) return null;
  const row = g.rows[0];
  const members = await pool.query<{ account_id: string; platform_user_id: string; role: string }>(
    'SELECT account_id, platform_user_id, role FROM group_members WHERE group_id = $1 ORDER BY account_id',
    [groupId],
  );
  const seq = await pool.query<{ id: string }>(
    `SELECT id FROM sequence_runs WHERE group_id = $1 AND status = 'running' ORDER BY created_at DESC LIMIT 1`,
    [groupId],
  );
  const run = await pool.query<{ id: string }>(
    `SELECT id FROM agent_runs WHERE group_id = $1 AND status = 'running' ORDER BY run_started_at DESC LIMIT 1`,
    [groupId],
  );
  return {
    id: row.id,
    gatewayGroupId: row.gateway_group_id,
    status: row.status,
    creatorAccountId: row.creator_account_id,
    agentEnabled: row.agent_enabled,
    autoKickEnabled: row.auto_kick_enabled,
    members: members.rows.map((r) => ({
      accountId: r.account_id,
      platformUserId: r.platform_user_id,
      role: r.role,
    })),
    activeSequenceRunId: seq.rows.length > 0 ? seq.rows[0].id : null,
    activeAgentRunId: run.rows.length > 0 ? run.rows[0].id : null,
  };
}

// ---------- 群内 send（走出站流水线） ----------

interface OutboundResult {
  clientMsgId: string;
  deliveryStatus: string;
}

type CreateOutboundFn = (
  groupId: string,
  accountId: string,
  text: string,
  opts?: { idempotencyKey?: string },
) => Promise<OutboundResult>;

// 出站流水线：send.ts 落地后用它的 createOutbound；落地前走本地最小实现
//（先落 queued 行再调网关，能覆盖联调与验收；queued/unknown 行由 send.ts 落地后接管）。
// eslint-disable-next-line @typescript-eslint/no-require-imports
function loadSendModule(): unknown {
  return require('./send.js');
}

function loadCreateOutbound(): CreateOutboundFn | null {
  let mod: unknown;
  try {
    mod = loadSendModule();
  } catch {
    return null; // send.ts 未落地：调用方走本地最小实现。
  }
  if (typeof mod !== 'object' || mod === null || !('createOutbound' in mod)) return null;
  if (typeof mod.createOutbound !== 'function') return null;
  // 已确认为函数；签名按 send.ts 约定转接（动态模块形参无法再校验）。
  const fn = mod.createOutbound as unknown as CreateOutboundFn;
  return fn;
}

// 本地最小出站：先落 queued 行（网关发出了、库里一定有记录），再调网关。
// rate_limited 不调网关（保持 queued，到期由流水线按顺序发出）；
// 限流窗口内调网关会重置计时，故这里先查状态。
// 同步网关错误按 A2 落行（SENDER_NOT_IN_GROUP 等只记 failed，不抛给调用方改 409）。
async function localCreateOutbound(
  groupId: string,
  gatewayGroupId: string,
  accountId: string,
  text: string,
): Promise<OutboundResult> {
  const pool = getPool();
  const clientMsgId = uuidv4();
  const id = uuidv4();
  await pool.query(
    `INSERT INTO outbound_messages(id, group_id, account_id, client_msg_id, text, delivery_status)
     VALUES ($1, $2, $3, $4, $5, 'queued')`,
    [id, groupId, accountId, clientMsgId, text],
  );
  async function mark(status: string, failCode: string | null): Promise<OutboundResult> {
    if (failCode === null) {
      await pool.query(
        `UPDATE outbound_messages SET delivery_status = $2, accepted_at = now() WHERE client_msg_id = $1`,
        [clientMsgId, status],
      );
    } else {
      await pool.query(`UPDATE outbound_messages SET delivery_status = $2, fail_code = $3 WHERE client_msg_id = $1`, [
        clientMsgId,
        status,
        failCode,
      ]);
    }
    return { clientMsgId, deliveryStatus: status };
  }
  const acc = await pool.query<{ status: string }>('SELECT status FROM accounts WHERE id = $1', [accountId]);
  if (acc.rows.length > 0 && acc.rows[0].status === 'rate_limited') return { clientMsgId, deliveryStatus: 'queued' };
  try {
    await gwFetch('POST', `/groups/${gatewayGroupId}/send`, { accountId, clientMsgId, text }, 20000);
    return await mark('accepted', null);
  } catch (e) {
    const code = thrownCode(e) ?? 'NETWORK_ERROR';
    if (code === 'RATE_LIMITED') return { clientMsgId, deliveryStatus: 'queued' }; // 保持 queued。
    if (code === 'NETWORK_TIMEOUT') return await mark('unknown', null); // 由流水线 5s 内确认。
    return await mark('failed', code);
  }
}

// ---------- 路由 ----------

export const groupsRouter: Router = Router();
export const jobsRouter: Router = Router();

// POST /api/groups { creatorAccountId, memberAccountIds[] } → 202 { jobId }。
// 校验：账号必须 online（否则 422 ACCOUNT_NOT_ONLINE）；memberAccountIds ≥1 且不含群主（否则 400）。
groupsRouter.post(
  '/',
  requireAuth,
  requireAdmin,
  asyncHandler(async (req: Request, res: Response) => {
    const rawBody: unknown = req.body ?? {};
    if (typeof rawBody !== 'object' || rawBody === null) {
      sendError(req, res, 400, 'VALIDATION_ERROR', 'creatorAccountId 与 memberAccountIds[] 必填');
      return;
    }
    const creatorRaw = 'creatorAccountId' in rawBody ? rawBody.creatorAccountId : undefined;
    const membersRaw = 'memberAccountIds' in rawBody ? rawBody.memberAccountIds : undefined;
    if (typeof creatorRaw !== 'string' || creatorRaw.length === 0 || !Array.isArray(membersRaw)) {
      sendError(req, res, 400, 'VALIDATION_ERROR', 'creatorAccountId 与 memberAccountIds[] 必填');
      return;
    }
    const deduped = [...new Set(membersRaw)];
    if (deduped.some((id) => typeof id !== 'string')) {
      sendError(req, res, 400, 'VALIDATION_ERROR', 'memberAccountIds 必须为账号 id 数组');
      return;
    }
    const members = deduped as string[];
    if (members.length < 1 || members.includes(creatorRaw)) {
      sendError(req, res, 400, 'VALIDATION_ERROR', 'memberAccountIds 至少 1 个且不能包含群主');
      return;
    }
    // 所有账号必须 online（含群主），否则 422 ACCOUNT_NOT_ONLINE。
    const pool = getPool();
    const all = [creatorRaw, ...members];
    const rows = await pool.query<{ id: string; status: string; platform_user_id: string | null }>(
      'SELECT id, status, platform_user_id FROM accounts WHERE id = ANY($1)',
      [all],
    );
    const statusOf = new Map<string, { status: string; pu: string | null }>();
    for (const r of rows.rows) statusOf.set(r.id, { status: r.status, pu: r.platform_user_id });
    const offline = all.find((id) => statusOf.get(id)?.status !== 'online');
    if (offline !== undefined) {
      sendError(req, res, 422, 'ACCOUNT_NOT_ONLINE', `账号 ${offline} 未 online，无法建群`, { accountId: offline });
      return;
    }
    const platformUserIds = new Map<string, string>();
    for (const id of all) {
      const pu = statusOf.get(id)?.pu;
      if (pu !== undefined && pu !== null) platformUserIds.set(id, pu);
    }
    const groupId = uuidv4();
    const jobId = await createJob('create-group', { groupId, creatorAccountId: creatorRaw, memberAccountIds: members });
    // 后台异步执行，不阻塞响应。
    void runCreateGroupJob(jobId, groupId, creatorRaw, members, platformUserIds);
    res.status(202).json({ jobId });
  }),
);

// GET /api/groups 及 GET /api/groups/:id。
groupsRouter.get(
  '/',
  requireAuth,
  asyncHandler(async (_req: Request, res: Response) => {
    const { rows } = await getPool().query<{ id: string }>('SELECT id FROM groups ORDER BY id');
    const items: Array<Record<string, unknown>> = [];
    for (const r of rows) {
      const view = await loadGroupView(r.id);
      if (view) items.push(view);
    }
    res.json(items);
  }),
);

// 路径参数 id 必为单个字符串（express 把通配/重复参数解析成数组时拒绝）。
function routeGroupId(req: Request): string | null {
  const raw: unknown = req.params.id;
  return typeof raw === 'string' ? raw : null;
}

groupsRouter.get(
  '/:id',
  requireAuth,
  asyncHandler(async (req: Request, res: Response) => {
    const groupId = routeGroupId(req);
    if (!groupId) {
      sendError(req, res, 400, 'VALIDATION_ERROR', '群 id 非法');
      return;
    }
    const view = await loadGroupView(groupId);
    if (!view) {
      sendError(req, res, 404, 'GROUP_NOT_FOUND', `群 ${groupId} 不存在`);
      return;
    }
    res.json(view);
  }),
);

// PATCH /api/groups/:id { agentEnabled?, autoKickEnabled? } → 200。
groupsRouter.patch(
  '/:id',
  requireAuth,
  requireAdmin,
  asyncHandler(async (req: Request, res: Response) => {
    const groupId = routeGroupId(req);
    if (!groupId) {
      sendError(req, res, 400, 'VALIDATION_ERROR', '群 id 非法');
      return;
    }
    const rawBody: unknown = req.body ?? {};
    if (typeof rawBody !== 'object' || rawBody === null) {
      sendError(req, res, 400, 'VALIDATION_ERROR', 'agentEnabled / autoKickEnabled 至少提供一个');
      return;
    }
    const agentRaw = 'agentEnabled' in rawBody ? rawBody.agentEnabled : undefined;
    const kickRaw = 'autoKickEnabled' in rawBody ? rawBody.autoKickEnabled : undefined;
    if (agentRaw !== undefined && typeof agentRaw !== 'boolean') {
      sendError(req, res, 400, 'VALIDATION_ERROR', 'agentEnabled 必须为布尔值');
      return;
    }
    if (kickRaw !== undefined && typeof kickRaw !== 'boolean') {
      sendError(req, res, 400, 'VALIDATION_ERROR', 'autoKickEnabled 必须为布尔值');
      return;
    }
    if (agentRaw === undefined && kickRaw === undefined) {
      sendError(req, res, 400, 'VALIDATION_ERROR', 'agentEnabled / autoKickEnabled 至少提供一个');
      return;
    }
    const sets: string[] = [];
    const values: unknown[] = [];
    if (agentRaw !== undefined) {
      sets.push(`agent_enabled = $${values.length + 1}`);
      values.push(agentRaw);
    }
    if (kickRaw !== undefined) {
      sets.push(`auto_kick_enabled = $${values.length + 1}`);
      values.push(kickRaw);
    }
    values.push(groupId);
    const r = await getPool().query(`UPDATE groups SET ${sets.join(', ')} WHERE id = $${values.length}`, values);
    if ((r.rowCount ?? 0) === 0) {
      sendError(req, res, 404, 'GROUP_NOT_FOUND', `群 ${groupId} 不存在`);
      return;
    }
    res.json({});
  }),
);

// POST /api/groups/:id/send { accountId, text } → 202 { clientMsgId }。
// idle/disconnected/终态 → 409 ACCOUNT_UNAVAILABLE；不在群 → 409 ACCOUNT_NOT_IN_GROUP；
// rate_limited 照常受理 queued；实际发送走出站流水线 createOutbound。
groupsRouter.post(
  '/:id/send',
  requireAuth,
  requireAdmin,
  asyncHandler(async (req: Request, res: Response) => {
    const groupId = routeGroupId(req);
    if (!groupId) {
      sendError(req, res, 400, 'VALIDATION_ERROR', '群 id 非法');
      return;
    }
    const rawBody: unknown = req.body ?? {};
    const accountRaw =
      typeof rawBody === 'object' && rawBody !== null && 'accountId' in rawBody ? rawBody.accountId : undefined;
    const textRaw = typeof rawBody === 'object' && rawBody !== null && 'text' in rawBody ? rawBody.text : undefined;
    if (typeof accountRaw !== 'string' || accountRaw.length === 0 || typeof textRaw !== 'string') {
      sendError(req, res, 400, 'VALIDATION_ERROR', 'accountId 与 text 必填');
      return;
    }
    const pool = getPool();
    const g = await pool.query<{ gateway_group_id: string }>('SELECT gateway_group_id FROM groups WHERE id = $1', [
      groupId,
    ]);
    if (g.rows.length === 0) {
      sendError(req, res, 404, 'GROUP_NOT_FOUND', `群 ${groupId} 不存在`);
      return;
    }
    const gatewayGroupId = g.rows[0].gateway_group_id;
    const acc = await pool.query<{ status: string }>('SELECT status FROM accounts WHERE id = $1', [accountRaw]);
    if (acc.rows.length === 0) {
      sendError(req, res, 409, 'ACCOUNT_NOT_IN_GROUP', `账号 ${accountRaw} 不在群 ${groupId} 中`);
      return;
    }
    const status = acc.rows[0].status;
    // idle/disconnected/终态不可用；rate_limited 照常受理（保持 queued）。
    if (status === 'idle' || status === 'disconnected' || status === 'suspended' || status === 'session_expired') {
      sendError(req, res, 409, 'ACCOUNT_UNAVAILABLE', `账号 ${accountRaw} 当前不可用（${status}）`);
      return;
    }
    const mem = await pool.query('SELECT 1 FROM group_members WHERE group_id = $1 AND account_id = $2', [
      groupId,
      accountRaw,
    ]);
    if ((mem.rowCount ?? 0) === 0) {
      sendError(req, res, 409, 'ACCOUNT_NOT_IN_GROUP', `账号 ${accountRaw} 不在群 ${groupId} 中`);
      return;
    }
    try {
      const createOutbound = loadCreateOutbound();
      const result = createOutbound
        ? await createOutbound(groupId, accountRaw, textRaw)
        : await localCreateOutbound(groupId, gatewayGroupId, accountRaw, textRaw);
      res.status(202).json({ clientMsgId: result.clientMsgId });
    } catch (e) {
      // 出站流水线的前置校验失败（如竞态下账号已离群）才到这里；网关同步错误已在行内记 failed。
      const code = thrownCode(e);
      if (code === 'ACCOUNT_NOT_IN_GROUP') {
        sendError(req, res, 409, 'ACCOUNT_NOT_IN_GROUP', `账号 ${accountRaw} 不在群 ${groupId} 中`);
        return;
      }
      if (code === 'ACCOUNT_UNAVAILABLE') {
        sendError(req, res, 409, 'ACCOUNT_UNAVAILABLE', `账号 ${accountRaw} 当前不可用`);
        return;
      }
      throw e;
    }
  }),
);

// POST /api/groups/:id/leave-all → 202 { jobId }（后台执行，非群主先退、群主最后退）。
groupsRouter.post(
  '/:id/leave-all',
  requireAuth,
  requireAdmin,
  asyncHandler(async (req: Request, res: Response) => {
    const groupId = routeGroupId(req);
    if (!groupId) {
      sendError(req, res, 400, 'VALIDATION_ERROR', '群 id 非法');
      return;
    }
    const g = await getPool().query<{ gateway_group_id: string }>(
      'SELECT gateway_group_id FROM groups WHERE id = $1',
      [groupId],
    );
    const jobId = await createJob('leave-all', { groupId });
    void runLeaveAllJob(jobId, groupId, g.rows[0].gateway_group_id);
    res.status(202).json({ jobId });
  }),
);

// GET /api/jobs/:jobId → { status, errors }；errors 非空即 failed。
jobsRouter.get(
  '/:jobId',
  requireAuth,
  asyncHandler(async (req: Request, res: Response) => {
    const raw: unknown = req.params.jobId;
    const jobId = typeof raw === 'string' ? raw : null;
    if (!jobId) {
      sendError(req, res, 400, 'VALIDATION_ERROR', '任务 id 非法');
      return;
    }
    const { rows } = await getPool().query<{ status: string; errors: unknown }>(
      'SELECT status, errors FROM jobs WHERE id = $1',
      [jobId],
    );
    if (rows.length === 0) {
      sendError(req, res, 404, 'JOB_NOT_FOUND', `任务 ${jobId} 不存在`);
      return;
    }
    res.json({ status: rows[0].status, errors: toJobErrors(rows[0].errors) });
  }),
);
