// 网关 HTTP 客户端：connect/disconnect/send/join/invite/promote/kick/leave/members/by-client-id。
// 所有失败统一抛 GatewayError{httpStatus, code, retryAfterSeconds}；任意端点 503 的 code 固定为 RETRYABLE。
// groups.ts 内联了自己的最小客户端，本文件供 send/agent/sequence 与后续替换使用。

// 结构化网关错误：httpStatus 为 HTTP 状态码（不可达时为 0），code 为网关业务码。
export class GatewayError extends Error {
  httpStatus: number;
  code: string;
  retryAfterSeconds?: number;
  constructor(httpStatus: number, code: string, message: string, retryAfterSeconds?: number) {
    super(message);
    this.name = 'GatewayError';
    this.httpStatus = httpStatus;
    this.code = code;
    if (retryAfterSeconds !== undefined) this.retryAfterSeconds = retryAfterSeconds;
  }
}

export function isGatewayError(e: unknown): e is GatewayError {
  return e instanceof GatewayError;
}

function gatewayBase(): string {
  const base = (process.env.GATEWAY_URL ?? '').replace(/\/+$/, '');
  if (!base) throw new GatewayError(0, 'GATEWAY_NOT_CONFIGURED', '缺少环境变量 GATEWAY_URL');
  return base;
}

// 网关错误码：兼容 { code } 与 { error: { code } } 两种形状。
function readCode(data: unknown): string | undefined {
  if (typeof data !== 'object' || data === null) return undefined;
  const d = data as Record<string, unknown>;
  if (typeof d.code === 'string') return d.code;
  const inner = d.error;
  if (typeof inner === 'object' && inner !== null && 'code' in inner) {
    const c = (inner as Record<string, unknown>).code;
    if (typeof c === 'string') return c;
  }
  return undefined;
}

// 429 携带的 retryAfterSeconds（在 { error: {...} } 或顶层）。
function readRetryAfterSeconds(data: unknown): number | undefined {
  if (typeof data !== 'object' || data === null) return undefined;
  const d = data as Record<string, unknown>;
  const inner = d.error;
  if (typeof inner === 'object' && inner !== null && 'retryAfterSeconds' in inner) {
    const v = (inner as Record<string, unknown>).retryAfterSeconds;
    if (typeof v === 'number' && Number.isFinite(v)) return v;
  }
  if (typeof d.retryAfterSeconds === 'number' && Number.isFinite(d.retryAfterSeconds)) return d.retryAfterSeconds;
  return undefined;
}

// 通用请求：非 2xx 抛 GatewayError；连不上/超时按 networkCode 抛（send 传 NETWORK_TIMEOUT，其余默认不可达）。
async function request(
  method: string,
  path: string,
  body?: unknown,
  timeoutMs = 15000,
  networkCode = 'GATEWAY_UNREACHABLE',
): Promise<unknown> {
  const base = gatewayBase();
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
    // 任意端点 503 统一上抛 RETRYABLE（忽略网关自带的 code）。
    if (res.status === 503) {
      throw new GatewayError(503, 'RETRYABLE', `网关暂不可用 ${method} ${path}`, readRetryAfterSeconds(data));
    }
    if (!res.ok) {
      const code = readCode(data) ?? `HTTP_${res.status}`;
      throw new GatewayError(res.status, code, `网关请求失败 ${method} ${path}: ${code}`, readRetryAfterSeconds(data));
    }
    return data;
  } catch (e) {
    if (e instanceof GatewayError) throw e;
    const detail = e instanceof Error ? e.message : '未知错误';
    throw new GatewayError(0, networkCode, `网关不可达 ${method} ${path}: ${detail}`);
  } finally {
    clearTimeout(timer);
  }
}

function badResponse(what: string): GatewayError {
  return new GatewayError(200, 'BAD_GATEWAY_RESPONSE', `网关响应非法：${what}`);
}

// POST /accounts/:id/connect → { platformUserId }。
export async function gwConnect(accountId: string): Promise<{ platformUserId: string }> {
  const data = await request('POST', `/accounts/${encodeURIComponent(accountId)}/connect`, {});
  if (typeof data === 'object' && data !== null && 'platformUserId' in data) {
    const pu = (data as Record<string, unknown>).platformUserId;
    if (typeof pu === 'string' && pu.length > 0) return { platformUserId: pu };
  }
  throw badResponse('connect 缺 platformUserId');
}

// POST /accounts/:id/disconnect → {}。
export async function gwDisconnect(accountId: string): Promise<void> {
  await request('POST', `/accounts/${encodeURIComponent(accountId)}/disconnect`, {});
}

// POST /groups/:id/send → 202 表示受理；202 本身可能延迟一两秒，超时放宽到 30s。
// 超时/不可达按 NETWORK_TIMEOUT 抛（结果未知，走确认循环）。
export async function gwSend(
  gatewayGroupId: string,
  accountId: string,
  clientMsgId: string,
  text: string,
): Promise<void> {
  await request('POST', `/groups/${encodeURIComponent(gatewayGroupId)}/send`, { accountId, clientMsgId, text }, 30000, 'NETWORK_TIMEOUT');
}

// POST /groups { creatorAccountId } → groupId。
export async function gwCreateGroup(creatorAccountId: string): Promise<string> {
  const data = await request('POST', '/groups', { creatorAccountId });
  if (typeof data === 'object' && data !== null && 'groupId' in data) {
    const gid = (data as Record<string, unknown>).groupId;
    if (typeof gid === 'string' && gid.length > 0) return gid;
  }
  throw badResponse('建群响应缺 groupId');
}

// POST /groups/:id/invite → { inviteLink, readyAfterMs }。
export async function gwInvite(gatewayGroupId: string): Promise<{ inviteLink: string; readyAfterMs: number }> {
  const data = await request('POST', `/groups/${encodeURIComponent(gatewayGroupId)}/invite`, {});
  if (typeof data === 'object' && data !== null && 'inviteLink' in data) {
    const d = data as Record<string, unknown>;
    if (typeof d.inviteLink === 'string' && d.inviteLink.length > 0) {
      const raw = d.readyAfterMs;
      return { inviteLink: d.inviteLink, readyAfterMs: typeof raw === 'number' && raw > 0 ? raw : 0 };
    }
  }
  throw badResponse('邀请响应缺 inviteLink');
}

// POST /groups/:id/join → 202 受理（真正入群以 member_joined 事件为准）。
export async function gwJoin(gatewayGroupId: string, accountId: string, inviteLink: string): Promise<void> {
  await request('POST', `/groups/${encodeURIComponent(gatewayGroupId)}/join`, { accountId, inviteLink });
}

// POST /groups/:id/promote → 200。
export async function gwPromote(gatewayGroupId: string, byAccountId: string, accountId: string): Promise<void> {
  await request('POST', `/groups/${encodeURIComponent(gatewayGroupId)}/promote`, { byAccountId, accountId });
}

// POST /groups/:id/kick → 200 { kicked: true }（响应可能 1-5 秒；504 走 NETWORK_TIMEOUT 由调用方收敛）。
export async function gwKick(
  gatewayGroupId: string,
  byAccountId: string,
  targetPlatformUserId: string,
): Promise<void> {
  await request(
    'POST',
    `/groups/${encodeURIComponent(gatewayGroupId)}/kick`,
    { byAccountId, targetPlatformUserId },
    15000,
    'NETWORK_TIMEOUT',
  );
}

// POST /groups/:id/leave → 200。
export async function gwLeave(gatewayGroupId: string, accountId: string): Promise<void> {
  await request('POST', `/groups/${encodeURIComponent(gatewayGroupId)}/leave`, { accountId });
}

// GET /groups/:id/members → platformUserId 数组（坏单条跳过）。
export async function gwMembers(gatewayGroupId: string): Promise<string[]> {
  const data = await request('GET', `/groups/${encodeURIComponent(gatewayGroupId)}/members`, undefined, 10000);
  if (!Array.isArray(data)) throw badResponse('成员列表响应非法');
  const out: string[] = [];
  for (const item of data) {
    if (typeof item === 'object' && item !== null && 'platformUserId' in item) {
      const pu = (item as Record<string, unknown>).platformUserId;
      if (typeof pu === 'string') out.push(pu);
    }
  }
  return out;
}

// GET /groups/:id/messages/by-client-id/:cid → 落地最早的一条；404（未落地/群不存在）返回 null。
export async function gwByClientId(
  gatewayGroupId: string,
  clientMsgId: string,
): Promise<{ msgId: string; sentAt: string } | null> {
  let data: unknown;
  try {
    data = await request(
      'GET',
      `/groups/${encodeURIComponent(gatewayGroupId)}/messages/by-client-id/${encodeURIComponent(clientMsgId)}`,
      undefined,
      10000,
    );
  } catch (e) {
    if (e instanceof GatewayError && e.httpStatus === 404) return null;
    throw e;
  }
  if (typeof data === 'object' && data !== null) {
    const d = data as Record<string, unknown>;
    if (typeof d.msgId === 'string' && typeof d.sentAt === 'string') return { msgId: d.msgId, sentAt: d.sentAt };
  }
  throw badResponse('by-client-id 响应非法');
}
