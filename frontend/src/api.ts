// 后端 API 类型（与 CONTRACT.md 字段对齐）+ 带鉴权的 fetch 封装
// 401 自动用 HttpOnly cookie 调 refresh 续期；并发 401 只发一次 refresh（refreshTask 单例）

export interface Account {
  id: string;
  status: string;
  platformUserId: string | null;
  rateLimitedUntil: string | null;
  version?: number;
}

export interface GroupMember { accountId: string; platformUserId: string; role: string; }

export interface Group {
  id: string;
  gatewayGroupId: string;
  status: string;
  creatorAccountId: string;
  agentEnabled: boolean;
  autoKickEnabled: boolean;
  members: GroupMember[];
  activeSequenceRunId: string | null;
  activeAgentRunId: string | null;
}

export interface TimelineMsg {
  msgId: string;
  clientMsgId: string | null;
  senderPlatformUserId: string;
  isOwn: boolean;
  text: string;
  sentAt: string;
  deliveryStatus: string | null;
  failCode: string | null;
}

export interface TimelinePage { items: TimelineMsg[]; nextCursor: string | null; }

export interface AgentStep {
  kind: string;
  toolUseId: string | null;
  name: string | null;
  input: unknown;
  resultSummary: string | null;
  isError: boolean;
  errorCode: string | null;
  auditVerdict: string | null;
  rawResponse: string | null;
}

export interface AgentRun {
  id: string;
  groupId: string;
  status: string;
  endReason: string | null;
  summary: string | null;
  steps?: AgentStep[];
}

export interface Job { status: string; errors: Array<{ step: string; code: string }>; }

export interface SequenceStepDef { index: number; accountRole: string; text: string; delaySeconds: number; }
export interface Sequence { id: string; name: string; steps: SequenceStepDef[]; }
export interface SequenceRunStep {
  index: number; status: string; scheduledAt: string | null; sentAt: string | null;
  clientMsgId: string | null; resolvedVars: Record<string, string>; varSources: Record<string, string>;
}
export interface SequenceRun {
  id?: string; status: string; currentStepIndex: number;
  steps: SequenceRunStep[];
  // 后端 GET /api/sequence-runs/:id 实际还返回 groupId/sequenceId（CONTRACT 未列出）：可选处理
  groupId?: string; sequenceId?: string;
}

// 统一错误：业务错误体 { error: { code, message, requestId, stepIndex?, key? } }
export class ApiError extends Error {
  status: number;
  code: string;
  stepIndex?: number;
  key?: string;
  requestId?: string;
  isNetwork: boolean;
  constructor(status: number, code: string, message: string, extra?: { stepIndex?: number; key?: string; requestId?: string; isNetwork?: boolean }) {
    super(message);
    this.status = status;
    this.code = code;
    this.stepIndex = extra?.stepIndex;
    this.key = extra?.key;
    this.requestId = extra?.requestId;
    this.isNetwork = extra?.isNetwork ?? false;
  }
}

// 统一错误文案：ApiError 带业务码与请求追踪，普通 Error 直接取 message
export function formatError(e: unknown, fallback = '操作失败'): string {
  if (e instanceof ApiError) {
    return `${e.message}（${e.code}${e.requestId ? `，req:${e.requestId.slice(0, 8)}` : ''}）`;
  }
  if (e instanceof Error) return e.message;
  return fallback;
}

async function toApiError(res: Response): Promise<ApiError> {
  let code = res.status === 401 ? 'UNAUTHORIZED' : res.status === 403 ? 'FORBIDDEN' : 'HTTP_' + res.status;
  let message = res.statusText;
  let stepIndex: number | undefined;
  let key: string | undefined;
  let requestId: string | undefined;
  try {
    const body = await res.json();
    if (body?.error) {
      code = body.error.code ?? code;
      message = body.error.message ?? message;
      stepIndex = body.error.stepIndex;
      key = body.error.key;
      if (typeof body.error.requestId === 'string') requestId = body.error.requestId;
    }
  } catch { /* 非 JSON 错误体：保留默认 */ }
  return new ApiError(res.status, code, message, { stepIndex, key, requestId });
}

function networkError(): ApiError {
  return new ApiError(0, 'NETWORK_ERROR', '无法连接后端（可能是后端未启动）', { isNetwork: true });
}

// accessToken 存内存 + localStorage（刷新页面后仍可用）
const TOKEN_KEY = 'exam_access_token';
let memToken: string | null = null;
try { memToken = localStorage.getItem(TOKEN_KEY); } catch { /* 非浏览器环境兜底 */ }

export function getToken(): string | null { return memToken; }
// token 订阅：api 层静默续期后同步回 React 状态，WS 也用新 token 重连
type TokenListener = (t: string | null) => void;
const tokenListeners: TokenListener[] = [];
export function subscribeToken(fn: TokenListener): () => void {
  tokenListeners.push(fn);
  return () => {
    const i = tokenListeners.indexOf(fn);
    if (i >= 0) tokenListeners.splice(i, 1);
  };
}
export function setToken(t: string | null): void {
  memToken = t;
  try {
    if (t) localStorage.setItem(TOKEN_KEY, t);
    else localStorage.removeItem(TOKEN_KEY);
  } catch { /* 忽略存储异常 */ }
  tokenListeners.slice().forEach((fn) => fn(t));
}

// 从 JWT payload 里读 role（后端 jsonwebtoken 签发，含 username/role）；解不出返回 null
export function roleFromToken(t: string | null): string | null {
  if (!t) return null;
  try {
    const payload = JSON.parse(atob(t.split('.')[1].replace(/-/g, '+').replace(/_/g, '/')));
    return typeof payload.role === 'string' ? payload.role : null;
  } catch {
    return null;
  }
}

// 并发只发一次 refresh：所有同时 401 的请求共享同一个 refreshTask
let refreshTask: Promise<string> | null = null;
export function refreshOnce(): Promise<string> {
  if (!refreshTask) {
    refreshTask = (async () => {
      let res: Response;
      try {
        res = await fetch('/api/auth/refresh', { method: 'POST', credentials: 'include' });
      } catch {
        throw networkError();
      }
      if (!res.ok) throw await toApiError(res);
      const data = await res.json();
      if (!data?.accessToken) throw new ApiError(401, 'UNAUTHORIZED', '刷新会话失败');
      setToken(data.accessToken as string);
      return data.accessToken as string;
    })();
    // 无论成功失败都要清掉，保证下一次 401 会重发
    const cur = refreshTask;
    cur.then(
      () => { if (refreshTask === cur) refreshTask = null; },
      () => { if (refreshTask === cur) refreshTask = null; },
    );
  }
  return refreshTask;
}

export async function apiFetch(path: string, init: RequestInit = {}): Promise<Response> {
  const headers = new Headers(init.headers);
  if (memToken) headers.set('Authorization', 'Bearer ' + memToken);
  let res: Response;
  try {
    res = await fetch(path, { ...init, headers, credentials: 'include' });
  } catch {
    throw networkError();
  }
  // 登录/刷新接口本身的 401 不触发续期，避免死循环
  if (res.status === 401 && !path.startsWith('/api/auth/')) {
    try {
      const t = await refreshOnce();
      const h2 = new Headers(init.headers);
      h2.set('Authorization', 'Bearer ' + t);
      try {
        res = await fetch(path, { ...init, headers: h2, credentials: 'include' });
      } catch {
        throw networkError();
      }
    } catch {
      // 续期失败（会话作废/登出）：广播后由 AuthExpiredWatcher 清登录态回登录页
      try {
        window.dispatchEvent(new Event('exam:unauthorized'));
      } catch { /* 非浏览器环境兜底 */ }
      throw await toApiError(res);
    }
  }
  return res;
}

export async function fetchJson<T>(path: string, init: RequestInit = {}): Promise<T> {
  const res = await apiFetch(path, init);
  if (!res.ok) throw await toApiError(res);
  if (res.status === 204) return undefined as unknown as T;
  return (await res.json()) as T;
}

export function postJson<T>(path: string, body?: unknown): Promise<T> {
  return fetchJson<T>(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}

