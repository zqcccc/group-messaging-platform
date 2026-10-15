// WS 服务端 /ws：首帧 auth → 回执 → 事件推送；seq 全局单调；sinceSeq 补发。
// 约定：先 INSERT ws_events 落库拿到 seq，再推送，保证 seq 单调且不断号。
import { Server as HttpServer } from 'http';
import { Pool } from 'pg';
import { WebSocket, WebSocketServer } from 'ws';
import { getPool } from './db';

// 前端事件类型（CONTRACT B4 至少包含这六类）。
export type WsEventType =
  | 'account_status_changed'
  | 'account_terminal'
  | 'inconsistency'
  | 'message'
  | 'agent_run'
  | 'sequence_run';

// WS 上行首帧：{ type:'auth', accessToken, sinceSeq? }。
interface AuthFrame {
  type: string;
  accessToken?: unknown;
  sinceSeq?: unknown;
}

interface LiveClient {
  socket: WebSocket;
}

// 存活连接集合（同进程广播；落库行由各模块直接写 ws_events）。
const clients = new Set<LiveClient>();
let attached = false;

function pool(): Pool {
  return getPool();
}

// 发往前端的事件帧：{ seq, type, payload }。
function sendFrame(socket: WebSocket, seq: number, type: string, payload: unknown): void {
  if (socket.readyState !== WebSocket.OPEN) return;
  try {
    socket.send(JSON.stringify({ seq, type, payload }));
  } catch {
    // 发送失败忽略，连接关闭时统一清理。
  }
}

// 广播已落库事件（seq/type/payload 都来自 ws_events 行）。
function broadcast(seq: number, type: string, payload: unknown): void {
  for (const c of clients) sendFrame(c.socket, seq, type, payload);
}

// 持久化 + 推送：先入库拿 seq（单调），再广播，保证推送顺序与 seq 一致。
export async function emit(type: WsEventType | string, payload: Record<string, unknown>): Promise<number> {
  const { rows } = await pool().query<{ seq: number }>(
    'INSERT INTO ws_events(type, payload) VALUES ($1, $2) RETURNING seq',
    [type, JSON.stringify(payload)],
  );
  const seq = Number(rows[0].seq);
  broadcast(seq, type, payload);
  return seq;
}

// DB 写入失败时的降级：inconsistency 本身也记 ws_events；若连它都写不进则只打日志，
// 调用方（sse）负责把原事件放入重试队列，不丢事件。
export async function emitInconsistency(kind: string, ref: string, message: string): Promise<void> {
  try {
    await emit('inconsistency', { kind, ref, message });
  } catch (e) {
    console.error(`[ws] inconsistency 落库失败 kind=${kind} ref=${ref}:`, e instanceof Error ? e.message : e);
  }
}

// sinceSeq 补发：seq > sinceSeq 的行按 seq 升序推送（上限 1000 行防刷屏）。
async function replay(socket: WebSocket, sinceSeq: number): Promise<void> {
  const { rows } = await pool().query<{ seq: string; type: string; payload: unknown }>(
    'SELECT seq, type, payload FROM ws_events WHERE seq > $1 ORDER BY seq ASC LIMIT 1000',
    [sinceSeq],
  );
  for (const r of rows) {
    let payload: unknown = r.payload;
    if (typeof payload === 'string') {
      try {
        payload = JSON.parse(payload);
      } catch {
        // 坏数据原样推送，由前端忽略。
      }
    }
    sendFrame(socket, Number(r.seq), r.type, payload);
  }
}

// WS 挂载到已有 HTTP server（index.ts 里 app.listen 返回的 server）。
export function attachWs(server: HttpServer): WebSocketServer {
  const wss = new WebSocketServer({ server, path: '/ws' });
  attached = true;
  wss.on('connection', (socket: WebSocket) => {
    let authed = false;
    const client: LiveClient = { socket };
    // 首帧 10s 超时：超时未认证直接关连接。
    const authTimer = setTimeout(() => {
      if (!authed) {
        try {
          socket.close(4401, 'auth timeout');
        } catch {
          // 关闭失败忽略。
        }
      }
    }, 10000);
    socket.on('message', (raw: Buffer) => {
      if (authed) return; // 认证后上行帧忽略（服务端只推送）。
      let frame: AuthFrame;
      try {
        frame = JSON.parse(raw.toString()) as AuthFrame;
      } catch {
        try {
          socket.close(4400, 'bad auth frame');
        } catch {
          // 关闭失败忽略。
        }
        return;
      }
      if (frame.type !== 'auth' || typeof frame.accessToken !== 'string' || !frame.accessToken) {
        try {
          socket.close(4400, 'bad auth frame');
        } catch {
          // 关闭失败忽略。
        }
        return;
      }
      void verifyAndAdmit(socket, client, frame, authTimer).then((ok) => {
        if (ok) authed = true;
      });
    });
    socket.on('close', () => {
      clearTimeout(authTimer);
      clients.delete(client);
    });
    socket.on('error', () => {
      clearTimeout(authTimer);
      clients.delete(client);
    });
  });
  return wss;
}

// 验 token → 回执 → 入集合 → sinceSeq 补发。失败关连接。
async function verifyAndAdmit(
  socket: WebSocket,
  client: LiveClient,
  frame: AuthFrame,
  authTimer: NodeJS.Timeout,
): Promise<boolean> {
  // 动态 require 避免循环依赖与构建顺序耦合。
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const auth = require('./auth.js') as {
    verifyAccessToken?: (token: string) => Promise<{ username: string; role: string } | null>;
  };
  let user: { username: string; role: string } | null = null;
  try {
    if (typeof auth.verifyAccessToken === 'function') user = await auth.verifyAccessToken(frame.accessToken as string);
  } catch {
    user = null;
  }
  if (!user) {
    clearTimeout(authTimer);
    try {
      socket.send(JSON.stringify({ type: 'auth', success: false }));
      socket.close(4401, 'unauthorized');
    } catch {
      // 关闭失败忽略。
    }
    return false;
  }
  clearTimeout(authTimer);
  clients.add(client);
  try {
    socket.send(JSON.stringify({ type: 'auth', success: true }));
  } catch {
    // 回执失败忽略，下面的补发会一并失败，连接关闭时清理。
  }
  const sinceRaw = frame.sinceSeq;
  const sinceSeq = typeof sinceRaw === 'number' && Number.isInteger(sinceRaw) && sinceRaw >= 0 ? sinceRaw : null;
  if (sinceSeq !== null) {
    try {
      await replay(socket, sinceSeq);
    } catch {
      // 补发失败忽略，实时事件照常推送。
    }
  }
  return true;
}

// ---------- WS 广播轮询（补推各模块直接 INSERT 的 ws_events 行） ----------

let broadcasterTimer: NodeJS.Timeout | null = null;
let broadcasterLastSeen = 0;
let broadcasterPolling = false;

// 轮询 ws_events（seq > lastSeen，ORDER BY seq，上限 200）并广播。
// 启动时 lastSeen = max(seq)，只推启动后新增的行；emit() 直推的行会被轮询再次推到，
// 前端按 seq 去重（<= lastSeq 丢弃），故重复推送无害。
export function startWsBroadcaster(intervalMs = 500): void {
  if (broadcasterTimer) return;
  void (async () => {
    try {
      const { rows } = await pool().query<{ m: string }>('SELECT COALESCE(MAX(seq), 0) AS m FROM ws_events');
      broadcasterLastSeen = Number(rows[0].m ?? 0);
    } catch (e) {
      console.error('[ws] broadcaster 初始化 lastSeen 失败:', e instanceof Error ? e.message : e);
    }
    broadcasterTimer = setInterval(() => {
      void pollWsEvents().catch((e: unknown) => {
        console.error('[ws] broadcaster 轮询失败:', e instanceof Error ? e.message : e);
      });
    }, intervalMs);
    if (typeof broadcasterTimer.unref === 'function') broadcasterTimer.unref();
  })();
}

async function pollWsEvents(): Promise<void> {
  if (broadcasterPolling) return; // 上一轮未完成则跳过，避免堆积。
  broadcasterPolling = true;
  try {
    const { rows } = await pool().query<{ seq: string; type: string; payload: unknown }>(
      'SELECT seq, type, payload FROM ws_events WHERE seq > $1 ORDER BY seq ASC LIMIT 200',
      [broadcasterLastSeen],
    );
    for (const r of rows) {
      const seq = Number(r.seq);
      let payload: unknown = r.payload;
      if (typeof payload === 'string') {
        try {
          payload = JSON.parse(payload);
        } catch {
          // 坏数据原样推送，由前端忽略。
        }
      }
      broadcast(seq, r.type, payload);
      broadcasterLastSeen = seq;
    }
  } finally {
    broadcasterPolling = false;
  }
}

export function isWsAttached(): boolean {
  return attached;
}
