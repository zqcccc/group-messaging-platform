import { useEffect, useRef, useState } from 'react';
import { getToken, refreshOnce } from './api';

// WS 连接状态与帧类型常量
const WS_STATUS_ONLINE = 'online';
const WS_STATUS_OFFLINE = 'offline';
const FRAME_TYPE_AUTH = 'auth';
const EVT_UNAUTHORIZED = 'exam:unauthorized';

// WS 连接管理：断线自动重连（退避 1s/2s/5s），重连带 sinceSeq；
// 服务端按 sinceSeq 补发，客户端用 seq 去重（<= lastSeq 丢弃），消息去重由各页按 msgId 做。
export interface WsFrame { seq: number; type: string; payload: Record<string, unknown>; }
export type WsStatus = 'online' | 'offline';

type Listener = (f: WsFrame) => void;

const SEQ_KEY = 'exam_ws_seq';
let lastSeq = 0;
try { lastSeq = Number(localStorage.getItem(SEQ_KEY) ?? 0) || 0; } catch { /* 忽略 */ }

const listeners = new Set<Listener>();
let socket: WebSocket | null = null;
let retryMs = 1000;
let timer: number | undefined = undefined;
let status: WsStatus = WS_STATUS_OFFLINE;
const statusListeners = new Set<(s: WsStatus) => void>();

function setStatus(s: WsStatus) {
  status = s;
  statusListeners.forEach((fn) => fn(s));
}

function dispatch(f: WsFrame) {
  if (typeof f.seq === 'number') {
    if (f.seq <= lastSeq) return;
    lastSeq = f.seq;
    try { localStorage.setItem(SEQ_KEY, String(lastSeq)); } catch { /* 忽略 */ }
  }
  listeners.forEach((fn) => fn(f));
}

function scheduleReconnect() {
  if (timer !== undefined) return;
  timer = window.setTimeout(() => {
    timer = undefined;
    connect();
  }, retryMs);
  retryMs = Math.min(retryMs * 2, 5000);
}

function connect() {
  const t = getToken();
  if (!t) return;
  try {
    socket?.close();
  } catch { /* 忽略旧连接关闭异常 */ }
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  const ws = new WebSocket(`${proto}://${location.host}/ws`);
  socket = ws;
  ws.onopen = () => {
    if (socket !== ws) return;
    retryMs = 1000;
    // 首帧认证 + 断线续传点
    ws.send(JSON.stringify({ type: FRAME_TYPE_AUTH, accessToken: t, sinceSeq: lastSeq }));
  };
  ws.onmessage = (ev) => {
    if (socket !== ws) return;
    let data: Record<string, unknown>;
    try {
      data = JSON.parse(ev.data as string);
    } catch {
      return;
    }
    if (data.type === FRAME_TYPE_AUTH) {
      if (data.success) {
        setStatus(WS_STATUS_ONLINE);
      } else {
        setStatus(WS_STATUS_OFFLINE);
        // 先置空避免 onclose 误判触发旧 token 重连死循环
        socket = null;
        try { ws.close(); } catch { /* 忽略 */ }
        // 先尝试 refresh 续期（access token 15m 过期但 cookie 仍有效时自动重连），续期失败再广播登出
        refreshOnce().then(
          () => { connect(); },
          () => { window.dispatchEvent(new Event(EVT_UNAUTHORIZED)); },
        );
      }
      return;
    }
    if (typeof data.seq === 'number' && typeof data.type === 'string') {
      dispatch(data as unknown as WsFrame);
    }
  };
  ws.onclose = () => {
    if (socket !== ws) return;
    setStatus(WS_STATUS_OFFLINE);
    scheduleReconnect();
  };
  ws.onerror = () => {
    if (socket !== ws) return;
    try { ws.close(); } catch { /* 忽略 */ }
  };
}

// 手动重连：清理退避定时器并立即建连
export function reconnectWs(): void {
  if (timer !== undefined) {
    window.clearTimeout(timer);
    timer = undefined;
  }
  retryMs = 1000;
  connect();
}

// token 就绪后建连；登出（token=null）时断开
export function ensureWs(token: string | null) {
  if (!token) {
    try { socket?.close(); } catch { /* 忽略 */ }
    socket = null;
    setStatus(WS_STATUS_OFFLINE);
    return;
  }
  if (!socket || socket.readyState === WebSocket.CLOSED) connect();
}

// 订阅某类事件；types 为空表示全量（回调恒取最新，避免闭包过期漏掉状态）
export function useWs(types: string[], onEvent: (f: WsFrame) => void) {
  const key = types.join(',');
  const cbRef = useRef(onEvent);
  cbRef.current = onEvent;
  useEffect(() => {
    const allow = key === '' ? [] as string[] : key.split(',');
    const fn: Listener = (f) => {
      if (allow.length === 0 || allow.includes(f.type)) cbRef.current(f);
    };
    listeners.add(fn);
    return () => { listeners.delete(fn); };
  }, [key]);
}

export function useWsStatus(): WsStatus {
  const [s, setS] = useState<WsStatus>(status);
  useEffect(() => {
    const fn = (v: WsStatus) => setS(v);
    statusListeners.add(fn);
    return () => { statusListeners.delete(fn); };
  }, []);
  return s;
}
