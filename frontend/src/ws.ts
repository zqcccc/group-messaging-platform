import { useEffect, useRef, useState } from 'react';
import { getToken } from './api';

// WS 连接管理：断线自动重连（退避 1s/2s/5s），重连带 sinceSeq；
// 服务端按 sinceSeq 补发，客户端用 seq 去重（<= lastSeq 丢弃），消息去重由各页按 msgId 做。
export interface WsFrame { seq: number; type: string; payload: Record<string, unknown>; }

type Listener = (f: WsFrame) => void;

const SEQ_KEY = 'exam_ws_seq';
let lastSeq = 0;
try { lastSeq = Number(localStorage.getItem(SEQ_KEY) ?? 0) || 0; } catch { /* 忽略 */ }

const listeners = new Set<Listener>();
let socket: WebSocket | null = null;
let retryMs = 1000;
let timer: ReturnType<typeof setTimeout> | null = null;
let status: 'online' | 'offline' = 'offline';
const statusListeners = new Set<(s: 'online' | 'offline') => void>();

function setStatus(s: 'online' | 'offline') {
  status = s;
  statusListeners.forEach((fn) => fn(s));
}

function dispatch(f: WsFrame) {
  if (typeof f.seq === 'number') {
    if (f.seq <= lastSeq) return; // 补发/重推的重复帧直接丢弃
    lastSeq = f.seq;
    try { localStorage.setItem(SEQ_KEY, String(lastSeq)); } catch { /* 忽略 */ }
  }
  listeners.forEach((fn) => fn(f));
}

function scheduleReconnect() {
  if (timer) return;
  timer = setTimeout(() => {
    timer = null;
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
    retryMs = 1000;
    // 首帧认证 + 断线续传点
    ws.send(JSON.stringify({ type: 'auth', accessToken: t, sinceSeq: lastSeq }));
  };
  ws.onmessage = (ev) => {
    let data: Record<string, unknown>;
    try {
      data = JSON.parse(ev.data as string);
    } catch {
      return;
    }
    if (data.type === 'auth') {
      if (data.success) setStatus('online');
      else setStatus('offline');
      return;
    }
    if (typeof data.seq === 'number' && typeof data.type === 'string') {
      dispatch(data as unknown as WsFrame);
    }
  };
  ws.onclose = () => {
    setStatus('offline');
    scheduleReconnect();
  };
  ws.onerror = () => {
    try { ws.close(); } catch { /* 忽略 */ }
  };
}

// token 就绪后建连；登出（token=null）时断开
export function ensureWs(token: string | null) {
  if (!token) {
    try { socket?.close(); } catch { /* 忽略 */ }
    socket = null;
    setStatus('offline');
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

export function useWsStatus(): 'online' | 'offline' {
  const [s, setS] = useState(status);
  useEffect(() => {
    const fn = (v: 'online' | 'offline') => setS(v);
    statusListeners.add(fn);
    return () => { statusListeners.delete(fn); };
  }, []);
  return s;
}
