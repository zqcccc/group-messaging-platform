import { useCallback, useEffect, useState } from 'react';
import { ApiError, fetchJson, postJson, type Account } from '../api';
import { useAuth } from '../auth';
import { useWs } from '../ws';
import { ReadOnlyNote } from '../components/ui';

// A1 转移表（前端镜像，仅控制按钮显隐；真实校验在后端）
const LEGAL_NEXT: Record<string, string[]> = {
  idle: ['online', 'suspended', 'session_expired'],
  online: ['idle', 'rate_limited', 'disconnected', 'suspended', 'session_expired'],
  rate_limited: ['online', 'disconnected', 'suspended', 'session_expired'],
  disconnected: ['idle', 'online', 'suspended', 'session_expired'],
  suspended: [],
  session_expired: [],
};

// 终态置灰
function isTerminal(status: string) {
  return status === 'suspended' || status === 'session_expired';
}

function statusBadge(status: string) {
  if (status === 'online') return <span className="badge green">online</span>;
  if (isTerminal(status)) return <span className="badge gray">{status}</span>;
  if (status === 'rate_limited') return <span className="badge orange">rate_limited</span>;
  return <span className="badge">{status}</span>;
}

// rate_limited 倒计时显示
function RateCountdown({ until }: { until: string }) {
  const [left, setLeft] = useState('');
  useEffect(() => {
    const tick = () => {
      const s = Math.max(0, Math.round((new Date(until).getTime() - Date.now()) / 1000));
      setLeft(s > 0 ? `${s}s 后恢复` : '即将恢复');
    };
    tick();
    const t = setInterval(tick, 1000);
    return () => clearInterval(t);
  }, [until]);
  return <span style={{ marginLeft: 6, fontSize: 12, color: '#b26a00' }}>{left}</span>;
}

// 账号列表页：轮询（10s）+ WS 事件实时更新；三个写按钮按转移表合法性显示
export default function AccountsPage() {
  const { isViewer } = useAuth();
  const [accounts, setAccounts] = useState<Account[]>([]);
  const [err, setErr] = useState('');
  const [notice, setNotice] = useState('');

  const load = useCallback(async () => {
    try {
      const data = await fetchJson<Account[]>('/api/accounts');
      setAccounts(data);
      setErr('');
    } catch (e) {
      setErr(e instanceof ApiError ? `${e.message}（${e.code}）` : '加载失败');
    }
  }, []);

  useEffect(() => {
    load();
    const t = setInterval(load, 10_000); // 兜底轮询
    return () => clearInterval(t);
  }, [load]);

  // WS 实时更新状态；account_terminal 到达时刷新整表（附带后果需重拉）
  useWs(['account_status_changed', 'account_terminal'], (f) => {
    const p = f.payload as { accountId?: string; from?: string; to?: string; status?: string };
    if (!p.accountId) return;
    if (f.type === 'account_terminal') {
      load();
      return;
    }
    const to = (p.to ?? p.status) as string | undefined;
    if (!to) return;
    setAccounts((prev) => prev.map((a) => (a.id === p.accountId ? { ...a, status: to } : a)));
  });

  const doConnect = async (a: Account) => {
    setNotice('');
    try {
      await postJson<{ status: string; platformUserId: string }>(`/api/accounts/${a.id}/connect`);
      load();
    } catch (e) {
      setNotice(e instanceof ApiError ? `重连失败：${e.message}（${e.code}）` : '重连失败');
    }
  };

  // 手动标记状态：带 expectedFrom（CAS）；CAS_CONFLICT 提示刷新
  const doTransition = async (a: Account, to: string) => {
    setNotice('');
    try {
      await postJson(`/api/accounts/${a.id}/transition`, { to, expectedFrom: a.status });
      load();
    } catch (e) {
      if (e instanceof ApiError && e.code === 'CAS_CONFLICT') {
        setNotice(`账号 ${a.id} 状态已变化，请刷新后重试（CAS_CONFLICT）`);
        load();
      } else {
        setNotice(e instanceof ApiError ? `操作失败：${e.message}（${e.code}）` : '操作失败');
      }
    }
  };

  // 三个按钮按转移表显示：
  // “标记离线” → disconnected（online / rate_limited 可用；题意 A1：限流到期时“已被操作员标记离线”则不再转回 online）
  // “释放账号” → idle（回到初始未占用状态；online / disconnected 可用）
  // “重连”     → 走 connect 接口回到 online（idle / disconnected 可用）
  const renderActions = (a: Account) => {
    if (isViewer || isTerminal(a.status)) return <span style={{ color: '#999' }}>{isTerminal(a.status) ? '终态，不可操作' : ''}</span>;
    const next = LEGAL_NEXT[a.status] ?? [];
    return (
      <>
        {next.includes('disconnected') && (
          <button onClick={() => doTransition(a, 'disconnected')}>标记离线</button>
        )}
        {next.includes('idle') && (
          <button onClick={() => doTransition(a, 'idle')}>释放账号</button>
        )}
        {(a.status === 'disconnected' || a.status === 'idle') && (
          <button className="primary" onClick={() => doConnect(a)}>重连</button>
        )}
      </>
    );
  };

  return (
    <div>
      <ReadOnlyNote />
      <div className="card">
        <h3>账号列表</h3>
        {err && <div className="err">{err}</div>}
        {notice && <div className="note">{notice}</div>}
        <table className="list">
          <thead>
            <tr><th>账号</th><th>状态</th><th>平台用户</th><th>限流</th><th>操作</th></tr>
          </thead>
          <tbody>
            {accounts.map((a) => (
              <tr key={a.id} style={isTerminal(a.status) ? { opacity: 0.55 } : undefined}>
                <td>{a.id}</td>
                <td>{statusBadge(a.status)}</td>
                <td>{a.platformUserId ?? '-'}</td>
                <td>
                  {a.status === 'rate_limited' && a.rateLimitedUntil
                    ? <RateCountdown until={a.rateLimitedUntil} />
                    : '-'}
                </td>
                <td>{renderActions(a)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
