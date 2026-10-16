import { useCallback, useEffect, useState } from 'react';
import { ApiError, fetchJson, formatError, postJson, type Account } from '../api';
import { useAuth } from '../auth';
import { useWs } from '../ws';
import { ReadOnlyNote } from '../components/ui';

// 账号状态常量
const STATUS_IDLE = 'idle';
const STATUS_ONLINE = 'online';
const STATUS_RATE_LIMITED = 'rate_limited';
const STATUS_DISCONNECTED = 'disconnected';
const STATUS_SUSPENDED = 'suspended';
const STATUS_SESSION_EXPIRED = 'session_expired';
// WS 事件类型常量
const EVT_STATUS_CHANGED = 'account_status_changed';
const EVT_TERMINAL = 'account_terminal';
const CAS_CONFLICT_CODE = 'CAS_CONFLICT';
const POLL_INTERVAL_MS = 10_000;

// A1 转移表（前端镜像，仅控制按钮显隐；真实校验在后端）
const LEGAL_NEXT: Record<string, string[]> = {
  [STATUS_IDLE]: [STATUS_ONLINE, STATUS_SUSPENDED, STATUS_SESSION_EXPIRED],
  [STATUS_ONLINE]: [STATUS_IDLE, STATUS_RATE_LIMITED, STATUS_DISCONNECTED, STATUS_SUSPENDED, STATUS_SESSION_EXPIRED],
  [STATUS_RATE_LIMITED]: [STATUS_ONLINE, STATUS_DISCONNECTED, STATUS_SUSPENDED, STATUS_SESSION_EXPIRED],
  [STATUS_DISCONNECTED]: [STATUS_IDLE, STATUS_ONLINE, STATUS_SUSPENDED, STATUS_SESSION_EXPIRED],
  [STATUS_SUSPENDED]: [],
  [STATUS_SESSION_EXPIRED]: [],
};

// 终态置灰
function isTerminal(status: string) {
  return status === STATUS_SUSPENDED || status === STATUS_SESSION_EXPIRED;
}

function statusBadge(status: string) {
  if (status === STATUS_ONLINE) return <span className="badge green">{STATUS_ONLINE}</span>;
  if (isTerminal(status)) return <span className="badge gray">{status}</span>;
  if (status === STATUS_RATE_LIMITED) return <span className="badge orange">{STATUS_RATE_LIMITED}</span>;
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
  const [loading, setLoading] = useState(true);
  const [busyId, setBusyId] = useState('');

  const load = useCallback(async () => {
    try {
      const data = await fetchJson<Account[]>('/api/accounts');
      setAccounts(data);
      setErr('');
    } catch (e) {
      setErr(formatError(e, '加载失败'));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    load();
    const t = setInterval(load, POLL_INTERVAL_MS); // 兜底轮询
    return () => clearInterval(t);
  }, [load]);

  // WS 实时更新状态；account_terminal 到达时刷新整表（附带后果需重拉）
  useWs([EVT_STATUS_CHANGED, EVT_TERMINAL], (f) => {
    const p = f.payload as { accountId?: string; from?: string; to?: string; status?: string; rateLimitedUntil?: string | null };
    if (!p.accountId) return;
    if (f.type === EVT_TERMINAL) {
      load();
      return;
    }
    const to = (p.to ?? p.status) as string | undefined;
    if (!to) return;
    if (to === STATUS_RATE_LIMITED && p.rateLimitedUntil === undefined) {
      // 缺少限流截止时间时重拉整表，确保倒计时准确
      load();
    }
    setAccounts((prev) => prev.map((a) => {
      if (a.id !== p.accountId) return a;
      const next: Account = { ...a, status: to };
      if (to === STATUS_RATE_LIMITED && typeof p.rateLimitedUntil !== 'undefined') {
        next.rateLimitedUntil = p.rateLimitedUntil;
      }
      return next;
    }));
  });

  const doConnect = async (a: Account) => {
    setNotice('');
    setBusyId(a.id);
    try {
      await postJson<{ status: string; platformUserId: string }>(`/api/accounts/${a.id}/connect`);
      load();
    } catch (e) {
      setNotice(`重连失败：${formatError(e)}`);
    } finally {
      setBusyId('');
    }
  };

  // 手动标记状态：带 expectedFrom（CAS）；CAS_CONFLICT 提示刷新
  const doTransition = async (a: Account, to: string) => {
    setNotice('');
    setBusyId(a.id);
    try {
      await postJson(`/api/accounts/${a.id}/transition`, { to, expectedFrom: a.status });
      load();
    } catch (e) {
      if (e instanceof ApiError && e.code === CAS_CONFLICT_CODE) {
        setNotice(`账号 ${a.id} 状态已变化，请刷新后重试（${CAS_CONFLICT_CODE}）`);
        load();
      } else {
        setNotice(`操作失败：${formatError(e)}`);
      }
    } finally {
      setBusyId('');
    }
  };

  // 三个按钮按转移表显示：
  // “标记离线” → disconnected（online / rate_limited 可用；题意 A1：限流到期时“已被操作员标记离线”则不再转回 online）
  // “释放账号” → idle（回到初始未占用状态；online / disconnected 可用）
  // “重连”     → 走 connect 接口回到 online（idle / disconnected 可用）
  const renderActions = (a: Account) => {
    if (isViewer || isTerminal(a.status)) return <span style={{ color: '#999' }}>{isTerminal(a.status) ? '终态，不可操作' : ''}</span>;
    const next = LEGAL_NEXT[a.status] ?? [];
    const busy = busyId === a.id;
    return (
      <>
        {next.includes(STATUS_DISCONNECTED) && (
          <button onClick={() => doTransition(a, STATUS_DISCONNECTED)} disabled={busy}>标记离线</button>
        )}
        {next.includes(STATUS_IDLE) && (
          <button onClick={() => doTransition(a, STATUS_IDLE)} disabled={busy}>释放账号</button>
        )}
        {(a.status === STATUS_DISCONNECTED || a.status === STATUS_IDLE) && (
          <button className="primary" onClick={() => doConnect(a)} disabled={busy}>重连</button>
        )}
      </>
    );
  };

  if (loading) return <div className="card">加载中…</div>;

  return (
    <div>
      <ReadOnlyNote />
      <div className="card">
        <h3>账号列表</h3>
        {err && <div className="err">{err} <button className="btn-sm" onClick={() => { setLoading(true); load(); }}>重试</button></div>}
        {notice && <div className="note">{notice} <button className="btn-sm" onClick={() => setNotice('')}>关闭</button></div>}
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
                  {a.status === STATUS_RATE_LIMITED && a.rateLimitedUntil
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
