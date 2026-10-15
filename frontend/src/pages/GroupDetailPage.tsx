import { useCallback, useEffect, useRef, useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import { ApiError, fetchJson, postJson, type AgentRun, type Group, type TimelineMsg } from '../api';
import { useAuth } from '../auth';
import { useWs, type WsFrame } from '../ws';
import { ReadOnlyNote } from '../components/ui';

// 群详情页：成员 role 徽；消息时间线（倒序分页 + 加载更早 + WS 实时追加，msgId 去重）；
// 自己消息的 deliveryStatus 徽；最近 agent run 列表（blocked 醒目红）
export default function GroupDetailPage() {
  const { id } = useParams<{ id: string }>();
  const { isViewer } = useAuth();
  const [group, setGroup] = useState<Group | null>(null);
  const [messages, setMessages] = useState<TimelineMsg[]>([]);
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [hasMore, setHasMore] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [runs, setRuns] = useState<AgentRun[]>([]);
  const [runsErr, setRunsErr] = useState('');
  const [err, setErr] = useState('');
  const [sendAccount, setSendAccount] = useState('');
  const [sendText, setSendText] = useState('');
  const [sendMsg, setSendMsg] = useState('');
  // 已见 msgId 集合：WS 补发/重推与分页重叠时去重（B4 不重复）
  const seenRef = useRef<Record<string, true>>({});
  const groupIdRef = useRef(id);
  groupIdRef.current = id;
  // 时间线按 sentAt 升序展示（旧在上、新在下）；接口返回倒序，入库时翻转。
  // 去重在 setState 之外同步完成（updater 保持纯函数，StrictMode 安全）；
  // latest 刷新只追加，不碰翻页游标，保证“加载更早”链式稳定。
  const mergePage = useCallback((items: TimelineMsg[], cursor: string | null, mode: 'reset' | 'older' | 'latest') => {
    const fresh = items.filter((m) => !seenRef.current[m.msgId]);
    fresh.forEach((m) => { seenRef.current[m.msgId] = true; });
    const asc = fresh.slice().reverse();
    if (mode === 'older') {
      if (asc.length > 0) setMessages((prev) => [...asc, ...prev]);
      setNextCursor(cursor);
      setHasMore(cursor !== null);
      return;
    }
    if (mode === 'latest') {
      if (asc.length > 0) setMessages((prev) => [...prev, ...asc]);
      // 已有消息的 deliveryStatus 可能推进（queued→sent）：用最新页刷新旧行状态
      if (items.length > 0) {
        const byId: Record<string, TimelineMsg> = {};
        items.forEach((m) => { byId[m.msgId] = m; });
        setMessages((prev) => prev.map((m) => {
          const cur = byId[m.msgId];
          if (!cur) return m;
          return { ...m, deliveryStatus: cur.deliveryStatus ?? m.deliveryStatus, failCode: cur.failCode ?? m.failCode, sentAt: cur.sentAt };
        }));
      }
      return;
    }
    setMessages(asc);
    setNextCursor(cursor);
    setHasMore(cursor !== null);
  }, []);

  const loadRuns = useCallback(async (gid: string) => {
    try {
      const list = await fetchJson<AgentRun[]>(`/api/groups/${gid}/agent-runs`);
      setRuns(list);
      setRunsErr('');
    } catch (e) {
      setRuns([]);
      setRunsErr(e instanceof ApiError ? `运行列表加载失败：${e.message}（${e.code}）` : '运行列表加载失败');
    }
  }, []);

  const loadFirst = useCallback(async (gid: string) => {
    try {
      const [g, r] = await Promise.all([
        fetchJson<Group>(`/api/groups/${gid}`),
        fetchJson<{ items: TimelineMsg[]; nextCursor: string | null }>(`/api/groups/${gid}/messages?limit=50`),
      ]);
      setGroup(g);
      setRuns([]);
      setRunsErr('');
      seenRef.current = {};
      mergePage(r.items, r.nextCursor, 'reset');
      setErr('');
      if (g.members.length > 0) setSendAccount((cur) => cur || g.members[0].accountId);
      loadRuns(gid);
    } catch (e) {
      setErr(e instanceof ApiError ? `${e.message}（${e.code}）` : '加载失败');
    }
  }, [mergePage, loadRuns]);

  useEffect(() => {
    if (id) loadFirst(id);
  }, [id, loadFirst]);

  // 加载更早：用服务端下发的 nextCursor 链式翻页；新消息从顶部插入不影响游标稳定性
  const loadMore = async () => {
    if (!id || !nextCursor || loadingMore) return;
    setLoadingMore(true);
    try {
      const page = await fetchJson<{ items: TimelineMsg[]; nextCursor: string | null }>(
        `/api/groups/${id}/messages?before=${encodeURIComponent(nextCursor)}&limit=50`,
      );
      mergePage(page.items, page.nextCursor, 'older');
    } catch (e) {
      setSendMsg(e instanceof ApiError ? `加载更早失败：${e.message}（${e.code}）` : '加载更早失败');
    } finally {
      setLoadingMore(false);
    }
  };

  // WS message 事件只带 { groupId, msgId, isOwn }：拉最新一页合并（去重后追加）
  const onWsMessage = useCallback(async (f: WsFrame) => {
    const p = f.payload as { groupId?: string; msgId?: string };
    if (p.groupId !== groupIdRef.current) return;
    try {
      const page = await fetchJson<{ items: TimelineMsg[]; nextCursor: string | null }>(
        `/api/groups/${groupIdRef.current}/messages?limit=50`,
      );
      mergePage(page.items, page.nextCursor, 'latest');
    } catch {
      // WS 触发的刷新失败不打扰用户，下次轮询/事件再补
    }
  }, [mergePage]);
  useWs(['message'], onWsMessage);

  // agent run / sequence run 事件到达时刷新运行列表
  useWs(['agent_run'], () => {
    if (!id) return;
    loadRuns(id);
  });

  const send = async () => {
    if (!id || !sendText.trim()) return;
    setSendMsg('');
    try {
      await postJson(`/api/groups/${id}/send`, { accountId: sendAccount, text: sendText });
      setSendText('');
      setSendMsg('已受理，等待网关发出（deliveryStatus 会经 WS 更新）');
    } catch (e) {
      setSendMsg(e instanceof ApiError ? `发送失败：${e.message}（${e.code}）` : '发送失败');
    }
  };

  const toggle = async (patch: { agentEnabled?: boolean; autoKickEnabled?: boolean }) => {
    if (!id || !group) return;
    try {
      await fetchJson(`/api/groups/${id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(patch),
      });
      setGroup({ ...group, ...patch });
    } catch (e) {
      setSendMsg(e instanceof ApiError ? `更新失败：${e.message}（${e.code}）` : '更新失败');
    }
  };

  if (!group) return <div className="card">加载中…{err && <div className="err">{err}</div>}</div>;

  return (
    <div>
      <ReadOnlyNote />
      <div className="card">
        <h3>群 {group.id.slice(0, 8)}…（{group.status}）</h3>
        <div className="form-row">
          <span>成员：</span>
          {group.members.map((m) => (
            <span key={m.accountId} style={{ marginRight: 8 }}>
              {m.accountId}/{m.platformUserId}{' '}
              <span className={`badge ${m.role === 'creator' ? 'green' : m.role === 'admin' ? 'orange' : ''}`}>{m.role}</span>
            </span>
          ))}
          {group.members.length === 0 && <span>（无成员）</span>}
        </div>
        {!isViewer && (
          <div className="form-row">
            <label><input type="checkbox" checked={group.agentEnabled} onChange={(e) => toggle({ agentEnabled: e.target.checked })} /> agentEnabled</label>
            <label><input type="checkbox" checked={group.autoKickEnabled} onChange={(e) => toggle({ autoKickEnabled: e.target.checked })} /> autoKickEnabled</label>
          </div>
        )}
      </div>

      <div className="card">
        <h3>消息时间线</h3>
        {hasMore && (
          <div style={{ marginBottom: 8 }}>
            <button onClick={loadMore} disabled={loadingMore}>{loadingMore ? '加载中…' : '加载更早'}</button>
          </div>
        )}
        <div className="timeline">
          {messages.map((m) => (
            <div className="msg" key={m.msgId}>
              <div className="meta">
                {m.sentAt} · {m.senderPlatformUserId} {m.isOwn ? '(我)' : ''}{' '}
                {m.isOwn && m.deliveryStatus && (
                  <span className={`badge ${m.deliveryStatus === 'sent' || m.deliveryStatus === 'accepted' ? 'green' : m.deliveryStatus === 'failed' || m.deliveryStatus === 'cancelled' ? 'red' : ''}`}>
                    {m.deliveryStatus}{m.failCode ? `:${m.failCode}` : ''}
                  </span>
                )}
              </div>
              <div>{m.text}</div>
            </div>
          ))}
          {messages.length === 0 && <div style={{ color: '#999' }}>暂无消息</div>}
        </div>
        {!isViewer && (
          <div style={{ marginTop: 12 }}>
            <div className="form-row">
              <select value={sendAccount} onChange={(e) => setSendAccount(e.target.value)}>
                {group.members.map((m) => <option key={m.accountId} value={m.accountId}>{m.accountId}</option>)}
              </select>
              <input value={sendText} onChange={(e) => setSendText(e.target.value)} placeholder="输入消息文本" style={{ flex: 1 }} />
              <button className="primary" onClick={send}>发送</button>
            </div>
            {sendMsg && <div className="note">{sendMsg}</div>}
          </div>
        )}
      </div>

      <div className="card">
        <h3>最近 Agent 运行</h3>
        {runsErr && <div className="err">{runsErr}</div>}
        <table className="list">
          <thead><tr><th>Run</th><th>状态</th><th>endReason</th><th>摘要</th></tr></thead>
          <tbody>
            {runs.map((r) => (
              <tr key={r.id} style={r.status === 'blocked' ? { background: '#fff0f0' } : undefined}>
                <td><Link to={`/agent-runs/${r.id}`}>{r.id.slice(0, 8)}…</Link></td>
                <td>
                  {r.status === 'blocked'
                    ? <span className="badge red">blocked（需处理）</span>
                    : <span className="badge">{r.status}</span>}
                </td>
                <td>{r.endReason ?? '-'}</td>
                <td>{r.summary ?? '-'}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {runs.length === 0 && <div style={{ color: '#999' }}>暂无运行</div>}
      </div>
    </div>
  );
}
