import { useCallback, useEffect, useRef, useState, type FormEvent } from 'react';
import { Link, useParams } from 'react-router-dom';
import { fetchJson, formatError, postJson, type AgentRun, type Group, type TimelineMsg } from '../api';
import { useAuth } from '../auth';
import { useWs, type WsFrame } from '../ws';
import { ReadOnlyNote } from '../components/ui';

// 投递状态常量
const DELIVERY_SENT = 'sent';
const DELIVERY_ACCEPTED = 'accepted';
const DELIVERY_QUEUED = 'queued';
const DELIVERY_UNKNOWN = 'unknown';
const DELIVERY_FAILED = 'failed';
const DELIVERY_CANCELLED = 'cancelled';
// 成员角色常量
const ROLE_CREATOR = 'creator';
const ROLE_ADMIN = 'admin';
// 群状态常量
const GROUP_STATUS_LEFT = 'left';
const GROUP_STATUS_UNREACHABLE = 'unreachable';
// 运行状态常量
const RUN_STATUS_BLOCKED = 'blocked';
// 合并模式常量
const MERGE_RESET = 'reset';
const MERGE_OLDER = 'older';
const MERGE_LATEST = 'latest';
// WS 事件常量
const EVT_MESSAGE = 'message';
const EVT_GROUP_UPDATED = 'group_updated';
const EVT_ACCOUNT_TERMINAL = 'account_terminal';
const EVT_AGENT_RUN = 'agent_run';
// 徽标样式常量
const BADGE_GREEN = 'green';
const BADGE_RED = 'red';
const BADGE_ORANGE = 'orange';
// 任务状态常量
const JOB_STATUS_FINISHED = 'finished';
const JOB_STATUS_FAILED = 'failed';
// 轮询与分页常量
const POLL_INTERVAL_MS = 5000;
const PAGE_LIMIT = 50;
const LEAVE_JOB_ROUNDS = 60;
const LEAVE_JOB_DELAY_MS = 1000;
const BOTTOM_THRESHOLD_PX = 80;

type MergeMode = typeof MERGE_RESET | typeof MERGE_OLDER | typeof MERGE_LATEST;

// 投递状态徽标样式：unknown 橙色提示确认中，queued 默认，sent/accepted 绿色，failed/cancelled 红色
function deliveryBadgeClass(status: string): string {
  if (status === DELIVERY_SENT || status === DELIVERY_ACCEPTED) return BADGE_GREEN;
  if (status === DELIVERY_FAILED || status === DELIVERY_CANCELLED) return BADGE_RED;
  if (status === DELIVERY_UNKNOWN) return BADGE_ORANGE;
  if (status === DELIVERY_QUEUED) return '';
  return '';
}

// 成员角色徽标样式
function roleBadgeClass(role: string): string {
  if (role === ROLE_CREATOR) return BADGE_GREEN;
  if (role === ROLE_ADMIN) return BADGE_ORANGE;
  return '';
}

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
  // 页面级加载错误：加载失败时渲染错误卡片并提供重试
  const [err, setErr] = useState('');
  // 群配置与退群操作提示：渲染在顶部群信息卡片内
  const [groupMsg, setGroupMsg] = useState('');
  // 历史翻页错误：渲染在加载更早按钮旁
  const [historyErr, setHistoryErr] = useState('');
  const [sendAccount, setSendAccount] = useState('');
  const [sendText, setSendText] = useState('');
  // 发送消息反馈：仅用于发送结果，渲染在发送框下方
  const [sendMsg, setSendMsg] = useState('');
  const [sending, setSending] = useState(false);
  const [confirmingLeave, setConfirmingLeave] = useState(false);
  const [leaving, setLeaving] = useState(false);
  const [showNewMsg, setShowNewMsg] = useState(false);
  // 已见 msgId 集合：WS 补发/重推与分页重叠时去重
  const seenRef = useRef<Record<string, true>>({});
  const groupIdRef = useRef(id);
  groupIdRef.current = id;
  // 时间线滚动管理
  const timelineRef = useRef<HTMLDivElement | null>(null);

  const scrollToBottom = useCallback(() => {
    const el = timelineRef.current;
    if (el) el.scrollTop = el.scrollHeight;
    setShowNewMsg(false);
  }, []);

  // 时间线按 sentAt 升序展示（旧在上、新在下）；接口返回倒序，入库时翻转。
  // 去重在 setState 之外同步完成（updater 保持纯函数，StrictMode 安全）；
  // latest 刷新只追加，不碰翻页游标，保证加载更早链式稳定。
  const mergePage = useCallback((items: TimelineMsg[], cursor: string | null, mode: MergeMode) => {
    const fresh = items.filter((m) => !seenRef.current[m.clientMsgId ?? m.msgId]);
    fresh.forEach((m) => { seenRef.current[m.clientMsgId ?? m.msgId] = true; });
    const asc = fresh.slice().reverse();
    if (mode === MERGE_OLDER) {
      if (asc.length > 0) setMessages((prev) => [...asc, ...prev]);
      setNextCursor(cursor);
      setHasMore(cursor !== null);
      return;
    }
    if (mode === MERGE_LATEST) {
      const el = timelineRef.current;
      const nearBottom = !el || (el.scrollHeight - el.scrollTop - el.clientHeight < BOTTOM_THRESHOLD_PX);
      if (asc.length > 0) setMessages((prev) => [...prev, ...asc]);
      // 已有消息的 deliveryStatus 可能推进（queued→sent）：用最新页刷新旧行状态与 msgId
      if (items.length > 0) {
        const byKey: Record<string, TimelineMsg> = {};
        items.forEach((m) => { byKey[m.clientMsgId ?? m.msgId] = m; });
        setMessages((prev) => prev.map((m) => {
          const cur = byKey[m.clientMsgId ?? m.msgId];
          if (!cur) return m;
          return { ...m, msgId: cur.msgId, deliveryStatus: cur.deliveryStatus ?? m.deliveryStatus, failCode: cur.failCode ?? m.failCode, sentAt: cur.sentAt };
        }));
      }
      // 有新增且用户不在底部时提示，否则自动滚到底部
      if (asc.length > 0) {
        requestAnimationFrame(() => {
          const cur = timelineRef.current;
          if (!cur) return;
          if (nearBottom) {
            cur.scrollTop = cur.scrollHeight;
            setShowNewMsg(false);
          } else {
            setShowNewMsg(true);
          }
        });
      }
      return;
    }
    setMessages(asc);
    setNextCursor(cursor);
    setHasMore(cursor !== null);
    setShowNewMsg(false);
    // 首次重置加载后自动滚到底部
    requestAnimationFrame(() => {
      const el = timelineRef.current;
      if (el) el.scrollTop = el.scrollHeight;
    });
  }, []);

  const loadRuns = useCallback(async (gid: string) => {
    try {
      const list = await fetchJson<AgentRun[]>(`/api/groups/${gid}/agent-runs`);
      setRuns(list);
      setRunsErr('');
    } catch (e) {
      setRuns([]);
      setRunsErr(formatError(e, '运行列表加载失败'));
    }
  }, []);

  const loadGroup = useCallback(async (gid: string) => {
    const g = await fetchJson<Group>(`/api/groups/${gid}`);
    setGroup(g);
    if (g.members.length > 0) setSendAccount((cur) => cur || g.members[0].accountId);
    return g;
  }, []);

  const refreshLatest = useCallback(async () => {
    const gid = groupIdRef.current;
    if (!gid) return;
    try {
      const page = await fetchJson<{ items: TimelineMsg[]; nextCursor: string | null }>(
        `/api/groups/${gid}/messages?limit=${PAGE_LIMIT}`,
      );
      mergePage(page.items, page.nextCursor, MERGE_LATEST);
    } catch {
      // WS/轮询触发的刷新失败不打扰用户，下次再补
    }
  }, [mergePage]);

  const loadFirst = useCallback(async (gid: string) => {
    try {
      const [g, r] = await Promise.all([
        fetchJson<Group>(`/api/groups/${gid}`),
        fetchJson<{ items: TimelineMsg[]; nextCursor: string | null }>(`/api/groups/${gid}/messages?limit=${PAGE_LIMIT}`),
      ]);
      setGroup(g);
      setRuns([]);
      setRunsErr('');
      seenRef.current = {};
      mergePage(r.items, r.nextCursor, MERGE_RESET);
      setErr('');
      if (g.members.length > 0) setSendAccount((cur) => cur || g.members[0].accountId);
      loadRuns(gid);
    } catch (e) {
      setErr(formatError(e, '加载失败'));
    }
  }, [mergePage, loadRuns]);

  useEffect(() => {
    if (id) loadFirst(id);
  }, [id, loadFirst]);

  // 5 秒兜底轮询：刷新最新消息、群详情与运行列表，应对 WS 短暂断开或状态异步收敛
  useEffect(() => {
    if (!id) return;
    const gid = id;
    const t = setInterval(() => {
      refreshLatest();
      loadGroup(gid).catch(() => undefined);
      loadRuns(gid);
    }, POLL_INTERVAL_MS);
    return () => clearInterval(t);
  }, [id, refreshLatest, loadGroup, loadRuns]);

  // 加载更早：用服务端下发的 nextCursor 链式翻页；插入前记录滚动位置，渲染后补偿，消除视角跳动
  const loadMore = async () => {
    if (!id || !nextCursor || loadingMore) return;
    const el = timelineRef.current;
    const prevScrollHeight = el ? el.scrollHeight : 0;
    const prevScrollTop = el ? el.scrollTop : 0;
    setLoadingMore(true);
    setHistoryErr('');
    try {
      const page = await fetchJson<{ items: TimelineMsg[]; nextCursor: string | null }>(
        `/api/groups/${id}/messages?before=${encodeURIComponent(nextCursor)}&limit=${PAGE_LIMIT}`,
      );
      mergePage(page.items, page.nextCursor, MERGE_OLDER);
      requestAnimationFrame(() => {
        const cur = timelineRef.current;
        if (cur) cur.scrollTop = prevScrollTop + (cur.scrollHeight - prevScrollHeight);
      });
    } catch (e) {
      setHistoryErr(formatError(e, '加载更早失败'));
    } finally {
      setLoadingMore(false);
    }
  };

  // WS message 事件只带 { groupId, msgId, isOwn }：拉最新一页合并（去重后追加）
  const onWsMessage = useCallback(async (f: WsFrame) => {
    const p = f.payload as { groupId?: string; msgId?: string };
    if (p.groupId !== groupIdRef.current) return;
    await refreshLatest();
  }, [refreshLatest]);
  useWs([EVT_MESSAGE], onWsMessage);

  // 成员被踢、账号终态退群或群变为 unreachable 时立即更新群详情与最新消息
  useWs([EVT_GROUP_UPDATED, EVT_ACCOUNT_TERMINAL], () => {
    const gid = groupIdRef.current;
    if (!gid) return;
    loadGroup(gid).catch(() => undefined);
    refreshLatest();
    loadRuns(gid);
  });

  // agent run / sequence run 事件到达时刷新运行列表
  useWs([EVT_AGENT_RUN], () => {
    if (!id) return;
    loadRuns(id);
  });

  const send = async (e?: FormEvent) => {
    if (e) e.preventDefault();
    if (!id || !sendText.trim() || sending) return;
    setSendMsg('');
    setSending(true);
    try {
      await postJson(`/api/groups/${id}/send`, { accountId: sendAccount, text: sendText });
      setSendText('');
      setSendMsg('已受理，等待网关发出');
      // 成功后立即拉取一次，使 queued/accepted/failed 立即出现在时间线底部
      await refreshLatest();
      // 自己主动发送后滚到底部
      requestAnimationFrame(() => {
        const el = timelineRef.current;
        if (el) el.scrollTop = el.scrollHeight;
      });
      setShowNewMsg(false);
    } catch (ex) {
      setSendMsg(formatError(ex, '发送失败'));
      // 发送失败也刷新一次，让 failed 状态行尽快可见
      await refreshLatest();
    } finally {
      setSending(false);
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
      setGroupMsg('');
    } catch (e) {
      setGroupMsg(formatError(e, '更新失败'));
    }
  };

  // 全员退群：后台 job，轮询直到完成，完成后刷新群详情
  const leaveAll = async () => {
    if (!id || leaving) return;
    setLeaving(true);
    setGroupMsg('');
    try {
      const r = await postJson<{ jobId: string }>(`/api/groups/${id}/leave-all`);
      const delay = (ms: number) => {
        const { promise, resolve } = Promise.withResolvers<void>();
        setTimeout(resolve, ms);
        return promise;
      };
      for (let i = 0; i < LEAVE_JOB_ROUNDS; i++) {
        await delay(LEAVE_JOB_DELAY_MS);
        try {
          const job = await fetchJson<{ status: string; errors: Array<{ step: string; code: string }> }>(`/api/jobs/${r.jobId}`);
          if (job.status === JOB_STATUS_FINISHED) {
            setGroupMsg(`全员退群完成（job ${r.jobId}）`);
            setConfirmingLeave(false);
            await loadGroup(id);
            refreshLatest();
            return;
          }
          if (job.status === JOB_STATUS_FAILED || job.errors.length > 0) {
            setGroupMsg(`全员退群失败：${job.errors.map((x) => `${x.step}:${x.code}`).join('；')}`);
            return;
          }
        } catch {
          // 单次抖动继续下一次轮询
          continue;
        }
      }
      setGroupMsg('退群仍在进行中，稍后刷新查看');
    } catch (e) {
      setGroupMsg(formatError(e, '全员退群失败'));
    } finally {
      setLeaving(false);
    }
  };

  const onTimelineScroll = () => {
    const el = timelineRef.current;
    if (!el) return;
    if (el.scrollHeight - el.scrollTop - el.clientHeight < BOTTOM_THRESHOLD_PX) setShowNewMsg(false);
  };

  // 页面级加载错误：渲染错误卡片并提供重试，不再显示加载中
  if (!group) {
    if (err) {
      return (
        <div className="card">
          <div className="err">{err}</div>
          <div style={{ marginTop: 8 }}>
            <button onClick={() => { if (id) loadFirst(id); }}>重试</button>
          </div>
        </div>
      );
    }
    return <div className="card">加载中…</div>;
  }

  const canLeaveAll = !isViewer && group.status !== GROUP_STATUS_LEFT;

  return (
    <div>
      <ReadOnlyNote />
      <div className="card">
        <h3>群 {group.id.slice(0, 8)}…（{group.status}）</h3>
        {group.status === GROUP_STATUS_UNREACHABLE && (
          <div className="note">群当前不可达，发送与退群可能失败，等待网关恢复后会自动同步</div>
        )}
        {groupMsg && <div className="note">{groupMsg}</div>}
        <div className="form-row">
          <span>成员：</span>
          {group.members.map((m) => (
            <span key={m.accountId} style={{ marginRight: 8 }}>
              {m.accountId}/{m.platformUserId}{' '}
              <span className={`badge ${roleBadgeClass(m.role)}`}>{m.role}</span>
            </span>
          ))}
          {group.members.length === 0 && <span>（无成员）</span>}
        </div>
        {group.activeSequenceRunId && (
          <div className="form-row">
            <Link to={`/sequences?runId=${encodeURIComponent(group.activeSequenceRunId)}&groupId=${encodeURIComponent(group.id)}`}>查看运行中的序列</Link>
          </div>
        )}
        {!isViewer && (
          <div className="form-row">
            <label><input type="checkbox" checked={group.agentEnabled} onChange={(e) => toggle({ agentEnabled: e.target.checked })} /> agentEnabled</label>
            <label><input type="checkbox" checked={group.autoKickEnabled} onChange={(e) => toggle({ autoKickEnabled: e.target.checked })} /> autoKickEnabled</label>
          </div>
        )}
        {canLeaveAll && (
          <div className="form-row">
            {!confirmingLeave ? (
              <button className="danger" onClick={() => setConfirmingLeave(true)}>全员退群</button>
            ) : (
              <>
                <span>确认全员退群？</span>
                <button className="danger" onClick={() => { void leaveAll(); }} disabled={leaving}>{leaving ? '退群中…' : '确认'}</button>
                <button onClick={() => setConfirmingLeave(false)} disabled={leaving}>取消</button>
              </>
            )}
          </div>
        )}
      </div>

      <div className="card">
        <h3>消息时间线</h3>
        {hasMore && (
          <div style={{ marginBottom: 8 }}>
            <button onClick={loadMore} disabled={loadingMore}>{loadingMore ? '加载中…' : '加载更早'}</button>
            {historyErr && <span className="err" style={{ marginLeft: 8 }}>{historyErr}</span>}
          </div>
        )}
        <div className="timeline" ref={timelineRef} onScroll={onTimelineScroll}>
          {messages.map((m) => (
            <div className="msg" key={m.clientMsgId ?? m.msgId}>
              <div className="meta">
                {m.sentAt} · {m.senderPlatformUserId} {m.isOwn ? '(我)' : ''}{' '}
                {m.isOwn && m.deliveryStatus && (
                  <span className={`badge ${deliveryBadgeClass(m.deliveryStatus)}`}>
                    {m.deliveryStatus}{m.failCode ? `:${m.failCode}` : ''}
                  </span>
                )}
              </div>
              <div>{m.text}</div>
            </div>
          ))}
          {messages.length === 0 && <div style={{ color: '#999' }}>暂无消息</div>}
        </div>
        {showNewMsg && (
          <div style={{ marginTop: 8 }}>
            <button onClick={scrollToBottom}>有新消息，点击回到底部</button>
          </div>
        )}
        {!isViewer && (
          <div style={{ marginTop: 12 }}>
            <form onSubmit={(e) => { void send(e); }}>
              <div className="form-row">
                <select value={sendAccount} onChange={(e) => setSendAccount(e.target.value)}>
                  {group.members.map((m) => <option key={m.accountId} value={m.accountId}>{m.accountId}</option>)}
                </select>
                <input value={sendText} onChange={(e) => setSendText(e.target.value)} placeholder="输入消息文本" style={{ flex: 1 }} />
                <button type="submit" className="primary" disabled={sending || !sendText.trim()}>{sending ? '发送中…' : '发送'}</button>
              </div>
            </form>
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
              <tr key={r.id} style={r.status === RUN_STATUS_BLOCKED ? { background: '#fff0f0' } : undefined}>
                <td><Link to={`/agent-runs/${r.id}`}>{r.id.slice(0, 8)}…</Link></td>
                <td>
                  {r.status === RUN_STATUS_BLOCKED
                    ? <span className={`badge ${BADGE_RED}`}>{RUN_STATUS_BLOCKED}（需处理）</span>
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
