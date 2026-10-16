import { useCallback, useEffect, useRef, useState, type FormEvent } from 'react';
import { Link } from 'react-router-dom';
import { fetchJson, formatError, postJson, type Account, type Group } from '../api';
import { useAuth } from '../auth';
import { useWs } from '../ws';
import { ReadOnlyNote } from '../components/ui';

// 状态与事件常量（顶部统一定义，禁止在函数体/JSX 中写字面量）
const JOB_STATUS_FINISHED = 'finished';
const JOB_STATUS_FAILED = 'failed';
const GROUP_STATUS_ACTIVE = 'active';
const ACCOUNT_STATUS_ONLINE = 'online';
const EVT_GROUP_UPDATED = 'group_updated';
const EVT_AGENT_RUN = 'agent_run';
const EVT_SEQUENCE_RUN = 'sequence_run';
const POLL_INTERVAL_MS = 10_000;
const JOB_POLL_ROUNDS = 60;
const JOB_POLL_DELAY_MS = 1000;
const DEFAULT_CREATOR = 'acc-1';
const DEFAULT_MEMBERS = 'acc-2';

// 群组列表页：建群表单 + 群列表；建群是异步 job，需要轮询 jobs 状态
export default function GroupsPage() {
  const { isViewer } = useAuth();
  const [groups, setGroups] = useState<Group[]>([]);
  const [accounts, setAccounts] = useState<Account[]>([]);
  const [err, setErr] = useState('');
  const [creator, setCreator] = useState(DEFAULT_CREATOR);
  const [members, setMembers] = useState(DEFAULT_MEMBERS);
  const [jobMsg, setJobMsg] = useState('');
  const [loading, setLoading] = useState(true);
  const [creating, setCreating] = useState(false);
  const [pollingJobId, setPollingJobId] = useState('');
  // 卸载标记：防止 waitJob 在组件卸载后继续更新 state
  const mountedRef = useRef(true);

  const load = useCallback(async () => {
    try {
      const data = await fetchJson<Group[]>('/api/groups');
      if (!mountedRef.current) return;
      setGroups(data);
      setErr('');
    } catch (e) {
      if (!mountedRef.current) return;
      setErr(formatError(e, '加载失败'));
    } finally {
      if (mountedRef.current) setLoading(false);
    }
  }, []);

  // 账号列表静默辅助：失败不打扰群组主流程
  const loadAccounts = useCallback(async () => {
    try {
      const data = await fetchJson<Account[]>('/api/accounts');
      if (!mountedRef.current) return;
      setAccounts(data);
    } catch {
      // 静默辅助失败不提示，下次轮询再补
    }
  }, []);

  useEffect(() => {
    mountedRef.current = true;
    load();
    loadAccounts();
    const t = setInterval(() => {
      load();
      loadAccounts();
    }, POLL_INTERVAL_MS);
    return () => {
      mountedRef.current = false;
      clearInterval(t);
    };
  }, [load, loadAccounts]);

  // WS 实时刷新 + 10 秒轮询兜底
  useWs([EVT_GROUP_UPDATED, EVT_AGENT_RUN, EVT_SEQUENCE_RUN], () => {
    load();
  });

  // 轮询建群 job 直到结束；单次网络抖动继续下一次轮询而非放弃
  const waitJob = useCallback(async (jobId: string) => {
    const delay = (ms: number) => {
      const { promise, resolve } = Promise.withResolvers<void>();
      setTimeout(resolve, ms);
      return promise;
    };
    for (let i = 0; i < JOB_POLL_ROUNDS; i++) {
      await delay(JOB_POLL_DELAY_MS);
      if (!mountedRef.current) return;
      try {
        const job = await fetchJson<{ status: string; errors: Array<{ step: string; code: string }> }>(`/api/jobs/${jobId}`);
        if (!mountedRef.current) return;
        if (job.status === JOB_STATUS_FINISHED) {
          setJobMsg(`建群成功（job ${jobId}）`);
          setPollingJobId('');
          load();
          return;
        }
        if (job.status === JOB_STATUS_FAILED || job.errors.length > 0) {
          setJobMsg(`建群失败：${job.errors.map((e) => `${e.step}:${e.code}`).join('；')}`);
          setPollingJobId('');
          return;
        }
      } catch {
        // 单次抖动：继续下一次轮询
        continue;
      }
    }
    if (mountedRef.current) {
      setJobMsg('建群仍在进行中，稍后刷新查看');
      setPollingJobId('');
    }
  }, [load]);

  const create = async (e?: FormEvent) => {
    if (e) e.preventDefault();
    if (creating) return;
    setJobMsg('');
    setCreating(true);
    const memberAccountIds = members.split(/[,\s]+/).map((s) => s.trim()).filter(Boolean);
    try {
      const r = await postJson<{ jobId: string }>('/api/groups', { creatorAccountId: creator, memberAccountIds });
      if (!mountedRef.current) return;
      setPollingJobId(r.jobId);
      setJobMsg(`建群任务已提交（job ${r.jobId}），等待完成…`);
      setCreating(false);
      await waitJob(r.jobId);
    } catch (ex) {
      if (!mountedRef.current) return;
      setJobMsg(`建群失败：${formatError(ex)}`);
    } finally {
      if (mountedRef.current) setCreating(false);
    }
  };

  const onlineAccounts = accounts.filter((a) => a.status === ACCOUNT_STATUS_ONLINE);
  const busy = creating || pollingJobId !== '';

  if (loading) return <div className="card">加载中…</div>;

  return (
    <div>
      <ReadOnlyNote />
      {!isViewer && (
        <div className="card">
          <h3>新建群</h3>
          <form onSubmit={(e) => { void create(e); }}>
            <div className="form-row">
              <span>创建者账号：</span>
              <input value={creator} onChange={(e) => setCreator(e.target.value)} placeholder={DEFAULT_CREATOR} />
              <span>成员账号（逗号分隔，至少 1 个，不含群主）：</span>
              <input value={members} onChange={(e) => setMembers(e.target.value)} placeholder={DEFAULT_MEMBERS} style={{ flex: 1 }} />
              <button type="submit" className="primary" disabled={busy}>{creating ? '提交中…' : pollingJobId !== '' ? '建群中…' : '建群'}</button>
            </div>
          </form>
          <div style={{ fontSize: 12, color: '#666', marginBottom: 8 }}>
            {onlineAccounts.length > 0
              ? `当前在线账号：${onlineAccounts.map((a) => a.id).join('、')}（可直接填入群主/成员）`
              : '当前暂无在线账号，可先到账号页将账号上线'}
          </div>
          {jobMsg && <div className="note">{jobMsg}</div>}
        </div>
      )}
      <div className="card">
        <div className="toolbar">
          <h3>群组列表</h3>
          <button className="btn-sm" onClick={() => { load(); loadAccounts(); }}>刷新</button>
        </div>
        {err && <div className="err">{err}</div>}
        <table className="list">
          <thead>
            <tr><th>群 ID</th><th>状态</th><th>成员数</th><th>Agent</th><th>运行中</th></tr>
          </thead>
          <tbody>
            {groups.map((g) => (
              <tr key={g.id} style={g.status === GROUP_STATUS_ACTIVE ? undefined : { opacity: 0.7 }}>
                <td><Link to={`/groups/${g.id}`}>{g.id.slice(0, 8)}…</Link></td>
                <td>{g.status}</td>
                <td>{g.members.length}</td>
                <td>{g.agentEnabled ? '开' : '关'}</td>
                <td>
                  {g.activeAgentRunId ? <Link to={`/agent-runs/${g.activeAgentRunId}`}>agent 运行中</Link> : null}
                  {g.activeSequenceRunId ? (
                    <Link to={`/sequences?runId=${encodeURIComponent(g.activeSequenceRunId)}&groupId=${encodeURIComponent(g.id)}`}>序列运行中</Link>
                  ) : null}
                  {!g.activeAgentRunId && !g.activeSequenceRunId ? '-' : null}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {groups.length === 0 && !err && <div style={{ color: '#999' }}>暂无群组</div>}
      </div>
    </div>
  );
}
