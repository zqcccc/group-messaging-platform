import { useCallback, useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { ApiError, fetchJson, postJson, type Group } from '../api';
import { useAuth } from '../auth';
import { ReadOnlyNote } from '../components/ui';

// 群组列表页：建群表单 + 群列表；建群是异步 job，需要轮询 jobs 状态
export default function GroupsPage() {
  const { isViewer } = useAuth();
  const [groups, setGroups] = useState<Group[]>([]);
  const [err, setErr] = useState('');
  const [creator, setCreator] = useState('acc-1');
  const [members, setMembers] = useState('acc-2');
  const [jobMsg, setJobMsg] = useState('');

  const load = useCallback(async () => {
    try {
      const data = await fetchJson<Group[]>('/api/groups');
      setGroups(data);
      setErr('');
    } catch (e) {
      setErr(e instanceof ApiError ? `${e.message}（${e.code}）` : '加载失败');
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  // 轮询建群 job 直到结束
  const waitJob = async (jobId: string) => {
    const delay = (ms: number) => {
      const { promise, resolve } = Promise.withResolvers<void>();
      setTimeout(resolve, ms);
      return promise;
    };
    for (let i = 0; i < 60; i++) {
      await delay(1000);
      try {
        const job = await fetchJson<{ status: string; errors: Array<{ step: string; code: string }> }>(`/api/jobs/${jobId}`);
        if (job.status === 'finished') {
          setJobMsg(`建群成功（job ${jobId}）`);
          load();
          return;
        }
        if (job.status === 'failed' || job.errors.length > 0) {
          setJobMsg(`建群失败：${job.errors.map((e) => `${e.step}:${e.code}`).join('；')}`);
          return;
        }
      } catch (e) {
        setJobMsg(e instanceof ApiError ? `建群状态查询失败：${e.message}（${e.code}）` : '建群状态查询失败');
        return;
      }
    }
    setJobMsg('建群仍在进行中，稍后刷新查看');
  };

  const create = async () => {
    setJobMsg('');
    const memberAccountIds = members.split(/[,\s]+/).map((s) => s.trim()).filter(Boolean);
    try {
      const r = await postJson<{ jobId: string }>('/api/groups', { creatorAccountId: creator, memberAccountIds });
      setJobMsg(`建群任务已提交（job ${r.jobId}），等待完成…`);
      waitJob(r.jobId);
    } catch (e) {
      setJobMsg(e instanceof ApiError ? `建群失败：${e.message}（${e.code}）` : '建群失败');
    }
  };

  return (
    <div>
      <ReadOnlyNote />
      {!isViewer && (
        <div className="card">
          <h3>新建群</h3>
          <div className="form-row">
            <span>创建者账号：</span>
            <input value={creator} onChange={(e) => setCreator(e.target.value)} placeholder="acc-1" />
            <span>成员账号（逗号分隔，至少 1 个，不含群主）：</span>
            <input value={members} onChange={(e) => setMembers(e.target.value)} placeholder="acc-2" style={{ flex: 1 }} />
            <button className="primary" onClick={create}>建群</button>
          </div>
          {jobMsg && <div className="note">{jobMsg}</div>}
        </div>
      )}
      <div className="card">
        <h3>群组列表</h3>
        {err && <div className="err">{err}</div>}
        <table className="list">
          <thead>
            <tr><th>群 ID</th><th>状态</th><th>成员数</th><th>Agent</th><th>运行中</th></tr>
          </thead>
          <tbody>
            {groups.map((g) => (
              <tr key={g.id}>
                <td><Link to={`/groups/${g.id}`}>{g.id.slice(0, 8)}…</Link></td>
                <td>{g.status}</td>
                <td>{g.members.length}</td>
                <td>{g.agentEnabled ? '开' : '关'}</td>
                <td>
                  {g.activeAgentRunId ? <Link to={`/agent-runs/${g.activeAgentRunId}`}>agent 运行中</Link> : null}
                  {g.activeSequenceRunId ? ` 序列运行中` : null}
                  {!g.activeAgentRunId && !g.activeSequenceRunId ? '-' : null}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
