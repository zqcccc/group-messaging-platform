import { useCallback, useEffect, useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import { ApiError, fetchJson, type AgentRun, type AgentStep } from '../api';
import { useWs } from '../ws';

// 单步展示：kind/工具名/入参/resultSummary/审计结论/错误码；协议错误步可展开 rawResponse
function StepCard({ step, n }: { step: AgentStep; n: number }) {
  const [open, setOpen] = useState(false);
  const isProto = step.kind === 'protocol_error';
  return (
    <div className="card" style={isProto ? { borderLeft: '4px solid #ff4d4f' } : undefined}>
      <div>
        <strong>步骤 {n}</strong>{' '}
        <span className={`badge ${isProto ? 'red' : step.kind === 'final' ? 'green' : ''}`}>{step.kind}</span>{' '}
        {step.name && <span>工具：<code>{step.name}</code></span>}{' '}
        {step.isError && step.errorCode && <span className="badge red">{step.errorCode}</span>}
      </div>
      <div style={{ marginTop: 8, fontSize: 13 }}>
        <div>toolUseId：{step.toolUseId ?? '-'}</div>
        <div>入参：<code>{step.input === null || step.input === undefined ? '-' : JSON.stringify(step.input)}</code></div>
        <div>结果摘要：{step.resultSummary ?? '-'}</div>
        <div>审计结论：{step.auditVerdict ?? '-'}</div>
      </div>
      {isProto && step.rawResponse !== null && step.rawResponse !== undefined && (
        <div style={{ marginTop: 8 }}>
          <button onClick={() => setOpen(!open)}>{open ? '收起原始响应' : '查看原始响应（截断 2KB）'}</button>
          {open && <pre className="raw">{step.rawResponse}</pre>}
        </div>
      )}
    </div>
  );
}

// Agent 运行详情页：GET /api/agent-runs/:id；WS agent_run 事件到达时刷新
export default function AgentRunPage() {
  const { id } = useParams<{ id: string }>();
  const [run, setRun] = useState<AgentRun | null>(null);
  const [err, setErr] = useState('');

  const load = useCallback(async () => {
    if (!id) return;
    try {
      const data = await fetchJson<AgentRun>(`/api/agent-runs/${id}`);
      setRun(data);
      setErr('');
    } catch (e) {
      setErr(e instanceof ApiError ? `${e.message}（${e.code}）` : '加载失败');
    }
  }, [id]);

  useEffect(() => {
    load();
  }, [load]);

  useWs(['agent_run'], (f) => {
    const p = f.payload as { runId?: string };
    if (p.runId === id) load();
  });

  if (!run) return <div className="card">加载中…{err && <div className="err">{err}</div>}</div>;

  return (
    <div>
      {run.status === 'blocked' && (
        <div className="card" style={{ background: '#fff0f0', border: '1px solid #ff4d4f' }}>
          <strong style={{ color: '#c00' }}>该运行已被阻塞（endReason: {run.endReason}），需要人工处理。</strong>
        </div>
      )}
      <div className="card">
        <h3>Agent 运行 {run.id.slice(0, 8)}…</h3>
        <div>群：<Link to={`/groups/${run.groupId}`}>{run.groupId.slice(0, 8)}…</Link></div>
        <div>状态：{run.status === 'blocked'
          ? <span className="badge red">blocked</span>
          : <span className="badge">{run.status}</span>} endReason：{run.endReason ?? '-'}</div>
        <div>摘要：{run.summary ?? '-'}</div>
      </div>
      <h3>步骤（共 {(run.steps ?? []).length} 步）</h3>
      {(run.steps ?? []).map((s, i) => <StepCard key={i} step={s} n={i + 1} />)}
      {(run.steps ?? []).length === 0 && <div className="card">暂无步骤</div>}
    </div>
  );
}
