import { useCallback, useEffect, useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import { fetchJson, formatError, type AgentRun, type AgentStep } from '../api';
import { useWs } from '../ws';

// 运行状态与步骤类型常量
const RUN_STATUS_BLOCKED = 'blocked';
const RUN_STATUS_RUNNING = 'running';
const STEP_KIND_PROTO_ERR = 'protocol_error';
const STEP_KIND_FINAL = 'final';
const WS_EVENT_AGENT_RUN = 'agent_run';
const POLL_INTERVAL_MS = 3000;

// 单步展示：kind/工具名/入参/resultSummary/审计结论/错误码；协议错误步可展开 rawResponse
function StepCard({ step, n }: { step: AgentStep; n: number }) {
  const [open, setOpen] = useState(false);
  const isProto = step.kind === STEP_KIND_PROTO_ERR;
  return (
    <div className="card" style={isProto ? { border: '1px solid #f5b7b1', background: '#fff8f8' } : undefined}>
      <div>
        <strong>步骤 {n}</strong>{' '}
        <span className={`badge ${isProto ? 'red' : step.kind === STEP_KIND_FINAL ? 'green' : ''}`}>{step.kind}</span>{' '}
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
      setErr(formatError(e, '加载失败'));
    }
  }, [id]);

  useEffect(() => {
    load();
  }, [load]);

  useWs([WS_EVENT_AGENT_RUN], (f) => {
    const p = f.payload as { runId?: string };
    if (p.runId === id) load();
  });

  // running 状态下定时轮询兜底（WS 漏事件时仍能跟进）
  useEffect(() => {
    if (run?.status !== RUN_STATUS_RUNNING) return;
    const t = setInterval(load, POLL_INTERVAL_MS);
    return () => clearInterval(t);
  }, [run?.status, load]);

  if (err && !run) {
    return (
      <div className="card">
        <div className="err">{err}</div>
        <div style={{ marginTop: 8 }}>
          <button onClick={() => load()}>重试</button>
        </div>
      </div>
    );
  }
  if (!run) return <div className="card">加载中…</div>;

  return (
    <div>
      {run.status === RUN_STATUS_BLOCKED && (
        <div className="card" style={{ background: '#fff0f0', border: '1px solid #ff4d4f' }}>
          <strong style={{ color: '#c00' }}>该运行已被阻塞（endReason: {run.endReason}），需要人工处理。</strong>
        </div>
      )}
      <div className="card">
        <div className="toolbar">
          <h3>Agent 运行 {run.id.slice(0, 8)}…</h3>
          <button className="btn-sm" onClick={() => load()}>刷新</button>
        </div>
        <div>群：<Link to={`/groups/${run.groupId}`}>{run.groupId.slice(0, 8)}…</Link></div>
        <div>状态：{run.status === RUN_STATUS_BLOCKED
          ? <span className="badge red">{RUN_STATUS_BLOCKED}</span>
          : <span className="badge">{run.status}</span>} endReason：{run.endReason ?? '-'}</div>
        <div>摘要：{run.summary ?? '-'}</div>
      </div>
      <h3>步骤（共 {(run.steps ?? []).length} 步）</h3>
      {(run.steps ?? []).map((s, i) => <StepCard key={i} step={s} n={i + 1} />)}
      {(run.steps ?? []).length === 0 && <div className="card">暂无步骤</div>}
    </div>
  );
}
