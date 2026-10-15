import { useCallback, useEffect, useState } from 'react';
import { ApiError, fetchJson, postJson, type Sequence, type SequenceRun } from '../api';
import { useAuth } from '../auth';
import { useWs } from '../ws';
import { ReadOnlyNote } from '../components/ui';

// 占位符 key 规则：{key}，key 匹配 [A-Za-z0-9_]+
const PLACEHOLDER_RE = /\{([A-Za-z0-9_]+)\}/g;

function keysOf(text: string): string[] {
  const out: string[] = [];
  const re = new RegExp(PLACEHOLDER_RE);
  let m: RegExpExecArray | null;
  while ((m = re.exec(text)) !== null) {
    if (!out.includes(m[1])) out.push(m[1]);
  }
  return out;
}

interface PreviewRow {
  stepIndex: number;
  accountRole: string;
  text: string;
  resolved: Array<{ key: string; value: string; source: string }>;
}

// 本地逐 key 解析预检：复刻 B1 取值规则
// - vars 里 "" 视为未提供；stepVars 里 "" 表示这一步不改
// - 某步给了值则从该步起后续沿用，直到更晚的步骤再给值
// - varSources 标最初给出该值的来源（default 或 step:N）
function previewSequence(
  steps: Array<{ index: number; accountRole: string; text: string }>,
  vars: Record<string, string>,
  stepVars: Record<string, Record<string, string>>,
): { rows: PreviewRow[]; error: { stepIndex: number; key: string } | null } {
  const cur: Record<string, string> = {};
  const src: Record<string, string> = {};
  Object.entries(vars).forEach(([k, v]) => {
    if (v !== '') {
      cur[k] = v;
      src[k] = 'default';
    }
  });
  const rows: PreviewRow[] = [];
  const ordered = steps.slice().sort((a, b) => a.index - b.index);
  for (const s of ordered) {
    const overrides = stepVars[String(s.index)] ?? {};
    Object.entries(overrides).forEach(([k, v]) => {
      if (v !== '') {
        cur[k] = v;
        src[k] = `step:${s.index}`;
      }
    });
    const resolved: PreviewRow['resolved'] = [];
    for (const key of keysOf(s.text)) {
      if (!(key in cur)) {
        return { rows, error: { stepIndex: s.index, key } };
      }
      resolved.push({ key, value: cur[key], source: src[key] });
    }
    rows.push({ stepIndex: s.index, accountRole: s.accountRole, text: s.text, resolved });
  }
  return { rows, error: null };
}

// 文本域解析：支持 JSON 或 "key=value" 每行一个；解析失败抛错提示
function parseVarsInput(text: string): Record<string, string> {
  const t = text.trim();
  if (!t) return {};
  if (t.startsWith('{')) {
    const obj = JSON.parse(t) as Record<string, unknown>;
    const out: Record<string, string> = {};
    Object.entries(obj).forEach(([k, v]) => { out[k] = String(v ?? ''); });
    return out;
  }
  const out: Record<string, string> = {};
  t.split('\n').forEach((line) => {
    const l = line.trim();
    if (!l || l.startsWith('#')) return;
    const eq = l.indexOf('=');
    if (eq < 0) throw new Error(`vars 行格式错误（应为 key=value）：${l}`);
    out[l.slice(0, eq).trim()] = l.slice(eq + 1).trim();
  });
  return out;
}

// 序列运行页：序列选择 + vars/stepVars 表单 → 本地预检弹窗 → POST 启动；
// 422 显示 stepIndex + key；运行中进度条 + 每步状态/时间戳
export default function SequencesPage() {
  const { isViewer } = useAuth();
  const [sequences, setSequences] = useState<Sequence[]>([]);
  const [seqId, setSeqId] = useState('');
  const [groupId, setGroupId] = useState('');
  const [varsText, setVarsText] = useState('event=发布会\ntime=今晚八点\nlocation=共享盘/第二季度');
  const [stepVarsText, setStepVarsText] = useState('{\n  "2": { "location": "共享盘/第二季度" }\n}');
  const [err, setErr] = useState('');
  const [preview, setPreview] = useState<PreviewRow[] | null>(null);
  const [previewErr, setPreviewErr] = useState('');
  const [showPreview, setShowPreview] = useState(false);
  const [runId, setRunId] = useState('');
  const [run, setRun] = useState<SequenceRun | null>(null);
  const [startMsg, setStartMsg] = useState('');

  // 后端无 GET /api/sequences 列表接口：404/405 时列表留空，允许手动填 sequenceId
  const load = useCallback(async () => {
    try {
      const data = await fetchJson<Sequence[]>('/api/sequences');
      setSequences(data);
      setErr('');
      if (!seqId && data.length > 0) setSeqId(data[0].id);
    } catch (e) {
      if (e instanceof ApiError && (e.status === 404 || e.status === 405)) {
        // 后端无列表接口：列表留空、手动填 sequenceId 或下方新建序列
        setSequences([]);
      } else {
        setErr(e instanceof ApiError ? `${e.message}（${e.code}）` : '加载失败');
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  const loadRun = useCallback(async (id: string) => {
    try {
      const r = await fetchJson<SequenceRun>(`/api/sequence-runs/${id}`);
      setRun(r);
    } catch {
      // 轮询间隙失败：保留上一次状态
    }
  }, []);

  // 运行中轮询（3s）+ WS sequence_run 事件刷新
  useEffect(() => {
    if (!runId) return;
    loadRun(runId);
    const t = setInterval(() => loadRun(runId), 3000);
    return () => clearInterval(t);
  }, [runId, loadRun]);

  useWs(['sequence_run'], (f) => {
    const p = f.payload as { runId?: string };
    if (p.runId && p.runId === runId) loadRun(runId);
  });

  // 手动填 sequenceId（后端无列表接口时用）；新建序列成功后也会回填到这里
  const [manualSeqId, setManualSeqId] = useState('');
  // 生效的 sequenceId：下拉选中优先，否则用手动输入框
  const effectiveSeqId = seqId || manualSeqId.trim();
  // 新建序列表单：POST /api/sequences { name, steps } → { id }，成功后回填到启动表单
  const [newSeqName, setNewSeqName] = useState('新序列');
  const [newSeqSteps, setNewSeqSteps] = useState('[\n  { "index": 1, "accountRole": "admin", "text": "{event} 将于 {time} 开始", "delaySeconds": 10 },\n  { "index": 2, "accountRole": "member", "text": "资料已上传到 {location}", "delaySeconds": 5 }\n]');
  const [createMsg, setCreateMsg] = useState('');

  const createSequence = async () => {
    setCreateMsg('');
    let steps: unknown;
    try {
      steps = JSON.parse(newSeqSteps);
    } catch {
      setCreateMsg('新建失败：steps 不是合法 JSON');
      return;
    }
    try {
      const r = await postJson<{ id: string }>('/api/sequences', { name: newSeqName.trim() || '未命名序列', steps });
      setCreateMsg(`已创建序列（id ${r.id}），已填入启动表单`);
      setSeqId(r.id);
      setManualSeqId(r.id);
      setSequences((prev) => {
        const stepsArr = Array.isArray(steps) ? (steps as Sequence['steps']) : [];
        return [...prev, { id: r.id, name: newSeqName.trim() || '未命名序列', steps: stepsArr }];
      });
    } catch (e) {
      setCreateMsg(e instanceof ApiError ? `新建失败：${e.message}（${e.code}）` : '新建失败');
    }
  };

  const selected = sequences.find((s) => s.id === seqId) ?? null;

  const doPreview = () => {
    setPreviewErr('');
    setPreview(null);
    if (!selected && !manualSeqId.trim()) {
      setPreviewErr('请先选择序列，或手动填写 sequenceId（后端无列表接口）');
      return;
    }
    // 手动 id 无本地步骤可预检：提示直接启动，由后端 422 返回 stepIndex/key
    if (!selected && manualSeqId.trim()) {
      setPreviewErr('手动填写的 sequenceId 无本地步骤可预检，请直接启动（缺 key 时后端会返回 stepIndex 与 key）');
      return;
    }
    const steps = (selected as Sequence).steps;
    let vars: Record<string, string>;
    let stepVars: Record<string, Record<string, string>>;
    try {
      vars = parseVarsInput(varsText);
    } catch (e) {
      setPreviewErr(e instanceof Error ? e.message : 'vars 解析失败');
      return;
    }
    try {
      const t = stepVarsText.trim();
      stepVars = t ? (JSON.parse(t) as Record<string, Record<string, string>>) : {};
    } catch {
      setPreviewErr('stepVars 不是合法 JSON');
      return;
    }
    const r = previewSequence(steps, vars, stepVars);
    if (r.error) {
      setPreviewErr(`预检不通过：stepIndex=${r.error.stepIndex}，key=${r.error.key}（占位符无法解析）`);
      return;
    }
    setPreview(r.rows);
    setShowPreview(true);
  };

  const start = async () => {
    setStartMsg('');
    if (!groupId.trim()) {
      setStartMsg('请填写群 ID');
      return;
    }
    let vars: Record<string, string>;
    let stepVars: Record<string, Record<string, string>>;
    try {
      vars = parseVarsInput(varsText);
      const t = stepVarsText.trim();
      stepVars = t ? (JSON.parse(t) as Record<string, Record<string, string>>) : {};
    } catch (e) {
      setStartMsg(`参数解析失败：${e instanceof Error ? e.message : '未知错误'}（vars 支持 JSON 或 key=value 每行一个，stepVars 只支持 JSON）`);
      return;
    }
    if (!effectiveSeqId) {
      setStartMsg('请先选择序列或填写 sequenceId');
      return;
    }
    try {
      const r = await postJson<{ runId: string }>(`/api/groups/${groupId.trim()}/sequence-runs`, {
        sequenceId: effectiveSeqId,
        vars,
        stepVars,
      });
      setRunId(r.runId);
      setRun(null);
      setStartMsg(`已启动（run ${r.runId}）`);
      setShowPreview(false);
      loadRun(r.runId);
    } catch (e) {
      if (e instanceof ApiError && e.code === 'UNRESOLVED_PLACEHOLDER') {
        setStartMsg(`预检不通过：stepIndex=${e.stepIndex}，key=${e.key}（${e.message}）`);
      } else {
        setStartMsg(e instanceof ApiError ? `启动失败：${e.message}（${e.code}）` : '启动失败');
      }
    }
  };

  const done = run ? run.steps.filter((s) => s.status !== 'pending').length : 0;
  const total = run ? run.steps.length : 0;

  return (
    <div>
      <ReadOnlyNote />
      <div className="card">
        <h3>序列运行</h3>
        {err && <div className="err">{err}</div>}
        <div className="form-row">
          <span>群 ID：</span>
          <input value={groupId} onChange={(e) => setGroupId(e.target.value)} placeholder="群 id（uuid）" style={{ flex: 1 }} />
        </div>
        <div className="form-row">
          <span>序列：</span>
          <select value={seqId} onChange={(e) => { setSeqId(e.target.value); setManualSeqId(''); }} style={{ flex: 1 }}>
            <option value="">（手动填写下方 sequenceId）</option>
            {sequences.map((s) => <option key={s.id} value={s.id}>{s.name}（{s.id.slice(0, 8)}…，{s.steps.length} 步）</option>)}
          </select>
        </div>
        {sequences.length === 0 && (
          <div className="form-row">
            <span>sequenceId：</span>
            <input value={manualSeqId} onChange={(e) => { setManualSeqId(e.target.value); setSeqId(''); }} placeholder="粘贴 POST /api/sequences 返回的 id，或下方新建" style={{ flex: 1 }} />
          </div>
        )}
        {!isViewer && sequences.length === 0 && (
          <div className="card" style={{ marginTop: 8 }}>
            <h4>新建序列（POST /api/sequences）</h4>
            <div className="form-row">
              <span>名称：</span>
              <input value={newSeqName} onChange={(e) => setNewSeqName(e.target.value)} style={{ flex: 1 }} />
            </div>
            <div className="form-row" style={{ alignItems: 'flex-start' }}>
              <span>steps（JSON 数组）：</span>
              <textarea value={newSeqSteps} onChange={(e) => setNewSeqSteps(e.target.value)} rows={5} style={{ flex: 1 }} />
            </div>
            <div className="form-row">
              <button onClick={createSequence}>创建序列</button>
            </div>
            {createMsg && <div className="note">{createMsg}</div>}
          </div>
        )}
        {selected && (
          <div style={{ fontSize: 13, color: '#666', marginBottom: 8 }}>
            {selected.steps.map((s) => (
              <div key={s.index}>步骤 {s.index} [{s.accountRole}，延迟 {s.delaySeconds}s]：{s.text}</div>
            ))}
          </div>
        )}
        {!isViewer && (
          <>
            <div className="form-row" style={{ alignItems: 'flex-start' }}>
              <span>vars（JSON 或 key=value 每行）：</span>
              <textarea value={varsText} onChange={(e) => setVarsText(e.target.value)} rows={4} style={{ flex: 1 }} />
            </div>
            <div className="form-row" style={{ alignItems: 'flex-start' }}>
              <span>stepVars（JSON）：</span>
              <textarea value={stepVarsText} onChange={(e) => setStepVarsText(e.target.value)} rows={4} style={{ flex: 1 }} />
            </div>
            <div className="form-row">
              <button onClick={doPreview}>本地预检</button>
              <button className="primary" onClick={start}>启动</button>
            </div>
          </>
        )}
        {previewErr && <div className="err">{previewErr}</div>}
        {startMsg && <div className="note">{startMsg}</div>}
      </div>

      {showPreview && preview && (
        <div className="modal-mask" onClick={() => setShowPreview(false)}>
          <div className="modal" onClick={(e) => e.stopPropagation()}>
            <h3>预检结果：每步 key 最终取值与来源</h3>
            {preview.map((row) => (
              <div key={row.stepIndex} style={{ marginBottom: 10 }}>
                <div><strong>步骤 {row.stepIndex}</strong> [{row.accountRole}]：{row.text}</div>
                {row.resolved.length === 0 && <div style={{ color: '#999' }}>无占位符</div>}
                {row.resolved.map((x) => (
                  <div key={x.key} style={{ fontSize: 13 }}>
                    <code>{`{${x.key}}`}</code> = {x.value} <span className="badge">{x.source === 'default' ? 'default' : x.source}</span>
                  </div>
                ))}
              </div>
            ))}
            <div className="form-row">
              {!isViewer && <button className="primary" onClick={start}>确认启动</button>}
              <button onClick={() => setShowPreview(false)}>关闭</button>
            </div>
          </div>
        </div>
      )}

      {run && (
        <div className="card">
          <h3>运行 {runId.slice(0, 12)}…（{run.status}，当前第 {run.currentStepIndex} 步）</h3>
          <div className="progress">
            <div style={{ width: total === 0 ? '0%' : `${Math.round((done / total) * 100)}%` }} />
          </div>
          <div style={{ fontSize: 13, margin: '6px 0' }}>{done}/{total} 步完成</div>
          <table className="list">
            <thead><tr><th>步骤</th><th>状态</th><th>计划/发送时间</th><th>clientMsgId</th><th>取值与来源</th></tr></thead>
            <tbody>
              {run.steps.map((s) => (
                <tr key={s.index}>
                  <td>{s.index}</td>
                  <td><span className={`badge ${s.status === 'sent' || s.status === 'accepted' ? 'green' : s.status === 'failed' ? 'red' : ''}`}>{s.status}</span></td>
                  <td>{s.scheduledAt ?? '-'} / {s.sentAt ?? '-'}</td>
                  <td>{s.clientMsgId ?? '-'}</td>
                  <td style={{ fontSize: 12 }}>
                    {Object.entries(s.resolvedVars).map(([k, v]) => (
                      <div key={k}><code>{`{${k}}`}</code>={v} <span className="badge">{s.varSources[k]}</span></div>
                    ))}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
