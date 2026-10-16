import { useCallback, useEffect, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import { ApiError, fetchJson, postJson, type Group, type Sequence, type SequenceRun } from '../api';
import { useAuth } from '../auth';
import { useWs } from '../ws';
import { ReadOnlyNote } from '../components/ui';

// 顶部常量：枚举、状态、来源、事件、键、参数名统一在此定义
const PLACEHOLDER_RE = /\{([A-Za-z0-9_]+)\}/g;
const VAR_SOURCE_DEFAULT = 'default';
const VAR_SOURCE_STEP_PREFIX = 'step:';
const STEP_STATUS_PENDING = 'pending';
const STEP_STATUS_SENT = 'sent';
const STEP_STATUS_FAILED = 'failed';
const STEP_STATUS_CANCELLED = 'cancelled';
const STEP_STATUS_ACCEPTED = 'accepted';
const ERR_UNRESOLVED_PLACEHOLDER = 'UNRESOLVED_PLACEHOLDER';
const WS_EVENT_SEQUENCE_RUN = 'sequence_run';
const KEY_ESCAPE = 'Escape';
const EVENT_KEYDOWN = 'keydown';
const ROLE_DIALOG = 'dialog' as const;
const PARAM_RUN_ID = 'runId';
const PARAM_GROUP_ID = 'groupId';
const BADGE_GREEN = 'green';
const BADGE_RED = 'red';
const HTTP_NOT_FOUND = 404;
const HTTP_METHOD_NOT_ALLOWED = 405;

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

// 本地逐 key 解析预检：复刻后端取值规则
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
      src[k] = VAR_SOURCE_DEFAULT;
    }
  });
  const rows: PreviewRow[] = [];
  const ordered = steps.slice().sort((a, b) => a.index - b.index);
  for (const s of ordered) {
    const overrides = stepVars[String(s.index)] ?? {};
    Object.entries(overrides).forEach(([k, v]) => {
      if (v !== '') {
        cur[k] = v;
        src[k] = VAR_SOURCE_STEP_PREFIX + String(s.index);
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

// 文本域解析：支持 JSON 或 key=value 每行一个
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

// 运行步骤状态徽标颜色
function statusBadgeClass(status: string): string {
  if (status === STEP_STATUS_SENT || status === STEP_STATUS_ACCEPTED) return BADGE_GREEN;
  if (status === STEP_STATUS_FAILED || status === STEP_STATUS_CANCELLED) return BADGE_RED;
  return '';
}

// 序列运行页：选择群组与序列，本地预检后启动，轮询展示进度
export default function SequencesPage() {
  const { isViewer } = useAuth();
  const [searchParams, setSearchParams] = useSearchParams();
  const [sequences, setSequences] = useState<Sequence[]>([]);
  const [groups, setGroups] = useState<Group[]>([]);
  const [seqId, setSeqId] = useState('');
  const [groupId, setGroupId] = useState(() => searchParams.get(PARAM_GROUP_ID) ?? '');
  const [varsText, setVarsText] = useState('event=发布会\ntime=今晚八点\nlocation=共享盘/第二季度');
  const [stepVarsText, setStepVarsText] = useState('{\n  "2": { "location": "共享盘/第二季度" }\n}');
  const [err, setErr] = useState('');
  const [preview, setPreview] = useState<PreviewRow[] | null>(null);
  const [previewErr, setPreviewErr] = useState('');
  const [showPreview, setShowPreview] = useState(false);
  const [runId, setRunId] = useState(() => searchParams.get(PARAM_RUN_ID) ?? '');
  const [run, setRun] = useState<SequenceRun | null>(null);
  const [startMsg, setStartMsg] = useState('');
  const [creatingSeq, setCreatingSeq] = useState(false);
  const [starting, setStarting] = useState(false);
  const [manualSeqId, setManualSeqId] = useState('');
  const effectiveSeqId = seqId || manualSeqId.trim();
  const [newSeqName, setNewSeqName] = useState('新序列');
  const [newSeqSteps, setNewSeqSteps] = useState('[\n  { "index": 1, "accountRole": "admin", "text": "{event} 将于 {time} 开始", "delaySeconds": 10 },\n  { "index": 2, "accountRole": "member", "text": "资料已上传到 {location}", "delaySeconds": 5 }\n]');
  const [createMsg, setCreateMsg] = useState('');

  // 同步运行进度到地址栏，刷新不丢失且支持深链直达
  const syncUrl = useCallback((rid: string, gid: string) => {
    const next: Record<string, string> = {};
    if (rid) next[PARAM_RUN_ID] = rid;
    const g = gid.trim();
    if (g) next[PARAM_GROUP_ID] = g;
    setSearchParams(next, { replace: true });
  }, [setSearchParams]);

  // GET /api/sequences 拉序列列表；404/405（旧后端无列表接口）时列表留空，允许手动填 sequenceId
  const load = useCallback(async () => {
    try {
      const data = await fetchJson<Sequence[]>('/api/sequences');
      setSequences(data);
      setErr('');
      if (data.length > 0) setSeqId((prev) => prev || data[0].id);
    } catch (e) {
      if (e instanceof ApiError && (e.status === HTTP_NOT_FOUND || e.status === HTTP_METHOD_NOT_ALLOWED)) {
        setSequences([]);
      } else {
        setErr(e instanceof ApiError ? `${e.message}（${e.code}）` : '加载失败');
      }
    }
    try {
      const g = await fetchJson<Group[]>('/api/groups');
      setGroups(g);
    } catch {
      setGroups([]);
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  const loadRun = useCallback(async (id: string) => {
    try {
      const r = await fetchJson<SequenceRun>(`/api/sequence-runs/${id}`);
      setRun(r);
    } catch {
      // 轮询间隙失败时保留上一次状态
    }
  }, []);

  // 运行中轮询（3s）+ WS 事件刷新
  useEffect(() => {
    if (!runId) return;
    loadRun(runId);
    const t = setInterval(() => loadRun(runId), 3000);
    return () => clearInterval(t);
  }, [runId, loadRun]);

  useWs([WS_EVENT_SEQUENCE_RUN], (f) => {
    const p = f.payload as { runId?: string };
    if (p.runId && p.runId === runId) loadRun(runId);
  });

  // 预检弹窗打开时支持按键关闭
  useEffect(() => {
    if (!showPreview) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === KEY_ESCAPE) setShowPreview(false);
    };
    window.addEventListener(EVENT_KEYDOWN, onKey);
    return () => window.removeEventListener(EVENT_KEYDOWN, onKey);
  }, [showPreview]);

  // 查看指定运行并同步地址栏
  const viewRun = useCallback((id: string) => {
    const v = id.trim();
    if (!v) return;
    setRunId(v);
    setRun(null);
    syncUrl(v, groupId);
    loadRun(v);
  }, [groupId, syncUrl, loadRun]);

  const createSequence = async () => {
    if (creatingSeq) return;
    setCreateMsg('');
    let steps: unknown;
    try {
      steps = JSON.parse(newSeqSteps);
    } catch {
      setCreateMsg('新建失败：steps 不是合法 JSON');
      return;
    }
    setCreatingSeq(true);
    try {
      const name = newSeqName.trim() || '未命名序列';
      const r = await postJson<{ id: string }>('/api/sequences', { name, steps });
      setCreateMsg(`已创建序列（id ${r.id}），已填入启动表单`);
      setSeqId(r.id);
      setManualSeqId(r.id);
      const stepsArr = Array.isArray(steps) ? (steps as Sequence['steps']) : [];
      setSequences((prev) => [...prev, { id: r.id, name, steps: stepsArr }]);
    } catch (e) {
      setCreateMsg(e instanceof ApiError ? `新建失败：${e.message}（${e.code}）` : '新建失败');
    } finally {
      setCreatingSeq(false);
    }
  };

  const selected = sequences.find((s) => s.id === seqId) ?? null;

  const doPreview = () => {
    setPreviewErr('');
    setPreview(null);
    if (!selected && !manualSeqId.trim()) {
      setPreviewErr('请先选择序列，或手动填写 sequenceId');
      return;
    }
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
    if (starting) return;
    setStartMsg('');
    const gid = groupId.trim();
    if (!gid) {
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
    setStarting(true);
    try {
      const r = await postJson<{ runId: string }>(`/api/groups/${gid}/sequence-runs`, {
        sequenceId: effectiveSeqId,
        vars,
        stepVars,
      });
      setRunId(r.runId);
      setRun(null);
      setStartMsg(`已启动（run ${r.runId}）`);
      setShowPreview(false);
      syncUrl(r.runId, gid);
      loadRun(r.runId);
    } catch (e) {
      if (e instanceof ApiError && e.code === ERR_UNRESOLVED_PLACEHOLDER) {
        setStartMsg(`预检不通过：stepIndex=${e.stepIndex}，key=${e.key}（${e.message}）`);
      } else {
        setStartMsg(e instanceof ApiError ? `启动失败：${e.message}（${e.code}）` : '启动失败');
      }
    } finally {
      setStarting(false);
    }
  };

  const done = run ? run.steps.filter((s) => s.status !== STEP_STATUS_PENDING).length : 0;
  const total = run ? run.steps.length : 0;
  const groupSelected = groups.some((g) => g.id === groupId) ? groupId : '';

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
        {groups.length > 0 && (
          <div className="form-row">
            <span>选择已有群：</span>
            <select value={groupSelected} onChange={(e) => setGroupId(e.target.value)} style={{ flex: 1 }}>
              <option value="">（手动输入群 ID）</option>
              {groups.map((g) => <option key={g.id} value={g.id}>{g.id.slice(0, 8)}…（{g.status}，{g.members.length} 成员）</option>)}
            </select>
          </div>
        )}
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
        {!isViewer && (
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
              <button onClick={createSequence} disabled={creatingSeq}>创建序列</button>
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
          {!isViewer && <button className="primary" onClick={start} disabled={starting}>启动</button>}
        </div>
        {previewErr && <div className="err">{previewErr}</div>}
        {startMsg && <div className="note">{startMsg}</div>}
        <div className="form-row">
          <span>运行 ID：</span>
          <input value={runId} onChange={(e) => setRunId(e.target.value)} placeholder="启动后自动填入，刷新不丢失" style={{ flex: 1 }} />
          <button onClick={() => viewRun(runId)} disabled={!runId.trim()}>查看</button>
        </div>
      </div>

      {showPreview && preview && (
        <div className="modal-mask" onClick={() => setShowPreview(false)}>
          <div className="modal" onClick={(e) => e.stopPropagation()} role={ROLE_DIALOG} aria-modal="true">
            <h3>预检结果：每步 key 最终取值与来源</h3>
            {preview.map((row) => (
              <div key={row.stepIndex} style={{ marginBottom: 10 }}>
                <div><strong>步骤 {row.stepIndex}</strong> [{row.accountRole}]：{row.text}</div>
                {row.resolved.length === 0 && <div style={{ color: '#999' }}>无占位符</div>}
                {row.resolved.map((x) => (
                  <div key={x.key} style={{ fontSize: 13 }}>
                    <code>{`{${x.key}}`}</code> = {x.value} <span className="badge">{x.source}</span>
                  </div>
                ))}
              </div>
            ))}
            {startMsg && <div className="err">{startMsg}</div>}
            <div className="form-row">
              {!isViewer && <button className="primary" onClick={start} disabled={starting}>确认启动</button>}
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
                  <td><span className={`badge ${statusBadgeClass(s.status)}`}>{s.status}</span></td>
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
