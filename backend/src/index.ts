// 服务组装与启动：先校验 schema 版本（落后拒绝启动），再挂路由，最后监听。
import express, { NextFunction, Request, Response } from 'express';
import { accountsRouter } from './accounts';
import { agentRouter, resumeAgentRuns } from './agent';
import { authRouter } from './auth';
import { assertSchemaVersion, CODE_SCHEMA_VERSION, ensureSeed } from './db';
import { groupsRouter, jobsRouter } from './groups';
import { resumeOutbound } from './send';
import { resumeSequenceRuns, sequenceRouter } from './sequence';
import { startSse } from './sse';
import { messagesRouter } from './timeline';
import { requestIdMiddleware, sendError } from './util';
import { attachWs, startWsBroadcaster } from './ws';

const app = express();
app.use(express.json());
app.use(requestIdMiddleware);

// 健康检查（免鉴权）：{ ok, schemaVersion }。
app.get('/api/health', (_req: Request, res: Response) => {
  res.json({ ok: true, schemaVersion: CODE_SCHEMA_VERSION });
});

app.use('/api/auth', authRouter);
app.use('/api/accounts', accountsRouter);
// 群组面：/api/groups（含 send/leave-all）、/api/jobs、群消息时间线。
app.use('/api/groups', groupsRouter, messagesRouter);
app.use('/api/jobs', jobsRouter);
// 序列路由（sequence.ts 路径已是 /sequences、/groups/:id/sequence-runs、/sequence-runs/:id）。
app.use('/api', sequenceRouter);
// agent 路由（agent.ts 路径已是 /api/agent-runs/:id、/api/groups/:id/agent-runs，挂载点能对上即可）。
app.use('/api', agentRouter);

// 未知路由：统一 404 错误体。
app.use((req: Request, res: Response) => {
  sendError(req, res, 404, 'NOT_FOUND', `未知路由 ${req.method} ${req.path}`);
});

// 兜底错误中间件：统一 500 错误体（requestId 用于关联日志）。
// eslint-disable-next-line @typescript-eslint/no-unused-vars
app.use((err: unknown, req: Request, res: Response, _next: NextFunction) => {
  console.error(`[${(req as Request & { requestId?: string }).requestId ?? '-'}]`, err);
  if (!res.headersSent) sendError(req, res, 500, 'INTERNAL_ERROR', '服务器内部错误');
});

async function main(): Promise<void> {
  const schemaVersion = await assertSchemaVersion();
  await ensureSeed();
  const port = Number(process.env.PORT ?? 3001);
  const server = app.listen(port, () => {
    console.log(`backend listening on :${port}, schemaVersion=${schemaVersion}`);
  });
  attachWs(server);
  startWsBroadcaster();
  // 恢复链：事件流先接上，再恢复出站行（sending/unknown 收敛、排队补发），最后续跑序列与 agent run。
  // 单个环节失败只打日志，不影响其余环节与已在服务的 HTTP。
  startSse();
  const steps: Array<[string, () => Promise<void>]> = [
    ['出站', resumeOutbound],
    ['序列', resumeSequenceRuns],
    ['agent', resumeAgentRuns],
  ];
  for (const [name, resume] of steps) {
    try {
      await resume();
    } catch (e) {
      console.error(`[index] ${name}恢复失败:`, e instanceof Error ? e.message : e);
    }
  }
}

main().catch((e: unknown) => {
  console.error(e instanceof Error ? e.message : e);
  process.exit(1);
});
