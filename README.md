# 多账号群组消息平台（exam）

后端（Node + TS + PostgreSQL）+ 操作控制台（React 18 + TS + Vite），对接消息网关与 Agent 服务（开发演示用 `mock-gateway` / `mock-agent` 模拟，接口与行为约定见 `mock-gateway/README.md`）。

## 架构

```
        控制台前端 (React + TS, :5173)
              │ REST + WebSocket(/ws)
        后端 (Node + TS, :3001)
         ├── 账号管理（connect/transition，CAS 并发守卫）
         ├── 群、成员与消息时间线（建群 job/leave-all job）
         ├── 定时序列（预检/排期/重启恢复）
         └── Agent 接入（触发/12 步循环/审计/幂等/重启恢复）
              │                          │
   mock-gateway (:3101)            mock-agent (:3102)
   HTTP + SSE /events              POST /agent/turn + /agent/audit
              │                          │
              └──── PostgreSQL (:15433, 库 exam) ────┘
```

- 后端启动时自动执行幂等迁移 + seed（账号 `acc-1..acc-4`、用户 `admin/admin` 与 `viewer/viewer`）；DB schema 落后于代码时拒绝启动。
- 事件链：网关 SSE → 后端落库（去重/排序）→ WS 推送前端；出站先落 `outbound_messages` 行再调网关，保证“网关发出而库里无记录”不可能发生。
- 端口被占时的改法：四个服务都只读环境变量（见下表），改对应变量即可；前端 dev 的 `/api`、`/ws` 代理目标默认指向 `127.0.0.1:3001`（见 `frontend/vite.config.ts`），后端端口变了同步改那里。

| 服务        | 目录            | 环境变量                                                                          | 默认值                                                    |
| ----------- | --------------- | --------------------------------------------------------------------------------- | --------------------------------------------------------- |
| PostgreSQL  | docker `exam-pg` | —（宿主映射）                                                                      | `127.0.0.1:15433` → 容器 5432，库 `exam`，用户/密码 `exam/exam` |
| mock-gateway | `mock-gateway/` | `PORT`                                                                            | `3101`                                                    |
| mock-agent   | `mock-agent/`   | `PORT`                                                                            | `3102`                                                    |
| backend      | `backend/`      | `PORT` / `DATABASE_URL` / `GATEWAY_URL` / `AGENT_URL`                              | `3001` / `postgres://exam:exam@127.0.0.1:15433/exam` / `http://127.0.0.1:3101` / `http://127.0.0.1:3102` |
| backend 密钥 | `backend/`      | `JWT_SECRET`（生产必须设置，缺省为开发默认值）                                     | 未设置时 `exam-dev-secret-change-me`                                    |
| frontend     | `frontend/`     | dev 端口 `5173`（`npm run dev -- --port XXX` 覆盖；被占时 vite 会自动顺延并打印实际端口） | `5173`                                                    |

## 从零启动

前置：Node 22+、Docker（跑 PG）。下面按顺序执行：

```bash
# 1. PostgreSQL（docker，无则新建；15433 被占则换 -p 映射并同步改 DATABASE_URL）
docker run -d --name exam-pg -e POSTGRES_USER=exam -e POSTGRES_PASSWORD=exam \
  -e POSTGRES_DB=exam -p 127.0.0.1:15433:5432 postgres:16

# 2. 消息网关模拟（新终端）
cd mock-gateway && npm install && PORT=3101 node server.js

# 3. Agent 服务模拟（新终端）
cd mock-agent && npm install && npm run build && PORT=3102 npm start

# 4. 后端：迁移 + seed 自动执行（新终端）
cd backend && npm install && npm run build
PORT=3001 DATABASE_URL=postgres://exam:exam@127.0.0.1:15433/exam \
  GATEWAY_URL=http://127.0.0.1:3101 AGENT_URL=http://127.0.0.1:3102 npm start
# 健康检查：curl http://127.0.0.1:3001/api/health → {"ok":true,"schemaVersion":1}

# 5. 前端（新终端，浏览器开 http://localhost:5173）
cd frontend && npm install && npm run dev
```

注意：本机前端 dev 只监听 IPv6 回环，用 `http://localhost:5173` 访问（`http://127.0.0.1:5173` 不通是环境特性，非 bug）。

## 默认账号

- `admin` / `admin`：全部权限（写操作可见）。
- `viewer` / `viewer`：只读，所有写操作隐藏且直调接口返回 `403`。

access token 有效期 15 分钟，过期前端自动用 HttpOnly cookie 续期（并发 401 只发一次 refresh，B3）。

## S1–S8 验证方法

脚本与结果都在 `verify/`：`common.sh`（登录/网关与 Agent 控制面封装）、`s5.sh`–`s8.sh`（场景脚本）、`RESULTS.md`（逐项结果与证据，S1–S8 全 pass）。S1–S4 用网关 `POST /__control` 做故障注入（事件重复、回流、限流），S5–S6 用 Agent `POST /__control {script/reset}` 预设行为，S7 并发双启动看 `201` + `409`，S8 缺 key 启动看 `422 UNRESOLVED_PLACEHOLDER + stepIndex + key`。示例：

```bash
cd verify && ./s8.sh   # 第 3 步缺占位符 → 422，error.stepIndex=3，网关无新消息
```

## 前端 5 页面操作路径（已对真实后端验证通过）

1. 登录（`/login`）：`admin/admin` 登录；`viewer` 登录后写按钮全部隐藏。
2. 账号列表（`/accounts`）：状态徽 + 限流倒计时；`online → 标记离线/释放账号`，`idle/disconnected → 重连`；终态置灰不可操作；CAS 冲突提示刷新。
3. 群组（`/groups`）→ 群详情（`/groups/:id`）：建群表单（创建者 + 成员，异步 job 轮询）→ 成员列表（含 role 徽）→ 消息时间线（“加载更早”链式翻页、实时追加、自发消息 `deliveryStatus` 徽）→ 最近 agent run 列表（`blocked` 红底醒目）→ 群内发送 + `agentEnabled/autoKickEnabled` 开关。
4. Agent 运行详情（`/agent-runs/:id`）：每步 `kind`/工具名/入参/`resultSummary`/审计结论/错误码；协议错误步可展开 2KB 原始响应。
5. 序列运行（`/sequences`）：后端无列表接口，下拉为空时手动填 `sequenceId`，或用页内“新建序列”表单（`POST /api/sequences`）创建后自动回填 → 填 `vars`（JSON 或 `key=value` 每行）与 `stepVars`（JSON）→ 本地预检弹窗（每步 key 取值与 `default`/`step:N` 来源）→ 启动；缺 key 时后端 `422` 会显示 `stepIndex` 与 `key`；运行中进度条 + 每步状态/时间。

## 联调记录（2026-10-16）

曾发现 WS 握手恒回 `{"type":"auth","success":false}`（后端 `ws.ts` 动态 require 的 `verifyAccessToken` 在 `auth.ts` 不存在）；本次已修复该后端问题并重启验证，浏览器实测导航栏 WS 显示“已连接”，实时推送链路恢复。无其他遗留已知问题。
