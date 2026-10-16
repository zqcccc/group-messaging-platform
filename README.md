# 多账号群组消息平台

后端（Node + TS + PostgreSQL）+ 操作控制台（React 18 + TS + Vite），对接消息网关与 Agent 服务。开发和演示时由仓库内的 `mock-gateway` / `mock-agent` 模拟这两个服务（行为按题目 2.1 / 2.2 实现，带故障注入控制面）。

**给检查者**：

- [`docs/需求对照.md`](docs/需求对照.md)：逐条列出题目要求，标明实现位置、验证方式和完成状态（含未完成项与已知限制）。
- [`verify/REPORT.md`](verify/REPORT.md)：`node verify/run-all.mjs` 生成的最近一次验收结果（S1–S8 + 补充用例）。
- [`e2e/screenshots/`](e2e/screenshots/)：Playwright 端到端测试截下的控制台页面。

## 快速开始（3 条命令）

前置：Node 22+、Docker（只用来跑 PostgreSQL；本机已有 PG 的话改 `DATABASE_URL` 即可）。

```bash
docker compose up -d                 # PostgreSQL 16 → 127.0.0.1:15433（库/用户/密码 exam/exam/exam）
scripts/start-all.sh --fresh         # 装依赖 + 构建 + 启动 mock-gateway / mock-agent / backend / 前端，--fresh 表示先清库
node verify/run-all.mjs              # 一键跑 S1–S8 + 补充用例，打印结果并写 verify/REPORT.md
```

然后浏览器打开 <http://127.0.0.1:5173>，用 `admin/admin` 或 `viewer/viewer` 登录。停止服务：`scripts/stop-all.sh`；日志在 `.run/*.log`。

> mock-gateway 是纯内存实现，重启后网关里的群和事件都会清空。重启它时请用 `--fresh` 一起清库，否则库里的群在网关里已不存在。

## 自动化验收

### `node verify/run-all.mjs`

零依赖脚本（Node 22 自带 fetch / WebSocket），自己建数据：把 4 个账号都 connect → 建一个新群（acc-1 群主、acc-2 管理员、acc-3/acc-4 成员）→ 打开 agent → 逐条跑用例。可以重复运行（每次新建一个群），也可以只跑某几条：`node verify/run-all.mjs S4 X3`。

| 用例 | 内容 |
| ---- | ---- |
| S1–S8 | 题目 2.4 的八个典型场景，通过网关和 agent 的 `/__control` 注入故障 |
| X1 | 鉴权与错误体：401 / viewer 403 / health |
| X2 | 账号状态机：404 / 400 / ILLEGAL_TRANSITION / CAS_CONFLICT / 并发转移，以及 WS `account_status_changed` 推送 |
| X3 | 不同 agent run 复用同一个 `idempotency_key` 时，两条消息都要发出 |
| X4 | 504 后 by-client-id 查询也不可用：保持 `unknown`、不重发，查询恢复后收敛 |
| X5 | 后端在等网关 202 时被 `kill -9`：重启后确认到已发出 → `sent`，网关里恰好一条（需用 `scripts/start-all.sh` 启动后端） |
| X6 | 账号进终态时，它排队中的序列步骤在同一事务里 `skipped`，下一步随即发出。**会把 acc-3 永久变成终态**，默认不跑，需点名：`node verify/run-all.mjs X6`，跑完用 `--fresh` 重启 |

### 端到端（C3，Playwright）

```bash
cd e2e && npm install && npx playwright install chromium   # 首次
npx playwright test                                         # 登录 → 打开群 → 看 agent run 步骤；viewer 无写按钮
```

截图输出到 `e2e/screenshots/`，HTML 报告在 `e2e/report/`。

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

## 默认账号

- `admin` / `admin`：全部权限（写操作可见）。
- `viewer` / `viewer`：只读，所有写操作隐藏且直调接口返回 `403`。

access token 有效期 15 分钟，过期前端自动用 HttpOnly cookie 续期（并发 401 只发一次 refresh，B3）。

## 前端 5 页面操作路径（已对真实后端验证通过）

1. 登录（`/login`）：`admin/admin` 登录；`viewer` 登录后写按钮全部隐藏。
2. 账号列表（`/accounts`）：状态徽 + 限流倒计时；`标记离线`（→ `disconnected`，online / rate_limited 可用）、`释放账号`（→ `idle`，online / disconnected 可用）、`重连`（idle / disconnected → online）；终态置灰不可操作；CAS 冲突提示刷新。
3. 群组（`/groups`）→ 群详情（`/groups/:id`）：建群表单（创建者 + 成员，异步 job 轮询）→ 成员列表（含 role 徽）+ 全员退群（`leave-all`）→ 消息时间线（“加载更早”链式翻页与滚动补偿、实时追加、自发消息 `deliveryStatus` 徽）→ 最近 agent run 列表（`blocked` 红底醒目）与运行中序列深链 → 群内发送 + `agentEnabled/autoKickEnabled` 开关。
4. Agent 运行详情（`/agent-runs/:id`）：每步 `kind`/工具名/入参/`resultSummary`/审计结论/错误码；协议错误步可展开 2KB 原始响应。
5. 序列运行（`/sequences`）：下拉选序列（`GET /api/sequences`），或用页内“新建序列”表单（`POST /api/sequences`）创建后自动选中 → 填 `vars`（JSON 或 `key=value` 每行）与 `stepVars`（JSON）→ 本地预检弹窗（每步 key 取值与 `default`/`step:N` 来源）→ 启动；缺 key 时后端 `422` 会显示 `stepIndex` 与 `key`；运行中进度条 + 每步状态/时间。

## 手动逐个启动（不用脚本时）

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
