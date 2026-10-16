# 验收结果（自动生成）

> 由 `node verify/run-all.mjs` 生成于 2026-10-16T04:47:55.104Z；群 `5be4d9ee-124c-4e3f-85cc-2454ce06515d`。重新运行即可复现。

| 用例 | 场景 | 对应需求 | 结果 |
| ---- | ---- | -------- | ---- |
| S1 | 受理与发出 | 2.4 S1 / A2 | ✅ PASS |
| S2 | 事件重复 | 2.4 S2 / A2 / A5-1 | ✅ PASS |
| S3 | 自己的消息回流 | 2.4 S3 / A2 | ✅ PASS |
| S4 | 限流 | 2.4 S4 / A1 / A2 | ✅ PASS |
| S5 | Agent 重试同一个 key | 2.4 S5 / A2 / A5-7 | ✅ PASS |
| S6 | Agent 坏响应 | 2.4 S6 / A5-3 | ✅ PASS |
| S7 | 序列并发启动 | 2.4 S7 / B1 | ✅ PASS |
| S8 | 序列预检 | 2.4 S8 / B1 | ✅ PASS |
| X1 | 鉴权与错误体 | A0 / 2.3 | ✅ PASS |
| X2 | 账号状态机 + WS 推送 | A1 / A4 | ✅ PASS |
| X3 | 不同 run 复用同一个 idempotency_key | A5-7 | ✅ PASS |
| X4 | 504 期间 by-client-id 不可用 | A2 | ✅ PASS |
| X5 | 崩溃恢复：网关受理途中进程被杀 | A2 / 总则 | ✅ PASS |

## 明细

### S1 受理与发出

- ✅ POST send → 202 { clientMsgId } — `HTTP 202`
- ✅ 202 不等 message_sent（< 1.5s 返回） — `6ms`
- ✅ message_sent 之前观察到 accepted — `queued → accepted → sent`
- ✅ 最终 sent，且时间线只有一行 — `rows=1`

### S2 事件重复

- ✅ 每个事件推两次，时间线该消息只有 1 行 — `rows=1`
- ✅ 触发且只触发了 1 个 agent run — `run=ad6a6d06 status=finished`

### S3 自己的消息回流

- ✅ 回流的自发消息 isOwn=true — `msgId=m-84b128b6`
- ✅ 没有产生新的 agent run — `runs 1 → 1`

### S4 限流

- ✅ 429 后账号 → rate_limited，rateLimitedUntil 有值 — `2026-10-16T04:47:18.471Z`
- ✅ 限流期内发送照常 202、保持 queued — `HTTP 202 queued`
- ✅ 到期（4s）自动回到 online — `online`
- ✅ 排队的消息到期后按原顺序发出 — `0d62a01e → 4807952b`

### S5 Agent 重试同一个 key

- ✅ run finished / final — `finished/final`
- ✅ 第 1 次 send 经审计后 sent — `send_message ok: clientMsgId=0b9f1219-1280-4b87-a62c-ca045126147e:k-s5-mvahkoycfkz0 deliveryStatus=sent`
- ✅ 同 key 第 2 次：不审计，返回 sent — `send_message ok: clientMsgId=0b9f1219-1280-4b87-a62c-ca045126147e:k-s5-mvahkoycfkz0 deliveryStatus=sent`
- ✅ 网关里恰好一条消息 — `message_sent=1`

### S6 Agent 坏响应

- ✅ run 以 final / budget_exhausted / protocol_errors 结束 — `finished/final`
- ✅ 坏 JSON → protocol_error(BAD_JSON) — `protocol_error:BAD_JSON, tool_use:UNKNOWN_TOOL, final:finish`
- ✅ 未知工具 → UNKNOWN_TOOL 错误结果
- ✅ 每一步都有 kind 和 rawResponse
- ✅ 服务未崩溃

### S7 序列并发启动

- ✅ GET /api/sequences 能列出刚建的序列
- ✅ 并发两次：恰好一个 201、一个 409 — `201 + 409`
- ✅ 409 的 error.code = SEQUENCE_ALREADY_RUNNING
- ✅ 获胜的运行正常跑完（finished） — `finished steps=sent,sent`

### S8 序列预检

- ✅ HTTP 422 + code=UNRESOLVED_PLACEHOLDER — `HTTP 422 UNRESOLVED_PLACEHOLDER`
- ✅ error.stepIndex = 3，error.key = 占位符名 — `stepIndex=3 key=nope_mvahkwhhwq52`
- ✅ 网关收不到任何消息 — `events=0`
- ✅ 不留下运行中的记录

### X1 鉴权与错误体

- ✅ 无 token → 401 UNAUTHORIZED（带 requestId）
- ✅ viewer 写操作 → 403 FORBIDDEN
- ✅ viewer 可读
- ✅ GET /api/health → { ok, schemaVersion } — `{"ok":true,"schemaVersion":1}`

### X2 账号状态机 + WS 推送

- ✅ 账号不存在 → 404 ACCOUNT_NOT_FOUND
- ✅ 缺 expectedFrom → 400 VALIDATION_ERROR
- ✅ 同状态 online→online → 409 ILLEGAL_TRANSITION
- ✅ expectedFrom 与当前不符 → 409 CAS_CONFLICT
- ✅ 并发两次同一转移：一个 200、一个 CAS_CONFLICT — `200 + 409`
- ✅ 重连 → online
- ✅ WS 收到 account_status_changed（离线、重连各一条） — `online→disconnected, disconnected→online`
- ✅ WS 帧 seq 单调递增

### X3 不同 run 复用同一个 idempotency_key

- ✅ 两个 run 都 finished — `finished, finished`
- ✅ 两条消息都真正到达网关 — `landed=2/2`

### X4 504 期间 by-client-id 不可用

- ✅ 查询不可用的 5 秒内一直是 unknown（不记 failed） — `queued → unknown`
- ✅ 查询恢复后确认没发出 → 重发一次 → sent — `sent`
- ✅ 网关里恰好 1 条 — `message_sent=1`

### X5 崩溃恢复：网关受理途中进程被杀

- ✅ 崩溃前对外显示 queued（内部为发送中） — `queued`
- ✅ 重启后收敛为 sent — `sent`
- ✅ 网关里恰好 1 条（没有误重发） — `message_sent=1`
