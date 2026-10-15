# S1–S8 验证结果（2026-10-16，逐项实测）

环境：后端 :3001，网关 :3101，Agent :3102，PG 15433。群 236331d5（网关 g-3b8da0f8）。

| # | 场景 | 结果 | 证据 |
|---|------|------|------|
| S1 | 受理与发出 | pass | send→202 accepted，message_sent 后 timeline sent（一行） |
| S2 | 事件重复 | pass | duplicateEvents 下外部消息 timeline 仍 1 行，只新增 1 个 run |
| S3 | 自己消息回流 | pass | 自发 isOwn=true，runs 数 3→3 不变 |
| S4 | 限流 | pass | 429 后 rate_limited，到期 online，排队按序 sent（修 flushQueuedForAccount 后通过） |
| S5 | Agent 同 key 重试 | pass | 504+同 key 再调：出站表只有一行 k-1，第二次返 sent，run finished/final |
| S6 | Agent 坏响应 | pass | bad-json→protocol_error(BAD_JSON+rawResponse)→未知工具→end_turn，finished/final，服务不崩 |
| S7 | 序列并发启动 | pass | 并发双启动：一个 201(runId)，一个 409 SEQUENCE_ALREADY_RUNNING |
| S8 | 序列预检 | pass | 第 3 步缺 key→422 UNRESOLVED_PLACEHOLDER + stepIndex=3 + key=missing3，无新消息 |

修复记录：建群外键顺序、uuid/text 比较 cast、限流排队 flush（含重启恢复）。
