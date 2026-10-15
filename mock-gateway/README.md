# mock-gateway（消息网关模拟，PORT=3101）

实现题目 2.1 的全部端点。纯内存实现，重启后状态清空。

## 启动

```bash
npm install
PORT=3101 node server.js   # 或 npm start
```

## 行为开关

环境变量（启动时）：`DUPLICATE_EVENTS=1`、`OUT_OF_ORDER=1`、`DROP_JOIN=1`、
`JOIN_DELAY_MS` / `SEND_DELAY_MS` / `KICK_DELAY_MS` / `ACCEPT_DELAY_MS`
（单个数字或 `min,max`）。

运行时：`POST /__control`（字段全可选，patch 语义），`GET /__control` 查看，
`POST /__reset` 清空。详见 `server.js` 顶部注释与控制面说明。
