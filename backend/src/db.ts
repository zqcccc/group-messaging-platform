// pg 连接池 + 001 迁移（幂等）+ 启动时 schema 版本校验 + seed 数据兜底。
import bcrypt from 'bcryptjs';
import { Pool, PoolClient } from 'pg';

// 代码期望的 schema 版本；新增迁移时同步递增。
export const CODE_SCHEMA_VERSION = 1;

// 001 全量建表（全部 IF NOT EXISTS，可重复执行）。
const MIGRATION_001 = `
CREATE TABLE IF NOT EXISTS schema_migrations(version INT PRIMARY KEY, applied_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS accounts(
  id TEXT PRIMARY KEY, status TEXT NOT NULL DEFAULT 'idle',
  platform_user_id TEXT,
  rate_limited_until TIMESTAMPTZ,
  version INT NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS users(username TEXT PRIMARY KEY, password_hash TEXT NOT NULL, role TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS refresh_sessions(
  id UUID PRIMARY KEY, username TEXT NOT NULL, token_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now(), revoked BOOLEAN DEFAULT FALSE, family UUID NOT NULL);
CREATE TABLE IF NOT EXISTS groups(
  id UUID PRIMARY KEY, gateway_group_id TEXT NOT NULL UNIQUE,
  status TEXT NOT NULL DEFAULT 'active',
  creator_account_id TEXT NOT NULL, agent_enabled BOOLEAN DEFAULT FALSE,
  auto_kick_enabled BOOLEAN DEFAULT FALSE);
CREATE TABLE IF NOT EXISTS group_members(
  group_id UUID REFERENCES groups(id) ON DELETE CASCADE,
  account_id TEXT REFERENCES accounts(id),
  platform_user_id TEXT NOT NULL, role TEXT NOT NULL,
  PRIMARY KEY(group_id, account_id));
CREATE TABLE IF NOT EXISTS outbound_messages(
  id UUID PRIMARY KEY, group_id UUID NOT NULL REFERENCES groups(id),
  account_id TEXT NOT NULL, client_msg_id TEXT NOT NULL UNIQUE,
  text TEXT NOT NULL, delivery_status TEXT NOT NULL DEFAULT 'queued',
  fail_code TEXT, gateway_msg_id TEXT, gateway_sent_at TIMESTAMPTZ,
  accepted_at TIMESTAMPTZ, retry_count INT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS inbound_messages(
  group_id UUID NOT NULL, msg_id TEXT NOT NULL,
  sender_platform_user_id TEXT NOT NULL, text TEXT NOT NULL,
  sent_at TIMESTAMPTZ NOT NULL, is_own BOOLEAN DEFAULT FALSE,
  client_msg_id TEXT, PRIMARY KEY(group_id, msg_id));
CREATE TABLE IF NOT EXISTS processed_gateway_events(event_id BIGINT PRIMARY KEY, processed_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS gateway_cursor(id INT PRIMARY KEY DEFAULT 1, last_event_id BIGINT NOT NULL DEFAULT 0,
  CONSTRAINT one_row CHECK (id = 1));
CREATE TABLE IF NOT EXISTS jobs(id UUID PRIMARY KEY, kind TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'running', errors JSONB NOT NULL DEFAULT '[]',
  payload JSONB NOT NULL DEFAULT '{}', created_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS agent_runs(
  id TEXT PRIMARY KEY, group_id UUID NOT NULL,
  status TEXT NOT NULL DEFAULT 'running', end_reason TEXT,
  summary TEXT, history JSONB NOT NULL DEFAULT '[]',
  steps JSONB NOT NULL DEFAULT '[]',
  pending_triggers JSONB NOT NULL DEFAULT '[]',
  consec_protocol_errors INT NOT NULL DEFAULT 0,
  used_idempotency_keys JSONB NOT NULL DEFAULT '{}',
  active_ms INT NOT NULL DEFAULT 0, run_started_at TIMESTAMPTZ DEFAULT now(),
  deadline_at TIMESTAMPTZ, step_count INT NOT NULL DEFAULT 0);
CREATE UNIQUE INDEX IF NOT EXISTS one_running_agent_run ON agent_runs(group_id) WHERE status = 'running';
CREATE TABLE IF NOT EXISTS sequences(id UUID PRIMARY KEY, name TEXT NOT NULL, steps JSONB NOT NULL);
CREATE TABLE IF NOT EXISTS sequence_runs(
  id UUID PRIMARY KEY, group_id UUID NOT NULL, sequence_id UUID NOT NULL,
  status TEXT NOT NULL DEFAULT 'running', current_step_index INT NOT NULL DEFAULT 0,
  steps JSONB NOT NULL DEFAULT '[]', vars JSONB NOT NULL DEFAULT '{}',
  step_vars JSONB NOT NULL DEFAULT '{}', created_at TIMESTAMPTZ DEFAULT now());
CREATE UNIQUE INDEX IF NOT EXISTS one_running_seq_run ON sequence_runs(group_id) WHERE status = 'running';
CREATE TABLE IF NOT EXISTS ws_events(seq BIGSERIAL PRIMARY KEY, type TEXT NOT NULL, payload JSONB NOT NULL);
`;

let pool: Pool | null = null;

// 全局单例连接池，连接串只从 DATABASE_URL 读。
export function getPool(): Pool {
  if (!pool) {
    if (!process.env.DATABASE_URL) throw new Error('缺少环境变量 DATABASE_URL');
    pool = new Pool({ connectionString: process.env.DATABASE_URL });
  }
  return pool;
}

// 在一个事务里执行 fn：正常返回即 COMMIT，抛错则 ROLLBACK 后原样上抛。
export async function withTransaction<T>(fn: (client: PoolClient) => Promise<T>): Promise<T> {
  const client = await getPool().connect();
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (e) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw e;
  } finally {
    client.release();
  }
}

// 执行迁移：整段 001 在一个事务里跑，版本号缺失才补记。
export async function runMigrations(): Promise<void> {
  const client = await getPool().connect();
  try {
    await client.query('BEGIN');
    await client.query(MIGRATION_001);
    await client.query('INSERT INTO schema_migrations(version) VALUES (1) ON CONFLICT DO NOTHING');
    await client.query('COMMIT');
  } catch (e) {
    try {
      await client.query('ROLLBACK');
    } catch {
      // 回滚失败也忽略，上层按迁移失败处理。
    }
    throw e;
  } finally {
    client.release();
  }
}

// 库里已应用的最大版本号。
export async function getDbSchemaVersion(): Promise<number> {
  const { rows } = await getPool().query('SELECT COALESCE(MAX(version), 0) AS v FROM schema_migrations');
  return Number(rows[0].v);
}

// 启动校验：先迁移，再比对版本；落后（迁移没补上）或超前（代码太旧）都拒绝启动。
export async function assertSchemaVersion(): Promise<number> {
  await runMigrations();
  const v = await getDbSchemaVersion();
  if (v < CODE_SCHEMA_VERSION) {
    throw new Error(`数据库 schema 版本过旧（db=${v}, code=${CODE_SCHEMA_VERSION}），拒绝启动`);
  }
  if (v > CODE_SCHEMA_VERSION) {
    throw new Error(`数据库 schema 版本超前（db=${v}, code=${CODE_SCHEMA_VERSION}），请升级代码，拒绝启动`);
  }
  return v;
}

// seed 兜底（启动时与 npm run seed 共用，幂等）：
// 账号只补不存在的行，绝不覆盖已有状态；用户固定为已知口令。
export async function ensureSeed(): Promise<void> {
  const p = getPool();
  for (const id of ['acc-1', 'acc-2', 'acc-3', 'acc-4']) {
    await p.query(
      `INSERT INTO accounts(id, status, platform_user_id, version)
       VALUES ($1, 'idle', NULL, 0) ON CONFLICT (id) DO NOTHING`,
      [id],
    );
  }
  const adminHash = await bcrypt.hash('admin', 10);
  const viewerHash = await bcrypt.hash('viewer', 10);
  await p.query(
    `INSERT INTO users(username, password_hash, role) VALUES ('admin', $1, 'admin')
     ON CONFLICT (username) DO UPDATE SET password_hash = EXCLUDED.password_hash, role = EXCLUDED.role`,
    [adminHash],
  );
  await p.query(
    `INSERT INTO users(username, password_hash, role) VALUES ('viewer', $1, 'viewer')
     ON CONFLICT (username) DO UPDATE SET password_hash = EXCLUDED.password_hash, role = EXCLUDED.role`,
    [viewerHash],
  );
}
