// 独立 seed 脚本：npm run seed（幂等；与启动时自动 seed 共用 ensureSeed）。
import { assertSchemaVersion, ensureSeed, getPool } from './db';

async function main(): Promise<void> {
  const v = await assertSchemaVersion();
  await ensureSeed();
  console.log(`seed ok, schemaVersion=${v}`);
  await getPool().end();
}

main().catch((e: unknown) => {
  console.error(e instanceof Error ? e.message : e);
  process.exit(1);
});
