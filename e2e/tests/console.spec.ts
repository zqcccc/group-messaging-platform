// C3：登录 → 打开群 → 看到 agent run 的步骤。数据用 API + 网关控制面现造（不依赖库里已有数据）。
import { expect, test, type APIRequestContext } from '@playwright/test';

const BACKEND = process.env.BACKEND ?? 'http://127.0.0.1:3001';
const GW = process.env.GW ?? 'http://127.0.0.1:3101';
const AG = process.env.AG ?? 'http://127.0.0.1:3102';
const SHOTS = 'screenshots';

async function adminToken(request: APIRequestContext): Promise<string> {
  const r = await request.post(`${BACKEND}/api/auth/login`, { data: { username: 'admin', password: 'admin' } });
  return (await r.json()).accessToken as string;
}

// 账号全部 online → 建群 → 开 agent → 外部消息触发一次 run（坏 JSON + 正常结束，步骤里有 protocol_error）。
async function seedGroupWithRun(request: APIRequestContext): Promise<{ groupId: string; runId: string }> {
  const token = await adminToken(request);
  const auth = { Authorization: `Bearer ${token}` };
  for (const id of ['acc-1', 'acc-2', 'acc-3', 'acc-4']) {
    await request.post(`${BACKEND}/api/accounts/${id}/connect`, { headers: auth });
  }
  const before = new Set(((await (await request.get(`${BACKEND}/api/groups`, { headers: auth })).json()) as { id: string }[]).map((g) => g.id));
  const job = await (
    await request.post(`${BACKEND}/api/groups`, { headers: auth, data: { creatorAccountId: 'acc-1', memberAccountIds: ['acc-2', 'acc-3'] } })
  ).json();
  await expect
    .poll(async () => (await (await request.get(`${BACKEND}/api/jobs/${job.jobId}`, { headers: auth })).json()).status, { timeout: 20_000 })
    .toBe('finished');
  const groups = (await (await request.get(`${BACKEND}/api/groups`, { headers: auth })).json()) as { id: string; gatewayGroupId: string }[];
  const group = groups.find((g) => !before.has(g.id))!;
  await request.patch(`${BACKEND}/api/groups/${group.id}`, { headers: auth, data: { agentEnabled: true } });
  await request.post(`${AG}/__control`, { data: { reset: true } });
  await request.post(`${AG}/__control`, { data: { script: ['bad-json', 'get-recent', 'finish'] } });
  await request.post(`${GW}/__control`, { data: { externalMessage: { groupId: group.gatewayGroupId, senderPlatformUserId: 'ext-e2e', text: '有人在吗？' } } });
  let runId = '';
  await expect
    .poll(
      async () => {
        const runs = (await (await request.get(`${BACKEND}/api/groups/${group.id}/agent-runs`, { headers: auth })).json()) as { id: string; status: string }[];
        runId = runs[0]?.id ?? '';
        return runs[0]?.status;
      },
      { timeout: 60_000 },
    )
    .toBe('finished');
  await request.post(`${AG}/__control`, { data: { reset: true } });
  return { groupId: group.id, runId };
}

async function login(page: import('@playwright/test').Page, user: string): Promise<void> {
  await page.goto('/');
  await page.getByPlaceholder('用户名').fill(user);
  await page.getByPlaceholder('密码').fill(user);
  await page.getByRole('button', { name: '登录' }).click();
  await expect(page.getByText(`用户：${user}`)).toBeVisible();
  await expect(page.getByText('已连接')).toBeVisible(); // WS 认证成功
}

test('admin：登录 → 打开群 → 查看 agent run 步骤', async ({ page, request }) => {
  const { groupId, runId } = await seedGroupWithRun(request);
  await login(page, 'admin');
  await page.screenshot({ path: `${SHOTS}/1-accounts-admin.png` });

  await page.goto(`/groups/${groupId}`);
  await expect(page.getByText('creator')).toBeVisible();
  await expect(page.getByText('有人在吗？')).toBeVisible();
  const runLink = page.getByRole('link', { name: `${runId.slice(0, 8)}…` });
  await expect(runLink).toBeVisible();
  await page.screenshot({ path: `${SHOTS}/2-group-detail.png`, fullPage: true });

  await runLink.click();
  await expect(page).toHaveURL(new RegExp(`/agent-runs/${runId}`));
  await expect(page.getByText(/步骤（共 \d+ 步）/)).toBeVisible();
  await expect(page.getByText('protocol_error').first()).toBeVisible();
  await expect(page.getByText('final').first()).toBeVisible();
  await page.screenshot({ path: `${SHOTS}/3-agent-run.png`, fullPage: true });
});

test('viewer：看不到任何写操作按钮', async ({ page }) => {
  await login(page, 'viewer');
  await expect(page.getByRole('cell', { name: 'acc-1', exact: true })).toBeVisible();
  for (const name of ['标记离线', '释放账号', '重连']) {
    await expect(page.getByRole('button', { name })).toHaveCount(0);
  }
  await page.screenshot({ path: `${SHOTS}/4-accounts-viewer.png` });
});
