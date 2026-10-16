import { defineConfig } from '@playwright/test';

// 前置：scripts/start-all.sh 已启动 backend / mock-gateway / mock-agent / frontend(dev :5173)。
export default defineConfig({
  testDir: './tests',
  timeout: 90_000,
  workers: 1,
  reporter: [['list'], ['html', { open: 'never', outputFolder: 'report' }]],
  use: {
    baseURL: process.env.FRONTEND ?? 'http://127.0.0.1:5173',
    viewport: { width: 1280, height: 860 },
    screenshot: 'only-on-failure',
    trace: 'retain-on-failure',
  },
});
