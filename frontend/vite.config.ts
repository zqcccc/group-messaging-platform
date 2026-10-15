import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// 开发服务跑在 5173；/api 走后端 3001，/ws 透传 WS
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': 'http://127.0.0.1:3001',
      '/ws': { target: 'ws://127.0.0.1:3001', ws: true },
    },
  },
});
