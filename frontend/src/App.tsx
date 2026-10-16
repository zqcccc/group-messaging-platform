import { useEffect, useState } from 'react';
import { BrowserRouter, Routes, Route, NavLink, Navigate } from 'react-router-dom';
import { AuthProvider, useAuth } from './auth';
import { ensureWs, reconnectWs, useWs, useWsStatus } from './ws';
import LoginPage from './pages/LoginPage';
import AccountsPage from './pages/AccountsPage';
import GroupsPage from './pages/GroupsPage';
import GroupDetailPage from './pages/GroupDetailPage';
import AgentRunPage from './pages/AgentRunPage';
import SequencesPage from './pages/SequencesPage';

// 侧边栏 WS 状态与全局事件常量
const WS_STATUS_ONLINE = 'online';
const WS_STATUS_OFFLINE = 'offline';
const WS_EVENT_INCONSIS = 'inconsistency';
const AUTH_EXPIRED_EVENT = 'exam:unauthorized';
const MAX_ALERTS = 5;

// 未登录挡板：无 token 直接去 /login
function RequireAuth({ children }: { children: JSX.Element }) {
  const { token } = useAuth();
  if (!token) return <Navigate to="/login" replace />;
  return children;
}

// 401 续期失败（会话作废/登出）时 api 层广播，这里清掉登录态回登录页
function AuthExpiredWatcher() {
  const { logout } = useAuth();
  useEffect(() => {
    const fn = () => logout();
    window.addEventListener(AUTH_EXPIRED_EVENT, fn);
    return () => window.removeEventListener(AUTH_EXPIRED_EVENT, fn);
  }, [logout]);
  return null;
}

interface InconsistencyAlert {
  kind: string;
  ref: string;
  message: string;
}

// 左侧导航 + token 变化时重建 WS（带 sinceSeq 续传）
function Shell() {
  const { token, username, role, logout } = useAuth();
  const wsStatus = useWsStatus();
  const [alerts, setAlerts] = useState<InconsistencyAlert[]>([]);
  useEffect(() => {
    ensureWs(token);
  }, [token]);

  useWs([WS_EVENT_INCONSIS], (f) => {
    const p = f.payload as { kind?: string; ref?: string; message?: string };
    const item: InconsistencyAlert = {
      kind: typeof p.kind === 'string' ? p.kind : WS_EVENT_INCONSIS,
      ref: typeof p.ref === 'string' ? p.ref : '',
      message: typeof p.message === 'string' ? p.message : '',
    };
    setAlerts((prev) => [item, ...prev].slice(0, MAX_ALERTS));
  });

  const dismissAlert = (idx: number) => {
    setAlerts((prev) => prev.filter((_, i) => i !== idx));
  };

  const isOffline = wsStatus === WS_STATUS_OFFLINE;

  return (
    <div className="app-shell">
      <nav className="nav">
        <h2>群组消息平台</h2>
        <NavLink to="/accounts">账号列表</NavLink>
        <NavLink to="/groups">群组</NavLink>
        <NavLink to="/sequences">序列运行</NavLink>
        <div className="meta">
          <div>用户：{username}（{role}）</div>
          <div>
            WS：<span className={`dot ${wsStatus === WS_STATUS_ONLINE ? 'on' : 'off'}`} />{wsStatus === WS_STATUS_ONLINE ? '已连接' : '未连接'}
            {isOffline && <button className="btn-sm" onClick={() => reconnectWs()}>重连</button>}
          </div>
          <div style={{ marginTop: 8 }}>
            <button onClick={logout}>退出</button>
          </div>
        </div>
      </nav>
      <div className="main">
        {isOffline && (
          <div className="warn-bar">
            <span>WS 连接已断开，数据可能不是最新。</span>
            <button className="btn-sm" onClick={() => reconnectWs()}>立即重连</button>
          </div>
        )}
        {alerts.map((a, i) => (
          <div key={`${a.kind}-${a.ref}-${i}`} className="alert-bar">
            <span>数据不一致：{a.kind} {a.ref} {a.message}</span>
            <button className="btn-sm" onClick={() => dismissAlert(i)}>忽略</button>
          </div>
        ))}
        <Routes>
          <Route path="/accounts" element={<AccountsPage />} />
          <Route path="/groups" element={<GroupsPage />} />
          <Route path="/groups/:id" element={<GroupDetailPage />} />
          <Route path="/agent-runs/:id" element={<AgentRunPage />} />
          <Route path="/sequences" element={<SequencesPage />} />
          <Route path="*" element={<Navigate to="/accounts" replace />} />
        </Routes>
      </div>
    </div>
  );
}

// 顶层：认证上下文 + 路由（/login 不在 Shell 内，不带导航）
export default function App() {
  return (
    <AuthProvider>
      <BrowserRouter>
        <AuthExpiredWatcher />
        <Routes>
          <Route path="/login" element={<LoginPage />} />
          <Route path="/*" element={<RequireAuth><Shell /></RequireAuth>} />
        </Routes>
      </BrowserRouter>
    </AuthProvider>
  );
}
