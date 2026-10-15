import { useEffect } from 'react';
import { BrowserRouter, Routes, Route, NavLink, Navigate } from 'react-router-dom';
import { AuthProvider, useAuth } from './auth';
import { ensureWs, useWsStatus } from './ws';
import LoginPage from './pages/LoginPage';
import AccountsPage from './pages/AccountsPage';
import GroupsPage from './pages/GroupsPage';
import GroupDetailPage from './pages/GroupDetailPage';
import AgentRunPage from './pages/AgentRunPage';
import SequencesPage from './pages/SequencesPage';

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
    window.addEventListener('exam:unauthorized', fn);
    return () => window.removeEventListener('exam:unauthorized', fn);
  }, [logout]);
  return null;
}

// 左侧导航 + token 变化时重建 WS（带 sinceSeq 续传）
function Shell() {
  const { token, username, role, logout } = useAuth();
  const wsStatus = useWsStatus();
  useEffect(() => {
    ensureWs(token);
  }, [token]);

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
            WS：<span className={`dot ${wsStatus === 'online' ? 'on' : 'off'}`} />{wsStatus === 'online' ? '已连接' : '未连接'}
          </div>
          <div style={{ marginTop: 8 }}>
            <button onClick={logout}>退出</button>
          </div>
        </div>
      </nav>
      <div className="main">
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
