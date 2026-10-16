import { useState } from 'react';
import { Navigate, useNavigate } from 'react-router-dom';
import { useAuth } from '../auth';
import { formatError } from '../api';

// 登录页默认路由与表单常量
const DEFAULT_ROUTE = '/accounts';
const LOGIN_ROUTE = '/accounts';
const DEFAULT_USERNAME = 'admin';
const DEFAULT_PASSWORD = 'admin';
const USERNAME_LABEL = '用户名';
const PASSWORD_LABEL = '密码';
const SUBMIT_TEXT = '登录';
const SUBMITTING_TEXT = '登录中…';

// 登录页：用户名/密码 → POST /api/auth/login
export default function LoginPage() {
  const { login, token } = useAuth();
  const nav = useNavigate();
  const [username, setUsername] = useState(DEFAULT_USERNAME);
  const [password, setPassword] = useState(DEFAULT_PASSWORD);
  const [err, setErr] = useState('');
  const [busy, setBusy] = useState(false);

  // 已登录直接进列表（渲染期跳转，不在渲染中调 nav）
  if (token) return <Navigate to={DEFAULT_ROUTE} replace />;

  const submit = async () => {
    setErr('');
    setBusy(true);
    try {
      await login(username, password);
      nav(LOGIN_ROUTE, { replace: true });
    } catch (e) {
      setErr(`登录失败：${formatError(e, '登录失败：未知错误')}`);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div style={{ maxWidth: 380, margin: '80px auto' }}>
      <div className="card">
        <h3>控制台登录</h3>
        <form onSubmit={(e) => { e.preventDefault(); void submit(); }}>
          <div className="form-row">
            <label htmlFor="login-username">{USERNAME_LABEL}</label>
            <input id="login-username" placeholder={USERNAME_LABEL} value={username} onChange={(e) => setUsername(e.target.value)} style={{ flex: 1 }} autoFocus />
          </div>
          <div className="form-row">
            <label htmlFor="login-password">{PASSWORD_LABEL}</label>
            <input id="login-password" placeholder={PASSWORD_LABEL} type="password" value={password} onChange={(e) => setPassword(e.target.value)} style={{ flex: 1 }} />
          </div>
          <div className="form-row">
            <button className="primary" type="submit" disabled={busy}>{busy ? SUBMITTING_TEXT : SUBMIT_TEXT}</button>
          </div>
        </form>
        {err && <div className="err">{err}</div>}
      </div>
    </div>
  );
}
