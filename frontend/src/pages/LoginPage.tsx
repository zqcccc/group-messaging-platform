import { useState } from 'react';
import { Navigate, useNavigate } from 'react-router-dom';
import { useAuth } from '../auth';
import { ApiError } from '../api';

// 登录页：用户名/密码 → POST /api/auth/login
export default function LoginPage() {
  const { login, token } = useAuth();
  const nav = useNavigate();
  const [username, setUsername] = useState('admin');
  const [password, setPassword] = useState('admin');
  const [err, setErr] = useState('');
  const [busy, setBusy] = useState(false);

  // 已登录直接进列表（渲染期跳转，不在渲染中调 nav）
  if (token) return <Navigate to="/accounts" replace />;

  const submit = async () => {
    setErr('');
    setBusy(true);
    try {
      await login(username, password);
      nav('/accounts', { replace: true });
    } catch (e) {
      if (e instanceof ApiError) {
        setErr(`登录失败：${e.message}（${e.code}）`);
      } else {
        setErr('登录失败：未知错误');
      }
    } finally {
      setBusy(false);
    }
  };

  return (
    <div style={{ maxWidth: 380, margin: '80px auto' }}>
      <div className="card">
        <h3>控制台登录</h3>
        <div className="form-row">
          <input placeholder="用户名" value={username} onChange={(e) => setUsername(e.target.value)} style={{ flex: 1 }} />
        </div>
        <div className="form-row">
          <input placeholder="密码" type="password" value={password} onChange={(e) => setPassword(e.target.value)} style={{ flex: 1 }} />
        </div>
        <div className="form-row">
          <button className="primary" onClick={submit} disabled={busy}>登录</button>
        </div>
        {err && <div className="err">{err}</div>}
      </div>
    </div>
  );
}
