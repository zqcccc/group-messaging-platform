import React, { createContext, useCallback, useContext, useEffect, useState } from 'react';
import { getToken, setToken as saveToken, roleFromToken, postJson, subscribeToken } from './api';

// 全局认证上下文：token 内存 + localStorage，role 只读 JWT
interface AuthState {
  token: string | null;
  role: string | null;
  username: string | null;
  isViewer: boolean;
  login: (username: string, password: string) => Promise<void>;
  logout: () => void;
}

const AuthCtx = createContext<AuthState | null>(null);

function usernameOf(t: string | null): string | null {
  if (!t) return null;
  try {
    const payload = JSON.parse(atob(t.split('.')[1].replace(/-/g, '+').replace(/_/g, '/')));
    // 后端 JWT 用 sub 存用户名（username 字段不存在），兼容两者
    if (typeof payload.sub === 'string') return payload.sub;
    return typeof payload.username === 'string' ? payload.username : null;
  } catch {
    return null;
  }
}

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [token, setTokenState] = useState<string | null>(() => getToken());

  // api 层静默续期换了新 token 后，同步回 React 状态（WS 也随之重连）
  useEffect(() => subscribeToken(setTokenState), []);

  const apply = useCallback((t: string | null) => {
    saveToken(t);
    setTokenState(t);
  }, []);

  // 密码登录：成功后 accessToken 同时进内存与 localStorage
  const login = useCallback(async (username: string, password: string) => {
    const data = await postJson<{ accessToken: string }>('/api/auth/login', { username, password });
    apply(data.accessToken);
  }, [apply]);

  const logout = useCallback(() => {
    apply(null);
    // 后台登出（refresh cookie 失效）；失败不阻塞本地退出
    postJson('/api/auth/logout').catch(() => undefined);
  }, [apply]);

  const role = roleFromToken(token);
  return (
    <AuthCtx.Provider value={{
      token,
      role,
      username: usernameOf(token),
      isViewer: role === 'viewer',
      login,
      logout,
    }}>
      {children}
    </AuthCtx.Provider>
  );
}

export function useAuth(): AuthState {
  const ctx = useContext(AuthCtx);
  if (!ctx) throw new Error('useAuth 必须在 AuthProvider 内使用');
  return ctx;
}
