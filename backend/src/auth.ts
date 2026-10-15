// 登录会话：JWT access token（15 分钟）+ HttpOnly cookie refresh token（轮换 + 复用即整 family 作废）。
import bcrypt from 'bcryptjs';
import { createHash, randomBytes } from 'crypto';
import { NextFunction, Request, Response, Router } from 'express';
import jwt from 'jsonwebtoken';
import { v4 as uuidv4 } from 'uuid';
import { getPool } from './db';
import { asyncHandler, parseCookies, sendError } from './util';

// JWT 签名密钥：优先读 JWT_SECRET 环境变量（生产必须设置），未设置时用开发默认值。
// 重启前后保持同一密钥 access token 才有效；logout 与 refresh 复作废靠 refresh_sessions 表的 family 状态保证。
const JWT_SECRET = process.env.JWT_SECRET ?? 'exam-dev-secret-change-me';
const ACCESS_TTL_SECONDS = 15 * 60;
const REFRESH_COOKIE = 'refresh_token';
const REFRESH_MAX_AGE_SECONDS = 30 * 24 * 3600;

// 认证后挂在请求上的用户信息。
export interface AuthUser {
  username: string;
  role: string;
  familyId: string;
  jti: string;
}

declare global {
  namespace Express {
    interface Request {
      requestId: string;
      user?: AuthUser;
    }
  }
}


// refresh token 存库的是 sha256，防拖库即用。
function hashToken(token: string): string {
  return createHash('sha256').update(token).digest('hex');
}

function setRefreshCookie(res: Response, token: string): void {
  const parts = [
    `${REFRESH_COOKIE}=${encodeURIComponent(token)}`,
    'HttpOnly',
    'Path=/',
    'SameSite=Lax',
    `Max-Age=${REFRESH_MAX_AGE_SECONDS}`,
  ];
  res.setHeader('Set-Cookie', parts.join('; '));
}

// Bearer 鉴权：验签 + 查 family 是否还有未作废的 refresh 会话。
// logout 与 refresh 复用作废都是整 family 置 revoked，落到这里 access token 立即 401。
export function requireAuth(req: Request, res: Response, next: NextFunction): void {
  const header = req.headers.authorization ?? '';
  const token = header.startsWith('Bearer ') ? header.slice('Bearer '.length) : '';
  if (!token) {
    sendError(req, res, 401, 'UNAUTHORIZED', '缺少 access token');
    return;
  }
  let payload: { sub?: unknown; role?: unknown; fid?: unknown; jti?: unknown };
  try {
    payload = jwt.verify(token, JWT_SECRET) as typeof payload;
  } catch {
    sendError(req, res, 401, 'UNAUTHORIZED', 'access token 无效或已过期');
    return;
  }
  if (typeof payload.sub !== 'string' || typeof payload.fid !== 'string') {
    sendError(req, res, 401, 'UNAUTHORIZED', 'access token 内容非法');
    return;
  }
  const username = payload.sub;
  const familyId = payload.fid;
  const role = typeof payload.role === 'string' ? payload.role : '';
  const jti = typeof payload.jti === 'string' ? payload.jti : '';
  getPool()
    .query('SELECT 1 FROM refresh_sessions WHERE family = $1 AND revoked = FALSE LIMIT 1', [familyId])
    .then(
      (r) => {
        if ((r.rowCount ?? 0) === 0) {
          sendError(req, res, 401, 'UNAUTHORIZED', '会话已失效（已登出或被作废）');
          return;
        }
        req.user = { username, role, familyId, jti };
        next();
      },
      () => {
        sendError(req, res, 500, 'INTERNAL_ERROR', '服务器内部错误');
      },
    );
}

// 写操作守卫：viewer 只读，走到这里一律 403（须排在 requireAuth 之后）。
export function requireAdmin(req: Request, res: Response, next: NextFunction): void {
  if (!req.user) {
    sendError(req, res, 401, 'UNAUTHORIZED', '缺少 access token');
    return;
  }
  if (req.user.role !== 'admin') {
    sendError(req, res, 403, 'FORBIDDEN', '只读用户无权执行写操作');
    return;
  }
  next();
}

// WS 握手用：与 requireAuth 同口径验 token（含 family 有效性），供 ws.ts 复用。
export async function verifyAccessToken(token: string): Promise<{ username: string; role: string } | null> {
  let payload: { sub?: unknown; role?: unknown; fid?: unknown };
  try {
    payload = jwt.verify(token, JWT_SECRET) as typeof payload;
  } catch {
    return null;
  }
  if (typeof payload.sub !== 'string' || typeof payload.fid !== 'string') return null;
  try {
    const r = await getPool().query('SELECT 1 FROM refresh_sessions WHERE family = $1 AND revoked = FALSE LIMIT 1', [
      payload.fid,
    ]);
    if ((r.rowCount ?? 0) === 0) return null;
  } catch {
    return null;
  }
  return { username: payload.sub, role: typeof payload.role === 'string' ? payload.role : '' };
}

export const authRouter: Router = Router();

// POST /api/auth/login { username, password } → { accessToken } + HttpOnly cookie。
authRouter.post(
  '/login',
  asyncHandler(async (req: Request, res: Response) => {
    const body = req.body as { username?: unknown; password?: unknown };
    if (typeof body?.username !== 'string' || !body.username || typeof body?.password !== 'string' || !body.password) {
      sendError(req, res, 400, 'VALIDATION_ERROR', 'username 与 password 必填');
      return;
    }
    const { rows } = await getPool().query('SELECT username, password_hash, role FROM users WHERE username = $1', [
      body.username,
    ]);
    if (rows.length === 0) {
      sendError(req, res, 401, 'UNAUTHORIZED', '用户名或密码错误');
      return;
    }
    const ok = await bcrypt.compare(body.password, rows[0].password_hash as string);
    if (!ok) {
      sendError(req, res, 401, 'UNAUTHORIZED', '用户名或密码错误');
      return;
    }
    const familyId = uuidv4();
    const refreshToken = randomBytes(32).toString('hex');
    await getPool().query('INSERT INTO refresh_sessions(id, username, token_hash, family) VALUES ($1, $2, $3, $4)', [
      uuidv4(),
      rows[0].username as string,
      hashToken(refreshToken),
      familyId,
    ]);
    setRefreshCookie(res, refreshToken);
    // access token 载荷：sub=用户名，role=角色，fid=refresh family（logout/复用作废时整 family 拉黑）。
    res.json({
      accessToken: jwt.sign(
        { sub: rows[0].username as string, role: rows[0].role as string, fid: familyId, jti: uuidv4() },
        JWT_SECRET,
        { expiresIn: ACCESS_TTL_SECONDS },
      ),
    });
  }),
);

// POST /api/auth/refresh（读 cookie）→ { accessToken } + 新 cookie；旧 token 复用则整 family 作废并 401。
authRouter.post(
  '/refresh',
  asyncHandler(async (req: Request, res: Response) => {
    const token = parseCookies(req.headers.cookie)[REFRESH_COOKIE];
    if (!token) {
      sendError(req, res, 401, 'UNAUTHORIZED', '缺少 refresh token');
      return;
    }
    const h = hashToken(token);
    const client = await getPool().connect();
    try {
      await client.query('BEGIN');
      const { rows } = await client.query(
        'SELECT id, username, family, revoked FROM refresh_sessions WHERE token_hash = $1 FOR UPDATE',
        [h],
      );
      if (rows.length === 0) {
        await client.query('ROLLBACK');
        sendError(req, res, 401, 'UNAUTHORIZED', 'refresh token 无效');
        return;
      }
      const sess = rows[0] as { id: string; username: string; family: string; revoked: boolean };
      // 已轮换过的旧 token 再次出现 = 复用攻击：整 family 作废，
      // 之前换出的新 refresh 与新 access（同 family）经 requireAuth 的 family 检查一并失效。
      if (sess.revoked) {
        await client.query('UPDATE refresh_sessions SET revoked = TRUE WHERE family = $1', [sess.family]);
        await client.query('COMMIT');
        sendError(req, res, 401, 'UNAUTHORIZED', 'refresh token 已被使用过，整个会话已作废');
        return;
      }
      const u = await client.query('SELECT role FROM users WHERE username = $1', [sess.username]);
      if (u.rows.length === 0) {
        await client.query('ROLLBACK');
        sendError(req, res, 401, 'UNAUTHORIZED', '用户已不存在');
        return;
      }
      const newToken = randomBytes(32).toString('hex');
      await client.query('INSERT INTO refresh_sessions(id, username, token_hash, family) VALUES ($1, $2, $3, $4)', [
        uuidv4(),
        sess.username,
        hashToken(newToken),
        sess.family,
      ]);
      await client.query('UPDATE refresh_sessions SET revoked = TRUE WHERE id = $1', [sess.id]);
      await client.query('COMMIT');
      setRefreshCookie(res, newToken);
      res.json({
        accessToken: jwt.sign(
          { sub: sess.username, role: u.rows[0].role as string, fid: sess.family, jti: uuidv4() },
          JWT_SECRET,
          { expiresIn: ACCESS_TTL_SECONDS },
        ),
      });
    } catch (e) {
      try {
        await client.query('ROLLBACK');
      } catch {
        // 回滚失败也忽略，上层按 500 处理。
      }
      throw e;
    } finally {
      client.release();
    }
  }),
);

// POST /api/auth/logout：作废当前 access token 所在 family，清 cookie。
authRouter.post(
  '/logout',
  requireAuth,
  asyncHandler(async (req: Request, res: Response) => {
    const familyId = req.user?.familyId;
    if (familyId) {
      await getPool().query('UPDATE refresh_sessions SET revoked = TRUE WHERE family = $1', [familyId]);
    }
    res.setHeader('Set-Cookie', `${REFRESH_COOKIE}=; HttpOnly; Path=/; SameSite=Lax; Max-Age=0`);
    res.json({});
  }),
);
