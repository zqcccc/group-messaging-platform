// 通用小工具：requestId、统一错误体、cookie 解析。
import { NextFunction, Request, Response } from 'express';
import { v4 as uuidv4 } from 'uuid';

// 带 requestId 的请求（错误体与日志靠它关联）。
export interface RequestWithId extends Request {
  requestId: string;
}

// 每个请求分配一个 uuid，错误中间件直接取用。
export function requestIdMiddleware(req: Request, _res: Response, next: NextFunction): void {
  (req as RequestWithId).requestId = uuidv4();
  next();
}

// 统一错误体：{ error: { code, message, requestId, ...业务字段 } }。
export function sendError(
  req: Request,
  res: Response,
  status: number,
  code: string,
  message: string,
  extra?: Record<string, unknown>,
): void {
  const requestId = (req as Partial<RequestWithId>).requestId ?? uuidv4();
  res.status(status).json({ error: { code, message, requestId, ...extra } });
}

// async 路由包一层，抛错统一走到错误中间件。
export function asyncHandler(
  fn: (req: Request, res: Response, next: NextFunction) => Promise<void> | void,
): (req: Request, res: Response, next: NextFunction) => void {
  return (req: Request, res: Response, next: NextFunction): void => {
    Promise.resolve(fn(req, res, next)).catch(next);
  };
}

// 手工解析 Cookie 头（依赖固定为七个包，不引入 cookie-parser）。
export function parseCookies(header: string | undefined): Record<string, string> {
  const out: Record<string, string> = {};
  if (!header) return out;
  for (const part of header.split(';')) {
    const idx = part.indexOf('=');
    if (idx <= 0) continue;
    const name = part.slice(0, idx).trim();
    if (!name || name in out) continue;
    const raw = part.slice(idx + 1).trim();
    try {
      out[name] = decodeURIComponent(raw);
    } catch {
      out[name] = raw;
    }
  }
  return out;
}
