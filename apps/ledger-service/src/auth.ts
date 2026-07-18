import { CanActivate, ExecutionContext, HttpException, Injectable } from '@nestjs/common';
import * as jwt from 'jsonwebtoken';
import type { Request } from 'express';

export type AuthenticatedRequest = Request & {
  user: { sub: string; role: 'CUSTOMER' | 'ADMIN'; permissions: string[] };
};

const secret = () => {
  const value = process.env.JWT_SECRET ?? '';
  if (value.length < 32) throw new Error('JWT_SECRET must be at least 32 characters');
  return value;
};

@Injectable()
export class JwtGuard implements CanActivate {
  canActivate(context: ExecutionContext): boolean {
    const request = context.switchToHttp().getRequest<AuthenticatedRequest>();
    if (['/health', '/ready', '/metrics'].includes(request.path)) return true;
    try {
      request.user = jwt.verify(
        request.headers.authorization?.replace(/^Bearer\s+/i, '') ?? '',
        secret(),
      ) as AuthenticatedRequest['user'];
      return true;
    } catch {
      throw new HttpException('unauthorized', 401);
    }
  }
}

export const requireAdmin = (request: AuthenticatedRequest) => {
  if (request.user.role !== 'ADMIN') throw new HttpException('forbidden', 403);
};
