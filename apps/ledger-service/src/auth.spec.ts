import * as jwt from 'jsonwebtoken';
import { JwtGuard, requireAdmin, type AuthenticatedRequest } from './auth';

const contextFor = (request: Partial<AuthenticatedRequest>) =>
  ({
    switchToHttp: () => ({ getRequest: () => request }),
  }) as never;

describe('ledger authorization', () => {
  const previousSecret = process.env.JWT_SECRET;

  beforeEach(() => {
    process.env.JWT_SECRET = 'a'.repeat(32);
  });
  afterAll(() => {
    process.env.JWT_SECRET = previousSecret;
  });

  it('accepts a valid bearer token and rejects an untrusted caller', () => {
    const token = jwt.sign(
      { sub: 'user-id', role: 'CUSTOMER', permissions: [] },
      process.env.JWT_SECRET!,
    );
    const request = { path: '/ledger/accounts', headers: { authorization: `Bearer ${token}` } };
    expect(new JwtGuard().canActivate(contextFor(request))).toBe(true);
    expect((request as AuthenticatedRequest).user.sub).toBe('user-id');
    expect(() =>
      new JwtGuard().canActivate(contextFor({ path: '/ledger/accounts', headers: {} })),
    ).toThrow('unauthorized');
  });

  it('permits privileged operational actions only to administrators', () => {
    expect(() => requireAdmin({ user: { role: 'CUSTOMER' } } as AuthenticatedRequest)).toThrow(
      'forbidden',
    );
    expect(() => requireAdmin({ user: { role: 'ADMIN' } } as AuthenticatedRequest)).not.toThrow();
  });
});
