import './tracing';
import 'reflect-metadata';
import {
  Body,
  Controller,
  Get,
  HttpException,
  Module,
  Post,
  Req,
  ServiceUnavailableException,
} from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { validateConfig } from '@corebank/config';
import { correlation, correlationIdFrom } from '@corebank/correlation';
import { logger } from '@corebank/logging';
import {
  messageEnvelope,
  type IdentityUserRegisteredV1,
  type MessageEnvelope,
} from '@corebank/event-contracts';
import * as bcrypt from 'bcryptjs';
import * as jwt from 'jsonwebtoken';
import { createHash, randomUUID } from 'node:crypto';
import type { NextFunction, Request, Response } from 'express';
import { Kafka } from 'kafkajs';
import { identityDataSource } from './data-source';

const service = 'identity-service';
const config = validateConfig(process.env);
const log = logger.child({ service });
const jwtSecret = process.env.JWT_SECRET ?? '';
if (!jwtSecret || jwtSecret.length < 32)
  throw new Error('JWT_SECRET must be at least 32 characters');
type Audit = {
  correlationId: string;
  actorId?: string;
  ipAddress?: string;
  userAgent?: string;
  occurredAt: string;
};
const audit = (request: Request, actorId?: string): Audit => ({
  correlationId: correlation.get()?.correlationId ?? randomUUID(),
  actorId,
  ipAddress: request.ip,
  userAgent: request.headers['user-agent']?.toString(),
  occurredAt: new Date().toISOString(),
});
const tokenHash = (token: string) => createHash('sha256').update(token).digest('hex');
const bootstrapSandboxAdmin = async () => {
  if (process.env.SANDBOX_ALLOW_BOOTSTRAP_ADMIN !== 'true') return;
  const email = process.env.SANDBOX_ADMIN_EMAIL?.toLowerCase();
  const password = process.env.SANDBOX_ADMIN_PASSWORD;
  if (!email || !/^\S+@\S+\.\S+$/.test(email) || !password || password.length < 12)
    throw new Error('sandbox admin email and a 12-character password are required');
  await identityDataSource.query(
    "insert into users(id,email,password_hash,role,audit) values($1,$2,$3,'ADMIN',$4::jsonb) on conflict(email) do nothing",
    [
      randomUUID(),
      email,
      await bcrypt.hash(password, 12),
      JSON.stringify({ actorId: 'sandbox-bootstrap', occurredAt: new Date().toISOString() }),
    ],
  );
};
const publishOutbox = async () => {
  const rows = await identityDataSource.query(
    'select id, topic, message, attempts from outbox_messages where published_at is null order by created_at limit 50',
  );
  if (!rows.length) return;
  const producer = new Kafka({ brokers: config.KAFKA_BROKERS.split(',') }).producer();
  await producer.connect();
  try {
    for (const row of rows) {
      try {
        await producer.send({
          topic: row.topic,
          messages: [{ key: row.message.aggregateId, value: JSON.stringify(row.message) }],
        });
        await identityDataSource.query(
          'update outbox_messages set published_at=now() where id=$1',
          [row.id],
        );
      } catch (error) {
        const attempts = Number(row.attempts) + 1;
        if (attempts >= 5)
          await identityDataSource.query(
            'insert into identity_dead_letters(id,message,reason) values($1,$2::jsonb,$3)',
            [
              randomUUID(),
              JSON.stringify(row.message),
              error instanceof Error ? error.message.slice(0, 500) : 'publish failure',
            ],
          );
        await identityDataSource.query(
          'update outbox_messages set attempts=$1,published_at=case when $1>=5 then now() else null end where id=$2',
          [attempts, row.id],
        );
      }
    }
  } finally {
    await producer.disconnect();
  }
};
@Controller()
class IdentityController {
  @Post('auth/register') async register(
    @Body() input: { email?: string; password?: string },
    @Req() request: Request,
  ) {
    if (
      !input.email ||
      !/^\S+@\S+\.\S+$/.test(input.email) ||
      !input.password ||
      input.password.length < 12
    )
      throw new HttpException('invalid registration request', 400);
    const id = randomUUID();
    const metadata = audit(request);
    const email = input.email.toLowerCase();
    const password = input.password;
    const message: MessageEnvelope<IdentityUserRegisteredV1> = messageEnvelope(
      {
        messageId: randomUUID(),
        messageType: 'identity.user-registered.v1',
        messageVersion: 1,
        aggregateId: id,
        correlationId: metadata.correlationId,
        producer: service,
        occurredAt: metadata.occurredAt,
      },
      { userId: id, role: 'CUSTOMER' },
    );
    try {
      await identityDataSource.transaction(async (manager) => {
        await manager.query(
          'insert into users(id,email,password_hash,role,audit) values($1,$2,$3,$4,$5::jsonb)',
          [id, email, await bcrypt.hash(password, 12), 'CUSTOMER', JSON.stringify(metadata)],
        );
        await manager.query(
          'insert into outbox_messages(id,topic,message,message_type,payload) values($1,$2,$3::jsonb,$4,$5::jsonb)',
          [
            message.messageId,
            message.messageType,
            JSON.stringify(message),
            message.messageType,
            JSON.stringify(message.payload),
          ],
        );
      });
    } catch (error) {
      if ((error as { code?: string }).code === '23505')
        throw new HttpException('email already registered', 409);
      throw error;
    }
    return { id, email };
  }
  @Post('auth/login') async login(
    @Body() input: { email?: string; password?: string },
    @Req() request: Request,
  ) {
    const rows = await identityDataSource.query(
      'select id,email,password_hash,role from users where email=$1',
      [input.email?.toLowerCase() ?? ''],
    );
    const user = rows[0];
    if (!user || !input.password || !(await bcrypt.compare(input.password, user.password_hash)))
      throw new HttpException('invalid credentials', 401);
    return this.issue(user, audit(request, user.id));
  }
  @Post('auth/refresh') async refresh(
    @Body() input: { refreshToken?: string },
    @Req() request: Request,
  ) {
    if (!input.refreshToken) throw new HttpException('invalid refresh token', 401);
    const hash = tokenHash(input.refreshToken);
    const rows = await identityDataSource.query(
      'select rt.id, u.id as user_id, u.role from refresh_tokens rt join users u on u.id=rt.user_id where rt.token_hash=$1 and rt.revoked_at is null and rt.expires_at>now()',
      [hash],
    );
    const row = rows[0];
    if (!row) throw new HttpException('invalid refresh token', 401);
    return identityDataSource.transaction(async (manager) => {
      await manager.query('update refresh_tokens set revoked_at=now() where id=$1', [row.id]);
      return this.issue({ id: row.user_id, role: row.role }, audit(request, row.user_id), manager);
    });
  }
  private async issue(
    user: { id: string; role: 'CUSTOMER' | 'ADMIN' },
    metadata: Audit,
    manager = identityDataSource.manager,
  ) {
    const refreshToken = randomUUID();
    await manager.query(
      "insert into refresh_tokens(id,token_hash,user_id,expires_at,audit) values($1,$2,$3,now() + interval '30 days',$4::jsonb)",
      [randomUUID(), tokenHash(refreshToken), user.id, JSON.stringify(metadata)],
    );
    return {
      accessToken: jwt.sign(
        {
          sub: user.id,
          role: user.role,
          permissions: user.role === 'ADMIN' ? ['*'] : ['accounts:read', 'accounts:write'],
        },
        jwtSecret,
        { expiresIn: '15m' },
      ),
      refreshToken,
      tokenType: 'Bearer',
      expiresIn: 900,
    };
  }
  @Get('health') health() {
    return { status: 'ok', service };
  }
  @Get('metrics') metrics() {
    return 'corebank_service_up{service="identity-service"} 1\n';
  }
  @Get('ready') async ready() {
    try {
      await identityDataSource.query('select 1');
      return { status: 'ready', service };
    } catch {
      throw new ServiceUnavailableException({ status: 'not_ready', service });
    }
  }
}
@Module({ controllers: [IdentityController] })
class AppModule {}
async function bootstrap() {
  await identityDataSource.initialize();
  await identityDataSource.runMigrations();
  await bootstrapSandboxAdmin();
  const app = await NestFactory.create(AppModule);
  app.use((request: Request, response: Response, next: NextFunction) => {
    const correlationId = correlationIdFrom(request.headers['x-correlation-id']);
    response.setHeader('x-correlation-id', correlationId);
    correlation.run({ correlationId }, next);
  });
  app.enableShutdownHooks();
  setInterval(() => void publishOutbox().catch(() => undefined), 1000);
  await app.listen(config.PORT);
  log.info({ port: config.PORT }, 'service started');
}
void bootstrap();
