import './tracing';
import 'reflect-metadata';
import {
  Body,
  CanActivate,
  Controller,
  ExecutionContext,
  Get,
  HttpException,
  Injectable,
  Module,
  Post,
  Req,
  ServiceUnavailableException,
  UseGuards,
} from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { validateConfig } from '@corebank/config';
import { correlation, correlationIdFrom } from '@corebank/correlation';
import { logger } from '@corebank/logging';
import {
  messageEnvelope,
  type AccountCreatedV1,
  type AccountCustomerCreatedV1,
  type AccountStatus,
  type AccountStatusChangedV1,
  type MessageEnvelope,
} from '@corebank/event-contracts';
import * as jwt from 'jsonwebtoken';
import { randomUUID } from 'node:crypto';
import type { NextFunction, Request, Response } from 'express';
import { Kafka } from 'kafkajs';
import { accountDataSource } from './data-source';

const service = 'account-service';
const config = validateConfig(process.env);
const log = logger.child({ service });
const jwtSecret = process.env.JWT_SECRET ?? '';
if (!jwtSecret || jwtSecret.length < 32)
  throw new Error('JWT_SECRET must be at least 32 characters');
type AuthRequest = Request & {
  user: { sub: string; role: 'CUSTOMER' | 'ADMIN'; permissions: string[] };
};
type Audit = {
  actorId: string;
  correlationId: string;
  ipAddress?: string;
  userAgent?: string;
  occurredAt: string;
};
const audit = (request: AuthRequest): Audit => ({
  actorId: request.user.sub,
  correlationId: correlation.get()?.correlationId ?? randomUUID(),
  ipAddress: request.ip,
  userAgent: request.headers['user-agent']?.toString(),
  occurredAt: new Date().toISOString(),
});
@Injectable()
class JwtGuard implements CanActivate {
  canActivate(context: ExecutionContext) {
    const request = context.switchToHttp().getRequest<AuthRequest>();
    try {
      request.user = jwt.verify(
        request.headers.authorization?.replace(/^Bearer\s+/i, '') ?? '',
        jwtSecret,
      ) as unknown as AuthRequest['user'];
      return true;
    } catch {
      throw new HttpException('unauthorized', 401);
    }
  }
}
const ensureWrite = (request: AuthRequest) => {
  if (!request.user.permissions?.includes('accounts:write') && request.user.role !== 'ADMIN')
    throw new HttpException('forbidden', 403);
};
const publishOutbox = async () => {
  const rows = await accountDataSource.query(
    'select id,topic,message from outbox_messages where published_at is null order by created_at limit 50',
  );
  if (!rows.length) return;
  const producer = new Kafka({ brokers: config.KAFKA_BROKERS.split(',') }).producer();
  await producer.connect();
  try {
    for (const row of rows) {
      await producer.send({
        topic: row.topic,
        messages: [{ key: row.message.aggregateId, value: JSON.stringify(row.message) }],
      });
      await accountDataSource.query('update outbox_messages set published_at=now() where id=$1', [
        row.id,
      ]);
    }
  } finally {
    await producer.disconnect();
  }
};
@Controller()
@UseGuards(JwtGuard)
class AccountController {
  @Post('customers') async createCustomer(
    @Body() body: { displayName?: string },
    @Req() request: AuthRequest,
  ) {
    ensureWrite(request);
    if (!body.displayName?.trim() || body.displayName.trim().length > 120)
      throw new HttpException('invalid customer request', 400);
    const displayName = body.displayName.trim();
    const id = randomUUID();
    const metadata = audit(request);
    const message: MessageEnvelope<AccountCustomerCreatedV1> = messageEnvelope(
      {
        messageId: randomUUID(),
        messageType: 'account.customer-created.v1',
        messageVersion: 1,
        aggregateId: id,
        correlationId: metadata.correlationId,
        producer: service,
        occurredAt: metadata.occurredAt,
      },
      { customerId: id, userId: request.user.sub },
    );
    try {
      await accountDataSource.transaction(async (manager) => {
        await manager.query(
          'insert into customers(id,user_id,display_name,audit) values($1,$2,$3,$4::jsonb)',
          [id, request.user.sub, displayName, JSON.stringify(metadata)],
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
    } catch {
      throw new HttpException('customer already exists', 409);
    }
    return { id, displayName };
  }
  @Post('accounts') async createAccount(
    @Body() body: { customerId?: string; currency?: 'EUR' | 'USD' | 'SEK' },
    @Req() request: AuthRequest,
  ) {
    ensureWrite(request);
    if (!body.customerId || !body.currency || !['EUR', 'USD', 'SEK'].includes(body.currency))
      throw new HttpException('invalid account request', 400);
    const customer = (
      await accountDataSource.query('select id from customers where id=$1 and user_id=$2', [
        body.customerId,
        request.user.sub,
      ])
    )[0];
    if (!customer && request.user.role !== 'ADMIN')
      throw new HttpException('customer not found', 404);
    const id = randomUUID();
    const metadata = audit(request);
    const message: MessageEnvelope<AccountCreatedV1> = messageEnvelope(
      {
        messageId: randomUUID(),
        messageType: 'account.created.v1',
        messageVersion: 1,
        aggregateId: id,
        correlationId: metadata.correlationId,
        producer: service,
        occurredAt: metadata.occurredAt,
      },
      { accountId: id, customerId: body.customerId, currency: body.currency, status: 'ACTIVE' },
    );
    await accountDataSource.transaction(async (manager) => {
      await manager.query(
        'insert into accounts(id,customer_id,currency,status,audit) values($1,$2,$3,$4,$5::jsonb)',
        [id, body.customerId, body.currency, 'ACTIVE', JSON.stringify(metadata)],
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
    return {
      id,
      customerId: body.customerId,
      currency: body.currency,
      status: 'ACTIVE',
      balanceMinor: '0',
    };
  }
  @Post('accounts/:id/status') async changeStatus(
    @Req() request: AuthRequest,
    @Body() body: { status?: AccountStatus },
  ) {
    ensureWrite(request);
    const id = String(request.params.id);
    if (!body.status || !['ACTIVE', 'SUSPENDED', 'CLOSED'].includes(body.status))
      throw new HttpException('invalid status', 400);
    const row = (
      await accountDataSource.query(
        "select a.status from accounts a join customers c on c.id::text=a.customer_id::text where a.id=$1::uuid and ($2::text='ADMIN' or c.user_id::text=$3::text)",
        [id, request.user.role, request.user.sub],
      )
    )[0];
    if (!row) throw new HttpException('account not found', 404);
    const allowed: Record<AccountStatus, AccountStatus[]> = {
      ACTIVE: ['SUSPENDED', 'CLOSED'],
      SUSPENDED: ['ACTIVE', 'CLOSED'],
      CLOSED: [],
    };
    if (!allowed[row.status as AccountStatus].includes(body.status))
      throw new HttpException('invalid account status transition', 409);
    const metadata = audit(request);
    const message: MessageEnvelope<AccountStatusChangedV1> = messageEnvelope(
      {
        messageId: randomUUID(),
        messageType: 'account.status-changed.v1',
        messageVersion: 1,
        aggregateId: id,
        correlationId: metadata.correlationId,
        producer: service,
        occurredAt: metadata.occurredAt,
      },
      { accountId: id, previousStatus: row.status, status: body.status },
    );
    await accountDataSource.transaction(async (manager) => {
      await manager.query('update accounts set status=$1,audit=$2::jsonb where id=$3::uuid', [
        body.status,
        JSON.stringify(metadata),
        id,
      ]);
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
    return { id, status: body.status };
  }
  @Get('accounts') async accounts(@Req() request: AuthRequest) {
    const rows = await accountDataSource.query(
      'select a.id,a.customer_id as "customerId",a.currency,a.status,a.created_at as "createdAt" from accounts a join customers c on c.id::text=a.customer_id::text where c.user_id::text=$1::text order by a.created_at',
      [request.user.sub],
    );
    return rows.map((row: Record<string, unknown>) => ({ ...row, balanceMinor: '0' }));
  }
  @Get('health') health() {
    return { status: 'ok', service };
  }
  @Get('ready') async ready() {
    try {
      await accountDataSource.query('select 1');
      return { status: 'ready', service };
    } catch {
      throw new ServiceUnavailableException({ status: 'not_ready', service });
    }
  }
}
@Controller()
class MetricsController {
  @Get('metrics') metrics() {
    return 'corebank_service_up{service="account-service"} 1\n';
  }
}
@Module({ controllers: [AccountController, MetricsController], providers: [JwtGuard] })
class AppModule {}
async function bootstrap() {
  await accountDataSource.initialize();
  await accountDataSource.runMigrations();
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
