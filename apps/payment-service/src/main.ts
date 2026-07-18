import './tracing';
import 'reflect-metadata';
import {
  Body,
  Controller,
  Get,
  Module,
  Param,
  Post,
  Req,
  ServiceUnavailableException,
  UseGuards,
} from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { validateConfig } from '@corebank/config';
import { correlation, correlationIdFrom } from '@corebank/correlation';
import { logger } from '@corebank/logging';
import { randomUUID } from 'node:crypto';
import type { NextFunction, Request, Response } from 'express';
import { paymentPool } from './data-source';
import { migratePayments } from './migrations/1710000000011-payments';
import { migrateRailPayments } from './migrations/1710000000012-rail-payments';
import { migrateRefunds } from './migrations/1710000000013-refunds';
import { migrateAdjustmentIdempotency } from './migrations/1710000000014-adjustment-idempotency';
import { migratePaymentSecurityAndRetries } from './migrations/1710000000015-security-and-retries';
import { publishPaymentOutbox, startPaymentConsumer } from './messaging';
import { PaymentService } from './payment.service';
import { JwtGuard, type AuthenticatedRequest } from './auth';

const service = 'payment-service';
const config = validateConfig(process.env);
const log = logger.child({ service });
@Controller()
@UseGuards(JwtGuard)
class PaymentController {
  constructor(private readonly payments: PaymentService) {}
  @Post('payments') async create(
    @Body() body: Record<string, unknown>,
    @Req() request: AuthenticatedRequest,
  ) {
    const sourceAccountId = typeof body.sourceAccountId === 'string' ? body.sourceAccountId : '';
    const destinationAccountId =
      typeof body.destinationAccountId === 'string' ? body.destinationAccountId : '';
    if (!sourceAccountId || !destinationAccountId)
      throw new ServiceUnavailableException(
        'sourceAccountId and destinationAccountId are required',
      );
    const accountUrl = process.env.ACCOUNT_URL ?? 'http://account-service:3002';
    const resolve = async (accountId: string) => {
      const response = await fetch(`${accountUrl}/accounts/${accountId}/ledger-link`, {
        headers: { authorization: request.headers.authorization ?? '' },
      });
      if (!response.ok)
        throw new ServiceUnavailableException(`account link unavailable: ${response.status}`);
      return response.json() as Promise<{ ledgerAccountId: string; currency: string }>;
    };
    const [source, destination] = await Promise.all([
      resolve(sourceAccountId),
      resolve(destinationAccountId),
    ]);
    if (source.currency !== destination.currency || source.currency !== body.currency)
      throw new ServiceUnavailableException('account currencies must match payment currency');
    return this.payments.create(
      body,
      {
        correlationId: correlation.get()?.correlationId ?? randomUUID(),
        actorId: request.user.sub,
        actorRole: request.user.role,
        occurredAt: new Date().toISOString(),
      },
      {
        sourceLedgerAccountId: source.ledgerAccountId,
        destinationLedgerAccountId: destination.ledgerAccountId,
      },
    );
  }
  @Get('payments/:id') get(@Param('id') id: string, @Req() request: AuthenticatedRequest) {
    return this.payments.get(id, request.user);
  }
  @Post('payments/:id/cancel') cancel(
    @Param('id') id: string,
    @Req() request: AuthenticatedRequest,
  ) {
    return this.payments.cancel(id, request.user);
  }
  @Post('payments/:id/refunds') refund(
    @Param('id') id: string,
    @Body() body: { amountMinor?: string; idempotencyKey?: string },
    @Req() request: AuthenticatedRequest,
  ) {
    return this.payments.refund(
      id,
      body.amountMinor ?? '',
      body.idempotencyKey ?? '',
      request.user,
    );
  }
  @Post('payments/:id/reverse') reverse(
    @Param('id') id: string,
    @Body() body: { idempotencyKey?: string },
    @Req() request: AuthenticatedRequest,
  ) {
    return this.payments.reverse(id, body.idempotencyKey ?? '', request.user);
  }
  @Get('health') health() {
    return { status: 'ok', service };
  }
  @Get('metrics') async metrics() {
    const [outbox, deadLetters, retries] = await Promise.all([
      paymentPool.query(
        'select count(*)::text as count from payment_outbox_messages where published_at is null',
      ),
      paymentPool.query('select count(*)::text as count from payment_dead_letters'),
      paymentPool.query(
        'select count(*)::text as count from payments where next_retry_at is not null',
      ),
    ]);
    return `corebank_service_up{service="payment-service"} 1\ncorebank_outbox_pending{service="payment-service"} ${outbox.rows[0].count}\ncorebank_dead_letters_total{service="payment-service"} ${deadLetters.rows[0].count}\ncorebank_payment_retries_pending ${retries.rows[0].count}\n`;
  }
  @Get('ready') async ready() {
    try {
      await paymentPool.query('select 1');
      return { status: 'ready', service };
    } catch {
      throw new ServiceUnavailableException({ status: 'not_ready', service });
    }
  }
}
@Module({ controllers: [PaymentController], providers: [PaymentService, JwtGuard] })
class AppModule {}
async function bootstrap() {
  await migratePayments(paymentPool);
  await migrateRailPayments(paymentPool);
  await migrateRefunds(paymentPool);
  await migrateAdjustmentIdempotency(paymentPool);
  await migratePaymentSecurityAndRetries(paymentPool);
  const app = await NestFactory.create(AppModule);
  const payments = app.get(PaymentService);
  app.use((request: Request, response: Response, next: NextFunction) => {
    const id = correlationIdFrom(request.headers['x-correlation-id']);
    response.setHeader('x-correlation-id', id);
    correlation.run({ correlationId: id }, next);
  });
  app.enableShutdownHooks();
  await startPaymentConsumer(payments);
  setInterval(() => void publishPaymentOutbox().catch(() => undefined), 1000);
  await app.listen(config.PORT);
  log.info({ port: config.PORT }, 'service started');
}
void bootstrap();
