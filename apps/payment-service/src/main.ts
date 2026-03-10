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
import { publishPaymentOutbox, startPaymentConsumer } from './messaging';
import { PaymentService } from './payment.service';

const service = 'payment-service';
const config = validateConfig(process.env);
const log = logger.child({ service });
@Controller()
class PaymentController {
  constructor(private readonly payments: PaymentService) {}
  @Post('payments') create(@Body() body: Record<string, unknown>, @Req() request: Request) {
    return this.payments.create(body, {
      correlationId: correlation.get()?.correlationId ?? randomUUID(),
      actorId: request.headers['x-actor-id']?.toString() ?? 'gateway',
      occurredAt: new Date().toISOString(),
    });
  }
  @Get('payments/:id') get(@Param('id') id: string) {
    return this.payments.get(id);
  }
  @Post('payments/:id/cancel') cancel(@Param('id') id: string) {
    return this.payments.cancel(id);
  }
  @Post('payments/:id/refunds') refund(
    @Param('id') id: string,
    @Body() body: { amountMinor?: string; idempotencyKey?: string },
  ) {
    return this.payments.refund(id, body.amountMinor ?? '', body.idempotencyKey ?? '');
  }
  @Post('payments/:id/reverse') reverse(
    @Param('id') id: string,
    @Body() body: { idempotencyKey?: string },
  ) {
    return this.payments.reverse(id, body.idempotencyKey ?? '');
  }
  @Get('health') health() {
    return { status: 'ok', service };
  }
  @Get('metrics') metrics() {
    return 'corebank_service_up{service="payment-service"} 1\n';
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
@Module({ controllers: [PaymentController], providers: [PaymentService] })
class AppModule {}
async function bootstrap() {
  await migratePayments(paymentPool);
  await migrateRailPayments(paymentPool);
  await migrateRefunds(paymentPool);
  await migrateAdjustmentIdempotency(paymentPool);
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
