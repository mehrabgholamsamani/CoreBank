import './tracing';
import 'reflect-metadata';
import { Body, Controller, Get, Module, Post, ServiceUnavailableException } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { validateConfig } from '@corebank/config';
import { correlation, correlationIdFrom } from '@corebank/correlation';
import {
  messageEnvelope,
  type MessageEnvelope,
  type RailScenario,
  type RailSubmitPaymentV1,
} from '@corebank/event-contracts';
import { Kafka } from 'kafkajs';
import { randomUUID } from 'node:crypto';
import { Pool } from 'pg';
import type { NextFunction, Request, Response } from 'express';
import { migrateRail } from './migrations/1710000000012-rail-scenarios';

const service = 'rail-simulator';
const config = validateConfig(process.env);
const pool = new Pool({ connectionString: config.DATABASE_URL });
const brokers = () => config.KAFKA_BROKERS.split(',').filter(Boolean);
const emit = async (topic: string, paymentId: string, correlationId: string, payload: unknown) => {
  const message = messageEnvelope(
    {
      messageId: randomUUID(),
      messageType: topic,
      messageVersion: 1,
      aggregateId: paymentId,
      correlationId,
      producer: service,
      occurredAt: new Date().toISOString(),
    },
    payload,
  );
  await pool.query('insert into rail_outbox_messages(id,topic,message) values($1,$2,$3::jsonb)', [
    message.messageId,
    topic,
    JSON.stringify(message),
  ]);
};
const scenarios: RailScenario[] = [
  'SUCCESS',
  'TEMPORARY_FAILURE',
  'PERMANENT_REJECTION',
  'TIMEOUT_AFTER_ACCEPTANCE',
  'DUPLICATE_EVENTS',
  'OUT_OF_ORDER_EVENTS',
];
const delay = (milliseconds: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, milliseconds));
async function submit(command: MessageEnvelope<RailSubmitPaymentV1>) {
  const c = await pool.connect();
  await c.query('begin');
  try {
    const exists = (
      await c.query(
        'insert into rail_inbox_messages(message_id) values($1::uuid) on conflict do nothing returning message_id',
        [command.messageId],
      )
    ).rowCount;
    if (!exists) {
      await c.query('commit');
      return;
    }
    const p = command.payload;
    const ref = `SIM-${randomUUID()}`;
    await c.query(
      "insert into rail_payments(id,payment_id,amount_minor,currency,scenario,rail_reference,status) values($1,$2::uuid,$3::bigint,$4,$5,$6,'SUBMITTED') on conflict(payment_id) do nothing",
      [randomUUID(), p.paymentId, p.amountMinor, p.currency, p.scenario, ref],
    );
    await c.query('commit');
    if (p.scenario === 'PERMANENT_REJECTION')
      await emit('rail.payment-rejected.v1', p.paymentId, command.correlationId, {
        paymentId: p.paymentId,
        railReference: ref,
        reason: 'deterministic rejection',
      });
    else if (p.scenario === 'OUT_OF_ORDER_EVENTS') {
      await emit('rail.payment-settled.v1', p.paymentId, command.correlationId, {
        paymentId: p.paymentId,
        railReference: ref,
      });
      await emit('rail.payment-accepted.v1', p.paymentId, command.correlationId, {
        paymentId: p.paymentId,
        railReference: ref,
      });
    } else {
      if (p.scenario === 'TEMPORARY_FAILURE') {
        await emit('rail.payment-temporarily-failed.v1', p.paymentId, command.correlationId, {
          paymentId: p.paymentId,
          railReference: ref,
          attempt: 1,
          retryAfterMs: 50,
        });
        await delay(50);
      }
      await delay(25);
      await emit('rail.payment-accepted.v1', p.paymentId, command.correlationId, {
        paymentId: p.paymentId,
        railReference: ref,
      });
      if (p.scenario === 'TIMEOUT_AFTER_ACCEPTANCE')
        await emit('rail.payment-timed-out.v1', p.paymentId, command.correlationId, {
          paymentId: p.paymentId,
          railReference: ref,
        });
      else {
        await delay(25);
        await emit('rail.payment-settled.v1', p.paymentId, command.correlationId, {
          paymentId: p.paymentId,
          railReference: ref,
        });
        await emit('rail.settlement-file-generated.v1', p.paymentId, command.correlationId, {
          fileReference: `SETTLEMENT-${ref}`,
          rows: [
            {
              externalReference: ref,
              expectedMinor: p.amountMinor,
              actualMinor: p.amountMinor,
              currency: p.currency,
            },
          ],
        });
        if (p.scenario === 'DUPLICATE_EVENTS')
          await emit('rail.payment-settled.v1', p.paymentId, command.correlationId, {
            paymentId: p.paymentId,
            railReference: ref,
          });
      }
    }
  } catch (e) {
    await c.query('rollback');
    throw e;
  } finally {
    c.release();
  }
}
async function publisher() {
  const rows = (
    await pool.query(
      'select id,topic,message from rail_outbox_messages where published_at is null order by created_at limit 50',
    )
  ).rows;
  if (!rows.length) return;
  const producer = new Kafka({ brokers: brokers() }).producer();
  await producer.connect();
  try {
    for (const row of rows) {
      await producer.send({
        topic: row.topic,
        messages: [{ key: row.message.aggregateId, value: JSON.stringify(row.message) }],
      });
      await pool.query('update rail_outbox_messages set published_at=now() where id=$1::uuid', [
        row.id,
      ]);
    }
  } finally {
    await producer.disconnect();
  }
}
async function consumer() {
  const c = new Kafka({ brokers: brokers() }).consumer({ groupId: 'rail-simulator-stage5' });
  await c.connect();
  await c.subscribe({ topic: 'rail.submit-payment.v1', fromBeginning: false });
  await c.run({
    eachMessage: async ({ message }) => {
      if (message.value)
        await submit(JSON.parse(message.value.toString()) as MessageEnvelope<RailSubmitPaymentV1>);
    },
  });
}
@Controller()
class RailController {
  @Post('rail/scenarios') scenario(@Body() b: { scenario?: RailScenario }) {
    if (!b.scenario || !scenarios.includes(b.scenario)) return { valid: false, scenarios };
    return { valid: true, scenario: b.scenario };
  }
  @Get('health') health() {
    return { status: 'ok', service };
  }
  @Get('metrics') metrics() {
    return 'corebank_service_up{service="rail-simulator"} 1\n';
  }
  @Get('ready') async ready() {
    try {
      await pool.query('select 1');
      return { status: 'ready', service };
    } catch {
      throw new ServiceUnavailableException({ status: 'not_ready', service });
    }
  }
}
@Module({ controllers: [RailController] })
class AppModule {}
async function bootstrap() {
  await migrateRail(pool);
  const app = await NestFactory.create(AppModule);
  app.use((r: Request, s: Response, n: NextFunction) => {
    const id = correlationIdFrom(r.headers['x-correlation-id']);
    s.setHeader('x-correlation-id', id);
    correlation.run({ correlationId: id }, n);
  });
  await consumer();
  setInterval(() => void publisher().catch(() => undefined), 1000);
  app.enableShutdownHooks();
  await app.listen(config.PORT);
}
void bootstrap();
