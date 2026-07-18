import './tracing';
import 'reflect-metadata';
import { Body, Controller, Get, Module, Post, ServiceUnavailableException } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { validateConfig } from '@corebank/config';
import { correlation, correlationIdFrom } from '@corebank/correlation';
import {
  messageEnvelope,
  parseMessageEnvelope,
  railSubmitPaymentV1Schema,
  type RailScenario,
  type RailSubmitPaymentV1,
} from '@corebank/event-contracts';
import { Kafka } from 'kafkajs';
import { randomUUID } from 'node:crypto';
import { Pool, type PoolClient } from 'pg';
import type { NextFunction, Request, Response } from 'express';
import { migrateRail } from './migrations/1710000000012-rail-scenarios';

const service = 'rail-simulator';
const config = validateConfig(process.env);
const pool = new Pool({ connectionString: config.DATABASE_URL });
const brokers = () => config.KAFKA_BROKERS.split(',').filter(Boolean);
const scenarios: RailScenario[] = [
  'SUCCESS',
  'TEMPORARY_FAILURE',
  'PERMANENT_REJECTION',
  'TIMEOUT_AFTER_ACCEPTANCE',
  'DUPLICATE_EVENTS',
  'OUT_OF_ORDER_EVENTS',
];

const enqueue = async (
  client: PoolClient,
  topic: string,
  paymentId: string,
  correlationId: string,
  payload: unknown,
) => {
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
  await client.query('insert into rail_outbox_messages(id,topic,message) values($1,$2,$3::jsonb)', [
    message.messageId,
    topic,
    JSON.stringify(message),
  ]);
};
const enqueueSettlement = async (
  client: PoolClient,
  payment: Record<string, unknown>,
  correlationId: string,
) => {
  const paymentId = String(payment.payment_id);
  const railReference = String(payment.rail_reference);
  await enqueue(client, 'rail.payment-accepted.v1', paymentId, correlationId, {
    paymentId,
    railReference,
  });
  await enqueue(client, 'rail.payment-settled.v1', paymentId, correlationId, {
    paymentId,
    railReference,
  });
  await enqueue(client, 'rail.settlement-file-generated.v1', paymentId, correlationId, {
    fileReference: `SETTLEMENT-${railReference}`,
    rows: [
      {
        externalReference: railReference,
        expectedMinor: String(payment.amount_minor),
        actualMinor: String(payment.amount_minor),
        currency: payment.currency,
      },
    ],
  });
  if (payment.scenario === 'DUPLICATE_EVENTS')
    await enqueue(client, 'rail.payment-settled.v1', paymentId, correlationId, {
      paymentId,
      railReference,
    });
};
const deadLetter = async (message: unknown, reason: string) => {
  await pool.query('insert into rail_dead_letters(id,message,reason) values($1,$2::jsonb,$3)', [
    randomUUID(),
    JSON.stringify(message),
    reason.slice(0, 500),
  ]);
};

async function submit(command: {
  messageId: string;
  correlationId: string;
  payload: RailSubmitPaymentV1;
}) {
  const client = await pool.connect();
  await client.query('begin');
  try {
    const inserted = await client.query(
      'insert into rail_inbox_messages(message_id) values($1::uuid) on conflict do nothing returning message_id',
      [command.messageId],
    );
    if (!inserted.rowCount) {
      await client.query('commit');
      return;
    }
    const p = command.payload;
    const railReference = `SIM-${randomUUID()}`;
    const row = (
      await client.query(
        "insert into rail_payments(id,payment_id,amount_minor,currency,scenario,rail_reference,status) values($1,$2::uuid,$3::bigint,$4,$5,$6,'SUBMITTED') on conflict(payment_id) do update set payment_id=excluded.payment_id returning *",
        [randomUUID(), p.paymentId, p.amountMinor, p.currency, p.scenario, railReference],
      )
    ).rows[0];
    if (p.scenario === 'PERMANENT_REJECTION') {
      await client.query("update rail_payments set status='REJECTED' where id=$1::uuid", [row.id]);
      await enqueue(client, 'rail.payment-rejected.v1', p.paymentId, command.correlationId, {
        paymentId: p.paymentId,
        railReference,
        reason: 'deterministic rejection',
      });
    } else if (p.scenario === 'TIMEOUT_AFTER_ACCEPTANCE') {
      await client.query(
        "update rail_payments set status='TIMED_OUT',accepted_at=now() where id=$1::uuid",
        [row.id],
      );
      await enqueue(client, 'rail.payment-accepted.v1', p.paymentId, command.correlationId, {
        paymentId: p.paymentId,
        railReference,
      });
      await enqueue(client, 'rail.payment-timed-out.v1', p.paymentId, command.correlationId, {
        paymentId: p.paymentId,
        railReference,
      });
    } else if (p.scenario === 'TEMPORARY_FAILURE') {
      await client.query(
        "update rail_payments set status='RETRY_PENDING',retry_attempts=1,next_retry_at=now() + interval '50 milliseconds' where id=$1::uuid",
        [row.id],
      );
      await enqueue(
        client,
        'rail.payment-temporarily-failed.v1',
        p.paymentId,
        command.correlationId,
        { paymentId: p.paymentId, railReference, attempt: 1, retryAfterMs: 50 },
      );
    } else {
      await client.query(
        "update rail_payments set status='SETTLED',accepted_at=now(),settled_at=now() where id=$1::uuid",
        [row.id],
      );
      if (p.scenario === 'OUT_OF_ORDER_EVENTS') {
        await enqueue(client, 'rail.payment-settled.v1', p.paymentId, command.correlationId, {
          paymentId: p.paymentId,
          railReference,
        });
        await enqueue(client, 'rail.payment-accepted.v1', p.paymentId, command.correlationId, {
          paymentId: p.paymentId,
          railReference,
        });
        await enqueue(
          client,
          'rail.settlement-file-generated.v1',
          p.paymentId,
          command.correlationId,
          {
            fileReference: `SETTLEMENT-${railReference}`,
            rows: [
              {
                externalReference: railReference,
                expectedMinor: p.amountMinor,
                actualMinor: p.amountMinor,
                currency: p.currency,
              },
            ],
          },
        );
      } else await enqueueSettlement(client, row, command.correlationId);
    }
    await client.query('commit');
  } catch (error) {
    await client.query('rollback');
    throw error;
  } finally {
    client.release();
  }
}

async function processRetries() {
  const client = await pool.connect();
  await client.query('begin');
  try {
    const rows = (
      await client.query(
        "select * from rail_payments where status='RETRY_PENDING' and next_retry_at<=now() for update skip locked limit 25",
      )
    ).rows;
    for (const row of rows) {
      await client.query(
        "update rail_payments set status='SETTLED',accepted_at=now(),settled_at=now(),next_retry_at=null where id=$1::uuid",
        [row.id],
      );
      await enqueueSettlement(client, row, randomUUID());
    }
    await client.query('commit');
  } catch (error) {
    await client.query('rollback');
    throw error;
  } finally {
    client.release();
  }
}
async function publisher() {
  const rows = (
    await pool.query(
      'select id,topic,message,attempts from rail_outbox_messages where published_at is null order by created_at limit 50',
    )
  ).rows;
  if (!rows.length) return;
  const producer = new Kafka({ brokers: brokers() }).producer();
  await producer.connect();
  try {
    for (const row of rows)
      try {
        await producer.send({
          topic: row.topic,
          messages: [{ key: row.message.aggregateId, value: JSON.stringify(row.message) }],
        });
        await pool.query('update rail_outbox_messages set published_at=now() where id=$1::uuid', [
          row.id,
        ]);
      } catch (error) {
        const attempts = Number(row.attempts) + 1;
        if (attempts >= 5)
          await deadLetter(row.message, error instanceof Error ? error.message : 'publish failure');
        await pool.query(
          'update rail_outbox_messages set attempts=$1,published_at=case when $1>=5 then now() else null end where id=$2::uuid',
          [attempts, row.id],
        );
      }
  } finally {
    await producer.disconnect();
  }
}
async function consumer() {
  const consumer = new Kafka({ brokers: brokers() }).consumer({ groupId: 'rail-simulator-stage5' });
  await consumer.connect();
  await consumer.subscribe({ topic: 'rail.submit-payment.v1', fromBeginning: false });
  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      let raw: unknown;
      try {
        raw = JSON.parse(message.value.toString());
      } catch {
        await deadLetter({ value: message.value.toString() }, 'invalid JSON');
        return;
      }
      const command = parseMessageEnvelope(
        raw,
        'rail.submit-payment.v1',
        railSubmitPaymentV1Schema,
      );
      if (!command) {
        await deadLetter(raw, 'invalid rail.submit-payment.v1 envelope');
        return;
      }
      await submit(command);
    },
  });
}
@Controller()
class RailController {
  @Post('rail/scenarios') scenario(@Body() body: { scenario?: RailScenario }) {
    return body.scenario && scenarios.includes(body.scenario)
      ? { valid: true, scenario: body.scenario }
      : { valid: false, scenarios };
  }
  @Get('health') health() {
    return { status: 'ok', service };
  }
  @Get('metrics') async metrics() {
    const [outbox, deadLetters, retries] = await Promise.all([
      pool.query(
        'select count(*)::text as count from rail_outbox_messages where published_at is null',
      ),
      pool.query('select count(*)::text as count from rail_dead_letters'),
      pool.query("select count(*)::text as count from rail_payments where status='RETRY_PENDING'"),
    ]);
    return `corebank_service_up{service="rail-simulator"} 1\ncorebank_outbox_pending{service="rail-simulator"} ${outbox.rows[0].count}\ncorebank_dead_letters_total{service="rail-simulator"} ${deadLetters.rows[0].count}\ncorebank_rail_retries_pending ${retries.rows[0].count}\n`;
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
  app.use((request: Request, response: Response, next: NextFunction) => {
    const id = correlationIdFrom(request.headers['x-correlation-id']);
    response.setHeader('x-correlation-id', id);
    correlation.run({ correlationId: id }, next);
  });
  await consumer();
  setInterval(() => void publisher().catch(() => undefined), 1000);
  setInterval(() => void processRetries().catch(() => undefined), 100);
  app.enableShutdownHooks();
  await app.listen(config.PORT);
}
void bootstrap();
