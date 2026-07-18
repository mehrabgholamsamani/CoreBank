import './tracing';
import 'reflect-metadata';
import {
  Body,
  Controller,
  Get,
  Header,
  HttpException,
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
import {
  parseMessageEnvelope,
  paymentCreatedV1Schema,
  railPaymentAcceptedV1Schema,
  railSettlementFileGeneratedV1Schema,
} from '@corebank/event-contracts';
import { createHash, randomUUID } from 'node:crypto';
import { Kafka } from 'kafkajs';
import { Pool } from 'pg';
import type { NextFunction, Request, Response } from 'express';
import { migrateReconciliation } from './migrations/1710000000015-reconciliation';
import { logger } from '@corebank/logging';
import { AdminGuard, type AuthenticatedRequest } from './auth';

const service = 'reconciliation-service';
const config = validateConfig(process.env);
const pool = new Pool({ connectionString: config.DATABASE_URL });
const log = logger.child({ service });
let requestCount = 0;
let errorCount = 0;
type SettlementRow = { externalReference?: string; actualMinor?: string; currency?: string };
const validAmount = (value: string | undefined) => value === undefined || /^-?\d+$/.test(value);
const checksum = (rows: unknown) => createHash('sha256').update(JSON.stringify(rows)).digest('hex');
const deadLetter = async (message: unknown, reason: string) =>
  pool.query('insert into reconciliation_dead_letters(id,message,reason) values($1,$2::jsonb,$3)', [
    randomUUID(),
    JSON.stringify(message),
    reason.slice(0, 500),
  ]);

async function startSettlementConsumer() {
  const consumer = new Kafka({ brokers: config.KAFKA_BROKERS.split(',') }).consumer({
    groupId: 'reconciliation-service-stage6',
  });
  await consumer.connect();
  await consumer.subscribe({
    topics: ['payment.created.v1', 'rail.payment-accepted.v1', 'rail.settlement-file-generated.v1'],
    fromBeginning: false,
  });
  await consumer.run({
    eachMessage: async ({ message, topic }) => {
      if (!message.value) return;
      let raw: unknown;
      try {
        raw = JSON.parse(message.value.toString());
      } catch {
        await deadLetter({ value: message.value.toString() }, 'invalid JSON');
        return;
      }
      const schema =
        topic === 'payment.created.v1'
          ? paymentCreatedV1Schema
          : topic === 'rail.payment-accepted.v1'
            ? railPaymentAcceptedV1Schema
            : railSettlementFileGeneratedV1Schema;
      const event = parseMessageEnvelope(raw, topic, schema as never) as any;
      if (!event) {
        await deadLetter(raw, `invalid ${topic} envelope`);
        return;
      }
      const client = await pool.connect();
      await client.query('begin');
      try {
        const inserted = await client.query(
          'insert into reconciliation_inbox(message_id) values($1::uuid) on conflict do nothing returning message_id',
          [event.messageId],
        );
        if (
          inserted.rowCount &&
          topic === 'payment.created.v1' &&
          event.payload.kind === 'RAIL_TRANSFER'
        )
          await client.query(
            'insert into expected_settlements(payment_id,amount_minor,currency) values($1::uuid,$2::bigint,$3) on conflict(payment_id) do update set amount_minor=excluded.amount_minor,currency=excluded.currency,updated_at=now()',
            [event.payload.paymentId, event.payload.amountMinor, event.payload.currency],
          );
        if (inserted.rowCount && topic === 'rail.payment-accepted.v1')
          await client.query(
            'update expected_settlements set external_reference=$1,updated_at=now() where payment_id=$2::uuid',
            [event.payload.railReference, event.payload.paymentId],
          );
        if (inserted.rowCount && topic === 'rail.settlement-file-generated.v1')
          await client.query(
            'insert into settlement_files(id,source,checksum,rows) values($1,$2,$3,$4::jsonb) on conflict(checksum) do nothing',
            [
              randomUUID(),
              event.payload.fileReference,
              checksum(event.payload.rows),
              JSON.stringify(event.payload.rows),
            ],
          );
        await client.query('commit');
      } catch (error) {
        await client.query('rollback');
        throw error;
      } finally {
        client.release();
      }
    },
  });
}
@Controller()
@UseGuards(AdminGuard)
class ReconciliationController {
  @Post('reconciliation/files') async importFile(
    @Body() body: { source?: string; rows?: SettlementRow[] },
  ) {
    if (
      !body.source?.trim() ||
      !Array.isArray(body.rows) ||
      body.rows.some(
        (row) =>
          !row.externalReference ||
          !validAmount(row.actualMinor) ||
          !['EUR', 'USD', 'SEK'].includes(row.currency ?? ''),
      )
    )
      throw new HttpException('invalid settlement file', 400);
    const id = randomUUID();
    const value = checksum(body.rows);
    try {
      await pool.query(
        'insert into settlement_files(id,source,checksum,rows) values($1,$2,$3,$4::jsonb)',
        [id, body.source.trim(), value, JSON.stringify(body.rows)],
      );
    } catch {
      throw new HttpException('settlement file already imported', 409);
    }
    return { id, checksum: value, rowCount: body.rows.length };
  }
  @Post('reconciliation/files/:id/runs') async run(@Param('id') fileId: string) {
    const file = (await pool.query('select rows from settlement_files where id=$1::uuid', [fileId]))
      .rows[0];
    if (!file) throw new HttpException('settlement file not found', 404);
    const id = randomUUID();
    const actualRows = file.rows as SettlementRow[];
    const expectedRows = (
      await pool.query(
        'select external_reference as "externalReference",amount_minor::text as "expectedMinor",currency from expected_settlements where external_reference is not null',
      )
    ).rows as Array<{ externalReference: string; expectedMinor: string; currency: string }>;
    const actual = new Map(actualRows.map((row) => [row.externalReference!, row]));
    const expected = new Map(expectedRows.map((row) => [row.externalReference, row]));
    const discrepancies: Array<{
      externalReference: string;
      expectedMinor: string | null;
      actualMinor: string | null;
      currency: string | null;
      kind: string;
    }> = [];
    for (const row of expectedRows) {
      const received = actual.get(row.externalReference);
      if (!received)
        discrepancies.push({
          externalReference: row.externalReference,
          expectedMinor: row.expectedMinor,
          actualMinor: null,
          currency: row.currency,
          kind: 'MISSING_ACTUAL',
        });
      else if (received.actualMinor !== row.expectedMinor || received.currency !== row.currency)
        discrepancies.push({
          externalReference: row.externalReference,
          expectedMinor: row.expectedMinor,
          actualMinor: received.actualMinor ?? null,
          currency: received.currency ?? row.currency,
          kind: received.currency !== row.currency ? 'CURRENCY_MISMATCH' : 'AMOUNT_MISMATCH',
        });
    }
    for (const row of actualRows)
      if (!expected.has(row.externalReference!))
        discrepancies.push({
          externalReference: row.externalReference!,
          expectedMinor: null,
          actualMinor: row.actualMinor ?? null,
          currency: row.currency ?? null,
          kind: 'MISSING_EXPECTED',
        });
    const client = await pool.connect();
    await client.query('begin');
    try {
      await client.query(
        "insert into reconciliation_runs(id,settlement_file_id,status,completed_at) values($1,$2::uuid,'COMPLETED',now())",
        [id, fileId],
      );
      for (const row of discrepancies)
        await client.query(
          'insert into discrepancies(id,run_id,external_reference,expected_minor,actual_minor,currency,kind) values($1,$2::uuid,$3,$4::bigint,$5::bigint,$6,$7)',
          [
            randomUUID(),
            id,
            row.externalReference,
            row.expectedMinor,
            row.actualMinor,
            row.currency,
            row.kind,
          ],
        );
      await client.query('commit');
    } catch (error) {
      await client.query('rollback');
      throw error;
    } finally {
      client.release();
    }
    return { id, status: 'COMPLETED', discrepancyCount: discrepancies.length };
  }
  @Get('reconciliation/runs/:id/discrepancies') async discrepancies(@Param('id') id: string) {
    return (
      await pool.query(
        'select id,external_reference as "externalReference",expected_minor::text as "expectedMinor",actual_minor::text as "actualMinor",currency,kind,status,resolution,created_at as "createdAt",resolved_at as "resolvedAt" from discrepancies where run_id=$1::uuid order by created_at',
        [id],
      )
    ).rows;
  }
  @Post('reconciliation/discrepancies/:id/resolve') async resolve(
    @Param('id') id: string,
    @Body() body: { resolution?: string },
    @Req() req: AuthenticatedRequest,
  ) {
    if (!body.resolution?.trim() || body.resolution.length > 500)
      throw new HttpException('resolution is required', 400);
    const client = await pool.connect();
    await client.query('begin');
    try {
      const row = (
        await client.query('select * from discrepancies where id=$1::uuid for update', [id])
      ).rows[0];
      if (!row) throw new HttpException('discrepancy not found', 404);
      if (row.status !== 'OPEN') throw new HttpException('discrepancy already resolved', 409);
      const details = { resolution: body.resolution.trim() };
      await client.query(
        "update discrepancies set status='RESOLVED',resolution=$1::jsonb,resolved_at=now() where id=$2::uuid",
        [JSON.stringify(details), id],
      );
      await client.query(
        'insert into reconciliation_audit(id,discrepancy_id,action,actor_id,correlation_id,details) values($1,$2::uuid,$3,$4,$5,$6::jsonb)',
        [
          randomUUID(),
          id,
          'RESOLVED',
          req.user.sub,
          correlation.get()?.correlationId ?? randomUUID(),
          JSON.stringify(details),
        ],
      );
      await client.query('commit');
      return { id, status: 'RESOLVED' };
    } catch (error) {
      await client.query('rollback');
      throw error;
    } finally {
      client.release();
    }
  }
  @Get('metrics')
  @Header('Content-Type', 'text/plain; version=0.0.4; charset=utf-8')
  async metrics() {
    const [open, deadLetters] = await Promise.all([
      pool.query("select count(*)::text as count from discrepancies where status='OPEN'"),
      pool.query('select count(*)::text as count from reconciliation_dead_letters'),
    ]);
    return `corebank_reconciliation_service_up 1\ncorebank_http_requests_total{service="reconciliation-service"} ${requestCount}\ncorebank_http_errors_total{service="reconciliation-service"} ${errorCount}\ncorebank_reconciliation_open_discrepancies ${open.rows[0].count}\ncorebank_dead_letters_total{service="reconciliation-service"} ${deadLetters.rows[0].count}\n`;
  }
  @Get('health') health() {
    return { status: 'ok', service };
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
@Module({ controllers: [ReconciliationController], providers: [AdminGuard] })
class AppModule {}
async function bootstrap() {
  await migrateReconciliation(pool);
  const app = await NestFactory.create(AppModule);
  app.use((request: Request, response: Response, next: NextFunction) => {
    const id = correlationIdFrom(request.headers['x-correlation-id']);
    response.setHeader('x-correlation-id', id);
    correlation.run({ correlationId: id }, next);
  });
  app.use((request: Request, response: Response, next: NextFunction) => {
    const started = Date.now();
    response.on('finish', () => {
      requestCount += 1;
      if (response.statusCode >= 400) errorCount += 1;
      log.info(
        {
          correlationId: correlation.get()?.correlationId,
          method: request.method,
          path: request.path,
          statusCode: response.statusCode,
          durationMs: Date.now() - started,
        },
        'http request completed',
      );
    });
    next();
  });
  app.enableShutdownHooks();
  await startSettlementConsumer();
  await app.listen(config.PORT);
}
void bootstrap();
