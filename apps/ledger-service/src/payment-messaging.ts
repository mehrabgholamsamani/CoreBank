import { correlation } from '@corebank/correlation';
import {
  messageEnvelope,
  ledgerPostAdjustmentV1Schema,
  ledgerPostInternalTransferV1Schema,
  ledgerReleaseFundsV1Schema,
  ledgerReserveFundsV1Schema,
  accountCreatedV1Schema,
  parseMessageEnvelope,
  type LedgerPostAdjustmentV1,
  type LedgerPostInternalTransferV1,
  type LedgerReleaseFundsV1,
  type LedgerReserveFundsV1,
  type AccountCreatedV1,
  type MessageEnvelope,
} from '@corebank/event-contracts';
import { Kafka } from 'kafkajs';
import { randomUUID } from 'node:crypto';
import type { QueryRunner } from 'typeorm';
import { processLedgerInbox } from './inbox.service';
import { LedgerService } from './ledger.service';

const brokers = () => (process.env.KAFKA_BROKERS ?? '').split(',').filter(Boolean);
const deadLetter = async (message: unknown, reason: string) => {
  const { ledgerDataSource } = await import('./data-source');
  await ledgerDataSource.query(
    'insert into ledger_dead_letters(id,message,reason) values($1,$2::jsonb,$3)',
    [randomUUID(), JSON.stringify(message), reason.slice(0, 500)],
  );
};

const persistEvent = async (
  runner: QueryRunner,
  type: string,
  aggregateId: string,
  correlationId: string,
  payload: unknown,
) => {
  const event = messageEnvelope(
    {
      messageId: randomUUID(),
      messageType: type,
      messageVersion: 1,
      aggregateId,
      correlationId,
      producer: 'ledger-service',
      occurredAt: new Date().toISOString(),
    },
    payload,
  );
  await runner.query(
    'insert into outbox_messages(id,message_type,payload) values($1,$2,$3::jsonb)',
    [event.messageId, type, JSON.stringify(event)],
  );
};

/** Kafka commands are deduplicated and their effects plus reply event commit atomically. */
export const startLedgerPaymentConsumer = async (ledger: LedgerService): Promise<void> => {
  if (!brokers().length) return;
  const consumer = new Kafka({ brokers: brokers() }).consumer({ groupId: 'ledger-service-stage4' });
  await consumer.connect();
  await consumer.subscribe({
    topics: [
      'ledger.reserve-funds.v1',
      'ledger.post-internal-transfer.v1',
      'ledger.release-funds.v1',
      'ledger.post-adjustment.v1',
      'account.created.v1',
    ],
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
        topic === 'account.created.v1'
          ? accountCreatedV1Schema
          : topic === 'ledger.reserve-funds.v1'
            ? ledgerReserveFundsV1Schema
            : topic === 'ledger.post-internal-transfer.v1'
              ? ledgerPostInternalTransferV1Schema
              : topic === 'ledger.release-funds.v1'
                ? ledgerReleaseFundsV1Schema
                : ledgerPostAdjustmentV1Schema;
      const command = parseMessageEnvelope(raw, topic, schema as never);
      if (!command) {
        await deadLetter(raw, `invalid ${topic} envelope`);
        return;
      }
      await correlation.run({ correlationId: command.correlationId }, async () => {
        await processLedgerInbox(command.messageId, async (runner) => {
          const paymentId = (command.payload as { paymentId?: string }).paymentId;
          try {
            if (topic === 'account.created.v1') {
              const p = command.payload as AccountCreatedV1;
              const account = await ledger.createLinkedAccount(
                runner,
                p.accountId,
                p.userId,
                p.currency,
              );
              await persistEvent(
                runner,
                'ledger.account-created.v1',
                p.accountId,
                command.correlationId,
                { accountId: p.accountId, ledgerAccountId: account.id, currency: p.currency },
              );
            } else if (topic === 'ledger.reserve-funds.v1') {
              const p = command.payload as LedgerReserveFundsV1;
              const reservation = await ledger.processReserve(
                runner,
                p.sourceLedgerAccountId,
                p.amountMinor,
                p.currency,
                p.idempotencyKey,
                p.paymentId,
                p.requesterId,
              );
              await persistEvent(
                runner,
                'ledger.funds-reserved.v1',
                p.paymentId,
                command.correlationId,
                {
                  paymentId: p.paymentId,
                  reservationId: reservation.id,
                  sourceLedgerAccountId: p.sourceLedgerAccountId,
                  amountMinor: p.amountMinor,
                  currency: p.currency,
                },
              );
            } else if (topic === 'ledger.post-internal-transfer.v1') {
              const p = command.payload as LedgerPostInternalTransferV1;
              const posted = await ledger.processTransfer(
                runner,
                p.reservationId,
                p.sourceLedgerAccountId,
                p.destinationLedgerAccountId,
                p.amountMinor,
                p.currency,
                p.idempotencyKey,
              );
              await persistEvent(
                runner,
                'ledger.internal-transfer-posted.v1',
                p.paymentId,
                command.correlationId,
                {
                  paymentId: p.paymentId,
                  transactionId: posted.id,
                  reservationId: p.reservationId,
                },
              );
            } else if (topic === 'ledger.release-funds.v1') {
              const p = command.payload as LedgerReleaseFundsV1;
              await ledger.processRelease(runner, p.reservationId, p.idempotencyKey);
              await persistEvent(
                runner,
                'ledger.funds-released.v1',
                p.paymentId,
                command.correlationId,
                { paymentId: p.paymentId, reservationId: p.reservationId },
              );
            } else if (topic === 'ledger.post-adjustment.v1') {
              const p = command.payload as LedgerPostAdjustmentV1;
              const result = await ledger.processAdjustment(
                runner,
                p.action,
                p.originalTransactionId,
                p.sourceLedgerAccountId,
                p.destinationLedgerAccountId,
                p.amountMinor,
                p.currency,
                p.idempotencyKey,
              );
              await persistEvent(
                runner,
                'ledger.adjustment-posted.v1',
                p.paymentId,
                command.correlationId,
                {
                  adjustmentId: p.adjustmentId,
                  paymentId: p.paymentId,
                  transactionId: result.id,
                  amountMinor: p.amountMinor,
                  action: p.action,
                },
              );
            }
          } catch (error) {
            if (!paymentId) throw error;
            await persistEvent(
              runner,
              'ledger.funds-reservation-rejected.v1',
              paymentId,
              command.correlationId,
              {
                paymentId,
                reason: error instanceof Error ? error.message : 'ledger command failed',
              },
            );
          }
        });
      });
    },
  });
};
