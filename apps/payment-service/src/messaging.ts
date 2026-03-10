import { Kafka } from 'kafkajs';
import type {
  LedgerFundsReservedV1,
  LedgerInternalTransferPostedV1,
  MessageEnvelope,
} from '@corebank/event-contracts';
import { randomUUID } from 'node:crypto';
import { paymentPool } from './data-source';
import { PaymentService } from './payment.service';

const brokers = () => (process.env.KAFKA_BROKERS ?? '').split(',').filter(Boolean);

/** The database remains the source of truth; Kafka publication is retried from this outbox. */
export const publishPaymentOutbox = async (): Promise<void> => {
  const rows = (
    await paymentPool.query(
      'select id,topic,message,attempts from payment_outbox_messages where published_at is null order by created_at limit 50',
    )
  ).rows;
  if (!rows.length || !brokers().length) return;
  const producer = new Kafka({ brokers: brokers() }).producer();
  await producer.connect();
  try {
    for (const row of rows) {
      try {
        await producer.send({
          topic: row.topic,
          messages: [{ key: row.message.aggregateId, value: JSON.stringify(row.message) }],
        });
        await paymentPool.query(
          'update payment_outbox_messages set published_at=now() where id=$1::uuid',
          [row.id],
        );
      } catch (error) {
        const attempts = Number(row.attempts) + 1;
        if (attempts >= 5)
          await paymentPool.query(
            'insert into payment_dead_letters(id,message,reason) values($1,$2::jsonb,$3)',
            [
              randomUUID(),
              JSON.stringify(row.message),
              error instanceof Error ? error.message.slice(0, 500) : 'publish failure',
            ],
          );
        await paymentPool.query(
          'update payment_outbox_messages set attempts=$1, published_at=case when $1>=5 then now() else null end where id=$2::uuid',
          [attempts, row.id],
        );
      }
    }
  } finally {
    await producer.disconnect();
  }
};

export const startPaymentConsumer = async (payments: PaymentService): Promise<void> => {
  if (!brokers().length) return;
  const consumer = new Kafka({ brokers: brokers() }).consumer({
    groupId: 'payment-service-stage4',
  });
  await consumer.connect();
  await consumer.subscribe({
    topics: [
      'ledger.funds-reserved.v1',
      'ledger.internal-transfer-posted.v1',
      'ledger.funds-reservation-rejected.v1',
      'rail.payment-accepted.v1',
      'rail.payment-settled.v1',
      'rail.payment-rejected.v1',
      'rail.payment-timed-out.v1',
      'rail.payment-temporarily-failed.v1',
      'ledger.adjustment-posted.v1',
    ],
    fromBeginning: false,
  });
  await consumer.run({
    eachMessage: async ({ message, topic }) => {
      if (!message.value) return;
      const event = JSON.parse(message.value.toString()) as MessageEnvelope<unknown>;
      if (topic === 'ledger.funds-reserved.v1')
        await payments.fundsReserved(event as MessageEnvelope<LedgerFundsReservedV1>);
      if (topic === 'ledger.internal-transfer-posted.v1')
        await payments.transferPosted(event as MessageEnvelope<LedgerInternalTransferPostedV1>);
      if (topic === 'ledger.funds-reservation-rejected.v1')
        await payments.reservationRejected(
          event as MessageEnvelope<{ paymentId: string; reason: string }>,
        );
      if (topic === 'rail.payment-accepted.v1')
        await payments.railAccepted(
          event as MessageEnvelope<{ paymentId: string; railReference: string }>,
        );
      if (topic === 'rail.payment-settled.v1')
        await payments.railSettled(event as MessageEnvelope<{ paymentId: string }>);
      if (topic === 'rail.payment-rejected.v1')
        await payments.railFailed(
          event as MessageEnvelope<{ paymentId: string; reason: string }>,
          'REJECTED',
        );
      if (topic === 'rail.payment-timed-out.v1')
        await payments.railFailed(event as MessageEnvelope<{ paymentId: string }>, 'FAILED');
      if (topic === 'rail.payment-temporarily-failed.v1')
        await payments.railTemporarilyFailed(event as MessageEnvelope<{ paymentId: string }>);
      if (topic === 'ledger.adjustment-posted.v1')
        await payments.adjustmentPosted(
          event as MessageEnvelope<{
            adjustmentId: string;
            paymentId: string;
            amountMinor: string;
            action: 'REFUND' | 'REVERSAL';
          }>,
        );
    },
  });
};
