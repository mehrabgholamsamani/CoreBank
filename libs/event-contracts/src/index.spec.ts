import {
  messageEnvelope,
  parseMessageEnvelope,
  railSubmitPaymentV1Schema,
  type MessageEnvelope,
} from './index';

describe('message envelope', () => {
  it('requires causation and correlation metadata fields by contract', () => {
    const event: MessageEnvelope<{ paymentId: string }> = {
      messageId: 'm1',
      messageType: 'payment.created.v1',
      messageVersion: 1,
      aggregateId: 'p1',
      correlationId: 'c1',
      causationId: 'command-1',
      producer: 'payment-service',
      occurredAt: new Date().toISOString(),
      payload: { paymentId: 'p1' },
    };
    expect(event.messageType).toBe('payment.created.v1');
  });

  it('keeps message metadata separate from the payload', () => {
    const message = messageEnvelope(
      {
        messageId: 'm1',
        messageType: 'example.completed.v1',
        messageVersion: 1,
        aggregateId: 'a1',
        correlationId: 'c1',
        producer: 'example-service',
        occurredAt: '2026-01-01T00:00:00.000Z',
      },
      { result: 'ok' },
    );
    expect(message).toMatchObject({ payload: { result: 'ok' }, correlationId: 'c1' });
  });

  it('rejects malformed runtime payloads', () => {
    const message = {
      messageId: '00000000-0000-4000-8000-000000000001',
      messageType: 'rail.submit-payment.v1',
      messageVersion: 1,
      aggregateId: '00000000-0000-4000-8000-000000000002',
      correlationId: '00000000-0000-4000-8000-000000000003',
      producer: 'test',
      occurredAt: '2026-01-01T00:00:00.000Z',
      payload: {
        paymentId: '00000000-0000-4000-8000-000000000004',
        amountMinor: '10',
        currency: 'EUR',
        scenario: 'SUCCESS',
      },
    };
    expect(
      parseMessageEnvelope(message, 'rail.submit-payment.v1', railSubmitPaymentV1Schema),
    ).toBeDefined();
    expect(
      parseMessageEnvelope(
        { ...message, payload: { ...message.payload, amountMinor: '1.5' } },
        'rail.submit-payment.v1',
        railSubmitPaymentV1Schema,
      ),
    ).toBeUndefined();
  });
});
