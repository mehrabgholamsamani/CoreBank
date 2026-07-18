import { z } from 'zod';

export interface MessageEnvelope<TPayload> {
  messageId: string;
  messageType: string;
  messageVersion: number;
  aggregateId: string;
  correlationId: string;
  causationId?: string;
  producer: string;
  occurredAt: string;
  payload: TPayload;
}

export type MessageMetadata = Omit<MessageEnvelope<never>, 'payload'>;

/** Business-specific payloads are added only by their owning implementation stage. */
export const messageEnvelope = <TPayload>(
  metadata: MessageMetadata,
  payload: TPayload,
): MessageEnvelope<TPayload> => ({ ...metadata, payload });

const envelopeMetadataSchema = z.object({
  messageId: z.string().uuid(),
  messageType: z.string().min(1),
  messageVersion: z.literal(1),
  aggregateId: z.string().uuid(),
  correlationId: z.string().uuid(),
  causationId: z.string().uuid().optional(),
  producer: z.string().min(1),
  occurredAt: z.string().datetime(),
});

export const parseMessageEnvelope = <TPayload>(
  input: unknown,
  messageType: string,
  payload: z.ZodType<TPayload>,
): MessageEnvelope<TPayload> | undefined => {
  const parsed = envelopeMetadataSchema.extend({ payload }).safeParse(input);
  if (!parsed.success || parsed.data.messageType !== messageType) return undefined;
  return parsed.data as unknown as MessageEnvelope<TPayload>;
};
export const unknownPayloadSchema = z.unknown();

export interface IdentityUserRegisteredV1 {
  userId: string;
  role: 'CUSTOMER' | 'ADMIN';
}
export interface AccountCustomerCreatedV1 {
  customerId: string;
  userId: string;
}
export interface AccountCreatedV1 {
  accountId: string;
  customerId: string;
  userId: string;
  currency: 'EUR' | 'USD' | 'SEK';
  status: 'ACTIVE';
}
export const accountCreatedV1Schema = z.object({
  accountId: z.string().uuid(),
  customerId: z.string().uuid(),
  userId: z.string().uuid(),
  currency: z.enum(['EUR', 'USD', 'SEK']),
  status: z.literal('ACTIVE'),
});
export interface LedgerAccountCreatedV1 {
  accountId: string;
  ledgerAccountId: string;
  currency: 'EUR' | 'USD' | 'SEK';
}
export const ledgerAccountCreatedV1Schema = z.object({
  accountId: z.string().uuid(),
  ledgerAccountId: z.string().uuid(),
  currency: z.enum(['EUR', 'USD', 'SEK']),
});
export interface AccountStatusChangedV1 {
  accountId: string;
  previousStatus: AccountStatus;
  status: AccountStatus;
}
export type AccountStatus = 'ACTIVE' | 'SUSPENDED' | 'CLOSED';

export type PaymentStatus =
  | 'CREATED'
  | 'VALIDATING'
  | 'RESERVING_FUNDS'
  | 'AUTHORIZED'
  | 'SUBMITTED'
  | 'ACCEPTED'
  | 'SETTLED'
  | 'REJECTED'
  | 'CANCELLED'
  | 'FAILED'
  | 'PARTIALLY_REFUNDED'
  | 'REFUNDED'
  | 'REVERSED';

export interface PaymentCreatedV1 {
  paymentId: string;
  sourceLedgerAccountId: string;
  destinationLedgerAccountId: string;
  amountMinor: string;
  currency: 'EUR' | 'USD' | 'SEK';
  kind: 'INTERNAL_TRANSFER' | 'RAIL_TRANSFER';
}
export interface LedgerReserveFundsV1 {
  paymentId: string;
  sourceLedgerAccountId: string;
  amountMinor: string;
  currency: 'EUR' | 'USD' | 'SEK';
  idempotencyKey: string;
  requesterId?: string;
}
export const ledgerReserveFundsV1Schema = z.object({
  paymentId: z.string().uuid(),
  sourceLedgerAccountId: z.string().uuid(),
  amountMinor: z.string().regex(/^[1-9]\d*$/),
  currency: z.enum(['EUR', 'USD', 'SEK']),
  idempotencyKey: z.string().min(1),
  requesterId: z.string().uuid().optional(),
});
export interface LedgerFundsReservedV1 {
  paymentId: string;
  reservationId: string;
  sourceLedgerAccountId: string;
  amountMinor: string;
  currency: 'EUR' | 'USD' | 'SEK';
}
export const ledgerFundsReservedV1Schema = z.object({
  paymentId: z.string().uuid(),
  reservationId: z.string().uuid(),
  sourceLedgerAccountId: z.string().uuid(),
  amountMinor: z.string().regex(/^[1-9]\d*$/),
  currency: z.enum(['EUR', 'USD', 'SEK']),
});
export interface LedgerReleaseFundsV1 {
  paymentId: string;
  reservationId: string;
  idempotencyKey: string;
}
export const ledgerReleaseFundsV1Schema = z.object({
  paymentId: z.string().uuid(),
  reservationId: z.string().uuid(),
  idempotencyKey: z.string().min(1),
});
export interface LedgerPostInternalTransferV1 {
  paymentId: string;
  reservationId: string;
  sourceLedgerAccountId: string;
  destinationLedgerAccountId: string;
  amountMinor: string;
  currency: 'EUR' | 'USD' | 'SEK';
  idempotencyKey: string;
}
export const ledgerPostInternalTransferV1Schema = z.object({
  paymentId: z.string().uuid(),
  reservationId: z.string().uuid(),
  sourceLedgerAccountId: z.string().uuid(),
  destinationLedgerAccountId: z.string().uuid(),
  amountMinor: z.string().regex(/^[1-9]\d*$/),
  currency: z.enum(['EUR', 'USD', 'SEK']),
  idempotencyKey: z.string().min(1),
});
export const ledgerInternalTransferPostedV1Schema = z.object({
  paymentId: z.string().uuid(),
  transactionId: z.string().uuid(),
  reservationId: z.string().uuid(),
});
export interface LedgerInternalTransferPostedV1 {
  paymentId: string;
  transactionId: string;
  reservationId: string;
}
export interface LedgerFundsReleasedV1 {
  paymentId: string;
  reservationId: string;
}

export type RailScenario =
  | 'SUCCESS'
  | 'TEMPORARY_FAILURE'
  | 'PERMANENT_REJECTION'
  | 'TIMEOUT_AFTER_ACCEPTANCE'
  | 'DUPLICATE_EVENTS'
  | 'OUT_OF_ORDER_EVENTS';
export interface RailSubmitPaymentV1 {
  paymentId: string;
  amountMinor: string;
  currency: 'EUR' | 'USD' | 'SEK';
  scenario: RailScenario;
}
export const railSubmitPaymentV1Schema = z.object({
  paymentId: z.string().uuid(),
  amountMinor: z.string().regex(/^[1-9]\d*$/),
  currency: z.enum(['EUR', 'USD', 'SEK']),
  scenario: z.enum([
    'SUCCESS',
    'TEMPORARY_FAILURE',
    'PERMANENT_REJECTION',
    'TIMEOUT_AFTER_ACCEPTANCE',
    'DUPLICATE_EVENTS',
    'OUT_OF_ORDER_EVENTS',
  ]),
});
export interface RailPaymentAcceptedV1 {
  paymentId: string;
  railReference: string;
}
export const railPaymentAcceptedV1Schema = z.object({
  paymentId: z.string().uuid(),
  railReference: z.string().min(1),
});
export const paymentIdSchema = z.object({ paymentId: z.string().uuid() });
export const paymentFailureSchema = paymentIdSchema.extend({ reason: z.string().min(1) });
export const railTemporaryFailureSchema = paymentIdSchema.extend({
  railReference: z.string().min(1),
  attempt: z.number().int().positive(),
  retryAfterMs: z.number().int().positive(),
});
export interface RailPaymentSettledV1 {
  paymentId: string;
  railReference: string;
}
export interface RailPaymentRejectedV1 {
  paymentId: string;
  railReference: string;
  reason: string;
}
export interface RailPaymentTimedOutV1 {
  paymentId: string;
  railReference: string;
}
export interface RailPaymentTemporarilyFailedV1 {
  paymentId: string;
  railReference: string;
  attempt: number;
  retryAfterMs: number;
}
export interface RailSettlementFileGeneratedV1 {
  fileReference: string;
  rows: Array<{
    externalReference: string;
    expectedMinor: string;
    actualMinor: string;
    currency: 'EUR' | 'USD' | 'SEK';
  }>;
}
export const railSettlementFileGeneratedV1Schema = z.object({
  fileReference: z.string().min(1),
  rows: z.array(
    z.object({
      externalReference: z.string().min(1),
      expectedMinor: z.string().regex(/^-?\d+$/),
      actualMinor: z.string().regex(/^-?\d+$/),
      currency: z.enum(['EUR', 'USD', 'SEK']),
    }),
  ),
});
export const paymentCreatedV1Schema = z.object({
  paymentId: z.string().uuid(),
  sourceLedgerAccountId: z.string().uuid(),
  destinationLedgerAccountId: z.string().uuid(),
  amountMinor: z.string().regex(/^[1-9]\d*$/),
  currency: z.enum(['EUR', 'USD', 'SEK']),
  kind: z.enum(['INTERNAL_TRANSFER', 'RAIL_TRANSFER']),
});
export interface LedgerPostSettlementV1 {
  paymentId: string;
  reservationId: string;
  sourceLedgerAccountId: string;
  destinationLedgerAccountId: string;
  amountMinor: string;
  currency: 'EUR' | 'USD' | 'SEK';
  idempotencyKey: string;
}
export interface LedgerSettlementPostedV1 {
  paymentId: string;
  transactionId: string;
  reservationId: string;
}
export interface LedgerPostAdjustmentV1 {
  adjustmentId: string;
  paymentId: string;
  originalTransactionId: string;
  sourceLedgerAccountId: string;
  destinationLedgerAccountId: string;
  amountMinor: string;
  currency: 'EUR' | 'USD' | 'SEK';
  action: 'REFUND' | 'REVERSAL';
  idempotencyKey: string;
}
export const ledgerPostAdjustmentV1Schema = z.object({
  adjustmentId: z.string().uuid(),
  paymentId: z.string().uuid(),
  originalTransactionId: z.string().uuid(),
  sourceLedgerAccountId: z.string().uuid(),
  destinationLedgerAccountId: z.string().uuid(),
  amountMinor: z.string().regex(/^[1-9]\d*$/),
  currency: z.enum(['EUR', 'USD', 'SEK']),
  action: z.enum(['REFUND', 'REVERSAL']),
  idempotencyKey: z.string().min(1),
});
export interface LedgerAdjustmentPostedV1 {
  adjustmentId: string;
  paymentId: string;
  transactionId: string;
  amountMinor: string;
  action: 'REFUND' | 'REVERSAL';
}
export const ledgerAdjustmentPostedV1Schema = z.object({
  adjustmentId: z.string().uuid(),
  paymentId: z.string().uuid(),
  transactionId: z.string().uuid(),
  amountMinor: z.string().regex(/^[1-9]\d*$/),
  action: z.enum(['REFUND', 'REVERSAL']),
});
