import type { Pool } from 'pg';

export const migratePayments = async (pool: Pool): Promise<void> => {
  await pool.query(`
    create table if not exists payments (
      id uuid primary key, idempotency_key varchar not null unique, request_hash varchar not null,
      source_ledger_account_id uuid not null, destination_ledger_account_id uuid not null,
      amount_minor bigint not null check(amount_minor > 0), currency varchar(3) not null check(currency in ('EUR','USD','SEK')),
      kind varchar not null check(kind = 'INTERNAL_TRANSFER'), status varchar not null,
      reservation_id uuid, ledger_transaction_id uuid, audit jsonb not null, failure_reason varchar,
      created_at timestamptz not null default now(), updated_at timestamptz not null default now()
    );
    create table if not exists payment_outbox_messages (
      id uuid primary key, topic varchar not null, message jsonb not null, published_at timestamptz, attempts integer not null default 0,
      created_at timestamptz not null default now()
    );
    create table if not exists payment_inbox_messages (message_id uuid primary key, processed_at timestamptz not null default now());
    create table if not exists payment_dead_letters (id uuid primary key, message jsonb not null, reason varchar not null, created_at timestamptz not null default now());
  `);
};
