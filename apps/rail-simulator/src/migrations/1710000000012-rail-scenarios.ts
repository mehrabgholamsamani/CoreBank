import type { Pool } from 'pg';
export const migrateRail = async (pool: Pool): Promise<void> => {
  await pool.query(`
  create table if not exists rail_payments (id uuid primary key, payment_id uuid not null unique, amount_minor bigint not null, currency varchar(3) not null, scenario varchar not null, rail_reference varchar not null unique, status varchar not null, accepted_at timestamptz, settled_at timestamptz, created_at timestamptz not null default now());
  create table if not exists rail_outbox_messages (id uuid primary key, topic varchar not null, message jsonb not null, published_at timestamptz, attempts integer not null default 0, created_at timestamptz not null default now());
  create table if not exists rail_inbox_messages (message_id uuid primary key, processed_at timestamptz not null default now());
  create table if not exists rail_dead_letters (id uuid primary key, message jsonb not null, reason varchar not null, created_at timestamptz not null default now());
  alter table rail_payments add column if not exists retry_attempts integer not null default 0;
  alter table rail_payments add column if not exists next_retry_at timestamptz;
`);
};
