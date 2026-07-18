import type { Pool } from 'pg';

export const migratePaymentSecurityAndRetries = async (pool: Pool): Promise<void> => {
  await pool.query(`
    alter table payments add column if not exists actor_id uuid;
    alter table payments add column if not exists retry_attempts integer not null default 0;
    alter table payments add column if not exists next_retry_at timestamptz;
    create index if not exists payments_actor_id_idx on payments(actor_id);
  `);
};
