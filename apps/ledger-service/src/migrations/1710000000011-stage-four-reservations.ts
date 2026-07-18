import type { MigrationInterface, QueryRunner } from 'typeorm';

export class StageFourReservations1710000000011 implements MigrationInterface {
  async up(q: QueryRunner): Promise<void> {
    await q.query(`
      alter table balance_reservations add column if not exists payment_id uuid;
      alter table balance_reservations add column if not exists idempotency_key varchar;
      alter table balance_reservations add column if not exists released_at timestamptz;
      create unique index if not exists balance_reservations_idempotency_idx on balance_reservations(idempotency_key) where idempotency_key is not null;
      create index if not exists balance_reservations_active_idx on balance_reservations(ledger_account_id) where status = 'ACTIVE';
      create table if not exists ledger_dead_letters(id uuid primary key,message jsonb not null,reason varchar not null,created_at timestamptz not null default now());
    `);
  }
  async down(): Promise<void> {}
}
