import type { MigrationInterface, QueryRunner } from 'typeorm';
export class AccountLedgerLinks1710000000011 implements MigrationInterface {
  async up(q: QueryRunner): Promise<void> {
    await q.query(
      'alter table accounts add column if not exists ledger_account_id uuid; create unique index if not exists accounts_ledger_account_idx on accounts(ledger_account_id) where ledger_account_id is not null; create table if not exists account_inbox_messages(message_id uuid primary key,processed_at timestamptz not null default now());',
    );
  }
  async down(): Promise<void> {}
}
