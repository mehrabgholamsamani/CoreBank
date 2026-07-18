import type { MigrationInterface, QueryRunner } from 'typeorm';
export class LedgerAccountLinks1710000000012 implements MigrationInterface {
  async up(q: QueryRunner): Promise<void> {
    await q.query(
      'alter table ledger_accounts add column if not exists external_account_id uuid; create unique index if not exists ledger_accounts_external_account_idx on ledger_accounts(external_account_id) where external_account_id is not null;',
    );
  }
  async down(): Promise<void> {}
}
