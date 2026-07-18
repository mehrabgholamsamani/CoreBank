import type { MigrationInterface, QueryRunner } from 'typeorm';

export class AccountOutboxRetries1710000000010 implements MigrationInterface {
  async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      alter table outbox_messages add column if not exists attempts integer not null default 0;
      create table if not exists account_dead_letters(id uuid primary key,message jsonb not null,reason varchar not null,created_at timestamptz not null default now());
    `);
  }
  async down(): Promise<void> {}
}
