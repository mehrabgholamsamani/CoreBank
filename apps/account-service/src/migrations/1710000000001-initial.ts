import type { MigrationInterface, QueryRunner } from 'typeorm';
export class InitialAccount1710000000001 implements MigrationInterface {
  async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `create table customers (id uuid primary key, user_id uuid not null unique, display_name text not null, audit jsonb not null, created_at timestamptz not null default now()); create table accounts (id uuid primary key, customer_id uuid not null references customers(id), currency varchar(3) not null check(currency in ('EUR','USD','SEK')), status text not null check(status in ('ACTIVE','SUSPENDED','CLOSED')), audit jsonb not null, created_at timestamptz not null default now()); create index accounts_customer_id_idx on accounts(customer_id); create table outbox_messages (id uuid primary key, topic text not null, message jsonb not null, published_at timestamptz, created_at timestamptz not null default now());`,
    );
  }
  async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      'drop table outbox_messages; drop table accounts; drop table customers;',
    );
  }
}
