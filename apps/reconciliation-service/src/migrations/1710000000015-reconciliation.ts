import type { Pool } from 'pg';
export const migrateReconciliation = async (pool: Pool): Promise<void> => {
  await pool.query(`
    create table if not exists settlement_files (id uuid primary key, source varchar not null, checksum varchar not null unique, rows jsonb not null, imported_at timestamptz not null default now());
    create table if not exists reconciliation_runs (id uuid primary key, settlement_file_id uuid not null references settlement_files(id), status varchar not null, created_at timestamptz not null default now(), completed_at timestamptz);
    create table if not exists discrepancies (id uuid primary key, run_id uuid not null references reconciliation_runs(id), external_reference varchar not null, expected_minor bigint, actual_minor bigint, currency varchar(3), kind varchar not null, status varchar not null default 'OPEN', resolution jsonb, created_at timestamptz not null default now(), resolved_at timestamptz);
    create table if not exists reconciliation_audit (id uuid primary key, discrepancy_id uuid not null references discrepancies(id), action varchar not null, actor_id varchar not null, correlation_id varchar not null, details jsonb not null, occurred_at timestamptz not null default now());
    create table if not exists reconciliation_inbox (message_id uuid primary key, processed_at timestamptz not null default now());
  `);
};
