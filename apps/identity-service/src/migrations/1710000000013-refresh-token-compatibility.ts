import type { MigrationInterface, QueryRunner } from 'typeorm';

export class IdentityRefreshTokenCompatibility1710000000013 implements MigrationInterface {
  async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query('alter table refresh_tokens add column if not exists token text;');
  }

  async down(): Promise<void> {}
}
