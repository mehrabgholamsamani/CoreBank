import type { MigrationInterface, QueryRunner } from 'typeorm';

export class RemovePlaintextRefreshTokens1710000000014 implements MigrationInterface {
  async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query('alter table refresh_tokens drop column if exists token;');
  }

  async down(): Promise<void> {}
}
