import type { QueryRunner } from 'typeorm';
import { ledgerDataSource } from './data-source';

/** Applies an inbound message and all resulting effects in one local transaction. */
export const processLedgerInbox = async <T>(
  messageId: string,
  operation: (runner: QueryRunner) => Promise<T>,
): Promise<{ processed: boolean; value?: T }> => {
  const runner = ledgerDataSource.createQueryRunner();
  await runner.connect();
  await runner.startTransaction();
  try {
    const inserted = await runner.query(
      'insert into inbox_messages(message_id) values($1::uuid) on conflict do nothing returning message_id',
      [messageId],
    );
    if (!inserted.length) {
      await runner.commitTransaction();
      return { processed: false };
    }
    const value = await operation(runner);
    await runner.commitTransaction();
    return { processed: true, value };
  } catch (error) {
    await runner.rollbackTransaction();
    throw error;
  } finally {
    await runner.release();
  }
};
