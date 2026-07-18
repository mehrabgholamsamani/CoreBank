import { HttpException, Injectable } from '@nestjs/common';
import { correlation } from '@corebank/correlation';
import { messageEnvelope } from '@corebank/event-contracts';
import { createHash, randomUUID } from 'node:crypto';
import type { QueryRunner } from 'typeorm';
import { ledgerDataSource } from './data-source';

export type Posting = {
  accountId: string;
  side: 'DEBIT' | 'CREDIT';
  amountMinor: string;
  currency: string;
};
export type LedgerActor = { sub: string; role: 'CUSTOMER' | 'ADMIN' };

@Injectable()
export class LedgerService {
  private async transaction<T>(operation: (runner: QueryRunner) => Promise<T>): Promise<T> {
    const runner = ledgerDataSource.createQueryRunner();
    await runner.connect();
    await runner.startTransaction();
    try {
      const result = await operation(runner);
      await runner.commitTransaction();
      return result;
    } catch (error) {
      await runner.rollbackTransaction();
      throw error;
    } finally {
      await runner.release();
    }
  }

  async createAccount(ownerId: string, currency: string) {
    if (!ownerId || !['EUR', 'USD', 'SEK'].includes(currency))
      throw new HttpException('invalid ledger account', 400);
    const id = randomUUID();
    await ledgerDataSource.query(
      "insert into ledger_accounts(id,owner_id,currency,status) values($1,$2::uuid,$3,'ACTIVE')",
      [id, ownerId, currency],
    );
    return { id, ownerId, currency, status: 'ACTIVE' };
  }
  async createLinkedAccount(
    runner: QueryRunner,
    externalAccountId: string,
    ownerId: string,
    currency: string,
  ) {
    const existing = (
      await runner.query(
        'select id,currency from ledger_accounts where external_account_id=$1::uuid for update',
        [externalAccountId],
      )
    )[0];
    if (existing) return { id: existing.id, currency: existing.currency };
    const id = randomUUID();
    await runner.query(
      "insert into ledger_accounts(id,owner_id,currency,status,external_account_id) values($1,$2::uuid,$3,'ACTIVE',$4::uuid)",
      [id, ownerId, currency, externalAccountId],
    );
    return { id, currency };
  }
  async sandboxFund(
    customerLedgerAccountId: string,
    amountMinor: string,
    currency: string,
    idempotencyKey: string,
  ) {
    if (process.env.SANDBOX_MODE !== 'true')
      throw new HttpException('sandbox funding disabled', 404);
    const fundingOwner = '00000000-0000-4000-8000-000000000001';
    return this.transaction(async (runner) => {
      const funding = (
        await runner.query(
          'select id from ledger_accounts where owner_id=$1::uuid and currency=$2 and external_account_id is null order by created_at limit 1 for update',
          [fundingOwner, currency],
        )
      )[0];
      const fundingId = funding?.id ?? randomUUID();
      if (!funding)
        await runner.query(
          "insert into ledger_accounts(id,owner_id,currency,status) values($1,$2::uuid,$3,'ACTIVE')",
          [fundingId, fundingOwner, currency],
        );
      return this.postInTransaction(runner, idempotencyKey, [
        { accountId: fundingId, side: 'DEBIT', amountMinor, currency },
        { accountId: customerLedgerAccountId, side: 'CREDIT', amountMinor, currency },
      ]);
    });
  }

  private async postInTransaction(runner: QueryRunner, key: string, postings: Posting[]) {
    if (!key || postings.length < 2)
      throw new HttpException('idempotency key and two entries required', 400);
    const hash = createHash('sha256').update(JSON.stringify(postings)).digest('hex');
    const old = (
      await runner.query('select * from ledger_transactions where idempotency_key=$1 for update', [
        key,
      ])
    )[0];
    if (old) {
      if (old.request_hash !== hash) throw new HttpException('idempotency conflict', 409);
      return { id: old.id, status: old.status };
    }
    const totals = new Map<string, [bigint, bigint]>();
    for (const posting of postings) {
      if (!/^[1-9]\\d*$/.test(posting.amountMinor) || !['DEBIT', 'CREDIT'].includes(posting.side))
        throw new HttpException('invalid posting', 400);
      const total = totals.get(posting.currency) ?? [0n, 0n];
      total[posting.side === 'DEBIT' ? 0 : 1] += BigInt(posting.amountMinor);
      totals.set(posting.currency, total);
    }
    for (const [debits, credits] of totals.values())
      if (debits !== credits) throw new HttpException('unbalanced entries', 400);
    const accountIds = [...new Set(postings.map((posting) => posting.accountId))];
    const accounts = await runner.query(
      'select * from ledger_accounts where id=any($1::uuid[]) for update',
      [accountIds],
    );
    if (accounts.length !== accountIds.length)
      throw new HttpException('ledger account not found', 404);
    for (const posting of postings) {
      const account = accounts.find((value: { id: string }) => value.id === posting.accountId);
      if (!account || account.status !== 'ACTIVE' || account.currency !== posting.currency)
        throw new HttpException('invalid ledger account', 400);
    }
    const id = randomUUID();
    await runner.query(
      "insert into ledger_transactions(id,idempotency_key,request_hash,status) values($1,$2,$3,'POSTED')",
      [id, key, hash],
    );
    for (const posting of postings)
      await runner.query(
        'insert into ledger_entries(id,transaction_id,ledger_account_id,side,amount_minor,currency) values($1,$2,$3::uuid,$4,$5::bigint,$6)',
        [randomUUID(), id, posting.accountId, posting.side, posting.amountMinor, posting.currency],
      );
    const event = messageEnvelope(
      {
        messageId: randomUUID(),
        messageType: 'ledger.transaction-posted.v1',
        messageVersion: 1,
        aggregateId: id,
        correlationId: correlation.get()?.correlationId ?? randomUUID(),
        producer: 'ledger-service',
        occurredAt: new Date().toISOString(),
      },
      { transactionId: id },
    );
    await runner.query(
      'insert into outbox_messages(id,message_type,payload) values($1,$2,$3::jsonb)',
      [event.messageId, event.messageType, JSON.stringify(event)],
    );
    return { id, status: 'POSTED' };
  }

  async post(key: string, postings: Posting[]) {
    if (!key || postings.length < 2)
      throw new HttpException('idempotency key and two entries required', 400);
    return this.transaction((runner) => this.postInTransaction(runner, key, postings));
  }

  private async reserveInTransaction(
    runner: QueryRunner,
    accountId: string,
    amountMinor: string,
    currency: string,
    idempotencyKey: string,
    paymentId?: string,
    actorId?: string,
  ) {
    if (!/^[1-9]\\d*$/.test(amountMinor) || !idempotencyKey)
      throw new HttpException('idempotency key and valid amount required', 400);
    const previous = (
      await runner.query('select * from balance_reservations where idempotency_key=$1 for update', [
        idempotencyKey,
      ])
    )[0];
    if (previous) {
      if (
        previous.ledger_account_id !== accountId ||
        String(previous.amount_minor) !== amountMinor ||
        previous.currency !== currency
      )
        throw new HttpException('idempotency conflict', 409);
      return { id: previous.id, accountId, amountMinor, currency, status: previous.status };
    }
    const account = (
      await runner.query('select * from ledger_accounts where id=$1::uuid for update', [accountId])
    )[0];
    if (!account || account.status !== 'ACTIVE' || account.currency !== currency)
      throw new HttpException('invalid ledger account', 400);
    if (actorId && account.owner_id !== actorId) throw new HttpException('forbidden', 403);
    const posted = (
      await runner.query(
        "select coalesce(sum(case when side='CREDIT' then amount_minor else -amount_minor end),0)::text as amount from ledger_entries where ledger_account_id=$1::uuid",
        [accountId],
      )
    )[0].amount;
    const reserved = (
      await runner.query(
        "select coalesce(sum(amount_minor),0)::text as amount from balance_reservations where ledger_account_id=$1::uuid and status='ACTIVE'",
        [accountId],
      )
    )[0].amount;
    if (BigInt(posted) - BigInt(reserved) < BigInt(amountMinor))
      throw new HttpException('insufficient available balance', 409);
    const id = randomUUID();
    await runner.query(
      "insert into balance_reservations(id,ledger_account_id,amount_minor,currency,status,payment_id,idempotency_key) values($1,$2::uuid,$3::bigint,$4,'ACTIVE',$5::uuid,$6)",
      [id, accountId, amountMinor, currency, paymentId ?? null, idempotencyKey],
    );
    return { id, accountId, amountMinor, currency, status: 'ACTIVE' };
  }

  async reserve(
    accountId: string,
    amountMinor: string,
    currency: string,
    idempotencyKey = '',
    paymentId?: string,
  ) {
    return this.transaction((runner) =>
      this.reserveInTransaction(
        runner,
        accountId,
        amountMinor,
        currency,
        idempotencyKey,
        paymentId,
      ),
    );
  }

  private async releaseInTransaction(
    runner: QueryRunner,
    reservationId: string,
    idempotencyKey: string,
  ) {
    if (!idempotencyKey) throw new HttpException('idempotency key required', 400);
    const row = (
      await runner.query('select * from balance_reservations where id=$1::uuid for update', [
        reservationId,
      ])
    )[0];
    if (!row) throw new HttpException('reservation not found', 404);
    if (row.status === 'ACTIVE')
      await runner.query(
        "update balance_reservations set status='RELEASED',released_at=now() where id=$1::uuid",
        [reservationId],
      );
    return { id: reservationId, status: 'RELEASED' };
  }

  async releaseReservation(reservationId: string, idempotencyKey: string) {
    return this.transaction((runner) =>
      this.releaseInTransaction(runner, reservationId, idempotencyKey),
    );
  }

  private async internalTransferInTransaction(
    runner: QueryRunner,
    reservationId: string,
    sourceAccountId: string,
    destinationAccountId: string,
    amountMinor: string,
    currency: string,
    idempotencyKey: string,
  ) {
    const reservation = (
      await runner.query('select * from balance_reservations where id=$1::uuid for update', [
        reservationId,
      ])
    )[0];
    if (
      !reservation ||
      reservation.status !== 'ACTIVE' ||
      reservation.ledger_account_id !== sourceAccountId ||
      String(reservation.amount_minor) !== amountMinor ||
      reservation.currency !== currency
    )
      throw new HttpException('invalid active reservation', 409);
    const result = await this.postInTransaction(runner, idempotencyKey, [
      { accountId: sourceAccountId, side: 'DEBIT', amountMinor, currency },
      { accountId: destinationAccountId, side: 'CREDIT', amountMinor, currency },
    ]);
    await runner.query(
      "update balance_reservations set status='RELEASED',released_at=now() where id=$1::uuid",
      [reservationId],
    );
    return { ...result, reservationId };
  }

  async internalTransfer(
    reservationId: string,
    sourceAccountId: string,
    destinationAccountId: string,
    amountMinor: string,
    currency: string,
    idempotencyKey: string,
  ) {
    return this.transaction((runner) =>
      this.internalTransferInTransaction(
        runner,
        reservationId,
        sourceAccountId,
        destinationAccountId,
        amountMinor,
        currency,
        idempotencyKey,
      ),
    );
  }

  private async reverseInTransaction(runner: QueryRunner, id: string, key: string) {
    const rows = await runner.query(
      'select ledger_account_id as "accountId",side,amount_minor as "amountMinor",currency from ledger_entries where transaction_id=$1::uuid',
      [id],
    );
    if (!rows.length) throw new HttpException('transaction not found', 404);
    const transaction = await this.postInTransaction(
      runner,
      key,
      rows.map((entry: Posting) => ({
        ...entry,
        side: entry.side === 'DEBIT' ? 'CREDIT' : 'DEBIT',
        amountMinor: String(entry.amountMinor),
      })),
    );
    await runner.query('update ledger_transactions set reversal_of=$1::uuid where id=$2::uuid', [
      id,
      transaction.id,
    ]);
    return transaction;
  }

  async reverse(id: string, key: string) {
    return this.transaction((runner) => this.reverseInTransaction(runner, id, key));
  }

  async processReserve(
    runner: QueryRunner,
    accountId: string,
    amountMinor: string,
    currency: string,
    key: string,
    paymentId: string,
    actorId?: string,
  ) {
    return this.reserveInTransaction(
      runner,
      accountId,
      amountMinor,
      currency,
      key,
      paymentId,
      actorId,
    );
  }
  async processRelease(runner: QueryRunner, reservationId: string, key: string) {
    return this.releaseInTransaction(runner, reservationId, key);
  }
  async processTransfer(
    runner: QueryRunner,
    reservationId: string,
    source: string,
    destination: string,
    amount: string,
    currency: string,
    key: string,
  ) {
    return this.internalTransferInTransaction(
      runner,
      reservationId,
      source,
      destination,
      amount,
      currency,
      key,
    );
  }
  async processAdjustment(
    runner: QueryRunner,
    action: 'REFUND' | 'REVERSAL',
    originalId: string,
    source: string,
    destination: string,
    amount: string,
    currency: string,
    key: string,
  ) {
    return action === 'REVERSAL'
      ? this.reverseInTransaction(runner, originalId, key)
      : this.postInTransaction(runner, key, [
          { accountId: destination, side: 'DEBIT', amountMinor: amount, currency },
          { accountId: source, side: 'CREDIT', amountMinor: amount, currency },
        ]);
  }

  async balance(id: string, actor: LedgerActor) {
    const account = (
      await ledgerDataSource.query(
        'select id,owner_id,currency from ledger_accounts where id=$1::uuid',
        [id],
      )
    )[0];
    if (!account || (actor.role !== 'ADMIN' && account.owner_id !== actor.sub))
      throw new HttpException('ledger account not found', 404);
    const result = (
      await ledgerDataSource.query(
        "select coalesce(sum(case when side='CREDIT' then amount_minor else -amount_minor end),0)::text as balance from ledger_entries where ledger_account_id=$1::uuid",
        [id],
      )
    )[0];
    return { accountId: id, currency: account.currency, balanceMinor: result.balance };
  }
  async history(id: string, actor: LedgerActor) {
    await this.balance(id, actor);
    return ledgerDataSource.query(
      'select id,transaction_id as "transactionId",side,amount_minor::text as "amountMinor",currency,created_at as "createdAt" from ledger_entries where ledger_account_id=$1::uuid order by created_at,id',
      [id],
    );
  }
}
