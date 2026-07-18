import assert from 'node:assert/strict';
import test from 'node:test';

const baseUrl = process.env.BANK_API_URL ?? 'http://localhost:3010';
const enabled = process.env.RUN_INTEGRATION === '1';
const waitFor = async (operation, predicate, timeoutMs = 15_000) => {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await operation();
    if (predicate(value)) return value;
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  assert.fail('timed out waiting for asynchronous workflow');
};

test(
  'bootstraps, links accounts, funds them, and settles an internal payment',
  { skip: !enabled },
  async () => {
    const request = async (path, options = {}) => {
      const response = await fetch(`${baseUrl}${path}`, options);
      if (!response.ok) assert.fail(`${path}: ${response.status} ${await response.text()}`);
      return response.json();
    };
    const admin = await request('/auth/login', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        email: process.env.SANDBOX_ADMIN_EMAIL ?? 'admin@corebank.local',
        password: process.env.SANDBOX_ADMIN_PASSWORD ?? 'corebank-local-admin-password',
      }),
    });
    const email = `workflow-${crypto.randomUUID()}@example.test`;
    await request('/auth/register', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ email, password: 'correct-horse-battery-staple' }),
    });
    const session = await request('/auth/login', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ email, password: 'correct-horse-battery-staple' }),
    });
    const customerHeaders = {
      'content-type': 'application/json',
      authorization: `Bearer ${session.accessToken}`,
    };
    const customer = await request('/customers', {
      method: 'POST',
      headers: customerHeaders,
      body: JSON.stringify({ displayName: 'Workflow customer' }),
    });
    await request('/accounts', {
      method: 'POST',
      headers: customerHeaders,
      body: JSON.stringify({ customerId: customer.id, currency: 'EUR' }),
    });
    await request('/accounts', {
      method: 'POST',
      headers: customerHeaders,
      body: JSON.stringify({ customerId: customer.id, currency: 'EUR' }),
    });
    const accounts = await waitFor(
      () => request('/accounts', { headers: customerHeaders }),
      (value) => value.length === 2 && value.every((account) => account.ledgerAccountId),
    );
    await request('/ledger/sandbox-funding', {
      method: 'POST',
      headers: { 'content-type': 'application/json', authorization: `Bearer ${admin.accessToken}` },
      body: JSON.stringify({
        customerLedgerAccountId: accounts[0].ledgerAccountId,
        amountMinor: '10000',
        currency: 'EUR',
        idempotencyKey: crypto.randomUUID(),
      }),
    });
    const payment = await request('/payments', {
      method: 'POST',
      headers: customerHeaders,
      body: JSON.stringify({
        sourceAccountId: accounts[0].id,
        destinationAccountId: accounts[1].id,
        amountMinor: '2500',
        currency: 'EUR',
        idempotencyKey: crypto.randomUUID(),
      }),
    });
    const settled = await waitFor(
      () => request(`/payments/${payment.id}`, { headers: customerHeaders }),
      (value) => value.status === 'SETTLED',
    );
    assert.equal(settled.amountMinor, '2500');
  },
);
