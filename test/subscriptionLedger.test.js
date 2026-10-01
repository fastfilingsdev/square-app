const test = require('node:test');
const assert = require('node:assert/strict');
const { createSubscriptionLedger, subscriptionFingerprint } = require('../src/core/subscriptionLedger');
const op = { providerScope: 'synthetic', transactionId: '12345678', fingerprint: 'a'.repeat(64) };
test('subscription receipt is committed before returning success', async () => {
  const sequence = [];
  const ledger = createSubscriptionLedger({ query: async sql => {
    sequence.push(sql.includes('ff_claim_') ? 'claim' : 'save');
    return { rows: [sql.includes('ff_claim_') ? { outcome: 'claimed' } : { saved: true }] };
  } });
  assert.deepEqual(await ledger.execute({ ...op, create: async () => { sequence.push('provider'); return { subscriptionId: '999' }; } }),
    { subscriptionId: '999', replayed: false });
  assert.deepEqual(sequence, ['claim', 'provider', 'save']);
});
for (const outcome of ['held', 'unknown']) test(`subscription ${outcome} cannot dispatch`, async () => {
  const ledger = createSubscriptionLedger({ query: async () => ({ rows: [{ outcome }] }) });
  await assert.rejects(ledger.execute({ ...op, create: () => assert.fail('dispatch') }), /reconciliation/);
});
test('lost claim acknowledgement cannot dispatch', async () => {
  const ledger = createSubscriptionLedger({ query: async () => { throw Error('lost'); } });
  await assert.rejects(ledger.execute({ ...op, create: () => assert.fail('dispatch') }));
});
test('saved subscription replay does not call provider or finish again', async () => {
  let queries = 0;
  const ledger = createSubscriptionLedger({ query: async () => { queries++; return { rows: [{ outcome: 'succeeded', subscription_id: '999' }] }; } });
  assert.equal((await ledger.execute({ ...op, create: () => assert.fail('dispatch') })).replayed, true);
  assert.equal(queries, 1);
});
test('order fingerprint normalizes email but binds invoice, amount and date', () => {
  const data = { invoice: '12345678', amount: '20.00', startDate: '2026-11-01', email: 'TEST@example.test' };
  const first = subscriptionFingerprint(data);
  assert.equal(first, subscriptionFingerprint({ ...data, email: ' test@example.test ' }));
  for (const key of ['invoice', 'amount', 'startDate', 'email']) assert.notEqual(first, subscriptionFingerprint({ ...data, [key]: 'changed' }));
});
