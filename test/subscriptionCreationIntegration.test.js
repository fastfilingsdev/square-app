const test = require('node:test');
const assert = require('node:assert/strict');
const { buildPlan, __authNetNewOrdersTestHooks: hooks } = require('../src/features/subscriptions/authnetNewOrdersSync');
function fixture(invoice = '1467834568') {
  const tx = { transId: '121657984202', responseCode: '1', transactionStatus: 'capturedPendingSettlement',
    authAmount: '20.00', submitTimeUTC: '2026-06-05T16:40:12Z', customer: { email: 'customer@example.test' },
    order: { invoiceNumber: invoice } };
  return buildPlan({ newOrderRows: [
    ['Time', 'Name', 'Email', 'Amount', 'Order / Invoice #', 'Auth.Net Transaction ID', 'Sub Created'],
    ['Jun 5, 2026 09:40:07', 'Synthetic', 'customer@example.test', '20', invoice, tx.transId, '']
  ], conversionRows: [['Source New Order Row', 'Auth.Net Transaction ID']], activeRows: [['Subscription ID']],
  onboardingRows: [], auth: { pulledAtUtc: '2026-06-05T20:00:00Z', records: [tx], errors: [] }, now: '2026-06-05T20:00:00Z' });
}
// The synthetic charge is months old; approve it explicitly so these tests
// exercise the ledger path behind the backlog hold.
process.env.FF_BILLING_ARB_APPROVED_TRANSACTION_IDS = '121657984202';
test('backlog hold keeps an old, unapproved charge away from the ledger and provider', async () => {
  const saved = process.env.FF_BILLING_ARB_APPROVED_TRANSACTION_IDS;
  delete process.env.FF_BILLING_ARB_APPROVED_TRANSACTION_IDS;
  try {
    const plan = fixture(); let claims = 0;
    const result = await hooks.maybeCreateArbs({ plan, arbLiveEnabled: true, arbLiveRequested: true, providerScope: 'synthetic',
      subscriptionLedger: { execute: async () => { claims++; throw Error('must not be called'); } } });
    assert.equal(claims, 0); assert.equal(result[0].status, 'held-owner-approval');
    assert.equal(plan.conversionUpserts[0].fields['ARB Creation Status'], 'Held — owner approval required');
    assert.equal(plan.rowUpdates[0].fields['Payment Status'], 'Review — held for owner approval');
  } finally { process.env.FF_BILLING_ARB_APPROVED_TRANSACTION_IDS = saved; }
});
test('backlog hold passes a fresh charge and can be switched off', () => {
  const saved = process.env.FF_BILLING_ARB_APPROVED_TRANSACTION_IDS;
  delete process.env.FF_BILLING_ARB_APPROVED_TRANSACTION_IDS;
  try {
    const now = Date.parse('2026-06-05T20:00:00Z');
    const tx = { transId: '1', submitTimeUTC: '2026-06-05T16:40:12Z' };
    assert.equal(hooks.arbBacklogHoldReason(tx, now), '');
    assert.match(hooks.arbBacklogHoldReason(tx, now + 48 * 3600 * 1000), /older than 24h/);
    assert.match(hooks.arbBacklogHoldReason({ transId: '2' }, now), /time unknown/);
    process.env.FF_BILLING_ARB_AUTO_MAX_AGE_HOURS = '0';
    assert.equal(hooks.arbBacklogHoldReason({ transId: '2' }, now), '');
  } finally { process.env.FF_BILLING_ARB_APPROVED_TRANSACTION_IDS = saved; delete process.env.FF_BILLING_ARB_AUTO_MAX_AGE_HOURS; }
});
test('E00040 is recognised from structured and text Auth.Net errors', () => {
  assert.equal(hooks.authNetErrorCodes({ authNetFailure: { messages: [{ code: 'E00040' }] } }).has('E00040'), true);
  assert.equal(hooks.authNetErrorCodes(Error('E00040 The record cannot be found.')).has('E00040'), true);
  assert.equal(hooks.authNetErrorCodes(Error('E00012 duplicate')).has('E00040'), false);
});
test('a ledger rejection before ARB does not count as a provider write', async () => {
  const plan = fixture();
  const progress = {};
  const result = await hooks.maybeCreateArbs({
    plan, arbLiveEnabled: true, arbLiveRequested: true, providerScope: 'synthetic', progress,
    subscriptionLedger: { execute: async () => { throw Error('Subscription creation held for reconciliation'); } }
  });
  assert.equal(result[0].status, 'failed');
  assert.equal(progress.providerWriteAttempted, undefined);
});
test('actual ARB path fails closed without the durable ledger', async () => {
  const plan = fixture(); assert.equal(plan.conversionUpserts.length, 1);
  const result = await hooks.maybeCreateArbs({ plan, arbLiveEnabled: true, arbLiveRequested: true });
  assert.equal(result[0].status, 'failed'); assert.match(result[0].reason, /ledger/);
  assert.equal(plan.activeInserts.length, 0);
});
test('actual ARB path uses a durable receipt without invoking profile or ARB creation', async () => {
  const plan = fixture(); let claims = 0;
  const result = await hooks.maybeCreateArbs({ plan, arbLiveEnabled: true, arbLiveRequested: true, providerScope: 'synthetic',
    subscriptionLedger: { execute: async input => {
      claims++; assert.equal(input.transactionId, '121657984202'); assert.match(input.fingerprint, /^[a-f0-9]{64}$/);
      // Deliberately never invoke input.create: no provider or profile writes.
      return { subscriptionId: '999999', replayed: true };
    } } });
  assert.equal(claims, 1); assert.equal(result[0].status, 'replayed-durable-receipt');
  assert.equal(plan.conversionUpserts[0].fields['New Subscription ID'], '999999');
});
test('actual ARB path cannot dispatch when durable claim is held', async () => {
  const plan = fixture();
  const result = await hooks.maybeCreateArbs({ plan, arbLiveEnabled: true, arbLiveRequested: true, providerScope: 'synthetic',
    subscriptionLedger: { execute: async () => { throw Error('held for reconciliation'); } } });
  assert.equal(result[0].status, 'failed'); assert.equal(plan.activeInserts.length, 0);
});
test('Google RST recovery invoices and backend numeric checkout invoices are disjoint', () => {
  for (const invoice of ['RST-123456', 'rst-123456', 'RST-123456-extra']) {
    const plan = fixture(invoice);
    assert.equal(plan.conversionUpserts.length, 0);
    assert.equal(plan.ready.length, 0);
  }
  assert.equal(fixture().ready.length, 1);
});
