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
