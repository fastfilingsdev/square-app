const test = require('node:test');
const assert = require('node:assert/strict');

const {
  buildPlan,
  __authNetNewOrdersTestHooks: {
    addDaysDateOnly,
    amountEqual,
    isMembershipAmount,
    matchTransactionForOrder,
    parseAmount,
    validateOrderAgainstTransaction
  }
} = require('../src/features/subscriptions/authnetNewOrdersSync');

function orderRows() {
  return [[
    'Time', 'Name', 'Email', 'Alt Email', 'Amount', 'Order / Invoice #', 'Auth.Net Transaction ID', 'Sub Created',
    'Payment Status', 'Route Target', 'Routed At', 'Review Status', 'Last AuthNet Check At', 'Connector Notes'
  ], [
    'Jun 5, 2026 09:40:07', 'Test Customer', 'customer@example.test', '', '20', '', '', '', '', '', '', '', '', ''
  ]];
}

function approvedTx(overrides = {}) {
  return {
    transId: '121657984202',
    responseCode: '1',
    transactionStatus: 'capturedPendingSettlement',
    authAmount: '20.00',
    submitTimeUTC: '2026-06-05T16:40:12Z',
    customer: { email: 'customer@example.test' },
    order: { invoiceNumber: '1467834568' },
    ...overrides
  };
}

test('billing new-order guards parse membership amounts and compute first ARB date', () => {
  assert.equal(parseAmount('$20.00'), '20.00');
  assert.equal(isMembershipAmount('20'), true);
  assert.equal(isMembershipAmount('29.00'), true);
  assert.equal(isMembershipAmount('80'), false);
  assert.equal(amountEqual('20', '20.00'), true);
  assert.equal(addDaysDateOnly('2026-06-05T16:40:12Z', 30), '2026-07-05');
});

test('matches a billing New Orders row to an approved Auth.Net transaction by email amount and time', () => {
  const rows = orderRows();
  const tableOrder = buildPlan({
    newOrderRows: rows,
    conversionRows: [['Source New Order Row', 'Auth.Net Transaction ID']],
    activeRows: [['Subscription ID']],
    onboardingRows: [],
    auth: { pulledAtUtc: '2026-06-05T20:00:00Z', records: [approvedTx()], errors: [] },
    now: '2026-06-05T20:00:00Z'
  });
  assert.equal(tableOrder.ready.length, 1);
  assert.equal(tableOrder.review.length, 0);
  assert.equal(tableOrder.conversionUpserts[0].fields['First Billing Date'], '2026-07-05');
  assert.equal(tableOrder.conversionUpserts[0].fields['Auth.Net Transaction ID'], '121657984202');
});

test('missing transaction evidence stays in review and does not route to conversion', () => {
  const plan = buildPlan({
    newOrderRows: orderRows(),
    conversionRows: [['Source New Order Row', 'Auth.Net Transaction ID']],
    activeRows: [['Subscription ID']],
    onboardingRows: [],
    auth: { pulledAtUtc: '2026-06-05T20:00:00Z', records: [], errors: [] },
    now: '2026-06-05T20:00:00Z'
  });
  assert.equal(plan.ready.length, 0);
  assert.equal(plan.conversionUpserts.length, 0);
  assert.equal(plan.review.length, 1);
  assert.equal(plan.rowUpdates[0].fields['Route Target'], 'Review / Payment Issue');
});

test('amount mismatch is blocked from Subscription Conversions and onboarding', () => {
  const rows = orderRows();
  const order = {
    rowNumber: 2,
    row: rows[1],
    map: new Map(rows[0].map((header, i) => [String(header).trim().toLowerCase().replace(/[^a-z0-9]+/g, ''), i]))
  };
  const validation = validateOrderAgainstTransaction(order, approvedTx({ authAmount: '29.00' }));
  assert.equal(validation.ok, false);
  assert.equal(validation.issues.includes('amount mismatch'), true);

  const plan = buildPlan({
    newOrderRows: rows,
    conversionRows: [['Source New Order Row', 'Auth.Net Transaction ID']],
    activeRows: [['Subscription ID']],
    onboardingRows: [],
    auth: { pulledAtUtc: '2026-06-05T20:00:00Z', records: [approvedTx({ authAmount: '29.00' })], errors: [] },
    now: '2026-06-05T20:00:00Z'
  });
  assert.equal(plan.conversionUpserts.length, 0);
  assert.equal(plan.review.length, 1);
  assert.equal(plan.rowUpdates[0].fields['Route Target'], 'Review / Payment Issue');
});

test('explicit transaction id that is not found remains review', () => {
  const rows = orderRows();
  rows[1][6] = 'missing_tx';
  const tableOrder = {
    rowNumber: 2,
    row: rows[1],
    map: new Map(rows[0].map((header, i) => [String(header).trim().toLowerCase().replace(/[^a-z0-9]+/g, ''), i]))
  };
  const match = matchTransactionForOrder(tableOrder, { records: [approvedTx()], indexes: undefined });
  assert.equal(match.tx, null);
  assert.match(match.reason, /not found/i);
});
