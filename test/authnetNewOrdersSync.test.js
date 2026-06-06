const test = require('node:test');
const assert = require('node:assert/strict');

const {
  buildPlan,
  __authNetNewOrdersTestHooks: {
    addDaysDateOnly,
    amountEqual,
    discoverNewOrderRows,
    isMembershipAmount,
    isNewMembershipCheckoutTransaction,
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

test('new-order guards reject recurring ARB transactions even when amount is 20 or 29', () => {
  const rows = orderRows();
  const order = {
    rowNumber: 2,
    row: rows[1],
    map: new Map(rows[0].map((header, i) => [String(header).trim().toLowerCase().replace(/[^a-z0-9]+/g, ''), i]))
  };
  const recurringTx = approvedTx({ recurringBilling: true, subscription: { id: '73299471', payNum: '1' } });
  assert.equal(isNewMembershipCheckoutTransaction(recurringTx), false);
  const validation = validateOrderAgainstTransaction(order, recurringTx);
  assert.equal(validation.ok, false);
  assert.equal(validation.issues.includes('transaction is recurring billing, not a new checkout charge'), true);
  assert.equal(validation.issues.includes('transaction already belongs to an Auth.Net subscription'), true);
});

test('auto-discovery appends only approved non-recurring numeric-invoice membership checkout transactions', () => {
  const newCheckout = approvedTx({
    transId: '121659009517',
    authAmount: '29.00',
    submitTimeUTC: '2026-06-05T22:31:43Z',
    customer: { email: 'new@example.test' },
    billTo: { firstName: 'New', lastName: 'Customer' },
    order: { invoiceNumber: '1468015046' },
    recurringBilling: false
  });
  const recurringPayment = approvedTx({
    transId: '121659540215',
    order: { invoiceNumber: '1458447780' },
    recurringBilling: true,
    subscription: { id: '73200000', payNum: '3' }
  });
  const bCatchup = approvedTx({
    transId: '121657115124',
    order: { invoiceNumber: 'BUPD-70722730-0604' },
    profile: { customerProfileId: 'cust', customerPaymentProfileId: 'pay' }
  });

  const discovery = discoverNewOrderRows({
    newOrderRows: [orderRows()[0]],
    conversionRows: [['Source New Order Row', 'Order / Invoice #', 'Auth.Net Transaction ID', 'New Subscription ID']],
    activeRows: [['Subscription ID', 'History']],
    onboardingRows: [],
    auth: { pulledAtUtc: '2026-06-06T12:00:00Z', records: [newCheckout, recurringPayment, bCatchup], errors: [] },
    now: '2026-06-06T12:00:00Z'
  });

  assert.equal(discovery.discovered.length, 1);
  assert.equal(discovery.discovered[0].transaction.transactionId, '121659009517');
  assert.equal(discovery.discovered[0].transaction.invoiceNumber, '1468015046');
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
