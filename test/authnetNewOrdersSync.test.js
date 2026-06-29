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

test('auto-discovery is disabled by default because Formstack owns New Orders row creation', () => {
  const newCheckout = approvedTx({
    transId: '121659009517',
    authAmount: '29.00',
    submitTimeUTC: '2026-06-05T22:31:43Z',
    customer: { email: 'new@example.test' },
    billTo: { firstName: 'New', lastName: 'Customer' },
    order: { invoiceNumber: '1468015046' },
    recurringBilling: false
  });

  const discovery = discoverNewOrderRows({
    newOrderRows: [orderRows()[0]],
    conversionRows: [['Source New Order Row', 'Order / Invoice #', 'Auth.Net Transaction ID', 'New Subscription ID']],
    activeRows: [['Subscription ID', 'History']],
    onboardingRows: [],
    auth: { pulledAtUtc: '2026-06-06T12:00:00Z', records: [newCheckout], errors: [] },
    now: '2026-06-06T12:00:00Z'
  });

  assert.equal(discovery.discovered.length, 0);
});

test('explicitly enabled auto-discovery includes only approved non-recurring numeric-invoice membership checkout transactions', () => {
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
    now: '2026-06-06T12:00:00Z',
    allowAutoDiscovery: true
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

test('duplicate New Orders rows are resolved without another ARB attempt', () => {
  const headers = orderRows()[0];
  const duplicateRows = [headers, [
    'Jun 8, 2026 15:04:42', 'Courtney Henig', 'courtney@example.test', '', '20', '1468775516', '121662468989', 'TRUE',
    'Subscription created', 'Active Subscriptions / Onboarding', '', '', '', 'ARB subscription 73319055 created starting 2026-07-08.'
  ], [
    '2026-06-08T19:04:43.057Z', 'Courtney Henig', 'courtney@example.test', '', '20', '1468775516', '121662468989', '',
    'Review — ARB failed', 'Review / Payment Issue', '', 'ARB failed: E00012 You have submitted a duplicate of Subscription 73319055. A duplicate subscription will not be created.', '',
    'ARB failed; not onboarded: E00012 You have submitted a duplicate of Subscription 73319055. A duplicate subscription will not be created.'
  ]];
  const conversionRows = [[
    'Source New Order Row', 'Order / Invoice #', 'Auth.Net Transaction ID', 'Email', 'Customer ID', 'Amount',
    'Desired Monthly Amount', 'First Billing Date', 'Profile Creation Status', 'ARB Creation Status',
    'New Subscription ID', 'Approval Evidence', 'Notes', 'Name', 'Routed At', 'Last Updated At'
  ], [
    '2', '1468775516', '121662468989', 'courtney@example.test', '', '20', '20', '2026-07-08',
    'Created from original transaction', 'Created', '73319055', '', 'ARB created starting 2026-07-08.', 'Courtney Henig', '', ''
  ], [
    '3', '1468775516', '121662468989', 'courtney@example.test', '', '20', '20', '2026-07-08',
    'Created from original transaction', 'Failed — E00012 You have submitted a duplicate of Subscription 73319055.', '', '', 'ARB failed.', 'Courtney Henig', '', ''
  ]];

  const plan = buildPlan({
    newOrderRows: duplicateRows,
    conversionRows,
    activeRows: [['Subscription ID'], ['73319055']],
    onboardingRows: [],
    auth: { pulledAtUtc: '2026-06-08T22:10:00Z', records: [approvedTx({
      transId: '121662468989',
      order: { invoiceNumber: '1468775516' },
      submitTimeUTC: '2026-06-08T19:04:43.057Z',
      customer: { email: 'courtney@example.test' }
    })], errors: [] },
    now: '2026-06-08T22:10:00Z'
  });

  assert.equal(plan.conversionUpserts.length, 0);
  assert.equal(plan.ready.length, 0);
  assert.equal(plan.rowUpdates.length, 1);
  assert.equal(plan.rowUpdates[0].rowNumber, 3);
  assert.equal(plan.rowUpdates[0].fields['Payment Status'], 'Duplicate — already routed');
  assert.match(plan.rowUpdates[0].fields['Review Status'], /Duplicate of New Orders row 2/);
  assert.match(plan.rowUpdates[0].fields['Review Status'], /73319055/);
  assert.equal(plan.skipped.some(item => item.reason === 'Duplicate of New Orders row 2'), true);
});

test('duplicate successful New Orders rows are downgraded to no-action duplicates', () => {
  const headers = orderRows()[0];
  const duplicateRows = [headers, [
    'Jun 8, 2026 18:00:02', 'Fiston Mucyo', 'fiston@example.test', '', '29', '1468841394', '121662781843', 'TRUE',
    'Subscription created', 'Active Subscriptions / Onboarding', '', '', '', 'ARB subscription 73321684 created starting 2026-07-08.'
  ], [
    '2026-06-08T22:00:03.1Z', 'Fiston Mucyo', 'fiston@example.test', '', '29', '1468841394', '121662781843', 'TRUE',
    'Subscription created', 'Active Subscriptions / Onboarding', '', '', '', 'ARB subscription 73321684 created starting 2026-07-08.'
  ]];
  const conversionRows = [[
    'Source New Order Row', 'Order / Invoice #', 'Auth.Net Transaction ID', 'Email', 'Customer ID', 'Amount',
    'Desired Monthly Amount', 'First Billing Date', 'Profile Creation Status', 'ARB Creation Status',
    'New Subscription ID', 'Approval Evidence', 'Notes', 'Name', 'Routed At', 'Last Updated At'
  ], [
    '2', '1468841394', '121662781843', 'fiston@example.test', '', '29', '29', '2026-07-08',
    'Created from original transaction', 'Created', '73321684', '', 'ARB created starting 2026-07-08.', 'Fiston Mucyo', '', ''
  ], [
    '3', '1468841394', '121662781843', 'fiston@example.test', '', '29', '29', '2026-07-08',
    'Created from original transaction', 'Created', '73321684', '', 'ARB created starting 2026-07-08.', 'Fiston Mucyo', '', ''
  ]];

  const plan = buildPlan({
    newOrderRows: duplicateRows,
    conversionRows,
    activeRows: [['Subscription ID'], ['73321684']],
    onboardingRows: [],
    auth: { pulledAtUtc: '2026-06-08T22:30:00Z', records: [approvedTx({
      transId: '121662781843',
      authAmount: '29.00',
      order: { invoiceNumber: '1468841394' },
      submitTimeUTC: '2026-06-08T22:00:03.1Z',
      customer: { email: 'fiston@example.test' }
    })], errors: [] },
    now: '2026-06-08T22:30:00Z'
  });

  assert.equal(plan.conversionUpserts.length, 0);
  assert.equal(plan.rowUpdates.length, 1);
  assert.equal(plan.rowUpdates[0].rowNumber, 3);
  assert.equal(plan.rowUpdates[0].fields['Sub Created'], '');
  assert.equal(plan.rowUpdates[0].fields['Payment Status'], 'Duplicate — already routed');
  assert.match(plan.rowUpdates[0].fields['Review Status'], /Duplicate of New Orders row 2/);
  assert.match(plan.rowUpdates[0].fields['Review Status'], /73321684/);
});

test('same email with existing active membership is blocked for duplicate review before ARB creation', () => {
  const rows = orderRows();
  rows[1][1] = 'Duplicate Customer';
  rows[1][2] = 'duplicate@example.test';
  rows[1][4] = '29';
  rows[1][5] = '9001000002';
  rows[1][6] = '9002000002';

  const plan = buildPlan({
    newOrderRows: rows,
    conversionRows: [['Source New Order Row', 'Auth.Net Transaction ID']],
    activeRows: [[
      'Time', 'Subscription ID', 'Customer ID', 'Name', 'Email', 'Alt Email', 'Amount', 'Onboarding Status', 'Notes', 'LTV', 'History'
    ], [
      '2026-07-18', '90030001', 'CUST-1', 'Duplicate Customer', 'duplicate@example.test', '', '29', '', '', '', ''
    ]],
    onboardingRows: [],
    auth: { pulledAtUtc: '2026-06-20T20:00:00Z', records: [approvedTx({
      transId: '9002000002',
      authAmount: '29.00',
      order: { invoiceNumber: '9001000002' },
      submitTimeUTC: '2026-06-20T19:36:20Z',
      customer: { email: 'duplicate@example.test' }
    })], errors: [] },
    now: '2026-06-20T20:00:00Z'
  });

  assert.equal(plan.conversionUpserts.length, 0);
  assert.equal(plan.ready.length, 0);
  assert.equal(plan.rowUpdates.length, 1);
  assert.equal(plan.rowUpdates[0].fields['Payment Status'], 'Review — possible duplicate order');
  assert.equal(plan.rowUpdates[0].fields['Route Target'], 'Duplicate / Review');
  assert.match(plan.rowUpdates[0].fields['Review Status'], /90030001/);
  assert.match(plan.rowUpdates[0].fields['Review Status'], /CUST-1/);
});

test('same sync run blocks second same-email new order with different invoice and transaction', () => {
  const headers = orderRows()[0];
  const rows = [headers, [
    'Jun 18, 2026 09:28:25', 'Duplicate Customer', 'duplicate@example.test', '', '29', '9001000001', '9002000001', '', '', '', '', '', '', ''
  ], [
    'Jun 20, 2026 15:36:20', 'Duplicate Customer', 'duplicate@example.test', '', '29', '9001000002', '9002000002', '', '', '', '', '', '', ''
  ]];

  const plan = buildPlan({
    newOrderRows: rows,
    conversionRows: [['Source New Order Row', 'Auth.Net Transaction ID']],
    activeRows: [['Subscription ID']],
    onboardingRows: [],
    auth: { pulledAtUtc: '2026-06-20T20:00:00Z', records: [
      approvedTx({
        transId: '9002000001',
        authAmount: '29.00',
        order: { invoiceNumber: '9001000001' },
        submitTimeUTC: '2026-06-18T13:28:25Z',
        customer: { email: 'duplicate@example.test' }
      }),
      approvedTx({
        transId: '9002000002',
        authAmount: '29.00',
        order: { invoiceNumber: '9001000002' },
        submitTimeUTC: '2026-06-20T19:36:20Z',
        customer: { email: 'duplicate@example.test' }
      })
    ], errors: [] },
    now: '2026-06-20T20:00:00Z'
  });

  assert.equal(plan.conversionUpserts.length, 1);
  assert.equal(plan.conversionUpserts[0].order.rowNumber, 2);
  assert.equal(plan.ready.length, 1);
  assert.equal(plan.rowUpdates.length, 2);
  assert.equal(plan.rowUpdates[1].rowNumber, 3);
  assert.equal(plan.rowUpdates[1].fields['Payment Status'], 'Review — possible duplicate order');
  assert.match(plan.rowUpdates[1].fields['Review Status'], /current FF Billing New Orders sync plan/);
  assert.match(plan.rowUpdates[1].fields['Review Status'], /row 2/);
});

test('existing ARB failed review rows are not retried automatically', () => {
  const rows = orderRows();
  rows[1][5] = '1467492646';
  rows[1][6] = '121662802867';
  rows[1][8] = 'Review — ARB failed';
  rows[1][9] = 'Review / Payment Issue';
  rows[1][11] = 'ARB failed: E00100 Customer profile creation failed. This transaction type does not support profile creation.';

  const plan = buildPlan({
    newOrderRows: rows,
    conversionRows: [['Source New Order Row', 'Auth.Net Transaction ID']],
    activeRows: [['Subscription ID']],
    onboardingRows: [],
    auth: { pulledAtUtc: '2026-06-08T22:40:00Z', records: [approvedTx({ transId: '121662802867', order: { invoiceNumber: '1467492646' } })], errors: [] },
    now: '2026-06-08T22:40:00Z'
  });

  assert.equal(plan.conversionUpserts.length, 0);
  assert.equal(plan.rowUpdates.length, 0);
  assert.equal(plan.skipped[0].reason, 'ARB failure already in human review; clear Payment Status to retry');
});
