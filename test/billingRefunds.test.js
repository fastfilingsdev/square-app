const test = require('node:test');
const assert = require('node:assert/strict');

const {
  buildRefundDryRun,
  lookupRefundCandidates,
  __billingRefundsTestHooks: {
    sanitizeDetailForCandidate,
    tableFromValues
  }
} = require('../src/features/billingRefunds/refundLookup');

function fakeSheets(ranges) {
  return {
    spreadsheets: {
      values: {
        get: async ({ range }) => {
          const key = Object.keys(ranges).find(name => range.startsWith(`'${name}'!`));
          if (!key) return { data: { values: [] } };
          return { data: { values: ranges[key] } };
        }
      }
    }
  };
}

function settledTx(overrides = {}) {
  return {
    transId: '121662802867',
    transactionType: 'authCaptureTransaction',
    transactionStatus: 'settledSuccessfully',
    settleAmount: '29.00',
    submitTimeUTC: '2026-06-05T16:40:12Z',
    customer: { email: 'customer@example.test' },
    billTo: { firstName: 'Test', lastName: 'Customer' },
    order: { invoiceNumber: '1467834568' },
    payment: { creditCard: { cardNumber: 'XXXX1111' } },
    ...overrides
  };
}

test('refund candidate marks settled Auth.Net transaction refundable and exposes only last4', () => {
  const candidate = sanitizeDetailForCandidate(settledTx(), { tables: [], candidateNumber: 1 });
  assert.equal(candidate.refundable, true);
  assert.equal(candidate.transactionId, '121662802867');
  assert.equal(candidate.originalAmount, '29.00');
  assert.equal(candidate.refundableAmount, '29.00');
  assert.equal(candidate.cardLast4, '1111');
});

test('refund candidate blocks unsettled or refund transactions', () => {
  const unsettled = sanitizeDetailForCandidate(settledTx({ transactionStatus: 'capturedPendingSettlement' }), { tables: [], candidateNumber: 1 });
  assert.equal(unsettled.refundable, false);
  assert.match(unsettled.blockReason, /not settled/);

  const refund = sanitizeDetailForCandidate(settledTx({ transactionType: 'refundTransaction' }), { tables: [], candidateNumber: 1 });
  assert.equal(refund.refundable, false);
  assert.match(refund.blockReason, /refund\/void/);
});

test('sheet refund ledger reduces refundable balance', () => {
  const refunds = tableFromValues([
    ['Original Transaction ID', 'Refund Amount', 'Refund Status', 'Refund Transaction ID'],
    ['121662802867', '9.00', 'REFUNDED', '999']
  ]);
  const candidate = sanitizeDetailForCandidate(settledTx(), {
    tables: [{ workbook: 'FF Billing', tab: 'Refunds', ...refunds }],
    candidateNumber: 1
  });
  assert.equal(candidate.alreadyRefunded, '9.00');
  assert.equal(candidate.refundableAmount, '20.00');
});

test('lookup by email uses sheet references to fetch transaction details', async () => {
  const sheets = fakeSheets({
    Refunds: [['Requested At', 'Lookup', 'Original Transaction ID']],
    'New Orders': [['Time', 'Name', 'Email', 'Order / Invoice #', 'Auth.Net Transaction ID'], ['2026-06-05', 'Test Customer', 'customer@example.test', '1467834568', '121662802867']]
  });
  const result = await lookupRefundCandidates({
    lookup: 'customer@example.test',
    sheets,
    subscriptionsSpreadsheetId: '',
    getTransactionDetailsFn: async id => ({ transaction: settledTx({ transId: id }) }),
    getSubscriptionFn: async () => { throw new Error('not expected'); },
    getTransactionListForCustomerFn: async () => ({ transactions: [] })
  });
  assert.equal(result.ok, true);
  assert.equal(result.counts.candidates, 1);
  assert.equal(result.candidates[0].refundable, true);
});

test('dry-run validates full and partial refund amounts without live processing', async () => {
  const sheets = fakeSheets({
    Refunds: [['Requested At', 'Lookup', 'Original Transaction ID']],
    'New Orders': [['Email', 'Auth.Net Transaction ID'], ['customer@example.test', '121662802867']]
  });
  const result = await buildRefundDryRun({
    lookup: 'customer@example.test',
    transactionId: '121662802867',
    refundType: 'PARTIAL',
    refundAmount: '10',
    reason: 'Customer request',
    sheets,
    subscriptionsSpreadsheetId: '',
    getTransactionDetailsFn: async id => ({ transaction: settledTx({ transId: id }) }),
    getSubscriptionFn: async () => { throw new Error('not expected'); },
    getTransactionListForCustomerFn: async () => ({ transactions: [] })
  });
  assert.equal(result.ok, true);
  assert.equal(result.status, 'DRY-RUN OK');
  assert.equal(result.refundAmount, '10.00');
  assert.equal(result.liveRefundsEnabled, false);
});

test('dry-run blocks partial refund greater than refundable balance', async () => {
  const sheets = fakeSheets({
    Refunds: [['Requested At', 'Lookup', 'Original Transaction ID']],
    'New Orders': [['Email', 'Auth.Net Transaction ID'], ['customer@example.test', '121662802867']]
  });
  const result = await buildRefundDryRun({
    lookup: 'customer@example.test',
    transactionId: '121662802867',
    refundType: 'PARTIAL',
    refundAmount: '30',
    sheets,
    subscriptionsSpreadsheetId: '',
    getTransactionDetailsFn: async id => ({ transaction: settledTx({ transId: id }) }),
    getSubscriptionFn: async () => { throw new Error('not expected'); },
    getTransactionListForCustomerFn: async () => ({ transactions: [] })
  });
  assert.equal(result.ok, false);
  assert.equal(result.status, 'BLOCKED / ERROR');
  assert.match(result.issues.join('; '), /exceeds refundable balance/);
});


const {
  allowedRefundGoogleEmails,
  hasValidBillingAccess
} = require('../src/features/billingRefunds/routes');

test('refund routes default to Fast Filings Google OAuth allowlist', () => {
  const allowed = allowedRefundGoogleEmails();
  assert.equal(allowed.has('returns@fastfilings.com'), true);
  assert.equal(allowed.has('returns1@fastfilings.com'), true);
});

test('refund route access accepts verified allowed Google OAuth identity', async () => {
  const req = {
    get(name) {
      if (String(name).toLowerCase() === 'authorization') return 'Bearer google-token';
      return '';
    }
  };
  const ok = await hasValidBillingAccess(req, {
    verifyGoogleAccessTokenFn: async token => ({ ok: token === 'google-token', email: 'returns1@fastfilings.com' })
  });
  assert.equal(ok, true);
});
