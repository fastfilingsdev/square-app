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
const {
  processRefundLive,
  __billingRefundProcessTestHooks
} = require('../src/features/billingRefunds/refundProcess');
const {
  AuthNetApiError,
  buildRefundTransactionRequest,
  formatAuthNetErrorMessage,
  sanitizeAuthNetFailureDetail
} = require('../src/connectors/authnet/client');

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

test('lookup by subscription id includes Auth.Net subscription status in refund candidates', async () => {
  const sheets = fakeSheets({
    Refunds: [['Requested At', 'Lookup', 'Original Transaction ID']]
  });
  const result = await lookupRefundCandidates({
    lookup: '73319055',
    sheets,
    subscriptionsSpreadsheetId: '',
    getSubscriptionFn: async id => ({
      subscription: {
        id,
        status: 'active',
        transactions: [{ transId: '121662802867' }]
      }
    }),
    getTransactionDetailsFn: async id => ({ transaction: settledTx({ transId: id, subscription: { id: '73319055' } }) }),
    getTransactionListForCustomerFn: async () => ({ transactions: [] })
  });
  assert.equal(result.ok, true);
  assert.equal(result.candidates[0].subscriptionId, '73319055');
  assert.equal(result.candidates[0].subscriptionStatus, 'active');
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

test('Authorize.Net refund request uses original transaction, amount, bare last4, and suppresses customer email', () => {
  const payload = buildRefundTransactionRequest({
    refTransId: '121662802867',
    amount: '20',
    cardLast4: '1111',
    invoiceNumber: '1467834568',
    emailCustomer: false
  }, { name: 'login', transactionKey: 'key' });
  const tx = payload.createTransactionRequest.transactionRequest;
  assert.equal(tx.transactionType, 'refundTransaction');
  assert.equal(tx.amount, '20.00');
  assert.equal(tx.refTransId, '121662802867');
  assert.equal(tx.payment.creditCard.cardNumber, '1111');
  assert.equal(tx.payment.creditCard.expirationDate, 'XXXX');
  assert.deepEqual(tx.transactionSettings.setting, [{ settingName: 'emailCustomer', settingValue: 'false' }]);
});

test('Authorize.Net refund request can use customer profile payment profile instead of card details', () => {
  const payload = buildRefundTransactionRequest({
    refTransId: '121662802867',
    amount: '20',
    customerProfileId: '40338125',
    customerPaymentProfileId: '1000177237',
    invoiceNumber: '1467834568',
    emailCustomer: false
  }, { name: 'login', transactionKey: 'key' });
  const tx = payload.createTransactionRequest.transactionRequest;
  assert.equal(tx.transactionType, 'refundTransaction');
  assert.equal(tx.amount, '20.00');
  assert.equal(tx.refTransId, '121662802867');
  assert.deepEqual(tx.profile, {
    customerProfileId: '40338125',
    paymentProfile: { paymentProfileId: '1000177237' }
  });
  assert.equal(tx.payment, undefined);
  assert.deepEqual(tx.transactionSettings.setting, [{ settingName: 'emailCustomer', settingValue: 'false' }]);
});

test('Authorize.Net refund request can carry original required customer and bill-to fields', () => {
  const payload = buildRefundTransactionRequest({
    refTransId: '121662802867',
    amount: '20',
    cardLast4: '1111',
    invoiceNumber: '1467834568',
    customer: { id: 'CUST-1', email: 'customer@example.test' },
    billTo: {
      firstName: 'Test',
      lastName: 'Customer',
      address: '123 Main St',
      city: 'Miami',
      state: 'FL',
      zip: '33101',
      country: 'US',
      phoneNumber: '5555550100'
    },
    emailCustomer: false
  }, { name: 'login', transactionKey: 'key' });
  const tx = payload.createTransactionRequest.transactionRequest;
  assert.equal(tx.refTransId, '121662802867');
  assert.equal(tx.customer.email, 'customer@example.test');
  assert.equal(tx.billTo.firstName, 'Test');
  assert.equal(tx.billTo.address, '123 Main St');
});

test('Authorize.Net refund errors preserve transactionResponse details behind E00027', () => {
  const message = formatAuthNetErrorMessage({
    transactionResponse: {
      responseCode: '3',
      errors: {
        error: [{ errorCode: '54', errorText: 'The referenced transaction does not meet the criteria for issuing a credit.' }]
      }
    },
    messages: {
      resultCode: 'Error',
      message: [{ code: 'E00027', text: 'The transaction was unsuccessful.' }]
    }
  });
  assert.match(message, /E00027 The transaction was unsuccessful/);
  assert.match(message, /54 The referenced transaction does not meet the criteria/);
});

test('Authorize.Net failure sanitizer keeps nested transaction errors without sensitive raw fields', () => {
  const failure = sanitizeAuthNetFailureDetail({
    transactionResponse: {
      responseCode: '3',
      transId: '0',
      refTransID: '121657719835',
      accountNumber: 'XXXX0026',
      accountType: 'MasterCard',
      authCode: 'SECRET',
      transHashSha2: 'SECRET_HASH',
      errors: {
        error: [{ errorCode: '33', errorText: 'Country cannot be left blank.' }]
      }
    },
    messages: {
      resultCode: 'Error',
      message: [{ code: 'E00027', text: 'The transaction was unsuccessful.' }]
    }
  });
  assert.equal(failure.resultCode, 'Error');
  assert.equal(failure.transactionResponse.responseCode, '3');
  assert.equal(failure.transactionResponse.refTransId, '121657719835');
  assert.equal(failure.transactionResponse.accountNumberMasked, 'XXXX0026');
  assert.deepEqual(failure.transactionResponse.errors, [{ code: '33', text: 'Country cannot be left blank.' }]);
  assert.equal(JSON.stringify(failure).includes('SECRET'), false);
});

test('live refund process remains blocked when live gate is disabled', async () => {
  delete process.env.FF_BILLING_REFUNDS_LIVE_ENABLED;
  const result = await processRefundLive({
    lookup: 'customer@example.test',
    transactionId: '121662802867',
    refundType: 'FULL',
    reason: 'Duplicate',
    approvedBy: 'Gilmar Arellano',
    liveConfirm: 'PROCESS LIVE REFUND',
    sheets: fakeSheets({
      Refunds: [['Requested At', 'Lookup', 'Original Transaction ID']],
      'New Orders': [['Email', 'Auth.Net Transaction ID'], ['customer@example.test', '121662802867']]
    }),
    subscriptionsSpreadsheetId: '',
    getTransactionDetailsFn: async id => ({ transaction: settledTx({ transId: id }) }),
    getSubscriptionFn: async () => { throw new Error('not expected'); },
    getTransactionListForCustomerFn: async () => ({ transactions: [] }),
    refundTransactionFn: async () => { throw new Error('should not refund'); }
  });
  assert.equal(result.ok, false);
  assert.equal(result.status, 'LIVE REFUND DISABLED');
  assert.match(result.issues.join('; '), /live refund gate is disabled/);
});

test('live refund process can execute only after gate and confirmation', async () => {
  process.env.FF_BILLING_REFUNDS_LIVE_ENABLED = 'true';
  __billingRefundProcessTestHooks.recentRefunds.clear();
  let refundRequest;
  const result = await processRefundLive({
    lookup: 'customer@example.test',
    transactionId: '121662802867',
    refundType: 'PARTIAL',
    refundAmount: '10.00',
    reason: 'Duplicate',
    approvedBy: 'Gilmar Arellano',
    liveConfirm: 'PROCESS LIVE REFUND',
    sheets: fakeSheets({
      Refunds: [['Requested At', 'Lookup', 'Original Transaction ID']],
      'New Orders': [['Email', 'Auth.Net Transaction ID'], ['customer@example.test', '121662802867']]
    }),
    subscriptionsSpreadsheetId: '',
    getTransactionDetailsFn: async id => ({ transaction: settledTx({ transId: id }) }),
    getSubscriptionFn: async () => { throw new Error('not expected'); },
    getTransactionListForCustomerFn: async () => ({ transactions: [] }),
    refundTransactionFn: async request => {
      refundRequest = request;
      return { transactionResponse: { responseCode: '1', transId: '987654321', messages: { message: [{ code: '1', description: 'Approved' }] } } };
    }
  });
  assert.equal(result.ok, true);
  assert.equal(result.status, 'REFUNDED');
  assert.equal(result.refundTransactionId, '987654321');
  assert.equal(result.customerEmailSent, false);
  assert.equal(refundRequest.emailCustomer, false);
  assert.equal(refundRequest.refTransId, '121662802867');
  assert.equal(refundRequest.amount, '10.00');
  delete process.env.FF_BILLING_REFUNDS_LIVE_ENABLED;
  __billingRefundProcessTestHooks.recentRefunds.clear();
});

test('live refund process prefers original transaction card last4 over current subscription payment profile', async () => {
  process.env.FF_BILLING_REFUNDS_LIVE_ENABLED = 'true';
  __billingRefundProcessTestHooks.recentRefunds.clear();
  let refundRequest;
  const result = await processRefundLive({
    lookup: 'customer@example.test',
    transactionId: '121662802867',
    refundType: 'FULL',
    reason: 'Duplicate Charge',
    approvedBy: 'Gilmar Arellano',
    liveConfirm: 'PROCESS LIVE REFUND',
    sheets: fakeSheets({
      Refunds: [
        ['Lookup', 'Email', 'Subscription ID', 'Original Transaction ID'],
        ['customer@example.test', 'customer@example.test', '73319055', '121662802867']
      ]
    }),
    subscriptionsSpreadsheetId: '',
    getSubscriptionFn: async id => ({
      subscription: {
        id,
        status: 'canceled',
        profile: {
          customerProfileId: '40338125',
          paymentProfile: { customerPaymentProfileId: '1000177237' }
        }
      }
    }),
    getTransactionDetailsFn: async id => ({ transaction: settledTx({
      transId: id,
      subscription: { id: '73319055' },
      customer: { id: 'CUST-1', email: 'customer@example.test' },
      billTo: { firstName: 'Test', lastName: 'Customer', address: '123 Main St', city: 'Miami', state: 'FL', zip: '33101', country: 'US' }
    }) }),
    getTransactionListForCustomerFn: async () => ({ transactions: [] }),
    refundTransactionFn: async request => {
      refundRequest = request;
      return { transactionResponse: { responseCode: '1', transId: '987654321', messages: { message: [{ code: '1', description: 'Approved' }] } } };
    }
  });
  assert.equal(result.ok, true);
  assert.equal(result.refundTransactionId, '987654321');
  assert.equal(refundRequest.cardLast4, '1111');
  assert.equal(refundRequest.customerProfileId, '');
  assert.equal(refundRequest.customerPaymentProfileId, '');
  assert.deepEqual(refundRequest.customer, { id: 'CUST-1', email: 'customer@example.test' });
  assert.equal(refundRequest.billTo.address, '123 Main St');
  assert.equal(JSON.stringify(result).includes('40338125'), false);
  assert.equal(JSON.stringify(result).includes('1000177237'), false);
  assert.equal(JSON.stringify(result).includes('123 Main St'), false);
  delete process.env.FF_BILLING_REFUNDS_LIVE_ENABLED;
  __billingRefundProcessTestHooks.recentRefunds.clear();
});

test('live refund process returns structured blocked result when Auth.Net rejects refund', async () => {
  process.env.FF_BILLING_REFUNDS_LIVE_ENABLED = 'true';
  __billingRefundProcessTestHooks.recentRefunds.clear();
  const result = await processRefundLive({
    lookup: 'customer@example.test',
    transactionId: '121662802867',
    refundType: 'FULL',
    reason: 'Duplicate Charge',
    approvedBy: 'Gilmar Arellano',
    liveConfirm: 'PROCESS LIVE REFUND',
    sheets: fakeSheets({
      Refunds: [['Requested At', 'Lookup', 'Original Transaction ID']],
      'New Orders': [['Email', 'Auth.Net Transaction ID'], ['customer@example.test', '121662802867']]
    }),
    subscriptionsSpreadsheetId: '',
    getTransactionDetailsFn: async id => ({ transaction: settledTx({ transId: id }) }),
    getSubscriptionFn: async () => { throw new Error('not expected'); },
    getTransactionListForCustomerFn: async () => ({ transactions: [] }),
    refundTransactionFn: async () => {
      throw new Error('E00027 The transaction was unsuccessful.; 54 The referenced transaction does not meet the criteria for issuing a credit.');
    }
  });
  assert.equal(result.ok, false);
  assert.equal(result.status, 'BLOCKED / ERROR');
  assert.match(result.issues.join('; '), /E00027/);
  assert.match(result.issues.join('; '), /referenced transaction/);
  assert.equal(result.customerEmailSent, false);
  assert.equal(result.originalTransactionId, '121662802867');
  delete process.env.FF_BILLING_REFUNDS_LIVE_ENABLED;
  __billingRefundProcessTestHooks.recentRefunds.clear();
});

test('live refund process exposes sanitized Auth.Net transactionResponse detail when available', async () => {
  process.env.FF_BILLING_REFUNDS_LIVE_ENABLED = 'true';
  __billingRefundProcessTestHooks.recentRefunds.clear();
  const result = await processRefundLive({
    lookup: 'customer@example.test',
    transactionId: '121662802867',
    refundType: 'FULL',
    reason: 'Duplicate Charge',
    approvedBy: 'Gilmar Arellano',
    liveConfirm: 'PROCESS LIVE REFUND',
    sheets: fakeSheets({
      Refunds: [['Requested At', 'Lookup', 'Original Transaction ID']],
      'New Orders': [['Email', 'Auth.Net Transaction ID'], ['customer@example.test', '121662802867']]
    }),
    subscriptionsSpreadsheetId: '',
    getTransactionDetailsFn: async id => ({ transaction: settledTx({ transId: id }) }),
    getSubscriptionFn: async () => { throw new Error('not expected'); },
    getTransactionListForCustomerFn: async () => ({ transactions: [] }),
    refundTransactionFn: async () => {
      throw new AuthNetApiError('E00027 The transaction was unsuccessful.; 33 Country cannot be left blank.', {
        transactionResponse: {
          responseCode: '3',
          transId: '0',
          refTransID: '121662802867',
          accountNumber: 'XXXX1111',
          errors: { error: [{ errorCode: '33', errorText: 'Country cannot be left blank.' }] }
        },
        messages: {
          resultCode: 'Error',
          message: [{ code: 'E00027', text: 'The transaction was unsuccessful.' }]
        }
      });
    }
  });
  assert.equal(result.ok, false);
  assert.equal(result.status, 'BLOCKED / ERROR');
  assert.match(result.issues.join('; '), /transactionResponse\.responseCode=3/);
  assert.match(result.issues.join('; '), /33 Country cannot be left blank/);
  assert.equal(result.authNetFailure.transactionResponse.accountNumberMasked, 'XXXX1111');
  assert.equal(JSON.stringify(result).includes('SECRET'), false);
  delete process.env.FF_BILLING_REFUNDS_LIVE_ENABLED;
  __billingRefundProcessTestHooks.recentRefunds.clear();
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
