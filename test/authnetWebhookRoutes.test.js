const test = require('node:test');
const assert = require('node:assert/strict');

const { buildCustomerPaymentProfileChargeRequest } = require('../src/connectors/authnet/client');
const {
  __authNetWebhookTestHooks: {
    buildEventSnapshot,
    buildWebhookLogRow,
    computeAuthNetSignature,
    normalizeHexKey,
    verifyAuthNetSignature
  }
} = require('../src/features/authnetWebhook/routes');
const {
  processBProfileUpdatedWebhook,
  __bRecoveryTestHooks: {
    bInvoiceNumber,
    chargeSummary,
    chooseHoldRow,
    extractProfileIds,
    findMatchingBRow,
    isPaymentProfileUpdatedEvent,
    isReadyBRow,
    parseAmount,
    RECOVERED_HEADERS
  }
} = require('../src/features/authnetWebhook/paymentUpdateBRecovery');

const key = 'A'.repeat(128);

test('Authorize.Net webhook signature verification accepts SHA512 header', () => {
  const rawBody = JSON.stringify({ eventType: 'net.authorize.payment.authcapture.created', payload: { id: '123' } });
  const signature = computeAuthNetSignature(rawBody, key);
  const result = verifyAuthNetSignature({ rawBody, signatureHeader: `SHA512=${signature}`, signatureKeyHex: key });
  assert.deepEqual(result, { ok: true, status: 'signature-valid', httpStatus: 200 });
});

test('Authorize.Net webhook signature verification rejects tampered payloads', () => {
  const rawBody = JSON.stringify({ eventType: 'net.authorize.payment.authcapture.created', payload: { id: '123' } });
  const signature = computeAuthNetSignature(rawBody, key);
  const result = verifyAuthNetSignature({ rawBody: rawBody.replace('123', '456'), signatureHeader: `SHA512=${signature}`, signatureKeyHex: key });
  assert.equal(result.ok, false);
  assert.equal(result.status, 'signature-invalid');
  assert.equal(result.httpStatus, 401);
});

test('Authorize.Net signature key normalization strips prefixes and separators', () => {
  assert.equal(normalizeHexKey('SHA512=aa bb-cc'), 'aabbcc');
});

test('webhook log row extracts support identifiers without card/payment data', () => {
  const rawBody = JSON.stringify({
    webhookId: 'wh_1',
    eventType: 'net.authorize.customer.subscription.suspended',
    payload: {
      id: 'txn_1',
      subscription: { id: 'sub_1' },
      order: { invoiceNumber: 'INV-1' },
      customerProfileId: 'cust_1',
      customerPaymentProfileId: 'payprof_1'
    }
  });
  const row = buildWebhookLogRow({ body: JSON.parse(rawBody), rawBody, signatureStatus: 'signature-valid', receivedAt: new Date('2026-05-28T00:00:00Z') });
  assert.equal(row[0], '2026-05-28T00:00:00.000Z');
  assert.equal(row[2], 'wh_1');
  assert.equal(row[3], 'net.authorize.customer.subscription.suspended');
  assert.equal(row[4], 'txn_1');
  assert.equal(row[5], 'sub_1');
  assert.equal(row[6], 'INV-1');
  assert.equal(row[7], 'cust_1');
  assert.equal(row[8], 'payprof_1');
  assert.equal(row[9], 'signature-valid');
});


test('event snapshot omits raw card/account/token-like fields', () => {
  const snapshot = buildEventSnapshot({
    webhookId: 'wh_2',
    eventType: 'net.authorize.payment.fraud.declined',
    payload: {
      id: 'txn_2',
      accountNumber: 'XXXX1111',
      cardNumber: '4111111111111111',
      token: 'secret-token',
      authAmount: '29.00',
      subscription: { id: 'sub_2' }
    }
  });
  const serialized = JSON.stringify(snapshot);
  assert.equal(serialized.includes('4111111111111111'), false);
  assert.equal(serialized.includes('secret-token'), false);
  assert.equal(serialized.includes('XXXX1111'), false);
  assert.equal(snapshot.payload.id, 'txn_2');
  assert.equal(snapshot.payload.authAmount, '29.00');
  assert.equal(snapshot.payload.subscriptionId, 'sub_2');
});

test('B webhook gate recognizes Authorize.Net payment profile update events only', () => {
  assert.equal(isPaymentProfileUpdatedEvent('net.authorize.customer.paymentProfile.updated'), true);
  assert.equal(isPaymentProfileUpdatedEvent('net.authorize.payment.authcapture.created'), false);
});

test('B webhook gate extracts profile ids from sanitized Authorize.Net payloads', () => {
  const ids = extractProfileIds({
    eventType: 'net.authorize.customer.paymentProfile.updated',
    payload: {
      customerProfileId: 'cust_123',
      customerPaymentProfileId: 'pay_456'
    }
  });
  assert.deepEqual(ids, { customerProfileId: 'cust_123', customerPaymentProfileId: 'pay_456' });
});

test('B recovery only treats unresolved ready B rows as eligible', () => {
  assert.equal(isReadyBRow({
    'Payment Update Type': 'SUB RECAPTURE B - Payment on Hold',
    'Payment Update Status': 'Live Link Ready',
    'Stop / Suppressed': '',
    'Subscription ID': '71669864'
  }), true);
  assert.equal(isReadyBRow({
    'Payment Update Type': 'SUB RECAPTURE B - Payment on Hold',
    'Payment Update Status': 'Live Link Ready',
    'Stop / Suppressed': 'Resolved',
    'Subscription ID': '71669864'
  }), false);
  assert.equal(isReadyBRow({
    'Payment Update Type': 'SUB RECAPTURE C - Terminated',
    'Payment Update Status': 'Live Link Ready',
    'Stop / Suppressed': '',
    'Subscription ID': '71669864'
  }), false);
});

test('B recovery matches a profile-updated webhook to the subscription profile currently in Auth.Net', async () => {
  const paymentUpdateRows = [{
    _rowNumber: 131,
    'Payment Update Type': 'SUB RECAPTURE B - Payment on Hold',
    'Payment Update Status': 'Live Link Ready',
    'Stop / Suppressed': '',
    'Subscription ID': '71669864'
  }];
  const match = await findMatchingBRow({
    paymentUpdateRows,
    profileIds: { customerProfileId: 'cust_abc', customerPaymentProfileId: 'pay_xyz' },
    getSubscriptionFn: async subscriptionId => ({
      subscription: {
        status: 'active',
        amount: '20.00',
        profile: {
          customerProfileId: 'cust_abc',
          paymentProfile: { customerPaymentProfileId: 'pay_xyz' }
        }
      },
      subscriptionId
    })
  });
  assert.equal(match.row._rowNumber, 131);
});

test('B catch-up guards parse amount, choose latest unresolved hold row, and summarize charges safely', () => {
  assert.equal(parseAmount('$20.00'), '20.00');
  assert.equal(parseAmount(''), '');
  assert.equal(chooseHoldRow([
    { _rowNumber: 20, 'Subscription ID': 'sub_1', 'Stop / Suppressed': 'Resolved' },
    { _rowNumber: 24, 'Subscription ID': 'sub_1', 'Stop / Suppressed': '' }
  ], 'sub_1')._rowNumber, 24);
  assert.equal(bInvoiceNumber('71669864', new Date('2026-06-01T20:00:00Z')), 'BUPD-71669864-0601');
  assert.deepEqual(chargeSummary({ transactionResponse: { responseCode: '1', transId: 'txn_1', authCode: 'ok' } }).approved, true);
});

test('Authorize.Net profile charge payload suppresses customer email by default', () => {
  const payload = buildCustomerPaymentProfileChargeRequest({
    customerProfileId: 'cust_abc',
    customerPaymentProfileId: 'pay_xyz',
    amount: '20.00',
    invoiceNumber: 'BUPD-71669864-0604',
    customerEmail: 'customer@example.test',
    emailCustomer: false,
    refId: 'b-catchup-71669864'
  }, { name: 'login', transactionKey: 'key' });
  const tx = payload.createTransactionRequest.transactionRequest;
  assert.equal(tx.customer, undefined);
  assert.deepEqual(tx.transactionSettings.setting, [{ settingName: 'emailCustomer', settingValue: 'false' }]);
});

test('B webhook safe mode suppresses follow-ups without charging', async () => {
  const updates = [];
  const appends = [];
  const tableValues = {
    'Payment Update': [[
      'Payment Update Type', 'Payment Update Status', 'Stop / Suppressed', 'Subscription ID',
      'Customer ID', 'Name', 'Email', 'Amount Due', 'Next Follow-Up Due', 'Stop Reason', 'Notes',
      'Source Tab', 'Source Row', 'Payment Update Link'
    ], [
      'SUB RECAPTURE B - Payment on Hold', 'Live Link Ready', '', '71669864',
      'FL-58', 'Test Customer', 'customer@example.test', '', '2026-06-05', '', '',
      'Payment on Hold', '24', 'https://fastfilings-api.onrender.com/payment-update/ticket_1'
    ]],
    'Payment on Hold': [[
      'Subscription ID', 'Customer ID', 'Name', 'Email', 'Amount Due', 'Subscription Status',
      'Last Charge Status', 'Last AuthNet Check At', 'Next Follow-Up Due', 'Stop / Suppressed', 'Stop Reason', 'Notes'
    ], [
      '71669864', 'FL-58', 'Test Customer', 'customer@example.test', '20', 'active',
      'This transaction has been declined.', '', '2026-06-05', '', '', ''
    ]],
    'Payment Update Link Tickets': [[
      'Ticket ID', 'Subscription ID', 'Ticket Status', 'Completed At', 'Notes'
    ], [
      'ticket_1', '71669864', 'Live Link Ready', '', ''
    ]],
    'AuthNet_Transactions': [['Invoice Number']],
    'Active Subscriptions': [['Subscription ID']],
    'Stop_Work_Feed': [['Subscription ID']]
  };
  const fakeSheets = {
    spreadsheets: {
      values: {
        get: async ({ range }) => {
          const match = String(range).match(/^'([^']+)'!/);
          return { data: { values: tableValues[match ? match[1] : ''] || [] } };
        },
        update: async ({ range, requestBody }) => {
          updates.push({ range, values: requestBody.values[0] });
          return { data: {} };
        },
        append: async ({ range, requestBody }) => {
          appends.push({ range, values: requestBody.values[0] });
          return { data: {} };
        }
      }
    }
  };

  let chargeCalled = false;
  const result = await processBProfileUpdatedWebhook({
    body: {
      eventType: 'net.authorize.customer.paymentProfile.updated',
      payload: { customerProfileId: 'cust_abc', customerPaymentProfileId: 'pay_xyz' }
    },
    sheets: fakeSheets,
    spreadsheetId: 'sheet_1',
    detectEnabled: true,
    chargeEnabled: false,
    getSubscriptionFn: async () => ({
      subscription: {
        status: 'active',
        amount: '20.00',
        profile: {
          customerProfileId: 'cust_abc',
          paymentProfile: { customerPaymentProfileId: 'pay_xyz' }
        }
      }
    }),
    chargeCustomerPaymentProfileFn: async () => {
      chargeCalled = true;
      throw new Error('charge should not be called in safe mode');
    },
    date: new Date('2026-06-04T18:00:00Z')
  });

  assert.equal(result.status, 'pending_approval');
  assert.equal(result.authnetChargeAttempted, false);
  assert.equal(result.customerEmails, false);
  assert.equal(chargeCalled, false);
  assert.equal(updates.length, 3);
  assert.equal(appends.length, 1);
  const paymentUpdate = updates.find(item => item.range.startsWith("'Payment Update'!"));
  assert.ok(paymentUpdate.values.includes('Card appears updated — payment still pending verification'));
  assert.ok(paymentUpdate.values.includes('Suppressed'));
  assert.ok(paymentUpdate.values.includes(''));

  const ticketUpdate = updates.find(item => item.range.startsWith("'Payment Update Link Tickets'!"));
  assert.ok(ticketUpdate.values.includes('Card updated — catch-up pending'));
});

test('B webhook safe mode is idempotent for already-pending suppressed rows', async () => {
  const updates = [];
  const appends = [];
  const tableValues = {
    'Payment Update': [[
      'Payment Update Type', 'Payment Update Status', 'Stop / Suppressed', 'Subscription ID',
      'Customer ID', 'Name', 'Email', 'Amount Due', 'Next Follow-Up Due', 'Stop Reason', 'Notes',
      'Source Tab', 'Source Row', 'Payment Update Link'
    ], [
      'SUB RECAPTURE B - Payment on Hold', 'Card appears updated — payment still pending verification', 'Suppressed', '71669864',
      'FL-58', 'Test Customer', 'customer@example.test', '', '', 'already handled', 'prior note',
      'Payment on Hold', '24', 'https://fastfilings-api.onrender.com/payment-update/ticket_1'
    ]],
    'Payment on Hold': [['Subscription ID', 'Amount Due'], ['71669864', '20']],
    'Payment Update Link Tickets': [['Ticket ID', 'Subscription ID', 'Ticket Status']],
    'AuthNet_Transactions': [['Invoice Number']],
    'Active Subscriptions': [['Subscription ID']],
    'Stop_Work_Feed': [['Subscription ID']]
  };
  const fakeSheets = {
    spreadsheets: {
      values: {
        get: async ({ range }) => {
          const match = String(range).match(/^'([^']+)'!/);
          return { data: { values: tableValues[match ? match[1] : ''] || [] } };
        },
        update: async ({ range, requestBody }) => {
          updates.push({ range, values: requestBody.values[0] });
          return { data: {} };
        },
        append: async ({ range, requestBody }) => {
          appends.push({ range, values: requestBody.values[0] });
          return { data: {} };
        }
      }
    }
  };

  const result = await processBProfileUpdatedWebhook({
    body: {
      eventType: 'net.authorize.customer.paymentProfile.updated',
      payload: { customerProfileId: 'cust_abc', customerPaymentProfileId: 'pay_xyz' }
    },
    sheets: fakeSheets,
    spreadsheetId: 'sheet_1',
    detectEnabled: true,
    chargeEnabled: false,
    getSubscriptionFn: async () => ({
      subscription: {
        status: 'active',
        amount: '20.00',
        profile: {
          customerProfileId: 'cust_abc',
          paymentProfile: { customerPaymentProfileId: 'pay_xyz' }
        }
      }
    })
  });

  assert.equal(result.status, 'skipped');
  assert.equal(result.reason, 'already-pending-approval');
  assert.equal(updates.length, 0);
  assert.equal(appends.length, 0);
});

test('B webhook charge mode charges once without Auth.Net customer email and writes Recovered Subs first', async () => {
  const updates = [];
  const appends = [];
  const tableValues = {
    'Payment Update': [[
      'Payment Update Type', 'Payment Update Status', 'Stop / Suppressed', 'Subscription ID',
      'Customer ID', 'Name', 'Email', 'Alt Email', 'Amount Due', 'Next Follow-Up Due', 'Stop Reason', 'Notes',
      'Source Tab', 'Source Row', 'Payment Update Link'
    ], [
      'SUB RECAPTURE B - Payment on Hold', 'Live Link Ready', '', '71669864',
      'FL-58', 'Test Customer', 'customer@example.test', 'alt@example.test', '', '2026-06-05', '', '',
      'Payment on Hold', '24', 'https://fastfilings-api.onrender.com/payment-update/ticket_1'
    ]],
    'Payment on Hold': [[
      'Subscription ID', 'Customer ID', 'Name', 'Email', 'Alt Email', 'Amount Due', 'Subscription Status',
      'Last Charge Status', 'Last Charge Transaction ID', 'Last AuthNet Check At', 'Next Follow-Up Due', 'Stop / Suppressed', 'Stop Reason', 'Notes'
    ], [
      '71669864', 'FL-58', 'Test Customer', 'customer@example.test', 'alt@example.test', '20', 'active',
      'declined', '', '', '2026-06-05', '', '', ''
    ]],
    'Payment Update Link Tickets': [[
      'Ticket ID', 'Subscription ID', 'Ticket Status', 'Completed At', 'Notes'
    ], [
      'ticket_1', '71669864', 'Live Link Ready', '', ''
    ]],
    'AuthNet_Transactions': [['Invoice Number']],
    'Recovered Subs': [RECOVERED_HEADERS],
    'Stop_Work_Feed': [['Subscription ID', 'Customer ID', 'Stop Work?', 'Reason', 'Pushed To Sheets?', 'Pushed At', 'Notes'], ['71669864', 'FL-58', 'TRUE', 'hold', 'TRUE', '2026-06-01', '']]
  };
  const fakeSheets = {
    spreadsheets: {
      values: {
        get: async ({ range }) => {
          const match = String(range).match(/^'([^']+)'!/);
          return { data: { values: tableValues[match ? match[1] : ''] || [] } };
        },
        update: async ({ range, requestBody }) => {
          updates.push({ range, values: requestBody.values[0] });
          return { data: {} };
        },
        append: async ({ range, requestBody }) => {
          appends.push({ range, values: requestBody.values[0] });
          return { data: {} };
        }
      }
    }
  };

  let chargeArgs;
  const result = await processBProfileUpdatedWebhook({
    body: {
      eventType: 'net.authorize.customer.paymentProfile.updated',
      payload: { customerProfileId: 'cust_abc', customerPaymentProfileId: 'pay_xyz' }
    },
    sheets: fakeSheets,
    spreadsheetId: 'sheet_1',
    detectEnabled: true,
    chargeEnabled: true,
    getSubscriptionFn: async () => ({
      subscription: {
        status: 'active',
        amount: '20.00',
        profile: {
          customerProfileId: 'cust_abc',
          paymentProfile: { customerPaymentProfileId: 'pay_xyz' }
        }
      }
    }),
    chargeCustomerPaymentProfileFn: async args => {
      chargeArgs = args;
      return { transactionResponse: { responseCode: '1', transId: 'txn_1', authCode: 'auth_ok', messages: [{ description: 'Approved' }] } };
    },
    date: new Date('2026-06-04T18:00:00Z')
  });

  assert.equal(result.status, 'charged');
  assert.equal(result.recoveredSubsWritten, true);
  assert.equal(result.authnetCustomerEmailSuppressed, true);
  assert.equal(chargeArgs.emailCustomer, false);
  assert.equal(Object.prototype.hasOwnProperty.call(chargeArgs, 'customerEmail'), false);
  assert.equal(chargeArgs.invoiceNumber, 'BUPD-71669864-0604');

  const appendRanges = appends.map(item => item.range);
  assert.equal(appendRanges.some(range => range.startsWith("'Active Subscriptions'!")), false);
  assert.equal(appendRanges.some(range => range.startsWith("'AuthNet_Transactions'!")), true);
  assert.equal(appendRanges.some(range => range.startsWith("'Recovered Subs'!")), true);
  assert.equal(appendRanges.some(range => range.startsWith("'Payment Update Email Log'!")), true);

  const recovered = appends.find(item => item.range.startsWith("'Recovered Subs'!"));
  assert.equal(recovered.values[RECOVERED_HEADERS.indexOf('Recovery Type')], 'SUB RECAPTURE B - Payment on Hold');
  assert.equal(recovered.values[RECOVERED_HEADERS.indexOf('Active Sync Status')], 'Pending Active history sync');
  assert.equal(recovered.values[RECOVERED_HEADERS.indexOf('Existing ARB Action')], 'kept active; no replacement/cancel');
  assert.equal(recovered.values[RECOVERED_HEADERS.indexOf('Active Subscription Row')], '');

  const paymentUpdate = updates.find(item => item.range.startsWith("'Payment Update'!"));
  assert.ok(paymentUpdate.values.includes('Completed — B catch-up charged / existing ARB active'));
  assert.ok(paymentUpdate.values.includes('Resolved'));
});

test('B webhook charge mode refuses duplicate charge when Recovered Subs already has B ledger', async () => {
  const updates = [];
  const appends = [];
  const recoveredPrior = RECOVERED_HEADERS.map(header => ({
    'Recovery Type': 'SUB RECAPTURE B - Payment on Hold',
    'Subscription ID': '71669864',
    'Recovered Payment Invoice': 'BUPD-71669864-0604'
  })[header] || '');
  const tableValues = {
    'Payment Update': [[
      'Payment Update Type', 'Payment Update Status', 'Stop / Suppressed', 'Subscription ID', 'Customer ID', 'Name', 'Email', 'Payment Update Link'
    ], [
      'SUB RECAPTURE B - Payment on Hold', 'Live Link Ready', '', '71669864', 'FL-58', 'Test Customer', 'customer@example.test', 'https://fastfilings-api.onrender.com/payment-update/ticket_1'
    ]],
    'Payment on Hold': [['Subscription ID', 'Amount Due'], ['71669864', '20']],
    'Payment Update Link Tickets': [['Ticket ID', 'Subscription ID', 'Ticket Status']],
    'AuthNet_Transactions': [['Invoice Number']],
    'Recovered Subs': [RECOVERED_HEADERS, recoveredPrior],
    'Stop_Work_Feed': [['Subscription ID']]
  };
  const fakeSheets = {
    spreadsheets: {
      values: {
        get: async ({ range }) => {
          const match = String(range).match(/^'([^']+)'!/);
          return { data: { values: tableValues[match ? match[1] : ''] || [] } };
        },
        update: async ({ range, requestBody }) => {
          updates.push({ range, values: requestBody.values[0] });
          return { data: {} };
        },
        append: async ({ range, requestBody }) => {
          appends.push({ range, values: requestBody.values[0] });
          return { data: {} };
        }
      }
    }
  };

  let chargeCalled = false;
  const result = await processBProfileUpdatedWebhook({
    body: {
      eventType: 'net.authorize.customer.paymentProfile.updated',
      payload: { customerProfileId: 'cust_abc', customerPaymentProfileId: 'pay_xyz' }
    },
    sheets: fakeSheets,
    spreadsheetId: 'sheet_1',
    detectEnabled: true,
    chargeEnabled: true,
    getSubscriptionFn: async () => ({
      subscription: {
        status: 'active',
        amount: '20.00',
        profile: {
          customerProfileId: 'cust_abc',
          paymentProfile: { customerPaymentProfileId: 'pay_xyz' }
        }
      }
    }),
    chargeCustomerPaymentProfileFn: async () => {
      chargeCalled = true;
      throw new Error('duplicate charge should not be attempted');
    },
    date: new Date('2026-06-04T18:00:00Z')
  });

  assert.equal(result.status, 'skipped');
  assert.equal(result.reason, 'recovered-ledger-already-recorded');
  assert.equal(chargeCalled, false);
  assert.equal(updates.length, 0);
  assert.equal(appends.length, 0);
});
