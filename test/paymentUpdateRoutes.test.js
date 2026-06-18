const test = require('node:test');
const assert = require('node:assert/strict');

const {
  __paymentUpdateTestHooks: {
    hostedReturnUrl,
    hostedPaymentSettings,
    paymentUpdateSettings,
    paymentFlowForTicket,
    loadPaymentUpdateRowContextForTicket
  }
} = require('../src/features/paymentUpdate/routes');

function mockReq() {
  return {
    protocol: 'https',
    get(name) {
      if (String(name).toLowerCase() === 'host') return 'fastfilings-api.test';
      return '';
    }
  };
}

function settingValue(settings, name) {
  const item = settings.find(s => s.settingName === name);
  assert.ok(item, `missing hosted setting ${name}`);
  return JSON.parse(item.settingValue);
}

test('recapture hosted-payment return URLs use path segments, not query strings', () => {
  const req = mockReq();
  const ticket = { ticketId: 'pu_live_abc123' };

  for (const flow of ['new-order', 'terminated']) {
    const url = hostedReturnUrl(req, ticket, flow);
    const parsed = new URL(url);
    assert.equal(parsed.origin, 'https://fastfilings-api.test');
    assert.equal(parsed.pathname, `/payment-update/return/${flow}/pu_live_abc123`);
    assert.equal(parsed.search, '', 'Authorize.Net Accept Hosted can blank the form when return URLs contain query strings');
    assert.equal(parsed.hash, '');
  }
});

test('hosted payment settings keep return and cancel URLs queryless for A/C recapture flows', () => {
  const req = mockReq();
  const ticket = { ticketId: 'pu_live_recapture' };

  for (const flow of ['new-order', 'terminated']) {
    const settings = hostedPaymentSettings(req, ticket, '29.00', flow);
    const returnOptions = settingValue(settings, 'hostedPaymentReturnOptions');
    for (const key of ['url', 'cancelUrl']) {
      const parsed = new URL(returnOptions[key]);
      assert.equal(parsed.pathname, `/payment-update/return/${flow}/pu_live_recapture`);
      assert.equal(parsed.search, '', `${key} must not contain query strings`);
      assert.equal(parsed.hash, '');
    }
  }
});

test('hosted profile payment-update return URL is also queryless', () => {
  const req = mockReq();
  const ticket = { ticketId: 'pu_live_update' };
  const settings = paymentUpdateSettings(req, ticket);
  const returnUrl = settings.find(s => s.settingName === 'hostedProfileReturnUrl').settingValue;
  const parsed = new URL(returnUrl);
  assert.equal(parsed.pathname, '/payment-update/return/payment-update/pu_live_update');
  assert.equal(parsed.search, '');
});

test('payment update type routing preserves A/B/C flow split', () => {
  assert.equal(paymentFlowForTicket({ paymentUpdateType: 'SUB RECAPTURE A - New Order' }), 'new-order');
  assert.equal(paymentFlowForTicket({ paymentUpdateType: 'SUB RECAPTURE B - Payment on Hold' }), 'payment-update');
  assert.equal(paymentFlowForTicket({ paymentUpdateType: 'SUB RECAPTURE C - Terminated' }), 'terminated');
});

test('payment update source context ignores stale stored row numbers and matches by ticket/subscription identity', async () => {
  const rows = [
    ['Payment Update Type', 'Customer ID', 'Name', 'Email', 'Subscription ID', 'Amount', 'Payment Update Link', 'Notes'],
    ['SUB RECAPTURE C - Terminated', 'AZ-7', 'Wrong Customer', 'wrong@example.test', '72249244', '20.00', 'https://fastfilings-api.test/payment-update/pu_live_wrong', 'wrong row'],
    ['SUB RECAPTURE C - Terminated', 'VA-3', 'Leonard Settles', 'lasone1@aol.com', '72221006', '29.00', 'https://fastfilings-api.test/payment-update/pu_live_va3', 'right row']
  ];
  const sheets = {
    spreadsheets: {
      values: {
        async get() {
          return { data: { values: rows } };
        }
      }
    }
  };

  const context = await loadPaymentUpdateRowContextForTicket(sheets, 'sheet-id', {
    ticketId: 'pu_live_va3',
    paymentUpdateRow: '2', // stale/wrong row after sheet shifts
    subscriptionId: '72221006'
  });

  assert.equal(context.customerId, 'VA-3');
  assert.equal(context.amount, '29.00');
  assert.equal(context.email, 'lasone1@aol.com');
});

test('payment update source context fails closed when a ticket cannot be matched to the current queue', async () => {
  const rows = [
    ['Payment Update Type', 'Customer ID', 'Name', 'Email', 'Subscription ID', 'Amount', 'Payment Update Link', 'Notes'],
    ['SUB RECAPTURE C - Terminated', 'AZ-7', 'Wrong Customer', 'wrong@example.test', '72249244', '20.00', 'https://fastfilings-api.test/payment-update/pu_live_wrong', 'wrong row']
  ];
  const sheets = {
    spreadsheets: {
      values: {
        async get() {
          return { data: { values: rows } };
        }
      }
    }
  };

  await assert.rejects(
    () => loadPaymentUpdateRowContextForTicket(sheets, 'sheet-id', {
      ticketId: 'pu_live_va3',
      paymentUpdateRow: '2',
      subscriptionId: '72221006'
    }),
    /identity guard could not match/
  );
});
