const test = require('node:test');
const assert = require('node:assert/strict');

const {
  __paymentUpdateTestHooks: {
    hostedReturnUrl,
    hostedPaymentSettings,
    paymentUpdateSettings,
    paymentFlowForTicket
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
