const test = require('node:test');
const assert = require('node:assert/strict');

const {
  authorizationTextFor,
  buildHostedPaymentTransactionRequest,
  hostedPaymentSettings,
  parseLineItemsText,
  parsePaymentLinkItems,
  paymentLinkReturnUrl,
  totalLineItems,
  validatePaymentLink
} = require('../src/features/billingPaymentLinks/paymentLinks');

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

test('payment link line items support multiple items in one link', () => {
  const items = parseLineItemsText([
    'Sales certificate cancellation assistance | 80 | 1',
    'Past period filing - FL Q1 2025 | $20.00 | 2',
    'Past period filing - FL Q2 2025 - $20 x 1'
  ].join('\n'));

  assert.equal(items.length, 3);
  assert.deepEqual(items[0], { name: 'Sales certificate cancellation assistance', amount: '80.00', quantity: 1 });
  assert.deepEqual(items[1], { name: 'Past period filing - FL Q1 2025', amount: '20.00', quantity: 2 });
  assert.deepEqual(items[2], { name: 'Past period filing - FL Q2 2025', amount: '20.00', quantity: 1 });
  assert.equal(totalLineItems(items), '140.00');
});

test('sales certificate cancellation payment links carry explicit authorization verbiage', () => {
  const rowObj = {
    'Link Type': 'Sales Certificate Cancellation',
    'Business Name': 'Acme Supply LLC',
    State: 'FL',
    Amount: '80',
    Purpose: 'Cancel FL sales tax certificate'
  };
  const items = parsePaymentLinkItems(rowObj);
  const text = authorizationTextFor(rowObj, items);

  assert.match(text, /authorize Fast Filings/i);
  assert.match(text, /cancel\/close the sales tax certificate\/account/i);
  assert.match(text, /Acme Supply LLC/);
  assert.match(text, /FL/);
  assert.match(text, /\$80/);
});

test('hosted payment settings use path return URLs without query strings', () => {
  const req = mockReq();
  const link = { linkId: 'ffpl_live_20260701_abc123', rowObj: {}, items: [{ name: 'Past filing', amount: '20.00', quantity: 1 }] };
  const settings = hostedPaymentSettings(req, link, '20.00');
  const returnOptions = settingValue(settings, 'hostedPaymentReturnOptions');

  for (const key of ['url', 'cancelUrl']) {
    const parsed = new URL(returnOptions[key]);
    assert.equal(parsed.origin, 'https://fastfilings-api.test');
    assert.equal(parsed.pathname, '/billing/payment-links/return/ffpl_live_20260701_abc123');
    assert.equal(parsed.search, '');
  }
  assert.equal(paymentLinkReturnUrl(req, link.linkId), 'https://fastfilings-api.test/billing/payment-links/return/ffpl_live_20260701_abc123');
});

test('Authorize.Net transaction request includes line items, invoice, email, customer id, and user fields', () => {
  const link = {
    linkId: 'ffpl_live_20260701_abc123',
    rowObj: {
      'Link Type': 'Multiple Items',
      'Customer ID': 'FL-9',
      'Business Name': 'Acme Supply LLC',
      Name: 'Ada Lovelace',
      Email: 'ada@example.test',
      Purpose: 'Past filings plus certificate cancellation'
    },
    items: [
      { name: 'Sales certificate cancellation assistance', amount: '80.00', quantity: 1 },
      { name: 'Past filing - Q1 2025', amount: '20.00', quantity: 2 }
    ]
  };

  const tx = buildHostedPaymentTransactionRequest(link);
  assert.equal(tx.transactionType, 'authCaptureTransaction');
  assert.equal(tx.amount, '120.00');
  assert.equal(tx.order.description, 'Past filings plus certificate cancellation');
  assert.equal(tx.customer.id, 'FL-9');
  assert.equal(tx.customer.email, 'ada@example.test');
  assert.equal(tx.billTo.firstName, 'Ada');
  assert.equal(tx.billTo.lastName, 'Lovelace');
  assert.equal(tx.billTo.company, 'Acme Supply LLC');
  assert.equal(tx.lineItems.lineItem.length, 2);
  assert.equal(tx.lineItems.lineItem[1].quantity, '2');
  assert.equal(tx.lineItems.lineItem[1].unitPrice, '20.00');
  assert.deepEqual(tx.userFields.userField.find(field => field.name === 'ffPaymentLinkId'), { name: 'ffPaymentLinkId', value: 'ffpl_live_20260701_abc123' });
});

test('live route validation blocks non-live, completed, expired, and duplicate-unsafe states', () => {
  const base = {
    linkId: 'ffpl_live_20260701_abc123',
    rowObj: { Status: 'Link Ready' },
    items: [{ name: 'Past filing', amount: '20.00', quantity: 1 }]
  };

  assert.equal(validatePaymentLink(base).linkId, base.linkId);
  assert.throws(() => validatePaymentLink({ ...base, linkId: 'ffpl_test_20260701_abc123' }), /requires a live/);
  assert.throws(() => validatePaymentLink({ ...base, rowObj: { ...base.rowObj, Status: 'Draft' } }), /not ready/);
  assert.throws(() => validatePaymentLink({ ...base, rowObj: { ...base.rowObj, 'Completed At': '2026-07-01T00:00:00Z' } }), /already marked completed/);
  assert.throws(() => validatePaymentLink({ ...base, rowObj: { ...base.rowObj, 'Expires At': '2000-01-01T00:00:00Z' } }), /expired/);
});
