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
const { __billingPaymentLinksTestHooks } = require('../src/features/billingPaymentLinks/routes');

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
  assert.ok(
    Object.keys(tx).indexOf('customer') < Object.keys(tx).indexOf('billTo'),
    'Authorize.Net schema requires customer before billTo'
  );
  assert.equal(tx.lineItems.lineItem.length, 2);
  assert.equal(tx.lineItems.lineItem[1].quantity, '2');
  assert.equal(tx.lineItems.lineItem[1].unitPrice, '20.00');
  assert.deepEqual(tx.userFields.userField.find(field => field.name === 'ffPaymentLinkId'), { name: 'ffPaymentLinkId', value: 'ffpl_live_20260701_abc123' });
});

test('cancellation checkout uses readable invoice numbers and state-aware description', () => {
  const link = {
    linkId: 'ffpl_live_20260701_baa4e518c6283cb',
    rowObj: {
      'Created At': '2026-07-01T23:00:00.000Z',
      'Link Type': 'Sales Certificate Cancellation',
      'Customer ID': 'AZ-37',
      State: 'AZ',
      Name: 'Gilmar Arellano',
      Email: 'hello@example.test',
      Purpose: 'Sales certificate cancellation assistance',
      'Invoice #': 'FFPL-baa4e518c6283cb'
    },
    items: [{ name: 'Sales certificate cancellation assistance', amount: '80.00', quantity: 1 }]
  };

  const tx = buildHostedPaymentTransactionRequest(link);
  assert.equal(tx.order.invoiceNumber, 'FFAZ37-CXL0701-3CB');
  assert.ok(tx.order.invoiceNumber.length <= 20);
  assert.equal(tx.order.description, 'Arizona sales certificate cancellation assistance');
  assert.equal(tx.lineItems.lineItem[0].description, 'Arizona sales certificate cancellation assistance');
});

test('custom readable invoice numbers are preserved', () => {
  const tx = buildHostedPaymentTransactionRequest({
    linkId: 'ffpl_live_20260701_custom',
    rowObj: { 'Link Type': 'Past Period Filings', 'Customer ID': 'FL-136', 'Invoice #': 'FFFL136-FIL0701-001', Purpose: 'Florida past-period filing assistance' },
    items: [{ name: 'Past-period filing', amount: '20.00', quantity: 1 }]
  });

  assert.equal(tx.order.invoiceNumber, 'FFFL136-FIL0701-001');
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

test('payment link page uses Fast Filings and Authorize.Net branding', () => {
  const html = __billingPaymentLinksTestHooks.renderPaymentLinkHtml({
    link: {
      linkId: 'ffpl_live_20260701_branding',
      rowObj: { 'Link Type': 'Sales Certificate Cancellation', 'Customer ID': 'AZ-37', Purpose: 'Sales certificate cancellation assistance' },
      items: [{ name: 'Sales certificate cancellation assistance', amount: '80.00', quantity: 1 }],
      authorizationText: 'I authorize Fast Filings to prepare and submit a cancellation request.'
    }
  });

  assert.match(html, /\/assets\/payment-update\/fast-filings-logo\.png/);
  assert.match(html, /\/assets\/payment-update\/authorize-net-logo\.svg/);
  assert.match(html, /Secured by/);
});

test('payment link page hides internal/test labels and card-storage paragraph', () => {
  const html = __billingPaymentLinksTestHooks.renderPaymentLinkHtml({
    link: {
      linkId: 'ffpl_live_20260701_internal_4d10342c4f',
      rowObj: {
        'Link Type': 'Sales Certificate Cancellation',
        'Customer ID': 'TEST-FFPL',
        Purpose: 'Internal rollout test: certificate cancellation authorization + bundled past-period filing item'
      },
      items: [{ name: 'Sales certificate cancellation assistance', amount: '80.00', quantity: 1 }],
      authorizationText: 'I authorize Fast Filings to prepare and submit a cancellation request.'
    }
  });

  assert.doesNotMatch(html, /This payment link is for/i);
  assert.doesNotMatch(html, /Fast Filings will not see or store your full card number/i);
  assert.doesNotMatch(html, /Secure link:/i);
  assert.doesNotMatch(html, /ffpl_live_20260701_internal_4d10342c4f/);
  assert.doesNotMatch(html, /<span class="pill">Sales Certificate Cancellation<\/span>/);
});
