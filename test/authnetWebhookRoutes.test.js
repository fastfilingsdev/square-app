const test = require('node:test');
const assert = require('node:assert/strict');

const {
  __authNetWebhookTestHooks: {
    buildWebhookLogRow,
    computeAuthNetSignature,
    normalizeHexKey,
    verifyAuthNetSignature
  }
} = require('../src/features/authnetWebhook/routes');

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
