'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const {
  __cloverHostedCheckoutTestHooks: {
    buildCloverHostedCheckoutIdempotencyKey,
    buildCloverHostedCheckoutLogRow,
    buildCloverHostedCheckoutSnapshot,
    computeCloverSignature,
    parseCloverSignatureHeader,
    safeEqualHex,
    verifyCloverSignature
  }
} = require('../src/features/cloverHostedCheckout/routes');

const secret = 'whsec_clover_hco_test_123';
const rawBody = JSON.stringify({
  type: 'PAYMENT',
  id: 'pay_123',
  merchantId: 'merchant_123',
  created: 1781611768.095000000,
  status: 'APPROVED',
  message: 'Approved for 100',
  checkoutSessionId: 'checkout_123'
});
const timestamp = '1781611768';

test('Clover signature parser extracts timestamp and v1 value', () => {
  assert.deepEqual(parseCloverSignatureHeader('t=1781611768,v1=abcdef'), {
    timestamp: '1781611768',
    signature: 'abcdef'
  });
});

test('Clover Hosted Checkout signature verification accepts HMAC-SHA256 timestamp.payload', () => {
  const signature = computeCloverSignature({ timestamp, rawBody, secret });
  const result = verifyCloverSignature({
    rawBody,
    signatureHeader: `t=${timestamp},v1=${signature}`,
    secret,
    maxAgeSeconds: 0
  });
  assert.equal(result.ok, true);
  assert.equal(result.status, 'signature-valid');
});

test('Clover Hosted Checkout signature verification rejects tampered payloads', () => {
  const signature = computeCloverSignature({ timestamp, rawBody, secret });
  const result = verifyCloverSignature({
    rawBody: rawBody.replace('APPROVED', 'DECLINED'),
    signatureHeader: `t=${timestamp},v1=${signature}`,
    secret,
    maxAgeSeconds: 0
  });
  assert.equal(result.ok, false);
  assert.equal(result.status, 'signature-invalid');
  assert.equal(result.httpStatus, 401);
});

test('Clover Hosted Checkout snapshot uses a safe whitelist only', () => {
  const snapshot = buildCloverHostedCheckoutSnapshot({
    type: 'PAYMENT',
    id: 'pay_123',
    merchantId: 'merchant_123',
    status: 'APPROVED',
    message: 'Approved for 100',
    checkoutSessionId: 'checkout_123',
    cardNumber: '4111111111111111',
    token: 'secret-token',
    signingSecret: 'secret'
  });
  const serialized = JSON.stringify(snapshot);
  assert.equal(serialized.includes('4111111111111111'), false);
  assert.equal(serialized.includes('secret-token'), false);
  assert.equal(serialized.includes('signingSecret'), false);
  assert.equal(snapshot.id, 'pay_123');
  assert.equal(snapshot.checkoutSessionId, 'checkout_123');
});

test('Clover Hosted Checkout log row includes idempotency key and no raw payload secrets', () => {
  const body = JSON.parse(rawBody);
  const row = buildCloverHostedCheckoutLogRow({
    body: { ...body, cardNumber: '4111111111111111', token: 'secret-token' },
    rawBody,
    signatureStatus: 'signature-valid',
    receivedAt: new Date('2026-06-16T12:09:28Z')
  });
  assert.equal(row[0], '2026-06-16T12:09:28.000Z');
  assert.equal(row[2], 'merchant_123');
  assert.equal(row[5], 'pay_123');
  assert.equal(row[6], 'checkout_123');
  assert.equal(row[9], 'signature-valid');
  assert.equal(row[10], 'merchant_123|PAYMENT|pay_123|checkout_123');
  assert.equal(row[11].includes('4111111111111111'), false);
  assert.equal(row[11].includes('secret-token'), false);
});

test('Clover Hosted Checkout idempotency key is stable for same payment/session', () => {
  const key = buildCloverHostedCheckoutIdempotencyKey({
    merchantId: 'merchant_123',
    type: 'PAYMENT',
    id: 'pay_123',
    checkoutSessionId: 'checkout_123'
  });
  assert.equal(key, 'merchant_123|PAYMENT|pay_123|checkout_123');
});

test('safeEqualHex rejects malformed hex without throwing', () => {
  assert.equal(safeEqualHex('not-hex', 'not-hex'), false);
});
