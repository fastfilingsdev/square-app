'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const {
  buildCloverConnectUrl,
  getCloverConnectLinkStatus,
  signCloverConnectLink,
  verifyCloverConnectLink
} = require('../src/features/clover/connectLinks');
const {
  TOKEN_PREFIX,
  decryptCloverTokenFromStorage,
  getCloverTokenStorageStatus,
  prepareCloverTokenForStorage
} = require('../src/features/clover/tokenCrypto');

test('production Clover token storage refuses plaintext without encryption key', () => {
  assert.throws(
    () => prepareCloverTokenForStorage('prod-access-token', { environment: 'Production', env: {} }),
    /Refusing to store plaintext production Clover token/
  );

  const status = getCloverTokenStorageStatus({}, 'Production');
  assert.equal(status.productionReady, false);
  assert.equal(status.storageMode, 'plaintext-sandbox-only');
});

test('sandbox Clover token storage can remain plaintext for local tests', () => {
  assert.equal(
    prepareCloverTokenForStorage('sandbox-token', { environment: 'Sandbox', env: {} }),
    'sandbox-token'
  );
});

test('Clover tokens encrypt and decrypt without exposing plaintext', () => {
  const env = { CLOVER_TOKEN_ENCRYPTION_KEY: 'test-encryption-key' };
  const encrypted = prepareCloverTokenForStorage('secret-token-value', { environment: 'Production', env });

  assert.equal(encrypted.startsWith(TOKEN_PREFIX), true);
  assert.equal(encrypted.includes('secret-token-value'), false);
  assert.equal(decryptCloverTokenFromStorage(encrypted, env), 'secret-token-value');
  assert.equal(getCloverTokenStorageStatus(env, 'Production').productionReady, true);
});

test('production Clover connect links require a configured signing secret', () => {
  assert.deepEqual(getCloverConnectLinkStatus({}, 'Production'), {
    signatureRequired: true,
    secretConfigured: false,
    productionReady: false
  });

  assert.deepEqual(getCloverConnectLinkStatus({ CLOVER_CONNECT_LINK_SECRET: 'secret' }, 'Production'), {
    signatureRequired: true,
    secretConfigured: true,
    productionReady: true
  });
});

test('Clover signed connect link verifies customer id, expiration, and signature', () => {
  const secret = 'connect-link-secret';
  const expires = 1800000000;
  const sig = signCloverConnectLink({ customerId: 'FL-123', expires, secret });

  assert.equal(verifyCloverConnectLink({ customerId: 'FL-123', expires, sig, secret, nowMs: 1700000000000 }).ok, true);
  assert.equal(verifyCloverConnectLink({ customerId: 'FL-123', expires, sig: sig.replace(/.$/, '0'), secret, nowMs: 1700000000000 }).ok, false);
  assert.equal(verifyCloverConnectLink({ customerId: 'FL-123', expires, sig, secret, nowMs: 1900000000000 }).error, 'expired');
});

test('Clover signed connect URL contains no secret and carries signed params', () => {
  const url = new URL(buildCloverConnectUrl({
    baseUrl: 'https://fastfilings-api.onrender.com',
    customerId: 'AZ-40',
    secret: 'connect-link-secret',
    ttlSeconds: 300,
    nowMs: 1700000000000
  }));

  assert.equal(url.origin + url.pathname, 'https://fastfilings-api.onrender.com/clover/connect');
  assert.equal(url.searchParams.get('customer_id'), 'AZ-40');
  assert.equal(url.searchParams.has('sig'), true);
  assert.equal(url.toString().includes('connect-link-secret'), false);
});
