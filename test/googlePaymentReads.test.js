'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { createPaymentReadHandler, validateRequest } = require('../src/features/googlePaymentReads/gateway');
const body = { operation: 'getTransactionDetailsRequest', parameters: { transId: '1234567' } };
const request = { authorization: 'Bearer synthetic-oauth-token', body };
const good = { status: 200, data: { messages: { resultCode: 'Ok' }, transaction: { transId: '1234567' } } };
const identity = { ok: true, verified: true, email: 'returns@fastfilings.com' };
function fixture(overrides = {}) {
  const calls = [], auth = [];
  const env = { FF_GOOGLE_PAYMENT_READS_ENABLED: 'true', AUTHNET_API_LOGIN_ID: 'synthetic-login',
    AUTHNET_TRANSACTION_KEY: 'synthetic-key', AUTHNET_SIGNATURE_KEY: 'synthetic-signature' };
  return { calls, auth, env, handle: createPaymentReadHandler({ env,
    verify: async token => { auth.push(token); return identity; },
    post: async (...args) => { calls.push(args); return good; }, ...overrides }) };
}

test('read bridge is default-off before authentication or provider access', async () => {
  const f = fixture({ env: {} });
  assert.equal((await f.handle(request)).status, 503);
  assert.equal(f.calls.length + f.auth.length, 0);
});

test('missing token, unverified identity and other Google users cannot reach provider', async () => {
  const f = fixture();
  assert.equal((await f.handle({ body })).status, 401);
  for (const principal of [{ ...identity, email: 'returns1@fastfilings.com' },
    { ...identity, verified: false }, { ...identity, ok: false }]) {
    const denied = fixture({ verify: async () => principal });
    assert.equal((await denied.handle(request)).status, 401);
    assert.equal(denied.calls.length, 0);
  }
  assert.equal(f.calls.length, 0);
});

test('all seven read operations have narrow request contracts', () => {
  const cases = {
    ARBGetSubscriptionListRequest: { searchType: 'subscriptionActive', paging: { limit: 1000, offset: 1 } },
    ARBGetSubscriptionRequest: { subscriptionId: '123', includeTransactions: true },
    getUnsettledTransactionListRequest: { sorting: { orderBy: 'id', orderDescending: false } },
    getSettledBatchListRequest: { firstSettlementDate: '2026-09-01T00:00:00Z', lastSettlementDate: '2026-09-30T00:00:00Z' },
    getTransactionListRequest: { batchId: '123' }, getTransactionDetailsRequest: { transId: '123' },
    getCustomerProfileRequest: { customerProfileId: '123', unmaskExpirationDate: false }
  };
  for (const [operation, parameters] of Object.entries(cases)) assert.equal(validateRequest({ operation, parameters }), true, operation);
});

test('mutation, authentication injection, unknown fields, unmasking and invalid pagination are rejected', async () => {
  const f = fixture();
  for (const invalid of [
    { ...body, operation: 'ARBCreateSubscriptionRequest' },
    { ...body, operation: 'ARBCancelSubscriptionRequest' },
    { ...body, operation: 'createCustomerProfileFromTransactionRequest' },
    { ...body, operation: '__proto__' },
    { ...body, url: 'https://attacker.invalid' },
    { ...body, parameters: { transId: '123', merchantAuthentication: {} } },
    { ...body, parameters: { transId: 123 } },
    { operation: 'getCustomerProfileRequest', parameters: { customerProfileId: '123', unmaskExpirationDate: true } },
    { operation: 'getUnsettledTransactionListRequest', parameters: { paging: { limit: 1001, offset: 1 } } },
    { operation: 'getUnsettledTransactionListRequest', parameters: { sorting: { orderBy: 'anything', orderDescending: false } } }
  ]) assert.equal((await f.handle({ ...request, body: invalid })).status, 400);
  assert.equal(f.calls.length, 0);
});

test('keys are injected only at fixed provider endpoint with bounded transport and no redirects', async () => {
  const f = fixture(); f.env.AUTHNET_API_URL = 'https://attacker.invalid';
  assert.equal((await f.handle(request)).status, 200);
  const [url, payload, options] = f.calls[0];
  assert.equal(url, 'https://api2.authorize.net/xml/v1/request.api');
  assert.deepEqual(payload.getTransactionDetailsRequest.merchantAuthentication,
    { name: 'synthetic-login', transactionKey: 'synthetic-key' });
  assert.equal(options.maxRedirects, 0);
  assert.equal(options.timeout, 45000);
  assert.equal(f.calls.length, 1);
});

test('response masks payment fields, strips credentials and redacts reflected secrets', async () => {
  const f = fixture({ post: async () => ({ status: 200, data: { messages: { resultCode: 'Ok' },
    transaction: { cardNumber: '4111111111111111', expirationDate: '2030-01', cardCode: '123',
      merchantAuthentication: { transactionKey: 'synthetic-key' }, accountNumber: '1234567890' },
    description: 'synthetic-login synthetic-key synthetic-signature' } }) });
  const result = await f.handle(request), text = JSON.stringify(result.body);
  assert.equal(result.body.transaction.cardNumber, 'XXXX1111');
  assert.equal(result.body.transaction.accountNumber, 'XXXX7890');
  for (const forbidden of ['4111111111111111', 'expirationDate', 'cardCode', 'merchantAuthentication', 'synthetic-key', 'synthetic-login', 'synthetic-signature']) {
    assert.equal(text.includes(forbidden), false, forbidden);
  }
});

test('provider and OAuth failures never leak tokens, error payloads or automatically retry', async () => {
  let calls = 0;
  const f = fixture({ post: async () => { calls++; throw Error('synthetic-key PRIVATE PAYMENT DATA'); } });
  const result = await f.handle(request);
  assert.equal(result.status, 502);
  assert.equal(result.body.retryAutomatically, false);
  assert.equal(JSON.stringify(result).includes('PRIVATE'), false);
  assert.equal(calls, 1);
  const authFailure = fixture({ verify: async () => { throw Error('synthetic-oauth-token'); } });
  assert.equal(JSON.stringify(await authFailure.handle(request)).includes('synthetic-oauth-token'), false);
  assert.equal(authFailure.calls.length, 0);
});

test('concurrent work is bounded and capacity is released after completion', async () => {
  const pending = [];
  const f = fixture({ post: () => new Promise(resolve => pending.push(resolve)) });
  const jobs = Array.from({ length: 8 }, () => f.handle(request));
  assert.equal((await f.handle(request)).status, 429);
  await Promise.resolve();
  assert.equal(pending.length, 8);
  pending.splice(0).forEach(resolve => resolve(good));
  await Promise.all(jobs);
  const later = f.handle(request); await Promise.resolve();
  pending[0](good); assert.equal((await later).status, 200);
});

test('Google adapter sends OAuth only; bounded batches preserve item/error ordering without retry', () => {
  const chunks = [];
  const source = fs.readFileSync(path.join(__dirname, '../docs/google-payment-read-adapter.gs'), 'utf8');
  const context = { ScriptApp: { getOAuthToken: () => 'synthetic-oauth-token' }, UrlFetchApp: {
    fetchAll: requests => { chunks.push(requests); return requests.map(r => {
      const transId = JSON.parse(r.payload).parameters.transId;
      return { getResponseCode: () => transId === '3' ? 503 : 200,
        getContentText: () => JSON.stringify({ messages: { resultCode: 'Ok' }, transId }) };
    }); }, fetch: () => { throw Error('Unexpected single call'); }
  } };
  vm.createContext(context); vm.runInContext(source, context);
  const results = context.FF_paymentReadBatch_(Array.from({ length: 6 }, (_, i) => ({ ...body, parameters: { transId: String(i + 1) } })));
  assert.deepEqual(chunks.map(c => c.length), [4, 2]);
  assert.deepEqual(Array.from(results, r => r.data?.transId || 'error'), ['1', '2', 'error', '4', '5', '6']);
  for (const r of chunks.flat()) {
    assert.equal(r.url, 'https://fastfilings-api.onrender.com/google-payments/read');
    assert.equal(r.headers.Authorization, 'Bearer synthetic-oauth-token');
    assert.equal(r.followRedirects, false);
    assert.equal(r.payload.includes('merchantAuthentication'), false);
  }
  assert.throws(() => context.FF_paymentReadRequest_('ARBCreateSubscriptionRequest', {}));
  assert.throws(() => context.FF_paymentReadRequest_('getTransactionDetailsRequest', { merchantAuthentication: {} }));
});

test('production reporting string booleans normalize to the strict backend contract without mutation', () => {
  const context = { ScriptApp: { getOAuthToken: () => 'synthetic-token' } };
  vm.createContext(context);
  vm.runInContext(fs.readFileSync(path.join(__dirname, '../docs/google-payment-read-adapter.gs'), 'utf8'), context);
  for (const orderDescending of ['true', 'false']) {
    const parameters = { sorting: { orderBy: 'submitTimeUTC', orderDescending }, paging: { limit: 1000, offset: 1 } };
    const built = context.FF_paymentReadRequest_('getUnsettledTransactionListRequest', parameters);
    const parsed = JSON.parse(built.payload);
    assert.equal(validateRequest(parsed), true);
    assert.equal(parsed.parameters.sorting.orderDescending, orderDescending === 'true');
    assert.equal(parameters.sorting.orderDescending, orderDescending);
  }
});

test('replacement detail caller preserves deduplication, ID matching, errors and pacing across batches', () => {
  const calls = [], sleeps = [];
  const context = { ACTIVE_SUBSCRIPTIONS_DETAIL_BATCH_SIZE: 2,
    Utilities: { sleep: n => sleeps.push(n) },
    FF_paymentReadBatch_: items => { calls.push(items); return items.map(r => r.parameters.subscriptionId === '2'
      ? { error: 'synthetic failure' } : { data: { subscription: { id: r.parameters.subscriptionId } } }); }
  };
  vm.createContext(context);
  vm.runInContext(fs.readFileSync(path.join(__dirname, '../docs/google-payment-read-replacements.gs'), 'utf8'), context);
  const result = context.activeWorkflowFetchSubscriptionDetails_(['1', '2', '1', '', ' 3 ']);
  assert.deepEqual(Object.keys(result), ['1', '2', '3']);
  assert.equal(result['1'].data.subscription.id, '1');
  assert.equal(result['2'].error, 'synthetic failure');
  assert.equal(result['3'].data.subscription.id, '3');
  assert.deepEqual(calls.map(c => c.length), [2, 1]);
  assert.deepEqual(sleeps, [150, 150]);
  assert.throws(() => context.activeWorkflowFetchSubscriptionDetails_(['__proto__']));
});

test('replacement payload adapter rejects old embedded credentials and financial requests before any I/O', () => {
  let fetched = 0;
  const context = { ScriptApp: { getOAuthToken: () => 'synthetic-token' }, UrlFetchApp: {
    fetch: () => { fetched++; throw Error('Network forbidden'); } } };
  vm.createContext(context);
  for (const file of ['google-payment-read-adapter.gs', 'google-payment-read-replacements.gs']) {
    vm.runInContext(fs.readFileSync(path.join(__dirname, '../docs', file), 'utf8'), context);
  }
  assert.throws(() => context.activeWorkflowAuthNetPost_({ getTransactionDetailsRequest: {
    merchantAuthentication: { name: 'synthetic', transactionKey: 'synthetic' }, transId: '123' } }));
  assert.throws(() => context.terminationCAuthNetPost_({ ARBCreateSubscriptionRequest: {} }));
  assert.throws(() => context.FF_paymentReadPayload_({ first: {}, second: {} }));
  assert.equal(fetched, 0);
});
