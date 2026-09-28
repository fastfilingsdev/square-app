'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

// Exercise the actual subscription router without starting timers, listening,
// loading provider clients or reading/writing any Google spreadsheet.
function fixture(env = {}, failure) {
  const handlers = new Map(), calls = [];
  const router = { get() {}, post(route, handler) { handlers.set(route, handler); } };
  const module = { exports: {} };
  const forbidden = () => { throw Error('Unexpected external operation'); };
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, '../src/features/subscriptions/routes.js'), 'utf8'), {
    module, process: { env }, console: { log() {}, error() {} },
    setTimeout: forbidden, setInterval: forbidden,
    require(name) {
      if (name === 'express') return { Router: () => router };
      if (name === '../../core/googleSheets') return { getSheetsClient: forbidden };
      if (name === './recoveredActiveSync') return {};
      if (name === './authnetNewOrdersSync') return {
        async syncAuthNetNewOrders(options) {
          calls.push(JSON.parse(JSON.stringify(options)));
          if (failure) throw Error(failure);
          return { ok: true, synthetic: true };
        }
      };
      throw Error('Unexpected dependency: ' + name);
    }
  });
  module.exports.createSubscriptionsRouter();
  return {
    calls,
    async invoke({ headers = {}, query = {}, body = {} } = {}) {
      const response = { code: 200, headers: {}, status(code) { this.code = code; return this; },
        set(headers) { Object.assign(this.headers, headers); return this; },
        json(body) { this.body = body; return this; } };
      await handlers.get('/authnet/new-orders/sync')({ get: key => headers[key] || '', query, body }, response);
      return response;
    }
  };
}

test('Render cron query contract keeps ARB creation disabled by default', async () => {
  const f = fixture({ AUTHNET_SYNC_TOKEN: 'synthetic-fallback' });
  const res = await f.invoke({ headers: { 'x-ff-sync-token': 'synthetic-fallback' },
    query: { mode: 'apply', triggeredBy: 'render-cron-0630-pt' } });
  assert.equal(res.code, 200);
  assert.deepEqual(f.calls, [{ mode: 'apply', triggeredBy: 'render-cron-0630-pt',
    lookbackDays: 14, maxDetails: 2500, arbMode: 'dry-run', allowLiveArb: false }]);
  assert.equal(res.headers['Cache-Control'], 'no-store, max-age=0');
});

test('cron fallback credential is rejected when a different primary token is configured', async () => {
  const f = fixture({ FF_SYNC_ADMIN_TOKEN: 'synthetic-primary', AUTHNET_SYNC_TOKEN: 'synthetic-fallback' });
  assert.equal((await f.invoke({ headers: { 'x-ff-sync-token': 'synthetic-fallback' }, query: { mode: 'apply' } })).code, 401);
  assert.equal(f.calls.length, 0);
});

test('aligned primary and cron tokens preserve the caller contract', async () => {
  const f = fixture({ FF_SYNC_ADMIN_TOKEN: 'synthetic-aligned', AUTHNET_SYNC_TOKEN: 'synthetic-aligned' });
  assert.equal((await f.invoke({ headers: { 'x-ff-sync-token': 'synthetic-aligned' }, query: { mode: 'apply' } })).code, 200);
  assert.equal(f.calls.length, 1);
});

test('missing auth and query-string credentials never reach subscription sync', async () => {
  for (const env of [{}, { AUTHNET_SYNC_TOKEN: 'synthetic-fallback' }]) {
    const f = fixture(env);
    assert.equal((await f.invoke({ query: { mode: 'apply', token: 'synthetic-fallback' } })).code, 401);
    assert.equal(f.calls.length, 0);
  }
});

test('query and string flags cannot grant live ARB permission', async () => {
  const f = fixture({ AUTHNET_SYNC_TOKEN: 'synthetic-fallback' });
  for (const body of [{}, { allowLiveArb: 'true' }]) {
    await f.invoke({ headers: { 'x-ff-sync-token': 'synthetic-fallback' },
      query: { mode: 'apply', arbMode: 'live', allowLiveArb: 'true' }, body });
  }
  assert.equal(f.calls.length, 2);
  assert.ok(f.calls.every(call => call.arbMode === 'live' && call.allowLiveArb === false));
});

test('subscription sync failure returns an error without an application retry', async () => {
  const f = fixture({ AUTHNET_SYNC_TOKEN: 'synthetic-fallback' }, 'Synthetic interrupted sync');
  const res = await f.invoke({ headers: { 'x-ff-sync-token': 'synthetic-fallback' }, query: { mode: 'apply' } });
  assert.equal(res.code, 500);
  assert.equal(f.calls.length, 1);
});
