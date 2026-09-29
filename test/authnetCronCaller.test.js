'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { run, isScheduledMinute, ENDPOINT } = require('../scripts/authnet-new-orders-cron');
const now = new Date('2026-09-29T13:30:00Z');
const env = { FF_SYNC_ADMIN_TOKEN: 'synthetic-primary', AUTHNET_SYNC_TOKEN: 'synthetic-fallback' };
const reply = (body, status = 200) => new Response(body, { status });

test('same Pacific 06:30 schedule in daylight and standard time', () => {
  for (const date of ['2026-09-29T13:30:00Z', '2026-12-29T14:30:00Z']) assert.equal(isScheduledMinute(new Date(date)), true);
  for (const date of ['2026-09-29T14:30:00Z', '2026-12-29T13:30:00Z', '2026-09-29T13:31:00Z']) assert.equal(isScheduledMinute(new Date(date)), false);
});
test('outside schedule does not require secrets or dispatch', async () => {
  const result = await run({ env: {}, now: new Date('2026-09-29T14:30:00Z'), http() { assert.fail('dispatch'); } });
  assert.equal(result.skipped, true); assert.equal(result.exitCode, 0);
});
test('missing or malformed credentials stop before dispatch', async () => {
  for (const token of ['', ' bad ', 'bad\nvalue']) {
    const result = await run({ env: { AUTHNET_SYNC_TOKEN: token }, now, http() { assert.fail('dispatch'); } });
    assert.equal(result.requestAttempted, false); assert.equal(result.exitCode, 1);
  }
});
test('fixed endpoint, explicit safe ARB flags, primary precedence and no redirects', async () => {
  let calls = 0;
  const result = await run({ env, now, http: async (url, init) => {
    calls++; assert.equal(url, ENDPOINT); assert.equal(init.method, 'POST'); assert.equal(init.redirect, 'manual');
    assert.equal(init.headers['x-ff-sync-token'], 'synthetic-primary');
    assert.equal(url.includes('synthetic-primary'), false);
    assert.deepEqual(JSON.parse(init.body), { arbMode: 'dry-run', allowLiveArb: false });
    return reply('{"ok":true,"customer":"must-not-log"}');
  } });
  assert.equal(calls, 1); assert.equal(result.ok, true); assert.equal(JSON.stringify(result).includes('must-not-log'), false);
});
test('fallback remains compatible when no primary token exists', async () => {
  await run({ env: { AUTHNET_SYNC_TOKEN: 'synthetic-fallback' }, now, http: async (_, init) => {
    assert.equal(init.headers['x-ff-sync-token'], 'synthetic-fallback'); return reply('{"ok":true}');
  } });
});
for (const status of [302, 401, 409, 429, 500, 503]) {
  test('HTTP ' + status + ' returns reconciliation without retry or error disclosure', async () => {
    let calls = 0;
    const result = await run({ env, now, http: async () => { calls++; return reply('synthetic-private-error', status); } });
    assert.equal(calls, 1); assert.equal(result.requiresReconciliation, true); assert.equal(result.automaticRetryAllowed, false);
    assert.equal(result.exitCode, 2); assert.equal(JSON.stringify(result).includes('synthetic-private-error'), false);
  });
}
for (const body of ['not JSON', '{"ok":"true"}', '{"ok":false}', '[]', 'null', 'x'.repeat(1024 * 1024 + 1)]) {
  test('invalid/oversized acknowledgement rejected: ' + body.slice(0, 20), async () => {
    let calls = 0;
    const result = await run({ env, now, http: async () => { calls++; return reply(body); } });
    assert.equal(calls, 1); assert.equal(result.requiresReconciliation, true);
  });
}
test('lost acknowledgement/timeout is not dispatched again', async () => {
  let calls = 0;
  const result = await run({ env, now, http: async () => { calls++; throw Error('synthetic-secret-timeout'); } });
  assert.equal(calls, 1); assert.equal(result.exitCode, 2); assert.equal(JSON.stringify(result).includes('synthetic-secret'), false);
});
test('actual cron caller reaches actual subscription router with safe options', async () => {
  const handlers = new Map(), calls = [], module = { exports: {} };
  const forbidden = () => { throw Error('External operation forbidden'); };
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, '../src/features/subscriptions/routes.js'), 'utf8'), {
    module, process: { env }, console: { log() {}, error() {} }, setTimeout: forbidden, setInterval: forbidden,
    require(name) {
      if (name === 'express') return { Router: () => ({ get() {}, post: (p, f) => handlers.set(p, f) }) };
      if (name === '../../core/googleSheets') return { getSheetsClient: forbidden };
      if (name === './recoveredActiveSync') return {};
      if (name === './authnetNewOrdersSync') return { syncAuthNetNewOrders: async options => { calls.push(JSON.parse(JSON.stringify(options))); return { ok: true }; } };
      throw Error('Unexpected dependency');
    }
  });
  module.exports.createSubscriptionsRouter();
  const result = await run({ env, now, http: async (url, init) => {
    const target = new URL(url), req = { get: k => init.headers[k] || '', query: Object.fromEntries(target.searchParams), body: JSON.parse(init.body) };
    const res = { code: 200, status(code) { this.code = code; return this; }, set() { return this; }, json(data) { this.data = data; return this; } };
    await handlers.get('/authnet/new-orders/sync')(req, res);
    return reply(JSON.stringify(res.data), res.code);
  } });
  assert.equal(result.ok, true);
  assert.deepEqual(calls, [{ mode: 'apply', triggeredBy: 'render-cron-0630-pt', lookbackDays: 14, maxDetails: 2500, arbMode: 'dry-run', allowLiveArb: false }]);
});
