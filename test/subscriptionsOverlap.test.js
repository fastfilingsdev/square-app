'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

// Actual router/timer entry points; only the business sync is synthetic.
// No provider, Google, network, financial operations, or real timers.
function fixture() {
  const handlers = new Map(), calls = [], pending = [];
  const maintenance = require('../src/core/maintenance').createMaintenance({ env: {} });
  const module = { exports: {} };
  const deny = () => { throw Error('External operation prohibited'); };
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, '../src/features/subscriptions/routes.js'), 'utf8'), {
    module, process: { env: { FF_SYNC_ADMIN_TOKEN: 'synthetic' } },
    console: { log() {}, error() {} }, setTimeout: deny, setInterval: deny,
    require(name) {
      if (name === 'express') return { Router: () => ({ get() {}, post(p, f) { handlers.set(p, f); } }) };
      if (name === '../../core/maintenance') return { maintenance };
      if (name === '../../core/googleSheets') return { getSheetsClient: deny };
      if (name === './recoveredActiveSync') return {};
      if (name === './authnetNewOrdersSync') return {
        newOrdersAutomationLookbackDays: () => 14,
        newOrdersAutomationMaxDetails: () => 2500,
        syncAuthNetNewOrders(options) {
          calls.push(options);
          return new Promise((resolve, reject) => pending.push({ resolve, reject }));
        }
      };
      throw Error('Unexpected dependency: ' + name);
    }
  });
  module.exports.createSubscriptionsRouter();
  return {
    calls, pending, maintenance,
    tick: () => module.exports.runNewOrdersAutomationOnce(),
    async manual() {
      const res = { code: 200, headers: {}, status(n) { this.code = n; return this; },
        set(h) { Object.assign(this.headers, h); return this; }, json(b) { this.body = b; return this; } };
      await handlers.get('/authnet/new-orders/sync')({
        get: () => 'synthetic', query: {}, body: { mode: 'apply', arbMode: 'live', allowLiveArb: true }
      }, res);
      return res;
    }
  };
}

test('manual sync cannot overlap an active scheduled sync', async () => {
  const f = fixture(), scheduled = f.tick(), manual = f.manual();
  assert.equal(f.calls.length, 1, 'overlap must not enter business sync twice');
  const blocked = await manual;
  assert.equal(blocked.code, 409);
  assert.equal(blocked.body.operationStarted, false);
  assert.equal(blocked.body.retryAutomatically, false);
  f.pending[0].resolve({ ok: true }); await scheduled;
});

test('scheduled sync cannot overlap an active manual sync', async () => {
  const f = fixture(), manual = f.manual(), scheduled = f.tick();
  assert.equal(f.calls.length, 1, 'timer must not enter the manual run');
  assert.equal((await scheduled).skipped, true);
  assert.equal(f.maintenance.status().uncertain, 0);
  f.pending[0].resolve({ ok: true }); await manual;
});

test('two authenticated manual calls share the same admission guard', async () => {
  const f = fixture(), first = f.manual(), second = f.manual();
  assert.equal(f.calls.length, 1);
  assert.equal((await second).code, 409);
  f.pending[0].resolve({ ok: true }); await first;
});

test('successful completion releases admission for the next scheduled pass', async () => {
  const f = fixture(), first = f.manual();
  f.pending[0].resolve({ ok: true }); await first;
  const next = f.tick();
  assert.equal(f.calls.length, 2);
  f.pending[1].resolve({ ok: true }); await next;
});

test('a failed sync is not automatically redispatched by the admission guard', async () => {
  const f = fixture(), first = f.manual();
  f.pending[0].reject(Error('synthetic failure'));
  assert.equal((await first).code, 500);
  assert.equal(f.calls.length, 1);
  const later = await f.tick();
  assert.equal(later.skipped, true);
  assert.equal(later.reason, 'subscription sync requires reconciliation');
  const manualRetry = await f.manual();
  assert.equal(manualRetry.code, 409);
  assert.equal(manualRetry.body.operationStarted, false);
  assert.equal(f.calls.length, 1);
});

test('a failed scheduled pass blocks the next tick and manual replay', async () => {
  const f = fixture(), first = f.tick();
  f.pending[0].reject(Error('synthetic lost sheet acknowledgement'));
  assert.equal((await first).ok, false);
  assert.equal(f.maintenance.status().uncertain, 1);
  assert.equal((await f.tick()).skipped, true);
  assert.equal((await f.manual()).code, 409);
  assert.equal(f.calls.length, 1);
  assert.equal(f.maintenance.status().active, 0);
  assert.equal(f.maintenance.status().uncertain, 1);
});
