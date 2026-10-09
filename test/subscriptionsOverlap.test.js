'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

// Actual router/timer entry points; only the business sync is synthetic.
// No provider, Google, network, financial operations, or real timers.
function fixture() {
  const handlers = new Map(), calls = [], pending = [], notes = [];
  const maintenance = require('../src/core/maintenance').createMaintenance({ env: {} });
  const module = { exports: {} };
  const deny = () => { throw Error('External operation prohibited'); };
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, '../src/features/subscriptions/routes.js'), 'utf8'), {
    module, process: { env: { FF_SYNC_ADMIN_TOKEN: 'synthetic' } },
    console: { log() {}, error() {} }, setTimeout: deny, setInterval: deny,
    require(name) {
      if (name === 'express') return { Router: () => ({
        get(p, f) { handlers.set('GET ' + p, f); },
        post(p, f) { handlers.set(p, f); }
      }) };
      if (name === '../../core/maintenance') return { maintenance };
      if (name === '../../core/googleSheets') return { getSheetsClient: deny };
      if (name === './recoveredActiveSync') return {};
      if (name === './authnetNewOrdersSync') return {
        appendBillingAuditNote: async note => { notes.push(note); return true; },
        isArbAutoCreateEnabled: () => false,
        isNewOrdersAutomationEnabled: () => true,
        isNewOrdersAutoDiscoveryEnabled: () => false,
        newOrdersAutomationIntervalMs: () => 900000,
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
    calls, pending, maintenance, notes,
    tick: () => module.exports.runNewOrdersAutomationOnce(),
    health() {
      const res = { body: null, json(b) { this.body = b; return this; } };
      handlers.get('GET /authnet/new-orders/sync/health')({}, res);
      return res.body;
    },
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

test('a failure before any provider write does not freeze later ticks', async () => {
  const f = fixture(), first = f.tick();
  const err = Error('synthetic Sheets 503 during read'); err.providerWriteAttempted = false;
  f.pending[0].reject(err);
  assert.equal((await first).ok, false);
  assert.equal(f.notes.length, 1); assert.match(f.notes[0].action, /ERROR/);
  const next = f.tick();
  assert.equal(f.calls.length, 2, 'next tick must run the sync again');
  f.pending[1].resolve({ ok: true }); await next;
});

test('a blocked tick is logged to the Billing Audit Log, at most hourly', async () => {
  const f = fixture(), first = f.tick();
  f.pending[0].reject(Error('synthetic lost sheet acknowledgement'));
  await first;
  await f.tick(); await f.tick();
  assert.equal(f.calls.length, 1);
  assert.equal(f.notes.filter(n => /BLOCKED/.test(n.action)).length, 1);
  const health = f.health();
  assert.equal(health.admission.needsReconciliation, true);
  assert.match(health.admission.reconciliationReason, /lost sheet acknowledgement/);
  assert.match(health.automation.lastSkipReason, /reconciliation/);
  assert.ok(health.automation.lastSkipAtUtc);
});

test('repeated pre-attempt failures retry and audit once until the message changes', async () => {
  const f = fixture();
  const fail = message => {
    const err = Error(message);
    err.providerWriteAttempted = false;
    return err;
  };
  const first = f.tick();
  f.pending[0].reject(fail('synthetic Sheets 503 during read'));
  await first;
  const second = f.tick();
  f.pending[1].reject(fail('synthetic Sheets 503 during read'));
  await second;
  assert.equal(f.calls.length, 2);
  assert.equal(f.notes.filter(n => /ERROR/.test(n.action)).length, 1);
  const third = f.tick();
  f.pending[2].reject(fail('synthetic Auth.Net timeout during read'));
  await third;
  assert.equal(f.notes.filter(n => /ERROR/.test(n.action)).length, 2);
  const health = f.health();
  assert.equal(health.admission.needsReconciliation, false);
  assert.match(health.automation.lastError, /Auth\.Net timeout/);
  assert.equal(health.automation.lastSkipReason, '');
});
