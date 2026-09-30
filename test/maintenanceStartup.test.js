'use strict';
// Operator regression: execute actual startup/config functions without loading
// network clients, Express, credentials, real timers or the application server.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const root = path.resolve(__dirname, '../src');
const allowed = new Set([
  'core/maintenance.js', 'core/subscriptionLedger.js',
  'features/subscriptions/routes.js', 'features/subscriptions/authnetNewOrdersSync.js',
  'features/subscriptions/recoveredActiveSync.js', 'features/authnetWebhook/routes.js',
  'features/authnetWebhook/paymentUpdateBRecovery.js', 'features/authnetWebhook/watchdog.js'
].map(p => path.join(root, p)));
const jobs = [
  ['new orders', 'FF_BILLING_NEW_ORDERS_AUTOMATION_ENABLED', 'features/subscriptions/routes.js', 'startNewOrdersAutomation', 30000],
  ['recovered active', 'RECOVERED_ACTIVE_SYNC_ENABLED', 'features/subscriptions/routes.js', 'startRecoveredActiveSyncAutomation', 60000],
  ['webhook watchdog', 'AUTHNET_WEBHOOK_WATCHDOG_ENABLED', 'features/authnetWebhook/watchdog.js', 'startWebhookWatchdogAutomation', 30000],
  ['B fallback', 'AUTHNET_B_FALLBACK_AUTOMATION_ENABLED', 'features/authnetWebhook/routes.js', 'startAuthNetBFallbackAutomation', 45000]
];
function fixture(env) {
  const timers = [], cache = new Map();
  let externalCalls = 0;
  const deny = () => { externalCalls++; throw Error('External operation prohibited'); };
  const connector = new Proxy({}, { get: () => deny });
  function load(file) {
    if (!allowed.has(file)) throw Error('Unapproved module: ' + file);
    if (cache.has(file)) return cache.get(file).exports;
    const module = { exports: {} }; cache.set(file, module);
    const schedule = type => (callback, ms) => {
      const handle = { type, callback, ms, unref() {} }; timers.push(handle); return handle;
    };
    vm.runInNewContext(fs.readFileSync(file, 'utf8'), {
      module, process: { env }, console: { log() {}, error() {}, warn() {} },
      setTimeout: schedule('timeout'), setInterval: schedule('interval'),
      clearTimeout: deny, clearInterval: deny,
      require(name) {
        if (name === 'crypto' || name === 'node:crypto') return require('node:crypto');
        if (name === 'express') return { Router: deny };
        if (/connectors\/authnet\/client$|core\/googleSheets$/.test(name)) return connector;
        return load(path.resolve(path.dirname(file), name + '.js'));
      }
    }, { filename: file });
    return module.exports;
  }
  return { load, timers, externalCalls: () => externalCalls };
}
for (const [name, key, file, entry, delay] of jobs) {
  test(name + ': maintenance flag suppresses startup even when legacy job flag enables it', () => {
    const f = fixture({ FF_FOUNDATION_MAINTENANCE:'true', [key]:'true' });
    const state=f.load(path.join(root,file))[entry]();
    assert.equal(state.started,false); assert.equal(f.timers.length,0);assert.equal(f.externalCalls(),0);
  });
  test(name + ': paused running process refuses an existing scheduled tick', async () => {
    const f=fixture({FF_SUBSCRIPTIONS_SPREADSHEET_ID:'synthetic'});
    f.load(path.join(root,file))[entry]();
    const gate=f.load(path.join(root,'core/maintenance.js')).maintenance;
    gate.pause(); await f.timers[0].callback();
    assert.equal(f.externalCalls(),0); assert.equal(gate.status().drained,true);
  });
  test(name + ': explicit false suppresses initial and recurring timers', () => {
    const f = fixture({ [key]: 'false' });
    const state = f.load(path.join(root, file))[entry]();
    assert.equal(state.started, false);
    assert.equal(state.running, false);
    assert.equal(f.timers.length, 0);
    assert.equal(f.externalCalls(), 0);
  });
  test(name + ': absent setting enables startup, not maintenance', () => {
    const f = fixture({});
    const state = f.load(path.join(root, file))[entry]();
    assert.equal(state.started, true);
    assert.deepEqual(f.timers.map(t => t.type), ['timeout', 'interval']);
    assert.equal(f.timers[0].ms, delay);
    assert.equal(f.externalCalls(), 0);
  });
  test(name + ': changing flag after startup does not drain or cancel timers', () => {
    const env = {}, f = fixture(env), start = f.load(path.join(root, file))[entry];
    const state = start(); env[key] = 'false';
    assert.equal(start(), state);
    assert.equal(state.started, true);
    assert.equal(f.timers.length, 2);
    assert.equal(f.externalCalls(), 0);
  });
}
