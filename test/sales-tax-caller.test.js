const test = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');
const path = require('node:path');
const source = fs.readFileSync(path.join(__dirname, '../integrations/sales-tax/02_Sales_Data_Import.gs'), 'utf8');
function fixture(options = {}) {
  const calls = [], alerts = [];
  const values = [[], [], ['Customer ID', 'Square Merchant ID', 'Status', 'Square Connected'], ['AZ-TEST', 'merchant-test', 'Active', 'Yes']];
  const sheet = { getSheetId: () => 1, getDataRange: () => ({ getValues: () => options.values || values }), getActiveCell: () => ({ getRow: () => options.row || 4 }) };
  let prompt = 0;
  const context = vm.createContext({
    PropertiesService: { getScriptProperties: () => ({ getProperty: () => options.missingToken ? '' : 'synthetic-secret' }) },
    UrlFetchApp: { fetch: (url, init) => {
      calls.push({ url, init });
      if (options.throw) throw new Error('synthetic-secret');
      return { getResponseCode: () => options.code || 200, getContentText: () => options.body === undefined ? '{"success":true}' : options.body };
    } },
    SpreadsheetApp: {
      getActiveSpreadsheet: () => ({ getSheetByName: () => sheet, getActiveSheet: () => ({ getSheetId: () => options.wrongSheet ? 2 : 1 }) }),
      getUi: () => ({ Button: { OK: 'OK', YES: 'YES' }, ButtonSet: {},
        prompt: () => { const n = prompt++; return { getSelectedButton: () => 'OK', getResponseText: () => options.selected || n ? '2026-Q1' : 'AZ-TEST' }; },
        alert: (...args) => { alerts.push(args); return 'YES'; }
      })
    }
  });
  vm.runInContext(source, context);
  return { context, calls, alerts, values };
}
test('POST query contract, pinned origin, header-only secret and no redirects', () => {
  const f = fixture(); f.context.ffSqPush_('AZ-TEST &x=1', '2026 Q1');
  assert.equal(f.calls.length, 1);
  const { url, init } = f.calls[0];
  assert.equal(new URL(url).origin, 'https://fastfilings-api.onrender.com');
  assert.equal(new URL(url).searchParams.get('customer_id'), 'AZ-TEST &x=1');
  assert.equal(init.method, 'post'); assert.equal(init.followRedirects, false);
  assert.equal(init.headers['x-ff-sync-token'], 'synthetic-secret');
  assert.ok(!url.includes('synthetic-secret'));
});
test('missing token fails before dispatch', () => {
  const f = fixture({ missingToken: true }); assert.throws(() => f.context.ffSqPush_('AZ-TEST', '2026-Q1'), /not configured/); assert.equal(f.calls.length, 0);
});
for (const options of [{throw:true}, {code:500}, {code:302}, {body:'synthetic-secret'}, {body:'{"success":"true"}'}, {code:401}, {code:405}]) {
  test('failure is sanitized and never retried: ' + JSON.stringify(options), () => {
    const f = fixture(options);
    assert.throws(() => f.context.ffSqPush_('AZ-TEST', '2026-Q1'), error => !error.message.includes('synthetic-secret'));
    assert.equal(f.calls.length, 1);
  });
}
for (const selected of [false, true]) {
  test('menu path uses merchant header without local writes: selected=' + selected, () => {
    const f = fixture({ selected });
    f.context[selected ? 'runSalesDataSQSelectedRow' : 'runSalesDataSQ']();
    assert.equal(f.calls.length, 1);
    assert.equal(f.alerts.at(-1)[0], 'Run complete');
    // Sheet mock exposes no write API: stale-row setValue would fail this test.
  });
}
test('wrong selected tab and out-of-range row do not dispatch', () => {
  for (const options of [{wrongSheet:true}, {row:3}, {row:20}]) {
    const f = fixture(options); f.context.runSalesDataSQSelectedRow(); assert.equal(f.calls.length, 0); assert.equal(f.alerts.at(-1)[0], 'Run stopped');
  }
});
test('duplicate customer and ambiguous merchant headers fail closed', () => {
  const f = fixture();
  assert.throws(() => f.context.ffSqCustomer_(f.values.concat([f.values[3]]), 'AZ-TEST'), /exactly one/);
  f.values[2].push('Square Customer ID');
  assert.throws(() => f.context.ffSqCustomer_(f.values, 'AZ-TEST'), /ambiguous/);
});
test('prefix avoids collisions with existing normalizeHeader helpers', () => {
  const f = fixture(); f.context.normalizeHeader_ = () => { throw new Error('legacy helper invoked'); };
  assert.equal(f.context.ffSqCustomer_(f.values, 'AZ-TEST').merchant, 'merchant-test');
});
