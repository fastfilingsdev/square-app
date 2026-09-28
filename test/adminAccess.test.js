'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { hasValidAdminToken, internalAdminHeaders } = require('../src/core/adminAccess');
const { FILING_HEADERS, REVIEW_HEADERS } = require('../src/core/filingSyncPreflight');
const syntheticMetadata = { sheets: ['Filings', 'Review Queue', 'Customers'].map((title, i) => ({
  properties: { title, sheetId: i, sheetType: 'GRID', gridProperties: { rowCount: 100, columnCount: 29 } }
})) };
const typedValue = cell => cell.userEnteredValue ? Object.values(cell.userEnteredValue)[0] : '';

function request(headers = {}) {
  return { query: {}, get: key => headers[key.toLowerCase()] || '' };
}

function response() {
  return {
    code: 200, headers: {}, payload: undefined,
    set(key, value) { this.headers[key] = value; return this; },
    status(code) { this.code = code; return this; },
    json(payload) { this.payload = payload; return this; }
  };
}

test('admin authentication fails closed and never accepts a token in query parameters', () => {
  assert.equal(hasValidAdminToken(request(), {}), false);
  assert.equal(hasValidAdminToken(request(), { FF_SYNC_ADMIN_TOKEN: 'test-secret' }), false);
  assert.equal(hasValidAdminToken(request({ 'x-ff-sync-token': 'wrong' }), { FF_SYNC_ADMIN_TOKEN: 'test-secret' }), false);
  const req = request(); req.query.token = 'test-secret';
  assert.equal(hasValidAdminToken(req, { FF_SYNC_ADMIN_TOKEN: 'test-secret' }), false);
});

test('admin token supports existing header/bearer formats and honors primary-secret precedence', () => {
  const env = { FF_SYNC_ADMIN_TOKEN: 'primary-test', AUTHNET_SYNC_TOKEN: 'fallback-test' };
  for (const headers of [
    { 'x-ff-sync-token': 'primary-test' },
    { 'x-authnet-sync-token': 'primary-test' },
    { authorization: 'Bearer primary-test' }
  ]) assert.equal(hasValidAdminToken(request(headers), env), true);
  assert.equal(hasValidAdminToken(request({ authorization: 'Bearer fallback-test' }), env), false);
  assert.equal(hasValidAdminToken(request({ 'x-ff-sync-token': 'fallback-test' }), { AUTHNET_SYNC_TOKEN: 'fallback-test' }), true);
});

test('fixed-loopback requests obtain configured credentials or fail closed', () => {
  assert.throws(() => internalAdminHeaders({}), /not configured/);
  assert.deepEqual(internalAdminHeaders({ AUTHNET_SYNC_TOKEN: 'test-secret' }), { 'x-ff-sync-token': 'test-secret' });
});

function loadServerRoutes({ env = {}, axiosGet, google } = {}) {
  const routes = new Map();
  let externalCalls = 0;
  const forbidden = () => { externalCalls++; throw new Error('External access forbidden in regression tests'); };
  const app = {
    use() {},
    get(route, handler) { routes.set('GET ' + route, handler); },
    post(route, handler) { routes.set('POST ' + route, handler); },
    listen() {} // Never start the real server or its scheduled jobs.
  };
  const express = () => app;
  express.json = () => () => {};
  express.static = () => () => {};
  const core = { exports: {} };
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, '../src/core/adminAccess.js'), 'utf8'), {
    module: core, Buffer, require, process: { env }
  });
  const features = new Proxy({}, { get: () => () => ({}) });
  const source = fs.readFileSync(path.join(__dirname, '../server.js'), 'utf8');
  vm.runInNewContext(source, {
    process: { env }, console: { log() {}, error() {} }, URL, Buffer,
    __dirname: path.join(__dirname, '..'),
    require(name) {
      if (name === 'dotenv') return { config() {} };
      if (name === 'express') return express;
      if (name === 'axios') return { get: axiosGet || forbidden, post: forbidden };
      if (name === 'googleapis') return { google: google || { auth: { GoogleAuth: forbidden }, sheets: forbidden } };
      if (name === './src/core/adminAccess') return core.exports;
      if (['./src/core/filingSyncPreflight', './src/core/filingSyncBatch', './src/core/filingTotals', './src/core/refundServiceRuntime'].includes(name)) return require('../' + name);
      if (name.startsWith('./src/features/')) return features;
      if (['crypto', 'path'].includes(name)) return require(name);
      throw new Error('Unexpected test dependency: ' + name);
    }
  });
  return { routes, externalCalls: () => externalCalls };
}

for (const [label, env, headers] of [
  ['primary header', { FF_SYNC_ADMIN_TOKEN: 'synthetic-primary' }, { 'x-ff-sync-token': 'synthetic-primary' }],
  ['legacy header', { AUTHNET_SYNC_TOKEN: 'synthetic-legacy' }, { 'x-authnet-sync-token': 'synthetic-legacy' }],
  ['bearer', { FF_SYNC_ADMIN_TOKEN: 'synthetic-primary' }, { authorization: 'Bearer synthetic-primary' }]
]) {
  test(`authenticated filing summary preserves query contract and loopback credentials: ${label}`, async () => {
    const calls = [];
    const { routes, externalCalls } = loadServerRoutes({ env, axiosGet: async (url, options) => {
      calls.push({ url, options });
      return { data: { orders: [{ tax_collected: 10, line_items: [
        { total: 110, tax: 10, classification_status: 'taxable' },
        { total: 20, tax: 0, classification_status: 'non_taxable' },
        { total: 30, tax: 0, classification_status: 'needs_review' }
      ] }] } };
    } });
    const req = request(headers);
    req.query = { period: '2026-09', customer_id: 'SYN-CUSTOMER', location_id: 'SYN-LOCATION' };
    const res = response();
    await routes.get('GET /filing-summary')(req, res);
    assert.equal(res.code, 200);
    assert.equal(calls.length, 1);
    assert.match(calls[0].url, /^http:\/\/localhost:\d+\/classification-layer$/);
    assert.deepEqual(JSON.parse(JSON.stringify(calls[0].options.params)), {
      ...req.query, start: '2026-09-01', end: '2026-09-30'
    });
    assert.equal(calls[0].options.headers['x-ff-sync-token'], env.FF_SYNC_ADMIN_TOKEN || env.AUTHNET_SYNC_TOKEN);
    assert.deepEqual(JSON.parse(JSON.stringify(res.payload.totals)), {
      gross_sales_before_tax: 150, gross_sales_including_tax: 160, taxable_sales: 110,
      non_taxable_sales: 20, needs_review_sales: 30, tax_collected: 10, review_count: 1
    });
    assert.equal(externalCalls(), 0);
  });
}

for (const { locked, duplicateLocked } of [
  { locked: true }, { locked: false },
  { locked: false, duplicateLocked: false }, { locked: false, duplicateLocked: true }
]) {
test(`authenticated POST preserves filing controls (locked=${locked}, duplicateLocked=${duplicateLocked})`, async () => {
  const duplicated = duplicateLocked !== undefined;
  const calls = [];
  const reads = [];
  let mutations = 0;
  const forbiddenWrite = async () => { mutations++; throw new Error('Unexpected write'); };
  const lockedRow = Array(20).fill('');
  lockedRow[1] = '2026-09'; lockedRow[3] = 'SYN-CUSTOMER';
  lockedRow.splice(16, 4, locked, 'SYN-REVIEWER', 'SYN-NOTE', 'SYN-CONTROL');
  const originalControls = lockedRow.slice(16);
  const { routes, externalCalls } = loadServerRoutes({
    env: { FF_SYNC_ADMIN_TOKEN: 'synthetic-primary', GOOGLE_SHEETS_SPREADSHEET_ID: 'SYN-SHEET',
      GOOGLE_SERVICE_ACCOUNT_EMAIL: 'synthetic@example.invalid', GOOGLE_PRIVATE_KEY: 'synthetic-not-a-key' },
    axiosGet: async (url, options) => {
      calls.push({ url, options });
      return { data: { totals: {}, counts: {}, orders: [] } };
    },
    google: {
      auth: { GoogleAuth: class { async getClient() { return {}; } } },
      sheets: () => ({ spreadsheets: {
        async get() { return { data: syntheticMetadata }; },
        async batchUpdate({ requestBody }, options) {
          mutations++;
          assert.equal(locked, false);
          assert.equal(options.retry, false);
          assert.equal(requestBody.requests.length, 2);
          const update = requestBody.requests[0].updateCells;
          assert.equal(update.range.startRowIndex, 3);
          assert.equal(update.range.endColumnIndex, 16);
          lockedRow.splice(0, 16, ...update.rows[0].values.map(typedValue));
        },
        values: { update: forbiddenWrite, append: forbiddenWrite, clear: forbiddenWrite,
        async get({ spreadsheetId, range }) {
          assert.equal(spreadsheetId, 'SYN-SHEET'); reads.push(range);
          if (range === 'Customers!A:Z') return { data: { values: [[], [],
            ['Customer ID', 'Square Merchant ID', 'Last Sync'], ['SYN-CUSTOMER', 'SYN-MERCHANT', 'OLD']] } };
          if (range === 'Filings!A:AC') {
            const rows = [['Filings'], [], Array.from(FILING_HEADERS), lockedRow];
            if (duplicated) {
              const duplicate = [...lockedRow]; duplicate[16] = duplicateLocked;
              rows.push(duplicate);
            }
            return { data: { values: rows } };
          }
          if (range === "'Review Queue'!A:AA") return { data: { values: [['Review Queue'], [], Array.from(REVIEW_HEADERS)] } };
          throw new Error('Unexpected read: ' + range);
        }
      } } })
    }
  });
  const req = request({ 'x-ff-sync-token': 'synthetic-primary' });
  req.query = { period: '2026-09', customer_id: 'SYN-CUSTOMER', location_id: 'SYN-LOCATION' };
  const res = response();
  await routes.get('POST /push-to-sheets')(req, res);
  assert.equal(res.code, duplicated ? 409 : locked ? 400 : 200);
  if (duplicated) assert.match(res.payload.error, /Multiple filings/);
  else if (locked) assert.match(res.payload.error, /LOCKED/);
  else {
    assert.equal(res.payload.success, true);
    assert.equal(res.payload.filings_action, 'updated');
    assert.equal(res.payload.review_rows_written, 0);
  }
  assert.equal(calls.length, 1);
  assert.match(calls[0].url, /\/classification-layer$/);
  for (const { options } of calls) {
    assert.equal(options.headers['x-ff-sync-token'], 'synthetic-primary');
    assert.equal(options.params.customer_id, 'SYN-CUSTOMER');
    assert.equal(options.params.start, '2026-09-01');
    assert.equal(options.params.end, '2026-09-30');
  }
  assert.deepEqual(reads, ['Customers!A:Z', 'Filings!A:AC', "'Review Queue'!A:AA", 'Customers!A:Z']);
  assert.deepEqual(lockedRow.slice(16), originalControls);
  assert.equal(mutations, locked || duplicated ? 0 : 1);
  assert.equal(externalCalls(), 0);
});
}

for (const scenario of ['missing-id', 'array-id', 'unmatched', 'duplicate', 'blank-merchant', 'unknown-merchant', 'read-failure', 'foreign-order']) {
  test(`sheet update fails closed on unverified customer identity: ${scenario}`, async () => {
    let reads = 0;
    let writes = 0;
    let reports = 0;
    const mutation = async () => { writes++; throw new Error('Unexpected sheet mutation'); };
    const { routes, externalCalls } = loadServerRoutes({
      env: { FF_SYNC_ADMIN_TOKEN: 'synthetic-primary', GOOGLE_SHEETS_SPREADSHEET_ID: 'SYN-SHEET',
        GOOGLE_SERVICE_ACCOUNT_EMAIL: 'synthetic@example.invalid', GOOGLE_PRIVATE_KEY: 'synthetic-not-a-key' },
      axiosGet: async () => {
        reports++;
        return { data: { orders: [{ square_customer_id: 'OTHER-MERCHANT' }], totals: {} } };
      },
      google: {
        auth: { GoogleAuth: class { async getClient() { return {}; } } },
        sheets: () => ({ spreadsheets: { batchUpdate: mutation, values: {
          append: mutation, update: mutation, clear: mutation, batchClear: mutation,
          async get({ range }) {
            reads++;
            assert.equal(range, 'Customers!A:Z');
            if (scenario === 'read-failure') throw new Error('Synthetic failed lookup');
            const rows = [[], [], ['Customer ID', 'Square Merchant ID']];
            rows.push([scenario === 'unmatched' ? 'OTHER-CUSTOMER' : 'SYN-CUSTOMER',
              scenario === 'blank-merchant' ? ' ' : scenario === 'unknown-merchant' ? 'UNKNOWN' : 'SYN-MERCHANT']);
            if (scenario === 'duplicate') rows.push(['SYN-CUSTOMER', 'OTHER-MERCHANT']);
            return { data: { values: rows } };
          }
        } } })
      }
    });
    const req = request({ 'x-ff-sync-token': 'synthetic-primary' });
    req.query = { period: '2026-09', customer_id: scenario === 'missing-id' ? '' :
      scenario === 'array-id' ? ['SYN-CUSTOMER', 'OTHER-CUSTOMER'] : 'SYN-CUSTOMER' };
    const res = response();
    await routes.get('POST /push-to-sheets')(req, res);
    const invalidInput = ['missing-id', 'array-id'].includes(scenario);
    assert.equal(res.code, invalidInput ? 400 : 409);
    assert.equal(reads, invalidInput ? 0 : 1);
    assert.equal(reports, scenario === 'foreign-order' ? 1 : 0);
    assert.equal(writes, 0);
    assert.equal(externalCalls(), 0);
  });
}

// Actual route uses one atomic batch. Mock each validation refusal before
// commit, and separately model a response lost after the complete commit.
for (const failure of ['none', 'filing', 'clear', 'append', 'append-after-commit', 'stamp']) {
  test(`atomic route failure preserves unrelated data without automatic retry: ${failure}`, async () => {
    const operations = [];
    let reportCalls = 0;
    const filing = Array(20).fill('');
    filing[1] = '2026-09'; filing[3] = 'SYN-CUSTOMER';
    filing.splice(16, 4, false, 'SYN-REVIEWER', 'SYN-NOTE', 'SYN-CONTROL');
    const originalFiling = structuredClone(filing);
    const oldReview = Array(14).fill('');
    oldReview[1] = '2026-09'; oldReview[4] = 'SYN-CUSTOMER';
    oldReview[5] = 'SYN-MERCHANT';
    const unrelated = Array(14).fill('SYN-UNRELATED');
    unrelated[4] = 'OTHER-CUSTOMER';
    const queue = [['Review Queue'], [], Array.from(REVIEW_HEADERS), oldReview, unrelated];
    const originalQueue = structuredClone(queue);
    const customers = [[], [], ['Customer ID', 'Square Merchant ID', 'Last Sync'],
      ['SYN-CUSTOMER', 'SYN-MERCHANT', 'SYN-PRIOR-STAMP']];
    const { routes, externalCalls } = loadServerRoutes({
      env: { FF_SYNC_ADMIN_TOKEN: 'synthetic-primary', GOOGLE_SHEETS_SPREADSHEET_ID: 'SYN-SHEET',
        GOOGLE_SERVICE_ACCOUNT_EMAIL: 'synthetic@example.invalid', GOOGLE_PRIVATE_KEY: 'synthetic-not-a-key' },
      axiosGet: async url => { reportCalls++; assert.match(url, /\/classification-layer$/); return { data: { totals: { review_count: 999 }, counts: { orders: 1 },
        orders: [{ square_customer_id: ' SYN-MERCHANT ', order_id: 'SYN-ORDER', line_items: [
          { classification_status: 'future_unknown_status', order_name: 'Synthetic item', total: 25 }
        ] }] } }; },
      google: {
        auth: { GoogleAuth: class { async getClient() { return {}; } } },
        sheets: () => ({ spreadsheets: {
          async get() { return { data: syntheticMetadata }; },
          async batchUpdate({ requestBody }, options) {
            operations.push('atomic_batch');
            assert.equal(options.retry, false);
            assert.equal(options.retryConfig.noResponseRetries, 0);
            assert.equal(requestBody.requests.length, 4);
            if (!['none', 'append-after-commit'].includes(failure)) throw new Error('Synthetic refusal: ' + failure);
            const [filingRequest, clearRequest, appendRequest, stampRequest] = requestBody.requests;
            assert.equal(filingRequest.updateCells.range.endColumnIndex, 16);
            const filingValues = filingRequest.updateCells.rows[0].values.map(typedValue);
            assert.equal(filingValues[6], 25);
            assert.equal(filingValues[10], 25);
            assert.equal(filingValues[12], 1);
            assert.equal(filingValues[15], 'Needs Review');
            assert.equal(clearRequest.updateCells.range.startRowIndex, 3);
            assert.equal(clearRequest.updateCells.range.endColumnIndex, 14);
            assert.equal(appendRequest.appendCells.sheetId, 1);
            assert.equal(stampRequest.updateCells.range.sheetId, 2);
            filing.splice(0, 16, ...filingRequest.updateCells.rows[0].values.map(typedValue));
            queue[3] = Array(14).fill('');
            queue.push(appendRequest.appendCells.rows[0].values.map(typedValue));
            customers[3][2] = typedValue(stampRequest.updateCells.rows[0].values[0]);
            if (failure === 'append-after-commit') throw new Error('Synthetic lost response');
          },
          values: {
          async get({ range }) {
            const rows = range === 'Customers!A:Z' ? customers : range === 'Filings!A:AC' ?
              [['Filings'], [], Array.from(FILING_HEADERS), filing] : range === "'Review Queue'!A:AA" ? queue : null;
            assert.ok(rows, range);
            return { data: { values: structuredClone(rows) } };
          },
          async update() { operations.push('FORBIDDEN update'); throw new Error('Sequential write forbidden'); },
          async batchClear() { operations.push('FORBIDDEN clear'); throw new Error('Sequential write forbidden'); },
          async append() { operations.push('FORBIDDEN append'); throw new Error('Sequential write forbidden'); }
        } } })
      }
    });
    const req = request({ 'x-ff-sync-token': 'synthetic-primary' });
    req.query = { period: '2026-09', customer_id: 'SYN-CUSTOMER' };
    const res = response();
    await routes.get('POST /push-to-sheets')(req, res);
    assert.equal(res.code, failure === 'none' ? 200 : 500);
    const committed = ['none', 'append-after-commit'].includes(failure);
    assert.deepEqual(operations, ['atomic_batch']);
    assert.equal(reportCalls, 1);
    assert.deepEqual(filing.slice(16), originalFiling.slice(16));
    if (!committed) assert.deepEqual(filing, originalFiling);
    assert.deepEqual(queue.slice(0, 3), originalQueue.slice(0, 3));
    assert.deepEqual(queue[4], originalQueue[4]);
    assert.deepEqual(queue[3], committed ? Array(14).fill('') : oldReview);
    assert.equal(queue.length, committed ? 6 : 5);
    assert.equal(customers[3][2] === 'SYN-PRIOR-STAMP', !committed);
    if (failure !== 'none') {
      assert.notEqual(res.payload.success, true);
      assert.equal(res.payload.code, 'SHEET_SYNC_RECONCILIATION_REQUIRED');
      assert.equal(res.payload.requires_reconciliation, true);
      assert.equal(res.payload.automatic_retry_allowed, false);
      assert.equal(res.payload.stage, 'atomic_batch');
      assert.match(res.payload.error, /may already have changed/);
      assert.ok(!JSON.stringify(res.payload).includes('Synthetic failure'));
    } else {
      assert.equal(res.payload.success, true);
      assert.equal(res.payload.review_rows_removed, 1);
      assert.equal(res.payload.review_rows_written, 1);
      assert.equal(res.payload.customer_last_sync_action, 'updated');
    }
    assert.equal(externalCalls(), 0);
  });
}

for (const scenario of ['new-filing', 'wrong-schema', 'mapping-changed', 'formula-stamp', 'metadata-failure', 'unknown-lock', 'invalid-amount', 'string-amount', 'foreign-old-review', 'unverified-old-review']) {
  test(`actual atomic route preflight: ${scenario}`, async () => {
    let batches = 0; let sequential = 0; let customerReads = 0;
    const customers = [[], [], ['Customer ID', 'Square Merchant ID', 'Last Sync'],
      ['SYN-CUSTOMER', 'SYN-MERCHANT', 'OLD']];
    const filings = [['Filings'], [], Array.from(FILING_HEADERS)];
    const review = [['Review Queue'], [], Array.from(REVIEW_HEADERS)];
    if (['foreign-old-review', 'unverified-old-review'].includes(scenario)) {
      const row = Array(14).fill(''); row[1] = '2026-09'; row[4] = 'SYN-CUSTOMER';
      row[5] = scenario === 'foreign-old-review' ? 'OTHER-MERCHANT' : '';
      review.push(row);
    }
    if (scenario === 'wrong-schema') filings[2][3] = 'Other';
    if (scenario === 'formula-stamp') customers[3][2] = '=NOW()';
    if (scenario === 'unknown-lock') {
      const row = Array(20).fill(''); row[1] = '2026-09'; row[3] = 'SYN-CUSTOMER'; row[16] = 'yes'; filings.push(row);
    }
    const mutation = async () => { sequential++; throw new Error('Sequential fallback forbidden'); };
    const { routes } = loadServerRoutes({
      env: { FF_SYNC_ADMIN_TOKEN: 'synthetic-primary', GOOGLE_SHEETS_SPREADSHEET_ID: 'SYN-SHEET',
        GOOGLE_SERVICE_ACCOUNT_EMAIL: 'synthetic@example.invalid', GOOGLE_PRIVATE_KEY: 'synthetic-not-a-key' },
      axiosGet: async () => ({ data: { totals: { gross_sales_before_tax: scenario === 'invalid-amount' ? Infinity : 12 },
        counts: {}, orders: ['invalid-amount', 'string-amount'].includes(scenario)
          ? [{ line_items: [{ total: scenario === 'string-amount' ? '12.50' : Infinity }] }] : [] } }),
      google: {
        auth: { GoogleAuth: class { async getClient() { return {}; } } },
        sheets: () => ({ spreadsheets: {
          async get() {
            if (scenario === 'metadata-failure') throw Error('SYN-SECRET-UPSTREAM');
            return { data: syntheticMetadata };
          },
          async batchUpdate({ requestBody }, options) {
            batches++;
            assert.equal(customerReads, 2);
            assert.equal(options.retry, false);
            assert.equal(requestBody.requests.length, 2);
            assert.equal(requestBody.requests[0].appendCells.sheetId, 0);
            assert.equal(requestBody.requests[0].appendCells.rows[0].values.length, 20);
            assert.equal(requestBody.requests[1].updateCells.range.startRowIndex, 3);
          },
          values: { update: mutation, append: mutation, batchClear: mutation,
            async get({ range, valueRenderOption }) {
              if (range === 'Customers!A:Z') {
                customerReads++;
                const values = structuredClone(customers);
                if (scenario === 'mapping-changed' && customerReads === 2) values[3][1] = 'OTHER';
                return { data: { values } };
              }
              assert.equal(valueRenderOption, 'FORMULA');
              return { data: { values: range === 'Filings!A:AC' ? filings : review } };
            }
          }
        } })
      }
    });
    const req = request({ 'x-ff-sync-token': 'synthetic-primary' });
    req.query = { customer_id: 'SYN-CUSTOMER', period: '2026-09' };
    const res = response(); await routes.get('POST /push-to-sheets')(req, res);
    assert.equal(sequential, 0);
    assert.equal(batches, scenario === 'new-filing' ? 1 : 0);
    if (scenario === 'new-filing') {
      assert.equal(res.code, 200);
      assert.equal(res.payload.filings_action, 'appended');
      assert.equal(res.payload.customer_last_sync_action, 'updated');
    } else {
      assert.equal(res.code, ['metadata-failure', 'invalid-amount', 'string-amount'].includes(scenario) ? 500 : 409);
      assert.equal(res.payload.requires_reconciliation, false);
      assert.equal(res.payload.automatic_retry_allowed, false);
      assert.ok(!JSON.stringify(res.payload).includes('SYN-SECRET-UPSTREAM'));
    }
  });
}

test('preflight report failure is distinguished from attempted writes and does not expose upstream data', async () => {
  let mutations = 0;
  const forbiddenWrite = async () => { mutations++; throw Error('Unexpected write'); };
  const { routes } = loadServerRoutes({
    env: { FF_SYNC_ADMIN_TOKEN: 'synthetic-primary', GOOGLE_SHEETS_SPREADSHEET_ID: 'SYN-SHEET',
      GOOGLE_SERVICE_ACCOUNT_EMAIL: 'synthetic@example.invalid', GOOGLE_PRIVATE_KEY: 'synthetic-not-a-key' },
    axiosGet: async () => {
      const error = Error('SYN-SENSITIVE-UPSTREAM');
      error.response = { data: { secret: 'SYN-SENSITIVE-UPSTREAM', customer: 'SYN-PRIVATE' } };
      throw error;
    },
    google: {
      auth: { GoogleAuth: class { async getClient() { return {}; } } },
      sheets: () => ({ spreadsheets: { values: {
        async get({ range }) {
          assert.equal(range, 'Customers!A:Z');
          return { data: { values: [[], [], ['Customer ID', 'Square Merchant ID'], ['SYN-CUSTOMER', 'SYN-MERCHANT']] } };
        },
        update: forbiddenWrite, append: forbiddenWrite, batchClear: forbiddenWrite
      } } })
    }
  });
  const req = request({ 'x-ff-sync-token': 'synthetic-primary' });
  req.query = { customer_id: 'SYN-CUSTOMER', period: '2026-09' };
  const res = response();
  await routes.get('POST /push-to-sheets')(req, res);
  assert.equal(res.code, 500);
  assert.equal(res.payload.code, 'SHEET_SYNC_PREFLIGHT_FAILED');
  assert.equal(res.payload.stage, 'preflight');
  assert.equal(res.payload.requires_reconciliation, false);
  assert.equal(res.payload.automatic_retry_allowed, false);
  assert.equal(mutations, 0);
  assert.ok(!JSON.stringify(res.payload).includes('SYN-SENSITIVE-UPSTREAM'));
  assert.ok(!JSON.stringify(res.payload).includes('SYN-PRIVATE'));
});

test('every sensitive reporting handler denies unauthenticated requests before external access', async () => {
  const { routes, externalCalls } = loadServerRoutes();
  for (const route of [
    'GET /pull-sales', 'GET /locations', 'GET /sales-summary', 'GET /sales-tax-ready',
    'GET /orders-tax-engine', 'GET /catalog-sync', 'GET /catalog-enriched-orders',
    'GET /classification-layer', 'GET /filing-summary', 'POST /push-to-sheets',
    'GET /clover-payments'
  ]) {
    assert.ok(routes.has(route), route);
    const res = response();
    await routes.get(route)(request(), res);
    assert.equal(res.code, 401, route);
    assert.equal(res.headers['Cache-Control'], 'no-store', route);
  }
  assert.equal(externalCalls(), 0);
});

test('legacy GET cannot write sheets, and debug endpoint exposes no configuration', async () => {
  const { routes, externalCalls } = loadServerRoutes();
  const writeRes = response();
  await routes.get('GET /push-to-sheets')(request(), writeRes);
  assert.equal(writeRes.code, 405);
  assert.equal(writeRes.headers.Allow, 'POST');
  const debugRes = response();
  await routes.get('GET /debug-env')(request(), debugRes);
  assert.equal(debugRes.code, 404);
  assert.equal(JSON.stringify(debugRes.payload), '{"ok":false,"error":"Not found"}');
  assert.equal(externalCalls(), 0);
});

test('authenticated Clover caller cannot redirect stored provider credentials to a query-supplied host', async () => {
  const { routes, externalCalls } = loadServerRoutes({ env: { FF_SYNC_ADMIN_TOKEN: 'synthetic-primary' } });
  const req = request({ 'x-ff-sync-token': 'synthetic-primary' });
  req.query = { base_url: 'https://untrusted.example.invalid', merchant_id: 'SYN-MERCHANT' };
  const res = response();
  await routes.get('GET /clover-payments')(req, res);
  assert.equal(res.code, 400);
  assert.match(res.payload.error, /configured provider URL only/);
  assert.equal(externalCalls(), 0);
});

test('all existing loopback reporting calls attach admin authentication; no token fragments are logged', () => {
  const source = fs.readFileSync(path.join(__dirname, '../server.js'), 'utf8');
  const calls = [...source.matchAll(/axios\.get\(`http:\/\/localhost:[\s\S]*?\n    \}\);/g)];
  assert.equal(calls.length, 2);
  calls.forEach(([call]) => assert.match(call, /headers: internalAdminHeaders\(\)/));
  assert.doesNotMatch(source, /access_token_prefix|accessTokenPrefix|googlePrivateKeyPrefix|clientIdPrefix/);
});

test('payment-link prepare explicitly responds 401 rather than hanging on denied access', async () => {
  const routes = new Map();
  const router = { get(route, fn) { routes.set('GET ' + route, fn); }, post(route, fn) { routes.set('POST ' + route, fn); } };
  const mod = { exports: {} };
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, '../src/features/billingPaymentLinks/routes.js'), 'utf8'), {
    module: mod,
    require(name) {
      if (name === 'express') return { Router: () => router };
      if (name === '../billingRefunds/routes') return { hasValidBillingAccess: async () => false };
      return {};
    }
  });
  mod.exports.createBillingPaymentLinksRouter();
  const res = response();
  await routes.get('POST /payment-links/prepare')(request(), res);
  assert.equal(res.code, 401);
  assert.equal(res.payload.ok, false);
});
