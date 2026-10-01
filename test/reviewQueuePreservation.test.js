'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

// Execute the production helper, not a duplicate implementation. Transport is
// mocked; no Google authentication, real sheet, listener or automation starts.
const source = fs.readFileSync(path.join(__dirname, '../server.js'), 'utf8');
const start = source.indexOf('async function removeExistingReviewRowsForPeriod(');
const end = source.indexOf('\nfunction isTokenExpiringSoon(', start);
assert.ok(start >= 0 && end > start);
const context = { process: { env: { GOOGLE_SHEETS_SPREADSHEET_ID: 'SYN-SHEET' } } };
vm.createContext(context);
vm.runInContext(source.slice(start, end), context);
const remove = context.removeExistingReviewRowsForPeriod;

function row(customer, period, note = '') {
  return ['SYN-STATE', period, '', '', customer, '', '', '', '', note, 0, '', false, ''];
}

function fixture({ fail = false } = {}) {
  const rows = [row('Header', 'Period'), row('SYN-A', '2026-09'),
    row('SYN-B', '2026-09', '=SYN_FORMULA()'), row('SYN-A', '2026-08', 'Historical note'),
    row(' SYN-A ', ' 2026-09 '), row('SYN-B', '2026-08', 'Unrelated note')];
  const original = structuredClone(rows);
  const calls = [];
  const forbidden = async () => { throw new Error('Broad clear/rewrite forbidden'); };
  const sheets = { spreadsheets: { values: {
    async get(params) {
      assert.equal(params.spreadsheetId, 'SYN-SHEET');
      assert.equal(params.range, 'Review Queue!A:N');
      return { data: { values: structuredClone(rows) } };
    },
    clear: forbidden, update: forbidden, append: forbidden,
    async batchClear(params) {
      calls.push(params);
      if (fail) throw new Error('Synthetic transport failure');
      for (const range of params.requestBody.ranges) {
        const match = /^'Review Queue'!A(\d+):N\1$/.exec(range);
        assert.ok(match, range);
        rows[Number(match[1]) - 1] = [];
      }
    }
  } } };
  return { sheets, rows, original, calls };
}

test('review cleanup clears only matching customer-period rows without moving unrelated rows', async () => {
  const f = fixture();
  assert.equal(await remove(f.sheets, 'SYN-A', '2026-09'), 2);
  assert.equal(f.calls.length, 1);
  assert.equal(f.calls[0].spreadsheetId, 'SYN-SHEET');
  assert.deepEqual(Array.from(f.calls[0].requestBody.ranges), ["'Review Queue'!A2:N2", "'Review Queue'!A5:N5"]);
  for (const index of [0, 2, 3, 5]) assert.deepEqual(f.rows[index], f.original[index]);
  assert.deepEqual(f.rows[1], []);
  assert.deepEqual(f.rows[4], []);
  assert.equal(await remove(f.sheets, 'SYN-A', '2026-09'), 0);
  assert.equal(f.calls.length, 1);
});

test('review cleanup with no matching rows performs no mutation', async () => {
  const f = fixture();
  assert.equal(await remove(f.sheets, 'SYN-NO-MATCH', '2026-09'), 0);
  assert.deepEqual(f.rows, f.original);
  assert.equal(f.calls.length, 0);
});

test('review cleanup transport failure propagates without a broad fallback rewrite', async () => {
  const f = fixture({ fail: true });
  await assert.rejects(remove(f.sheets, 'SYN-A', '2026-09'), /Synthetic transport failure/);
  assert.deepEqual(f.rows, f.original);
  assert.equal(f.calls.length, 1);
});

test('review cleanup missing customer or period does not even read the sheet', async () => {
  assert.equal(await remove({}, '', '2026-09'), 0);
  assert.equal(await remove({}, 'SYN-A', ''), 0);
});
