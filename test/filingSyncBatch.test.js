'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { buildFilingSyncBatch, applyFilingSyncBatch } = require('../src/core/filingSyncBatch');

function fixture() {
  return { filing: { sheetId: 0, rowIndex: 1, isNew: false, values: Array(20).fill('') },
    review: { sheetId: 2, removeRowIndexes: [1, 4], rows: [Array(14).fill('SYN-REVIEW')] },
    stamp: { sheetId: 3, rowIndex: 3, columnIndex: 2, value: '2026-09-28T00:00:00Z' } };
}

test('batch executor submits exactly one mutation with transport retries disabled', async () => {
  const calls = [];
  const sheets = { spreadsheets: { async batchUpdate(...args) { calls.push(args); return { data: {} }; } } };
  const result = await applyFilingSyncBatch(sheets, 'SYN-SHEET', fixture());
  assert.equal(calls.length, 1);
  assert.equal(calls[0][0].spreadsheetId, 'SYN-SHEET');
  assert.deepEqual(calls[0][0].requestBody.requests, buildFilingSyncBatch(fixture()).requests);
  assert.deepEqual(calls[0][1], { retry: false, retryConfig: { retry: 0, noResponseRetries: 0 } });
  assert.deepEqual(result, { success: true, filingAction: 'updated', reviewRemoved: 2, reviewWritten: 1 });
});

for (const committed of [false, true]) {
  test('batch executor treats rejected transport as ambiguous, committed=' + committed, async () => {
    let calls = 0, simulatedCommit = false;
    const sheets = { spreadsheets: { async batchUpdate() {
      calls++; simulatedCommit = committed;
      const error = Error('SYN-SENSITIVE-DETAIL'); error.response = { data: 'SYN-PRIVATE' }; throw error;
    } } };
    await assert.rejects(applyFilingSyncBatch(sheets, 'SYN-SHEET', fixture()), error => {
      assert.equal(error.code, 'SHEET_SYNC_RECONCILIATION_REQUIRED');
      assert.equal(error.requires_reconciliation, true);
      assert.equal(error.automatic_retry_allowed, false);
      assert.ok(!String(error).includes('SYN-SENSITIVE-DETAIL'));
      assert.equal(error.response, undefined);
      return true;
    });
    assert.equal(calls, 1); assert.equal(simulatedCommit, committed);
  });
}

test('invalid plan never reaches batch transport', async () => {
  let calls = 0;
  const sheets = { spreadsheets: { async batchUpdate() { calls++; } } };
  const input = fixture(); input.review.rows[0].pop();
  await assert.rejects(applyFilingSyncBatch(sheets, 'SYN-SHEET', input), /row width/);
  assert.equal(calls, 0);
});

test('one batch plans filing refresh, targeted clears, replacement append and stamp', () => {
  const input = fixture(), before = structuredClone(input);
  const plan = buildFilingSyncBatch(input);
  assert.deepEqual(input, before);
  assert.deepEqual(plan.counts, { filingAction: 'updated', reviewRemoved: 2, reviewWritten: 1 });
  assert.equal(plan.requests.length, 5);
  assert.deepEqual(plan.requests[0].updateCells.range, { sheetId: 0, startRowIndex: 1, endRowIndex: 2, startColumnIndex: 0, endColumnIndex: 16 });
  assert.deepEqual(plan.requests.slice(1, 3).map(r => r.updateCells.range.startRowIndex), [1, 4]);
  for (const request of plan.requests) {
    const body = request.updateCells || request.appendCells;
    assert.equal(body.fields, 'userEnteredValue');
    assert.equal(Object.keys(request).length, 1);
  }
  assert.equal(plan.requests[3].appendCells.sheetId, 2);
  assert.equal(plan.requests[4].updateCells.range.sheetId, 3);
});

test('new filing appends twenty cells without table-detection values.append', () => {
  const input = fixture(); input.filing.isNew = true; delete input.filing.rowIndex;
  const plan = buildFilingSyncBatch(input);
  assert.equal(plan.requests[0].appendCells.rows[0].values.length, 20);
  assert.equal(plan.counts.filingAction, 'appended');
});

test('provider formula-like strings remain literal while native numbers/booleans retain type', () => {
  const input = fixture(); input.review.rows[0][0] = '=SYN_FORMULA()';
  input.review.rows[0][1] = 12.5; input.review.rows[0][2] = false;
  const cells = buildFilingSyncBatch(input).requests[3].appendCells.rows[0].values;
  assert.deepEqual(cells.slice(0, 3), [
    { userEnteredValue: { stringValue: '=SYN_FORMULA()' } },
    { userEnteredValue: { numberValue: 12.5 } },
    { userEnteredValue: { boolValue: false } }
  ]);
});

test('empty replacement queue clears targeted rows without a separate empty append', () => {
  const input = fixture(); input.review.rows = [];
  const plan = buildFilingSyncBatch(input);
  assert.equal(plan.requests.length, 4);
  assert.ok(plan.requests.every(r => r.updateCells));
  assert.deepEqual(plan.requests[1].updateCells.rows[0].values, Array.from({ length: 14 }, () => ({})));
});

for (const [name, mutate] of [
  ['filing header overwrite', x => { x.filing.rowIndex = 0; }],
  ['review header overwrite', x => { x.review.removeRowIndexes = [0]; }],
  ['customer header overwrite', x => { x.stamp.rowIndex = 2; }],
  ['duplicate review coordinates', x => { x.review.removeRowIndexes = [1, 1]; }],
  ['colliding sheet identities', x => { x.stamp.sheetId = x.review.sheetId; }],
  ['ambiguous new row coordinates', x => { x.filing.isNew = true; }],
  ['implicit creation mode', x => { delete x.filing.isNew; }],
  ['wrong review width', x => { x.review.rows[0].push('EXTRA'); }],
  ['invalid sheet ID', x => { x.filing.sheetId = -1; }],
  ['nonfinite amount', x => { x.filing.values[6] = Infinity; }],
  ['unsupported object cell', x => { x.review.rows[0][4] = {}; }],
  ['sparse cells', x => { delete x.review.rows[0][4]; }],
  ['oversized cell', x => { x.review.rows[0][4] = 'x'.repeat(50001); }],
  ['missing stamp', x => { x.stamp.value = ''; }],
  ['oversized request count', x => { x.review.removeRowIndexes = Array.from({ length: 1000 }, (_, i) => i + 1); }],
  ['oversized byte budget', x => { x.review.rows = Array.from({ length: 3 }, () => Array(14).fill('x'.repeat(40000))); }]
]) {
  test('atomic plan rejects ' + name + ' before exposing a usable plan', () => {
    const input = fixture(); mutate(input);
    assert.throws(() => buildFilingSyncBatch(input));
  });
}
