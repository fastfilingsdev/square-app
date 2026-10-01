'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { FILING_HEADERS, REVIEW_HEADERS, prepareFilingSyncInput, readFilingSyncInput } = require('../src/core/filingSyncPreflight');
const { buildFilingSyncBatch } = require('../src/core/filingSyncBatch');

function fixture() {
  const filingValues = Array(20).fill('');
  filingValues[1] = '2026-09'; filingValues[3] = 'SYN-CUSTOMER'; filingValues[6] = 12;
  const reviewRow = Array(14).fill('');
  reviewRow[1] = '2026-09'; reviewRow[4] = 'SYN-CUSTOMER'; reviewRow[5] = 'SYN-MERCHANT';
  return {
    metadata: { sheets: ['Filings', 'Review Queue', 'Customers'].map((title, i) => ({ properties: {
      title, sheetId: i, sheetType: 'GRID', gridProperties: { rowCount: 20, columnCount: 29 }
    } })) },
    filings: [['Filings'], [], Array.from(FILING_HEADERS), [...filingValues, 'KEEP NOTES']],
    review: [['Review Queue'], [], Array.from(REVIEW_HEADERS), reviewRow, ['OTHER']],
    customers: [['Customers'], [], ['Customer ID', 'Square Merchant ID', 'Last Sync'],
      ['SYN-CUSTOMER', 'SYN-MERCHANT', 'OLD']],
    customerId: 'SYN-CUSTOMER', merchantId: 'SYN-MERCHANT', period: '2026-09',
    filingValues, reviewRows: [Array.from(reviewRow)], stampValue: '2026-09-28T02:05:10Z'
  };
}

test('observed row-three layout resolves exact zero-based coordinates without mutation', () => {
  const data = fixture(); const before = structuredClone(data);
  const input = prepareFilingSyncInput(data);
  assert.equal(input.filing.sheetId, 0);
  assert.equal(input.filing.rowIndex, 3);
  assert.equal(input.filing.isNew, false);
  assert.deepEqual(input.review.removeRowIndexes, [3]);
  assert.equal(input.stamp.rowIndex, 3);
  assert.equal(input.stamp.columnIndex, 2);
  assert.deepEqual(data, before);
  const plan = buildFilingSyncBatch(input);
  assert.equal(plan.requests.length, 4);
  assert.equal(plan.requests[0].updateCells.range.endColumnIndex, 16);
});

test('new filing and empty review do not touch title or header rows', () => {
  const data = fixture(); data.filings.pop(); data.reviewRows = []; data.review.splice(3);
  const input = prepareFilingSyncInput(data);
  assert.equal(input.filing.isNew, true);
  assert.equal(input.filing.rowIndex, null);
  assert.deepEqual(input.review.removeRowIndexes, []);
  assert.equal(buildFilingSyncBatch(input).requests.length, 2);
});

for (const [name, mutate, code] of [
  ['row-one fixture mistaken for live headers', x => { x.filings = [Array.from(FILING_HEADERS), x.filings[3]]; }, 'SCHEMA_MISMATCH'],
  ['reordered filing column', x => { x.filings[2][6] = 'Other'; }, 'SCHEMA_MISMATCH'],
  ['reordered review column', x => { x.review[2][4] = 'Other'; }, 'SCHEMA_MISMATCH'],
  ['duplicate header after standard columns', x => { x.filings[2].push('Customer ID'); }, 'SCHEMA_MISMATCH'],
  ['ambiguous customer aliases', x => { x.customers[2].push('Internal Customer ID'); }, 'SCHEMA_MISMATCH'],
  ['missing last sync column', x => { x.customers[2][2] = 'Other'; }, 'SCHEMA_MISMATCH'],
  ['missing target sheet', x => { x.metadata.sheets.pop(); }, 'SCHEMA_MISMATCH'],
  ['duplicate target title', x => { x.metadata.sheets.push(structuredClone(x.metadata.sheets[0])); }, 'SCHEMA_MISMATCH'],
  ['not a grid', x => { x.metadata.sheets[0].properties.sheetType = 'OBJECT'; }, 'SCHEMA_MISMATCH'],
  ['short filing grid', x => { x.metadata.sheets[0].properties.gridProperties.columnCount = 20; }, 'SCHEMA_MISMATCH'],
  ['duplicate customer', x => { x.customers.push(Array.from(x.customers[3])); }, 'IDENTITY_MISMATCH'],
  ['customer mapping changed after report', x => { x.customers[3][1] = 'OTHER'; }, 'IDENTITY_MISMATCH'],
  ['foreign filing data', x => { x.filingValues[3] = 'OTHER'; }, 'IDENTITY_MISMATCH'],
  ['foreign replacement review', x => { x.reviewRows[0][5] = 'OTHER'; }, 'IDENTITY_MISMATCH'],
  ['existing review belongs to another merchant', x => { x.review[3][5] = 'OTHER'; }, 'IDENTITY_MISMATCH'],
  ['existing review has no verified merchant', x => { x.review[3][5] = ''; }, 'IDENTITY_MISMATCH'],
  ['duplicate filing including whitespace', x => { const row = Array.from(x.filings[3]); row[3] += ' '; x.filings.push(row); }, 'DUPLICATE_FILING'],
  ['boolean lock', x => { x.filings[3][16] = true; }, 'LOCKED'],
  ['unknown lock content', x => { x.filings[3][16] = 'yes'; }, 'UNKNOWN_LOCK'],
  ['formula lock', x => { x.filings[3][16] = '=FALSE()'; }, 'UNKNOWN_LOCK'],
  ['filing output formula', x => { x.filings[3][6] = '=1+1'; }, 'FORMULA_TARGET'],
  ['review formula', x => { x.review[3][9] = '=1+1'; }, 'FORMULA_TARGET'],
  ['stamp formula', x => { x.customers[3][2] = '=NOW()'; }, 'FORMULA_TARGET'],
  ['merged filing target', x => { x.metadata.sheets[0].merges = [{ startRowIndex: 3, endRowIndex: 4, startColumnIndex: 0, endColumnIndex: 2 }]; }, 'MERGED_TARGET'],
  ['merged review target', x => { x.metadata.sheets[1].merges = [{ startRowIndex: 2, endRowIndex: 4, startColumnIndex: 9, endColumnIndex: 10 }]; }, 'MERGED_TARGET'],
  ['merged stamp target', x => { x.metadata.sheets[2].merges = [{ startRowIndex: 3, endRowIndex: 4, startColumnIndex: 1, endColumnIndex: 3 }]; }, 'MERGED_TARGET']
]) test(`preflight refuses ${name}`, () => {
  const data = fixture(); mutate(data);
  assert.throws(() => prepareFilingSyncInput(data), error => error.code === 'SHEET_SYNC_' + code &&
    error.requires_reconciliation === false && !error.message.includes('SYN-CUSTOMER'));
});

test('formulas and merges outside overwritten columns and unrelated rows are preserved', () => {
  const data = fixture(); data.filings[3][17] = '=TODAY()'; data.review[4][9] = '=1+1';
  data.metadata.sheets[0].merges = [{ startRowIndex: 0, endRowIndex: 1, startColumnIndex: 0, endColumnIndex: 21 },
    { startRowIndex: 3, endRowIndex: 4, startColumnIndex: 17, endColumnIndex: 19 }];
  assert.equal(prepareFilingSyncInput(data).filing.rowIndex, 3);
});

test('full planner validation runs during preflight, not after a write begins', () => {
  const data = fixture(); data.filingValues[6] = Infinity;
  assert.throws(() => prepareFilingSyncInput(data), /Unsupported cell value/);
});

test('read adapter obtains metadata and formula-aware reads with no mutation methods', async () => {
  const data = fixture(); const reads = [];
  const rows = { 'Filings!A:AC': data.filings, "'Review Queue'!A:AA": data.review, 'Customers!A:Z': data.customers };
  const sheets = { spreadsheets: {
    async get(options) { reads.push(options); return { data: data.metadata }; },
    values: { async get(options) {
      assert.equal(options.valueRenderOption, 'FORMULA'); reads.push(options);
      return { data: { values: rows[options.range] } };
    } }
  } };
  const input = await readFilingSyncInput(sheets, 'SYN-WORKBOOK', data);
  assert.equal(input.stamp.rowIndex, 3);
  assert.equal(reads.length, 4);
  assert.ok(reads.every(x => x.spreadsheetId === 'SYN-WORKBOOK'));
  assert.equal(reads[0].fields, 'sheets(properties,merges)');
});

test('read failure returns no plan and never falls back to partial or stale input', async () => {
  const data = fixture();
  const sheets = { spreadsheets: { async get() { return { data: data.metadata }; },
    values: { async get() { throw new Error('synthetic read refusal'); } } } };
  await assert.rejects(readFilingSyncInput(sheets, 'SYN-WORKBOOK', data), /synthetic read refusal/);
});
