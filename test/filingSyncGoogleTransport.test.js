'use strict';

// Installed googleapis/gaxios execution with an in-memory transport adapter.
// Run with noNetwork.cjs: no Google credentials, sockets or native Sheets calls.
const test = require('node:test');
const assert = require('node:assert/strict');
const { google } = require('googleapis');
const { readFilingSyncInput, FILING_HEADERS, REVIEW_HEADERS } = require('../src/core/filingSyncPreflight');
const { applyFilingSyncBatch } = require('../src/core/filingSyncBatch');

function fixture() {
  const values = Array(20).fill(''); values[1] = '2026-09'; values[3] = 'SYN-CUSTOMER';
  values[5] = '=SYN_LITERAL()'; values[6] = 12.5;
  return {
    metadata: { sheets: ['Filings', 'Review Queue', 'Customers'].map((title, sheetId) => ({
      properties: { title, sheetId, sheetType: 'GRID', gridProperties: { rowCount: 20, columnCount: 29 } }
    })) },
    rows: {
      'Filings!A:AC': [['Filings'], [], Array.from(FILING_HEADERS)],
      "'Review Queue'!A:AA": [['Review Queue'], [], Array.from(REVIEW_HEADERS)],
      'Customers!A:Z': [['Customers'], [], ['Customer ID', 'Square Merchant ID', 'Last Sync'],
        ['SYN-CUSTOMER', 'SYN-MERCHANT', 'OLD']]
    },
    values: { customerId: 'SYN-CUSTOMER', merchantId: 'SYN-MERCHANT', period: '2026-09',
      filingValues: values, reviewRows: [], stampValue: '2026-09-28T03:07:08Z' }
  };
}

function reply(options, data, status = 200) {
  return { config: options, data, status, statusText: String(status), headers: new Headers() };
}

for (const outcome of ['success', 'invalid-batch', 'server-error', 'lost-response']) {
  test(`installed Google client read/preflight/batch transport: ${outcome}`, async () => {
    const data = fixture(); const reads = []; const writes = [];
    const client = google.sheets({ version: 'v4', retry: true,
      retryConfig: { retry: 3, noResponseRetries: 3, retryDelay: 1 },
      adapter: async options => {
        const url = new URL(options.url);
        assert.equal(url.origin, 'https://sheets.googleapis.com');
        if (options.method === 'GET') {
          reads.push(url);
          assert.equal(writes.length, 0);
          if (url.pathname === '/v4/spreadsheets/SYN-WORKBOOK') {
            assert.equal(url.searchParams.get('fields'), 'sheets(properties,merges)');
            return reply(options, data.metadata);
          }
          const range = decodeURIComponent(url.pathname.split('/values/')[1]);
          assert.ok(data.rows[range], range);
          assert.equal(url.searchParams.get('valueRenderOption'), 'FORMULA');
          return reply(options, { values: data.rows[range] });
        }
        writes.push(options);
        assert.equal(options.method, 'POST');
        assert.equal(url.pathname, '/v4/spreadsheets/SYN-WORKBOOK:batchUpdate');
        assert.equal(reads.length, 4);
        assert.equal(options.retry, false);
        assert.equal(options.retryConfig.retry, 0);
        assert.equal(options.retryConfig.noResponseRetries, 0);
        const body = JSON.parse(options.body);
        assert.equal(body.includeSpreadsheetInResponse, false);
        assert.equal(body.requests.length, 2);
        const row = body.requests[0].appendCells.rows[0].values;
        assert.deepEqual(row[5], { userEnteredValue: { stringValue: '=SYN_LITERAL()' } });
        assert.deepEqual(row[6], { userEnteredValue: { numberValue: 12.5 } });
        assert.equal(body.requests[1].updateCells.range.startRowIndex, 3);
        if (outcome === 'lost-response') throw new Error('SYN-PRIVATE-UPSTREAM timeout after possible commit');
        if (outcome === 'invalid-batch') return reply(options, { error: 'SYN-PRIVATE-UPSTREAM' }, 400);
        if (outcome === 'server-error') return reply(options, { error: 'SYN-PRIVATE-UPSTREAM' }, 503);
        return reply(options, { replies: [{}, {}] });
      }
    });
    const input = await readFilingSyncInput(client, 'SYN-WORKBOOK', data.values);
    if (outcome === 'success') {
      assert.deepEqual(await applyFilingSyncBatch(client, 'SYN-WORKBOOK', input), {
        success: true, filingAction: 'appended', reviewRemoved: 0, reviewWritten: 0
      });
    } else {
      await assert.rejects(applyFilingSyncBatch(client, 'SYN-WORKBOOK', input), error =>
        error.code === 'SHEET_SYNC_RECONCILIATION_REQUIRED' && error.automatic_retry_allowed === false &&
        !error.message.includes('SYN-PRIVATE-UPSTREAM'));
    }
    assert.equal(writes.length, 1, 'client defaults must not retry an append');
  });
}
