'use strict';

const { buildFilingSyncBatch } = require('./filingSyncBatch');

// Observed Sales Tax Calculations schema: titles in row 1, headers in row 3.
// Do not discover a different schema by guessing column offsets at write time.
const FILING_HEADERS = Object.freeze([
  'State', 'Period', 'Period Type', 'Customer ID', 'Name', 'Business Name',
  'Gross Sales Before Tax', 'Gross Sales Including Tax', 'Taxable Sales',
  'Non-Taxable Sales', 'Needs Review Sales', 'Tax Collected', 'Review Count',
  'Transaction Count', 'Order Count', 'Status', 'Locked', 'Filed Date',
  'Date Sent to State', 'Confirmation Number', 'Notes'
]);
const REVIEW_HEADERS = Object.freeze([
  'State', 'Period', 'Period Start', 'Period End', 'Customer ID', 'Square Merchant ID',
  'Business Name', 'Order ID', 'Item Name', 'Reason', 'Amount', 'Suggested Fix', 'Reviewed?', 'Notes'
]);
const normal = value => String(value == null ? '' : value).trim();
const label = value => normal(value).toLowerCase();

function refuse(code) {
  const error = new Error('Filing sync preflight refused; inspect schema, identity or filing controls.');
  error.code = code;
  error.requires_reconciliation = false;
  throw error;
}

function exactHeaders(rows, expected) {
  const headers = rows[2];
  if (!Array.isArray(headers) || expected.some((name, i) => label(headers[i]) !== label(name))) {
    refuse('SHEET_SYNC_SCHEMA_MISMATCH');
  }
  const labels = headers.map(label).filter(Boolean);
  if (new Set(labels).size !== labels.length) refuse('SHEET_SYNC_SCHEMA_MISMATCH');
}

function oneColumn(headers, aliases) {
  const matches = headers.flatMap((name, i) => aliases.includes(label(name)) ? [i] : []);
  if (matches.length !== 1) refuse('SHEET_SYNC_SCHEMA_MISMATCH');
  return matches[0];
}

function sheet(metadata, title, minimumColumns) {
  const matches = (metadata.sheets || []).filter(entry => entry.properties?.title === title);
  if (matches.length !== 1) refuse('SHEET_SYNC_SCHEMA_MISMATCH');
  const properties = matches[0].properties;
  if (!Number.isSafeInteger(properties.sheetId) || properties.sheetId < 0 ||
      properties.sheetType !== 'GRID' || !Number.isSafeInteger(properties.gridProperties?.rowCount) ||
      properties.gridProperties.rowCount < 4 || !Number.isSafeInteger(properties.gridProperties?.columnCount) ||
      properties.gridProperties.columnCount < minimumColumns) refuse('SHEET_SYNC_SCHEMA_MISMATCH');
  return matches[0];
}

function writable(target, rows, rowIndex, startColumn, endColumn) {
  if (rowIndex < 3 || rowIndex >= target.properties.gridProperties.rowCount) refuse('SHEET_SYNC_SCHEMA_MISMATCH');
  for (const merge of target.merges || []) {
    if ((merge.startRowIndex || 0) <= rowIndex && merge.endRowIndex > rowIndex &&
        (merge.startColumnIndex || 0) < endColumn && merge.endColumnIndex > startColumn) {
      refuse('SHEET_SYNC_MERGED_TARGET');
    }
  }
  // Reads must use FORMULA. Conservatively refuse literal '=' prefixes too;
  // never destroy an existing formula to refresh generated output.
  if ((rows[rowIndex] || []).slice(startColumn, endColumn).some(value =>
    typeof value === 'string' && value.startsWith('='))) refuse('SHEET_SYNC_FORMULA_TARGET');
}

function prepareFilingSyncInput({ metadata, filings, review, customers,
  customerId, merchantId, period, filingValues, reviewRows, stampValue }) {
  if (![filings, review, customers, filingValues, reviewRows].every(Array.isArray) ||
      ![customerId, merchantId, period].every(value => typeof value === 'string' && value.trim()) ||
      label(merchantId) === 'unknown') refuse('SHEET_SYNC_IDENTITY_MISMATCH');
  const filingSheet = sheet(metadata, 'Filings', 21);
  const reviewSheet = sheet(metadata, 'Review Queue', 14);
  const customerSheet = sheet(metadata, 'Customers', 3);
  exactHeaders(filings, FILING_HEADERS);
  exactHeaders(review, REVIEW_HEADERS);
  const headers = customers[2];
  if (!Array.isArray(headers)) refuse('SHEET_SYNC_SCHEMA_MISMATCH');
  const idColumn = oneColumn(headers, ['customer id', 'internal customer id', 'id']);
  const merchantColumn = oneColumn(headers, ['square merchant id', 'square customer id']);
  const stampColumn = oneColumn(headers, ['last sync', 'lastsync']);
  if (stampColumn >= customerSheet.properties.gridProperties.columnCount) refuse('SHEET_SYNC_SCHEMA_MISMATCH');
  const matchingCustomers = customers.flatMap((row, i) =>
    i >= 3 && normal(row[idColumn]) === normal(customerId) ? [i] : []);
  if (matchingCustomers.length !== 1 || normal(customers[matchingCustomers[0]][merchantColumn]) !== normal(merchantId)) {
    refuse('SHEET_SYNC_IDENTITY_MISMATCH');
  }
  if (normal(filingValues[1]) !== normal(period) || normal(filingValues[3]) !== normal(customerId) ||
      reviewRows.some(row => !Array.isArray(row) || normal(row[1]) !== normal(period) ||
        normal(row[4]) !== normal(customerId) || normal(row[5]) !== normal(merchantId))) {
    refuse('SHEET_SYNC_IDENTITY_MISMATCH');
  }
  const filingMatches = filings.flatMap((row, i) =>
    i >= 3 && normal(row[1]) === normal(period) && normal(row[3]) === normal(customerId) ? [i] : []);
  if (filingMatches.length > 1) refuse('SHEET_SYNC_DUPLICATE_FILING');
  const filingIndex = filingMatches.length ? filingMatches[0] : null;
  if (filingIndex !== null) {
    const locked = label(filings[filingIndex][16]);
    if (locked === 'true') refuse('SHEET_SYNC_LOCKED');
    if (locked !== '' && locked !== 'false') refuse('SHEET_SYNC_UNKNOWN_LOCK');
    writable(filingSheet, filings, filingIndex, 0, 16);
  }
  const removeRowIndexes = review.flatMap((row, i) =>
    i >= 3 && normal(row[1]) === normal(period) && normal(row[4]) === normal(customerId) ? [i] : []);
  removeRowIndexes.forEach(i => {
    // A customer mapping can change while historical review rows retain the
    // former merchant. Do not erase those rows as though ownership matched.
    if (normal(review[i][5]) !== normal(merchantId)) refuse('SHEET_SYNC_IDENTITY_MISMATCH');
    writable(reviewSheet, review, i, 0, 14);
  });
  const stampIndex = matchingCustomers[0];
  writable(customerSheet, customers, stampIndex, stampColumn, stampColumn + 1);
  const input = {
    filing: { sheetId: filingSheet.properties.sheetId, isNew: filingIndex === null,
      rowIndex: filingIndex, values: filingValues },
    review: { sheetId: reviewSheet.properties.sheetId, removeRowIndexes, rows: reviewRows },
    stamp: { sheetId: customerSheet.properties.sheetId, rowIndex: stampIndex,
      columnIndex: stampColumn, value: stampValue }
  };
  buildFilingSyncBatch(input); // Validate every value and total budget before writes.
  return input;
}

// Read-only adapter: all reads finish before a plan is returned. The caller
// still needs concurrency/replay controls; this is NOT a transactional read.
async function readFilingSyncInput(sheets, spreadsheetId, values) {
  if (typeof spreadsheetId !== 'string' || !spreadsheetId.trim()) refuse('SHEET_SYNC_SCHEMA_MISMATCH');
  const [metadata, filings, review, customers] = await Promise.all([
    sheets.spreadsheets.get({ spreadsheetId, fields: 'sheets(properties,merges)' }),
    ...['Filings!A:AC', "'Review Queue'!A:AA", 'Customers!A:Z'].map(range =>
      sheets.spreadsheets.values.get({ spreadsheetId, range, valueRenderOption: 'FORMULA' }))
  ]);
  return prepareFilingSyncInput({ ...values, metadata: metadata.data,
    filings: filings.data.values || [], review: review.data.values || [], customers: customers.data.values || [] });
}

module.exports = { FILING_HEADERS, REVIEW_HEADERS, prepareFilingSyncInput, readFilingSyncInput };
