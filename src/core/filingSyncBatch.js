'use strict';

// Pure request planner used by the filing route after preflight. The caller
// must resolve/validate schema, identities, locks and row coordinates first.
// All returned requests belong in ONE spreadsheets.batchUpdate call. Never
// split the plan into a clear request followed by a separate append request.
const MAX_REQUESTS = 1000;
const MAX_PLAN_BYTES = 1500000;

function integer(value, minimum, name) {
  if (!Number.isSafeInteger(value) || value < minimum) throw new Error('Invalid ' + name);
  return value;
}

function cell(value) {
  if (value === null || value === '') return {};
  if (typeof value === 'string') {
    if (value.length > 50000) throw new Error('Cell exceeds supported length');
    // Explicit stringValue, never formulaValue or USER_ENTERED: provider and
    // customer text beginning with '=' must remain literal text.
    return { userEnteredValue: { stringValue: value } };
  }
  if (typeof value === 'boolean') return { userEnteredValue: { boolValue: value } };
  if (typeof value === 'number' && Number.isFinite(value)) return { userEnteredValue: { numberValue: value } };
  throw new Error('Unsupported cell value');
}

function row(values, width) {
  if (!Array.isArray(values) || values.length !== width) throw new Error('Unexpected row width');
  // Array.from visits sparse entries so holes fail validation as undefined.
  return { values: Array.from(values, cell) };
}

function update(sheetId, rowIndex, columnIndex, values) {
  return { updateCells: {
    range: { sheetId, startRowIndex: rowIndex, endRowIndex: rowIndex + 1,
      startColumnIndex: columnIndex, endColumnIndex: columnIndex + values.length },
    rows: [{ values }], fields: 'userEnteredValue'
  } };
}

function buildFilingSyncBatch({ filing, review, stamp }) {
  if (!filing || !review || !stamp) throw new Error('Complete filing, review and stamp plans required');
  const filingId = integer(filing.sheetId, 0, 'filing sheet ID');
  const reviewId = integer(review.sheetId, 0, 'review sheet ID');
  const customerId = integer(stamp.sheetId, 0, 'customer sheet ID');
  if (new Set([filingId, reviewId, customerId]).size !== 3) throw new Error('Target sheets must be distinct');
  if (typeof filing.isNew !== 'boolean') throw new Error('Explicit filing creation mode required');
  const filingValues = row(filing.values, 20).values;
  const requests = [];
  if (filing.isNew) {
    if (filing.rowIndex != null) throw new Error('New filing must not specify an existing row');
    requests.push({ appendCells: { sheetId: filingId, rows: [{ values: filingValues }], fields: 'userEnteredValue' } });
  } else {
    const index = integer(filing.rowIndex, 1, 'filing row index');
    // Existing workflow controls Q:T remain completely outside the write.
    requests.push(update(filingId, index, 0, filingValues.slice(0, 16)));
  }
  if (!Array.isArray(review.removeRowIndexes) || !Array.isArray(review.rows)) throw new Error('Review rows required');
  if (new Set(review.removeRowIndexes).size !== review.removeRowIndexes.length) throw new Error('Duplicate review row index');
  for (const index of review.removeRowIndexes) {
    integer(index, 1, 'review row index');
    requests.push(update(reviewId, index, 0, Array.from({ length: 14 }, () => ({}))));
  }
  if (review.rows.length) {
    requests.push({ appendCells: { sheetId: reviewId,
      rows: Array.from(review.rows, values => row(values, 14)), fields: 'userEnteredValue' } });
  }
  const stampRow = integer(stamp.rowIndex, 3, 'customer row index');
  const stampColumn = integer(stamp.columnIndex, 0, 'customer stamp column');
  if (stamp.value == null || stamp.value === '' || typeof stamp.value !== 'string') throw new Error('Explicit serialized sync stamp required');
  requests.push(update(customerId, stampRow, stampColumn, [cell(stamp.value)]));
  if (requests.length > MAX_REQUESTS || Buffer.byteLength(JSON.stringify({ requests }), 'utf8') > MAX_PLAN_BYTES) {
    throw new Error('Atomic sync plan exceeds safety budget; do not split or partially apply');
  }
  return { requests, counts: { filingAction: filing.isNew ? 'appended' : 'updated',
    reviewRemoved: review.removeRowIndexes.length, reviewWritten: review.rows.length } };
}

async function applyFilingSyncBatch(sheets, spreadsheetId, input) {
  if (typeof spreadsheetId !== 'string' || !spreadsheetId.trim()) throw new Error('Spreadsheet ID required');
  const plan = buildFilingSyncBatch(input);
  if (typeof sheets?.spreadsheets?.batchUpdate !== 'function') throw new Error('Sheets batch client required');
  try {
    await sheets.spreadsheets.batchUpdate({ spreadsheetId,
      requestBody: { requests: plan.requests, includeSpreadsheetInResponse: false } },
    { retry: false, retryConfig: { retry: 0, noResponseRetries: 0 } });
  } catch (_) {
    // An atomic request may have committed completely before a timeout. Never
    // retry an append automatically or expose the upstream body to callers.
    const error = new Error('Atomic sheet sync outcome requires reconciliation before retrying.');
    error.code = 'SHEET_SYNC_RECONCILIATION_REQUIRED';
    error.requires_reconciliation = true;
    error.automatic_retry_allowed = false;
    throw error;
  }
  return { success: true, ...plan.counts };
}

module.exports = { buildFilingSyncBatch, applyFilingSyncBatch };
