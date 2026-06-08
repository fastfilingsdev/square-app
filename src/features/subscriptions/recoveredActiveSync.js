const { getSubscription } = require('../../connectors/authnet/client');
const { getSheetsClient } = require('../../core/googleSheets');

const RECOVERED_TAB = 'Recovered Subs';
const ACTIVE_TAB = 'Active Subscriptions';
const DEFAULT_ACTIVE_SYNC_INTERVAL_MINUTES = 15;
const DEFAULT_ACTIVE_SYNC_MAX_ROWS = 50;
const ACTIVE_PRESENTATION_WIDTH = 52; // A:AZ
const PASTEL_BLUE = { red: 0.80, green: 0.90, blue: 1.0 };
const WHITE = { red: 1.0, green: 1.0, blue: 1.0 };

function firstString(...values) {
  for (const value of values) {
    if (value === null || value === undefined) continue;
    const text = String(value).trim();
    if (text) return text;
  }
  return '';
}

function normalize(value) {
  return String(value || '').trim().toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
}

function envFlag(name, defaultValue = false) {
  const raw = process.env[name];
  if (raw === undefined || raw === null || String(raw).trim() === '') return defaultValue;
  return ['1', 'true', 'yes', 'on'].includes(String(raw).trim().toLowerCase());
}

function isRecoveredActiveSyncEnabled() {
  // Safe sheet-only presentation sync. It does not move money, send email, or
  // mutate Auth.Net; it uses read-only subscription history plus Recovered Subs.
  return envFlag('RECOVERED_ACTIVE_SYNC_ENABLED', true);
}

function recoveredActiveSyncIntervalMs() {
  const minutes = Number(process.env.RECOVERED_ACTIVE_SYNC_INTERVAL_MINUTES || DEFAULT_ACTIVE_SYNC_INTERVAL_MINUTES);
  const safeMinutes = Number.isFinite(minutes) && minutes > 0 ? minutes : DEFAULT_ACTIVE_SYNC_INTERVAL_MINUTES;
  return Math.max(5, safeMinutes) * 60 * 1000;
}

function recoveredActiveSyncMaxRows() {
  const max = Number(process.env.RECOVERED_ACTIVE_SYNC_MAX_ROWS || DEFAULT_ACTIVE_SYNC_MAX_ROWS);
  if (!Number.isFinite(max) || max <= 0) return DEFAULT_ACTIVE_SYNC_MAX_ROWS;
  return Math.max(1, Math.min(max, 250));
}

function getSubscriptionsSpreadsheetId() {
  return process.env.FF_SUBSCRIPTIONS_SPREADSHEET_ID
    || process.env.SUBSCRIPTIONS_SPREADSHEET_ID
    || process.env.PAYMENT_UPDATE_SPREADSHEET_ID
    || process.env.GOOGLE_SHEETS_SPREADSHEET_ID
    || '';
}

function quoteSheetName(sheetName) {
  return `'${String(sheetName).replace(/'/g, "''")}'`;
}

function columnName(index) {
  let n = Number(index || 0);
  let out = '';
  while (n > 0) {
    const rem = (n - 1) % 26;
    out = String.fromCharCode(65 + rem) + out;
    n = Math.floor((n - 1) / 26);
  }
  return out || 'A';
}

function rowObject(headers, values, rowNumber) {
  const object = { _rowNumber: rowNumber, _values: values.slice(0, headers.length) };
  headers.forEach((header, i) => {
    object[String(header || '').trim()] = String(values[i] || '').trim();
  });
  return object;
}

async function readTable(sheets, spreadsheetId, tabName, range = 'A:ZZ') {
  const response = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: `${quoteSheetName(tabName)}!${range}`,
    valueRenderOption: 'FORMATTED_VALUE'
  });
  const values = response.data.values || [];
  const headers = (values[0] || []).map(header => String(header || '').trim());
  const rows = [];
  for (let i = 1; i < values.length; i += 1) {
    const row = values[i] || [];
    if (!row.some(cell => String(cell || '').trim())) continue;
    rows.push(rowObject(headers, row, i + 1));
  }
  return { tabName, headers, rows };
}

function getField(row, field) {
  return String((row && row[field]) || '').trim();
}

function setField(headers, values, field, value) {
  const index = headers.indexOf(field);
  if (index === -1) return;
  while (values.length < headers.length) values.push('');
  values[index] = value == null ? '' : String(value);
}

function parseDate(value) {
  const text = String(value || '').trim();
  if (!text) return null;
  const date = new Date(text.replace(/Z$/, '+00:00'));
  if (!Number.isNaN(date.getTime())) return date;
  const short = text.slice(0, 10);
  const shortDate = new Date(`${short}T00:00:00.000Z`);
  return Number.isNaN(shortDate.getTime()) ? null : shortDate;
}

function dateOnly(value) {
  const date = parseDate(value);
  if (date) return date.toISOString().slice(0, 10);
  const text = String(value || '').trim();
  return text ? text.slice(0, 10) : '';
}

function money(value) {
  const raw = String(value == null ? '0' : value).replace(/[$,]/g, '').trim();
  const parsed = Number(raw || 0);
  if (!Number.isFinite(parsed)) return 0;
  return Math.round(parsed * 100) / 100;
}

function displayMoney(value) {
  const amount = money(value);
  const fixed = amount.toFixed(2);
  return fixed.replace(/\.00$/, '').replace(/(\.\d)0$/, '$1');
}

function ordinal(value) {
  const n = Number(String(value || '').trim());
  if (!Number.isInteger(n) || n <= 0) return '';
  const lastTwo = n % 100;
  const suffix = lastTwo >= 10 && lastTwo <= 20 ? 'th' : ({ 1: 'st', 2: 'nd', 3: 'rd' }[n % 10] || 'th');
  return `${n}${suffix}`;
}

function approvedArbTransactions(subscriptionResponse) {
  const subscription = subscriptionResponse?.subscription || {};
  const subscriptionAmount = subscription.amount;
  const transactions = subscription.arbTransactions || subscription.transactions || [];
  const list = Array.isArray(transactions) ? transactions : (transactions ? [transactions] : []);
  return list
    .filter(tx => tx && typeof tx === 'object')
    .filter(tx => {
      const responseText = normalize(firstString(tx.response, tx.responseCode));
      return responseText.includes('approved') || String(tx.responseCode || '').trim() === '1';
    })
    .map(tx => {
      const transactionId = firstString(tx.transId, tx.transactionId);
      const payNum = firstString(tx.payNum, tx.paymentNumber);
      const date = dateOnly(firstString(tx.submitTimeUTC, tx.submitTimeUtc, tx.submitTimeLocal));
      return {
        kind: 'ARB_APPROVED',
        payNum,
        date,
        sortDate: parseDate(firstString(tx.submitTimeUTC, tx.submitTimeUtc, tx.submitTimeLocal)) || new Date(0),
        transactionId,
        amount: money(firstString(tx.amount, subscriptionAmount)),
        label: `${ordinal(payNum) || 'Paid'} - ${date} - ${transactionId}`,
        highlight: false
      };
    })
    .filter(item => item.transactionId);
}

function recoveredTransaction(row) {
  const transactionId = getField(row, 'Recovered Payment Transaction ID');
  const invoice = getField(row, 'Recovered Payment Invoice');
  const date = getField(row, 'Recovered Payment Date') || dateOnly(getField(row, 'Recovered At'));
  const recoveryType = getField(row, 'Recovery Type');
  const normalized = normalize(recoveryType);
  let prefix = 'Recovered';
  if (normalized.includes('new order') || /\ba\b/i.test(recoveryType)) prefix = 'Recovered A';
  else if (normalized.includes('terminated') || /\bc\b/i.test(recoveryType)) prefix = 'Recovered C';
  else if (normalized.includes('payment on hold') || /\bb\b/i.test(recoveryType)) prefix = 'Recovered B';
  return {
    kind: 'RECOVERED',
    payNum: '',
    date,
    sortDate: parseDate(date) || new Date('9999-12-31T00:00:00.000Z'),
    transactionId,
    invoice,
    amount: money(getField(row, 'Amount')),
    label: `${prefix} - ${date} - ${transactionId || invoice}`.trim(),
    highlight: true
  };
}

async function buildHistory(recoveredRow, getSubscriptionFn = getSubscription) {
  const subscriptionId = firstString(getField(recoveredRow, 'Current Active Subscription ID'), getField(recoveredRow, 'Subscription ID'));
  const subscription = await getSubscriptionFn(subscriptionId);
  const items = approvedArbTransactions(subscription);
  const recovered = recoveredTransaction(recoveredRow);
  if (recovered.transactionId || recovered.invoice) {
    if (!items.some(item => item.transactionId && item.transactionId === recovered.transactionId)) {
      items.push(recovered);
    }
  }
  items.sort((a, b) => {
    const diff = (a.sortDate?.getTime?.() || 0) - (b.sortDate?.getTime?.() || 0);
    if (diff !== 0) return diff;
    return (a.highlight ? 1 : 0) - (b.highlight ? 1 : 0);
  });
  const total = items.reduce((sum, item) => sum + money(item.amount), 0);
  return { items, ltv: Math.round(total * 100) / 100 };
}

function buildActiveValues(recoveredRow, historyItems, ltv) {
  const subscriptionId = firstString(getField(recoveredRow, 'Current Active Subscription ID'), getField(recoveredRow, 'Subscription ID'));
  return [
    dateOnly(getField(recoveredRow, 'Recovered At')) || dateOnly(new Date().toISOString()),
    subscriptionId,
    getField(recoveredRow, 'Customer ID'),
    getField(recoveredRow, 'Name'),
    getField(recoveredRow, 'Email'),
    getField(recoveredRow, 'Alt Email'),
    displayMoney(getField(recoveredRow, 'Amount')),
    '',
    getField(recoveredRow, 'Notes'),
    displayMoney(ltv),
    ...historyItems.map(item => item.label)
  ];
}

function findActiveRow(activeRows, subscriptionId) {
  return activeRows.find(row => getField(row, 'Subscription ID') === String(subscriptionId)) || null;
}

async function getSheetId(sheets, spreadsheetId, tabName) {
  const metadata = await sheets.spreadsheets.get({
    spreadsheetId,
    fields: 'sheets.properties(sheetId,title)'
  });
  for (const sheet of metadata.data.sheets || []) {
    const props = sheet.properties || {};
    if (props.title === tabName) return Number(props.sheetId);
  }
  throw new Error(`Missing tab ${tabName}`);
}

async function updateActiveFormat(sheets, spreadsheetId, rowNumber, historyItems, width) {
  const sheetId = await getSheetId(sheets, spreadsheetId, ACTIVE_TAB);
  const requests = [{
    repeatCell: {
      range: { sheetId, startRowIndex: rowNumber - 1, endRowIndex: rowNumber, startColumnIndex: 0, endColumnIndex: width },
      cell: { userEnteredFormat: { backgroundColor: WHITE } },
      fields: 'userEnteredFormat.backgroundColor'
    }
  }];
  historyItems.forEach((item, index) => {
    if (!item.highlight) return;
    const column = 10 + index; // K is zero-index 10.
    requests.push({
      repeatCell: {
        range: { sheetId, startRowIndex: rowNumber - 1, endRowIndex: rowNumber, startColumnIndex: column, endColumnIndex: column + 1 },
        cell: { userEnteredFormat: { backgroundColor: PASTEL_BLUE } },
        fields: 'userEnteredFormat.backgroundColor'
      }
    });
  });
  await sheets.spreadsheets.batchUpdate({ spreadsheetId, requestBody: { requests } });
}

async function updateRecoveredStatus(sheets, spreadsheetId, recoveredHeaders, recoveredRow, activeRowNumber, now) {
  const values = recoveredRow._values.slice();
  setField(recoveredHeaders, values, 'Active Sync Status', 'Synced to Active history');
  setField(recoveredHeaders, values, 'Active Subscription Row', String(activeRowNumber));
  setField(recoveredHeaders, values, 'Last Updated At', now);
  await sheets.spreadsheets.values.update({
    spreadsheetId,
    range: `${quoteSheetName(RECOVERED_TAB)}!A${recoveredRow._rowNumber}:${columnName(recoveredHeaders.length)}${recoveredRow._rowNumber}`,
    valueInputOption: 'USER_ENTERED',
    requestBody: { values: [values.slice(0, recoveredHeaders.length)] }
  });
}

function isPendingRecoveredRow(row) {
  return normalize(getField(row, 'Active Sync Status')) !== normalize('Synced to Active history');
}

async function syncRecoveredSubsToActive({
  sheets,
  spreadsheetId = getSubscriptionsSpreadsheetId(),
  mode = 'dry-run',
  triggeredBy = 'recovered-active-sync',
  onlyCustomerIds = [],
  onlySubscriptionIds = [],
  maxRows = recoveredActiveSyncMaxRows(),
  getSubscriptionFn = getSubscription
} = {}) {
  if (!sheets) sheets = await getSheetsClient();
  if (!spreadsheetId) throw new Error('subscriptions_spreadsheet_not_configured');
  const execute = String(mode || '').toLowerCase() === 'apply' || String(mode || '').toLowerCase() === 'execute';
  const startedAtUtc = new Date().toISOString();
  const recovered = await readTable(sheets, spreadsheetId, RECOVERED_TAB, 'A:X');
  const active = await readTable(sheets, spreadsheetId, ACTIVE_TAB, 'A:AZ');
  const customerSet = new Set((onlyCustomerIds || []).map(String).filter(Boolean));
  const subscriptionSet = new Set((onlySubscriptionIds || []).map(String).filter(Boolean));
  const targets = recovered.rows
    .filter(row => !customerSet.size || customerSet.has(getField(row, 'Customer ID')))
    .filter(row => {
      if (!subscriptionSet.size) return true;
      const sid = firstString(getField(row, 'Current Active Subscription ID'), getField(row, 'Subscription ID'));
      return subscriptionSet.has(sid);
    })
    .filter(isPendingRecoveredRow)
    .slice(0, Math.max(1, Number(maxRows) || DEFAULT_ACTIVE_SYNC_MAX_ROWS));

  const actions = [];
  const errors = [];
  const prepared = [];
  for (const row of targets) {
    const subscriptionId = firstString(getField(row, 'Current Active Subscription ID'), getField(row, 'Subscription ID'));
    const base = {
      customerId: getField(row, 'Customer ID'),
      subscriptionId,
      recoveredSubsRow: row._rowNumber,
      mode: execute ? 'APPLY' : 'DRY_RUN_NO_MUTATION'
    };
    try {
      const { items, ltv } = await buildHistory(row, getSubscriptionFn);
      const values = buildActiveValues(row, items, ltv);
      const width = Math.max(ACTIVE_PRESENTATION_WIDTH, values.length);
      const writeValues = values.concat(Array(Math.max(0, width - values.length)).fill(''));
      const existingActive = findActiveRow(active.rows, subscriptionId);
      const action = {
        ...base,
        historyCellCount: items.length,
        ltv: displayMoney(ltv),
        history: items.map(item => item.label),
        recoveredHighlightIndexes1BasedWithinHistory: items.map((item, index) => item.highlight ? index + 1 : null).filter(Boolean),
        activeAction: existingActive ? 'would update existing row' : 'would insert new row',
        activeRow: existingActive ? existingActive._rowNumber : null
      };
      prepared.push({ row, subscriptionId, items, width, writeValues, existingActive, action });
      actions.push(action);
    } catch (err) {
      errors.push({ ...base, reason: String(err?.message || err).slice(0, 300) });
    }
  }

  if (execute && prepared.length) {
    const inserts = prepared.filter(item => !item.existingActive);
    if (inserts.length) {
      const sheetId = await getSheetId(sheets, spreadsheetId, ACTIVE_TAB);
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId,
        requestBody: {
          requests: [{
            insertDimension: {
              range: { sheetId, dimension: 'ROWS', startIndex: 1, endIndex: 1 + inserts.length },
              inheritFromBefore: false
            }
          }]
        }
      });
      for (const existing of active.rows) existing._rowNumber = Number(existing._rowNumber) + inserts.length;
    }

    let insertIndex = 0;
    for (const item of prepared) {
      try {
        let rowNumber;
        if (item.existingActive) {
          rowNumber = Number(item.existingActive._rowNumber);
          await sheets.spreadsheets.values.update({
            spreadsheetId,
            range: `${quoteSheetName(ACTIVE_TAB)}!A${rowNumber}:${columnName(item.width)}${rowNumber}`,
            valueInputOption: 'USER_ENTERED',
            requestBody: { values: [item.writeValues] }
          });
          item.action.activeAction = 'updated existing row';
        } else {
          rowNumber = 2 + insertIndex;
          insertIndex += 1;
          await sheets.spreadsheets.values.update({
            spreadsheetId,
            range: `${quoteSheetName(ACTIVE_TAB)}!A${rowNumber}:${columnName(item.width)}${rowNumber}`,
            valueInputOption: 'USER_ENTERED',
            requestBody: { values: [item.writeValues] }
          });
          item.action.activeAction = 'inserted new row';
        }
        await updateActiveFormat(sheets, spreadsheetId, rowNumber, item.items, item.width);
        await updateRecoveredStatus(sheets, spreadsheetId, recovered.headers, item.row, rowNumber, new Date().toISOString());
        item.action.activeRow = rowNumber;
      } catch (err) {
        errors.push({
          customerId: item.action.customerId,
          subscriptionId: item.subscriptionId,
          recoveredSubsRow: item.row._rowNumber,
          mode: 'APPLY',
          reason: String(err?.message || err).slice(0, 300)
        });
      }
    }
  }

  const appliedActions = execute ? actions.filter(action => action.activeRow) : [];
  return {
    ok: errors.length === 0,
    mode: execute ? 'APPLY' : 'DRY_RUN_NO_MUTATION',
    triggeredBy,
    startedAtUtc,
    finishedAtUtc: new Date().toISOString(),
    targetCount: targets.length,
    counts: {
      synced: execute ? appliedActions.length : 0,
      planned: execute ? 0 : actions.length,
      inserted: actions.filter(action => execute ? action.activeAction === 'inserted new row' : action.activeAction === 'would insert new row').length,
      updated: actions.filter(action => execute ? action.activeAction === 'updated existing row' : action.activeAction === 'would update existing row').length,
      errors: errors.length
    },
    actions,
    errors,
    safety: 'Recovered Subs → Active Subscriptions presentation sync only. Auth.Net access is read-only subscription history; no Auth.Net mutations, no customer emails, no raw card/bank/profile data, and no Returns operational edits.'
  };
}

module.exports = {
  ACTIVE_TAB,
  RECOVERED_TAB,
  getSubscriptionsSpreadsheetId,
  isRecoveredActiveSyncEnabled,
  recoveredActiveSyncIntervalMs,
  recoveredActiveSyncMaxRows,
  syncRecoveredSubsToActive,
  __recoveredActiveSyncTestHooks: {
    approvedArbTransactions,
    buildActiveValues,
    buildHistory,
    columnName,
    dateOnly,
    displayMoney,
    isPendingRecoveredRow,
    ordinal,
    recoveredTransaction
  }
};
