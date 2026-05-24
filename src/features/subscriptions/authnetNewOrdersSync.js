const crypto = require('crypto');
const { getSheetsClient } = require('../../core/googleSheets');
const { authNetPost } = require('../../connectors/authnet/client');

const FF_SUBSCRIPTIONS_SPREADSHEET_ID = () => (
  process.env.FF_SUBSCRIPTIONS_SPREADSHEET_ID
  || process.env.SUBSCRIPTIONS_SPREADSHEET_ID
  || process.env.PAYMENT_UPDATE_SPREADSHEET_ID
  || process.env.GOOGLE_SHEETS_SPREADSHEET_ID
  || ''
);

const FF_ONBOARDING_SPREADSHEET_ID = () => (
  process.env.FF_ONBOARDING_SPREADSHEET_ID
  || process.env.ONBOARDING_SPREADSHEET_ID
  || ''
);

const TABS = {
  newOrders: 'New Orders',
  active: 'Active Subscriptions',
  paymentUpdate: 'Payment Update',
  onboardingEmails: 'Onboarding Emails',
  authNetLog: 'AuthNet Sync Log'
};

const NEW_ORDER_REQUIRED_HEADERS = [
  'Time', 'Name', 'Email', 'Alt Email', 'Subscription Status', 'Subscription ID', 'Order Status',
  'Order / Invoice #', 'Amount', 'Billing Zip', 'Initial Charge Status', 'Initial Charge Transaction ID',
  'Payment Cleared At'
];

const NEW_ORDER_OPTIONAL_HEADERS = [
  'Last AuthNet Check At', 'Onboarding Routed At', 'Payment Update Routed At', 'Connector Notes'
];

const ACTIVE_HEADERS = [
  'Time', 'Subscription ID', 'Customer ID', 'Name', 'Email', 'Alt Email',
  'Amount', 'Onboarding Status', 'Notes', 'LTV', 'History'
];

const PAYMENT_UPDATE_HEADERS = [
  'Payment Update Type', 'Source Tab', 'Source Row', 'Customer ID', 'Name', 'Email', 'Alt Email',
  'Subscription ID', 'Subscription Status', 'Order / Customer Status', 'Payment Update Status',
  'Payment Update Link', 'Link Generated At', 'Link Expires At', 'Email 1 Sent At', 'Email 2 Sent At',
  'Email 3 Sent At', 'Last Follow-Up At', 'Next Follow-Up Due', 'Stop / Suppressed', 'Stop Reason', 'Notes'
];

const ONBOARDING_HEADERS = [
  'Time', 'Name', 'Email', 'Alt Email', 'Status', 'Next Due', 'Onboarded', 'Onboarding Email',
  'Follow-up 1', 'Follow-up 2', 'Follow-up 3', 'Follow-up 4', 'Follow-up 5', 'Notes', 'Snooze', 'Unsubscribed'
];

function nowIso() {
  return new Date().toISOString();
}

function normHeader(value) {
  return String(value || '').trim().toLowerCase().replace(/[^a-z0-9]+/g, '');
}

function normalizeEmail(value) {
  return String(value || '').trim().toLowerCase();
}

function normalizeString(value) {
  return String(value == null ? '' : value).trim();
}

function emailHash(value) {
  const email = normalizeEmail(value);
  return email ? crypto.createHash('sha256').update(email).digest('hex').slice(0, 12) : '';
}

function colLetter(n) {
  let out = '';
  let x = n;
  while (x > 0) {
    const r = (x - 1) % 26;
    out = String.fromCharCode(65 + r) + out;
    x = Math.floor((x - r - 1) / 26);
  }
  return out;
}

function headerMap(headers) {
  const map = new Map();
  headers.forEach((header, index) => {
    const key = normHeader(header);
    if (key) map.set(key, index);
  });
  return map;
}

function getCell(row, map, header) {
  const idx = map.get(normHeader(header));
  return idx == null ? '' : normalizeString(row[idx]);
}

function setCell(row, map, header, value) {
  const idx = map.get(normHeader(header));
  if (idx == null) return;
  row[idx] = value == null ? '' : value;
}

function parseDateMs(value) {
  if (value instanceof Date) return value.getTime();
  const text = normalizeString(value);
  if (!text) return 0;
  const parsed = Date.parse(text);
  return Number.isNaN(parsed) ? 0 : parsed;
}

function safeAmount(value) {
  if (value == null || value === '') return '';
  const n = Number(value);
  return Number.isFinite(n) ? n.toFixed(2).replace(/\.00$/, '') : normalizeString(value);
}

function initialChargeStatus(record) {
  const response = String(record.firstTransactionResponse || '').toLowerCase();
  if (response.includes('approved')) return 'Approved';
  if (response.includes('declined') || response.includes('failed') || response.includes('avs mismatch')) return 'Declined';
  return record.firstTransactionResponse ? 'Review' : 'No Transaction Found';
}

function isApprovedInitial(record) {
  return initialChargeStatus(record) === 'Approved';
}

function sanitizeRecord(record) {
  return {
    subscriptionId: record.subscriptionId,
    status: record.status,
    amount: record.amount,
    invoiceNumber: record.invoiceNumber,
    emailHash: emailHash(record.email),
    firstTransactionId: record.firstTransactionId,
    firstTransactionStatus: initialChargeStatus(record),
    firstTransactionTimeUtc: record.firstTransactionTimeUtc,
    lastTransactionId: record.lastTransactionId,
    lastTransactionResponse: record.lastTransactionResponse,
    lastTransactionTimeUtc: record.lastTransactionTimeUtc
  };
}

async function authNetListPost(payload) {
  // authNetPost normally expects callers to pass full payload. Inject merchant auth here without
  // leaking credentials to route code or responses.
  const { getMerchantAuthentication } = require('../../connectors/authnet/client');
  const merchantAuthentication = getMerchantAuthentication();
  const key = Object.keys(payload)[0];
  // Authorize.Net's JSON/XML bridge is order-sensitive for ARB list requests.
  // merchantAuthentication must be serialized before searchType/sorting/paging.
  return authNetPost({
    [key]: {
      merchantAuthentication,
      ...payload[key]
    }
  });
}

async function listSubscriptionIdsWithAuth(searchType) {
  const allRows = [];
  let total = 0;
  let offset = 1;
  const limit = 1000;

  while (true) {
    const data = await authNetListPost({
      ARBGetSubscriptionListRequest: {
        searchType,
        sorting: { orderBy: 'id', orderDescending: true },
        paging: { limit, offset }
      }
    });

    total = Number(data.totalNumInResultSet || 0);
    const rows = data.subscriptionDetails || [];
    allRows.push(...rows);
    if (rows.length < limit || (total && allRows.length >= total)) break;
    offset += limit;
    await new Promise(resolve => setTimeout(resolve, 150));
  }

  return { rows: allRows, total };
}

async function getSubscriptionDetail(subscriptionId) {
  return authNetListPost({
    ARBGetSubscriptionRequest: {
      subscriptionId: String(subscriptionId),
      includeTransactions: true
    }
  });
}

function extractRecord(listRow, detail, searchType) {
  const subscriptionId = normalizeString(listRow.id || listRow.subscriptionId);
  const sub = detail.subscription || {};
  const profile = sub.profile || {};
  const order = sub.order || {};
  const schedule = sub.paymentSchedule || {};
  const txs = Array.isArray(sub.arbTransactions) ? sub.arbTransactions.slice() : [];
  txs.sort((a, b) => {
    const ap = Number(a.payNum || 0) - Number(b.payNum || 0);
    if (ap) return ap;
    const aa = Number(a.attemptNum || 0) - Number(b.attemptNum || 0);
    if (aa) return aa;
    return String(a.submitTimeUTC || '').localeCompare(String(b.submitTimeUTC || ''));
  });
  const first = txs[0] || {};
  const last = txs[txs.length - 1] || {};

  return {
    subscriptionId,
    status: normalizeString(sub.status || listRow.status).toLowerCase(),
    amount: safeAmount(sub.amount != null ? sub.amount : listRow.amount),
    invoiceNumber: normalizeString(order.invoiceNumber || listRow.invoice),
    serviceName: normalizeString(sub.name || order.description || listRow.name),
    email: normalizeString(profile.email || listRow.email),
    firstName: normalizeString(listRow.firstName),
    lastName: normalizeString(listRow.lastName),
    createdAtUtc: normalizeString(listRow.createTimeStampUTC),
    startDate: normalizeString(schedule.startDate),
    firstTransactionId: normalizeString(first.transId),
    firstTransactionResponse: normalizeString(first.response),
    firstTransactionTimeUtc: normalizeString(first.submitTimeUTC),
    lastTransactionId: normalizeString(last.transId),
    lastTransactionResponse: normalizeString(last.response),
    lastTransactionTimeUtc: normalizeString(last.submitTimeUTC),
    searchType
  };
}

async function mapLimit(items, limit, worker) {
  const results = [];
  const executing = new Set();
  for (const item of items) {
    const promise = Promise.resolve().then(() => worker(item));
    results.push(promise);
    executing.add(promise);
    const cleanup = () => executing.delete(promise);
    promise.then(cleanup, cleanup);
    if (executing.size >= limit) await Promise.race(executing);
  }
  return Promise.allSettled(results);
}

async function pullAuthNetSubscriptions({ maxRecords = 0 } = {}) {
  const pulledAtUtc = nowIso();
  const listCounts = {};
  const jobs = [];
  const seen = new Set();

  for (const searchType of ['subscriptionActive', 'subscriptionInactive']) {
    const { rows, total } = await listSubscriptionIdsWithAuth(searchType);
    listCounts[searchType] = { returned: rows.length, reportedTotal: total };
    rows.forEach(row => {
      const sid = normalizeString(row.id || row.subscriptionId);
      if (!sid || seen.has(sid)) return;
      seen.add(sid);
      jobs.push({ sid, row, searchType });
    });
  }

  const cappedJobs = maxRecords ? jobs.slice(0, maxRecords) : jobs;
  const settled = await mapLimit(cappedJobs, 8, async job => {
    const detail = await getSubscriptionDetail(job.sid);
    return extractRecord(job.row, detail, job.searchType);
  });

  const records = [];
  const errors = [];
  settled.forEach((result, index) => {
    const job = cappedJobs[index];
    if (result.status === 'fulfilled') records.push(result.value);
    else errors.push({ subscriptionId: job.sid, searchType: job.searchType, error: String(result.reason?.message || result.reason).slice(0, 240) });
  });
  records.sort((a, b) => Number(b.subscriptionId || 0) - Number(a.subscriptionId || 0));

  return { pulledAtUtc, listCounts, records, errors };
}

async function readValues(sheets, spreadsheetId, tab, range = 'A:AZ') {
  const response = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: `'${tab}'!${range}`,
    valueRenderOption: 'FORMATTED_VALUE'
  });
  return response.data.values || [];
}

async function spreadsheetMeta(sheets, spreadsheetId) {
  const response = await sheets.spreadsheets.get({
    spreadsheetId,
    fields: 'properties(title),sheets(properties(sheetId,title,index,hidden,gridProperties(rowCount,columnCount)))'
  });
  return response.data;
}

function sheetProps(meta, title) {
  const sheet = (meta.sheets || []).find(item => item.properties?.title === title);
  return sheet ? sheet.properties : null;
}

async function ensureHeaders(sheets, spreadsheetId, tab, desiredHeaders) {
  const rows = await readValues(sheets, spreadsheetId, tab, '1:1');
  const headers = (rows[0] || []).slice();
  let changed = false;
  desiredHeaders.forEach(header => {
    const exists = headers.some(h => normHeader(h) === normHeader(header));
    if (!exists) {
      headers.push(header);
      changed = true;
    }
  });
  if (changed) {
    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range: `'${tab}'!A1:${colLetter(headers.length)}1`,
      valueInputOption: 'RAW',
      requestBody: { values: [headers] }
    });
  }
  return headers;
}

function indexIds(rows, headerName) {
  if (!rows.length) return new Set();
  const map = headerMap(rows[0]);
  const idx = map.get(normHeader(headerName));
  const out = new Set();
  if (idx == null) return out;
  rows.slice(1).forEach(row => {
    const value = normalizeString(row[idx]);
    if (value && !value.toUpperCase().startsWith('TEST')) out.add(value);
  });
  return out;
}

function indexNewOrders(rows) {
  const map = headerMap(rows[0] || []);
  const bySubId = new Map();
  const byEmail = new Map();
  const items = [];

  rows.slice(1).forEach((row, i) => {
    if (!row.some(cell => normalizeString(cell))) return;
    const rowNumber = i + 2;
    const item = { rowNumber, row, map };
    const subId = getCell(row, map, 'Subscription ID');
    const email = normalizeEmail(getCell(row, map, 'Email'));
    const altEmail = normalizeEmail(getCell(row, map, 'Alt Email'));
    if (subId) bySubId.set(subId, item);
    [email, altEmail].filter(Boolean).forEach(e => {
      if (!byEmail.has(e)) byEmail.set(e, []);
      byEmail.get(e).push(item);
    });
    items.push(item);
  });

  return { map, items, bySubId, byEmail };
}

function matchNewOrder(record, newOrders) {
  if (record.subscriptionId && newOrders.bySubId.has(record.subscriptionId)) return newOrders.bySubId.get(record.subscriptionId);
  const matches = newOrders.byEmail.get(normalizeEmail(record.email)) || [];
  const blank = matches.filter(item => !getCell(item.row, item.map, 'Subscription ID'));
  if (blank.length === 1) return blank[0];
  return null;
}

function proposedNewOrderFields(record) {
  const approved = isApprovedInitial(record);
  if (record.status === 'active') {
    return {
      'Subscription Status': 'Active',
      'Subscription ID': record.subscriptionId,
      'Order Status': approved ? 'Ready for Active Subscriptions' : 'Review',
      'Initial Charge Status': initialChargeStatus(record),
      'Initial Charge Transaction ID': record.firstTransactionId,
      'Payment Cleared At': approved ? record.firstTransactionTimeUtc : ''
    };
  }
  if (record.status === 'suspended') {
    return {
      'Subscription Status': 'Declined',
      'Subscription ID': record.subscriptionId,
      'Order Status': 'Payment Update',
      'Initial Charge Status': initialChargeStatus(record),
      'Initial Charge Transaction ID': record.firstTransactionId,
      'Payment Cleared At': ''
    };
  }
  return {
    'Subscription Status': 'Review',
    'Subscription ID': record.subscriptionId,
    'Order Status': 'Review',
    'Initial Charge Status': initialChargeStatus(record),
    'Initial Charge Transaction ID': record.firstTransactionId,
    'Payment Cleared At': isApprovedInitial(record) ? record.firstTransactionTimeUtc : ''
  };
}

function buildActiveRow(record, newOrder) {
  const m = newOrder.map;
  const row = newOrder.row;
  const time = record.startDate || (record.firstTransactionTimeUtc || getCell(row, m, 'Time')).slice(0, 10) || getCell(row, m, 'Time');
  const amount = record.amount || getCell(row, m, 'Amount');
  return [
    time,
    record.subscriptionId,
    '',
    getCell(row, m, 'Name') || [record.firstName, record.lastName].filter(Boolean).join(' '),
    getCell(row, m, 'Email') || record.email,
    getCell(row, m, 'Alt Email'),
    amount,
    '',
    '',
    amount,
    record.firstTransactionId ? `1st - ${(record.firstTransactionTimeUtc || '').slice(0, 10)} - ${record.firstTransactionId}` : ''
  ];
}

function buildPaymentUpdateRow(record, newOrder) {
  const m = newOrder.map;
  const row = newOrder.row;
  return [
    'New Order',
    TABS.newOrders,
    String(newOrder.rowNumber),
    '',
    getCell(row, m, 'Name') || [record.firstName, record.lastName].filter(Boolean).join(' '),
    getCell(row, m, 'Email') || record.email,
    getCell(row, m, 'Alt Email'),
    record.subscriptionId,
    'Declined',
    'Payment Update',
    'Queued',
    '', '', '', '', '', '', '', '', '', '',
    `Imported by Auth.Net New Orders connector because initial charge was ${initialChargeStatus(record)}.`
  ];
}

function buildOnboardingRow(record, newOrder) {
  const m = newOrder.map;
  const row = newOrder.row;
  return [
    getCell(row, m, 'Time') || record.firstTransactionTimeUtc || record.startDate,
    getCell(row, m, 'Name') || [record.firstName, record.lastName].filter(Boolean).join(' '),
    getCell(row, m, 'Email') || record.email,
    getCell(row, m, 'Alt Email'),
    '', '', '', '', '', '', '', '', '',
    `AUTO ROUTED from FF Subscriptions / New Orders row ${newOrder.rowNumber}; Subscription ID: ${record.subscriptionId}; Initial charge transaction: ${record.firstTransactionId}; Amount: ${record.amount || getCell(row, m, 'Amount')}`,
    'FALSE',
    'FALSE'
  ];
}

function buildPlan({ auth, newOrderRows, activeRows, paymentUpdateRows, onboardingRows }) {
  const newOrders = indexNewOrders(newOrderRows);
  const activeIds = indexIds(activeRows, 'Subscription ID');
  const paymentUpdateIds = indexIds(paymentUpdateRows, 'Subscription ID');
  const onboardingMap = headerMap((onboardingRows[2] || onboardingRows[0] || []));
  const onboardingEmailIdx = onboardingMap.get(normHeader('Email'));
  const onboardingTimeIdx = onboardingMap.get(normHeader('Time'));
  const onboardingNotesIdx = onboardingMap.get(normHeader('Notes'));
  const onboardingKeys = new Set();
  const onboardingSubIds = new Set();
  onboardingRows.slice(onboardingRows[2] ? 3 : 1).forEach(row => {
    const email = onboardingEmailIdx == null ? '' : normalizeEmail(row[onboardingEmailIdx]);
    const time = onboardingTimeIdx == null ? '' : normalizeString(row[onboardingTimeIdx]);
    if (email && time) onboardingKeys.add(`${email}|${time}`);
    const notes = onboardingNotesIdx == null ? '' : normalizeString(row[onboardingNotesIdx]);
    const match = /Subscription ID:\s*([0-9]+)/i.exec(notes);
    if (match) onboardingSubIds.add(match[1]);
  });

  const updates = [];
  const activeInserts = [];
  const paymentUpdateInserts = [];
  const onboardingInserts = [];
  const unmatched = [];
  const review = [];
  const matchedSubIds = new Set();

  auth.records.forEach(record => {
    const newOrder = matchNewOrder(record, newOrders);
    if (!newOrder) {
      if (['active', 'suspended'].includes(record.status)) unmatched.push(sanitizeRecord(record));
      return;
    }
    matchedSubIds.add(record.subscriptionId);
    const proposed = proposedNewOrderFields(record);
    proposed['Last AuthNet Check At'] = auth.pulledAtUtc;
    const current = {};
    Object.keys(proposed).forEach(header => { current[header] = getCell(newOrder.row, newOrder.map, header); });
    const changed = Object.keys(proposed).some(header => current[header] !== String(proposed[header] || ''));
    if (changed) updates.push({ rowNumber: newOrder.rowNumber, current, proposed, record: sanitizeRecord(record) });

    if (record.status === 'active' && isApprovedInitial(record)) {
      if (!activeIds.has(record.subscriptionId)) {
        activeInserts.push({ record: sanitizeRecord(record), row: buildActiveRow(record, newOrder) });
        activeIds.add(record.subscriptionId);
      }
      const onboardingKey = `${normalizeEmail(getCell(newOrder.row, newOrder.map, 'Email') || record.email)}|${getCell(newOrder.row, newOrder.map, 'Time')}`;
      if (!onboardingSubIds.has(record.subscriptionId) && !onboardingKeys.has(onboardingKey)) {
        onboardingInserts.push({ record: sanitizeRecord(record), row: buildOnboardingRow(record, newOrder) });
        onboardingSubIds.add(record.subscriptionId);
        onboardingKeys.add(onboardingKey);
      }
    } else if (record.status === 'suspended' || initialChargeStatus(record) === 'Declined') {
      if (!paymentUpdateIds.has(record.subscriptionId)) {
        paymentUpdateInserts.push({ record: sanitizeRecord(record), row: buildPaymentUpdateRow(record, newOrder) });
        paymentUpdateIds.add(record.subscriptionId);
      }
    } else {
      review.push({ record: sanitizeRecord(record), newOrderRow: newOrder.rowNumber });
    }
  });

  return { updates, activeInserts, paymentUpdateInserts, onboardingInserts, unmatched, review, matchedSubIds: Array.from(matchedSubIds) };
}

async function appendLog(sheets, spreadsheetId, result) {
  const meta = await spreadsheetMeta(sheets, spreadsheetId);
  if (!sheetProps(meta, TABS.authNetLog)) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId,
      requestBody: { requests: [{ addSheet: { properties: { title: TABS.authNetLog, hidden: true } } }] }
    });
    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range: `'${TABS.authNetLog}'!A1:J1`,
      valueInputOption: 'RAW',
      requestBody: { values: [[
        'Run At UTC', 'Mode', 'Triggered By', 'Matched New Orders', 'New Order Updates',
        'Active Inserts', 'Payment Update Inserts', 'Onboarding Inserts', 'Detail Errors', 'Safety'
      ]] }
    });
  }
  await sheets.spreadsheets.values.append({
    spreadsheetId,
    range: `'${TABS.authNetLog}'!A:J`,
    valueInputOption: 'USER_ENTERED',
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values: [[
      result.runAtUtc,
      result.mode,
      result.triggeredBy,
      result.counts.matchedNewOrders,
      result.counts.newOrderUpdates,
      result.counts.activeInserts,
      result.counts.paymentUpdateInserts,
      result.counts.onboardingInserts,
      result.authNet.detailErrorCount,
      result.safety
    ]] }
  });
}

async function applyPlan({ sheets, subscriptionsSpreadsheetId, onboardingSpreadsheetId, newOrderRows, plan, auth, triggeredBy }) {
  const requests = [];
  const values = [];
  const newOrderHeaders = newOrderRows[0];
  const newOrderMap = headerMap(newOrderHeaders);

  plan.updates.forEach(update => {
    const row = newOrderRows[update.rowNumber - 1].slice();
    Object.entries(update.proposed).forEach(([header, value]) => setCell(row, newOrderMap, header, value));
    values.push({
      range: `'${TABS.newOrders}'!A${update.rowNumber}:${colLetter(newOrderHeaders.length)}${update.rowNumber}`,
      values: [row.slice(0, newOrderHeaders.length)]
    });
  });

  if (values.length) {
    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: subscriptionsSpreadsheetId,
      requestBody: { valueInputOption: 'USER_ENTERED', data: values }
    });
  }

  if (plan.activeInserts.length) {
    const activeMeta = await spreadsheetMeta(sheets, subscriptionsSpreadsheetId);
    const activeProps = sheetProps(activeMeta, TABS.active);
    requests.push({ insertDimension: { range: { sheetId: activeProps.sheetId, dimension: 'ROWS', startIndex: 1, endIndex: 1 + plan.activeInserts.length }, inheritFromBefore: false } });
  }
  if (plan.paymentUpdateInserts.length) {
    const paymentMeta = await spreadsheetMeta(sheets, subscriptionsSpreadsheetId);
    const paymentProps = sheetProps(paymentMeta, TABS.paymentUpdate);
    requests.push({ insertDimension: { range: { sheetId: paymentProps.sheetId, dimension: 'ROWS', startIndex: 1, endIndex: 1 + plan.paymentUpdateInserts.length }, inheritFromBefore: false } });
  }
  if (requests.length) {
    await sheets.spreadsheets.batchUpdate({ spreadsheetId: subscriptionsSpreadsheetId, requestBody: { requests } });
  }
  if (plan.activeInserts.length) {
    await sheets.spreadsheets.values.update({
      spreadsheetId: subscriptionsSpreadsheetId,
      range: `'${TABS.active}'!A2:${colLetter(ACTIVE_HEADERS.length)}${1 + plan.activeInserts.length}`,
      valueInputOption: 'USER_ENTERED',
      requestBody: { values: plan.activeInserts.map(item => item.row) }
    });
  }
  if (plan.paymentUpdateInserts.length) {
    await sheets.spreadsheets.values.update({
      spreadsheetId: subscriptionsSpreadsheetId,
      range: `'${TABS.paymentUpdate}'!A2:${colLetter(PAYMENT_UPDATE_HEADERS.length)}${1 + plan.paymentUpdateInserts.length}`,
      valueInputOption: 'USER_ENTERED',
      requestBody: { values: plan.paymentUpdateInserts.map(item => item.row) }
    });
  }

  let onboardingApplied = false;
  let onboardingNote = '';
  if (plan.onboardingInserts.length && onboardingSpreadsheetId) {
    try {
      const onboardingMeta = await spreadsheetMeta(sheets, onboardingSpreadsheetId);
      const props = sheetProps(onboardingMeta, TABS.onboardingEmails);
      if (!props) throw new Error('Missing Onboarding Emails tab');
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: onboardingSpreadsheetId,
        requestBody: { requests: [{ insertDimension: { range: { sheetId: props.sheetId, dimension: 'ROWS', startIndex: 3, endIndex: 3 + plan.onboardingInserts.length }, inheritFromBefore: false } }] }
      });
      await sheets.spreadsheets.values.update({
        spreadsheetId: onboardingSpreadsheetId,
        range: `'${TABS.onboardingEmails}'!A4:P${3 + plan.onboardingInserts.length}`,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: plan.onboardingInserts.map(item => item.row) }
      });
      onboardingApplied = true;
    } catch (err) {
      onboardingNote = `Onboarding route skipped/error: ${err.message}`;
    }
  } else if (plan.onboardingInserts.length) {
    onboardingNote = 'Onboarding route skipped: FF_ONBOARDING_SPREADSHEET_ID not configured.';
  }

  return { onboardingApplied, onboardingNote };
}

async function syncAuthNetNewOrders({ mode = 'dry-run', triggeredBy = 'api', maxRecords = 0 } = {}) {
  const dryRun = mode !== 'apply';
  const subscriptionsSpreadsheetId = FF_SUBSCRIPTIONS_SPREADSHEET_ID();
  const onboardingSpreadsheetId = FF_ONBOARDING_SPREADSHEET_ID();
  if (!subscriptionsSpreadsheetId) throw new Error('Missing FF_SUBSCRIPTIONS_SPREADSHEET_ID');

  const sheets = await getSheetsClient();
  if (!dryRun) {
    await ensureHeaders(sheets, subscriptionsSpreadsheetId, TABS.newOrders, [...NEW_ORDER_REQUIRED_HEADERS, ...NEW_ORDER_OPTIONAL_HEADERS]);
    await ensureHeaders(sheets, subscriptionsSpreadsheetId, TABS.paymentUpdate, PAYMENT_UPDATE_HEADERS);
    await ensureHeaders(sheets, subscriptionsSpreadsheetId, TABS.active, ACTIVE_HEADERS);
  }

  const [newOrderRows, activeRows, paymentUpdateRows, onboardingRows, auth] = await Promise.all([
    readValues(sheets, subscriptionsSpreadsheetId, TABS.newOrders, 'A:Q'),
    readValues(sheets, subscriptionsSpreadsheetId, TABS.active, 'A:K'),
    readValues(sheets, subscriptionsSpreadsheetId, TABS.paymentUpdate, 'A:V'),
    onboardingSpreadsheetId ? readValues(sheets, onboardingSpreadsheetId, TABS.onboardingEmails, 'A:P').catch(() => []) : Promise.resolve([]),
    pullAuthNetSubscriptions({ maxRecords })
  ]);

  const plan = buildPlan({ auth, newOrderRows, activeRows, paymentUpdateRows, onboardingRows });
  let applyResult = { onboardingApplied: false, onboardingNote: '' };
  if (!dryRun) {
    applyResult = await applyPlan({ sheets, subscriptionsSpreadsheetId, onboardingSpreadsheetId, newOrderRows, plan, auth, triggeredBy });
  }

  const result = {
    ok: true,
    runAtUtc: nowIso(),
    mode: dryRun ? 'dry-run' : 'apply',
    triggeredBy,
    safety: 'Read-only Authorize.Net list/detail/transaction-summary pull. Sheet writes only when mode=apply. No charges, cancellations, subscription creation, refunds, payment profile edits, customer emails, or card/bank data handling.',
    authNet: {
      pulledAtUtc: auth.pulledAtUtc,
      listCounts: auth.listCounts,
      detailErrorCount: auth.errors.length
    },
    counts: {
      matchedNewOrders: plan.matchedSubIds.length,
      newOrderUpdates: plan.updates.length,
      activeInserts: plan.activeInserts.length,
      paymentUpdateInserts: plan.paymentUpdateInserts.length,
      onboardingInserts: plan.onboardingInserts.length,
      unmatchedActiveOrSuspended: plan.unmatched.length,
      review: plan.review.length
    },
    applied: !dryRun,
    onboardingApplied: applyResult.onboardingApplied,
    onboardingNote: applyResult.onboardingNote,
    samples: {
      newOrderUpdates: plan.updates.slice(0, 10).map(item => ({ rowNumber: item.rowNumber, record: item.record, proposed: item.proposed })),
      activeInserts: plan.activeInserts.slice(0, 10).map(item => item.record),
      paymentUpdateInserts: plan.paymentUpdateInserts.slice(0, 10).map(item => item.record),
      onboardingInserts: plan.onboardingInserts.slice(0, 10).map(item => item.record),
      review: plan.review.slice(0, 10)
    }
  };

  if (!dryRun) await appendLog(sheets, subscriptionsSpreadsheetId, result);
  return result;
}

module.exports = {
  syncAuthNetNewOrders,
  pullAuthNetSubscriptions,
  buildPlan,
  initialChargeStatus
};
