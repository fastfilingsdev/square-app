const crypto = require('crypto');
const { getSheetsClient } = require('../../core/googleSheets');
const { authNetPost, getMerchantAuthentication } = require('../../connectors/authnet/client');

const DEFAULT_FF_BILLING_SPREADSHEET_ID = '1DANHiunfffxvN7PWBxxO0WIzPWVGeOlaWMEcH-eJxBg';

const FF_BILLING_SPREADSHEET_ID = () => (
  process.env.FF_BILLING_SPREADSHEET_ID
  || process.env.BILLING_SPREADSHEET_ID
  || DEFAULT_FF_BILLING_SPREADSHEET_ID
);

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
  billingNewOrders: 'New Orders',
  subscriptionConversions: 'Subscription Conversions',
  billingAuditLog: 'Billing Audit Log',
  active: 'Active Subscriptions',
  onboardingEmails: 'Onboarding Emails'
};

const NEW_ORDER_REQUIRED_HEADERS = [
  'Time', 'Name', 'Email', 'Alt Email', 'Amount', 'Order / Invoice #', 'Auth.Net Transaction ID', 'Sub Created'
];

const NEW_ORDER_OPTIONAL_HEADERS = [
  'Payment Status', 'Route Target', 'Routed At', 'Review Status', 'Last AuthNet Check At', 'Connector Notes'
];

const CONVERSION_HEADERS = [
  'Source New Order Row', 'Order / Invoice #', 'Auth.Net Transaction ID', 'Email', 'Customer ID', 'Amount',
  'Desired Monthly Amount', 'First Billing Date', 'Profile Creation Status', 'ARB Creation Status',
  'New Subscription ID', 'Approval Evidence', 'Notes', 'Name', 'Routed At', 'Last Updated At'
];

const BILLING_AUDIT_HEADERS = [
  'Timestamp', 'Actor', 'Action', 'Source Tab', 'Source Row', 'Order / Invoice #', 'Transaction ID',
  'Subscription ID', 'Result', 'Safety Notes'
];

const ACTIVE_HEADERS = [
  'Time', 'Subscription ID', 'Customer ID', 'Name', 'Email', 'Alt Email',
  'Amount', 'Onboarding Status', 'Notes', 'LTV', 'History'
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

function normalizeString(value) {
  return String(value == null ? '' : value).trim();
}

function normalizeEmail(value) {
  return normalizeString(value).toLowerCase();
}

function emailHash(value) {
  const email = normalizeEmail(value);
  return email ? crypto.createHash('sha256').update(email).digest('hex').slice(0, 12) : '';
}

function envFlag(name, defaultValue = false) {
  const raw = process.env[name];
  if (raw === undefined || raw === null || String(raw).trim() === '') return defaultValue;
  return ['1', 'true', 'yes', 'on'].includes(String(raw).trim().toLowerCase());
}

function isArbAutoCreateEnabled() {
  return envFlag('FF_BILLING_ARB_AUTO_CREATE_ENABLED', true);
}

function isNewOrdersAutomationEnabled() {
  return envFlag('FF_BILLING_NEW_ORDERS_AUTOMATION_ENABLED', true);
}

function newOrdersAutomationIntervalMs() {
  const minutes = Number(process.env.FF_BILLING_NEW_ORDERS_AUTOMATION_INTERVAL_MINUTES || 15);
  const safeMinutes = Number.isFinite(minutes) && minutes > 0 ? minutes : 15;
  return Math.max(5, safeMinutes) * 60 * 1000;
}

function newOrdersAutomationLookbackDays() {
  const days = Number(process.env.FF_BILLING_NEW_ORDERS_AUTOMATION_LOOKBACK_DAYS || 14);
  if (!Number.isFinite(days) || days <= 0) return 14;
  return Math.max(1, Math.min(days, 31));
}

function newOrdersAutomationMaxDetails() {
  const max = Number(process.env.FF_BILLING_NEW_ORDERS_AUTOMATION_MAX_DETAILS || 2500);
  if (!Number.isFinite(max) || max <= 0) return 2500;
  return Math.max(100, Math.min(max, 5000));
}

function colLetter(n) {
  let out = '';
  let x = Number(n || 0);
  while (x > 0) {
    const r = (x - 1) % 26;
    out = String.fromCharCode(65 + r) + out;
    x = Math.floor((x - r - 1) / 26);
  }
  return out || 'A';
}

function quoteSheetName(sheetName) {
  return `'${String(sheetName).replace(/'/g, "''")}'`;
}

function headerMap(headers) {
  const map = new Map();
  (headers || []).forEach((header, index) => {
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
  while (row.length <= idx) row.push('');
  row[idx] = value == null ? '' : String(value);
}

function withMissingHeaders(headers, desiredHeaders) {
  const out = (headers || []).slice();
  desiredHeaders.forEach(header => {
    if (!out.some(current => normHeader(current) === normHeader(header))) out.push(header);
  });
  return out;
}

function normalizeConversionHeaderRow(headers) {
  const out = (headers || []).slice();
  const oldIdx = out.findIndex(header => normHeader(header) === normHeader('Source Direct Charge Row'));
  const newExists = out.some(header => normHeader(header) === normHeader('Source New Order Row'));
  if (oldIdx !== -1 && !newExists) out[oldIdx] = 'Source New Order Row';
  return out;
}

async function readValues(sheets, spreadsheetId, tab, range = 'A:ZZ') {
  const response = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: `${quoteSheetName(tab)}!${range}`,
    valueRenderOption: 'FORMATTED_VALUE'
  });
  return response.data.values || [];
}

async function ensureHeaders(sheets, spreadsheetId, tab, desiredHeaders, { normalizeHeaders = null } = {}) {
  const rows = await readValues(sheets, spreadsheetId, tab, '1:1').catch(() => []);
  let headers = (rows[0] || []).slice();
  if (normalizeHeaders) headers = normalizeHeaders(headers);
  const nextHeaders = withMissingHeaders(headers, desiredHeaders);
  const changed = nextHeaders.length !== (rows[0] || []).length || nextHeaders.some((header, i) => header !== (rows[0] || [])[i]);
  if (changed) {
    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range: `${quoteSheetName(tab)}!A1:${colLetter(nextHeaders.length)}1`,
      valueInputOption: 'RAW',
      requestBody: { values: [nextHeaders] }
    });
  }
  return nextHeaders;
}

function tableFromValues(values) {
  const headers = (values[0] || []).map(header => normalizeString(header));
  const map = headerMap(headers);
  const rows = [];
  (values || []).slice(1).forEach((row, index) => {
    if (!row.some(cell => normalizeString(cell))) return;
    rows.push({ rowNumber: index + 2, row: row.slice(), map });
  });
  return { headers, map, rows };
}

function parseAmount(value) {
  const text = normalizeString(value).replace(/[$,]/g, '');
  const match = text.match(/-?\d+(?:\.\d+)?/);
  if (!match) return '';
  const amount = Number(match[0]);
  if (!Number.isFinite(amount)) return '';
  return amount.toFixed(2);
}

function displayAmount(value) {
  const amount = parseAmount(value);
  return amount ? amount.replace(/\.00$/, '') : '';
}

function amountEqual(left, right) {
  const a = parseAmount(left);
  const b = parseAmount(right);
  return Boolean(a && b && a === b);
}

function isMembershipAmount(value) {
  const amount = parseAmount(value);
  return amount === '20.00' || amount === '29.00';
}

function isTruthy(value) {
  return ['true', 'yes', 'y', '1', 'checked', 'done', 'created'].includes(normalizeString(value).toLowerCase());
}

function parseDateMs(value) {
  const text = normalizeString(value);
  if (!text) return 0;
  const parsed = Date.parse(text);
  return Number.isNaN(parsed) ? 0 : parsed;
}

function dateOnlyUtc(value) {
  const ms = parseDateMs(value);
  if (!ms) return '';
  return new Date(ms).toISOString().slice(0, 10);
}

function addDaysDateOnly(value, days) {
  const ms = parseDateMs(value);
  if (!ms) return '';
  const date = new Date(ms);
  date.setUTCDate(date.getUTCDate() + Number(days || 0));
  return date.toISOString().slice(0, 10);
}

function appendNote(existing, note) {
  const prior = normalizeString(existing);
  const text = normalizeString(note);
  if (!text) return prior;
  return prior ? `${prior}\n${text}` : text;
}

async function authNetPostWithAuth(payload) {
  const merchantAuthentication = getMerchantAuthentication();
  const key = Object.keys(payload)[0];
  return authNetPost({
    [key]: {
      merchantAuthentication,
      ...payload[key]
    }
  });
}

function transactionId(tx) {
  return normalizeString(tx?.transId || tx?.transactionId || tx?.id);
}

function transactionInvoice(tx) {
  return normalizeString(tx?.order?.invoiceNumber || tx?.invoiceNumber);
}

function transactionAmount(tx) {
  return parseAmount(tx?.authAmount ?? tx?.settleAmount ?? tx?.amount);
}

function transactionEmail(tx) {
  return normalizeEmail(tx?.customer?.email || tx?.billTo?.email || tx?.shipTo?.email);
}

function transactionSubmitTime(tx) {
  return normalizeString(tx?.submitTimeUTC || tx?.submitTimeLocal || tx?.settleTimeUTC || tx?.createdAt);
}

function transactionStatus(tx) {
  return normalizeString(tx?.transactionStatus || tx?.status);
}

function isApprovedTransaction(tx) {
  if (normalizeString(tx?.responseCode) === '1') return true;
  const status = transactionStatus(tx).toLowerCase();
  if (!transactionId(tx)) return false;
  return /approved|captured|settled|pendingsettlement|authcapture created/.test(status);
}

function transactionProfileIds(tx) {
  const profile = tx?.profile || {};
  const customer = tx?.customer || {};
  const paymentProfile = profile.paymentProfile || {};
  return {
    customerProfileId: normalizeString(profile.customerProfileId || customer.customerProfileId),
    customerPaymentProfileId: normalizeString(
      profile.customerPaymentProfileId
      || paymentProfile.customerPaymentProfileId
      || paymentProfile.paymentProfileId
      || customer.customerPaymentProfileId
    )
  };
}

function transactionSubscriptionId(tx) {
  const subscription = tx?.subscription || {};
  return normalizeString(subscription.id || subscription.subscriptionId);
}

function transactionRecurringBilling(tx) {
  const value = tx?.recurringBilling;
  if (typeof value === 'boolean') return value;
  return ['true', '1', 'yes', 'y'].includes(normalizeString(value).toLowerCase());
}

function transactionBillToName(tx) {
  const billTo = tx?.billTo || {};
  const first = normalizeString(billTo.firstName);
  const last = normalizeString(billTo.lastName);
  return [first, last].filter(Boolean).join(' ').trim();
}

function isNumericCheckoutInvoice(invoice) {
  return /^\d{7,20}$/.test(normalizeString(invoice));
}

function isNewMembershipCheckoutTransaction(tx) {
  const invoice = transactionInvoice(tx);
  return Boolean(
    isApprovedTransaction(tx)
    && transactionId(tx)
    && isMembershipAmount(transactionAmount(tx))
    && isNumericCheckoutInvoice(invoice)
    && !transactionRecurringBilling(tx)
    && !transactionSubscriptionId(tx)
  );
}

function sanitizeTransaction(tx, source = '') {
  return {
    source,
    transactionId: transactionId(tx),
    invoiceNumber: transactionInvoice(tx),
    amount: transactionAmount(tx),
    status: transactionStatus(tx),
    responseCode: normalizeString(tx?.responseCode),
    submitTimeUTC: transactionSubmitTime(tx),
    emailHash: emailHash(transactionEmail(tx)),
    approved: isApprovedTransaction(tx),
    profileIdsPresent: Boolean(transactionProfileIds(tx).customerProfileId && transactionProfileIds(tx).customerPaymentProfileId)
  };
}

async function getTransactionDetail(transId) {
  const data = await authNetPostWithAuth({
    getTransactionDetailsRequest: {
      transId: String(transId)
    }
  });
  return data.transaction || {};
}

async function listUnsettledTransactionIds() {
  const ids = new Set();
  const notes = [];
  try {
    const data = await authNetPostWithAuth({
      getUnsettledTransactionListRequest: {
        sorting: { orderBy: 'submitTimeUTC', orderDescending: true },
        paging: { limit: 1000, offset: 1 }
      }
    });
    (data.transactions || data.transactionList || []).forEach(tx => {
      const id = normalizeString(tx.transId || tx.id);
      if (id) ids.add(id);
    });
  } catch (err) {
    notes.push(`Unsettled transaction scan skipped/error: ${String(err.message || err).slice(0, 220)}`);
  }
  return { ids, notes };
}

async function listSettledTransactionIds({ lookbackDays = 14 } = {}) {
  const ids = new Set();
  const notes = [];
  const days = Math.max(1, Math.min(Number(lookbackDays) || 14, 31));
  const end = new Date();
  const start = new Date(end.getTime() - days * 24 * 60 * 60 * 1000);
  try {
    const data = await authNetPostWithAuth({
      getSettledBatchListRequest: {
        includeStatistics: false,
        firstSettlementDate: start.toISOString().replace(/\.\d{3}Z$/, 'Z'),
        lastSettlementDate: end.toISOString().replace(/\.\d{3}Z$/, 'Z')
      }
    });
    const batches = data.batchList || [];
    for (const batch of batches) {
      const batchId = normalizeString(batch.batchId);
      if (!batchId) continue;
      let offset = 1;
      const limit = 1000;
      while (true) {
        const txData = await authNetPostWithAuth({
          getTransactionListRequest: {
            batchId,
            sorting: { orderBy: 'submitTimeUTC', orderDescending: true },
            paging: { limit, offset }
          }
        });
        const rows = txData.transactions || txData.transactionList || [];
        rows.forEach(tx => {
          const id = normalizeString(tx.transId || tx.id);
          if (id) ids.add(id);
        });
        if (rows.length < limit) break;
        offset += limit;
        await new Promise(resolve => setTimeout(resolve, 120));
      }
    }
  } catch (err) {
    notes.push(`Settled transaction scan skipped/error: ${String(err.message || err).slice(0, 220)}`);
  }
  return { ids, notes };
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

async function pullRecentTransactions({ lookbackDays = 14, maxDetails = 2500, requiredTransactionIds = [] } = {}) {
  const pulledAtUtc = nowIso();
  const [unsettled, settled] = await Promise.all([
    listUnsettledTransactionIds(),
    listSettledTransactionIds({ lookbackDays })
  ]);
  const ids = new Set([...unsettled.ids, ...settled.ids]);
  requiredTransactionIds.map(normalizeString).filter(Boolean).forEach(id => ids.add(id));
  const allIds = Array.from(ids).sort((a, b) => Number(b || 0) - Number(a || 0));
  const capped = allIds.slice(0, Math.max(1, Number(maxDetails) || 2500));
  const settledDetails = await mapLimit(capped, 8, async id => getTransactionDetail(id));
  const records = [];
  const errors = [];
  settledDetails.forEach((result, index) => {
    const id = capped[index];
    if (result.status === 'fulfilled') records.push(result.value);
    else errors.push({ transactionId: id, error: String(result.reason?.message || result.reason).slice(0, 240) });
  });
  records.sort((a, b) => Number(transactionId(b) || 0) - Number(transactionId(a) || 0));
  return {
    pulledAtUtc,
    listCounts: {
      unsettled: unsettled.ids.size,
      settled: settled.ids.size,
      totalUnique: allIds.length,
      detailed: records.length
    },
    notes: [...unsettled.notes, ...settled.notes],
    records,
    errors
  };
}

function transactionIndexes(records) {
  const byId = new Map();
  const byInvoice = new Map();
  records.forEach(tx => {
    const id = transactionId(tx);
    const invoice = transactionInvoice(tx);
    if (id) byId.set(id, tx);
    if (invoice) {
      const key = invoice.toLowerCase();
      if (!byInvoice.has(key)) byInvoice.set(key, []);
      byInvoice.get(key).push(tx);
    }
  });
  return { byId, byInvoice };
}

function chooseByTime(candidates, orderTime) {
  const orderMs = parseDateMs(orderTime);
  if (!orderMs) return candidates.length === 1 ? { tx: candidates[0], ambiguous: candidates.length > 1 } : { tx: null, ambiguous: candidates.length > 1 };
  const withDistance = candidates.map(tx => ({ tx, distance: Math.abs((parseDateMs(transactionSubmitTime(tx)) || orderMs) - orderMs) }));
  withDistance.sort((a, b) => a.distance - b.distance);
  if (!withDistance.length) return { tx: null, ambiguous: false };
  if (withDistance.length > 1 && withDistance[0].distance === withDistance[1].distance) return { tx: null, ambiguous: true };
  return { tx: withDistance[0].tx, ambiguous: false };
}

function matchTransactionForOrder(order, auth) {
  const map = order.map;
  const row = order.row;
  const explicitTxId = getCell(row, map, 'Auth.Net Transaction ID');
  const explicitInvoice = getCell(row, map, 'Order / Invoice #');
  const email = normalizeEmail(getCell(row, map, 'Email'));
  const altEmail = normalizeEmail(getCell(row, map, 'Alt Email'));
  const amount = parseAmount(getCell(row, map, 'Amount'));
  const orderTime = getCell(row, map, 'Time');
  const indexes = auth.indexes || transactionIndexes(auth.records || []);

  if (explicitTxId) {
    const tx = indexes.byId.get(explicitTxId);
    return tx ? { tx, source: 'sheet-transaction-id' } : { tx: null, source: 'sheet-transaction-id', reason: 'Transaction ID not found in Auth.Net lookback/details pull' };
  }

  if (explicitInvoice) {
    const candidates = (indexes.byInvoice.get(explicitInvoice.toLowerCase()) || []).filter(tx => !amount || amountEqual(amount, transactionAmount(tx)));
    if (candidates.length === 1) return { tx: candidates[0], source: 'sheet-invoice' };
    if (candidates.length > 1) {
      const chosen = chooseByTime(candidates, orderTime);
      if (chosen.tx && !chosen.ambiguous) return { tx: chosen.tx, source: 'sheet-invoice-time-nearest' };
      return { tx: null, source: 'sheet-invoice', reason: 'Multiple Auth.Net transactions matched invoice/amount' };
    }
    return { tx: null, source: 'sheet-invoice', reason: 'Invoice not found in Auth.Net lookback/details pull' };
  }

  const orderMs = parseDateMs(orderTime);
  const maxDistanceMs = 3 * 24 * 60 * 60 * 1000;
  const candidates = (auth.records || []).filter(tx => {
    if (!isApprovedTransaction(tx)) return false;
    if (amount && !amountEqual(amount, transactionAmount(tx))) return false;
    const txEmail = transactionEmail(tx);
    if (email && txEmail !== email && (!altEmail || txEmail !== altEmail)) return false;
    if (orderMs) {
      const txMs = parseDateMs(transactionSubmitTime(tx));
      if (txMs && Math.abs(txMs - orderMs) > maxDistanceMs) return false;
    }
    return true;
  });
  if (candidates.length === 1) return { tx: candidates[0], source: 'email-amount-time' };
  if (candidates.length > 1) {
    const chosen = chooseByTime(candidates, orderTime);
    if (chosen.tx && !chosen.ambiguous) return { tx: chosen.tx, source: 'email-amount-time-nearest' };
    return { tx: null, source: 'email-amount-time', reason: 'Multiple approved Auth.Net transactions matched email/amount/time' };
  }
  return { tx: null, source: 'email-amount-time', reason: 'No approved Auth.Net transaction matched email/amount/time' };
}

function validateOrderAgainstTransaction(order, tx) {
  const map = order.map;
  const row = order.row;
  const issues = [];
  const sheetAmount = parseAmount(getCell(row, map, 'Amount'));
  const txAmount = transactionAmount(tx);
  const invoice = transactionInvoice(tx);
  const txId = transactionId(tx);

  if (!sheetAmount) issues.push('missing order amount');
  if (sheetAmount && !isMembershipAmount(sheetAmount)) issues.push('membership amount is not 20 or 29');
  if (!txId) issues.push('missing transaction ID');
  if (!invoice) issues.push('missing invoice number');
  if (invoice && !isNumericCheckoutInvoice(invoice)) issues.push('invoice is not a numeric checkout invoice');
  if (!txAmount) issues.push('missing transaction amount');
  if (sheetAmount && txAmount && !amountEqual(sheetAmount, txAmount)) issues.push('amount mismatch');
  if (!isApprovedTransaction(tx)) issues.push('transaction not approved/captured');
  if (transactionRecurringBilling(tx)) issues.push('transaction is recurring billing, not a new checkout charge');
  if (transactionSubscriptionId(tx)) issues.push('transaction already belongs to an Auth.Net subscription');

  return { ok: issues.length === 0, issues };
}

function conversionSourceRow(existing) {
  return getCell(existing.row, existing.map, 'Source New Order Row') || getCell(existing.row, existing.map, 'Source Direct Charge Row');
}

function indexConversions(conversionRows) {
  const table = tableFromValues(conversionRows);
  const byTransactionId = new Map();
  const bySourceRow = new Map();
  table.rows.forEach(item => {
    const txId = getCell(item.row, item.map, 'Auth.Net Transaction ID');
    const sourceRow = conversionSourceRow(item);
    if (txId) byTransactionId.set(txId, item);
    if (sourceRow) bySourceRow.set(String(sourceRow), item);
  });
  return { ...table, byTransactionId, bySourceRow };
}

function indexIds(rows, headerName) {
  const table = tableFromValues(rows);
  const ids = new Set();
  table.rows.forEach(item => {
    const value = getCell(item.row, item.map, headerName);
    if (value && !value.toUpperCase().startsWith('TEST')) ids.add(value);
  });
  return ids;
}

function indexOnboarding(onboardingRows) {
  const headerRowIndex = onboardingRows[2] ? 2 : 0;
  const headers = onboardingRows[headerRowIndex] || [];
  const map = headerMap(headers);
  const emailIdx = map.get(normHeader('Email'));
  const timeIdx = map.get(normHeader('Time'));
  const notesIdx = map.get(normHeader('Notes'));
  const keys = new Set();
  const subIds = new Set();
  onboardingRows.slice(headerRowIndex + 1).forEach(row => {
    const email = emailIdx == null ? '' : normalizeEmail(row[emailIdx]);
    const time = timeIdx == null ? '' : normalizeString(row[timeIdx]);
    if (email && time) keys.add(`${email}|${time}`);
    const notes = notesIdx == null ? '' : normalizeString(row[notesIdx]);
    const match = /Subscription ID:\s*([0-9]+)/i.exec(notes);
    if (match) subIds.add(match[1]);
  });
  return { keys, subIds };
}

function valuesContainToken(values, token) {
  const needle = normalizeString(token);
  if (!needle) return false;
  return (values || []).some(row => (row || []).some(cell => normalizeString(cell) === needle || normalizeString(cell).includes(needle)));
}

function discoverNewOrderRows({ newOrderRows, conversionRows, activeRows, onboardingRows, auth, now = nowIso() }) {
  const headers = withMissingHeaders((newOrderRows[0] || []).slice(), [...NEW_ORDER_REQUIRED_HEADERS, ...NEW_ORDER_OPTIONAL_HEADERS]);
  const map = headerMap(headers);
  const newOrders = tableFromValues(newOrderRows);
  const conversionIndex = indexConversions(conversionRows);
  const existingTransactionIds = new Set();
  const existingInvoices = new Set();

  newOrders.rows.forEach(item => {
    const txId = getCell(item.row, item.map, 'Auth.Net Transaction ID');
    const invoice = getCell(item.row, item.map, 'Order / Invoice #');
    if (txId) existingTransactionIds.add(txId);
    if (invoice) existingInvoices.add(invoice.toLowerCase());
  });
  conversionIndex.rows.forEach(item => {
    const txId = getCell(item.row, item.map, 'Auth.Net Transaction ID');
    const invoice = getCell(item.row, item.map, 'Order / Invoice #');
    const subId = getCell(item.row, item.map, 'New Subscription ID');
    if (txId) existingTransactionIds.add(txId);
    if (invoice) existingInvoices.add(invoice.toLowerCase());
    if (subId) existingTransactionIds.add(subId);
  });

  const discovered = [];
  (auth.records || []).forEach(tx => {
    if (!isNewMembershipCheckoutTransaction(tx)) return;
    const txId = transactionId(tx);
    const invoice = transactionInvoice(tx);
    if (existingTransactionIds.has(txId) || existingInvoices.has(invoice.toLowerCase())) return;
    if (valuesContainToken(activeRows, txId) || valuesContainToken(activeRows, invoice)) return;
    if (valuesContainToken(onboardingRows, txId) || valuesContainToken(onboardingRows, invoice)) return;
    const row = Array(headers.length).fill('');
    setCell(row, map, 'Time', transactionSubmitTime(tx) || now);
    setCell(row, map, 'Name', transactionBillToName(tx));
    setCell(row, map, 'Email', transactionEmail(tx));
    setCell(row, map, 'Alt Email', '');
    setCell(row, map, 'Amount', displayAmount(transactionAmount(tx)));
    setCell(row, map, 'Order / Invoice #', invoice);
    setCell(row, map, 'Auth.Net Transaction ID', txId);
    setCell(row, map, 'Sub Created', '');
    setCell(row, map, 'Payment Status', 'Auto-discovered — pending guarded conversion');
    setCell(row, map, 'Route Target', 'Subscription Conversions');
    setCell(row, map, 'Routed At', now);
    setCell(row, map, 'Review Status', '');
    setCell(row, map, 'Last AuthNet Check At', auth.pulledAtUtc || now);
    setCell(row, map, 'Connector Notes', `AUTO DISCOVERED from Auth.Net approved checkout transaction ${txId}; guarded membership conversion pending.`);
    discovered.push({ transactionId: txId, invoice, row: row.slice(0, headers.length), transaction: sanitizeTransaction(tx, 'authnet-auto-discovery') });
    existingTransactionIds.add(txId);
    existingInvoices.add(invoice.toLowerCase());
  });

  discovered.sort((a, b) => parseDateMs(a.row[map.get(normHeader('Time'))]) - parseDateMs(b.row[map.get(normHeader('Time'))]));
  return { headers, discovered };
}

function buildConversionFields({ order, tx, status, notes = '', newSubscriptionId = '', now = nowIso() }) {
  const m = order.map;
  const row = order.row;
  const amount = transactionAmount(tx) || parseAmount(getCell(row, m, 'Amount'));
  const firstBillingDate = addDaysDateOnly(transactionSubmitTime(tx) || getCell(row, m, 'Time'), 30);
  const invoice = transactionInvoice(tx) || getCell(row, m, 'Order / Invoice #');
  const txId = transactionId(tx) || getCell(row, m, 'Auth.Net Transaction ID');
  const approvalEvidence = txId && invoice
    ? `Auth.Net approved original membership charge verified: invoice ${invoice}, transaction ${txId}, amount ${displayAmount(amount)}, first ARB date ${firstBillingDate || 'pending date review'}.`
    : '';
  return {
    'Source New Order Row': String(order.rowNumber),
    'Order / Invoice #': invoice,
    'Auth.Net Transaction ID': txId,
    Email: getCell(row, m, 'Email') || transactionEmail(tx),
    'Customer ID': '',
    Amount: displayAmount(amount),
    'Desired Monthly Amount': displayAmount(amount),
    'First Billing Date': firstBillingDate,
    'Profile Creation Status': status.profileCreationStatus || 'Ready — transaction verified',
    'ARB Creation Status': status.arbCreationStatus || 'Ready — live ARB creation disabled/dry-run',
    'New Subscription ID': newSubscriptionId,
    'Approval Evidence': approvalEvidence,
    Notes: notes,
    Name: getCell(row, m, 'Name'),
    'Routed At': now,
    'Last Updated At': now
  };
}

function buildRowFromFields(headers, fields, existingRow = []) {
  const map = headerMap(headers);
  const row = existingRow.slice(0, headers.length);
  while (row.length < headers.length) row.push('');
  Object.entries(fields).forEach(([header, value]) => setCell(row, map, header, value));
  return row.slice(0, headers.length);
}

function buildActiveRow({ order, tx, subscriptionId }) {
  const m = order.map;
  const row = order.row;
  const amount = displayAmount(transactionAmount(tx) || getCell(row, m, 'Amount'));
  return [
    dateOnlyUtc(transactionSubmitTime(tx)) || getCell(row, m, 'Time'),
    subscriptionId,
    '',
    getCell(row, m, 'Name'),
    getCell(row, m, 'Email') || transactionEmail(tx),
    getCell(row, m, 'Alt Email'),
    amount,
    '',
    'AUTO ROUTED from FF - Billing / New Orders after guarded ARB creation.',
    amount,
    `1st - ${dateOnlyUtc(transactionSubmitTime(tx)) || ''} - ${transactionId(tx)}`
  ];
}

function buildOnboardingRow({ order, tx, subscriptionId }) {
  const m = order.map;
  const row = order.row;
  return [
    getCell(row, m, 'Time') || transactionSubmitTime(tx),
    getCell(row, m, 'Name'),
    getCell(row, m, 'Email') || transactionEmail(tx),
    getCell(row, m, 'Alt Email'),
    '', '', '', '', '', '', '', '', '',
    `AUTO ROUTED from FF - Billing / New Orders row ${order.rowNumber}; Subscription ID: ${subscriptionId}; Original charge transaction: ${transactionId(tx)}; Amount: ${displayAmount(transactionAmount(tx) || getCell(row, m, 'Amount'))}`,
    'FALSE',
    'FALSE'
  ];
}

function safeErrorMessage(err) {
  return String(err?.message || err || '').replace(/\b[0-9]{13,19}\b/g, '[redacted-number]').slice(0, 300);
}

async function createCustomerProfileFromTransaction(transId) {
  const data = await authNetPostWithAuth({
    createCustomerProfileFromTransactionRequest: {
      transId: String(transId)
    }
  });
  const customerProfileId = normalizeString(data.customerProfileId);
  let ids = data.customerPaymentProfileIdList || data.customerPaymentProfileIdList?.numericString || [];
  if (ids && !Array.isArray(ids) && typeof ids === 'object') ids = ids.numericString || ids.customerPaymentProfileId || [];
  if (!Array.isArray(ids)) ids = [ids];
  const customerPaymentProfileId = normalizeString(ids.filter(Boolean).slice(-1)[0]);
  if (!customerProfileId || !customerPaymentProfileId) {
    throw new Error('Authorize.Net profile creation did not return usable customer/payment profile IDs');
  }
  return { customerProfileId, customerPaymentProfileId, source: 'createCustomerProfileFromTransaction' };
}

async function ensureProfileForTransaction(tx) {
  const existing = transactionProfileIds(tx);
  if (existing.customerProfileId && existing.customerPaymentProfileId) {
    return { ...existing, source: 'transactionDetail' };
  }
  return createCustomerProfileFromTransaction(transactionId(tx));
}

function arbInvoiceNumber(invoiceNumber, rowNumber) {
  const base = normalizeString(invoiceNumber) || `ROW-${rowNumber}`;
  return `SUB-${base}`.slice(0, 20);
}

async function createArbSubscriptionForOrder({ order, tx, profileIds, firstBillingDate }) {
  const amount = transactionAmount(tx) || parseAmount(getCell(order.row, order.map, 'Amount'));
  if (!amountEqual(amount, getCell(order.row, order.map, 'Amount'))) {
    throw new Error('Refusing ARB creation: amount mismatch before subscription create');
  }
  if (!firstBillingDate) throw new Error('Refusing ARB creation: missing first billing date');
  const invoice = transactionInvoice(tx) || getCell(order.row, order.map, 'Order / Invoice #');
  const payload = {
    ARBCreateSubscriptionRequest: {
      refId: `ffb-${order.rowNumber}-${Date.now().toString().slice(-8)}`.slice(0, 20),
      subscription: {
        name: 'Fast Filings Membership',
        paymentSchedule: {
          interval: { length: 1, unit: 'months' },
          startDate: firstBillingDate,
          totalOccurrences: 9999,
          trialOccurrences: 0
        },
        amount,
        trialAmount: '0.00',
        order: {
          invoiceNumber: arbInvoiceNumber(invoice, order.rowNumber),
          description: 'Fast Filings Membership'
        },
        profile: {
          customerProfileId: String(profileIds.customerProfileId),
          customerPaymentProfileId: String(profileIds.customerPaymentProfileId)
        }
      }
    }
  };
  const data = await authNetPostWithAuth(payload);
  const subscriptionId = normalizeString(data.subscriptionId);
  if (!subscriptionId) throw new Error('Authorize.Net did not return a subscription ID');
  return { subscriptionId, messages: data.messages?.message || [] };
}

function buildPlan({ newOrderRows, conversionRows, activeRows, onboardingRows, auth, now = nowIso() }) {
  const newOrders = tableFromValues(newOrderRows);
  const conversionIndex = indexConversions(conversionRows);
  const activeIds = indexIds(activeRows, 'Subscription ID');
  const onboarding = indexOnboarding(onboardingRows || []);

  const rowUpdates = [];
  const conversionUpserts = [];
  const review = [];
  const skipped = [];
  const ready = [];

  const authWithIndexes = { ...auth, indexes: transactionIndexes(auth.records || []) };

  newOrders.rows.forEach(order => {
    const m = order.map;
    const row = order.row;
    const email = getCell(row, m, 'Email');
    const amount = getCell(row, m, 'Amount');
    const currentSubCreated = getCell(row, m, 'Sub Created');
    const existingTxId = getCell(row, m, 'Auth.Net Transaction ID');
    const existingInvoice = getCell(row, m, 'Order / Invoice #');

    if (isTruthy(currentSubCreated)) {
      skipped.push({ rowNumber: order.rowNumber, reason: 'Sub Created already checked' });
      return;
    }
    if (!email && !amount && !existingTxId && !existingInvoice) {
      skipped.push({ rowNumber: order.rowNumber, reason: 'Blank/incomplete order row' });
      return;
    }

    const match = matchTransactionForOrder(order, authWithIndexes);
    if (!match.tx) {
      const reason = match.reason || 'No Auth.Net transaction matched';
      rowUpdates.push({
        rowNumber: order.rowNumber,
        fields: {
          'Payment Status': 'Review — transaction not matched',
          'Route Target': 'Review / Payment Issue',
          'Review Status': reason,
          'Last AuthNet Check At': auth.pulledAtUtc || now,
          'Connector Notes': appendNote(getCell(row, m, 'Connector Notes'), reason)
        }
      });
      review.push({ rowNumber: order.rowNumber, reason });
      return;
    }

    const validation = validateOrderAgainstTransaction(order, match.tx);
    if (!validation.ok) {
      const reason = validation.issues.join('; ');
      rowUpdates.push({
        rowNumber: order.rowNumber,
        fields: {
          'Order / Invoice #': transactionInvoice(match.tx) || existingInvoice,
          'Auth.Net Transaction ID': transactionId(match.tx) || existingTxId,
          'Payment Status': reason.includes('not approved') ? 'Payment Issue — transaction not approved' : 'Review — guard failed',
          'Route Target': 'Review / Payment Issue',
          'Review Status': reason,
          'Last AuthNet Check At': auth.pulledAtUtc || now,
          'Connector Notes': appendNote(getCell(row, m, 'Connector Notes'), `Guard blocked routing: ${reason}`)
        }
      });
      review.push({ rowNumber: order.rowNumber, reason, transaction: sanitizeTransaction(match.tx, match.source) });
      return;
    }

    const txId = transactionId(match.tx);
    const existingConversion = conversionIndex.byTransactionId.get(txId) || conversionIndex.bySourceRow.get(String(order.rowNumber));
    const conversionFields = buildConversionFields({
      order,
      tx: match.tx,
      status: {},
      notes: `Matched from ${match.source}; guarded conversion is ready. ARB must start 30 days after original charge.`,
      now
    });
    const firstBillingDate = conversionFields['First Billing Date'];

    rowUpdates.push({
      rowNumber: order.rowNumber,
      fields: {
        'Order / Invoice #': transactionInvoice(match.tx),
        'Auth.Net Transaction ID': txId,
        'Payment Status': 'Verified — ready for subscription conversion',
        'Route Target': 'Subscription Conversions',
        'Routed At': getCell(row, m, 'Routed At') || now,
        'Review Status': '',
        'Last AuthNet Check At': auth.pulledAtUtc || now,
        'Connector Notes': appendNote(getCell(row, m, 'Connector Notes'), `Verified Auth.Net original charge; first ARB billing date ${firstBillingDate}.`)
      }
    });

    conversionUpserts.push({
      rowNumber: existingConversion ? existingConversion.rowNumber : null,
      fields: conversionFields,
      existingRow: existingConversion ? existingConversion.row : [],
      order,
      tx: match.tx,
      firstBillingDate,
      newSubscriptionId: getCell(existingConversion?.row || [], existingConversion?.map || headerMap([]), 'New Subscription ID')
    });
    ready.push({ rowNumber: order.rowNumber, transaction: sanitizeTransaction(match.tx, match.source), firstBillingDate });
  });

  return { rowUpdates, conversionUpserts, activeInserts: [], onboardingInserts: [], review, skipped, ready, activeIds, onboarding };
}

async function maybeCreateArbs({ plan, arbLiveEnabled, arbLiveRequested }) {
  const results = [];
  if (!arbLiveRequested) {
    plan.conversionUpserts.forEach(item => {
      item.fields['ARB Creation Status'] = 'Ready — dry-run/no live ARB creation requested';
    });
    return results;
  }
  if (!arbLiveEnabled) {
    plan.conversionUpserts.forEach(item => {
      item.fields['ARB Creation Status'] = 'Ready — live ARB creation blocked by safety gate';
      item.fields.Notes = appendNote(item.fields.Notes, 'Live ARB creation was requested but not allowed because FF_BILLING_ARB_AUTO_CREATE_ENABLED and per-request allowLiveArb were not both true.');
    });
    return results;
  }

  for (const item of plan.conversionUpserts) {
    if (item.newSubscriptionId) {
      item.fields['ARB Creation Status'] = 'Already created';
      item.fields['New Subscription ID'] = item.newSubscriptionId;
      results.push({ rowNumber: item.order.rowNumber, status: 'skipped-existing-subscription', subscriptionId: item.newSubscriptionId });
      continue;
    }
    try {
      const profileIds = await ensureProfileForTransaction(item.tx);
      item.fields['Profile Creation Status'] = profileIds.source === 'transactionDetail' ? 'Existing profile on transaction detail' : 'Created from original transaction';
      const arb = await createArbSubscriptionForOrder({
        order: item.order,
        tx: item.tx,
        profileIds,
        firstBillingDate: item.firstBillingDate
      });
      item.fields['ARB Creation Status'] = 'Created';
      item.fields['New Subscription ID'] = arb.subscriptionId;
      item.fields.Notes = appendNote(item.fields.Notes, `ARB created starting ${item.firstBillingDate}; original charge kept as first charge evidence.`);
      const rowUpdate = plan.rowUpdates.find(update => update.rowNumber === item.order.rowNumber);
      if (rowUpdate) {
        rowUpdate.fields['Sub Created'] = 'TRUE';
        rowUpdate.fields['Payment Status'] = 'Subscription created';
        rowUpdate.fields['Route Target'] = 'Active Subscriptions / Onboarding';
        rowUpdate.fields['Review Status'] = '';
        rowUpdate.fields['Connector Notes'] = appendNote(rowUpdate.fields['Connector Notes'], `ARB subscription ${arb.subscriptionId} created starting ${item.firstBillingDate}.`);
      }
      if (!plan.activeIds.has(arb.subscriptionId)) {
        plan.activeInserts.push({ row: buildActiveRow({ order: item.order, tx: item.tx, subscriptionId: arb.subscriptionId }), subscriptionId: arb.subscriptionId });
        plan.activeIds.add(arb.subscriptionId);
      }
      const onboardingKey = `${normalizeEmail(getCell(item.order.row, item.order.map, 'Email') || transactionEmail(item.tx))}|${getCell(item.order.row, item.order.map, 'Time')}`;
      if (!plan.onboarding.subIds.has(arb.subscriptionId) && !plan.onboarding.keys.has(onboardingKey)) {
        plan.onboardingInserts.push({ row: buildOnboardingRow({ order: item.order, tx: item.tx, subscriptionId: arb.subscriptionId }), subscriptionId: arb.subscriptionId });
        plan.onboarding.subIds.add(arb.subscriptionId);
        plan.onboarding.keys.add(onboardingKey);
      }
      results.push({ rowNumber: item.order.rowNumber, status: 'created', subscriptionId: arb.subscriptionId });
    } catch (err) {
      const reason = safeErrorMessage(err);
      item.fields['ARB Creation Status'] = `Failed — ${reason}`;
      item.fields.Notes = appendNote(item.fields.Notes, `ARB failed; not routed to onboarding: ${reason}`);
      const rowUpdate = plan.rowUpdates.find(update => update.rowNumber === item.order.rowNumber);
      if (rowUpdate) {
        rowUpdate.fields['Sub Created'] = '';
        rowUpdate.fields['Payment Status'] = 'Review — ARB failed';
        rowUpdate.fields['Route Target'] = 'Review / Payment Issue';
        rowUpdate.fields['Review Status'] = `ARB failed: ${reason}`;
        rowUpdate.fields['Connector Notes'] = appendNote(rowUpdate.fields['Connector Notes'], `ARB failed; not onboarded: ${reason}`);
      }
      results.push({ rowNumber: item.order.rowNumber, status: 'failed', reason });
    }
  }
  return results;
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

async function applyPlan({ sheets, billingSpreadsheetId, subscriptionsSpreadsheetId, onboardingSpreadsheetId, newOrderRows, conversionRows, plan, auth, triggeredBy }) {
  const newOrderHeaders = withMissingHeaders((newOrderRows[0] || []).slice(), [...NEW_ORDER_REQUIRED_HEADERS, ...NEW_ORDER_OPTIONAL_HEADERS]);
  const newOrderMap = headerMap(newOrderHeaders);
  const conversionHeaders = withMissingHeaders(normalizeConversionHeaderRow((conversionRows[0] || []).slice()), CONVERSION_HEADERS);

  const valueUpdates = [];
  plan.rowUpdates.forEach(update => {
    const source = (newOrderRows[update.rowNumber - 1] || []).slice();
    while (source.length < newOrderHeaders.length) source.push('');
    Object.entries(update.fields).forEach(([header, value]) => setCell(source, newOrderMap, header, value));
    valueUpdates.push({
      range: `${quoteSheetName(TABS.billingNewOrders)}!A${update.rowNumber}:${colLetter(newOrderHeaders.length)}${update.rowNumber}`,
      values: [source.slice(0, newOrderHeaders.length)]
    });
  });

  const conversionAppends = [];
  plan.conversionUpserts.forEach(item => {
    const row = buildRowFromFields(conversionHeaders, item.fields, item.existingRow || []);
    if (item.rowNumber) {
      valueUpdates.push({
        range: `${quoteSheetName(TABS.subscriptionConversions)}!A${item.rowNumber}:${colLetter(conversionHeaders.length)}${item.rowNumber}`,
        values: [row]
      });
    } else {
      conversionAppends.push(row);
    }
  });

  if (valueUpdates.length) {
    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: billingSpreadsheetId,
      requestBody: { valueInputOption: 'USER_ENTERED', data: valueUpdates }
    });
  }
  if (conversionAppends.length) {
    await sheets.spreadsheets.values.append({
      spreadsheetId: billingSpreadsheetId,
      range: `${quoteSheetName(TABS.subscriptionConversions)}!A:${colLetter(conversionHeaders.length)}`,
      valueInputOption: 'USER_ENTERED',
      insertDataOption: 'INSERT_ROWS',
      requestBody: { values: conversionAppends }
    });
  }

  let activeApplied = false;
  if (plan.activeInserts.length && subscriptionsSpreadsheetId) {
    const meta = await spreadsheetMeta(sheets, subscriptionsSpreadsheetId);
    const props = sheetProps(meta, TABS.active);
    if (!props) throw new Error('Missing FF Subscriptions / Active Subscriptions tab');
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: subscriptionsSpreadsheetId,
      requestBody: { requests: [{ insertDimension: { range: { sheetId: props.sheetId, dimension: 'ROWS', startIndex: 1, endIndex: 1 + plan.activeInserts.length }, inheritFromBefore: false } }] }
    });
    await sheets.spreadsheets.values.update({
      spreadsheetId: subscriptionsSpreadsheetId,
      range: `${quoteSheetName(TABS.active)}!A2:${colLetter(ACTIVE_HEADERS.length)}${1 + plan.activeInserts.length}`,
      valueInputOption: 'USER_ENTERED',
      requestBody: { values: plan.activeInserts.map(item => item.row) }
    });
    activeApplied = true;
  }

  let onboardingApplied = false;
  let onboardingNote = '';
  if (plan.onboardingInserts.length && onboardingSpreadsheetId) {
    try {
      const meta = await spreadsheetMeta(sheets, onboardingSpreadsheetId);
      const props = sheetProps(meta, TABS.onboardingEmails);
      if (!props) throw new Error('Missing Onboarding Emails tab');
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: onboardingSpreadsheetId,
        requestBody: { requests: [{ insertDimension: { range: { sheetId: props.sheetId, dimension: 'ROWS', startIndex: 3, endIndex: 3 + plan.onboardingInserts.length }, inheritFromBefore: false } }] }
      });
      await sheets.spreadsheets.values.update({
        spreadsheetId: onboardingSpreadsheetId,
        range: `${quoteSheetName(TABS.onboardingEmails)}!A4:${colLetter(ONBOARDING_HEADERS.length)}${3 + plan.onboardingInserts.length}`,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: plan.onboardingInserts.map(item => item.row) }
      });
      onboardingApplied = true;
    } catch (err) {
      onboardingNote = `Onboarding route skipped/error: ${safeErrorMessage(err)}`;
    }
  } else if (plan.onboardingInserts.length) {
    onboardingNote = 'Onboarding route skipped: FF_ONBOARDING_SPREADSHEET_ID not configured.';
  }

  await sheets.spreadsheets.values.append({
    spreadsheetId: billingSpreadsheetId,
    range: `${quoteSheetName(TABS.billingAuditLog)}!A:${colLetter(BILLING_AUDIT_HEADERS.length)}`,
    valueInputOption: 'USER_ENTERED',
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values: [[
      nowIso(),
      triggeredBy,
      'Auth.Net new membership order sync',
      TABS.billingNewOrders,
      '',
      '',
      '',
      '',
      `updates=${plan.rowUpdates.length}; conversions=${plan.conversionUpserts.length}; active=${plan.activeInserts.length}; onboarding=${plan.onboardingInserts.length}`,
      `Auth.Net read-only transaction pull; ARB live mutations only when explicit gates are enabled. Detail errors=${auth.errors.length}.`
    ]] }
  });

  return { activeApplied, onboardingApplied, onboardingNote, conversionAppends: conversionAppends.length };
}

function collectRequiredTransactionIds(newOrderRows) {
  const table = tableFromValues(newOrderRows);
  const ids = new Set();
  table.rows.forEach(item => {
    const id = getCell(item.row, item.map, 'Auth.Net Transaction ID');
    if (id) ids.add(id);
  });
  return Array.from(ids);
}

async function syncAuthNetNewOrders({
  mode = 'dry-run',
  triggeredBy = 'api',
  lookbackDays = 14,
  maxDetails = 2500,
  arbMode = 'dry-run',
  allowLiveArb = false
} = {}) {
  const dryRun = String(mode || '').toLowerCase() !== 'apply';
  const arbLiveRequested = String(arbMode || '').toLowerCase() === 'live';
  const arbLiveEnabled = !dryRun && arbLiveRequested && Boolean(allowLiveArb) && isArbAutoCreateEnabled();
  const billingSpreadsheetId = FF_BILLING_SPREADSHEET_ID();
  const subscriptionsSpreadsheetId = FF_SUBSCRIPTIONS_SPREADSHEET_ID();
  const onboardingSpreadsheetId = FF_ONBOARDING_SPREADSHEET_ID();
  if (!billingSpreadsheetId) throw new Error('Missing FF_BILLING_SPREADSHEET_ID');

  const sheets = await getSheetsClient();
  if (!dryRun) {
    await ensureHeaders(sheets, billingSpreadsheetId, TABS.billingNewOrders, [...NEW_ORDER_REQUIRED_HEADERS, ...NEW_ORDER_OPTIONAL_HEADERS]);
    await ensureHeaders(sheets, billingSpreadsheetId, TABS.subscriptionConversions, CONVERSION_HEADERS, { normalizeHeaders: normalizeConversionHeaderRow });
    await ensureHeaders(sheets, billingSpreadsheetId, TABS.billingAuditLog, BILLING_AUDIT_HEADERS);
    if (subscriptionsSpreadsheetId && arbLiveEnabled) await ensureHeaders(sheets, subscriptionsSpreadsheetId, TABS.active, ACTIVE_HEADERS);
  }

  const initialNewOrderRows = await readValues(sheets, billingSpreadsheetId, TABS.billingNewOrders, 'A:AZ');
  const requiredTransactionIds = collectRequiredTransactionIds(initialNewOrderRows);

  const [newOrderRows, conversionRows, activeRows, onboardingRows, auth] = await Promise.all([
    Promise.resolve(initialNewOrderRows),
    readValues(sheets, billingSpreadsheetId, TABS.subscriptionConversions, 'A:AZ'),
    subscriptionsSpreadsheetId ? readValues(sheets, subscriptionsSpreadsheetId, TABS.active, 'A:AZ').catch(() => []) : Promise.resolve([]),
    onboardingSpreadsheetId ? readValues(sheets, onboardingSpreadsheetId, TABS.onboardingEmails, 'A:P').catch(() => []) : Promise.resolve([]),
    pullRecentTransactions({ lookbackDays, maxDetails, requiredTransactionIds })
  ]);

  const discovery = discoverNewOrderRows({ newOrderRows, conversionRows, activeRows, onboardingRows, auth });
  const plannedNewOrderRows = discovery.discovered.length
    ? [discovery.headers, ...(newOrderRows || []).slice(1), ...discovery.discovered.map(item => item.row)]
    : newOrderRows;

  const plan = buildPlan({ newOrderRows: plannedNewOrderRows, conversionRows, activeRows, onboardingRows, auth });
  plan.newOrderDiscovery = discovery.discovered;
  const arbResults = !dryRun ? await maybeCreateArbs({ plan, arbLiveEnabled, arbLiveRequested }) : [];

  let applyResult = { activeApplied: false, onboardingApplied: false, onboardingNote: '', conversionAppends: 0 };
  if (!dryRun) {
    applyResult = await applyPlan({
      sheets,
      billingSpreadsheetId,
      subscriptionsSpreadsheetId,
      onboardingSpreadsheetId,
      newOrderRows: plannedNewOrderRows,
      conversionRows,
      plan,
      auth,
      triggeredBy
    });
  }

  const result = {
    ok: true,
    runAtUtc: nowIso(),
    mode: dryRun ? 'dry-run' : 'apply',
    triggeredBy,
    safety: arbLiveEnabled
      ? 'Live ARB creation enabled by explicit request + FF_BILLING_ARB_AUTO_CREATE_ENABLED. Guards block missing transaction ID, missing invoice, amount mismatch, unapproved transactions, ARB failures, and onboarding before ARB success. No customer emails, refunds, cancellations, or card/bank data handling.'
      : 'Read-only Authorize.Net transaction pull; sheet writes only when mode=apply. ARB creation is dry-run/blocked unless explicit live gates are enabled. No customer emails, refunds, cancellations, or card/bank data handling.',
    spreadsheets: {
      billing: billingSpreadsheetId,
      subscriptionsConfigured: Boolean(subscriptionsSpreadsheetId),
      onboardingConfigured: Boolean(onboardingSpreadsheetId)
    },
    authNet: {
      pulledAtUtc: auth.pulledAtUtc,
      listCounts: auth.listCounts,
      notes: auth.notes,
      detailErrorCount: auth.errors.length
    },
    guards: {
      arbLiveRequested,
      arbLiveEnabled,
      envLiveGate: isArbAutoCreateEnabled(),
      requestLiveGate: Boolean(allowLiveArb)
    },
    counts: {
      readyForConversion: plan.ready.length,
      newOrderAutoDiscovered: plan.newOrderDiscovery.length,
      newOrderUpdates: plan.rowUpdates.length,
      conversionUpserts: plan.conversionUpserts.length,
      conversionAppends: applyResult.conversionAppends,
      review: plan.review.length,
      skipped: plan.skipped.length,
      arbsCreated: arbResults.filter(item => item.status === 'created').length,
      arbsFailed: arbResults.filter(item => item.status === 'failed').length,
      activeInserts: plan.activeInserts.length,
      onboardingInserts: plan.onboardingInserts.length
    },
    applied: !dryRun,
    activeApplied: applyResult.activeApplied,
    onboardingApplied: applyResult.onboardingApplied,
    onboardingNote: applyResult.onboardingNote,
    samples: {
      autoDiscovered: plan.newOrderDiscovery.map(item => item.transaction).slice(0, 10),
      ready: plan.ready.slice(0, 10),
      review: plan.review.slice(0, 10),
      arbResults: arbResults.slice(0, 10),
      skipped: plan.skipped.slice(0, 10)
    }
  };

  return result;
}

module.exports = {
  syncAuthNetNewOrders,
  pullRecentTransactions,
  buildPlan,
  matchTransactionForOrder,
  validateOrderAgainstTransaction,
  isApprovedTransaction,
  isArbAutoCreateEnabled,
  isNewOrdersAutomationEnabled,
  newOrdersAutomationIntervalMs,
  newOrdersAutomationLookbackDays,
  newOrdersAutomationMaxDetails,
  __authNetNewOrdersTestHooks: {
    addDaysDateOnly,
    amountEqual,
    buildConversionFields,
    discoverNewOrderRows,
    isMembershipAmount,
    isNewMembershipCheckoutTransaction,
    isTruthy,
    matchTransactionForOrder,
    parseAmount,
    sanitizeTransaction,
    transactionAmount,
    transactionEmail,
    transactionId,
    transactionInvoice,
    validateOrderAgainstTransaction
  }
};
