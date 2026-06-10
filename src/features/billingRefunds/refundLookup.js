const crypto = require('crypto');
const { getSheetsClient } = require('../../core/googleSheets');
const {
  getSubscription,
  getTransactionDetails,
  getTransactionListForCustomer
} = require('../../connectors/authnet/client');

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

const BILLING_TABS = ['Refunds', 'New Orders', 'Subscription Conversions', 'Direct Charges', 'Invoices', 'Receipts', 'Payment Links'];
const SUBSCRIPTION_TABS = [
  'Active Subscriptions',
  'Recovered Subs',
  'Payment on Hold',
  'Terminated',
  'Cancellations',
  'AuthNet_Active_Subscriptions_Live'
];

const REFUND_HEADERS = [
  'Requested At', 'Lookup', 'Customer ID', 'Name', 'Email', 'Subscription ID', 'Original Transaction ID', 'Invoice #',
  'Original Transaction Date', 'Original Amount', 'Already Refunded', 'Refundable Amount', 'Refund Type', 'Refund Amount',
  'Reason', 'Refund Status', 'Approved By', 'Refund Transaction ID', 'Processed At', 'Customer Email Status', 'Email Sent At',
  'Candidate Count', 'Selected Candidate #', 'Last AuthNet Check At', 'Notes / Audit Log'
];

function nowIso() {
  return new Date().toISOString();
}

function normalizeString(value) {
  return String(value == null ? '' : value).trim();
}

function normalizeEmail(value) {
  return normalizeString(value).toLowerCase();
}

function normHeader(value) {
  return normalizeString(value).toLowerCase().replace(/[^a-z0-9]+/g, '');
}

function headerMap(headers) {
  const map = new Map();
  (headers || []).forEach((header, i) => {
    const key = normHeader(header);
    if (key && !map.has(key)) map.set(key, i);
  });
  return map;
}

function getCell(row, map, header) {
  const idx = map.get(normHeader(header));
  return idx == null ? '' : normalizeString(row[idx]);
}

function firstCell(row, map, headers) {
  for (const header of headers) {
    const value = getCell(row, map, header);
    if (value) return value;
  }
  return '';
}

function parseAmount(value) {
  const text = normalizeString(value).replace(/[$,]/g, '');
  const match = text.match(/-?\d+(?:\.\d+)?/);
  if (!match) return '';
  const amount = Number(match[0]);
  if (!Number.isFinite(amount)) return '';
  return amount.toFixed(2);
}

function amountNumber(value) {
  const parsed = parseAmount(value);
  return parsed ? Number(parsed) : 0;
}

function money(value) {
  const num = Number(value || 0);
  if (!Number.isFinite(num)) return '0.00';
  return Math.max(0, num).toFixed(2);
}

function quoteSheetName(sheetName) {
  return `'${String(sheetName).replace(/'/g, "''")}'`;
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

async function readValues(sheets, spreadsheetId, tab, range = 'A:ZZ') {
  if (!spreadsheetId) return [];
  try {
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range: `${quoteSheetName(tab)}!${range}`,
      valueRenderOption: 'FORMATTED_VALUE'
    });
    return response.data.values || [];
  } catch (err) {
    const message = String(err.message || err);
    if (/Unable to parse range|not found|Requested entity was not found|404/.test(message)) return [];
    throw err;
  }
}

async function readTables(sheets, { billingSpreadsheetId = FF_BILLING_SPREADSHEET_ID(), subscriptionsSpreadsheetId = FF_SUBSCRIPTIONS_SPREADSHEET_ID() } = {}) {
  const tables = [];
  for (const tab of BILLING_TABS) {
    const values = await readValues(sheets, billingSpreadsheetId, tab);
    if (values.length) tables.push({ workbook: 'FF Billing', spreadsheetId: billingSpreadsheetId, tab, ...tableFromValues(values) });
  }
  if (subscriptionsSpreadsheetId) {
    for (const tab of SUBSCRIPTION_TABS) {
      const values = await readValues(sheets, subscriptionsSpreadsheetId, tab);
      if (values.length) tables.push({ workbook: 'FF Subscriptions', spreadsheetId: subscriptionsSpreadsheetId, tab, ...tableFromValues(values) });
    }
  }
  return tables;
}

function emailHash(value) {
  const email = normalizeEmail(value);
  return email ? crypto.createHash('sha256').update(email).digest('hex').slice(0, 12) : '';
}

function looksLikeEmail(value) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(normalizeEmail(value));
}

function isLikelyTransactionId(value) {
  return /^\d{9,20}$/.test(normalizeString(value));
}

function isLikelySubscriptionId(value) {
  return /^\d{6,12}$/.test(normalizeString(value));
}

function addSet(set, value) {
  const text = normalizeString(value);
  if (text) set.add(text);
}

function rowRefFromSheet(table, item) {
  const row = item.row;
  const map = item.map;
  const subscriptionId = firstCell(row, map, ['Subscription ID', 'New Subscription ID', 'Auth.Net Subscription ID', 'Subscription']);
  return {
    workbook: table.workbook,
    tab: table.tab,
    rowNumber: item.rowNumber,
    email: normalizeEmail(firstCell(row, map, ['Email', 'Billing Email', 'Customer Email', 'Alt Email'])),
    customerId: firstCell(row, map, ['Customer ID', 'CustomerId', 'Customer']),
    name: firstCell(row, map, ['Name', 'Customer Name', 'Business Name']),
    subscriptionId,
    subscriptionStatus: subscriptionId ? firstCell(row, map, [
      'Subscription Status', 'Sub Status', 'Auth.Net Subscription Status', 'AuthNet Subscription Status',
      'ARB Status', 'Auth.Net Status', 'AuthNet Status', 'Status', 'Payment Status', 'Cancellation Status'
    ]) : '',
    transactionId: firstCell(row, map, ['Auth.Net Transaction ID', 'Transaction ID', 'Original Transaction ID', 'Refund Transaction ID']),
    invoiceNumber: firstCell(row, map, ['Order / Invoice #', 'Invoice #', 'Invoice Number', 'Original Invoice #']),
    amount: firstCell(row, map, ['Amount', 'Original Amount', 'Refund Amount'])
  };
}

function refMatchesLookup(ref, lookup) {
  const text = normalizeString(lookup);
  const lower = text.toLowerCase();
  if (!text) return false;
  if (looksLikeEmail(text)) return ref.email === lower;
  return [ref.customerId, ref.subscriptionId, ref.transactionId, ref.invoiceNumber]
    .map(value => normalizeString(value).toLowerCase())
    .includes(lower);
}

function collectSheetReferences(tables, lookup) {
  const refs = [];
  const txIds = new Set();
  const subIds = new Set();
  const customerIds = new Set();
  const emails = new Set();
  const names = new Set();
  const invoices = new Set();

  tables.forEach(table => {
    table.rows.forEach(item => {
      const ref = rowRefFromSheet(table, item);
      if (!refMatchesLookup(ref, lookup)) return;
      refs.push(ref);
      addSet(txIds, ref.transactionId);
      addSet(subIds, ref.subscriptionId);
      addSet(customerIds, ref.customerId);
      addSet(emails, ref.email);
      addSet(names, ref.name);
      addSet(invoices, ref.invoiceNumber);
    });
  });

  const raw = normalizeString(lookup);
  if (looksLikeEmail(raw)) addSet(emails, raw.toLowerCase());
  if (isLikelyTransactionId(raw)) addSet(txIds, raw);
  if (isLikelySubscriptionId(raw)) addSet(subIds, raw);

  return {
    refs,
    txIds,
    subIds,
    customerIds,
    emails,
    names,
    invoices
  };
}

function transactionId(tx) {
  return normalizeString(tx?.transId || tx?.transactionId || tx?.id);
}

function transactionInvoice(tx) {
  return normalizeString(tx?.order?.invoiceNumber || tx?.invoiceNumber);
}

function transactionStatus(tx) {
  return normalizeString(tx?.transactionStatus || tx?.status);
}

function transactionType(tx) {
  return normalizeString(tx?.transactionType || tx?.type);
}

function transactionAmount(tx) {
  return parseAmount(tx?.settleAmount ?? tx?.authAmount ?? tx?.authorizeAmount ?? tx?.amount);
}

function transactionEmail(tx) {
  return normalizeEmail(tx?.customer?.email || tx?.billTo?.email || tx?.shipTo?.email);
}

function transactionSubmitTime(tx) {
  return normalizeString(tx?.submitTimeUTC || tx?.submitTimeLocal || tx?.settleTimeUTC || tx?.createdAt);
}

function transactionSubscriptionId(tx) {
  const subscription = tx?.subscription || {};
  return normalizeString(subscription.id || subscription.subscriptionId);
}

function subscriptionObject(subscriptionResponse) {
  return subscriptionResponse?.subscription || subscriptionResponse || {};
}

function subscriptionStatus(subscriptionResponse) {
  const subscription = subscriptionObject(subscriptionResponse);
  return normalizeString(subscription.status || subscription.subscriptionStatus || subscription.arbStatus || subscription.state);
}

function subscriptionIdFromResponse(subscriptionResponse) {
  const subscription = subscriptionObject(subscriptionResponse);
  return normalizeString(subscription.id || subscription.subscriptionId);
}

function subscriptionRefundProfile(subscriptionResponse) {
  const subscription = subscriptionObject(subscriptionResponse);
  const profile = subscription.profile || {};
  const paymentProfile = profile.paymentProfile || {};
  const customerProfileId = normalizeString(profile.customerProfileId || subscription.customerProfileId);
  const customerPaymentProfileId = normalizeString(
    paymentProfile.customerPaymentProfileId
    || paymentProfile.paymentProfileId
    || profile.customerPaymentProfileId
    || subscription.customerPaymentProfileId
  );
  if (!customerProfileId || !customerPaymentProfileId) return null;
  return { customerProfileId, customerPaymentProfileId };
}

function transactionCustomerProfileId(tx) {
  return normalizeString(tx?.profile?.customerProfileId || tx?.customer?.customerProfileId);
}

function transactionBillToName(tx) {
  const billTo = tx?.billTo || {};
  const first = normalizeString(billTo.firstName);
  const last = normalizeString(billTo.lastName);
  return [first, last].filter(Boolean).join(' ').trim();
}

function refundRequiredFieldsFromTransaction(tx) {
  const customer = tx?.customer || {};
  const billTo = tx?.billTo || {};
  const clean = value => normalizeString(value);
  const pick = (source, keys) => Object.fromEntries(keys
    .map(key => [key, clean(source?.[key])])
    .filter(([, value]) => value));
  const customerPayload = pick(customer, ['id', 'email']);
  const billToPayload = pick(billTo, ['firstName', 'lastName', 'company', 'address', 'city', 'state', 'zip', 'country', 'phoneNumber']);
  return {
    customer: Object.keys(customerPayload).length ? customerPayload : null,
    billTo: Object.keys(billToPayload).length ? billToPayload : null
  };
}

function cardLast4(tx) {
  const payment = tx?.payment || {};
  const creditCard = payment.creditCard || {};
  const raw = normalizeString(creditCard.cardNumber || payment.accountNumber || tx?.accountNumber);
  const digits = raw.replace(/\D/g, '');
  return digits.length >= 4 ? digits.slice(-4) : '';
}

function isRefundOrVoidTransaction(tx) {
  const text = `${transactionType(tx)} ${transactionStatus(tx)}`.toLowerCase();
  return /refund|void/.test(text);
}

function isSettledForRefund(tx) {
  const status = transactionStatus(tx).toLowerCase();
  const type = transactionType(tx).toLowerCase();
  if (/refund|void/.test(type) || /voided|refund/.test(status)) return false;
  return /settledsuccessfully|settled successfully|settled/.test(status) && !/pending/.test(status);
}

function possibleRefundAmountFromObject(obj) {
  if (!obj || typeof obj !== 'object') return 0;
  const typeText = `${normalizeString(obj.transactionType)} ${normalizeString(obj.type)} ${normalizeString(obj.transactionStatus)} ${normalizeString(obj.status)}`.toLowerCase();
  const idText = normalizeString(obj.transId || obj.transactionId || obj.id);
  if (/refund/.test(typeText) || idText) {
    const amount = amountNumber(obj.settleAmount ?? obj.authAmount ?? obj.amount ?? obj.refundAmount);
    if (amount > 0) return amount;
  }
  return 0;
}

function authNetRefundTotal(tx) {
  let total = 0;
  const arrays = [
    tx?.refunds,
    tx?.refund,
    tx?.refundTransactions,
    tx?.returnedItems,
    tx?.relatedTransactions
  ];
  arrays.forEach(value => {
    const rows = Array.isArray(value) ? value : (value ? [value] : []);
    rows.forEach(item => {
      total += possibleRefundAmountFromObject(item);
    });
  });
  return Number(total.toFixed(2));
}

function sheetRefundTotal(tables, originalTransactionId) {
  if (!originalTransactionId) return 0;
  const refunds = tables.find(table => table.workbook === 'FF Billing' && table.tab === 'Refunds');
  if (!refunds) return 0;
  let total = 0;
  refunds.rows.forEach(item => {
    const original = firstCell(item.row, item.map, ['Original Transaction ID', 'Auth.Net Transaction ID', 'Transaction ID']);
    if (normalizeString(original) !== normalizeString(originalTransactionId)) return;
    const status = firstCell(item.row, item.map, ['Refund Status', 'Status', 'Approval Status']).toLowerCase();
    const refundTx = firstCell(item.row, item.map, ['Refund Transaction ID']);
    if (!/refunded|settled|approved|processed/.test(status) && !refundTx) return;
    total += amountNumber(firstCell(item.row, item.map, ['Refund Amount', 'Amount']));
  });
  return Number(total.toFixed(2));
}

function subscriptionStatusForCandidate(tx, matchedRefs, subscriptionStatusById = new Map()) {
  const subId = transactionSubscriptionId(tx);
  const id = transactionId(tx);
  const invoice = transactionInvoice(tx);
  if (subId && subscriptionStatusById.has(subId)) return subscriptionStatusById.get(subId);

  const exactRef = matchedRefs.find(ref => ref.subscriptionStatus && (
    (subId && ref.subscriptionId === subId)
    || (id && ref.transactionId === id)
    || (invoice && ref.invoiceNumber === invoice)
  ));
  if (exactRef) return exactRef.subscriptionStatus;

  const anySubRef = matchedRefs.find(ref => ref.subscriptionStatus && ref.subscriptionId);
  return anySubRef ? anySubRef.subscriptionStatus : '';
}

function sanitizeDetailForCandidate(tx, { tables = [], candidateNumber = 0, source = '', matchedRefs = [], subscriptionStatusById = new Map(), refundProfileBySubscriptionId = new Map() } = {}) {
  const id = transactionId(tx);
  const originalAmount = amountNumber(transactionAmount(tx));
  const authNetRefunded = authNetRefundTotal(tx);
  const sheetRefunded = sheetRefundTotal(tables, id);
  const alreadyRefunded = Math.max(authNetRefunded, sheetRefunded);
  const refundableAmount = Math.max(0, originalAmount - alreadyRefunded);
  const last4 = cardLast4(tx);
  const settled = isSettledForRefund(tx);
  const refundOrVoid = isRefundOrVoidTransaction(tx);
  const issues = [];
  if (!id) issues.push('missing transaction id');
  if (refundOrVoid) issues.push('transaction is already a refund/void');
  if (!settled) issues.push('transaction is not settled/refundable yet');
  if (!originalAmount) issues.push('missing original amount');
  if (refundableAmount <= 0) issues.push('no refundable balance remains');
  if (!last4) issues.push('card last4 not available from Auth.Net detail');

  const email = transactionEmail(tx) || matchedRefs.find(ref => ref.email)?.email || '';
  const name = transactionBillToName(tx) || matchedRefs.find(ref => ref.name)?.name || '';
  const customerId = matchedRefs.find(ref => ref.customerId)?.customerId || '';
  const subscriptionId = transactionSubscriptionId(tx) || matchedRefs.find(ref => ref.subscriptionId)?.subscriptionId || '';

  const candidate = {
    candidateNumber,
    refundable: issues.length === 0,
    blockReason: issues.join('; '),
    transactionId: id,
    invoiceNumber: transactionInvoice(tx) || matchedRefs.find(ref => ref.invoiceNumber)?.invoiceNumber || '',
    transactionDate: transactionSubmitTime(tx),
    status: transactionStatus(tx),
    transactionType: transactionType(tx),
    originalAmount: money(originalAmount),
    alreadyRefunded: money(alreadyRefunded),
    refundableAmount: money(refundableAmount),
    emailHash: emailHash(email),
    email,
    name,
    customerId,
    subscriptionId,
    subscriptionStatus: subscriptionStatusForCandidate(tx, matchedRefs, subscriptionStatusById),
    cardLast4: last4,
    refundProfileAvailable: Boolean(subscriptionId && refundProfileBySubscriptionId.has(subscriptionId)),
    source,
    sourceRows: matchedRefs.slice(0, 10).map(ref => ({ workbook: ref.workbook, tab: ref.tab, rowNumber: ref.rowNumber }))
  };
  if (subscriptionId && refundProfileBySubscriptionId.has(subscriptionId)) {
    Object.defineProperty(candidate, '__refundProfile', {
      value: refundProfileBySubscriptionId.get(subscriptionId),
      enumerable: false,
      configurable: false
    });
  }
  const requiredFields = refundRequiredFieldsFromTransaction(tx);
  if (requiredFields.customer || requiredFields.billTo) {
    Object.defineProperty(candidate, '__refundRequiredFields', {
      value: requiredFields,
      enumerable: false,
      configurable: false
    });
  }
  return candidate;
}

function collectTransactionIdsFromSubscription(subscriptionResponse) {
  const ids = new Set();
  const subscription = subscriptionObject(subscriptionResponse);
  const transactionContainers = [
    subscription.transactions,
    subscription.transaction,
    subscription.arbTransactions,
    subscription.payments
  ];
  transactionContainers.forEach(value => {
    const rows = Array.isArray(value) ? value : (value ? [value] : []);
    rows.forEach(tx => addSet(ids, tx?.transId || tx?.transactionId || tx?.id));
  });
  return {
    ids,
    id: subscriptionIdFromResponse(subscriptionResponse),
    status: subscriptionStatus(subscriptionResponse),
    customerProfileId: normalizeString(subscription?.profile?.customerProfileId || subscription?.customerProfileId),
    refundProfile: subscriptionRefundProfile(subscriptionResponse)
  };
}

async function safeGetTransactionDetail(transId, getTransactionDetailsFn, notes) {
  try {
    const detail = await getTransactionDetailsFn(transId);
    return detail?.transaction || detail || null;
  } catch (err) {
    notes.push(`Could not fetch transaction ${transId}: ${String(err.message || err).slice(0, 220)}`);
    return null;
  }
}

async function safeGetSubscription(subscriptionId, getSubscriptionFn, notes) {
  try {
    return await getSubscriptionFn(subscriptionId);
  } catch (err) {
    notes.push(`Could not fetch subscription ${subscriptionId}: ${String(err.message || err).slice(0, 220)}`);
    return null;
  }
}

async function safeGetCustomerTransactions(customerProfileId, getTransactionListForCustomerFn, notes) {
  if (!customerProfileId) return [];
  try {
    const data = await getTransactionListForCustomerFn(customerProfileId);
    return data?.transactions || data?.transactionList || [];
  } catch (err) {
    notes.push(`Could not fetch customer transaction list ${customerProfileId}: ${String(err.message || err).slice(0, 220)}`);
    return [];
  }
}

function refsForTransaction(refs, tx) {
  const id = transactionId(tx);
  const invoice = transactionInvoice(tx);
  const email = transactionEmail(tx);
  const subId = transactionSubscriptionId(tx);
  return refs.filter(ref => (
    (id && ref.transactionId === id)
    || (invoice && ref.invoiceNumber === invoice)
    || (email && ref.email === email)
    || (subId && ref.subscriptionId === subId)
  ));
}

async function lookupRefundCandidates({
  lookup,
  sheets,
  billingSpreadsheetId = FF_BILLING_SPREADSHEET_ID(),
  subscriptionsSpreadsheetId = FF_SUBSCRIPTIONS_SPREADSHEET_ID(),
  getTransactionDetailsFn = getTransactionDetails,
  getSubscriptionFn = getSubscription,
  getTransactionListForCustomerFn = getTransactionListForCustomer,
  maxDetails = 75
} = {}) {
  const text = normalizeString(lookup);
  if (!text) throw new Error('Lookup value is required');
  const notes = [];
  const sheetsClient = sheets || await getSheetsClient();
  const tables = await readTables(sheetsClient, { billingSpreadsheetId, subscriptionsSpreadsheetId });
  const collected = collectSheetReferences(tables, text);
  const txIds = new Set(collected.txIds);
  const customerProfileIds = new Set();
  const subscriptionStatusById = new Map();
  const refundProfileBySubscriptionId = new Map();

  for (const subId of Array.from(collected.subIds).slice(0, 25)) {
    const subscription = await safeGetSubscription(subId, getSubscriptionFn, notes);
    if (!subscription) continue;
    const fromSub = collectTransactionIdsFromSubscription(subscription);
    fromSub.ids.forEach(id => addSet(txIds, id));
    if (fromSub.status) subscriptionStatusById.set(fromSub.id || subId, fromSub.status);
    if (fromSub.refundProfile) refundProfileBySubscriptionId.set(fromSub.id || subId, fromSub.refundProfile);
    addSet(customerProfileIds, fromSub.customerProfileId);
  }

  const detailById = new Map();
  for (const txId of Array.from(txIds).slice(0, maxDetails)) {
    const tx = await safeGetTransactionDetail(txId, getTransactionDetailsFn, notes);
    if (!tx) continue;
    detailById.set(transactionId(tx) || txId, tx);
    addSet(customerProfileIds, transactionCustomerProfileId(tx));
  }

  for (const profileId of Array.from(customerProfileIds).slice(0, 10)) {
    const txList = await safeGetCustomerTransactions(profileId, getTransactionListForCustomerFn, notes);
    txList.slice(0, maxDetails).forEach(tx => addSet(txIds, transactionId(tx) || tx.transId));
  }

  for (const txId of Array.from(txIds).slice(0, maxDetails)) {
    if (detailById.has(txId)) continue;
    const tx = await safeGetTransactionDetail(txId, getTransactionDetailsFn, notes);
    if (tx) detailById.set(transactionId(tx) || txId, tx);
  }

  const candidates = [];
  Array.from(detailById.values()).forEach(tx => {
    const id = transactionId(tx);
    if (!id) return;
    const matchedRefs = refsForTransaction(collected.refs, tx);
    const candidate = sanitizeDetailForCandidate(tx, {
      tables,
      candidateNumber: candidates.length + 1,
      source: matchedRefs.length ? 'sheet+authnet' : 'authnet',
      matchedRefs,
      subscriptionStatusById,
      refundProfileBySubscriptionId
    });
    // Keep non-refundable matches in the response for safety/diagnosis, but sort refundable first below.
    candidates.push(candidate);
  });

  candidates.sort((a, b) => {
    if (a.refundable !== b.refundable) return a.refundable ? -1 : 1;
    return String(b.transactionDate || '').localeCompare(String(a.transactionDate || ''));
  });
  candidates.forEach((candidate, i) => { candidate.candidateNumber = i + 1; });

  return {
    ok: true,
    lookup: text,
    pulledAtUtc: nowIso(),
    counts: {
      sheetReferences: collected.refs.length,
      transactionIdsConsidered: txIds.size,
      candidates: candidates.length,
      refundable: candidates.filter(candidate => candidate.refundable).length
    },
    candidates,
    references: collected.refs.slice(0, 25),
    notes,
    safety: 'Lookup/dry-run only. No Auth.Net refund, void, charge, cancellation, ARB mutation, customer email, raw card/bank/profile data, or Returns operational edit.'
  };
}

function validateRefundSelection({ candidate, refundType, refundAmount }) {
  const type = normalizeString(refundType || 'FULL').toUpperCase();
  const requested = type === 'FULL' ? amountNumber(candidate?.refundableAmount) : amountNumber(refundAmount);
  const issues = [];
  if (!candidate) issues.push('missing selected transaction candidate');
  if (candidate && !candidate.refundable) issues.push(candidate.blockReason || 'selected transaction is not refundable');
  if (!['FULL', 'PARTIAL'].includes(type)) issues.push('refund type must be FULL or PARTIAL');
  if (!requested || requested <= 0) issues.push('refund amount must be greater than zero');
  if (candidate && requested > amountNumber(candidate.refundableAmount)) issues.push('refund amount exceeds refundable balance');
  return {
    ok: issues.length === 0,
    issues,
    refundType: type,
    refundAmount: money(requested)
  };
}

async function buildRefundDryRun({ lookup, transactionId: selectedTransactionId, candidateNumber, refundType = 'FULL', refundAmount = '', reason = '', customerEmail = false, ...deps } = {}) {
  const lookupResult = await lookupRefundCandidates({ lookup: lookup || selectedTransactionId, ...deps });
  let candidate = null;
  if (selectedTransactionId) {
    candidate = lookupResult.candidates.find(item => item.transactionId === normalizeString(selectedTransactionId));
  }
  if (!candidate && candidateNumber) {
    candidate = lookupResult.candidates.find(item => Number(item.candidateNumber) === Number(candidateNumber));
  }
  if (!candidate && lookupResult.candidates.length === 1) candidate = lookupResult.candidates[0];
  const validation = validateRefundSelection({ candidate, refundType, refundAmount });
  return {
    ok: validation.ok,
    status: validation.ok ? 'DRY-RUN OK' : 'BLOCKED / ERROR',
    pulledAtUtc: nowIso(),
    selected: candidate || null,
    refundType: validation.refundType,
    refundAmount: validation.refundAmount,
    reason: normalizeString(reason),
    customerEmailRequested: customerEmail === true,
    issues: validation.issues,
    liveRefundsEnabled: false,
    safety: 'Dry-run only. Live refund processing is not implemented/enabled in this build; no customer email is sent.'
  };
}

module.exports = {
  FF_BILLING_SPREADSHEET_ID,
  FF_SUBSCRIPTIONS_SPREADSHEET_ID,
  REFUND_HEADERS,
  buildRefundDryRun,
  lookupRefundCandidates,
  validateRefundSelection,
  __billingRefundsTestHooks: {
    amountNumber,
    cardLast4,
    collectSheetReferences,
    isSettledForRefund,
    parseAmount,
    sanitizeDetailForCandidate,
    sheetRefundTotal,
    tableFromValues
  }
};
