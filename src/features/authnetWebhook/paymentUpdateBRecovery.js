const {
  chargeCustomerPaymentProfile,
  getSubscription
} = require('../../connectors/authnet/client');

const PAYMENT_UPDATE_TAB = 'Payment Update';
const PAYMENT_HOLD_TAB = 'Payment on Hold';
const TICKETS_TAB = 'Payment Update Link Tickets';
const AUTH_TX_TAB = 'AuthNet_Transactions';
const ACTIVE_TAB = 'Active Subscriptions';
const STOP_WORK_TAB = 'Stop_Work_Feed';
const EMAIL_LOG_TAB = 'Payment Update Email Log';

const B_TYPE = 'SUB RECAPTURE B - Payment on Hold';
const PAYMENT_UPDATE_PENDING_STATUS = 'Card appears updated — payment still pending verification';
const TICKET_PENDING_STATUS = 'Card updated — catch-up pending';
const READY_STATUSES = new Set([
  'live link ready',
  'link ready',
  'payment update link ready',
  'card appears updated payment still pending verification'
]);
const FINAL_STOP_VALUES = new Set(['resolved', 'completed']);

function normalize(value) {
  return String(value || '').trim().toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
}

function nowIso() {
  return new Date().toISOString();
}

function envFlag(name, defaultValue = false) {
  const raw = process.env[name];
  if (raw === undefined || raw === null || String(raw).trim() === '') return defaultValue;
  return ['1', 'true', 'yes', 'on'].includes(String(raw).trim().toLowerCase());
}

function isBDetectionEnabled() {
  // Step 2 safe mode: detection/sheet hold is enabled by default once v2 is deployed.
  // It can be disabled explicitly without changing code.
  return envFlag('AUTHNET_WEBHOOK_B_DETECT_ENABLED', true);
}

function isBChargeEnabled() {
  // Charging is intentionally a separate hard gate. The legacy catch-up flag is
  // not honored for live money movement, so deploying v2 cannot accidentally
  // charge because an old env var was left behind.
  return envFlag('AUTHNET_WEBHOOK_B_CHARGE_ENABLED', false);
}

function isPaymentProfileUpdatedEvent(eventType) {
  const normalized = normalize(eventType);
  return normalized.includes('paymentprofile') && normalized.includes('updated');
}

function firstString(...values) {
  for (const value of values) {
    if (value === null || value === undefined) continue;
    const text = String(value).trim();
    if (text) return text;
  }
  return '';
}

function nestedValue(obj, path) {
  let cursor = obj || {};
  for (const part of String(path || '').split('.')) {
    if (!cursor || typeof cursor !== 'object' || !(part in cursor)) return '';
    cursor = cursor[part];
  }
  return firstString(cursor);
}

function firstNested(obj, paths) {
  for (const path of paths) {
    const value = nestedValue(obj, path);
    if (value) return value;
  }
  return '';
}

function extractProfileIds(body) {
  const payload = body && typeof body === 'object' ? (body.payload || {}) : {};
  return {
    customerProfileId: firstString(
      payload.customerProfileId,
      payload.customerProfileID,
      firstNested(payload, ['customer.customerProfileId', 'profile.customerProfileId', 'paymentProfile.customerProfileId'])
    ),
    customerPaymentProfileId: firstString(
      payload.customerPaymentProfileId,
      payload.customerPaymentProfileID,
      payload.paymentProfileId,
      payload.paymentProfileID,
      firstNested(payload, ['customerPaymentProfileId', 'paymentProfile.customerPaymentProfileId', 'paymentProfile.paymentProfileId'])
    )
  };
}

function quoteSheetName(sheetName) {
  return `'${String(sheetName).replace(/'/g, "''")}'`;
}

function headerIndex(headers) {
  return Object.fromEntries(headers.map((header, i) => [String(header || '').trim(), i]));
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
  }).catch(err => {
    if (tabName === STOP_WORK_TAB) return { data: { values: [] } };
    throw err;
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

function appendNote(existing, note) {
  const text = String(note || '').trim();
  if (!text) return String(existing || '').trim();
  const prior = String(existing || '').trim();
  return prior ? `${prior}\n${text}` : text;
}

function parseAmount(value) {
  const raw = String(value == null ? '' : value).replace(/[$,]/g, '');
  const match = raw.match(/\d+(?:\.\d+)?/);
  if (!match) return '';
  const amount = Number(match[0]);
  if (!Number.isFinite(amount) || amount <= 0) return '';
  return amount.toFixed(2);
}

function displayMoney(amount) {
  const normalized = parseAmount(amount);
  if (!normalized) return '';
  return normalized.replace(/\.00$/, '');
}

function localMmdd(date = new Date()) {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/Los_Angeles',
    month: '2-digit',
    day: '2-digit'
  }).formatToParts(date);
  const month = parts.find(part => part.type === 'month')?.value || '01';
  const day = parts.find(part => part.type === 'day')?.value || '01';
  return `${month}${day}`;
}

function localDateIso(date = new Date()) {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Los_Angeles',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  }).formatToParts(date);
  const year = parts.find(part => part.type === 'year')?.value || '1970';
  const month = parts.find(part => part.type === 'month')?.value || '01';
  const day = parts.find(part => part.type === 'day')?.value || '01';
  return `${year}-${month}-${day}`;
}

function bInvoiceNumber(subscriptionId, date = new Date()) {
  return `BUPD-${subscriptionId}-${localMmdd(date)}`.slice(0, 20);
}

function isReadyBRow(row) {
  if (normalize(getField(row, 'Payment Update Type')) !== normalize(B_TYPE)) return false;
  const status = normalize(getField(row, 'Payment Update Status'));
  if (!READY_STATUSES.has(status)) return false;
  const stop = normalize(getField(row, 'Stop / Suppressed'));
  if (FINAL_STOP_VALUES.has(stop)) return false;
  return Boolean(getField(row, 'Subscription ID'));
}

function subscriptionProfileIds(subscriptionData) {
  const profile = subscriptionData?.subscription?.profile || {};
  const paymentProfile = profile.paymentProfile || {};
  return {
    customerProfileId: firstString(profile.customerProfileId),
    customerPaymentProfileId: firstString(paymentProfile.customerPaymentProfileId, paymentProfile.paymentProfileId),
    subscriptionStatus: firstString(subscriptionData?.subscription?.status),
    amount: firstString(subscriptionData?.subscription?.amount)
  };
}

async function findMatchingBRow({ paymentUpdateRows, profileIds, getSubscriptionFn = getSubscription }) {
  const candidates = paymentUpdateRows.filter(isReadyBRow);
  for (const row of candidates) {
    const subscriptionId = getField(row, 'Subscription ID');
    const subscriptionData = await getSubscriptionFn(subscriptionId);
    const ids = subscriptionProfileIds(subscriptionData);
    if (
      ids.customerProfileId &&
      ids.customerPaymentProfileId &&
      String(ids.customerProfileId) === String(profileIds.customerProfileId) &&
      String(ids.customerPaymentProfileId) === String(profileIds.customerPaymentProfileId)
    ) {
      return { row, subscriptionData, profileIds: ids };
    }
  }
  return null;
}

function chooseHoldRow(holdRows, subscriptionId) {
  const rows = holdRows.filter(row => getField(row, 'Subscription ID') === String(subscriptionId));
  if (!rows.length) return null;
  const unresolved = rows.filter(row => !FINAL_STOP_VALUES.has(normalize(getField(row, 'Stop / Suppressed'))));
  return (unresolved.length ? unresolved : rows).sort((a, b) => Number(b._rowNumber) - Number(a._rowNumber))[0];
}

function ticketIdFromLink(link) {
  return String(link || '').trim().replace(/\/$/, '').split('/').pop() || '';
}

function findTicket(tickets, paymentUpdateRow) {
  const linkTicketId = ticketIdFromLink(getField(paymentUpdateRow, 'Payment Update Link'));
  const subscriptionId = getField(paymentUpdateRow, 'Subscription ID');
  if (linkTicketId) {
    const byLink = tickets.find(row => getField(row, 'Ticket ID') === linkTicketId && getField(row, 'Subscription ID') === subscriptionId);
    if (byLink) return byLink;
  }
  return tickets.find(row => getField(row, 'Subscription ID') === subscriptionId) || null;
}

function invoiceExists(authTxRows, invoiceNumber) {
  return authTxRows.some(row => getField(row, 'Invoice Number') === invoiceNumber);
}

function chargeSummary(response) {
  const tx = response?.transactionResponse || {};
  const messages = Array.isArray(tx.messages) ? tx.messages : [];
  const errors = Array.isArray(tx.errors) ? tx.errors : [];
  return {
    approved: String(tx.responseCode || '') === '1',
    responseCode: String(tx.responseCode || ''),
    transactionId: String(tx.transId || ''),
    authCode: String(tx.authCode || ''),
    message: firstString(
      ...messages.map(item => item.description || item.code),
      ...errors.map(item => item.errorText || item.errorCode),
      response?.messages?.message?.[0]?.text
    )
  };
}

async function updateRow(sheets, spreadsheetId, tabName, headers, rowNumber, values) {
  const endColumn = columnName(headers.length);
  await sheets.spreadsheets.values.update({
    spreadsheetId,
    range: `${quoteSheetName(tabName)}!A${rowNumber}:${endColumn}${rowNumber}`,
    valueInputOption: 'USER_ENTERED',
    requestBody: { values: [values.slice(0, headers.length)] }
  });
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

async function appendRow(sheets, spreadsheetId, tabName, values) {
  await sheets.spreadsheets.values.append({
    spreadsheetId,
    range: `${quoteSheetName(tabName)}!A:ZZ`,
    valueInputOption: 'USER_ENTERED',
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values: [values] }
  });
}

function buildEmailLogRow({ eventAt, status, puRow, ticket, reason, mode = 'authnet-profile-webhook-b-catchup' }) {
  return [
    eventAt,
    mode,
    status,
    puRow._rowNumber,
    getField(puRow, 'Payment Update Type'),
    getField(puRow, 'Source Tab'),
    getField(puRow, 'Source Row'),
    '',
    getField(puRow, 'Name'),
    getField(puRow, 'Email'),
    '',
    getField(puRow, 'Subscription ID'),
    '',
    'system',
    ticket ? getField(ticket, 'Ticket ID') : ticketIdFromLink(getField(puRow, 'Payment Update Link')),
    getField(puRow, 'Payment Update Link'),
    B_TYPE,
    '',
    '',
    getField(puRow, 'Email 1 Sent At'),
    getField(puRow, 'Email 2 Sent At'),
    getField(puRow, 'Email 3 Sent At'),
    getField(puRow, 'Last Follow-Up At'),
    getField(puRow, 'Next Follow-Up Due'),
    reason
  ];
}

function buildAuthTxRow({ eventAt, charge, puRow, holdRow, invoiceNumber, amount }) {
  const name = getField(puRow, 'Name') || getField(holdRow, 'Name');
  const parts = name.split(/\s+/).filter(Boolean);
  const firstName = parts[0] || '';
  const lastName = parts.length > 1 ? parts.slice(1).join(' ') : '';
  return [
    eventAt,
    charge.transactionId || '',
    firstName,
    lastName,
    getField(puRow, 'Email') || getField(holdRow, 'Email'),
    amount,
    charge.approved ? 'settledSuccessfully' : 'declined',
    charge.responseCode || '',
    eventAt,
    charge.authCode || '',
    invoiceNumber,
    getField(puRow, 'Subscription ID'),
    JSON.stringify({ source: 'AUTHNET_WEBHOOK_B_CATCHUP', responseCode: charge.responseCode || '', message: charge.message || '' }).slice(0, 4000)
  ];
}

async function applyApprovedRecovery({ sheets, spreadsheetId, tables, puRow, holdRow, ticket, charge, invoiceNumber, amount, eventAt }) {
  const subscriptionId = getField(puRow, 'Subscription ID');
  const reason = `Authorize.Net payment profile updated; supervised B catch-up charge approved against updated card/payment profile; existing Auth.Net subscription ${subscriptionId} remains active. No replacement ARB created and no ARB canceled.`;

  await appendRow(sheets, spreadsheetId, AUTH_TX_TAB, buildAuthTxRow({ eventAt, charge, puRow, holdRow, invoiceNumber, amount }));

  const puValues = puRow._values.slice();
  setField(tables.paymentUpdate.headers, puValues, 'Payment Update Status', 'Completed — B catch-up charged / existing ARB active');
  setField(tables.paymentUpdate.headers, puValues, 'Stop / Suppressed', 'Resolved');
  setField(tables.paymentUpdate.headers, puValues, 'Stop Reason', reason);
  setField(tables.paymentUpdate.headers, puValues, 'Next Follow-Up Due', '');
  setField(tables.paymentUpdate.headers, puValues, 'Notes', appendNote(getField(puRow, 'Notes'), `[${eventAt}] ML: Resolution gate: ${reason} No further B payment-update follow-up emails will send.`));
  await updateRow(sheets, spreadsheetId, PAYMENT_UPDATE_TAB, tables.paymentUpdate.headers, puRow._rowNumber, puValues);

  const holdValues = holdRow._values.slice();
  setField(tables.paymentHold.headers, holdValues, 'Subscription Status', 'active/current after catch-up');
  setField(tables.paymentHold.headers, holdValues, 'Last Charge Status', 'catch-up approved');
  setField(tables.paymentHold.headers, holdValues, 'Last Charge Transaction ID', charge.transactionId || '');
  setField(tables.paymentHold.headers, holdValues, 'Last AuthNet Check At', eventAt);
  setField(tables.paymentHold.headers, holdValues, 'Stop / Suppressed', 'Resolved');
  setField(tables.paymentHold.headers, holdValues, 'Stop Reason', reason);
  setField(tables.paymentHold.headers, holdValues, 'Next Follow-Up Due', '');
  setField(tables.paymentHold.headers, holdValues, 'Notes', appendNote(getField(holdRow, 'Notes'), `[${eventAt}] ML: ${reason}`));
  await updateRow(sheets, spreadsheetId, PAYMENT_HOLD_TAB, tables.paymentHold.headers, holdRow._rowNumber, holdValues);

  if (ticket) {
    const ticketValues = ticket._values.slice();
    setField(tables.tickets.headers, ticketValues, 'Ticket Status', 'Completed — no follow-up needed');
    setField(tables.tickets.headers, ticketValues, 'Completed At', getField(ticket, 'Completed At') || eventAt);
    setField(tables.tickets.headers, ticketValues, 'Notes', appendNote(getField(ticket, 'Notes'), `[${eventAt}] ML: ${reason}`));
    await updateRow(sheets, spreadsheetId, TICKETS_TAB, tables.tickets.headers, ticket._rowNumber, ticketValues);
  }

  const activeExisting = tables.active.rows.find(row => getField(row, 'Subscription ID') === subscriptionId);
  const activeNote = `B Payment on Hold recovered by webhook catch-up charge; existing subscription ${subscriptionId} remains active.`;
  const activeHistory = `B catch-up charge - ${localDateIso()} - tx ${charge.transactionId || '(present)'}; existing sub ${subscriptionId} kept active; no replacement/cancel.`;
  if (activeExisting) {
    const activeValues = activeExisting._values.slice();
    setField(tables.active.headers, activeValues, 'Amount', displayMoney(amount));
    setField(tables.active.headers, activeValues, 'LTV', getField(activeExisting, 'LTV') || displayMoney(amount));
    setField(tables.active.headers, activeValues, 'Notes', appendNote(getField(activeExisting, 'Notes'), `[${eventAt}] ML: ${activeNote}`));
    setField(tables.active.headers, activeValues, 'History', appendNote(getField(activeExisting, 'History'), activeHistory));
    await updateRow(sheets, spreadsheetId, ACTIVE_TAB, tables.active.headers, activeExisting._rowNumber, activeValues);
  } else {
    await appendRow(sheets, spreadsheetId, ACTIVE_TAB, [
      localDateIso(),
      subscriptionId,
      getField(puRow, 'Customer ID') || getField(holdRow, 'Customer ID'),
      getField(puRow, 'Name') || getField(holdRow, 'Name'),
      getField(puRow, 'Email') || getField(holdRow, 'Email'),
      getField(puRow, 'Alt Email') || getField(holdRow, 'Alt Email'),
      displayMoney(amount),
      '',
      activeNote,
      displayMoney(amount),
      activeHistory
    ]);
  }

  const stopRows = tables.stopWork.rows.filter(row => getField(row, 'Subscription ID') === subscriptionId || getField(row, 'Customer ID') === getField(puRow, 'Customer ID'));
  for (const stopRow of stopRows) {
    const stopValues = stopRow._values.slice();
    setField(tables.stopWork.headers, stopValues, 'Stop Work?', 'FALSE');
    setField(tables.stopWork.headers, stopValues, 'Reason', 'Resolved / B catch-up charged; existing ARB active');
    setField(tables.stopWork.headers, stopValues, 'Pushed To Sheets?', 'FALSE');
    setField(tables.stopWork.headers, stopValues, 'Pushed At', '');
    setField(tables.stopWork.headers, stopValues, 'Notes', appendNote(getField(stopRow, 'Notes'), `${eventAt} [AUTHNET_WEBHOOK_B_CATCHUP] ${reason} Downstream sync may clear visibility-only FF Stop Work fields; no Returns filing/status outcome fields edited.`));
    await updateRow(sheets, spreadsheetId, STOP_WORK_TAB, tables.stopWork.headers, stopRow._rowNumber, stopValues);
  }

  await appendRow(sheets, spreadsheetId, EMAIL_LOG_TAB, buildEmailLogRow({
    eventAt,
    status: 'RESOLVED - B CATCH-UP CHARGED / EXISTING ARB ACTIVE',
    puRow,
    ticket,
    reason
  }));

  return { status: 'charged', reason, stopRowsUpdated: stopRows.length };
}

async function applyDeclinedRecovery({ sheets, spreadsheetId, tables, puRow, holdRow, ticket, charge, reason, eventAt }) {
  const fullReason = reason || 'B catch-up charge after payment-profile update was not approved. Existing ARB remains active; manual review needed.';
  const puValues = puRow._values.slice();
  setField(tables.paymentUpdate.headers, puValues, 'Payment Update Status', 'Catch-up charge declined — manual review needed');
  setField(tables.paymentUpdate.headers, puValues, 'Stop / Suppressed', 'Suppressed');
  setField(tables.paymentUpdate.headers, puValues, 'Stop Reason', fullReason);
  setField(tables.paymentUpdate.headers, puValues, 'Next Follow-Up Due', '');
  setField(tables.paymentUpdate.headers, puValues, 'Notes', appendNote(getField(puRow, 'Notes'), `[${eventAt}] ML: ${fullReason}`));
  await updateRow(sheets, spreadsheetId, PAYMENT_UPDATE_TAB, tables.paymentUpdate.headers, puRow._rowNumber, puValues);

  const holdValues = holdRow._values.slice();
  setField(tables.paymentHold.headers, holdValues, 'Last Charge Status', 'catch-up declined');
  setField(tables.paymentHold.headers, holdValues, 'Last AuthNet Check At', eventAt);
  setField(tables.paymentHold.headers, holdValues, 'Stop / Suppressed', 'Suppressed');
  setField(tables.paymentHold.headers, holdValues, 'Stop Reason', fullReason);
  setField(tables.paymentHold.headers, holdValues, 'Next Follow-Up Due', '');
  setField(tables.paymentHold.headers, holdValues, 'Notes', appendNote(getField(holdRow, 'Notes'), `[${eventAt}] ML: ${fullReason}`));
  await updateRow(sheets, spreadsheetId, PAYMENT_HOLD_TAB, tables.paymentHold.headers, holdRow._rowNumber, holdValues);

  if (ticket) {
    const ticketValues = ticket._values.slice();
    setField(tables.tickets.headers, ticketValues, 'Ticket Status', 'Catch-up charge declined — manual review needed');
    setField(tables.tickets.headers, ticketValues, 'Notes', appendNote(getField(ticket, 'Notes'), `[${eventAt}] ML: ${fullReason}`));
    await updateRow(sheets, spreadsheetId, TICKETS_TAB, tables.tickets.headers, ticket._rowNumber, ticketValues);
  }

  await appendRow(sheets, spreadsheetId, EMAIL_LOG_TAB, buildEmailLogRow({
    eventAt,
    status: 'B CATCH-UP CHARGE DECLINED',
    puRow,
    ticket,
    reason: fullReason
  }));

  return { status: 'declined', reason: fullReason, responseCode: charge.responseCode || '' };
}

async function applyPendingApprovalRecovery({ sheets, spreadsheetId, tables, puRow, holdRow, ticket, amount, eventAt }) {
  const subscriptionId = getField(puRow, 'Subscription ID');
  const reason = `Authorize.Net payment profile updated and existing Auth.Net subscription ${subscriptionId} is active. Follow-up emails suppressed pending Gil-approved B catch-up charge for $${amount}. No charge was run by webhook and no ARB was canceled or recreated.`;

  const puValues = puRow._values.slice();
  setField(tables.paymentUpdate.headers, puValues, 'Payment Update Status', PAYMENT_UPDATE_PENDING_STATUS);
  setField(tables.paymentUpdate.headers, puValues, 'Stop / Suppressed', 'Suppressed');
  setField(tables.paymentUpdate.headers, puValues, 'Stop Reason', reason);
  setField(tables.paymentUpdate.headers, puValues, 'Next Follow-Up Due', '');
  setField(tables.paymentUpdate.headers, puValues, 'Notes', appendNote(getField(puRow, 'Notes'), `[${eventAt}] ML: ${reason}`));
  await updateRow(sheets, spreadsheetId, PAYMENT_UPDATE_TAB, tables.paymentUpdate.headers, puRow._rowNumber, puValues);

  const holdValues = holdRow._values.slice();
  setField(tables.paymentHold.headers, holdValues, 'Subscription Status', 'active / card updated pending catch-up approval');
  setField(tables.paymentHold.headers, holdValues, 'Last Charge Status', 'card updated; catch-up pending approval');
  setField(tables.paymentHold.headers, holdValues, 'Last AuthNet Check At', eventAt);
  setField(tables.paymentHold.headers, holdValues, 'Stop / Suppressed', 'Suppressed');
  setField(tables.paymentHold.headers, holdValues, 'Stop Reason', reason);
  setField(tables.paymentHold.headers, holdValues, 'Next Follow-Up Due', '');
  setField(tables.paymentHold.headers, holdValues, 'Notes', appendNote(getField(holdRow, 'Notes'), `[${eventAt}] ML: ${reason}`));
  await updateRow(sheets, spreadsheetId, PAYMENT_HOLD_TAB, tables.paymentHold.headers, holdRow._rowNumber, holdValues);

  if (ticket) {
    const ticketValues = ticket._values.slice();
    setField(tables.tickets.headers, ticketValues, 'Ticket Status', TICKET_PENDING_STATUS);
    setField(tables.tickets.headers, ticketValues, 'Notes', appendNote(getField(ticket, 'Notes'), `[${eventAt}] ML: ${reason}`));
    await updateRow(sheets, spreadsheetId, TICKETS_TAB, tables.tickets.headers, ticket._rowNumber, ticketValues);
  }

  await appendRow(sheets, spreadsheetId, EMAIL_LOG_TAB, buildEmailLogRow({
    eventAt,
    status: 'B CARD UPDATED - CATCH-UP PENDING APPROVAL',
    puRow,
    ticket,
    reason,
    mode: 'authnet-profile-webhook-b-detect-no-charge'
  }));

  return { status: 'pending_approval', reason };
}

async function loadBRecoveryTables(sheets, spreadsheetId) {
  const [paymentUpdate, paymentHold, tickets, authTx, active, stopWork] = await Promise.all([
    readTable(sheets, spreadsheetId, PAYMENT_UPDATE_TAB, 'A:V'),
    readTable(sheets, spreadsheetId, PAYMENT_HOLD_TAB, 'A:U'),
    readTable(sheets, spreadsheetId, TICKETS_TAB, 'A:Q'),
    readTable(sheets, spreadsheetId, AUTH_TX_TAB, 'A:L'),
    readTable(sheets, spreadsheetId, ACTIVE_TAB, 'A:K'),
    readTable(sheets, spreadsheetId, STOP_WORK_TAB, 'A:L')
  ]);
  return { paymentUpdate, paymentHold, tickets, authTx, active, stopWork };
}

async function processBProfileUpdatedWebhook({
  body,
  sheets,
  spreadsheetId,
  detectEnabled = isBDetectionEnabled(),
  chargeEnabled = isBChargeEnabled(),
  getSubscriptionFn = getSubscription,
  chargeCustomerPaymentProfileFn = chargeCustomerPaymentProfile,
  date = new Date()
}) {
  const eventType = body && body.eventType;
  if (!isPaymentProfileUpdatedEvent(eventType)) {
    return { eligible: false, reason: 'not-payment-profile-updated-event' };
  }
  if (!detectEnabled) {
    return { eligible: true, detectEnabled: false, chargeEnabled, reason: 'b-detection-disabled' };
  }

  const incomingProfileIds = extractProfileIds(body);
  if (!incomingProfileIds.customerProfileId || !incomingProfileIds.customerPaymentProfileId) {
    return { eligible: true, detectEnabled, chargeEnabled, status: 'skipped', reason: 'missing-profile-ids' };
  }

  const tables = await loadBRecoveryTables(sheets, spreadsheetId);
  const match = await findMatchingBRow({
    paymentUpdateRows: tables.paymentUpdate.rows,
    profileIds: incomingProfileIds,
    getSubscriptionFn
  });
  if (!match) {
    return { eligible: true, detectEnabled, chargeEnabled, status: 'skipped', reason: 'no-ready-b-row-matched-profile' };
  }

  const puRow = match.row;
  const subscriptionId = getField(puRow, 'Subscription ID');
  if (
    !chargeEnabled &&
    normalize(getField(puRow, 'Payment Update Status')) === normalize(PAYMENT_UPDATE_PENDING_STATUS) &&
    normalize(getField(puRow, 'Stop / Suppressed')) === 'suppressed'
  ) {
    return { eligible: true, detectEnabled, chargeEnabled: false, status: 'skipped', subscriptionId, reason: 'already-pending-approval' };
  }

  if (normalize(match.profileIds.subscriptionStatus) !== 'active') {
    return { eligible: true, detectEnabled, chargeEnabled, status: 'blocked', subscriptionId, reason: 'subscription-not-active' };
  }
  const holdRow = chooseHoldRow(tables.paymentHold.rows, subscriptionId);
  if (!holdRow) {
    return { eligible: true, detectEnabled, chargeEnabled, status: 'blocked', subscriptionId, reason: 'no-payment-hold-row' };
  }

  const amount = parseAmount(getField(holdRow, 'Amount Due') || match.profileIds.amount || '');
  if (!amount) {
    return { eligible: true, detectEnabled, chargeEnabled, status: 'blocked', subscriptionId, reason: 'invalid-amount-due' };
  }

  const invoiceNumber = bInvoiceNumber(subscriptionId, date);
  if (invoiceExists(tables.authTx.rows, invoiceNumber)) {
    return { eligible: true, detectEnabled, chargeEnabled, status: 'skipped', subscriptionId, invoiceNumber, reason: 'invoice-already-recorded' };
  }

  const ticket = findTicket(tables.tickets.rows, puRow);
  const eventAt = nowIso();
  if (!chargeEnabled) {
    const applied = await applyPendingApprovalRecovery({
      sheets,
      spreadsheetId,
      tables,
      puRow,
      holdRow,
      ticket,
      amount,
      eventAt
    });
    return {
      eligible: true,
      detectEnabled,
      chargeEnabled: false,
      status: applied.status,
      subscriptionId,
      paymentUpdateRow: puRow._rowNumber,
      paymentHoldRow: holdRow._rowNumber,
      ticketRow: ticket ? ticket._rowNumber : null,
      invoiceNumber,
      existingArbKeptActive: true,
      authnetChargeAttempted: false,
      customerEmails: false,
      reason: applied.reason
    };
  }

  const chargeResponse = await chargeCustomerPaymentProfileFn({
    customerProfileId: incomingProfileIds.customerProfileId,
    customerPaymentProfileId: incomingProfileIds.customerPaymentProfileId,
    amount,
    invoiceNumber,
    description: 'Fast Filings Sales Tax Filing',
    customerEmail: getField(puRow, 'Email') || getField(holdRow, 'Email'),
    refId: `b-catchup-${subscriptionId}`
  });
  const charge = chargeSummary(chargeResponse);

  if (charge.approved) {
    const applied = await applyApprovedRecovery({
      sheets,
      spreadsheetId,
      tables,
      puRow,
      holdRow,
      ticket,
      charge,
      invoiceNumber,
      amount,
      eventAt
    });
    return {
      eligible: true,
      detectEnabled,
      chargeEnabled,
      status: applied.status,
      subscriptionId,
      paymentUpdateRow: puRow._rowNumber,
      paymentHoldRow: holdRow._rowNumber,
      ticketRow: ticket ? ticket._rowNumber : null,
      invoiceNumber,
      transactionIdPresent: Boolean(charge.transactionId),
      existingArbKeptActive: true,
      stopRowsUpdated: applied.stopRowsUpdated,
      reason: applied.reason
    };
  }

  const reason = `B catch-up charge after payment-profile update was not approved${charge.message ? `: ${charge.message}` : ''}. Existing ARB remains active; manual review needed.`;
  const applied = await applyDeclinedRecovery({
    sheets,
    spreadsheetId,
    tables,
    puRow,
    holdRow,
    ticket,
    charge,
    reason,
    eventAt
  });
  return {
      eligible: true,
      detectEnabled,
      chargeEnabled,
      status: applied.status,
    subscriptionId,
    paymentUpdateRow: puRow._rowNumber,
    paymentHoldRow: holdRow._rowNumber,
    ticketRow: ticket ? ticket._rowNumber : null,
    invoiceNumber,
    responseCode: applied.responseCode,
    existingArbKeptActive: true,
    reason: applied.reason
  };
}

module.exports = {
  B_TYPE,
  isBChargeEnabled,
  isBDetectionEnabled,
  processBProfileUpdatedWebhook,
  __bRecoveryTestHooks: {
    bInvoiceNumber,
    chargeSummary,
    chooseHoldRow,
    extractProfileIds,
    findMatchingBRow,
    PAYMENT_UPDATE_PENDING_STATUS,
    TICKET_PENDING_STATUS,
    isPaymentProfileUpdatedEvent,
    isReadyBRow,
    parseAmount,
    subscriptionProfileIds
  }
};
