const crypto = require('crypto');
const { getHostedPaymentPageToken, getAuthNetConfig } = require('../../connectors/authnet/client');

const DEFAULT_FF_BILLING_SPREADSHEET_ID = '1DANHiunfffxvN7PWBxxO0WIzPWVGeOlaWMEcH-eJxBg';
const PAYMENT_LINKS_TAB = 'Payment Links';
const PAYMENT_LINK_BASE_PATH = '/billing/payment-links';

const PAYMENT_LINK_HEADERS = [
  'Created At',
  'Link Type',
  'Link ID',
  'Payment Link',
  'Customer ID',
  'State',
  'Business Name',
  'Name',
  'Email',
  'Amount',
  'Line Items',
  'Purpose',
  'Status',
  'Expires At',
  'Completed At',
  'Auth.Net Transaction ID',
  'Invoice #',
  'Authorization Required?',
  'Authorization Text',
  'Authorization Accepted At',
  'Last Token Generated At',
  'Last Click At',
  'Checkout Returned At',
  'Created By',
  'Notes / Audit Log'
];

const LINK_TYPE_CERTIFICATE_CANCELLATION = 'Sales Certificate Cancellation';
const LINK_TYPE_PAST_PERIOD_FILINGS = 'Past Period Filings';
const LINK_TYPE_MULTIPLE_ITEMS = 'Multiple Items';

function getBillingSpreadsheetId() {
  return process.env.FF_BILLING_SPREADSHEET_ID
    || process.env.BILLING_SPREADSHEET_ID
    || DEFAULT_FF_BILLING_SPREADSHEET_ID;
}

function normalizeString(value) {
  return String(value == null ? '' : value).trim();
}

function normalizeHeader(value) {
  return normalizeString(value).toLowerCase().replace(/[^a-z0-9]+/g, '');
}

function normalizeKey(value) {
  return normalizeString(value).toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
}

function firstNonEmpty(...values) {
  for (const value of values) {
    const text = normalizeString(value);
    if (text) return text;
  }
  return '';
}

function quoteSheetName(sheetName) {
  return `'${String(sheetName).replace(/'/g, "''")}'`;
}

function headerMap(headers) {
  const map = new Map();
  (headers || []).forEach((header, index) => {
    const key = normalizeHeader(header);
    if (key && !map.has(key)) map.set(key, index);
  });
  return map;
}

function valueByHeader(row, map, header) {
  const index = map.get(normalizeHeader(header));
  return index == null ? '' : normalizeString(row[index]);
}

function setValueByHeader(row, map, header, value) {
  const index = map.get(normalizeHeader(header));
  if (index == null) return;
  row[index] = value;
}

function parseAmount(value) {
  const text = normalizeString(value).replace(/[$,]/g, '');
  const match = text.match(/-?\d+(?:\.\d+)?/);
  if (!match) return '';
  const amount = Number(match[0]);
  if (!Number.isFinite(amount) || amount <= 0) return '';
  return amount.toFixed(2);
}

function amountNumber(value) {
  const parsed = parseAmount(value);
  return parsed ? Number(parsed) : 0;
}

function money(value) {
  const amount = amountNumber(value);
  return amount ? amount.toFixed(2) : '';
}

function displayMoney(value) {
  const amount = money(value);
  return amount ? `$${amount.replace(/\.00$/, '')}` : '';
}

function parseQuantity(value) {
  const raw = normalizeString(value);
  if (!raw) return 1;
  const n = Number(raw.replace(/[^0-9.]/g, ''));
  if (!Number.isFinite(n) || n <= 0) return 1;
  return Math.min(n, 9999);
}

function cleanLineItemName(value) {
  return normalizeString(value).replace(/\s+/g, ' ').slice(0, 255);
}

function parseLineItemLine(line) {
  const raw = normalizeString(line);
  if (!raw) return null;
  const parts = raw.split('|').map(part => normalizeString(part));
  let name = parts[0] || '';
  let amount = parseAmount(parts[1] || '');
  let quantity = parseQuantity(parts[2] || '1');

  if (!amount) {
    const dollarMatches = raw.match(/\$\s*\d+(?:\.\d{1,2})?/g);
    const amountMatch = dollarMatches && dollarMatches.length
      ? dollarMatches
      : raw.match(/\b\d+(?:\.\d{1,2})?\b/g);
    if (amountMatch && amountMatch.length) {
      const amountToken = amountMatch[amountMatch.length - 1];
      amount = parseAmount(amountToken);
      name = raw
        .replace(amountToken, '')
        .replace(/\b(?:x|qty)\s*:?\s*\d+(?:\.\d+)?\b/i, '')
        .replace(/[|–—-]+\s*$/, '')
        .trim() || name;
    }
  }

  const qtyMatch = raw.match(/(?:\bx\s*|\bqty\s*:?\s*)(\d+(?:\.\d+)?)/i);
  if (qtyMatch) quantity = parseQuantity(qtyMatch[1]);

  name = cleanLineItemName(name);
  if (!name || !amount) return null;
  return { name, amount, quantity };
}

function parseLineItemsText(text) {
  return normalizeString(text)
    .split(/\r?\n/)
    .map(parseLineItemLine)
    .filter(Boolean);
}

function fallbackItemName(rowObj) {
  return firstNonEmpty(rowObj.Purpose, rowObj['Link Type'], 'Fast Filings service');
}

function parsePaymentLinkItems(rowObj) {
  const textItems = parseLineItemsText(rowObj['Line Items']);
  const items = textItems.length ? textItems : [];

  if (!items.length) {
    const amount = parseAmount(rowObj.Amount);
    const name = fallbackItemName(rowObj);
    if (amount && name) items.push({ name, amount, quantity: 1 });
  }

  if (!items.length) throw new Error('Payment link requires at least one billable line item or Amount/Purpose.');
  if (items.length > 30) throw new Error('Authorize.Net supports up to 30 line items per hosted payment link.');
  return items;
}

function totalLineItems(items) {
  const total = (items || []).reduce((sum, item) => sum + (amountNumber(item.amount) * parseQuantity(item.quantity)), 0);
  if (!Number.isFinite(total) || total <= 0) throw new Error('Payment link total amount must be greater than zero.');
  return total.toFixed(2);
}

function normalizedLinkType(rowObj) {
  return normalizeKey(firstNonEmpty(rowObj['Link Type'], rowObj.Purpose));
}

function isCertificateCancellation(rowObj) {
  const type = normalizedLinkType(rowObj);
  return type.includes('cancel') && (type.includes('sales') || type.includes('certificate') || type.includes('cert'));
}

function isPastPeriodFiling(rowObj) {
  const type = normalizedLinkType(rowObj);
  return type.includes('past') || type.includes('period') || type.includes('filing');
}

function truthy(value) {
  return ['true', 'yes', 'y', '1', 'required'].includes(normalizeString(value).toLowerCase());
}

function authorizationRequired(rowObj) {
  if (truthy(rowObj['Authorization Required?'])) return true;
  // Require a checked authorization for every payment link, with stronger default text for cancellation links.
  return true;
}

function authorizationTextFor(rowObj, items = []) {
  const custom = normalizeString(rowObj['Authorization Text']);
  if (custom) return custom;

  const business = firstNonEmpty(rowObj['Business Name'], rowObj.Name, 'the business/customer shown on this page');
  const state = firstNonEmpty(rowObj.State, (rowObj['Customer ID'] || '').split('-')[0]);
  const stateText = state ? ` in ${state}` : '';
  const amountText = displayMoney(totalLineItems(items));

  if (isCertificateCancellation(rowObj)) {
    return `I authorize Fast Filings to prepare and submit a request to cancel/close the sales tax certificate/account for ${business}${stateText}. I understand the state may require additional information or approval, and this payment${amountText ? ` of ${amountText}` : ''} covers Fast Filings' cancellation assistance/service.`;
  }

  if (isPastPeriodFiling(rowObj)) {
    return `I authorize Fast Filings to process this payment${amountText ? ` of ${amountText}` : ''} for the past-period filing service(s) listed on this page.`;
  }

  return `I authorize Fast Filings to process this payment${amountText ? ` of ${amountText}` : ''} for the service item(s) listed on this page.`;
}

function generatePaymentLinkId({ live = true } = {}) {
  const date = new Date().toISOString().slice(0, 10).replace(/-/g, '');
  const token = crypto.randomBytes(18).toString('base64url').slice(0, 24);
  return `${live ? 'ffpl_live' : 'ffpl_test'}_${date}_${token}`;
}

function paymentLinkBaseUrl(req) {
  const configured = process.env.FF_BILLING_PAYMENT_LINK_BASE_URL || process.env.PAYMENT_UPDATE_BASE_URL || '';
  if (configured) return configured.replace(/\/$/, '');
  const protocol = req.get('x-forwarded-proto') || req.protocol || 'https';
  return `${protocol}://${req.get('host')}`.replace(/\/$/, '');
}

function paymentLinkUrl(req, linkId, { testOnly = false } = {}) {
  const testSegment = testOnly ? '/test' : '';
  return `${paymentLinkBaseUrl(req)}${PAYMENT_LINK_BASE_PATH}${testSegment}/${encodeURIComponent(linkId)}`;
}

function paymentLinkReturnUrl(req, linkId) {
  return `${paymentLinkBaseUrl(req)}${PAYMENT_LINK_BASE_PATH}/return/${encodeURIComponent(linkId)}`;
}

function hostedPaymentSetting(settingName, settingValue) {
  return { settingName, settingValue: JSON.stringify(settingValue) };
}

function hostedPaymentSettings(req, link, amount) {
  const returnUrl = paymentLinkReturnUrl(req, link.linkId);
  return [
    hostedPaymentSetting('hostedPaymentReturnOptions', {
      showReceipt: true,
      url: returnUrl,
      urlText: 'Return to Fast Filings',
      cancelUrl: returnUrl,
      cancelUrlText: 'Cancel'
    }),
    hostedPaymentSetting('hostedPaymentButtonOptions', { text: `Pay ${displayMoney(amount) || '$0.00'}` }),
    hostedPaymentSetting('hostedPaymentStyleOptions', { bgColor: '#1d4ed8' }),
    hostedPaymentSetting('hostedPaymentOrderOptions', { show: true, merchantName: 'Fast Filings' }),
    hostedPaymentSetting('hostedPaymentPaymentOptions', {
      cardCodeRequired: true,
      showCreditCard: true,
      showBankAccount: false
    }),
    hostedPaymentSetting('hostedPaymentBillingAddressOptions', { show: true, required: true }),
    hostedPaymentSetting('hostedPaymentCustomerOptions', {
      showEmail: true,
      requiredEmail: true,
      addPaymentProfile: false
    }),
    hostedPaymentSetting('hostedPaymentSecurityOptions', { captcha: false })
  ];
}

function splitFullName(fullName) {
  const parts = normalizeString(fullName).split(/\s+/).filter(Boolean);
  if (!parts.length) return { firstName: '', lastName: '' };
  if (parts.length === 1) return { firstName: parts[0], lastName: '' };
  return { firstName: parts[0], lastName: parts.slice(1).join(' ') };
}

function invoiceNumberFor(link) {
  const existing = normalizeString(link.rowObj['Invoice #']);
  if (existing) return existing.slice(0, 20);
  return `FFPL-${link.linkId.replace(/^ffpl_(live|test)_?/i, '').replace(/[^A-Za-z0-9]/g, '').slice(-15)}`.slice(0, 20);
}

function authNetLineItems(items) {
  return {
    lineItem: items.map((item, index) => ({
      itemId: `FF-${index + 1}`.slice(0, 31),
      name: cleanLineItemName(item.name).slice(0, 31) || `Item ${index + 1}`,
      description: cleanLineItemName(item.name).slice(0, 255) || `Fast Filings item ${index + 1}`,
      quantity: String(parseQuantity(item.quantity)),
      unitPrice: money(item.amount)
    }))
  };
}

function buildHostedPaymentTransactionRequest(link) {
  const items = link.items;
  const amount = totalLineItems(items);
  const { firstName, lastName } = splitFullName(link.rowObj.Name);
  const invoiceNumber = invoiceNumberFor(link);
  const description = firstNonEmpty(link.rowObj.Purpose, link.rowObj['Link Type'], 'Fast Filings service').slice(0, 255);
  const email = normalizeString(link.rowObj.Email);
  const customerId = normalizeString(link.rowObj['Customer ID']);

  return {
    transactionType: 'authCaptureTransaction',
    amount,
    order: { invoiceNumber, description },
    lineItems: authNetLineItems(items),
    // Authorize.Net's JSON API is mapped onto an XML schema where sibling
    // order matters. Keep customer before billTo; putting customer after
    // billTo triggers E00003 invalid child element errors on Accept Hosted.
    ...(email || customerId ? {
      customer: {
        ...(customerId ? { id: customerId.slice(0, 20) } : {}),
        ...(email ? { email: email.slice(0, 255) } : {})
      }
    } : {}),
    ...(firstName || lastName || link.rowObj['Business Name'] ? {
      billTo: {
        ...(firstName ? { firstName } : {}),
        ...(lastName ? { lastName } : {}),
        ...(link.rowObj['Business Name'] ? { company: normalizeString(link.rowObj['Business Name']).slice(0, 50) } : {})
      }
    } : {}),
    userFields: {
      userField: [
        { name: 'ffPaymentLinkId', value: link.linkId.slice(0, 255) },
        ...(customerId ? [{ name: 'ffCustomerId', value: customerId.slice(0, 255) }] : []),
        ...(link.rowObj['Link Type'] ? [{ name: 'ffLinkType', value: normalizeString(link.rowObj['Link Type']).slice(0, 255) }] : [])
      ]
    }
  };
}

function rowObjectFromValues(headers, row) {
  const map = headerMap(headers);
  const out = {};
  PAYMENT_LINK_HEADERS.forEach(header => {
    out[header] = valueByHeader(row, map, header);
  });
  return out;
}

function linkFromRow(headers, row, rowNumber) {
  const rowObj = rowObjectFromValues(headers, row);
  const linkId = firstNonEmpty(rowObj['Link ID'], rowObj['Auth.Net Ticket / Link ID']);
  const items = parsePaymentLinkItems(rowObj);
  return {
    rowNumber,
    linkId,
    rowObj,
    items,
    amount: totalLineItems(items),
    authorizationRequired: authorizationRequired(rowObj),
    authorizationText: authorizationTextFor(rowObj, items)
  };
}

function validatePaymentLink(link, { testOnly = false } = {}) {
  if (!link || !link.linkId) throw new Error('Payment link not found.');
  const status = normalizeKey(link.rowObj.Status);
  const allowed = testOnly
    ? new Set(['test link ready', 'link ready', 'live link ready'])
    : new Set(['link ready', 'live link ready']);
  if (!allowed.has(status)) throw new Error('Payment link is not ready or has been disabled.');
  if (!testOnly && !String(link.linkId).startsWith('ffpl_live_')) throw new Error('Live payment route requires a live FF Billing Payment Link ID.');
  if (testOnly && !String(link.linkId).startsWith('ffpl_')) throw new Error('Invalid FF Billing Payment Link ID.');
  if (link.rowObj['Completed At'] || link.rowObj['Auth.Net Transaction ID']) throw new Error('This payment link is already marked completed.');

  const expiresAt = normalizeString(link.rowObj['Expires At']);
  if (expiresAt) {
    const expires = Date.parse(expiresAt);
    if (Number.isFinite(expires) && expires < Date.now()) throw new Error('Payment link has expired.');
  }
  return link;
}

async function readPaymentLinks(sheets, spreadsheetId = getBillingSpreadsheetId()) {
  const response = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: `${quoteSheetName(PAYMENT_LINKS_TAB)}!A:AZ`,
    valueRenderOption: 'FORMATTED_VALUE'
  });
  return response.data.values || [];
}

async function findPaymentLink(sheets, linkId, spreadsheetId = getBillingSpreadsheetId()) {
  const rows = await readPaymentLinks(sheets, spreadsheetId);
  if (!rows.length) throw new Error('Payment Links tab is empty.');
  const headers = rows[0] || [];
  const map = headerMap(headers);
  const matches = [];
  for (let i = 1; i < rows.length; i += 1) {
    const row = rows[i] || [];
    if (!row.some(value => normalizeString(value))) continue;
    const rowLinkId = firstNonEmpty(valueByHeader(row, map, 'Link ID'), valueByHeader(row, map, 'Auth.Net Ticket / Link ID'));
    if (rowLinkId === linkId) matches.push(linkFromRow(headers, row, i + 1));
  }
  if (!matches.length) throw new Error('Payment link not found.');
  if (matches.length > 1) throw new Error('Duplicate Payment Links rows found for this Link ID; payment is blocked.');
  return matches[0];
}

async function updatePaymentLinkRow(sheets, link, fields, spreadsheetId = getBillingSpreadsheetId()) {
  const rows = await readPaymentLinks(sheets, spreadsheetId);
  const headers = rows[0] || [];
  const map = headerMap(headers);
  const updates = [];
  for (const [header, value] of Object.entries(fields || {})) {
    const index = map.get(normalizeHeader(header));
    if (index == null) continue;
    const col = columnName(index + 1);
    updates.push({ range: `${quoteSheetName(PAYMENT_LINKS_TAB)}!${col}${link.rowNumber}`, values: [[value]] });
  }
  if (!updates.length) return { updated: false };
  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId,
    requestBody: {
      valueInputOption: 'USER_ENTERED',
      data: updates
    }
  });
  return { updated: true, cells: updates.length };
}

function columnName(index) {
  let n = Number(index);
  let name = '';
  while (n > 0) {
    const rem = (n - 1) % 26;
    name = String.fromCharCode(65 + rem) + name;
    n = Math.floor((n - 1) / 26);
  }
  return name;
}

async function createHostedPaymentLinkSession({ req, sheets, link, authorizationAccepted = false, testOnly = false }) {
  if (link.authorizationRequired && !authorizationAccepted) {
    const err = new Error('Authorization checkbox is required before opening secure checkout.');
    err.statusCode = 400;
    throw err;
  }

  const now = new Date().toISOString();
  await updatePaymentLinkRow(sheets, link, {
    'Last Click At': now,
    ...(link.authorizationRequired ? { 'Authorization Accepted At': now } : {})
  });

  const transactionRequest = buildHostedPaymentTransactionRequest(link);
  const hostedToken = await getHostedPaymentPageToken(
    transactionRequest,
    hostedPaymentSettings(req, link, link.amount),
    `ffpl-${String(link.linkId).slice(-15)}`
  );

  await updatePaymentLinkRow(sheets, link, { 'Last Token Generated At': new Date().toISOString() });
  const config = getAuthNetConfig();
  return {
    ok: true,
    formActionUrl: config.acceptHostedPaymentUrl,
    acceptHostedPaymentUrl: config.acceptHostedPaymentUrl,
    hostedToken,
    amount: link.amount,
    invoiceNumber: invoiceNumberFor(link),
    rawTokenStored: false,
    customerEmailSent: false,
    authNetMutationBeforeCustomerSubmit: false,
    testOnly: Boolean(testOnly)
  };
}

module.exports = {
  DEFAULT_FF_BILLING_SPREADSHEET_ID,
  LINK_TYPE_CERTIFICATE_CANCELLATION,
  LINK_TYPE_MULTIPLE_ITEMS,
  LINK_TYPE_PAST_PERIOD_FILINGS,
  PAYMENT_LINKS_TAB,
  PAYMENT_LINK_BASE_PATH,
  PAYMENT_LINK_HEADERS,
  authorizationTextFor,
  buildHostedPaymentTransactionRequest,
  createHostedPaymentLinkSession,
  displayMoney,
  findPaymentLink,
  generatePaymentLinkId,
  getBillingSpreadsheetId,
  hostedPaymentSettings,
  isCertificateCancellation,
  isPastPeriodFiling,
  parseLineItemsText,
  parsePaymentLinkItems,
  paymentLinkReturnUrl,
  paymentLinkUrl,
  totalLineItems,
  updatePaymentLinkRow,
  validatePaymentLink
};
