const crypto = require('crypto');
const express = require('express');
const { getSheetsClient } = require('../../core/googleSheets');
const {
  isBChargeEnabled,
  isBDetectionEnabled,
  processBProfileUpdatedWebhook
} = require('./paymentUpdateBRecovery');

const WEBHOOK_LOG_SHEET_NAME = 'AuthNet_Webhook_Log';
const RECEIVER_VERSION = 'authnet-webhook-log-b-catchup-v3';

const WEBHOOK_LOG_HEADERS = [
  'Received At',
  'Receiver Version',
  'Webhook ID',
  'Event Type',
  'Payload ID / Transaction ID',
  'Subscription ID',
  'Invoice Number',
  'Customer Profile ID',
  'Payment Profile ID',
  'Signature Status',
  'Sanitized Event JSON'
];

function getWebhookSpreadsheetId() {
  return process.env.AUTHNET_WEBHOOK_SPREADSHEET_ID
    || process.env.FF_SUBSCRIPTIONS_SPREADSHEET_ID
    || process.env.PAYMENT_UPDATE_SPREADSHEET_ID
    || process.env.GOOGLE_SHEETS_SPREADSHEET_ID
    || '';
}

function normalizeHexKey(value) {
  return String(value || '')
    .replace(/^SHA512=/i, '')
    .replace(/[^0-9a-f]/gi, '')
    .trim();
}

function getSignatureKeyHex() {
  return normalizeHexKey(
    process.env.AUTHNET_WEBHOOK_SIGNATURE_KEY_HEX
      || process.env.AUTHNET_SIGNATURE_KEY_HEX
      || process.env.AUTHNET_SIGNATURE_KEY
      || ''
  );
}

function computeAuthNetSignature(rawBody, signatureKeyHex) {
  const keyHex = normalizeHexKey(signatureKeyHex);
  if (!keyHex || keyHex.length < 32 || keyHex.length % 2 !== 0) {
    throw new Error('Invalid Authorize.Net signature key configuration');
  }
  return crypto
    .createHmac('sha512', Buffer.from(keyHex, 'hex'))
    .update(String(rawBody || ''), 'utf8')
    .digest('hex');
}

function safeEqualHex(a, b) {
  const left = normalizeHexKey(a).toLowerCase();
  const right = normalizeHexKey(b).toLowerCase();
  if (!left || !right || left.length !== right.length) return false;
  return crypto.timingSafeEqual(Buffer.from(left, 'hex'), Buffer.from(right, 'hex'));
}

function verifyAuthNetSignature({ rawBody, signatureHeader, signatureKeyHex = getSignatureKeyHex() }) {
  const keyHex = normalizeHexKey(signatureKeyHex);
  if (!keyHex) return { ok: false, status: 'signature-key-missing', httpStatus: 503 };
  if (!signatureHeader) return { ok: false, status: 'signature-header-missing', httpStatus: 401 };

  const provided = normalizeHexKey(signatureHeader);
  const expected = computeAuthNetSignature(rawBody, keyHex);
  if (!safeEqualHex(provided, expected)) {
    return { ok: false, status: 'signature-invalid', httpStatus: 401 };
  }
  return { ok: true, status: 'signature-valid', httpStatus: 200 };
}

function firstString(...values) {
  for (const value of values) {
    if (value === null || value === undefined) continue;
    const str = String(value).trim();
    if (str) return str;
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

function buildEventSnapshot(body) {
  const payload = body && typeof body === 'object' ? (body.payload || {}) : {};
  return {
    notificationId: firstString(body && body.notificationId),
    webhookId: firstString(body && body.webhookId, payload.webhookId),
    eventType: firstString(body && body.eventType),
    eventDate: firstString(body && body.eventDate),
    payload: {
      id: firstString(payload.id, payload.transId, firstNested(payload, ['transaction.id', 'transaction.transId', 'transaction.transactionId'])),
      entityName: firstString(payload.entityName, firstNested(payload, ['transaction.entityName'])),
      transactionType: firstString(payload.transactionType, firstNested(payload, ['transaction.transactionType'])),
      transactionStatus: firstString(payload.transactionStatus, firstNested(payload, ['transaction.transactionStatus'])),
      responseCode: firstString(payload.responseCode, firstNested(payload, ['transaction.responseCode'])),
      authAmount: firstString(payload.authAmount, payload.settleAmount, payload.amount, firstNested(payload, ['transaction.authAmount', 'transaction.settleAmount', 'transaction.amount'])),
      invoiceNumber: firstString(payload.invoiceNumber, firstNested(payload, ['order.invoiceNumber', 'transaction.order.invoiceNumber'])),
      subscriptionId: firstString(payload.subscriptionId, firstNested(payload, ['subscription.id', 'transaction.subscription.id'])),
      customerProfileId: firstString(payload.customerProfileId, firstNested(payload, ['customer.customerProfileId', 'transaction.customerProfileId'])),
      customerPaymentProfileId: firstString(payload.customerPaymentProfileId, payload.paymentProfileId, firstNested(payload, ['customerPaymentProfileId', 'paymentProfileId', 'transaction.customerPaymentProfileId']))
    }
  };
}

function buildWebhookLogRow({ body, rawBody, signatureStatus, receivedAt = new Date() }) {
  const payload = body && typeof body === 'object' ? (body.payload || {}) : {};
  const eventSnapshot = buildEventSnapshot(body || {});
  return [
    receivedAt.toISOString(),
    RECEIVER_VERSION,
    firstString(body && body.webhookId, payload.webhookId),
    firstString(body && body.eventType),
    firstString(payload.id, payload.transId, firstNested(payload, ['transaction.id', 'transaction.transId', 'transaction.transactionId'])),
    firstString(payload.subscriptionId, firstNested(payload, ['subscription.id', 'transaction.subscription.id'])),
    firstString(payload.invoiceNumber, firstNested(payload, ['order.invoiceNumber', 'transaction.order.invoiceNumber'])),
    firstString(payload.customerProfileId, firstNested(payload, ['customerProfileId', 'customer.customerProfileId', 'transaction.customerProfileId'])),
    firstString(payload.customerPaymentProfileId, payload.paymentProfileId, firstNested(payload, ['customerPaymentProfileId', 'paymentProfileId', 'transaction.customerPaymentProfileId'])),
    signatureStatus || '',
    JSON.stringify(eventSnapshot).slice(0, 4000)
  ];
}

function quoteSheetName(sheetName) {
  return `'${String(sheetName).replace(/'/g, "''")}'`;
}

async function ensureWebhookLogSheet(sheets, spreadsheetId) {
  const metadata = await sheets.spreadsheets.get({
    spreadsheetId,
    fields: 'sheets.properties(sheetId,title,hidden)'
  });
  let sheet = (metadata.data.sheets || []).find(item => item.properties && item.properties.title === WEBHOOK_LOG_SHEET_NAME);

  if (!sheet) {
    const addResponse = await sheets.spreadsheets.batchUpdate({
      spreadsheetId,
      requestBody: {
        requests: [{
          addSheet: {
            properties: {
              title: WEBHOOK_LOG_SHEET_NAME,
              hidden: true,
              gridProperties: { frozenRowCount: 1 }
            }
          }
        }]
      }
    });
    const added = (addResponse.data.replies || []).find(reply => reply.addSheet);
    sheet = added && added.addSheet;
  } else if (sheet.properties && sheet.properties.hidden !== true) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId,
      requestBody: {
        requests: [{
          updateSheetProperties: {
            properties: { sheetId: sheet.properties.sheetId, hidden: true },
            fields: 'hidden'
          }
        }]
      }
    });
  }

  const headerRange = `${quoteSheetName(WEBHOOK_LOG_SHEET_NAME)}!A1:${String.fromCharCode(64 + WEBHOOK_LOG_HEADERS.length)}1`;
  const existingHeader = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: headerRange
  }).catch(() => ({ data: { values: [] } }));
  const current = (existingHeader.data.values && existingHeader.data.values[0]) || [];
  const expected = WEBHOOK_LOG_HEADERS.join('\u0001');
  const actual = current.slice(0, WEBHOOK_LOG_HEADERS.length).join('\u0001');
  if (actual !== expected) {
    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range: headerRange,
      valueInputOption: 'RAW',
      requestBody: { values: [WEBHOOK_LOG_HEADERS] }
    });
  }
}

async function appendWebhookLogRow({ sheets, spreadsheetId, row }) {
  await ensureWebhookLogSheet(sheets, spreadsheetId);
  await sheets.spreadsheets.values.append({
    spreadsheetId,
    range: `${quoteSheetName(WEBHOOK_LOG_SHEET_NAME)}!A:K`,
    valueInputOption: 'USER_ENTERED',
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values: [row] }
  });
}

function createAuthNetWebhookRouter(options = {}) {
  const router = express.Router();
  const getSheets = options.getSheetsClient || getSheetsClient;

  router.get('/health', (req, res) => {
    const bDetectionEnabled = isBDetectionEnabled();
    const bChargeEnabled = isBChargeEnabled();
    res.json({
      ok: true,
      route: '/authnet/webhook',
      receiverVersion: RECEIVER_VERSION,
      spreadsheetConfigured: Boolean(getWebhookSpreadsheetId()),
      signatureConfigured: Boolean(getSignatureKeyHex()),
      bDetectionEnabled,
      bChargeEnabled,
      legacyCatchupFlagIgnoredForCharges: Boolean(String(process.env.AUTHNET_WEBHOOK_B_CATCHUP_ENABLED || '').trim()),
      writes: bDetectionEnabled
        ? [WEBHOOK_LOG_SHEET_NAME, 'Payment Update B pending-approval/suppression rows']
        : [WEBHOOK_LOG_SHEET_NAME],
      authnetMutations: bChargeEnabled ? ['B profile-updated catch-up charge'] : false,
      customerEmails: false
    });
  });

  router.post('/webhook', async (req, res) => {
    const rawBody = req.rawBody || JSON.stringify(req.body || {});
    if (!rawBody || rawBody === '{}') {
      return res.status(400).json({ ok: false, error: 'no_body' });
    }

    const verification = verifyAuthNetSignature({
      rawBody,
      signatureHeader: req.get('x-anet-signature')
    });
    if (!verification.ok) {
      return res.status(verification.httpStatus).json({ ok: false, error: verification.status });
    }

    let body;
    try {
      body = typeof req.body === 'object' && req.body !== null && Object.keys(req.body).length
        ? req.body
        : JSON.parse(rawBody);
    } catch (err) {
      return res.status(400).json({ ok: false, error: 'invalid_json' });
    }

    const spreadsheetId = getWebhookSpreadsheetId();
    if (!spreadsheetId) {
      return res.status(503).json({ ok: false, error: 'spreadsheet_not_configured' });
    }

    const sheets = await getSheets();
    const row = buildWebhookLogRow({ body, rawBody, signatureStatus: verification.status });
    await appendWebhookLogRow({ sheets, spreadsheetId, row });

    let bRecovery = { eligible: false, reason: 'not-processed' };
    try {
      bRecovery = await processBProfileUpdatedWebhook({ body, sheets, spreadsheetId });
    } catch (err) {
      // Keep the webhook receiver reliable: the sanitized support log above is
      // already written, and Authorize.Net should not retry indefinitely because
      // a downstream B recovery gate hit a workbook/Auth.Net issue.
      console.error('AUTHNET WEBHOOK B RECOVERY ERROR:', err.message);
      bRecovery = { eligible: true, enabled: true, status: 'error', reason: err.message || 'b-recovery-error' };
    }

    return res.status(200).json({ ok: true, logged: true, eventType: body.eventType || '', receiverVersion: RECEIVER_VERSION, bRecovery });
  });

  return router;
}

module.exports = {
  createAuthNetWebhookRouter,
  __authNetWebhookTestHooks: {
    WEBHOOK_LOG_HEADERS,
    WEBHOOK_LOG_SHEET_NAME,
    RECEIVER_VERSION,
    buildEventSnapshot,
    buildWebhookLogRow,
    computeAuthNetSignature,
    getWebhookSpreadsheetId,
    normalizeHexKey,
    safeEqualHex,
    verifyAuthNetSignature
  }
};
