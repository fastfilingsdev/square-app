const crypto = require('crypto');
const express = require('express');
const { getSheetsClient } = require('../../core/googleSheets');

const WEBHOOK_LOG_SHEET_NAME = 'Clover_Hosted_Checkout_Webhook_Log';
const RECEIVER_VERSION = 'clover-hosted-checkout-webhook-log-v1';
const WEBHOOK_LOG_HEADERS = [
  'Received At',
  'Receiver Version',
  'Merchant ID',
  'Type',
  'Status',
  'Payment ID',
  'Checkout Session ID',
  'Message',
  'Clover Created',
  'Signature Status',
  'Idempotency Key',
  'Sanitized Event JSON'
];

function firstString(...values) {
  for (const value of values) {
    if (value === null || value === undefined) continue;
    const str = String(value).trim();
    if (str) return str;
  }
  return '';
}

function getCloverHcoWebhookSpreadsheetId(env = process.env) {
  return env.CLOVER_HCO_WEBHOOK_SPREADSHEET_ID
    || env.CLOVER_HOSTED_CHECKOUT_WEBHOOK_SPREADSHEET_ID
    || env.FF_BILLING_SPREADSHEET_ID
    || env.GOOGLE_SHEETS_SPREADSHEET_ID
    || '';
}

function getCloverHcoWebhookSecret(env = process.env) {
  return String(
    env.CLOVER_HCO_WEBHOOK_SECRET
      || env.CLOVER_HOSTED_CHECKOUT_WEBHOOK_SECRET
      || env.CLOVER_SANDBOX_HCO_WEBHOOK_SECRET
      || ''
  ).trim();
}

function parseCloverSignatureHeader(signatureHeader) {
  const result = { timestamp: '', signature: '' };
  for (const part of String(signatureHeader || '').split(',')) {
    const [rawKey, ...rest] = part.trim().split('=');
    const key = String(rawKey || '').trim().toLowerCase();
    const value = rest.join('=').trim();
    if (key === 't') result.timestamp = value;
    if (key === 'v1') result.signature = value;
  }
  return result;
}

function computeCloverSignature({ timestamp, rawBody, secret }) {
  if (!timestamp) throw new Error('Missing Clover webhook timestamp');
  if (!secret) throw new Error('Missing Clover webhook secret');
  return crypto
    .createHmac('sha256', String(secret))
    .update(`${timestamp}.${String(rawBody || '')}`, 'utf8')
    .digest('hex');
}

function isHex(value) {
  return /^[0-9a-f]+$/i.test(String(value || ''));
}

function safeEqualHex(a, b) {
  const left = String(a || '').trim().toLowerCase();
  const right = String(b || '').trim().toLowerCase();
  if (!left || !right || left.length !== right.length || !isHex(left) || !isHex(right)) return false;
  return crypto.timingSafeEqual(Buffer.from(left, 'hex'), Buffer.from(right, 'hex'));
}

function verifyCloverSignature({
  rawBody,
  signatureHeader,
  secret = getCloverHcoWebhookSecret(),
  nowMs = Date.now(),
  maxAgeSeconds = Number(process.env.CLOVER_HCO_SIGNATURE_MAX_AGE_SECONDS || 0)
}) {
  if (!secret) return { ok: false, status: 'signature-secret-missing', httpStatus: 503 };
  if (!signatureHeader) return { ok: false, status: 'signature-header-missing', httpStatus: 401 };

  const { timestamp, signature } = parseCloverSignatureHeader(signatureHeader);
  if (!timestamp || !signature) return { ok: false, status: 'signature-header-malformed', httpStatus: 401 };
  if (!isHex(signature)) return { ok: false, status: 'signature-malformed', httpStatus: 401 };

  if (maxAgeSeconds > 0) {
    const timestampMs = Number(timestamp) * 1000;
    if (!Number.isFinite(timestampMs)) return { ok: false, status: 'signature-timestamp-invalid', httpStatus: 401 };
    if (Math.abs(nowMs - timestampMs) > maxAgeSeconds * 1000) {
      return { ok: false, status: 'signature-timestamp-out-of-range', httpStatus: 401 };
    }
  }

  const expected = computeCloverSignature({ timestamp, rawBody, secret });
  if (!safeEqualHex(signature, expected)) {
    return { ok: false, status: 'signature-invalid', httpStatus: 401 };
  }
  return { ok: true, status: 'signature-valid', httpStatus: 200, timestamp };
}

function buildCloverHostedCheckoutSnapshot(body) {
  const source = body && typeof body === 'object' ? body : {};
  return {
    type: firstString(source.type, source.eventType),
    id: firstString(source.id, source.paymentId, source.payment_id),
    merchantId: firstString(source.merchantId, source.merchant_id),
    created: firstString(source.created, source.createdTime, source.created_at),
    status: firstString(source.status),
    message: firstString(source.message),
    checkoutSessionId: firstString(
      source.checkoutSessionId,
      source.checkout_session_id,
      source.checkoutSessionUuid,
      source.data
    )
  };
}

function buildCloverHostedCheckoutIdempotencyKey(snapshot) {
  const merchantId = firstString(snapshot.merchantId);
  const paymentId = firstString(snapshot.id);
  const checkoutSessionId = firstString(snapshot.checkoutSessionId);
  const type = firstString(snapshot.type);
  if (merchantId || paymentId || checkoutSessionId || type) {
    return [merchantId, type, paymentId, checkoutSessionId].join('|');
  }
  return '';
}

function buildCloverHostedCheckoutLogRow({ body, rawBody, signatureStatus, receivedAt = new Date() }) {
  const snapshot = buildCloverHostedCheckoutSnapshot(body || {});
  const idempotencyKey = buildCloverHostedCheckoutIdempotencyKey(snapshot)
    || crypto.createHash('sha256').update(String(rawBody || '')).digest('hex');
  return [
    receivedAt.toISOString(),
    RECEIVER_VERSION,
    snapshot.merchantId,
    snapshot.type,
    snapshot.status,
    snapshot.id,
    snapshot.checkoutSessionId,
    snapshot.message,
    snapshot.created,
    signatureStatus || '',
    idempotencyKey,
    JSON.stringify(snapshot).slice(0, 4000)
  ];
}

function quoteSheetName(sheetName) {
  return `'${String(sheetName).replace(/'/g, "''")}'`;
}

function headerEndColumn() {
  return String.fromCharCode(64 + WEBHOOK_LOG_HEADERS.length);
}

async function ensureCloverHostedCheckoutLogSheet(sheets, spreadsheetId) {
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
              gridProperties: { frozenRowCount: 1, columnCount: WEBHOOK_LOG_HEADERS.length }
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

  const headerRange = `${quoteSheetName(WEBHOOK_LOG_SHEET_NAME)}!A1:${headerEndColumn()}1`;
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

async function cloverHostedCheckoutLogKeyExists({ sheets, spreadsheetId, idempotencyKey }) {
  const key = String(idempotencyKey || '').trim();
  if (!key) return false;
  const response = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: `${quoteSheetName(WEBHOOK_LOG_SHEET_NAME)}!K2:K`
  }).catch(() => ({ data: { values: [] } }));
  const rows = response.data.values || [];
  return rows.some(row => String(row && row[0] || '').trim() === key);
}

async function appendCloverHostedCheckoutLogRow({ sheets, spreadsheetId, row }) {
  await sheets.spreadsheets.values.append({
    spreadsheetId,
    range: `${quoteSheetName(WEBHOOK_LOG_SHEET_NAME)}!A:L`,
    valueInputOption: 'USER_ENTERED',
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values: [row] }
  });
}

function createCloverHostedCheckoutRouter(options = {}) {
  const router = express.Router();
  const getSheets = options.getSheetsClient || getSheetsClient;

  router.get('/hosted-checkout/health', (req, res) => {
    res.json({
      ok: true,
      route: '/clover/hosted-checkout/webhook',
      receiverVersion: RECEIVER_VERSION,
      spreadsheetConfigured: Boolean(getCloverHcoWebhookSpreadsheetId()),
      signatureConfigured: Boolean(getCloverHcoWebhookSecret()),
      writes: [WEBHOOK_LOG_SHEET_NAME],
      idempotency: 'merchantId|type|paymentId|checkoutSessionId',
      moneyMovement: false,
      customerEmails: false,
      downstreamActions: false,
      safety: 'Logs verified Clover Hosted Checkout webhook events only. No customer emails, subscription changes, refunds, charges, or filing actions.'
    });
  });

  router.post('/hosted-checkout/webhook', async (req, res) => {
    const rawBody = req.rawBody || JSON.stringify(req.body || {});
    if (!rawBody || rawBody === '{}') {
      return res.status(400).json({ ok: false, error: 'no_body' });
    }

    const verification = verifyCloverSignature({
      rawBody,
      signatureHeader: req.get('clover-signature')
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

    const spreadsheetId = getCloverHcoWebhookSpreadsheetId();
    if (!spreadsheetId) {
      return res.status(503).json({ ok: false, error: 'spreadsheet_not_configured' });
    }

    const sheets = await getSheets();
    await ensureCloverHostedCheckoutLogSheet(sheets, spreadsheetId);
    const row = buildCloverHostedCheckoutLogRow({ body, rawBody, signatureStatus: verification.status });
    const idempotencyKey = row[10];
    const duplicate = await cloverHostedCheckoutLogKeyExists({ sheets, spreadsheetId, idempotencyKey });
    if (!duplicate) {
      await appendCloverHostedCheckoutLogRow({ sheets, spreadsheetId, row });
    }

    const snapshot = buildCloverHostedCheckoutSnapshot(body);
    return res.status(200).json({
      ok: true,
      logged: !duplicate,
      duplicate,
      type: snapshot.type,
      status: snapshot.status,
      receiverVersion: RECEIVER_VERSION,
      customerEmails: false,
      moneyMovement: false,
      downstreamActions: false
    });
  });

  return router;
}

module.exports = {
  createCloverHostedCheckoutRouter,
  __cloverHostedCheckoutTestHooks: {
    WEBHOOK_LOG_HEADERS,
    WEBHOOK_LOG_SHEET_NAME,
    RECEIVER_VERSION,
    buildCloverHostedCheckoutIdempotencyKey,
    buildCloverHostedCheckoutLogRow,
    buildCloverHostedCheckoutSnapshot,
    computeCloverSignature,
    getCloverHcoWebhookSecret,
    getCloverHcoWebhookSpreadsheetId,
    parseCloverSignatureHeader,
    safeEqualHex,
    verifyCloverSignature
  }
};
