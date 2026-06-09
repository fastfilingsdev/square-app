const crypto = require('crypto');
const express = require('express');
const { getSheetsClient } = require('../../core/googleSheets');
const {
  bFallbackAutomationIntervalMs,
  bFallbackAutomationLookbackDays,
  bFallbackAutomationMaxCharges,
  bFallbackAutomationMaxRows,
  isBFallbackAutomationEnabled,
  isBChargeEnabled,
  isBDetectionEnabled,
  processBProfileUpdatedWebhook,
  runBValidationFallback
} = require('./paymentUpdateBRecovery');

const WEBHOOK_LOG_SHEET_NAME = 'AuthNet_Webhook_Log';
const RECEIVER_VERSION = 'authnet-webhook-log-b-catchup-v5-auto-charge';

const fallbackAutomationState = {
  started: false,
  running: false,
  timer: null,
  initialTimer: null,
  lastRunAtUtc: null,
  lastSuccessAtUtc: null,
  lastErrorAtUtc: null,
  lastError: '',
  lastCounts: null
};

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

function hasValidSyncToken(req) {
  const expected = process.env.FF_SYNC_ADMIN_TOKEN || process.env.AUTHNET_SYNC_TOKEN || '';
  if (!expected) return false;
  const headerToken = String(req.get('x-ff-sync-token') || req.get('x-authnet-sync-token') || '').trim();
  const auth = String(req.get('authorization') || '').trim();
  const bearer = auth.toLowerCase().startsWith('bearer ') ? auth.slice(7).trim() : '';
  return headerToken === expected || bearer === expected;
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
  // Authorize.Net signs webhook bodies using the Signature Key string from the
  // merchant interface as the HMAC key. The key looks like hex, but it must not
  // be decoded into bytes before HMAC verification.
  return crypto
    .createHmac('sha512', keyHex)
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
      writes: bChargeEnabled
        ? [WEBHOOK_LOG_SHEET_NAME, 'AuthNet_Transactions', 'Recovered Subs', 'Active Subscriptions presentation sync', 'Payment Update B resolved rows', 'Payment on Hold resolved rows', 'Payment Update Link Tickets', 'Payment Update Email Log', 'Stop_Work_Feed']
        : (bDetectionEnabled
            ? [WEBHOOK_LOG_SHEET_NAME, 'Payment Update B pending-approval/suppression rows']
            : [WEBHOOK_LOG_SHEET_NAME]),
      authnetMutations: bChargeEnabled ? ['B profile-updated catch-up charge'] : false,
      customerEmails: false,
      bFallbackAutomation: {
        enabled: isBFallbackAutomationEnabled(),
        started: fallbackAutomationState.started,
        running: fallbackAutomationState.running,
        intervalMs: bFallbackAutomationIntervalMs(),
        lookbackDays: bFallbackAutomationLookbackDays(),
        maxRows: bFallbackAutomationMaxRows(),
        maxCharges: bFallbackAutomationMaxCharges(),
        lastRunAtUtc: fallbackAutomationState.lastRunAtUtc,
        lastSuccessAtUtc: fallbackAutomationState.lastSuccessAtUtc,
        lastErrorAtUtc: fallbackAutomationState.lastErrorAtUtc,
        lastError: fallbackAutomationState.lastError,
        lastCounts: fallbackAutomationState.lastCounts
      }
    });
  });

  router.get('/b-catchup/fallback/health', (req, res) => {
    res.json({
      ok: true,
      route: '/authnet/b-catchup/fallback',
      authRequired: true,
      authConfigured: Boolean(process.env.FF_SYNC_ADMIN_TOKEN || process.env.AUTHNET_SYNC_TOKEN),
      spreadsheetConfigured: Boolean(getWebhookSpreadsheetId()),
      authnetConfigured: Boolean(process.env.AUTHNET_API_LOGIN_ID && process.env.AUTHNET_TRANSACTION_KEY),
      bDetectionEnabled: isBDetectionEnabled(),
      bChargeEnabled: isBChargeEnabled(),
      automation: {
        enabled: isBFallbackAutomationEnabled(),
        started: fallbackAutomationState.started,
        running: fallbackAutomationState.running,
        intervalMs: bFallbackAutomationIntervalMs(),
        lookbackDays: bFallbackAutomationLookbackDays(),
        maxRows: bFallbackAutomationMaxRows(),
        maxCharges: bFallbackAutomationMaxCharges(),
        lastRunAtUtc: fallbackAutomationState.lastRunAtUtc,
        lastSuccessAtUtc: fallbackAutomationState.lastSuccessAtUtc,
        lastErrorAtUtc: fallbackAutomationState.lastErrorAtUtc,
        lastError: fallbackAutomationState.lastError,
        lastCounts: fallbackAutomationState.lastCounts
      },
      safety: 'SUB B / Payment on Hold missed-webhook fallback only. Detects post-click $0.01 auth-only validations, then reuses the existing B catch-up flow in apply mode. No customer emails, no ARB create/cancel/replacement, no refunds, and no raw card/bank/profile data returned.'
    });
  });

  router.post('/b-catchup/fallback', async (req, res) => {
    if (!hasValidSyncToken(req)) {
      return res.status(401).json({ ok: false, error: 'Unauthorized Auth.Net B fallback request' });
    }
    const spreadsheetId = getWebhookSpreadsheetId();
    if (!spreadsheetId) {
      return res.status(503).json({ ok: false, error: 'spreadsheet_not_configured' });
    }
    try {
      const mode = String(req.body?.mode || req.query?.mode || 'audit').toLowerCase() === 'apply' ? 'apply' : 'audit';
      const triggeredBy = String(req.body?.triggeredBy || req.query?.triggeredBy || 'api').slice(0, 80);
      const lookbackDays = Number(req.body?.lookbackDays || req.query?.lookbackDays || bFallbackAutomationLookbackDays());
      const maxRows = Number(req.body?.maxRows || req.query?.maxRows || bFallbackAutomationMaxRows());
      const maxCharges = Number(req.body?.maxCharges || req.query?.maxCharges || bFallbackAutomationMaxCharges());
      const sheets = await getSheets();
      const result = await runBValidationFallback({ sheets, spreadsheetId, mode, triggeredBy, lookbackDays, maxRows, maxCharges });
      res.set({ 'Cache-Control': 'no-store, max-age=0', Pragma: 'no-cache' });
      return res.status(200).json(result);
    } catch (err) {
      console.error('AUTHNET B FALLBACK ERROR:', err.message);
      return res.status(500).json({ ok: false, error: err.message });
    }
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

async function runBFallbackAutomationOnce(triggeredBy = 'authnet-b-validation-fallback-auto') {
  if (fallbackAutomationState.running) {
    return { ok: true, skipped: true, reason: 'B fallback automation already running' };
  }
  const spreadsheetId = getWebhookSpreadsheetId();
  if (!spreadsheetId) {
    return { ok: false, error: 'spreadsheet_not_configured' };
  }
  fallbackAutomationState.running = true;
  fallbackAutomationState.lastRunAtUtc = new Date().toISOString();
  try {
    const sheets = await getSheetsClient();
    const result = await runBValidationFallback({
      sheets,
      spreadsheetId,
      mode: 'apply',
      triggeredBy,
      lookbackDays: bFallbackAutomationLookbackDays(),
      maxRows: bFallbackAutomationMaxRows(),
      maxCharges: bFallbackAutomationMaxCharges()
    });
    fallbackAutomationState.lastSuccessAtUtc = new Date().toISOString();
    fallbackAutomationState.lastError = '';
    fallbackAutomationState.lastCounts = result.counts || null;
    console.log('Auth.Net B fallback automation completed', JSON.stringify({ counts: fallbackAutomationState.lastCounts }));
    return result;
  } catch (err) {
    fallbackAutomationState.lastErrorAtUtc = new Date().toISOString();
    fallbackAutomationState.lastError = String(err?.message || err).slice(0, 300);
    console.error('Auth.Net B fallback automation error:', fallbackAutomationState.lastError);
    return { ok: false, error: fallbackAutomationState.lastError };
  } finally {
    fallbackAutomationState.running = false;
  }
}

function startAuthNetBFallbackAutomation({ initialDelayMs = 45000 } = {}) {
  if (fallbackAutomationState.started) return fallbackAutomationState;
  if (!isBFallbackAutomationEnabled()) {
    console.log('Auth.Net B fallback automation disabled by AUTHNET_B_FALLBACK_AUTOMATION_ENABLED=false');
    return fallbackAutomationState;
  }
  fallbackAutomationState.started = true;
  const intervalMs = bFallbackAutomationIntervalMs();
  const tick = () => runBFallbackAutomationOnce('authnet-b-validation-fallback-auto').catch(err => {
    fallbackAutomationState.lastErrorAtUtc = new Date().toISOString();
    fallbackAutomationState.lastError = String(err?.message || err).slice(0, 300);
    fallbackAutomationState.running = false;
  });
  fallbackAutomationState.initialTimer = setTimeout(tick, Math.max(0, initialDelayMs));
  fallbackAutomationState.timer = setInterval(tick, intervalMs);
  if (fallbackAutomationState.initialTimer.unref) fallbackAutomationState.initialTimer.unref();
  if (fallbackAutomationState.timer.unref) fallbackAutomationState.timer.unref();
  console.log('Auth.Net B fallback automation started', JSON.stringify({
    intervalMs,
    lookbackDays: bFallbackAutomationLookbackDays(),
    maxRows: bFallbackAutomationMaxRows(),
    maxCharges: bFallbackAutomationMaxCharges(),
    bChargeEnabled: isBChargeEnabled()
  }));
  return fallbackAutomationState;
}

module.exports = {
  startAuthNetBFallbackAutomation,
  runBFallbackAutomationOnce,
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
