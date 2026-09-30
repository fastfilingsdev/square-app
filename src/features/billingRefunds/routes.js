const axios = require('axios');
const express = require('express');
const { buildRefundDryRun, lookupRefundCandidates, FF_BILLING_SPREADSHEET_ID, FF_SUBSCRIPTIONS_SPREADSHEET_ID } = require('./refundLookup');
const { liveRefundsEnabled } = require('./refundProcess');
const { createGuardedRefundProcessor } = require('./refundGuard');
const { hasValidAdminToken } = require('../../core/adminAccess');

function bearerToken(req) {
  const auth = String(req.get('authorization') || '').trim();
  return auth.toLowerCase().startsWith('bearer ') ? auth.slice(7).trim() : '';
}

function hasValidSyncToken(req) {
  return hasValidAdminToken(req);
}

function allowedRefundGoogleEmails() {
  const raw = process.env.FF_BILLING_REFUNDS_ALLOWED_GOOGLE_EMAILS || 'returns@fastfilings.com,returns1@fastfilings.com';
  return new Set(String(raw).split(',').map(item => item.trim().toLowerCase()).filter(Boolean));
}

async function verifyGoogleAccessToken(token) {
  if (!token) return { ok: false, email: '', error: 'missing bearer token' };
  try {
    const response = await axios.get('https://oauth2.googleapis.com/tokeninfo', {
      params: { access_token: token },
      timeout: 10000,
      maxRedirects: 0
    });
    const email = String(response.data?.email || '').trim().toLowerCase();
    const verified = response.data?.email_verified === true || String(response.data?.email_verified || '').toLowerCase() === 'true';
    const allowed = allowedRefundGoogleEmails();
    return {
      ok: Boolean(email && verified && allowed.has(email)),
      email,
      verified,
      allowed: allowed.has(email)
    };
  } catch (err) {
    return { ok: false, email: '', error: 'Google authorization could not be verified' };
  }
}

async function hasValidBillingAccess(req, { verifyGoogleAccessTokenFn = verifyGoogleAccessToken } = {}) {
  if (hasValidSyncToken(req)) return true;
  const google = await verifyGoogleAccessTokenFn(bearerToken(req));
  return Boolean(google.ok);
}

async function requireBillingAccess(req, res) {
  const ok = await hasValidBillingAccess(req);
  if (!ok) {
    res.status(401).json({ ok: false, error: 'Unauthorized refund request' });
    return false;
  }
  return true;
}

function createBillingRefundsRouter({ refundLedger, providerScope, refundCurrency, verifyRefundHistoryFn, refundTransactionFn } = {}) {
  const router = express.Router();
  const processRefund = createGuardedRefundProcessor({ ledger: refundLedger, providerScope, refundCurrency, verifyRefundHistoryFn, refundTransactionFn });
  const providerHistoryConfigured = refundLedger?.requiresRequestId !== true || typeof verifyRefundHistoryFn === 'function';

  // Authentication only: no provider, ledger, spreadsheet or financial handler.
  // Exact GET path is admitted during maintenance; normal operations stay held.
  router.get('/refunds/auth-check', async (req, res) => {
    res.set('Cache-Control', 'no-store');
    if (!await requireBillingAccess(req, res)) return;
    return res.json({ ok: true, authenticated: true, operationStarted: false });
  });

  router.get('/refunds/health', (req, res) => {
    res.json({
      ok: true,
      route: '/billing/refunds',
      authRequired: true,
      authConfigured: Boolean(process.env.FF_SYNC_ADMIN_TOKEN || process.env.AUTHNET_SYNC_TOKEN),
      googleOauthAllowedEmails: Array.from(allowedRefundGoogleEmails()),
      authnetConfigured: Boolean(process.env.AUTHNET_API_LOGIN_ID && process.env.AUTHNET_TRANSACTION_KEY),
      billingSpreadsheetConfigured: Boolean(FF_BILLING_SPREADSHEET_ID()),
      subscriptionsSpreadsheetConfigured: Boolean(FF_SUBSCRIPTIONS_SPREADSHEET_ID()),
      liveRefundsEnabled: liveRefundsEnabled() && Boolean(refundLedger && providerScope) && providerHistoryConfigured,
      providerHistoryConfigured,
      persistentLedgerConfigured: Boolean(refundLedger && providerScope),
      liveRefundEmergencyDisableEnv: 'FF_BILLING_REFUNDS_DISABLED',
      refundFailureReporting: 'structured-authnet-transaction-response-errors',
      refundCardNumberFormat: 'last4-with-expiration-XXXX',
      liveRefundRequires: ['DRY-RUN OK', 'Approved By', 'Reason', 'typed confirmation', 'not emergency-disabled'],
      customerEmailsSentByRefundRoute: false,
      safety: 'Refund lookup/dry-run/live-process route protected by admin token or verified Google OAuth allowlist. Live Auth.Net refund execution requires sheet row safeguards, typed confirmation, duplicate locks, and no emergency disable. Customer emails are never sent by this route.'
    });
  });

  router.post('/refunds/lookup', async (req, res) => {
    if (!await requireBillingAccess(req, res)) return;
    try {
      const lookup = req.body?.lookup || req.query?.lookup || '';
      const maxDetails = Number(req.body?.maxDetails || req.query?.maxDetails || 75) || 75;
      const result = await lookupRefundCandidates({ lookup, maxDetails });
      res.set({ 'Cache-Control': 'no-store, max-age=0', Pragma: 'no-cache' });
      return res.status(200).json(result);
    } catch (err) {
      console.error('FF BILLING REFUND LOOKUP ERROR:', err.message);
      return res.status(500).json({ ok: false, error: err.message });
    }
  });

  router.post('/refunds/dry-run', async (req, res) => {
    if (!await requireBillingAccess(req, res)) return;
    try {
      const result = await buildRefundDryRun(req.body || {});
      res.set({ 'Cache-Control': 'no-store, max-age=0', Pragma: 'no-cache' });
      return res.status(result.ok ? 200 : 409).json(result);
    } catch (err) {
      console.error('FF BILLING REFUND DRY-RUN ERROR:', err.message);
      return res.status(500).json({ ok: false, error: err.message });
    }
  });

  router.post('/refunds/process', async (req, res) => {
    if (!await requireBillingAccess(req, res)) return;
    try {
      const result = await processRefund(req.body || {});
      res.set({ 'Cache-Control': 'no-store, max-age=0', Pragma: 'no-cache' });
      return res.status(result.ok ? 200 : 409).json(result);
    } catch (err) {
      console.error('FF BILLING LIVE REFUND ERROR:', err.message);
      return res.status(500).json({
        ok: false,
        status: 'BLOCKED / ERROR',
        error: err.message,
        liveRefundsEnabled: liveRefundsEnabled(),
        safety: 'Live refund attempt failed or was blocked. No customer email was sent by this route.'
      });
    }
  });

  return router;
}

module.exports = {
  allowedRefundGoogleEmails,
  createBillingRefundsRouter,
  hasValidBillingAccess,
  hasValidSyncToken,
  verifyGoogleAccessToken
};
