const axios = require('axios');
const express = require('express');
const { buildRefundDryRun, lookupRefundCandidates, FF_BILLING_SPREADSHEET_ID, FF_SUBSCRIPTIONS_SPREADSHEET_ID } = require('./refundLookup');

function bearerToken(req) {
  const auth = String(req.get('authorization') || '').trim();
  return auth.toLowerCase().startsWith('bearer ') ? auth.slice(7).trim() : '';
}

function hasValidSyncToken(req) {
  const expected = process.env.FF_SYNC_ADMIN_TOKEN || process.env.AUTHNET_SYNC_TOKEN || '';
  if (!expected) return false;
  const headerToken = String(req.get('x-ff-sync-token') || req.get('x-authnet-sync-token') || '').trim();
  const bearer = bearerToken(req);
  return headerToken === expected || bearer === expected;
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
      timeout: 10000
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
    return { ok: false, email: '', error: String(err.message || err).slice(0, 220) };
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

function createBillingRefundsRouter() {
  const router = express.Router();

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
      liveRefundsEnabled: false,
      safety: 'Refund lookup/dry-run only. Protected by admin token or verified Google OAuth allowlist. Live Auth.Net refund execution and customer emails are disabled/not implemented in this phase.'
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
    return res.status(409).json({
      ok: false,
      status: 'LIVE REFUND DISABLED',
      error: 'Live Auth.Net refund execution is intentionally disabled in this phase. Use /billing/refunds/lookup and /billing/refunds/dry-run for safe validation first.',
      liveRefundsEnabled: false,
      safety: 'No Auth.Net refund, void, charge, cancellation, ARB mutation, customer email, raw card/bank/profile data, or Returns operational edit was performed.'
    });
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
