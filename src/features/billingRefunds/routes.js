const express = require('express');
const { buildRefundDryRun, lookupRefundCandidates, FF_BILLING_SPREADSHEET_ID, FF_SUBSCRIPTIONS_SPREADSHEET_ID } = require('./refundLookup');

function hasValidBillingToken(req) {
  const expected = process.env.FF_SYNC_ADMIN_TOKEN || process.env.AUTHNET_SYNC_TOKEN || '';
  if (!expected) return false;
  const headerToken = String(req.get('x-ff-sync-token') || req.get('x-authnet-sync-token') || '').trim();
  const auth = String(req.get('authorization') || '').trim();
  const bearer = auth.toLowerCase().startsWith('bearer ') ? auth.slice(7).trim() : '';
  return headerToken === expected || bearer === expected;
}

function createBillingRefundsRouter() {
  const router = express.Router();

  router.get('/refunds/health', (req, res) => {
    res.json({
      ok: true,
      route: '/billing/refunds',
      authRequired: true,
      authConfigured: Boolean(process.env.FF_SYNC_ADMIN_TOKEN || process.env.AUTHNET_SYNC_TOKEN),
      authnetConfigured: Boolean(process.env.AUTHNET_API_LOGIN_ID && process.env.AUTHNET_TRANSACTION_KEY),
      billingSpreadsheetConfigured: Boolean(FF_BILLING_SPREADSHEET_ID()),
      subscriptionsSpreadsheetConfigured: Boolean(FF_SUBSCRIPTIONS_SPREADSHEET_ID()),
      liveRefundsEnabled: false,
      safety: 'Refund lookup/dry-run only. Live Auth.Net refund execution and customer emails are disabled/not implemented in this phase.'
    });
  });

  router.post('/refunds/lookup', async (req, res) => {
    if (!hasValidBillingToken(req)) return res.status(401).json({ ok: false, error: 'Unauthorized refund lookup request' });
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
    if (!hasValidBillingToken(req)) return res.status(401).json({ ok: false, error: 'Unauthorized refund dry-run request' });
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
    if (!hasValidBillingToken(req)) return res.status(401).json({ ok: false, error: 'Unauthorized refund process request' });
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
  createBillingRefundsRouter,
  hasValidBillingToken
};
