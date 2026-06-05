const express = require('express');
const { syncAuthNetNewOrders } = require('./authnetNewOrdersSync');

function hasValidSyncToken(req) {
  const expected = process.env.FF_SYNC_ADMIN_TOKEN || process.env.AUTHNET_SYNC_TOKEN || '';
  if (!expected) return false;
  const headerToken = String(req.get('x-ff-sync-token') || req.get('x-authnet-sync-token') || '').trim();
  const auth = String(req.get('authorization') || '').trim();
  const bearer = auth.toLowerCase().startsWith('bearer ') ? auth.slice(7).trim() : '';
  return headerToken === expected || bearer === expected;
}

function createSubscriptionsRouter() {
  const router = express.Router();

  router.get('/authnet/new-orders/sync/health', (req, res) => {
    res.json({
      ok: true,
      route: '/subscriptions/authnet/new-orders/sync',
      authRequired: true,
      authConfigured: Boolean(process.env.FF_SYNC_ADMIN_TOKEN || process.env.AUTHNET_SYNC_TOKEN),
      authnetConfigured: Boolean(process.env.AUTHNET_API_LOGIN_ID && process.env.AUTHNET_TRANSACTION_KEY),
      billingSpreadsheetConfigured: Boolean(process.env.FF_BILLING_SPREADSHEET_ID || process.env.BILLING_SPREADSHEET_ID || '1DANHiunfffxvN7PWBxxO0WIzPWVGeOlaWMEcH-eJxBg'),
      subscriptionsSpreadsheetConfigured: Boolean(process.env.FF_SUBSCRIPTIONS_SPREADSHEET_ID || process.env.SUBSCRIPTIONS_SPREADSHEET_ID || process.env.PAYMENT_UPDATE_SPREADSHEET_ID || process.env.GOOGLE_SHEETS_SPREADSHEET_ID),
      onboardingSpreadsheetConfigured: Boolean(process.env.FF_ONBOARDING_SPREADSHEET_ID || process.env.ONBOARDING_SPREADSHEET_ID),
      arbLiveGateEnabled: ['1', 'true', 'yes', 'on'].includes(String(process.env.FF_BILLING_ARB_AUTO_CREATE_ENABLED || '').trim().toLowerCase()),
      safety: 'Targets FF - Billing / New Orders. Pulls Auth.Net invoice/transaction evidence and routes verified rows to Subscription Conversions. Live ARB creation requires mode=apply, arbMode=live, allowLiveArb=true, and FF_BILLING_ARB_AUTO_CREATE_ENABLED=true; otherwise no money/payment mutations.'
    });
  });

  router.post('/authnet/new-orders/sync', async (req, res) => {
    if (!hasValidSyncToken(req)) {
      return res.status(401).json({ ok: false, error: 'Unauthorized Auth.Net sync request' });
    }

    try {
      const mode = String(req.body?.mode || req.query?.mode || 'dry-run').toLowerCase() === 'apply' ? 'apply' : 'dry-run';
      const triggeredBy = String(req.body?.triggeredBy || req.query?.triggeredBy || 'api').slice(0, 80);
      const lookbackDays = Number(req.body?.lookbackDays || req.query?.lookbackDays || 14) || 14;
      const maxDetails = Number(req.body?.maxDetails || req.query?.maxDetails || req.body?.maxRecords || req.query?.maxRecords || 2500) || 2500;
      const arbMode = String(req.body?.arbMode || req.query?.arbMode || 'dry-run').toLowerCase() === 'live' ? 'live' : 'dry-run';
      const allowLiveArb = req.body?.allowLiveArb === true;
      const result = await syncAuthNetNewOrders({ mode, triggeredBy, lookbackDays, maxDetails, arbMode, allowLiveArb });
      res.set({ 'Cache-Control': 'no-store, max-age=0', Pragma: 'no-cache' });
      return res.status(200).json(result);
    } catch (err) {
      console.error('AUTHNET NEW ORDERS SYNC ERROR:', err.message);
      return res.status(500).json({ ok: false, error: err.message });
    }
  });

  return router;
}

module.exports = { createSubscriptionsRouter };
