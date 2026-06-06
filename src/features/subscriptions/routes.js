const express = require('express');
const {
  syncAuthNetNewOrders,
  isArbAutoCreateEnabled,
  isNewOrdersAutomationEnabled,
  newOrdersAutomationIntervalMs,
  newOrdersAutomationLookbackDays,
  newOrdersAutomationMaxDetails
} = require('./authnetNewOrdersSync');

const automationState = {
  started: false,
  running: false,
  timer: null,
  initialTimer: null,
  lastRunAtUtc: null,
  lastSuccessAtUtc: null,
  lastErrorAtUtc: null,
  lastError: '',
  lastCounts: null,
  lastGuards: null
};

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
      arbLiveGateEnabled: isArbAutoCreateEnabled(),
      automation: {
        enabled: isNewOrdersAutomationEnabled(),
        started: automationState.started,
        running: automationState.running,
        intervalMs: newOrdersAutomationIntervalMs(),
        lookbackDays: newOrdersAutomationLookbackDays(),
        maxDetails: newOrdersAutomationMaxDetails(),
        lastRunAtUtc: automationState.lastRunAtUtc,
        lastSuccessAtUtc: automationState.lastSuccessAtUtc,
        lastErrorAtUtc: automationState.lastErrorAtUtc,
        lastError: automationState.lastError,
        lastCounts: automationState.lastCounts,
        lastGuards: automationState.lastGuards
      },
      safety: 'Targets FF - Billing / New Orders. Auto-discovers approved non-recurring $20/$29 numeric-invoice checkout transactions, blocks recurring ARB payments/declines/mismatches, creates ARBs only after verified original-charge evidence, then routes to Active Subscriptions and Onboarding. No customer emails, refunds, cancellations, or card/bank data handling.'
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

async function runNewOrdersAutomationOnce(triggeredBy = 'ff-billing-new-orders-automation') {
  if (automationState.running) {
    return { ok: true, skipped: true, reason: 'automation already running' };
  }
  automationState.running = true;
  automationState.lastRunAtUtc = new Date().toISOString();
  try {
    const result = await syncAuthNetNewOrders({
      mode: 'apply',
      triggeredBy,
      lookbackDays: newOrdersAutomationLookbackDays(),
      maxDetails: newOrdersAutomationMaxDetails(),
      arbMode: 'live',
      allowLiveArb: true
    });
    automationState.lastSuccessAtUtc = new Date().toISOString();
    automationState.lastError = '';
    automationState.lastCounts = result.counts || null;
    automationState.lastGuards = result.guards || null;
    console.log('FF Billing New Orders automation completed', JSON.stringify({ counts: automationState.lastCounts, guards: automationState.lastGuards }));
    return result;
  } catch (err) {
    automationState.lastErrorAtUtc = new Date().toISOString();
    automationState.lastError = String(err?.message || err).slice(0, 300);
    console.error('FF Billing New Orders automation error:', automationState.lastError);
    return { ok: false, error: automationState.lastError };
  } finally {
    automationState.running = false;
  }
}

function startNewOrdersAutomation({ initialDelayMs = 30000 } = {}) {
  if (automationState.started) return automationState;
  if (!isNewOrdersAutomationEnabled()) {
    console.log('FF Billing New Orders automation disabled by FF_BILLING_NEW_ORDERS_AUTOMATION_ENABLED=false');
    return automationState;
  }
  automationState.started = true;
  const intervalMs = newOrdersAutomationIntervalMs();
  const tick = () => runNewOrdersAutomationOnce('ff-billing-new-orders-auto').catch(err => {
    automationState.lastErrorAtUtc = new Date().toISOString();
    automationState.lastError = String(err?.message || err).slice(0, 300);
  });
  automationState.initialTimer = setTimeout(tick, Math.max(0, initialDelayMs));
  automationState.timer = setInterval(tick, intervalMs);
  if (automationState.initialTimer.unref) automationState.initialTimer.unref();
  if (automationState.timer.unref) automationState.timer.unref();
  console.log('FF Billing New Orders automation started', JSON.stringify({ intervalMs, lookbackDays: newOrdersAutomationLookbackDays(), arbLiveGateEnabled: isArbAutoCreateEnabled() }));
  return automationState;
}

module.exports = { createSubscriptionsRouter, startNewOrdersAutomation, runNewOrdersAutomationOnce };
