const {
  getAuthNetWebhook,
  updateAuthNetWebhook
} = require('../../connectors/authnet/client');

const DEFAULT_WEBHOOK_ID = '5fad34db-0bc8-4256-8f8a-969ce415258d';
const DEFAULT_WEBHOOK_URL = 'https://fastfilings-api.onrender.com/authnet/webhook';
const DEFAULT_CRITICAL_EVENTS = [
  'net.authorize.customer.paymentProfile.updated',
  'net.authorize.payment.authcapture.created',
  'net.authorize.customer.subscription.failed'
];

const webhookWatchdogState = {
  started: false,
  running: false,
  timer: null,
  initialTimer: null,
  checks: 0,
  reactivations: 0,
  lastRunAtUtc: null,
  lastSuccessAtUtc: null,
  lastReactivatedAtUtc: null,
  lastStatus: '',
  lastWebhook: null,
  lastAction: '',
  lastErrorAtUtc: null,
  lastError: ''
};

function nowIso() {
  return new Date().toISOString();
}

function envFlag(name, defaultValue = false) {
  const raw = process.env[name];
  if (raw === undefined || raw === null || String(raw).trim() === '') return defaultValue;
  return ['1', 'true', 'yes', 'on'].includes(String(raw).trim().toLowerCase());
}

function isWebhookWatchdogEnabled() {
  // Approved by Gil on 2026-06-10 after Auth.Net disabled the same webhook
  // again despite healthy 200 deliveries. The automation is intentionally
  // narrow: same existing webhook ID only, status=active only.
  return envFlag('AUTHNET_WEBHOOK_WATCHDOG_ENABLED', true);
}

function webhookWatchdogIntervalMs() {
  const minutes = Number(process.env.AUTHNET_WEBHOOK_WATCHDOG_INTERVAL_MINUTES || 5);
  const safeMinutes = Number.isFinite(minutes) && minutes > 0 ? minutes : 5;
  return Math.max(1, Math.min(safeMinutes, 60)) * 60 * 1000;
}

function getWatchdogWebhookId() {
  return String(process.env.AUTHNET_WEBHOOK_ID || process.env.AUTHNET_WATCHDOG_WEBHOOK_ID || DEFAULT_WEBHOOK_ID).trim();
}

function getWatchdogExpectedUrl() {
  return String(process.env.AUTHNET_WEBHOOK_EXPECTED_URL || process.env.AUTHNET_WATCHDOG_EXPECTED_URL || DEFAULT_WEBHOOK_URL).trim();
}

function getWatchdogCriticalEvents() {
  const raw = String(process.env.AUTHNET_WEBHOOK_WATCHDOG_CRITICAL_EVENTS || '').trim();
  if (!raw) return DEFAULT_CRITICAL_EVENTS;
  return raw.split(',').map(item => item.trim()).filter(Boolean);
}

function summarizeWebhook(webhook) {
  if (!webhook || typeof webhook !== 'object') return null;
  const eventTypes = Array.isArray(webhook.eventTypes) ? webhook.eventTypes.map(item => String(item || '').trim()).filter(Boolean) : [];
  const criticalEvents = getWatchdogCriticalEvents();
  const missingCriticalEvents = criticalEvents.filter(eventType => !eventTypes.includes(eventType));
  return {
    webhookId: String(webhook.webhookId || ''),
    name: String(webhook.name || ''),
    status: String(webhook.status || ''),
    url: String(webhook.url || ''),
    eventTypeCount: eventTypes.length,
    hasPaymentProfileUpdated: eventTypes.includes('net.authorize.customer.paymentProfile.updated'),
    missingCriticalEvents
  };
}

function shouldReactivateWebhook(summary, {
  webhookId = getWatchdogWebhookId(),
  expectedUrl = getWatchdogExpectedUrl()
} = {}) {
  if (!summary) return { ok: false, reason: 'missing-webhook-summary' };
  if (String(summary.webhookId || '') !== String(webhookId || '')) {
    return { ok: false, reason: 'webhook-id-mismatch' };
  }
  if (String(summary.url || '') !== String(expectedUrl || '')) {
    return { ok: false, reason: 'webhook-url-mismatch' };
  }
  if (Array.isArray(summary.missingCriticalEvents) && summary.missingCriticalEvents.length) {
    return { ok: false, reason: 'webhook-critical-events-missing' };
  }
  if (String(summary.status || '').toLowerCase() === 'active') {
    return { ok: false, reason: 'already-active' };
  }
  if (String(summary.status || '').toLowerCase() !== 'inactive') {
    return { ok: false, reason: `unsupported-status-${summary.status || 'blank'}` };
  }
  return { ok: true, reason: 'inactive-existing-webhook-safe-to-reactivate' };
}

function publicWatchdogState() {
  return {
    enabled: isWebhookWatchdogEnabled(),
    started: webhookWatchdogState.started,
    running: webhookWatchdogState.running,
    intervalMs: webhookWatchdogIntervalMs(),
    webhookId: getWatchdogWebhookId(),
    expectedUrl: getWatchdogExpectedUrl(),
    criticalEvents: getWatchdogCriticalEvents(),
    checks: webhookWatchdogState.checks,
    reactivations: webhookWatchdogState.reactivations,
    lastRunAtUtc: webhookWatchdogState.lastRunAtUtc,
    lastSuccessAtUtc: webhookWatchdogState.lastSuccessAtUtc,
    lastReactivatedAtUtc: webhookWatchdogState.lastReactivatedAtUtc,
    lastStatus: webhookWatchdogState.lastStatus,
    lastWebhook: webhookWatchdogState.lastWebhook,
    lastAction: webhookWatchdogState.lastAction,
    lastErrorAtUtc: webhookWatchdogState.lastErrorAtUtc,
    lastError: webhookWatchdogState.lastError,
    safety: 'Existing Auth.Net webhook watchdog only. If the configured webhook is inactive and still matches the expected ID, URL, and critical event set, it sends PUT status=active. It does not create webhooks, change URL/event types, charge/refund/cancel/create subscriptions, mutate profiles, or send customer emails.'
  };
}

async function runWebhookWatchdogOnce({
  triggeredBy = 'authnet-webhook-watchdog-auto',
  getWebhookFn = getAuthNetWebhook,
  updateWebhookFn = updateAuthNetWebhook
} = {}) {
  if (!isWebhookWatchdogEnabled()) {
    webhookWatchdogState.lastAction = 'disabled';
    return { ok: true, skipped: true, reason: 'watchdog-disabled', watchdog: publicWatchdogState() };
  }
  if (webhookWatchdogState.running) {
    return { ok: true, skipped: true, reason: 'watchdog-already-running', watchdog: publicWatchdogState() };
  }

  const webhookId = getWatchdogWebhookId();
  webhookWatchdogState.running = true;
  webhookWatchdogState.checks += 1;
  webhookWatchdogState.lastRunAtUtc = nowIso();
  try {
    const before = summarizeWebhook(await getWebhookFn(webhookId));
    webhookWatchdogState.lastWebhook = before;
    webhookWatchdogState.lastStatus = before ? before.status : '';
    const decision = shouldReactivateWebhook(before, { webhookId, expectedUrl: getWatchdogExpectedUrl() });
    if (!decision.ok) {
      webhookWatchdogState.lastAction = decision.reason;
      webhookWatchdogState.lastSuccessAtUtc = nowIso();
      webhookWatchdogState.lastError = '';
      return {
        ok: true,
        action: 'none',
        reason: decision.reason,
        triggeredBy,
        before,
        watchdog: publicWatchdogState()
      };
    }

    const updateResponse = summarizeWebhook(await updateWebhookFn(webhookId, { status: 'active' }));
    const after = summarizeWebhook(await getWebhookFn(webhookId));
    if (!after || String(after.status || '').toLowerCase() !== 'active') {
      throw new Error(`Auth.Net webhook reactivation did not verify active; after=${after ? after.status : 'missing'}`);
    }

    webhookWatchdogState.reactivations += 1;
    webhookWatchdogState.lastReactivatedAtUtc = nowIso();
    webhookWatchdogState.lastSuccessAtUtc = nowIso();
    webhookWatchdogState.lastStatus = after.status;
    webhookWatchdogState.lastWebhook = after;
    webhookWatchdogState.lastAction = 'reactivated-existing-webhook';
    webhookWatchdogState.lastError = '';
    console.warn('Auth.Net webhook watchdog reactivated existing webhook', JSON.stringify({ webhookId, triggeredBy, before, after }));
    return {
      ok: true,
      action: 'reactivated-existing-webhook',
      triggeredBy,
      before,
      updateResponse,
      after,
      watchdog: publicWatchdogState()
    };
  } catch (err) {
    webhookWatchdogState.lastErrorAtUtc = nowIso();
    webhookWatchdogState.lastError = String(err?.message || err).slice(0, 300);
    webhookWatchdogState.lastAction = 'error';
    console.error('Auth.Net webhook watchdog error:', webhookWatchdogState.lastError);
    return { ok: false, error: webhookWatchdogState.lastError, triggeredBy, watchdog: publicWatchdogState() };
  } finally {
    webhookWatchdogState.running = false;
  }
}

function startWebhookWatchdogAutomation({ initialDelayMs = 30000 } = {}) {
  if (webhookWatchdogState.started) return webhookWatchdogState;
  if (!isWebhookWatchdogEnabled()) {
    console.log('Auth.Net webhook watchdog disabled by AUTHNET_WEBHOOK_WATCHDOG_ENABLED=false');
    return webhookWatchdogState;
  }
  webhookWatchdogState.started = true;
  const intervalMs = webhookWatchdogIntervalMs();
  const tick = () => runWebhookWatchdogOnce({ triggeredBy: 'authnet-webhook-watchdog-auto' }).catch(err => {
    webhookWatchdogState.lastErrorAtUtc = nowIso();
    webhookWatchdogState.lastError = String(err?.message || err).slice(0, 300);
    webhookWatchdogState.running = false;
  });
  webhookWatchdogState.initialTimer = setTimeout(tick, Math.max(0, initialDelayMs));
  webhookWatchdogState.timer = setInterval(tick, intervalMs);
  if (webhookWatchdogState.initialTimer.unref) webhookWatchdogState.initialTimer.unref();
  if (webhookWatchdogState.timer.unref) webhookWatchdogState.timer.unref();
  console.log('Auth.Net webhook watchdog started', JSON.stringify({
    intervalMs,
    webhookId: getWatchdogWebhookId(),
    expectedUrl: getWatchdogExpectedUrl(),
    criticalEvents: getWatchdogCriticalEvents()
  }));
  return webhookWatchdogState;
}

function resetWebhookWatchdogStateForTest() {
  if (webhookWatchdogState.initialTimer) clearTimeout(webhookWatchdogState.initialTimer);
  if (webhookWatchdogState.timer) clearInterval(webhookWatchdogState.timer);
  Object.assign(webhookWatchdogState, {
    started: false,
    running: false,
    timer: null,
    initialTimer: null,
    checks: 0,
    reactivations: 0,
    lastRunAtUtc: null,
    lastSuccessAtUtc: null,
    lastReactivatedAtUtc: null,
    lastStatus: '',
    lastWebhook: null,
    lastAction: '',
    lastErrorAtUtc: null,
    lastError: ''
  });
}

module.exports = {
  getWatchdogCriticalEvents,
  getWatchdogExpectedUrl,
  getWatchdogWebhookId,
  isWebhookWatchdogEnabled,
  publicWatchdogState,
  resetWebhookWatchdogStateForTest,
  runWebhookWatchdogOnce,
  shouldReactivateWebhook,
  startWebhookWatchdogAutomation,
  summarizeWebhook,
  webhookWatchdogIntervalMs,
  __watchdogState: webhookWatchdogState
};
