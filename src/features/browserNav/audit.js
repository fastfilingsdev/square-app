const crypto = require('crypto');
const { browserNavAuditStdoutEnabled } = require('./policy');

function createRunId(prefix = 'bnr') {
  return `${prefix}_${new Date().toISOString().replace(/[-:.TZ]/g, '').slice(0, 14)}_${crypto.randomBytes(4).toString('hex')}`;
}

function summarizeStepForAudit(step = {}) {
  const out = {
    index: step.index,
    action: step.action,
    label: step.label || ''
  };
  if (step.url) {
    try {
      const parsed = new URL(step.url);
      out.urlHost = parsed.hostname;
      out.urlPath = parsed.pathname.slice(0, 120);
    } catch (err) {
      out.urlHost = '(invalid)';
    }
  }
  if (step.selector) out.selector = step.selector.slice(0, 180);
  if (step.key) out.key = step.key;
  if (step.action === 'fill') out.textLength = String(step.text || '').length;
  return out;
}

function buildAuditEvent(runId, normalized, result = {}) {
  return {
    event: 'browser-nav-run',
    runId,
    timestamp: new Date().toISOString(),
    agentId: normalized.run.agentId,
    profileId: normalized.run.profileId,
    business: normalized.run.business || null,
    requestedBy: normalized.run.requestedBy || null,
    reason: normalized.run.reason || null,
    dryRun: normalized.run.dryRun,
    liveEnabled: normalized.run.liveEnabled,
    status: result.status || 'planned',
    ok: result.ok !== false,
    stepCount: normalized.run.steps.length,
    steps: normalized.run.steps.map(summarizeStepForAudit),
    error: result.error || null
  };
}

function writeAuditEvent(event) {
  if (!browserNavAuditStdoutEnabled()) return;
  // Intentionally log a single sanitized JSON line. Do not include fill values, auth headers, cookies, or screenshots.
  console.log(JSON.stringify({ audit: event }));
}

module.exports = {
  buildAuditEvent,
  createRunId,
  summarizeStepForAudit,
  writeAuditEvent
};
