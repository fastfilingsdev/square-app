const express = require('express');
const { buildAuditEvent, createRunId, writeAuditEvent } = require('./audit');
const { executeBrowserNavRun } = require('./executor');
const { requireBrowserNavAdmin } = require('./auth');
const { browserNavLiveEnabled, getBrowserNavStatus, normalizeBrowserNavRun, redactProfileConfig } = require('./policy');

function createBrowserNavRouter({ executeRun = executeBrowserNavRun } = {}) {
  const router = express.Router();

  router.get('/health', (req, res) => {
    res.json(getBrowserNavStatus());
  });

  router.post('/plan', (req, res) => {
    if (!requireBrowserNavAdmin(req, res)) return;
    const normalized = normalizeBrowserNavRun(req.body || {});
    const runId = createRunId('bnplan');
    const audit = buildAuditEvent(runId, normalized, { status: normalized.ok ? 'planned' : 'blocked', ok: normalized.ok, error: normalized.errors.join('; ') || null });
    writeAuditEvent(audit);
    res.status(normalized.ok ? 200 : 409).json({
      ok: normalized.ok,
      runId,
      errors: normalized.errors,
      warnings: normalized.warnings,
      run: normalized.run,
      profile: normalized.profile ? redactProfileConfig(normalized.profile) : null,
      audit,
      safety: 'Plan only. No browser was opened and no navigation/action was performed.'
    });
  });

  router.post('/runs', async (req, res) => {
    if (!requireBrowserNavAdmin(req, res)) return;
    const normalized = normalizeBrowserNavRun(req.body || {});
    const runId = createRunId('bnrun');

    if (!normalized.ok) {
      const audit = buildAuditEvent(runId, normalized, { status: 'blocked', ok: false, error: normalized.errors.join('; ') });
      writeAuditEvent(audit);
      return res.status(409).json({
        ok: false,
        runId,
        status: 'blocked',
        errors: normalized.errors,
        warnings: normalized.warnings,
        audit,
        safety: 'Browser run blocked by validation/policy before opening any browser.'
      });
    }

    if (normalized.run.dryRun) {
      const audit = buildAuditEvent(runId, normalized, { status: 'dry-run', ok: true });
      writeAuditEvent(audit);
      return res.status(200).json({
        ok: true,
        runId,
        status: 'dry-run',
        warnings: normalized.warnings,
        run: normalized.run,
        profile: normalized.profile ? redactProfileConfig(normalized.profile) : null,
        audit,
        safety: 'Dry run only. No browser was opened and no navigation/action was performed.'
      });
    }

    if (!browserNavLiveEnabled()) {
      const audit = buildAuditEvent(runId, normalized, { status: 'blocked-live-disabled', ok: false, error: 'BROWSER_NAV_LIVE_ENABLED is not true' });
      writeAuditEvent(audit);
      return res.status(409).json({
        ok: false,
        runId,
        status: 'blocked-live-disabled',
        error: 'Live browser navigation is disabled. Set BROWSER_NAV_LIVE_ENABLED=true only after profile/permission review.',
        warnings: normalized.warnings,
        audit,
        safety: 'No browser was opened because live execution is disabled by env.'
      });
    }

    if (!normalized.profile) {
      const audit = buildAuditEvent(runId, normalized, { status: 'blocked-profile-not-configured', ok: false, error: 'Browser profile CDP config is missing' });
      writeAuditEvent(audit);
      return res.status(409).json({
        ok: false,
        runId,
        status: 'blocked-profile-not-configured',
        error: 'Browser profile CDP config is missing. Configure BROWSER_NAV_PROFILES_JSON before live execution.',
        warnings: normalized.warnings,
        audit,
        safety: 'No browser was opened because the requested profile has no CDP config.'
      });
    }

    try {
      const result = await executeRun(normalized);
      const audit = buildAuditEvent(runId, normalized, { status: result.status, ok: result.ok, error: result.error || null });
      writeAuditEvent(audit);
      return res.status(result.ok ? 200 : 409).json({
        ok: result.ok,
        runId,
        status: result.status,
        durationMs: result.durationMs,
        warnings: normalized.warnings,
        results: result.results,
        failedStep: result.failedStep,
        error: result.error,
        audit,
        safety: 'Live browser navigation executed through configured CDP profile. Audit intentionally excludes form-fill text values, cookies, headers, and secrets.'
      });
    } catch (err) {
      const audit = buildAuditEvent(runId, normalized, { status: 'error', ok: false, error: err.message });
      writeAuditEvent(audit);
      return res.status(500).json({
        ok: false,
        runId,
        status: 'error',
        error: err.message,
        audit,
        safety: 'Browser run failed before completion. Audit intentionally excludes secrets.'
      });
    }
  });

  return router;
}

module.exports = {
  createBrowserNavRouter
};
