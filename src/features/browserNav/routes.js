const express = require('express');
const { buildAuditEvent, createRunId, writeAuditEvent } = require('./audit');
const { executeBrowserNavRun } = require('./executor');
const { createBrowserNavJobQueue } = require('./jobQueue');
const { requireBrowserNavAdmin } = require('./auth');
const { browserNavLiveEnabled, getBrowserNavStatus, normalizeBrowserNavRun, redactProfileConfig } = require('./policy');
const { defaultRunStore } = require('./runStore');

function createBrowserNavRouter({
  executeRun = executeBrowserNavRun,
  runStore = defaultRunStore,
  jobQueue = createBrowserNavJobQueue({ executeRun, runStore })
} = {}) {
  const router = express.Router();

  router.get('/health', (req, res) => {
    res.json(getBrowserNavStatus());
  });

  router.get('/runs', (req, res) => {
    if (!requireBrowserNavAdmin(req, res)) return;
    const runs = runStore.list({
      limit: req.query.limit,
      agentId: req.query.agentId || req.query.agent,
      profileId: req.query.profileId || req.query.profile,
      status: req.query.status
    });
    res.json({
      ok: true,
      count: runs.length,
      runs,
      safety: 'Run history is sanitized. It does not store fill text values, cookies, request headers, screenshot bytes, or full snapshot text.'
    });
  });

  router.get('/runs/:runId', (req, res) => {
    if (!requireBrowserNavAdmin(req, res)) return;
    const run = runStore.get(req.params.runId);
    if (!run) return res.status(404).json({ ok: false, error: 'Browser nav run not found' });
    return res.json({
      ok: true,
      run,
      safety: 'Run history is sanitized. It does not store fill text values, cookies, request headers, screenshot bytes, or full snapshot text.'
    });
  });

  router.get('/queue', (req, res) => {
    if (!requireBrowserNavAdmin(req, res)) return;
    res.json({
      ...jobQueue.status(),
      safety: 'Queue status is in-memory and sanitized. Concurrency is limited to one running browser navigation job per profile.'
    });
  });

  router.post('/plan', (req, res) => {
    if (!requireBrowserNavAdmin(req, res)) return;
    const normalized = normalizeBrowserNavRun(req.body || {});
    const runId = createRunId('bnplan');
    const audit = buildAuditEvent(runId, normalized, { status: normalized.ok ? 'planned' : 'blocked', ok: normalized.ok, error: normalized.errors.join('; ') || null });
    writeAuditEvent(audit);
    runStore.createFromNormalized(runId, normalized, {
      status: normalized.ok ? 'planned' : 'blocked',
      ok: normalized.ok,
      errors: normalized.errors,
      warnings: normalized.warnings,
      audit
    });
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
      runStore.createFromNormalized(runId, normalized, {
        status: 'blocked',
        ok: false,
        errors: normalized.errors,
        warnings: normalized.warnings,
        audit
      });
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
      runStore.createFromNormalized(runId, normalized, {
        status: 'dry-run',
        ok: true,
        warnings: normalized.warnings,
        audit
      });
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
      runStore.createFromNormalized(runId, normalized, {
        status: 'blocked-live-disabled',
        ok: false,
        errors: ['BROWSER_NAV_LIVE_ENABLED is not true'],
        warnings: normalized.warnings,
        audit
      });
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
      runStore.createFromNormalized(runId, normalized, {
        status: 'blocked-profile-not-configured',
        ok: false,
        errors: ['Browser profile CDP config is missing'],
        warnings: normalized.warnings,
        audit
      });
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
      const { result, audit } = await jobQueue.enqueue(runId, normalized, { wait: true });
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
        safety: 'Live browser navigation executed through the per-profile queue. Audit intentionally excludes form-fill text values, cookies, headers, and secrets.'
      });
    } catch (err) {
      const audit = buildAuditEvent(runId, normalized, { status: 'error', ok: false, error: err.message });
      writeAuditEvent(audit);
      runStore.createFromNormalized(runId, normalized, {
        status: 'error',
        ok: false,
        errors: [err.message],
        warnings: normalized.warnings,
        audit
      });
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

  router.post('/jobs', (req, res) => {
    if (!requireBrowserNavAdmin(req, res)) return;
    const normalized = normalizeBrowserNavRun(req.body || {});
    const runId = createRunId('bnjob');

    if (!normalized.ok) {
      const audit = buildAuditEvent(runId, normalized, { status: 'blocked', ok: false, error: normalized.errors.join('; ') });
      writeAuditEvent(audit);
      runStore.createFromNormalized(runId, normalized, {
        status: 'blocked',
        ok: false,
        errors: normalized.errors,
        warnings: normalized.warnings,
        audit
      });
      return res.status(409).json({
        ok: false,
        runId,
        status: 'blocked',
        errors: normalized.errors,
        warnings: normalized.warnings,
        audit,
        safety: 'Async browser job blocked by validation/policy before opening any browser.'
      });
    }

    if (normalized.run.dryRun) {
      const audit = buildAuditEvent(runId, normalized, { status: 'dry-run', ok: true });
      writeAuditEvent(audit);
      runStore.createFromNormalized(runId, normalized, {
        status: 'dry-run',
        ok: true,
        warnings: normalized.warnings,
        audit
      });
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
      runStore.createFromNormalized(runId, normalized, {
        status: 'blocked-live-disabled',
        ok: false,
        errors: ['BROWSER_NAV_LIVE_ENABLED is not true'],
        warnings: normalized.warnings,
        audit
      });
      return res.status(409).json({
        ok: false,
        runId,
        status: 'blocked-live-disabled',
        error: 'Live browser navigation is disabled. Set BROWSER_NAV_LIVE_ENABLED=true only after profile/permission review.',
        warnings: normalized.warnings,
        audit,
        safety: 'No browser job was queued because live execution is disabled by env.'
      });
    }

    if (!normalized.profile) {
      const audit = buildAuditEvent(runId, normalized, { status: 'blocked-profile-not-configured', ok: false, error: 'Browser profile CDP config is missing' });
      writeAuditEvent(audit);
      runStore.createFromNormalized(runId, normalized, {
        status: 'blocked-profile-not-configured',
        ok: false,
        errors: ['Browser profile CDP config is missing'],
        warnings: normalized.warnings,
        audit
      });
      return res.status(409).json({
        ok: false,
        runId,
        status: 'blocked-profile-not-configured',
        error: 'Browser profile CDP config is missing. Configure BROWSER_NAV_PROFILES_JSON before live execution.',
        warnings: normalized.warnings,
        audit,
        safety: 'No browser job was queued because the requested profile has no CDP config.'
      });
    }

    const queued = jobQueue.enqueue(runId, normalized, { wait: false });
    return res.status(202).json({
      ...queued,
      warnings: normalized.warnings,
      run: normalized.run,
      profile: redactProfileConfig(normalized.profile),
      safety: 'Async browser job queued. It will run through the per-profile concurrency guard; inspect /browser-nav/runs/:runId for sanitized result history.'
    });
  });

  return router;
}

module.exports = {
  createBrowserNavRouter
};
