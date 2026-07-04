const { summarizeStepForAudit } = require('./audit');

function summarizeResultForHistory(entry = {}) {
  const result = entry.result || {};
  const summary = {
    index: entry.index,
    ok: entry.ok !== false,
    durationMs: entry.durationMs || 0,
    action: result.action || null
  };

  if (entry.error) summary.error = String(entry.error).slice(0, 500);
  if (result.url) {
    try {
      const parsed = new URL(result.url);
      summary.urlHost = parsed.hostname;
      summary.urlPath = parsed.pathname.slice(0, 180);
    } catch (err) {
      summary.urlHost = '(invalid)';
    }
  }
  if (result.selector) summary.selector = String(result.selector).slice(0, 180);
  if (result.title) summary.title = String(result.title).slice(0, 180);
  if (result.text) summary.textLength = String(result.text).length;
  if (result.valueLength !== undefined) summary.valueLength = result.valueLength;
  if (result.byteLengthApprox !== undefined) summary.byteLengthApprox = result.byteLengthApprox;
  if (result.mimeType) summary.mimeType = result.mimeType;
  if (result.loaded !== undefined) summary.loaded = result.loaded;
  if (result.key) summary.key = result.key;

  return summary;
}

function createRunStore({ maxRuns = Number(process.env.BROWSER_NAV_RUN_HISTORY_LIMIT || 200) || 200 } = {}) {
  const runs = new Map();
  const order = [];
  const limit = Math.max(10, Math.min(maxRuns, 1000));

  function put(run) {
    if (!run?.runId) return null;
    if (!runs.has(run.runId)) order.push(run.runId);
    const value = {
      ...run,
      updatedAt: new Date().toISOString()
    };
    runs.set(run.runId, value);
    while (order.length > limit) {
      const oldest = order.shift();
      if (oldest) runs.delete(oldest);
    }
    return value;
  }

  function createFromNormalized(runId, normalized, { status = 'received', ok = true, errors = [], warnings = normalized.warnings || [], audit = null } = {}) {
    return put({
      runId,
      status,
      ok,
      createdAt: new Date().toISOString(),
      agentId: normalized.run.agentId,
      profileId: normalized.run.profileId,
      business: normalized.run.business || null,
      requestedBy: normalized.run.requestedBy || null,
      reason: normalized.run.reason || null,
      dryRun: normalized.run.dryRun,
      liveEnabled: normalized.run.liveEnabled,
      stepCount: normalized.run.steps.length,
      steps: normalized.run.steps.map(summarizeStepForAudit),
      warnings,
      errors,
      audit
    });
  }

  function update(runId, patch = {}) {
    const existing = runs.get(runId);
    if (!existing) return null;
    return put({ ...existing, ...patch });
  }

  function recordResult(runId, result = {}, { audit = null, warnings = [] } = {}) {
    return update(runId, {
      status: result.status || (result.ok ? 'completed' : 'failed'),
      ok: result.ok !== false,
      durationMs: result.durationMs || 0,
      failedStep: result.failedStep,
      error: result.error || null,
      warnings,
      resultSummaries: Array.isArray(result.results) ? result.results.map(summarizeResultForHistory) : [],
      audit
    });
  }

  function get(runId) {
    return runs.get(runId) || null;
  }

  function list({ limit: requestedLimit = 50, agentId = '', profileId = '', status = '' } = {}) {
    const n = Math.max(1, Math.min(Number(requestedLimit) || 50, 200));
    const agent = String(agentId || '').trim();
    const profile = String(profileId || '').trim();
    const wantedStatus = String(status || '').trim();
    const out = [];
    for (const runId of order.slice().reverse()) {
      const run = runs.get(runId);
      if (!run) continue;
      if (agent && run.agentId !== agent) continue;
      if (profile && run.profileId !== profile) continue;
      if (wantedStatus && run.status !== wantedStatus) continue;
      out.push(run);
      if (out.length >= n) break;
    }
    return out;
  }

  return {
    createFromNormalized,
    get,
    list,
    put,
    recordResult,
    update
  };
}

const defaultRunStore = createRunStore();

module.exports = {
  createRunStore,
  defaultRunStore,
  summarizeResultForHistory
};
