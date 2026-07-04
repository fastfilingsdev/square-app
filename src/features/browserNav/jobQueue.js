const { buildAuditEvent, writeAuditEvent } = require('./audit');
const { executeBrowserNavRun } = require('./executor');

function createBrowserNavJobQueue({ executeRun = executeBrowserNavRun, runStore } = {}) {
  if (!runStore) throw new Error('createBrowserNavJobQueue requires runStore');

  const queues = new Map();
  const active = new Map();

  function queueFor(profileId) {
    const key = String(profileId || 'default');
    if (!queues.has(key)) queues.set(key, []);
    return queues.get(key);
  }

  function queuedPosition(profileId, runId) {
    const queue = queueFor(profileId);
    const index = queue.findIndex(job => job.runId === runId);
    return index >= 0 ? index + 1 : 0;
  }

  function activeRunFor(profileId) {
    return active.get(String(profileId || 'default')) || null;
  }

  function enqueue(runId, normalized, { wait = false } = {}) {
    const profileId = normalized.run.profileId;
    const queue = queueFor(profileId);
    let resolveJob;
    const done = new Promise(resolve => { resolveJob = resolve; });
    const job = {
      runId,
      normalized,
      profileId,
      enqueuedAt: new Date().toISOString(),
      resolve: resolveJob
    };
    queue.push(job);
    runStore.createFromNormalized(runId, normalized, {
      status: 'queued',
      ok: true,
      warnings: normalized.warnings,
      audit: null
    });
    drain(profileId);

    const queued = {
      ok: true,
      runId,
      status: 'queued',
      profileId,
      queuePosition: queuedPosition(profileId, runId),
      activeRunId: activeRunFor(profileId)?.runId || null
    };
    if (!wait) return queued;
    return done;
  }

  async function drain(profileId) {
    const key = String(profileId || 'default');
    if (active.has(key)) return;
    const queue = queueFor(key);
    const job = queue.shift();
    if (!job) return;

    active.set(key, { runId: job.runId, profileId: key, startedAt: new Date().toISOString() });
    runStore.update(job.runId, {
      status: 'running',
      ok: true,
      startedAt: new Date().toISOString(),
      queuePosition: 0
    });

    const startedAt = Date.now();
    let result;
    let audit;
    try {
      result = await executeRun(job.normalized);
      audit = buildAuditEvent(job.runId, job.normalized, {
        status: result.status,
        ok: result.ok,
        error: result.error || null
      });
    } catch (err) {
      result = {
        ok: false,
        status: 'error',
        durationMs: Date.now() - startedAt,
        error: err.message,
        results: []
      };
      audit = buildAuditEvent(job.runId, job.normalized, {
        status: 'error',
        ok: false,
        error: err.message
      });
    }

    writeAuditEvent(audit);
    runStore.recordResult(job.runId, result, { audit, warnings: job.normalized.warnings });
    active.delete(key);
    job.resolve({ result, audit });
    // Continue asynchronously so a long chain cannot recurse deeply.
    setImmediate(() => drain(key));
  }

  function status() {
    const profiles = new Set([...queues.keys(), ...active.keys()]);
    const out = {};
    for (const profileId of profiles) {
      const queue = queueFor(profileId);
      const activeJob = activeRunFor(profileId);
      out[profileId] = {
        activeRunId: activeJob?.runId || null,
        activeStartedAt: activeJob?.startedAt || null,
        queuedCount: queue.length,
        queuedRunIds: queue.map(job => job.runId)
      };
    }
    return {
      ok: true,
      profiles: out,
      concurrency: 'one-running-browser-nav-job-per-profile'
    };
  }

  return {
    activeRunFor,
    enqueue,
    queueFor,
    status
  };
}

module.exports = {
  createBrowserNavJobQueue
};
