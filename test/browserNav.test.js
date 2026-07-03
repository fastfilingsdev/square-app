const test = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const express = require('express');

const { createBrowserNavRouter } = require('../src/features/browserNav/routes');
const { normalizeBrowserNavRun, getBrowserNavStatus } = require('../src/features/browserNav/policy');

function withEnv(env, fn) {
  const old = {};
  for (const key of Object.keys(env)) {
    old[key] = process.env[key];
    if (env[key] === undefined) delete process.env[key];
    else process.env[key] = env[key];
  }
  return Promise.resolve()
    .then(fn)
    .finally(() => {
      for (const key of Object.keys(env)) {
        if (old[key] === undefined) delete process.env[key];
        else process.env[key] = old[key];
      }
    });
}

async function withServer(router, fn) {
  const app = express();
  app.use(express.json());
  app.use('/browser-nav', router);
  const server = http.createServer(app);
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address();
  try {
    return await fn(`http://127.0.0.1:${port}`);
  } finally {
    await new Promise(resolve => server.close(resolve));
  }
}

function headers(token = 'test-token') {
  return { 'content-type': 'application/json', 'x-ff-sync-token': token };
}

const validEnv = {
  FF_SYNC_ADMIN_TOKEN: 'test-token',
  BROWSER_NAV_LIVE_ENABLED: undefined,
  BROWSER_NAV_PROFILES_JSON: JSON.stringify({
    mark: { cdpHttpUrl: 'http://127.0.0.1:9222', defaultUrl: 'about:blank', label: 'Mark local Chrome' },
    'mark-bb': { cdpHttpUrl: 'http://browserbase.example.test', defaultUrl: 'about:blank', label: 'Mark AZ Browserbase' }
  }),
  BROWSER_NAV_AGENT_POLICIES_JSON: undefined
};

test('browser nav health is safe by default and exposes Mark policy', () => withEnv(validEnv, async () => {
  const status = getBrowserNavStatus();
  assert.equal(status.ok, true);
  assert.equal(status.liveEnabled, false);
  assert.deepEqual(status.agents.mark.allowedProfiles, ['mark', 'mark-bb']);
  assert.equal(status.profiles.mark.configured, true);
  assert.ok(status.agents.mark.blockedHosts.includes('mail.google.com'));
}));

test('browser nav plan requires admin token', () => withEnv(validEnv, async () => {
  await withServer(createBrowserNavRouter(), async base => {
    const res = await fetch(`${base}/browser-nav/plan`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ agentId: 'mark', profileId: 'mark', steps: [{ action: 'snapshot' }] })
    });
    assert.equal(res.status, 401);
    const body = await res.json();
    assert.equal(body.ok, false);
  });
}));

test('browser nav dry-run returns a plan without opening a browser', () => withEnv(validEnv, async () => {
  await withServer(createBrowserNavRouter(), async base => {
    const res = await fetch(`${base}/browser-nav/runs`, {
      method: 'POST',
      headers: headers(),
      body: JSON.stringify({
        agentId: 'mark',
        profileId: 'mark',
        dryRun: true,
        reason: 'unit test',
        steps: [{ action: 'navigate', url: 'https://example.com' }, { action: 'snapshot', maxChars: 500 }]
      })
    });
    assert.equal(res.status, 200);
    const body = await res.json();
    assert.equal(body.ok, true);
    assert.equal(body.status, 'dry-run');
    assert.equal(body.run.steps.length, 2);
    assert.match(body.safety, /No browser was opened/);
  });
}));

test('browser nav blocks live execution until explicitly enabled', () => withEnv(validEnv, async () => {
  await withServer(createBrowserNavRouter(), async base => {
    const res = await fetch(`${base}/browser-nav/runs`, {
      method: 'POST',
      headers: headers(),
      body: JSON.stringify({
        agentId: 'mark',
        profileId: 'mark',
        steps: [{ action: 'navigate', url: 'https://example.com' }]
      })
    });
    assert.equal(res.status, 409);
    const body = await res.json();
    assert.equal(body.status, 'blocked-live-disabled');
  });
}));

test('browser nav enforces agent/profile separation', () => withEnv(validEnv, async () => {
  const normalized = normalizeBrowserNavRun({
    agentId: 'mark',
    profileId: 'logan',
    steps: [{ action: 'snapshot' }]
  });
  assert.equal(normalized.ok, false);
  assert.match(normalized.errors.join('\n'), /not allowed/);
}));

test('browser nav blocks Mark Google Workspace browser navigation', () => withEnv(validEnv, async () => {
  const normalized = normalizeBrowserNavRun({
    agentId: 'mark',
    profileId: 'mark',
    steps: [{ action: 'navigate', url: 'https://sheets.google.com/spreadsheets/d/test' }]
  });
  assert.equal(normalized.ok, false);
  assert.match(normalized.errors.join('\n'), /blocked/);
}));

test('browser nav live route blocks cleanly when profile CDP config is missing', () => withEnv({
  ...validEnv,
  BROWSER_NAV_LIVE_ENABLED: 'true',
  BROWSER_NAV_PROFILES_JSON: '{}'
}, async () => {
  await withServer(createBrowserNavRouter(), async base => {
    const res = await fetch(`${base}/browser-nav/runs`, {
      method: 'POST',
      headers: headers(),
      body: JSON.stringify({ agentId: 'mark', profileId: 'mark', steps: [{ action: 'snapshot' }] })
    });
    assert.equal(res.status, 409);
    const body = await res.json();
    assert.equal(body.status, 'blocked-profile-not-configured');
    assert.match(body.safety, /No browser was opened/);
  });
}));

test('browser nav live route can execute through injected executor when enabled', () => withEnv({
  ...validEnv,
  BROWSER_NAV_LIVE_ENABLED: 'true'
}, async () => {
  let executed = false;
  const router = createBrowserNavRouter({
    executeRun: async normalized => {
      executed = true;
      assert.equal(normalized.run.agentId, 'mark');
      assert.equal(normalized.run.profileId, 'mark');
      return { ok: true, status: 'completed', durationMs: 12, results: [{ index: 0, ok: true, result: { action: 'snapshot', title: 'Example' } }] };
    }
  });

  await withServer(router, async base => {
    const res = await fetch(`${base}/browser-nav/runs`, {
      method: 'POST',
      headers: headers(),
      body: JSON.stringify({ agentId: 'mark', profileId: 'mark', steps: [{ action: 'snapshot' }] })
    });
    assert.equal(res.status, 200);
    const body = await res.json();
    assert.equal(body.ok, true);
    assert.equal(body.status, 'completed');
    assert.equal(executed, true);
  });
}));
