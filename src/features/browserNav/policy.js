const DEFAULT_MAX_STEPS = 30;
const DEFAULT_STEP_TIMEOUT_MS = 15000;
const DEFAULT_RUN_TIMEOUT_MS = 120000;

const DEFAULT_MARK_BLOCKED_HOSTS = [
  'accounts.google.com',
  'admin.google.com',
  'calendar.google.com',
  'contacts.google.com',
  'docs.google.com',
  'drive.google.com',
  'mail.google.com',
  'script.google.com',
  'sheets.google.com'
];

const DEFAULT_AGENT_POLICIES = {
  mark: {
    business: 'Fast Filings',
    allowedProfiles: ['mark', 'mark-bb'],
    blockedHosts: DEFAULT_MARK_BLOCKED_HOSTS,
    notes: 'Fast Filings Mark profile policy. Google Workspace browser navigation is blocked; use API-only Google tooling.'
  }
};

const DEFAULT_ALLOWED_ACTIONS = new Set([
  'navigate',
  'click',
  'fill',
  'press',
  'waitForSelector',
  'snapshot',
  'screenshot'
]);

function parseJsonEnv(name, fallback) {
  const raw = process.env[name];
  if (!raw) return fallback;
  try {
    return JSON.parse(raw);
  } catch (err) {
    return fallback;
  }
}

function browserNavLiveEnabled() {
  return String(process.env.BROWSER_NAV_LIVE_ENABLED || '').toLowerCase() === 'true';
}

function browserNavAuditStdoutEnabled() {
  return String(process.env.BROWSER_NAV_AUDIT_STDOUT || 'true').toLowerCase() !== 'false';
}

function getAgentPolicies() {
  const configured = parseJsonEnv('BROWSER_NAV_AGENT_POLICIES_JSON', null);
  if (!configured || typeof configured !== 'object' || Array.isArray(configured)) {
    return DEFAULT_AGENT_POLICIES;
  }

  const out = {};
  for (const [agentId, policy] of Object.entries(configured)) {
    if (!agentId || !policy || typeof policy !== 'object' || Array.isArray(policy)) continue;
    out[String(agentId).trim()] = {
      business: String(policy.business || '').trim() || null,
      allowedProfiles: Array.isArray(policy.allowedProfiles)
        ? policy.allowedProfiles.map(item => String(item).trim()).filter(Boolean)
        : [],
      blockedHosts: Array.isArray(policy.blockedHosts)
        ? policy.blockedHosts.map(item => String(item).trim().toLowerCase()).filter(Boolean)
        : [],
      notes: String(policy.notes || '').trim()
    };
  }
  return Object.keys(out).length ? out : DEFAULT_AGENT_POLICIES;
}

function getProfileConfigs() {
  const configured = parseJsonEnv('BROWSER_NAV_PROFILES_JSON', null);
  if (!configured || typeof configured !== 'object' || Array.isArray(configured)) return {};

  const out = {};
  for (const [profileId, profile] of Object.entries(configured)) {
    if (!profileId || !profile || typeof profile !== 'object' || Array.isArray(profile)) continue;
    out[String(profileId).trim()] = {
      cdpUrl: String(profile.cdpUrl || profile.cdp_url || '').trim(),
      cdpHttpUrl: String(profile.cdpHttpUrl || profile.cdp_http_url || '').trim(),
      defaultUrl: String(profile.defaultUrl || profile.default_url || 'about:blank').trim() || 'about:blank',
      label: String(profile.label || profileId).trim()
    };
  }
  return out;
}

function redactProfileConfig(profile = {}) {
  return {
    configured: Boolean(profile.cdpUrl || profile.cdpHttpUrl),
    hasCdpUrl: Boolean(profile.cdpUrl),
    hasCdpHttpUrl: Boolean(profile.cdpHttpUrl),
    defaultUrl: profile.defaultUrl || 'about:blank',
    label: profile.label || ''
  };
}

function normalizeHost(value) {
  return String(value || '').trim().toLowerCase().replace(/^\.+/, '').replace(/\.+$/, '');
}

function hostnameMatchesPattern(hostname, pattern) {
  const host = normalizeHost(hostname);
  const pat = normalizeHost(pattern);
  if (!host || !pat) return false;
  if (pat.startsWith('*.')) {
    const suffix = pat.slice(2);
    return host === suffix || host.endsWith(`.${suffix}`);
  }
  return host === pat;
}

function urlHostname(url) {
  try {
    const parsed = new URL(url);
    return parsed.hostname;
  } catch (err) {
    return '';
  }
}

function isBlockedUrlForPolicy(url, policy) {
  const hostname = urlHostname(url);
  if (!hostname) return false;
  return (policy.blockedHosts || []).some(pattern => hostnameMatchesPattern(hostname, pattern));
}

function getBrowserNavStatus() {
  const policies = getAgentPolicies();
  const profiles = getProfileConfigs();
  return {
    ok: true,
    route: '/browser-nav',
    authRequired: true,
    adminTokenConfigured: Boolean(process.env.FF_SYNC_ADMIN_TOKEN || process.env.AUTHNET_SYNC_TOKEN),
    liveEnabled: browserNavLiveEnabled(),
    liveEnableEnv: 'BROWSER_NAV_LIVE_ENABLED=true',
    profileConfigEnv: 'BROWSER_NAV_PROFILES_JSON',
    agentPolicyEnv: 'BROWSER_NAV_AGENT_POLICIES_JSON',
    maxSteps: Number(process.env.BROWSER_NAV_MAX_STEPS || DEFAULT_MAX_STEPS),
    allowedActions: Array.from(DEFAULT_ALLOWED_ACTIONS),
    agents: Object.fromEntries(Object.entries(policies).map(([agentId, policy]) => [agentId, {
      business: policy.business || null,
      allowedProfiles: policy.allowedProfiles || [],
      blockedHosts: policy.blockedHosts || [],
      notes: policy.notes || ''
    }])),
    profiles: Object.fromEntries(Object.entries(profiles).map(([profileId, profile]) => [profileId, redactProfileConfig(profile)])),
    safety: 'Browser navigation is admin-token protected, agent/profile scoped, audited, and live execution is disabled unless explicitly enabled by env. Mark policy blocks Google Workspace browser navigation.'
  };
}

function normalizeStepTimeout(value) {
  const n = Number(value || 0);
  if (!Number.isFinite(n) || n <= 0) return DEFAULT_STEP_TIMEOUT_MS;
  return Math.max(1000, Math.min(n, 60000));
}

function normalizeRunTimeout(value) {
  const n = Number(value || 0);
  if (!Number.isFinite(n) || n <= 0) return DEFAULT_RUN_TIMEOUT_MS;
  return Math.max(5000, Math.min(n, 10 * 60 * 1000));
}

function normalizeSelector(value) {
  const selector = String(value || '').trim();
  if (!selector) return '';
  if (selector.length > 500) return selector.slice(0, 500);
  return selector;
}

function normalizeText(value, limit = 5000) {
  const text = String(value ?? '');
  return text.length > limit ? text.slice(0, limit) : text;
}

function normalizeKey(value) {
  const key = String(value || '').trim();
  if (!key) return '';
  return key.length > 80 ? key.slice(0, 80) : key;
}

function normalizeBrowserNavRun(input = {}) {
  const policies = getAgentPolicies();
  const profiles = getProfileConfigs();
  const errors = [];
  const warnings = [];

  const agentId = String(input.agentId || input.agent || '').trim();
  const profileId = String(input.profileId || input.profile || '').trim();
  const reason = String(input.reason || '').trim().slice(0, 500);
  const requestedBy = String(input.requestedBy || input.requested_by || '').trim().slice(0, 120);
  const dryRun = input.dryRun === true || String(input.mode || '').toLowerCase() === 'dry-run';
  const runTimeoutMs = normalizeRunTimeout(input.timeoutMs || input.runTimeoutMs);

  if (!agentId) errors.push('agentId is required');
  if (!profileId) errors.push('profileId is required');

  const policy = policies[agentId];
  if (agentId && !policy) errors.push(`agentId ${agentId} is not configured for browser navigation`);
  if (policy && !policy.allowedProfiles.includes(profileId)) {
    errors.push(`profileId ${profileId} is not allowed for agentId ${agentId}`);
  }

  const profile = profiles[profileId];
  if (!profile) {
    warnings.push(`profileId ${profileId || '(missing)'} has no CDP config; live execution will be blocked until BROWSER_NAV_PROFILES_JSON is configured`);
  }

  const maxSteps = Math.max(1, Math.min(Number(process.env.BROWSER_NAV_MAX_STEPS || DEFAULT_MAX_STEPS) || DEFAULT_MAX_STEPS, 100));
  const rawSteps = Array.isArray(input.steps) ? input.steps : [];
  if (!Array.isArray(input.steps)) errors.push('steps must be an array');
  if (rawSteps.length < 1) errors.push('at least one step is required');
  if (rawSteps.length > maxSteps) errors.push(`too many steps: max ${maxSteps}`);

  const steps = [];
  rawSteps.slice(0, maxSteps).forEach((rawStep, index) => {
    const step = rawStep && typeof rawStep === 'object' && !Array.isArray(rawStep) ? rawStep : {};
    const action = String(step.action || step.type || '').trim();
    const normalized = {
      index,
      action,
      timeoutMs: normalizeStepTimeout(step.timeoutMs),
      label: String(step.label || '').trim().slice(0, 120)
    };

    if (!DEFAULT_ALLOWED_ACTIONS.has(action)) {
      errors.push(`steps[${index}].action is not allowed: ${action || '(missing)'}`);
    }

    if (action === 'navigate') {
      const url = String(step.url || '').trim();
      normalized.url = url;
      if (!/^https?:\/\//i.test(url) && url !== 'about:blank') {
        errors.push(`steps[${index}].url must be http(s) or about:blank`);
      }
      if (policy && isBlockedUrlForPolicy(url, policy)) {
        errors.push(`steps[${index}].url is blocked for agentId ${agentId}`);
      }
    }

    if (['click', 'fill', 'waitForSelector'].includes(action)) {
      normalized.selector = normalizeSelector(step.selector);
      if (!normalized.selector) errors.push(`steps[${index}].selector is required for ${action}`);
    }

    if (action === 'fill') {
      normalized.text = normalizeText(step.text ?? step.value ?? '', 10000);
    }

    if (action === 'press') {
      normalized.key = normalizeKey(step.key);
      if (!normalized.key) errors.push(`steps[${index}].key is required for press`);
    }

    if (action === 'screenshot') {
      normalized.fullPage = step.fullPage === true;
    }

    if (action === 'snapshot') {
      normalized.maxChars = Math.max(100, Math.min(Number(step.maxChars || 4000) || 4000, 20000));
    }

    steps.push(normalized);
  });

  return {
    ok: errors.length === 0,
    errors,
    warnings,
    run: {
      agentId,
      profileId,
      business: policy?.business || null,
      requestedBy: requestedBy || null,
      reason: reason || null,
      dryRun,
      runTimeoutMs,
      liveEnabled: browserNavLiveEnabled(),
      steps
    },
    policy: policy || null,
    profile: profile || null
  };
}

module.exports = {
  DEFAULT_ALLOWED_ACTIONS,
  DEFAULT_AGENT_POLICIES,
  DEFAULT_MARK_BLOCKED_HOSTS,
  browserNavAuditStdoutEnabled,
  browserNavLiveEnabled,
  getAgentPolicies,
  getBrowserNavStatus,
  getProfileConfigs,
  isBlockedUrlForPolicy,
  normalizeBrowserNavRun,
  redactProfileConfig
};
