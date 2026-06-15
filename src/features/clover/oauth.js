'use strict';

function trimTrailingSlash(value) {
  return String(value || '').trim().replace(/\/+$/, '');
}

function inferCloverAuthBaseUrl(apiBaseUrl) {
  const normalized = trimTrailingSlash(apiBaseUrl);
  if (/apisandbox\.dev\.clover\.com$/i.test(normalized)) {
    return 'https://sandbox.dev.clover.com';
  }
  if (/api\.eu\.clover\.com$/i.test(normalized)) {
    return 'https://www.eu.clover.com';
  }
  if (/api\.la\.clover\.com$/i.test(normalized)) {
    return 'https://www.la.clover.com';
  }
  return 'https://www.clover.com';
}

function getCloverOAuthConfig(env = process.env, overrides = {}) {
  const apiBaseUrl = trimTrailingSlash(
    overrides.apiBaseUrl ||
    env.CLOVER_API_BASE_URL ||
    env.CLOVER_BASE_URL ||
    'https://api.clover.com'
  );
  const authBaseUrl = trimTrailingSlash(
    overrides.authBaseUrl ||
    env.CLOVER_AUTH_BASE_URL ||
    inferCloverAuthBaseUrl(apiBaseUrl)
  );
  const clientId = String(
    overrides.clientId ||
    env.CLOVER_CLIENT_ID ||
    env.CLOVER_APP_ID ||
    ''
  ).trim();
  const clientSecret = String(
    overrides.clientSecret ||
    env.CLOVER_CLIENT_SECRET ||
    env.CLOVER_APP_SECRET ||
    ''
  ).trim();
  const redirectUri = String(
    overrides.redirectUri ||
    env.CLOVER_REDIRECT_URI ||
    'http://localhost:3000/clover/callback'
  ).trim();

  if (!clientId) {
    throw new Error('Missing CLOVER_CLIENT_ID / CLOVER_APP_ID in environment.');
  }

  if (!clientSecret) {
    throw new Error('Missing CLOVER_CLIENT_SECRET / CLOVER_APP_SECRET in environment.');
  }

  if (!redirectUri) {
    throw new Error('Missing CLOVER_REDIRECT_URI in environment.');
  }

  return {
    apiBaseUrl,
    authBaseUrl,
    clientId,
    clientSecret,
    redirectUri,
    environment: apiBaseUrl.includes('apisandbox.dev.clover.com') ? 'Sandbox' : 'Production'
  };
}

function getCloverOAuthConfigStatus(env = process.env, overrides = {}) {
  const apiBaseUrl = trimTrailingSlash(
    overrides.apiBaseUrl ||
    env.CLOVER_API_BASE_URL ||
    env.CLOVER_BASE_URL ||
    'https://api.clover.com'
  );
  const authBaseUrl = trimTrailingSlash(
    overrides.authBaseUrl ||
    env.CLOVER_AUTH_BASE_URL ||
    inferCloverAuthBaseUrl(apiBaseUrl)
  );
  const redirectUri = String(
    overrides.redirectUri ||
    env.CLOVER_REDIRECT_URI ||
    ''
  ).trim();
  const clientIdConfigured = !!String(
    overrides.clientId ||
    env.CLOVER_CLIENT_ID ||
    env.CLOVER_APP_ID ||
    ''
  ).trim();
  const clientSecretConfigured = !!String(
    overrides.clientSecret ||
    env.CLOVER_CLIENT_SECRET ||
    env.CLOVER_APP_SECRET ||
    ''
  ).trim();

  return {
    apiBaseUrl,
    authBaseUrl,
    redirectUri,
    redirectUriConfigured: !!redirectUri,
    clientIdConfigured,
    clientSecretConfigured,
    environment: apiBaseUrl.includes('apisandbox.dev.clover.com') ? 'Sandbox' : 'Production'
  };
}

function buildCloverAuthorizeUrl({ authBaseUrl, clientId, redirectUri, state }) {
  if (!authBaseUrl) throw new Error('Missing Clover auth base URL.');
  if (!clientId) throw new Error('Missing Clover client ID.');
  if (!redirectUri) throw new Error('Missing Clover redirect URI.');
  if (!state) throw new Error('Missing Clover OAuth state.');

  const params = new URLSearchParams({
    client_id: clientId,
    response_type: 'code',
    redirect_uri: redirectUri,
    state
  });

  return `${trimTrailingSlash(authBaseUrl)}/oauth/v2/authorize?${params.toString()}`;
}

function buildCloverTokenRequest({ clientId, clientSecret, code }) {
  if (!clientId) throw new Error('Missing Clover client ID.');
  if (!clientSecret) throw new Error('Missing Clover client secret.');
  if (!code) throw new Error('Missing Clover authorization code.');

  return {
    client_id: clientId,
    client_secret: clientSecret,
    code
  };
}

function buildCloverRefreshRequest({ clientId, refreshToken }) {
  if (!clientId) throw new Error('Missing Clover client ID.');
  if (!refreshToken) throw new Error('Missing Clover refresh token.');

  return {
    client_id: clientId,
    refresh_token: refreshToken
  };
}

function normalizeCloverTokenExpiration(value) {
  if (!value) return '';
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return String(value);
  // Clover v2 docs return Unix seconds for expiration timestamps.
  return new Date(numeric * 1000).toISOString();
}

module.exports = {
  buildCloverAuthorizeUrl,
  buildCloverRefreshRequest,
  buildCloverTokenRequest,
  getCloverOAuthConfig,
  getCloverOAuthConfigStatus,
  inferCloverAuthBaseUrl,
  normalizeCloverTokenExpiration
};
