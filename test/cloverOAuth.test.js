'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const {
  buildCloverAuthorizeUrl,
  buildCloverRefreshRequest,
  buildCloverTokenRequest,
  getCloverOAuthConfig,
  getCloverOAuthConfigStatus,
  inferCloverAuthBaseUrl,
  normalizeCloverTokenExpiration
} = require('../src/features/clover/oauth');

test('Clover OAuth config defaults to production v2 endpoints', () => {
  const config = getCloverOAuthConfig({
    CLOVER_CLIENT_ID: 'app-123',
    CLOVER_CLIENT_SECRET: 'secret-456'
  });

  assert.equal(config.apiBaseUrl, 'https://api.clover.com');
  assert.equal(config.authBaseUrl, 'https://www.clover.com');
  assert.equal(config.redirectUri, 'http://localhost:3000/clover/callback');
  assert.equal(config.environment, 'Production');
});

test('Clover OAuth config infers sandbox authorization host from sandbox API host', () => {
  const config = getCloverOAuthConfig({
    CLOVER_CLIENT_ID: 'sandbox-app',
    CLOVER_CLIENT_SECRET: 'sandbox-secret',
    CLOVER_BASE_URL: 'https://apisandbox.dev.clover.com'
  });

  assert.equal(config.apiBaseUrl, 'https://apisandbox.dev.clover.com');
  assert.equal(config.authBaseUrl, 'https://sandbox.dev.clover.com');
  assert.equal(config.environment, 'Sandbox');
});

test('Clover authorize URL uses v2 OAuth code flow with caller state', () => {
  const url = new URL(buildCloverAuthorizeUrl({
    authBaseUrl: 'https://www.clover.com/',
    clientId: 'app-123',
    redirectUri: 'https://fastfilings-api.onrender.com/clover/callback',
    state: 'state-token'
  }));

  assert.equal(url.origin + url.pathname, 'https://www.clover.com/oauth/v2/authorize');
  assert.equal(url.searchParams.get('client_id'), 'app-123');
  assert.equal(url.searchParams.get('response_type'), 'code');
  assert.equal(url.searchParams.get('redirect_uri'), 'https://fastfilings-api.onrender.com/clover/callback');
  assert.equal(url.searchParams.get('state'), 'state-token');
});

test('Clover token request preserves only required token-exchange fields', () => {
  assert.deepEqual(buildCloverTokenRequest({
    clientId: 'app-123',
    clientSecret: 'secret-456',
    code: 'auth-code'
  }), {
    client_id: 'app-123',
    client_secret: 'secret-456',
    code: 'auth-code'
  });
});

test('Clover refresh request uses single-use refresh token shape', () => {
  assert.deepEqual(buildCloverRefreshRequest({
    clientId: 'app-123',
    refreshToken: 'refresh-token'
  }), {
    client_id: 'app-123',
    refresh_token: 'refresh-token'
  });
});

test('Clover region host inference covers production regions', () => {
  assert.equal(inferCloverAuthBaseUrl('https://api.clover.com'), 'https://www.clover.com');
  assert.equal(inferCloverAuthBaseUrl('https://api.eu.clover.com'), 'https://www.eu.clover.com');
  assert.equal(inferCloverAuthBaseUrl('https://api.la.clover.com'), 'https://www.la.clover.com');
});

test('Clover token expiration normalizes Unix seconds to ISO for sheets', () => {
  assert.equal(normalizeCloverTokenExpiration(1709498373), '2024-03-03T20:39:33.000Z');
  assert.equal(normalizeCloverTokenExpiration('not-a-number'), 'not-a-number');
  assert.equal(normalizeCloverTokenExpiration(''), '');
});

test('Clover OAuth config fails closed without app credentials', () => {
  assert.throws(() => getCloverOAuthConfig({}), /Missing CLOVER_CLIENT_ID/);
  assert.throws(() => getCloverOAuthConfig({ CLOVER_CLIENT_ID: 'app-123' }), /Missing CLOVER_CLIENT_SECRET/);
});

test('Clover OAuth config status reports readiness without leaking credential values', () => {
  const status = getCloverOAuthConfigStatus({
    CLOVER_CLIENT_ID: 'sandbox-app-id',
    CLOVER_CLIENT_SECRET: 'sandbox-secret-value',
    CLOVER_BASE_URL: 'https://apisandbox.dev.clover.com',
    CLOVER_REDIRECT_URI: 'https://fastfilings-api.onrender.com/clover/callback'
  });

  assert.equal(status.environment, 'Sandbox');
  assert.equal(status.clientIdConfigured, true);
  assert.equal(status.clientSecretConfigured, true);
  assert.equal(status.redirectUriConfigured, true);
  assert.equal(status.redirectUri, 'https://fastfilings-api.onrender.com/clover/callback');
  assert.equal(JSON.stringify(status).includes('sandbox-app-id'), false);
  assert.equal(JSON.stringify(status).includes('sandbox-secret-value'), false);
});
