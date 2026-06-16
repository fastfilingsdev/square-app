'use strict';

const crypto = require('crypto');

function getCloverConnectLinkSecret(env = process.env) {
  return String(
    env.CLOVER_CONNECT_LINK_SECRET ||
    env.PLATFORM_CONNECT_LINK_SECRET ||
    env.FF_PLATFORM_CONNECT_LINK_SECRET ||
    ''
  ).trim();
}

function isCloverConnectSignatureRequired(env = process.env, environment = 'Production') {
  const explicit = String(env.CLOVER_CONNECT_SIGNED_REQUIRED || '').trim().toLowerCase();
  if (['true', '1', 'yes', 'required'].includes(explicit)) return true;
  if (['false', '0', 'no', 'off'].includes(explicit)) return false;
  return String(environment || '').toLowerCase() === 'production';
}

function normalizeCustomerId(customerId) {
  const value = String(customerId || '').trim();
  if (!value) throw new Error('Missing customer_id.');
  if (!/^[A-Za-z0-9][A-Za-z0-9._:-]{0,80}$/.test(value)) {
    throw new Error('Invalid customer_id format.');
  }
  return value;
}

function normalizeExpires(expires) {
  const numeric = Number(expires);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    throw new Error('Invalid Clover connect link expiration.');
  }
  return Math.floor(numeric);
}

function signCloverConnectLink({ customerId, expires, secret }) {
  const normalizedCustomerId = normalizeCustomerId(customerId);
  const normalizedExpires = normalizeExpires(expires);
  const signingSecret = String(secret || '').trim();
  if (!signingSecret) throw new Error('Missing Clover connect link secret.');

  return crypto
    .createHmac('sha256', signingSecret)
    .update(`${normalizedCustomerId}.${normalizedExpires}`)
    .digest('hex');
}

function safeEqualHex(left, right) {
  const a = String(left || '').trim();
  const b = String(right || '').trim();
  if (!/^[a-f0-9]{64}$/i.test(a) || !/^[a-f0-9]{64}$/i.test(b)) return false;
  return crypto.timingSafeEqual(Buffer.from(a, 'hex'), Buffer.from(b, 'hex'));
}

function verifyCloverConnectLink({ customerId, expires, sig, secret, nowMs = Date.now() }) {
  const normalizedCustomerId = normalizeCustomerId(customerId);
  const normalizedExpires = normalizeExpires(expires);
  if (normalizedExpires < Math.floor(nowMs / 1000)) {
    return { ok: false, error: 'expired', customerId: normalizedCustomerId };
  }

  const expected = signCloverConnectLink({
    customerId: normalizedCustomerId,
    expires: normalizedExpires,
    secret
  });
  if (!safeEqualHex(expected, sig)) {
    return { ok: false, error: 'invalid-signature', customerId: normalizedCustomerId };
  }

  return { ok: true, customerId: normalizedCustomerId, expires: normalizedExpires };
}

function buildCloverConnectUrl({ baseUrl, customerId, secret, ttlSeconds = 7 * 24 * 60 * 60, nowMs = Date.now() }) {
  const normalizedCustomerId = normalizeCustomerId(customerId);
  const expires = Math.floor((nowMs + (Number(ttlSeconds) || 0) * 1000) / 1000);
  const sig = signCloverConnectLink({ customerId: normalizedCustomerId, expires, secret });
  const url = new URL('/clover/connect', String(baseUrl || '').replace(/\/+$/, '') || 'http://localhost:3000');
  url.searchParams.set('customer_id', normalizedCustomerId);
  url.searchParams.set('expires', String(expires));
  url.searchParams.set('sig', sig);
  return url.toString();
}

function getCloverConnectLinkStatus(env = process.env, environment = 'Production') {
  const secretConfigured = Boolean(getCloverConnectLinkSecret(env));
  const signatureRequired = isCloverConnectSignatureRequired(env, environment);
  return {
    signatureRequired,
    secretConfigured,
    productionReady: !signatureRequired || secretConfigured
  };
}

module.exports = {
  buildCloverConnectUrl,
  getCloverConnectLinkSecret,
  getCloverConnectLinkStatus,
  isCloverConnectSignatureRequired,
  normalizeCustomerId,
  signCloverConnectLink,
  verifyCloverConnectLink
};
