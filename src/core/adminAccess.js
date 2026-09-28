'use strict';

const { timingSafeEqual } = require('node:crypto');

function matchesSecret(value, expected) {
  if (!expected || !value) return false;
  const left = Buffer.from(String(value));
  const right = Buffer.from(String(expected));
  return left.length === right.length && timingSafeEqual(left, right);
}

function hasValidAdminToken(req, env = process.env) {
  const expected = env.FF_SYNC_ADMIN_TOKEN || env.AUTHNET_SYNC_TOKEN || '';
  const header = String(req.get('x-ff-sync-token') || req.get('x-authnet-sync-token') || '').trim();
  const auth = String(req.get('authorization') || '').trim();
  const bearer = auth.toLowerCase().startsWith('bearer ') ? auth.slice(7).trim() : '';
  return matchesSecret(header, expected) || matchesSecret(bearer, expected);
}

function requireAdminToken(req, res) {
  res.set('Cache-Control', 'no-store');
  if (hasValidAdminToken(req)) return true;
  res.status(401).json({ ok: false, error: 'Unauthorized request' });
  return false;
}

// Only attach this to fixed loopback URLs, never a user-supplied destination.
function internalAdminHeaders(env = process.env) {
  const token = env.FF_SYNC_ADMIN_TOKEN || env.AUTHNET_SYNC_TOKEN;
  if (!token) throw new Error('Internal admin authentication is not configured');
  return { 'x-ff-sync-token': token };
}

module.exports = { hasValidAdminToken, requireAdminToken, internalAdminHeaders };
