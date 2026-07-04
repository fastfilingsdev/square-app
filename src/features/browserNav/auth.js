function bearerToken(req) {
  const auth = String(req.get('authorization') || '').trim();
  return auth.toLowerCase().startsWith('bearer ') ? auth.slice(7).trim() : '';
}

function hasValidAdminToken(req) {
  const expected = process.env.FF_SYNC_ADMIN_TOKEN || process.env.AUTHNET_SYNC_TOKEN || '';
  if (!expected) return false;
  const headerToken = String(req.get('x-ff-sync-token') || req.get('x-authnet-sync-token') || '').trim();
  const bearer = bearerToken(req);
  return Boolean(expected && (headerToken === expected || bearer === expected));
}

function requireBrowserNavAdmin(req, res) {
  if (hasValidAdminToken(req)) return true;
  res.status(401).json({ ok: false, error: 'Unauthorized browser navigation request' });
  return false;
}

module.exports = {
  bearerToken,
  hasValidAdminToken,
  requireBrowserNavAdmin
};
