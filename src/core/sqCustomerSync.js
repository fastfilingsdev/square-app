'use strict';
const { createHmac, randomUUID } = require('node:crypto');

// Dedicated signing secret, never the reporting/payment admin credential.
async function triggerSqCustomerSync({ http, env = process.env, now = Date.now, nonce = randomUUID }) {
  if (!env.SQ_CUSTOMER_SYNC_URL) return { skipped: true };
  const secret = env.SQ_CUSTOMER_SYNC_SECRET || '';
  if (secret.length < 32) throw new Error('SQ sync signing configuration missing');
  const url = new URL(env.SQ_CUSTOMER_SYNC_URL);
  if (url.origin !== 'https://script.google.com' || !/^\/macros\/s\/[A-Za-z0-9_-]+\/exec$/.test(url.pathname) || url.search || url.hash || url.username || url.password) {
    throw new Error('SQ sync destination rejected');
  }
  const body = { action: 'syncCustomers', timestamp: now(), nonce: nonce() };
  body.signature = createHmac('sha256', secret).update([body.action, body.timestamp, body.nonce].join('\n')).digest('hex');
  try {
    let result = await http.post(url.href, body, { timeout: 15000, maxRedirects: 0, validateStatus: () => true });
    // ContentService redirects only to retrieve output. Never replay the signed
    // POST or attach credentials to the response download.
    if (result.status === 302 || result.status === 303) {
      const output = new URL(result.headers?.location || '');
      if (output.origin !== 'https://script.googleusercontent.com' || output.pathname !== '/macros/echo' || output.username || output.password || output.hash) throw new Error();
      result = await http.get(output.href, { timeout: 15000, maxRedirects: 0, validateStatus: () => true });
    }
    if (result.status !== 200 || result.data?.success !== true) throw new Error();
    return { success: true };
  } catch (_) {
    // Do not print axios errors: they contain the signed body and output URL.
    throw new Error('SQ sync outcome unconfirmed; reconcile before rerun');
  }
}
module.exports = { triggerSqCustomerSync };
