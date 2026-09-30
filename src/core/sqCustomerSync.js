'use strict';
const { createHmac, randomUUID } = require('node:crypto');

// Native testing observed a 404 at a ContentService output URL. Retry ONLY
// that same trusted read-only GET once; never the signed POST. Persistent 404
// remains unconfirmed; this does not assume why Google returned it.
async function readGoogleSyncOutput({ http, url, delay = ms => new Promise(resolve => setTimeout(resolve, ms)) }) {
  const output = new URL(url || '');
  if (output.origin !== 'https://script.googleusercontent.com' || output.pathname !== '/macros/echo' || output.username || output.password || output.hash) throw new Error('SQ output destination rejected');
  let result = await http.get(output.href, { timeout: 15000, maxRedirects: 0, validateStatus: () => true });
  if (result.status === 404) {
    await delay(1000);
    result = await http.get(output.href, { timeout: 15000, maxRedirects: 0, validateStatus: () => true });
  }
  return result;
}

// Dedicated signing secret, never the reporting/payment admin credential.
async function signedSqRequest({ http, env = process.env, now = Date.now, nonce = randomUUID, delay, action }) {
  if (!['syncCustomers', 'verifyConnection'].includes(action)) throw new Error('SQ action rejected');
  if (!env.SQ_CUSTOMER_SYNC_URL) return { skipped: true };
  const secret = env.SQ_CUSTOMER_SYNC_SECRET || '';
  if (secret.length < 32) throw new Error('SQ sync signing configuration missing');
  const url = new URL(env.SQ_CUSTOMER_SYNC_URL);
  if (url.origin !== 'https://script.google.com' || !/^\/macros\/s\/[A-Za-z0-9_-]+\/exec$/.test(url.pathname) || url.search || url.hash || url.username || url.password) {
    throw new Error('SQ sync destination rejected');
  }
  const body = { action, timestamp: now(), nonce: nonce() };
  body.signature = createHmac('sha256', secret).update([body.action, body.timestamp, body.nonce].join('\n')).digest('hex');
  try {
    let result = await http.post(url.href, body, { timeout: 15000, maxRedirects: 0, validateStatus: () => true });
    // ContentService redirects only to retrieve output. Never replay the signed
    // POST or attach credentials to the response download.
    if (result.status === 302 || result.status === 303) {
      result = await readGoogleSyncOutput({http, url:result.headers?.location, delay});
    }
    if (result.status !== 200 || result.data?.success !== true) throw new Error();
    if (action === 'verifyConnection' && (result.data.connectionVerified !== true || result.data.customerWrites !== 0)) throw new Error();
    return { success: true };
  } catch (_) {
    // Do not print axios errors: they contain the signed body and output URL.
    throw new Error('SQ sync outcome unconfirmed; reconcile before rerun');
  }
}
function triggerSqCustomerSync(options) { return signedSqRequest({ ...options, action: 'syncCustomers' }); }
function verifySqConnection(options) { return signedSqRequest({ ...options, action: 'verifyConnection' }); }
module.exports = { triggerSqCustomerSync, verifySqConnection, readGoogleSyncOutput };
