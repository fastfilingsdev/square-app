'use strict';

// Candidate Render start command: node scripts/authnet-new-orders-cron.js
// Keep schedule "30 13,14 * * *". Do not switch the live cron until reviewed.
// One dispatch per invocation is NOT durable idempotency across invocations.
const ENDPOINT = 'https://fastfilings-api.onrender.com/subscriptions/authnet/new-orders/sync?mode=apply&triggeredBy=render-cron-0630-pt';
const MAX_BYTES = 1024 * 1024;

function isScheduledMinute(now) {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/Los_Angeles', hour: '2-digit', minute: '2-digit', hourCycle: 'h23'
  }).formatToParts(now);
  return parts.find(p => p.type === 'hour').value === '06' &&
    parts.find(p => p.type === 'minute').value === '30';
}

async function readBoundedJson(response) {
  if (!response.body) throw new Error('Missing response');
  const chunks = [];
  let size = 0;
  for await (const chunk of response.body) {
    size += chunk.byteLength;
    if (size > MAX_BYTES) throw new Error('Oversized response');
    chunks.push(Buffer.from(chunk));
  }
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}

async function run({ env = process.env, now = new Date(), http = globalThis.fetch } = {}) {
  if (!isScheduledMinute(now)) return { ok: true, skipped: true, reason: 'outside_0630_pacific', exitCode: 0 };
  // Same primary/fallback precedence as the receiving router. Never log values.
  const token = env.FF_SYNC_ADMIN_TOKEN || env.AUTHNET_SYNC_TOKEN;
  if (typeof token !== 'string' || !token || token !== token.trim() || /[\r\n]/.test(token)) {
    return { ok: false, error: 'sync_credential_missing_or_invalid', requestAttempted: false, exitCode: 1 };
  }
  let response;
  try {
    response = await http(ENDPOINT, {
      method: 'POST', redirect: 'manual', signal: AbortSignal.timeout(900000),
      headers: { 'Content-Type': 'application/json', Accept: 'application/json', 'x-ff-sync-token': token },
      body: JSON.stringify({ arbMode: 'dry-run', allowLiveArb: false })
    });
    if (response.status !== 200) throw new Error('Unconfirmed response');
    const data = await readBoundedJson(response);
    if (!data || typeof data !== 'object' || Array.isArray(data) || data.ok !== true) {
      throw new Error('Unconfirmed acknowledgement');
    }
    // Do not copy provider errors, customer records, URLs or arbitrary counts to logs.
    return { ok: true, acknowledged: true, requestAttempted: true, automaticRetryAllowed: false, exitCode: 0 };
  } catch {
    // The server may already have applied writes. No retry, redirect or fallback.
    return { ok: false, error: 'sync_outcome_unconfirmed', requestAttempted: true,
      requiresReconciliation: true, automaticRetryAllowed: false, exitCode: 2 };
  } finally {
    // Cancel unread bodies on redirect/error without following the Location header.
    try { if (response?.body && !response.body.locked) await response.body.cancel(); } catch { /* sanitized */ }
  }
}

if (require.main === module) {
  run().then(result => { console.log(JSON.stringify(result)); process.exitCode = result.exitCode; })
    .catch(() => { console.log(JSON.stringify({ ok: false, error: 'cron_preflight_failed' })); process.exitCode = 1; });
}
module.exports = { run, isScheduledMinute, ENDPOINT };
