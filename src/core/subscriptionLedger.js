'use strict';
const { randomUUID, createHash } = require('node:crypto');

// A claim has no expiry or automatic reset. Unknown provider outcomes stay held.
// This protects one original charge, not unrelated charges for the same person.
function createSubscriptionLedger({ query }) {
  return Object.freeze({
    async execute({ providerScope, transactionId, fingerprint, create }) {
      if (!/^[a-zA-Z0-9_-]{1,80}$/.test(providerScope || '') ||
          !/^\d{1,30}$/.test(transactionId || '') ||
          !/^[a-f0-9]{64}$/.test(fingerprint || '') || typeof create !== 'function') {
        throw Error('Invalid subscription claim');
      }
      const attempt = randomUUID();
      const values = [providerScope, transactionId, fingerprint, attempt];
      const claimed = await query('SELECT * FROM public.ff_claim_subscription($1,$2,$3,$4)', values);
      const row = claimed.rows?.[0];
      if (claimed.rows?.length !== 1) throw Error('Subscription claim unconfirmed');
      if (row.outcome === 'succeeded' && /^\d{1,30}$/.test(row.subscription_id || '')) {
        return { subscriptionId: row.subscription_id, replayed: true };
      }
      if (row.outcome !== 'claimed') throw Error('Subscription creation held for reconciliation');
      // Never catch and retry a provider call, including profile creation.
      const result = await create();
      if (!/^\d{1,30}$/.test(result?.subscriptionId || '')) throw Error('Subscription receipt unconfirmed');
      const finished = await query('SELECT public.ff_finish_subscription($1,$2,$3,$4,$5) AS saved',
        [...values, result.subscriptionId]);
      if (finished.rows?.length !== 1 || finished.rows[0].saved !== true) throw Error('Subscription receipt save unconfirmed');
      return { subscriptionId: result.subscriptionId, replayed: false };
    }
  });
}

function subscriptionFingerprint({ invoice, amount, startDate, email }) {
  return createHash('sha256').update(JSON.stringify([
    String(invoice || '').trim(), String(amount || ''), String(startDate || ''),
    String(email || '').trim().toLowerCase()
  ])).digest('hex');
}
module.exports = { createSubscriptionLedger, subscriptionFingerprint };
