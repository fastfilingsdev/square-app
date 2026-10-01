'use strict';
const { randomUUID, createHash } = require('node:crypto');

// Both new-order and recovery writers use the same normalized provider email.
// A successor must name the last successful subscription, verified terminal by
// the server's provider reader. Unknown outcomes never expire or auto-release.
function membershipKey(email) {
  const normalized = String(email || '').trim().toLowerCase();
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(normalized) || normalized.length > 254) {
    throw Error('Verified membership email required');
  }
  return createHash('sha256').update(normalized).digest('hex');
}

function createMembershipLedger({ query }) {
  return Object.freeze({
    async execute({ providerScope, transactionId, fingerprint, customerEmail,
      previousSubscriptionId = '', create }) {
      if (!/^[a-zA-Z0-9_-]{1,80}$/.test(providerScope || '') ||
          !/^[1-9]\d{0,29}$/.test(transactionId || '') ||
          !/^[a-f0-9]{64}$/.test(fingerprint || '') ||
          (previousSubscriptionId !== '' && !/^[1-9]\d{0,29}$/.test(previousSubscriptionId)) ||
          typeof create !== 'function') throw Error('Invalid membership claim');
      const values = [providerScope, transactionId, membershipKey(customerEmail),
        fingerprint, previousSubscriptionId || null, randomUUID()];
      const claim = await query('SELECT * FROM public.ff_claim_membership($1,$2,$3,$4,$5,$6)', values);
      const row = claim.rows?.[0];
      if (claim.rows?.length !== 1) throw Error('Membership claim unconfirmed');
      if (row.outcome === 'succeeded' && /^[1-9]\d{0,29}$/.test(row.subscription_id || '')) {
        return { subscriptionId: row.subscription_id, replayed: true };
      }
      if (row.outcome !== 'claimed') throw Error('Membership creation held for reconciliation');
      const result = await create();
      if (!/^[1-9]\d{0,29}$/.test(result?.subscriptionId || '')) throw Error('Membership receipt unconfirmed');
      const finish = await query('SELECT public.ff_finish_membership($1,$2,$3,$4,$5,$6,$7) AS saved',
        [...values, result.subscriptionId]);
      if (finish.rows?.length !== 1 || finish.rows[0].saved !== true) throw Error('Membership receipt save unconfirmed');
      return { subscriptionId: result.subscriptionId, replayed: false };
    }
  });
}
module.exports = { createMembershipLedger, membershipKey };
