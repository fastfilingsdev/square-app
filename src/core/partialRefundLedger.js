const { randomUUID } = require('node:crypto');
const { minorUnits, decimalAmount } = require('./refundBudget');
const uuid = value => typeof value === 'string' && /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(value);

// Locally wired behind explicit server configuration only. Production enablement
// still requires caller IDs, baseline reconciliation and reviewed database roles.
function createPartialRefundLedger({ query } = {}) {
  if (typeof query !== 'function') throw new Error('Persistent refund ledger is required');
  const owned = new WeakSet();
  return Object.freeze({
    requiresRequestId: true,
    async claim(input = {}) {
      const { providerScope, transactionId, requestId, currency } = input;
      const amountMinor = minorUnits(input.amount);
      if (typeof providerScope !== 'string' || !/^[a-zA-Z0-9_-]{1,80}$/.test(providerScope) ||
          typeof transactionId !== 'string' || !/^[1-9][0-9]{0,29}$/.test(transactionId) ||
          !uuid(requestId) || typeof currency !== 'string' || !/^[A-Z]{3}$/.test(currency) || amountMinor === 0n) {
        throw new Error('Invalid partial refund operation');
      }
      const attemptId = randomUUID();
      let result;
      try {
        result = await query('SELECT * FROM ff_claim_partial_refund($1,$2,$3::uuid,$4::bigint,$5,$6::uuid)',
          [providerScope, transactionId, requestId, amountMinor.toString(), currency, attemptId]);
      } catch { throw new Error('Refund ledger claim unconfirmed; reconcile before retrying'); }
      if (result?.rowCount === 0 && result.rows?.length === 0) return null;
      if (result?.rowCount !== 1 || result.rows?.length !== 1 || result.rows[0].attempt_id !== attemptId) {
        throw new Error('Refund ledger claim unconfirmed; reconcile before retrying');
      }
      const claim = Object.freeze({providerScope,transactionId,requestId,currency,amount:decimalAmount(amountMinor),attemptId});
      owned.add(claim);
      return claim;
    },
    async finish(claim, state, receipt = null) {
      if (!owned.has(claim) || !['succeeded','needs_reconciliation'].includes(state) ||
          (state === 'succeeded' && (typeof receipt !== 'string' || !/^[1-9][0-9]{0,29}$/.test(receipt))) ||
          (state === 'needs_reconciliation' && receipt !== null)) throw new Error('Invalid refund ledger completion');
      owned.delete(claim);
      let result;
      try {
        result = await query('SELECT * FROM ff_finish_partial_refund($1,$2,$3::uuid,$4::bigint,$5::uuid,$6,$7)',
          [claim.providerScope,claim.transactionId,claim.requestId,minorUnits(claim.amount).toString(),claim.attemptId,state,receipt]);
      } catch { throw new Error('Refund ledger completion unconfirmed; reconcile before retrying'); }
      if (result?.rowCount !== 1 || result.rows?.length !== 1 || result.rows[0].attempt_id !== claim.attemptId) {
        throw new Error('Refund ledger completion unconfirmed; reconcile before retrying');
      }
    }
  });
}
module.exports = { createPartialRefundLedger };
