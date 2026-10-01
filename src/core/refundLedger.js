const { randomUUID } = require('node:crypto');

// The query adapter must execute against the primary, with autocommit and
// synchronous_commit=on. Never supply it from an HTTP request body.
const CLAIM_SQL = `INSERT INTO ff_refund_operations
  (provider_scope, original_transaction_id, amount, attempt_id, state)
  VALUES ($1, $2, $3::numeric, $4::uuid, 'dispatching')
  ON CONFLICT (provider_scope, original_transaction_id) DO NOTHING
  RETURNING attempt_id`;
const FINISH_SQL = `UPDATE ff_refund_operations
  SET state = $5, provider_refund_id = $6, updated_at = clock_timestamp()
  WHERE provider_scope = $1 AND original_transaction_id = $2
    AND amount = $3::numeric AND attempt_id = $4::uuid AND state = 'dispatching'
  RETURNING attempt_id`;

function validateOperation(input) {
  const { providerScope, transactionId, amount } = input || {};
  // Scope is a stable, server-configured provider-account/environment ID,
  // NOT a secret, request-supplied merchant ID or rotating API-key fingerprint.
  if (typeof providerScope !== 'string' || !/^[a-zA-Z0-9_-]{1,80}$/.test(providerScope) ||
      typeof transactionId !== 'string' || !/^[1-9][0-9]{0,29}$/.test(transactionId) ||
      typeof amount !== 'string' || !/^(0|[1-9][0-9]{0,9})\.[0-9]{2}$/.test(amount) || amount === '0.00') {
    throw new Error('Invalid refund ledger operation');
  }
  return { providerScope, transactionId, amount };
}

function createRefundLedger({ query } = {}) {
  if (typeof query !== 'function') throw new Error('Persistent refund ledger is required');
  const ownedClaims = new WeakSet();
  return Object.freeze({
    async claim(input) {
      const op = validateOperation(input);
      const attemptId = randomUUID();
      let result;
      try {
        result = await query(CLAIM_SQL, [op.providerScope, op.transactionId, op.amount, attemptId]);
      } catch {
        // INSERT may have committed despite a lost response. Do not retry it
        // or invoke the provider without proof this process acquired ownership.
        throw new Error('Refund ledger claim unconfirmed; reconcile before retrying');
      }
      if (result?.rowCount === 0 && Array.isArray(result.rows) && result.rows.length === 0) return null;
      if (result?.rowCount !== 1 || result.rows?.length !== 1 || result.rows[0].attempt_id !== attemptId) {
        throw new Error('Refund ledger claim unconfirmed; reconcile before retrying');
      }
      const claim = Object.freeze({ ...op, attemptId });
      ownedClaims.add(claim);
      return claim;
    },
    async finish(claim, state, providerRefundId = null) {
      if (!ownedClaims.has(claim) || !['succeeded', 'needs_reconciliation'].includes(state) ||
          (state === 'succeeded' && (typeof providerRefundId !== 'string' || !/^[1-9][0-9]{0,29}$/.test(providerRefundId))) ||
          (state === 'needs_reconciliation' && providerRefundId !== null)) {
        throw new Error('Invalid refund ledger completion');
      }
      // Single-use ownership; a failed/ambiguous completion is not retried here.
      ownedClaims.delete(claim);
      let result;
      try {
        result = await query(FINISH_SQL, [claim.providerScope, claim.transactionId, claim.amount, claim.attemptId, state, providerRefundId]);
      } catch {
        throw new Error('Refund ledger completion unconfirmed; reconcile before retrying');
      }
      if (result?.rowCount !== 1 || result.rows?.length !== 1 || result.rows[0].attempt_id !== claim.attemptId) {
        throw new Error('Refund ledger completion unconfirmed; reconcile before retrying');
      }
    }
  });
}

// Runs only the explicitly supplied provider adapter after durable ownership.
// Callers must complete approval, identity and amount preflight before calling.
async function executeLedgerRefund({ ledger, operation, refund, parseRefundId } = {}) {
  const blocked = code => ({ ok: false, code, automatic_retry_allowed: false, requires_reconciliation: true });
  if (!ledger || typeof ledger.claim !== 'function' || typeof ledger.finish !== 'function' ||
      typeof refund !== 'function' || typeof parseRefundId !== 'function') return blocked('REFUND_LEDGER_UNAVAILABLE');
  let claim;
  try { claim = await ledger.claim(operation); } catch { return blocked('REFUND_LEDGER_CLAIM_UNCONFIRMED'); }
  if (!claim) return blocked('REFUND_OPERATION_ALREADY_RECORDED');
  let providerRefundId;
  try {
    providerRefundId = parseRefundId(await refund());
    if (typeof providerRefundId !== 'string' || !/^[1-9][0-9]{0,29}$/.test(providerRefundId)) throw new Error('Unconfirmed provider result');
  } catch {
    try { await ledger.finish(claim, 'needs_reconciliation'); } catch { /* durable claim remains blocking */ }
    return blocked('REFUND_PROVIDER_OUTCOME_UNCONFIRMED');
  }
  try { await ledger.finish(claim, 'succeeded', providerRefundId); } catch {
    return { ...blocked('REFUND_LEDGER_COMPLETION_UNCONFIRMED'), providerRefundId };
  }
  return { ok: true, providerRefundId, automatic_retry_allowed: false, requires_reconciliation: false };
}

module.exports = { createRefundLedger, executeLedgerRefund };
