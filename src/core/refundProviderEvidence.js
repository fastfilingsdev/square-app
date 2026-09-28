const { minorUnits } = require('./refundBudget');

// Evidence must come from a server-owned provider-history adapter, never an
// HTTP body or a sheet balance. A snapshot is not a lock against manual credits.
// Standard linked-refund enforcement must be confirmed separately; unlinked
// credits require reconciliation because they lack original-payment identity.
function validateRefundProviderEvidence(operation, evidence, nowMs = Date.now()) {
  if (!evidence || evidence.complete !== true || evidence.linkedRefundPolicyVerified !== true ||
      evidence.unlinkedCreditsExcluded !== true || evidence.hasUncertainRefunds !== false ||
      evidence.providerScope !== operation.providerScope || evidence.transactionId !== operation.transactionId ||
      evidence.currency !== operation.currency || !Number.isSafeInteger(evidence.checkedAtMs) ||
      !Number.isSafeInteger(nowMs) || evidence.checkedAtMs > nowMs || nowMs - evidence.checkedAtMs > 30000) {
    throw new Error('Refund provider history is unverified; reconcile before proceeding');
  }
  const original = minorUnits(evidence.originalAmount);
  const remaining = minorUnits(evidence.remainingAmount);
  const requested = minorUnits(operation.amount);
  if (original === 0n || requested === 0n || remaining > original || requested > remaining) {
    throw new Error('Refund exceeds verified provider balance');
  }
}
module.exports = { validateRefundProviderEvidence };
