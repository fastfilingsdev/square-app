// Two-decimal payment currencies only. Never round or extract a number from
// malformed input. BigInt keeps cumulative accounting independent of floats.
function minorUnits(value) {
  if (typeof value !== 'string' && typeof value !== 'number') throw new Error('Invalid refund amount');
  if (typeof value === 'number' && !Number.isFinite(value)) throw new Error('Invalid refund amount');
  const text = String(value).trim();
  const match = /^(0|[1-9][0-9]{0,9})(?:\.([0-9]{1,2}))?$/.exec(text);
  if (!match) throw new Error('Invalid refund amount');
  return BigInt(match[1]) * 100n + BigInt((match[2] || '').padEnd(2, '0'));
}

function decimalAmount(cents) {
  if (typeof cents !== 'bigint' || cents < 0n) throw new Error('Invalid refund amount');
  return `${cents / 100n}.${String(cents % 100n).padStart(2, '0')}`;
}

// Server-side reconciliation building block, NOT an authorization to dispatch.
// The caller must establish provider snapshot completeness and fence external
// refund writers. A local snapshot alone cannot guarantee a global ceiling.
function reconcileRefundBudget({ originalAmount, providerRefunds, ledgerRefunds } = {}) {
  const original = minorUnits(originalAmount);
  if (original === 0n || !Array.isArray(providerRefunds) || !Array.isArray(ledgerRefunds)) {
    throw new Error('Invalid refund reconciliation');
  }
  const receipts = new Map();
  const requests = new Set();
  let unresolved = 0n;
  let pending = false;
  function receipt(id, amount) {
    if (typeof id !== 'string' || !/^[1-9][0-9]{0,29}$/.test(id)) throw new Error('Invalid refund receipt');
    const cents = minorUnits(amount);
    if (cents === 0n) throw new Error('Invalid refund amount');
    if (receipts.has(id) && receipts.get(id) !== cents) throw new Error('Conflicting refund evidence');
    receipts.set(id, cents);
  }
  for (const item of providerRefunds) {
    if (!item || item.state !== 'succeeded') throw new Error('Unresolved provider refund evidence');
    receipt(item.providerRefundId, item.amount);
  }
  for (const item of ledgerRefunds) {
    if (!item || typeof item.requestId !== 'string' || !item.requestId || requests.has(item.requestId)) {
      throw new Error('Invalid refund request identity');
    }
    requests.add(item.requestId);
    if (item.state === 'succeeded') {
      receipt(item.providerRefundId, item.amount);
    } else if (['dispatching', 'needs_reconciliation'].includes(item.state) && item.providerRefundId == null) {
      const cents = minorUnits(item.amount);
      if (cents === 0n) throw new Error('Invalid refund amount');
      unresolved += cents;
      pending = true;
    } else {
      throw new Error('Invalid refund ledger state');
    }
  }
  const consumed = [...receipts.values()].reduce((sum, amount) => sum + amount, unresolved);
  if (consumed > original) throw new Error('Refund evidence exceeds original payment; reconcile');
  return Object.freeze({ originalAmount: decimalAmount(original), consumedAmount: decimalAmount(consumed),
    remainingAmount: decimalAmount(original - consumed), requiresReconciliation: pending });
}

module.exports = { minorUnits, decimalAmount, reconcileRefundBudget };
