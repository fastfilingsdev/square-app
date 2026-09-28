const { executeLedgerRefund } = require('../../core/refundLedger');
const { refundTransaction } = require('../../connectors/authnet/client');
const { processRefundLive, parseRefundTransactionId } = require('./refundProcess');
const { validateRefundProviderEvidence } = require('../../core/refundProviderEvidence');

// Dependencies are server-owned construction arguments, never request fields.
function createGuardedRefundProcessor({ ledger, providerScope, refundCurrency, verifyRefundHistoryFn,
  refundTransactionFn = refundTransaction, processRefundFn = processRefundLive } = {}) {
  return async function guardedRefund(input = {}) {
    if (!ledger || typeof providerScope !== 'string' || !/^[a-zA-Z0-9_-]{1,80}$/.test(providerScope)) {
      return { ok: false, status: 'LIVE REFUND DISABLED', code: 'REFUND_LEDGER_UNAVAILABLE',
        issues: ['Persistent refund protection is not configured. No refund was attempted.'],
        customerEmailSent: false, automatic_retry_allowed: false };
    }
    const partial = ledger.requiresRequestId === true;
    const requestId = input.requestId;
    if (partial && (refundCurrency !== 'USD' || typeof requestId !== 'string' ||
        !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(requestId))) {
      return {ok:false,status:'BLOCKED / ERROR',code:'REFUND_REQUEST_ID_REQUIRED',
        automatic_retry_allowed:false,customerEmailSent:false,
        issues:['A persisted refund request ID and configured currency are required. No refund was attempted.']};
    }
    let outcome;
    let selectedOperation;
    const result = await processRefundFn({ ...input, refundRequestId: partial ? requestId : '', refundTransactionFn: async request => {
      let response;
      selectedOperation = { providerScope, transactionId: request.refTransId, amount: request.amount,
        ...(partial ? {requestId,currency:refundCurrency} : {}) };
      if (partial) {
        try {
          if (typeof verifyRefundHistoryFn !== 'function') throw new Error('Missing trusted provider history');
          const evidence = await verifyRefundHistoryFn(Object.freeze({...selectedOperation}));
          validateRefundProviderEvidence(selectedOperation, evidence);
        } catch {
          outcome = {ok:false,code:'REFUND_PROVIDER_HISTORY_UNVERIFIED'};
          throw new Error('Refund provider history requires reconciliation');
        }
      }
      outcome = await executeLedgerRefund({ ledger,
        operation: selectedOperation,
        refund: async () => { response = await refundTransactionFn(request); return response; },
        parseRefundId: parseRefundTransactionId
      });
      if (!outcome.ok) throw new Error('Refund requires reconciliation; do not retry automatically');
      return response;
    } });
    if (outcome && !outcome.ok) {
      return { ok: false, status: 'RECONCILIATION REQUIRED', code: outcome.code,
        requires_reconciliation: true, automatic_retry_allowed: false, customerEmailSent: false,
        ...(partial ? {requestId} : {}),
        originalTransactionId:selectedOperation.transactionId,refundAmount:selectedOperation.amount,
        ...(outcome.providerRefundId ? { refundTransactionId: outcome.providerRefundId } : {}),
        issues: ['Refund outcome must be checked against the provider and persistent ledger before another attempt.'] };
    }
    return {...result,...(partial ? {requestId} : {})};
  };
}
module.exports = { createGuardedRefundProcessor };
