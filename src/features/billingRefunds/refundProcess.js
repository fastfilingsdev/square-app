const { refundTransaction } = require('../../connectors/authnet/client');
const { buildRefundDryRun } = require('./refundLookup');

const RECENT_REFUND_TTL_MS = Number(process.env.FF_BILLING_REFUND_RECENT_LOCK_TTL_MS || 10 * 60 * 1000);
const recentRefunds = new Map();

function nowIso() {
  return new Date().toISOString();
}

function normalizeString(value) {
  return String(value == null ? '' : value).trim();
}

function liveRefundsEnabled() {
  return /^(1|true|yes|enabled)$/i.test(normalizeString(process.env.FF_BILLING_REFUNDS_LIVE_ENABLED));
}

function money(value) {
  const num = Number(String(value == null ? '' : value).replace(/[$,]/g, ''));
  if (!Number.isFinite(num) || num <= 0) return '';
  return num.toFixed(2);
}

function recentKey({ transactionId, refundAmount }) {
  return `${normalizeString(transactionId)}|${money(refundAmount)}`;
}

function clearExpiredRecentRefunds(now = Date.now()) {
  for (const [key, value] of recentRefunds.entries()) {
    if (!value?.createdAtMs || now - value.createdAtMs > RECENT_REFUND_TTL_MS) recentRefunds.delete(key);
  }
}

function reserveRecentRefund(key, now = Date.now()) {
  clearExpiredRecentRefunds(now);
  if (recentRefunds.has(key)) return false;
  recentRefunds.set(key, { createdAtMs: now });
  return true;
}

function releaseRecentRefund(key) {
  recentRefunds.delete(key);
}

function requireLiveRefundApproval({ liveConfirm, approvedBy, reason }) {
  const issues = [];
  if (!liveRefundsEnabled()) issues.push('live refund gate is disabled');
  if (normalizeString(liveConfirm) !== 'PROCESS LIVE REFUND') issues.push('missing live confirmation token');
  if (!normalizeString(approvedBy)) issues.push('Approved By is required');
  if (!normalizeString(reason)) issues.push('Reason is required');
  return issues;
}

function parseRefundTransactionId(authNetResponse) {
  const tx = authNetResponse?.transactionResponse || authNetResponse?.createTransactionResponse?.transactionResponse || {};
  const responseCode = normalizeString(tx.responseCode);
  const transId = normalizeString(tx.transId || tx.transactionId);
  const errorValue = tx.errors?.error;
  const errors = Array.isArray(errorValue) ? errorValue : (errorValue ? [errorValue] : []);
  if (errors.length) {
    throw new Error(errors.map(item => `${item.errorCode || ''} ${item.errorText || ''}`.trim()).join('; '));
  }
  const messageValue = tx.messages?.message;
  const messages = Array.isArray(messageValue) ? messageValue : (messageValue ? [messageValue] : []);
  if (!transId || (responseCode && responseCode !== '1')) {
    const message = messages.map(item => `${item.code || ''} ${item.description || ''}`.trim()).filter(Boolean).join('; ');
    throw new Error(message || 'Authorize.Net refund did not return an approved transaction id');
  }
  return transId;
}

function safeSelectedSummary(selected = {}) {
  return {
    transactionId: selected.transactionId,
    invoiceNumber: selected.invoiceNumber,
    transactionDate: selected.transactionDate,
    originalAmount: selected.originalAmount,
    alreadyRefunded: selected.alreadyRefunded,
    refundableAmount: selected.refundableAmount,
    emailHash: selected.emailHash,
    name: selected.name,
    customerId: selected.customerId,
    subscriptionId: selected.subscriptionId,
    subscriptionStatus: selected.subscriptionStatus,
    sourceRows: selected.sourceRows
  };
}

function compactAuthNetFailureIssue(err) {
  const failure = err?.authNetFailure;
  const tx = failure?.transactionResponse;
  if (!tx) return '';
  const parts = [];
  if (tx.responseCode) parts.push(`transactionResponse.responseCode=${tx.responseCode}`);
  if (tx.transId) parts.push(`transactionResponse.transId=${tx.transId}`);
  if (tx.refTransId) parts.push(`transactionResponse.refTransId=${tx.refTransId}`);
  const errorText = (tx.errors || [])
    .map(item => `${item.code || ''} ${item.text || ''}`.trim())
    .filter(Boolean)
    .join('; ');
  const messageText = (tx.messages || [])
    .map(item => `${item.code || ''} ${item.text || ''}`.trim())
    .filter(Boolean)
    .join('; ');
  if (errorText) parts.push(`transactionResponse.errors=${errorText}`);
  if (messageText) parts.push(`transactionResponse.messages=${messageText}`);
  return parts.length ? `Authorize.Net detail: ${parts.join(', ')}` : '';
}

async function processRefundLive({
  lookup,
  transactionId,
  candidateNumber,
  refundType = 'FULL',
  refundAmount = '',
  reason = '',
  approvedBy = '',
  liveConfirm = '',
  refundTransactionFn = refundTransaction,
  ...deps
} = {}) {
  const approvalIssues = requireLiveRefundApproval({ liveConfirm, approvedBy, reason });
  const dryRun = await buildRefundDryRun({
    lookup: lookup || transactionId,
    transactionId,
    candidateNumber,
    refundType,
    refundAmount,
    reason,
    customerEmail: false,
    ...deps
  });

  if (!dryRun.ok) {
    return {
      ok: false,
      status: 'BLOCKED / ERROR',
      liveRefundsEnabled: liveRefundsEnabled(),
      issues: dryRun.issues,
      dryRun,
      safety: 'No Auth.Net refund, void, charge, cancellation, ARB mutation, customer email, raw card/bank/profile data, or Returns operational edit was performed.'
    };
  }

  if (approvalIssues.length) {
    return {
      ok: false,
      status: 'LIVE REFUND DISABLED',
      liveRefundsEnabled: liveRefundsEnabled(),
      issues: approvalIssues,
      dryRun,
      safety: 'Live refund was blocked before Auth.Net mutation. No customer email was sent.'
    };
  }

  const selected = dryRun.selected;
  const key = recentKey({ transactionId: selected.transactionId, refundAmount: dryRun.refundAmount });
  if (!reserveRecentRefund(key)) {
    return {
      ok: false,
      status: 'BLOCKED / ERROR',
      liveRefundsEnabled: true,
      issues: ['recent duplicate live refund attempt is locked; wait before retrying or verify Auth.Net/refund ledger first'],
      dryRun,
      safety: 'Duplicate guard blocked the Auth.Net refund call. No customer email was sent.'
    };
  }

  try {
    // Authorize.Net requires the refund payment object to match the original
    // transaction's payment instrument. Subscription records may point at the
    // customer's current ARB payment profile, which can differ from the card
    // that issued the original charge after a card update. Prefer the masked
    // card last4 from getTransactionDetails when available; use a profile only
    // as a fallback when transaction detail does not expose card last4.
    const useProfileFallback = !selected.cardLast4 && selected.__refundProfile;
    const authNetResponse = await refundTransactionFn({
      refTransId: selected.transactionId,
      amount: dryRun.refundAmount,
      cardLast4: selected.cardLast4,
      customerProfileId: useProfileFallback ? selected.__refundProfile.customerProfileId : '',
      customerPaymentProfileId: useProfileFallback ? selected.__refundProfile.customerPaymentProfileId : '',
      customer: selected.__refundRequiredFields?.customer || null,
      billTo: selected.__refundRequiredFields?.billTo || null,
      invoiceNumber: selected.invoiceNumber,
      description: `Fast Filings refund ${selected.invoiceNumber || selected.transactionId}`,
      emailCustomer: false,
      refId: `FFRF${String(selected.transactionId || '').slice(-8)}`
    });
    const refundTransactionId = parseRefundTransactionId(authNetResponse);
    return {
      ok: true,
      status: 'REFUNDED',
      processedAtUtc: nowIso(),
      refundTransactionId,
      refundAmount: dryRun.refundAmount,
      originalTransactionId: selected.transactionId,
      invoiceNumber: selected.invoiceNumber,
      subscriptionId: selected.subscriptionId,
      approvedBy: normalizeString(approvedBy),
      reason: normalizeString(reason),
      customerEmailSent: false,
      liveRefundsEnabled: true,
      selected: safeSelectedSummary(selected),
      safety: 'Live Auth.Net refund processed with emailCustomer=false. No customer email sent, no charge/cancel/ARB/profile mutation, no raw card/bank/profile data returned, and no Returns operational edit.'
    };
  } catch (err) {
    releaseRecentRefund(key);
    const authNetFailureIssue = compactAuthNetFailureIssue(err);
    const issues = [`Authorize.Net refund failed: ${String(err.message || err).slice(0, 500)}`];
    if (authNetFailureIssue) issues.push(authNetFailureIssue.slice(0, 700));
    return {
      ok: false,
      status: 'BLOCKED / ERROR',
      liveRefundsEnabled: true,
      issues,
      dryRun,
      selected: safeSelectedSummary(selected),
      ...(err?.authNetFailure ? { authNetFailure: err.authNetFailure } : {}),
      refundAmount: dryRun.refundAmount,
      originalTransactionId: selected.transactionId,
      invoiceNumber: selected.invoiceNumber,
      subscriptionId: selected.subscriptionId,
      customerEmailSent: false,
      safety: 'Authorize.Net refund was attempted but did not return an approved refund transaction. No customer email was sent, no charge/cancel/ARB/profile mutation was performed by this route, no raw card/bank/profile data returned, and no Returns operational edit was performed.'
    };
  }
}

module.exports = {
  liveRefundsEnabled,
  parseRefundTransactionId,
  processRefundLive,
  requireLiveRefundApproval,
  __billingRefundProcessTestHooks: {
    clearExpiredRecentRefunds,
    recentKey,
    recentRefunds,
    reserveRecentRefund,
    releaseRecentRefund
  }
};
