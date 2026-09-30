const {createRefundLedgerRuntime}=require('./refundLedgerRuntime');
const {createRefundHistoryVerifier}=require('../features/billingRefunds/providerHistory');
const {refundTransaction}=require('../connectors/authnet/client');

// Configuration gates are operator attestations, not proof of provider policy.
// Leave unset until sandbox/account-policy and baseline reconciliation review.
function createRefundServiceRuntime({env=process.env, onPoolError,
  ledgerFactory=createRefundLedgerRuntime, historyFactory=createRefundHistoryVerifier,
  refundTransactionFn=refundTransaction}={}) {
  // The legacy single-claim adapter remains available for isolated tests, not
  // serving traffic: it does not enforce the approved partial/history policy.
  if ((env.FF_REFUND_LEDGER_DATABASE_URL || env.FF_REFUND_PROVIDER_SCOPE) &&
      env.FF_REFUND_LEDGER_MODE !== 'partial') {
    throw new Error('Refund history requires explicit partial ledger mode');
  }
  const historyEnabled=env.FF_REFUND_HISTORY_ENABLED;
  if(historyEnabled !== undefined && !['true','false'].includes(historyEnabled)) {
    throw new Error('Invalid refund history configuration');
  }
  let verifyRefundHistoryFn;
  let guardedRefundTransaction;
  if(historyEnabled==='true') {
    const endpoint=env.AUTHNET_API_URL || 'https://api2.authorize.net/xml/v1/request.api';
    if(env.FF_REFUND_LEDGER_MODE!=='partial' || env.FF_REFUND_CURRENCY!=='USD' ||
      !env.FF_REFUND_LEDGER_DATABASE_URL || !env.FF_REFUND_PROVIDER_SCOPE ||
      env.FF_REFUND_LINKED_POLICY_VERIFIED!=='true' || env.FF_REFUND_UNLINKED_CREDITS_EXCLUDED!=='true' ||
      !env.AUTHNET_API_LOGIN_ID || !env.AUTHNET_TRANSACTION_KEY ||
      !['https://api2.authorize.net/xml/v1/request.api','https://apitest.authorize.net/xml/v1/request.api'].includes(endpoint)) {
      throw new Error('Refund history prerequisites are unverified');
    }
    const keys=['AUTHNET_API_URL','AUTHNET_API_LOGIN_ID','AUTHNET_TRANSACTION_KEY',
      'FF_REFUND_PROVIDER_SCOPE','FF_REFUND_CURRENCY','FF_REFUND_LINKED_POLICY_VERIFIED',
      'FF_REFUND_UNLINKED_CREDITS_EXCLUDED','FF_REFUND_HISTORY_ENABLED'];
    const initial=keys.map(key=>env[key]);
    function assertUnchanged() {
      if(keys.some((key,i)=>env[key]!==initial[i])) throw new Error('Refund provider configuration changed; restart and reconcile');
    }
    const verifier=historyFactory({providerScope:env.FF_REFUND_PROVIDER_SCOPE,currency:env.FF_REFUND_CURRENCY,
      linkedRefundPolicyVerified:true,unlinkedCreditsExcluded:true});
    verifyRefundHistoryFn=async operation=>{
      assertUnchanged();
      const evidence=await verifier(operation);
      assertUnchanged();
      return evidence;
    };
    // A ledger claim may wait after history verification. Recheck immediately
    // before dispatch; a failed check leaves the reservation for reconciliation.
    guardedRefundTransaction=async request=>{
      assertUnchanged();
      return refundTransactionFn(request);
    };
  }
  const ledger=ledgerFactory({env,onPoolError});
  return Object.freeze({subscriptionOptions:ledger.subscriptionOptions,
    routerOptions:Object.freeze({...ledger.routerOptions,
    ...(verifyRefundHistoryFn ? {verifyRefundHistoryFn,refundTransactionFn:guardedRefundTransaction} : {})}),close:()=>ledger.close()});
}
module.exports={createRefundServiceRuntime};
