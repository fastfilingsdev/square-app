'use strict';
const { subscriptionFingerprint } = require('../../core/subscriptionLedger');
const { membershipKey } = require('../../core/membershipLedger');
const ID=/^[1-9]\d{0,29}$/;
const email=x=>String(x||'').trim().toLowerCase();
function money(value){
  const text=String(value??'');
  if(!/^\d{1,7}(?:\.\d{1,2})?$/.test(text))throw Error('Invalid recovery amount');
  const minor=Math.round(Number(text)*100);
  if(!Number.isSafeInteger(minor)||minor<=0)throw Error('Invalid recovery amount');
  return (minor/100).toFixed(2);
}
function nextMonthlyDate(value){
  if(typeof value!=='string'||!/^\d{4}-\d{2}-\d{2}T/.test(value))throw Error('Missing provider payment date');
  const d=new Date(value);if(!Number.isFinite(d.getTime()))throw Error('Invalid provider payment date');
  const next=new Date(Date.UTC(d.getUTCFullYear(),d.getUTCMonth()+1,1));
  const last=new Date(Date.UTC(next.getUTCFullYear(),next.getUTCMonth()+1,0)).getUTCDate();
  next.setUTCDate(Math.min(d.getUTCDate(),last));return next.toISOString().slice(0,10);
}
function validateRecoveryInput(input){
  return input && Object.getPrototypeOf(input)===Object.prototype &&
    Object.keys(input).length===5 && ['rowNumber','oldSubscriptionId','transactionId','amount','startDate'].every(k=>Object.hasOwn(input,k)) &&
    Number.isInteger(input.rowNumber)&&input.rowNumber>=2&&input.rowNumber<=10000&&
    typeof input.oldSubscriptionId==='string'&&ID.test(input.oldSubscriptionId)&&
    typeof input.transactionId==='string'&&ID.test(input.transactionId)&&
    typeof input.amount==='string'&&/^\d{1,7}\.\d{2}$/.test(input.amount)&&
    typeof input.startDate==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(input.startDate);
}

// Reader must load the fixed production workbook, not caller-supplied addresses.
// Provider operations are injected separately from the public read bridge.
function createRecoveryProcessor({env=process.env,ledger,providerScope,readContext,provider,now=()=>new Date()}){
  return async input=>{
    if(env.FF_GOOGLE_RECOVERY_ENABLED!=='true')throw Error('Google recovery disabled');
    if(!ledger||!providerScope||!validateRecoveryInput(input))throw Error('Invalid recovery request or configuration');
    const context=await readContext(input.rowNumber);
    const row=context.row;
    if(!row||String(row['Subscription ID']||'').trim()!==input.oldSubscriptionId||
      row['Payment Update Type']!=='SUB RECAPTURE C - Terminated'||!String(row['Customer ID']||'').trim()||
      /completed|resolved|recovered|cancel/i.test(String(row['Payment Update Status']||''))||
      /^(true|yes|1)$/i.test(String(row['Stop / Suppressed']||'')))throw Error('Recovery row is not eligible');
    const identity=email(row.Email);membershipKey(identity);
    // Pending and active records are both blocking. Never accept an incomplete
    // active-membership scan, nor match the first row among duplicate identities.
    if(context.complete!==true||context.activeEmails.some(x=>email(x)===identity))throw Error('Existing membership or incomplete membership check');
    const txResponse=await provider('getTransactionDetailsRequest',{transId:input.transactionId});
    const tx=txResponse.transaction;
    if(!tx||String(tx.transId)!==input.transactionId||String(tx.responseCode)!=='1'||
      !['settledSuccessfully','capturedPendingSettlement'].includes(tx.transactionStatus)||tx.subscription?.id||
      String(tx.order?.invoiceNumber||'')!=='RST-'+input.oldSubscriptionId||email(tx.customer?.email)!==identity||
      money(tx.settleAmount||tx.authAmount)!==money(input.amount)||nextMonthlyDate(tx.submitTimeUTC)!==input.startDate||
      input.startDate<now().toISOString().slice(0,10))throw Error('Recovery payment evidence does not match');
    const oldResponse=await provider('ARBGetSubscriptionRequest',{subscriptionId:input.oldSubscriptionId,includeTransactions:false});
    const old=oldResponse.subscription;
    if(!old||String(old.status).toLowerCase()!=='terminated'||money(old.amount)!==money(input.amount)||
      String(old.paymentSchedule?.interval?.unit)!=='months'||Number(old.paymentSchedule?.interval?.length)!==1||
      !ID.test(String(old.profile?.customerProfileId||'')))throw Error('Original membership is not eligible for recovery');
    const profileId=String(old.profile.customerProfileId);
    const profileResponse=await provider('getCustomerProfileRequest',{customerProfileId:profileId});
    if(String(profileResponse.profile?.customerProfileId)!==profileId||email(profileResponse.profile?.email)!==identity){
      throw Error('Provider customer identity does not match');
    }
    const related=profileResponse.subscriptionIds??[];
    if(!Array.isArray(related)||related.length>100||related.some(x=>!ID.test(String(x)))||new Set(related.map(String)).size!==related.length){
      throw Error('Related provider membership inventory is incomplete');
    }
    for(const relatedId of related.map(String)){
      if(relatedId===input.oldSubscriptionId)continue;
      const detail=await provider('ARBGetSubscriptionRequest',{subscriptionId:relatedId,includeTransactions:false});
      if(String(detail.subscription?.profile?.customerProfileId)!==profileId||
        !['terminated','canceled','cancelled','expired'].includes(String(detail.subscription?.status).toLowerCase())){
        throw Error('Another provider membership exists or cannot be verified');
      }
    }
    return ledger.execute({providerScope,transactionId:input.transactionId,customerEmail:identity,
      previousSubscriptionId:input.oldSubscriptionId,
      fingerprint:subscriptionFingerprint({invoice:tx.order.invoiceNumber,amount:money(input.amount),startDate:input.startDate,email:identity}),
      create:async()=>{
        // Both mutations occur only after the same durable customer claim as
        // new orders. Never reuse a profile on last-four digits alone.
        const created=await provider('createCustomerProfileFromTransactionRequest',{
          transId:input.transactionId,customerProfileId:profileId});
        const raw=created.customerPaymentProfileIdList;
        const ids=Array.isArray(raw)?raw:raw?.numericString;
        const list=Array.isArray(ids)?ids:(ids?[ids]:[]);
        if(String(created.customerProfileId)!==profileId||list.length!==1||!ID.test(String(list[0]))){
          throw Error('Recovery payment profile receipt unconfirmed');
        }
        // A cancellation/row change during provider reads must not be ignored.
        const fresh=await readContext(input.rowNumber);
        if(fresh.complete!==true||JSON.stringify(fresh.row)!==JSON.stringify(row)||
          fresh.activeEmails.some(x=>email(x)===identity))throw Error('Recovery context changed; claim retained');
        const result=await provider('ARBCreateSubscriptionRequest',{
          refId:('ff-c-'+input.transactionId).slice(0,20),subscription:{
            name:'Fast Filings Sales Tax Filing',
            paymentSchedule:{interval:{length:1,unit:'months'},startDate:input.startDate,totalOccurrences:9999,trialOccurrences:0},
            amount:money(input.amount),trialAmount:'0.00',
            order:{invoiceNumber:('SUB-'+input.oldSubscriptionId).slice(0,20),description:'Fast Filings Sales Tax Filing'},
            profile:{customerProfileId:profileId,customerPaymentProfileId:String(list[0])}
          }});
        return {subscriptionId:String(result.subscriptionId||'')};
      }});
  };
}
module.exports={createRecoveryProcessor,validateRecoveryInput,nextMonthlyDate,money};
