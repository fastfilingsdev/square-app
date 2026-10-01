'use strict';
const {membershipKey}=require('../../core/membershipLedger');
const ID=/^[1-9]\d{0,29}$/;
function createCancellationProcessor({env=process.env,ledger,providerScope,readContext,provider}){
  return async input=>{
    if(env.FF_GOOGLE_CANCELLATIONS_ENABLED!=='true')throw Error('Google cancellation disabled');
    if(!ledger||!providerScope||!input||Object.getPrototypeOf(input)!==Object.prototype||
      Object.keys(input).length!==2||!Number.isInteger(input.rowNumber)||input.rowNumber<2||input.rowNumber>10000||
      typeof input.subscriptionId!=='string'||!ID.test(input.subscriptionId))throw Error('Invalid cancellation request');
    const row=await readContext(input.rowNumber);
    if(String(row['Subscription ID']||'').trim()!==input.subscriptionId||!String(row['Customer Name']||'').trim()||
      !row['Cancel Requested At']||row['Processed At']||/^CANCELED|^ALREADY_CANCELED/i.test(row['Auth.Net Result']||'')){
      throw Error('Cancellation row not eligible');
    }
    const identity=String(row.Email||'').trim().toLowerCase();membershipKey(identity);
    const response=await provider('ARBGetSubscriptionRequest',{subscriptionId:input.subscriptionId,includeTransactions:false});
    const sub=response.subscription;
    const profileId=String(sub?.profile?.customerProfileId||'');
    if(!ID.test(profileId))throw Error('Cancellation customer binding missing');
    const profile=await provider('getCustomerProfileRequest',{customerProfileId:profileId});
    if(String(profile.profile?.customerProfileId)!==profileId||String(profile.profile?.email||'').trim().toLowerCase()!==identity){
      throw Error('Cancellation customer binding mismatch');
    }
    const status=String(sub.status||'').toLowerCase();
    if(['canceled','cancelled','terminated','expired'].includes(status))return {subscriptionId:input.subscriptionId,alreadyEnded:true};
    if(status!=='active')throw Error('Cancellation status requires review');
    return ledger.execute({providerScope,subscriptionId:input.subscriptionId,customerEmail:identity,cancel:async()=>{
      const fresh=await readContext(input.rowNumber);
      if(JSON.stringify(fresh)!==JSON.stringify(row))throw Error('Cancellation row changed; claim retained');
      const result=await provider('ARBCancelSubscriptionRequest',{subscriptionId:input.subscriptionId});
      return result.messages?.resultCode==='Ok';
    }});
  };
}
module.exports={createCancellationProcessor};
