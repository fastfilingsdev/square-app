'use strict';
const {randomUUID}=require('node:crypto');
const {membershipKey}=require('./membershipLedger');
function createCancellationLedger({query}){
  return Object.freeze({async execute({providerScope,subscriptionId,customerEmail,cancel}){
    if(!/^[a-zA-Z0-9_-]{1,80}$/.test(providerScope||'')||!/^[1-9]\d{0,29}$/.test(subscriptionId||'')||typeof cancel!=='function'){
      throw Error('Invalid cancellation claim');
    }
    const values=[providerScope,subscriptionId,membershipKey(customerEmail),randomUUID()];
    const claim=await query('SELECT public.ff_claim_cancellation($1,$2,$3,$4) AS outcome',values);
    if(claim.rows?.length!==1)throw Error('Cancellation claim unconfirmed');
    if(claim.rows[0].outcome==='succeeded')return {subscriptionId,replayed:true};
    if(claim.rows[0].outcome!=='claimed')throw Error('Cancellation held for reconciliation');
    if(await cancel()!==true)throw Error('Cancellation receipt unconfirmed');
    const saved=await query('SELECT public.ff_finish_cancellation($1,$2,$3,$4) AS saved',values);
    if(saved.rows?.length!==1||saved.rows[0].saved!==true)throw Error('Cancellation receipt save unconfirmed');
    return {subscriptionId,replayed:false};
  }});
}
module.exports={createCancellationLedger};
