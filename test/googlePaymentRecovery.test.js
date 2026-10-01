'use strict';
const test=require('node:test');const assert=require('node:assert/strict');
const {createRecoveryProcessor,nextMonthlyDate}=require('../src/features/googlePaymentReads/recovery');
const input={rowNumber:2,oldSubscriptionId:'700',transactionId:'800',amount:'20.00',startDate:'2026-10-30'};
function fixture({patchRow={},txPatch={},oldPatch={},profilePatch={},active=[],complete=true,env={FF_GOOGLE_RECOVERY_ENABLED:'true'},ledgerMode='create',failOperation='',changed=false}={}){
  const calls=[];let reads=0;let claim;
  const row={'Subscription ID':'700','Payment Update Type':'SUB RECAPTURE C - Terminated','Customer ID':'AZ-SYNTHETIC',Email:'test@example.test',...patchRow};
  const results={getTransactionDetailsRequest:{transaction:{transId:'800',responseCode:'1',transactionStatus:'settledSuccessfully',
    settleAmount:'20.00',submitTimeUTC:'2026-09-30T12:00:00Z',order:{invoiceNumber:'RST-700'},customer:{email:'test@example.test'},...txPatch}},
    ARBGetSubscriptionRequest:{subscription:{status:'terminated',amount:'20.00',paymentSchedule:{interval:{unit:'months',length:1}},profile:{customerProfileId:'900'},...oldPatch}},
    getCustomerProfileRequest:{profile:{customerProfileId:'900',email:'test@example.test',...profilePatch}},
    createCustomerProfileFromTransactionRequest:{customerProfileId:'900',customerPaymentProfileIdList:['901']},
    ARBCreateSubscriptionRequest:{subscriptionId:'1000'}};
  const run=createRecoveryProcessor({env,providerScope:'synthetic',now:()=>new Date('2026-10-01T00:00:00Z'),
    readContext:async()=>({row:{...row,...(changed&&++reads>1?{'Stop / Suppressed':'TRUE'}:{})},activeEmails:active,complete}),
    ledger:{execute:async x=>{claim=x;calls.push('claim');if(ledgerMode==='held')throw Error('held');
      if(ledgerMode==='replay')return {subscriptionId:'1000',replayed:true};return x.create();}},
    provider:async(op,body)=>{calls.push(op);if(op===failOperation)throw Error('timeout');return results[op];}});
  return {run,calls,getClaim:()=>claim,results};
}
test('disabled recovery and extra payload fields fail before any provider access',async()=>{
  const off=fixture({env:{}});await assert.rejects(off.run(input));assert.deepEqual(off.calls,[]);
  const extra=fixture();await assert.rejects(extra.run({...input,merchantAuthentication:{}}));assert.deepEqual(extra.calls,[]);
});
test('recovery binds provider evidence, original terminated membership and customer before shared claim',async()=>{
  const f=fixture();assert.equal((await f.run(input)).subscriptionId,'1000');
  assert.deepEqual(f.calls,['getTransactionDetailsRequest','ARBGetSubscriptionRequest','getCustomerProfileRequest','claim',
    'createCustomerProfileFromTransactionRequest','ARBCreateSubscriptionRequest']);
  assert.equal(f.getClaim().customerEmail,'test@example.test');assert.equal(f.getClaim().previousSubscriptionId,'700');
});
test('canceled is not terminated; mismatched payment/customer/schedule or incomplete scan cannot dispatch',async()=>{
  for(const patch of [{patchRow:{'Stop / Suppressed':'TRUE'}},{patchRow:{'Subscription ID':'701'}},
    {patchRow:{'Payment Update Status':'Completed'}},{active:['TEST@example.test']},{complete:false},
    {txPatch:{responseCode:'2'}},{txPatch:{transactionStatus:'refundSettledSuccessfully'}},
    {txPatch:{customer:{email:'other@example.test'}}},{txPatch:{order:{invoiceNumber:'RST-700-extra'}}},
    {txPatch:{settleAmount:'21.00'}},{txPatch:{subscription:{id:'700'}}},{oldPatch:{status:'canceled'}},
    {oldPatch:{status:'active'}},{oldPatch:{amount:'21.00'}},
    {oldPatch:{paymentSchedule:{interval:{unit:'months',length:3}}}},
    {profilePatch:{email:'other@example.test'}}]){
    const f=fixture(patch);await assert.rejects(f.run(input));assert.ok(!f.calls.includes('claim'));assert.ok(!f.calls.includes('ARBCreateSubscriptionRequest'));
  }
});
test('durable replay and held claims never invoke either provider mutation',async()=>{
  const replay=fixture({ledgerMode:'replay'});assert.equal((await replay.run(input)).replayed,true);
  assert.equal(replay.calls.at(-1),'claim');
  const held=fixture({ledgerMode:'held'});await assert.rejects(held.run(input));assert.equal(held.calls.at(-1),'claim');
});
test('malformed related provider membership inventory cannot dispatch',async()=>{
  for(const value of [{subscriptionId:['700']},['700','700'],['bad'],Array.from({length:101},(_,i)=>String(1000+i))]){
    const f=fixture();f.results.getCustomerProfileRequest.subscriptionIds=value;
    await assert.rejects(f.run(input));assert.ok(!f.calls.includes('claim'));
  }
});
test('profile uncertainty, ambiguous receipt and changed sheet preserve hold without ARB dispatch',async()=>{
  for(const options of [{failOperation:'createCustomerProfileFromTransactionRequest'},{changed:true},{}]){
    const f=fixture(options);if(!Object.keys(options).length)f.results.createCustomerProfileFromTransactionRequest.customerPaymentProfileIdList=['901','902'];
    await assert.rejects(f.run(input));assert.ok(!f.calls.includes('ARBCreateSubscriptionRequest'));
    assert.equal(f.calls.filter(x=>x==='createCustomerProfileFromTransactionRequest').length,1);
  }
});
test('billing date preserves month-end clamping and invalid date fails closed',()=>{
  assert.equal(nextMonthlyDate('2026-01-31T12:00:00Z'),'2026-02-28');assert.throws(()=>nextMonthlyDate(''));
});
