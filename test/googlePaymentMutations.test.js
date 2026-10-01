'use strict';
const test=require('node:test');const assert=require('node:assert/strict');
const fs=require('node:fs');const vm=require('node:vm');const path=require('node:path');
const {createRecoveryHandler}=require('../src/features/googlePaymentReads/mutations');
const {createCancellationProcessor}=require('../src/features/googlePaymentReads/cancellation');
const {createProviderTransport}=require('../src/features/googlePaymentReads/provider');
const {createCancellationLedger}=require('../src/core/cancellationLedger');
const identity={ok:true,verified:true,email:'returns@fastfilings.com'};
test('mutation admission is bounded before authentication and released on failure',async()=>{
  let release;let checks=0;
  const pending=new Promise(resolve=>{release=resolve;});
  const handler=createRecoveryHandler({env:{FF_GOOGLE_RECOVERY_ENABLED:'true'},verify:async()=>{checks++;await pending;return null;}});
  const held=Array.from({length:4},()=>handler({authorization:'Bearer synthetic',body:{}}));
  const rejected=await handler({authorization:'Bearer synthetic',body:{}});
  assert.equal(rejected.status,503);assert.equal(rejected.body.operationStarted,false);assert.equal(checks,4);
  release();await Promise.all(held);
  assert.equal((await handler({authorization:'Bearer synthetic',body:{}})).status,401);assert.equal(checks,5);
});
test('mutation routes remain behind maintenance and independent default-off gates',async()=>{
  const source=fs.readFileSync(path.join(__dirname,'../server.js'),'utf8');
  assert.ok(source.indexOf('app.use(maintenance.middleware)')<source.indexOf("app.use('/google-payments'"));
  for(const kind of ['recovery','cancellation']){
    const handler=createRecoveryHandler({kind,env:{},verify:()=>assert.fail('auth'),provider:()=>assert.fail('provider')});
    assert.equal((await handler({authorization:'Bearer fake',body:{}})).status,503);
  }
});
test('OAuth authorization cannot be replaced by admin token, other account or unverified email',async()=>{
  for(const user of [null,{...identity,verified:false},{...identity,email:'returns1@fastfilings.com'},{...identity,email:'other@example.test'}]){
    const handler=createRecoveryHandler({env:{FF_GOOGLE_RECOVERY_ENABLED:'true'},verify:async()=>user,
      provider:()=>assert.fail('provider'),readContext:()=>assert.fail('sheet')});
    assert.equal((await handler({authorization:'Bearer fake',body:{}})).status,401);
  }
});
test('handler never leaks exception detail or falsely reports no operation after uncertainty',async()=>{
  const handler=createRecoveryHandler({env:{FF_GOOGLE_RECOVERY_ENABLED:'true'},verify:async()=>{throw Error('SECRET');}});
  const result=await handler({authorization:'Bearer fake',body:{}});
  assert.equal(result.body.retryAutomatically,false);assert.equal(result.body.operationStarted,undefined);
  assert.ok(!JSON.stringify(result).includes('SECRET'));
});
function cancellation({rowPatch={},status='active',profileEmail='test@example.test',ledgerMode='create',changed=false}={}){
  let reads=0;const calls=[];
  const row={'Customer Name':'Synthetic',Email:'test@example.test','Subscription ID':'700','Cancel Requested At':'2026-10-01',...rowPatch};
  const run=createCancellationProcessor({env:{FF_GOOGLE_CANCELLATIONS_ENABLED:'true'},providerScope:'synthetic',
    readContext:async()=>({...row,...(changed&&++reads>1?{Email:'different@example.test'}:{})}),
    provider:async op=>{calls.push(op);return op==='ARBGetSubscriptionRequest'?{subscription:{status,profile:{customerProfileId:'900'}}}:
      op==='getCustomerProfileRequest'?{profile:{customerProfileId:'900',email:profileEmail}}:{messages:{resultCode:'Ok'}};},
    ledger:{execute:async input=>{calls.push('claim');if(ledgerMode==='held')throw Error('held');
      if(ledgerMode==='replay')return {subscriptionId:'700',replayed:true};assert.equal(await input.cancel(),true);return {subscriptionId:'700'};}}});
  return {run,calls};
}
test('cancellation reads fixed row and provider customer identity before durable claim and one mutation',async()=>{
  const f=cancellation();await f.run({rowNumber:2,subscriptionId:'700'});
  assert.deepEqual(f.calls,['ARBGetSubscriptionRequest','getCustomerProfileRequest','claim','ARBCancelSubscriptionRequest']);
});
test('wrong cancellation customer, row state, suspended subscription, extra fields and held claims cannot cancel',async()=>{
  for(const options of [{profileEmail:'other@example.test'},{rowPatch:{'Cancel Requested At':''}},
    {rowPatch:{'Processed At':'done'}},{status:'suspended'},{ledgerMode:'held'},{changed:true}]){
    const f=cancellation(options);await assert.rejects(f.run({rowNumber:2,subscriptionId:'700'}));
    assert.ok(!f.calls.includes('ARBCancelSubscriptionRequest'));
  }
  const f=cancellation();await assert.rejects(f.run({rowNumber:2,subscriptionId:'700',arbitrary:true}));assert.deepEqual(f.calls,[]);
});
test('already ended subscription is returned without provider mutation or claim',async()=>{
  for(const status of ['canceled','terminated','expired']){
    const f=cancellation({status});assert.equal((await f.run({rowNumber:2,subscriptionId:'700'})).alreadyEnded,true);
    assert.ok(!f.calls.includes('claim'));
  }
});
test('cancellation ledger does not retry lost claims, provider responses or completion acknowledgements',async()=>{
  const op={providerScope:'synthetic',subscriptionId:'700',customerEmail:'test@example.test'};
  for(const stage of ['claim','provider','finish']){
    let calls=0,queries=0;
    const ledger=createCancellationLedger({query:async()=>{queries++;if((stage==='claim'&&queries===1)||(stage==='finish'&&queries===2))throw Error('lost');
      return {rows:[queries===1?{outcome:'claimed'}:{saved:true}]};}});
    await assert.rejects(ledger.execute({...op,cancel:async()=>{calls++;if(stage==='provider')throw Error('lost');return true;}}));
    assert.equal(calls,stage==='claim'?0:1);
  }
  const replay=createCancellationLedger({query:async()=>({rows:[{outcome:'succeeded'}]})});
  assert.equal((await replay.execute({...op,cancel:()=>assert.fail('dispatch')})).replayed,true);
});
test('internal transport fixes destination, rejects unknown operations and hides credential-bearing errors',async()=>{
  let calls=0;
  const transport=createProviderTransport({env:{AUTHNET_API_LOGIN_ID:'synthetic-login',AUTHNET_TRANSACTION_KEY:'synthetic-key'},
    post:async(url,body,options)=>{calls++;assert.equal(url,'https://api2.authorize.net/xml/v1/request.api');
      assert.equal(options.maxRedirects,0);assert.equal(options.timeout,45000);throw Error('synthetic-key');}});
  await assert.rejects(transport('createTransactionRequest',{}));assert.equal(calls,0);
  await assert.rejects(transport('ARBCancelSubscriptionRequest',{subscriptionId:'700'}),e=>!e.message.includes('synthetic-key'));
  assert.equal(calls,1);
});
test('Google recovery adapter uses fixed OAuth endpoint without credentials, redirects or retry',()=>{
  const calls=[];
  const context={ScriptApp:{getOAuthToken:()=> 'synthetic-token'},UrlFetchApp:{fetch:(url,options)=>{
    calls.push({url,options});return {getResponseCode:()=>200,getContentText:()=>JSON.stringify({ok:true,subscriptionId:'1000'})};}}};
  vm.createContext(context);vm.runInContext(fs.readFileSync(path.join(__dirname,'../docs/google-payment-recovery-adapter.gs'),'utf8'),context);
  assert.equal(context.FF_recoverTerminationOnRender_(2,'700','800','20.00','2026-10-30').subscriptionId,'1000');
  assert.equal(calls[0].url,'https://fastfilings-api.onrender.com/google-payments/recover-terminated');
  assert.equal(calls[0].options.headers.Authorization,'Bearer synthetic-token');assert.equal(calls[0].options.followRedirects,false);
  context.UrlFetchApp.fetch=()=>{calls.push({});throw Error('lost');};
  assert.throws(()=>context.FF_recoverTerminationOnRender_(2,'700','800','20.00','2026-10-30'));
  assert.equal(calls.length,2);
});
