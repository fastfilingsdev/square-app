const test=require('node:test');
const assert=require('node:assert/strict');
const {createRefundServiceRuntime}=require('../src/core/refundServiceRuntime');
const enabled={FF_REFUND_HISTORY_ENABLED:'true',FF_REFUND_LEDGER_MODE:'partial',FF_REFUND_CURRENCY:'USD',
  FF_REFUND_LEDGER_DATABASE_URL:'synthetic-only',FF_REFUND_PROVIDER_SCOPE:'synthetic_sandbox',
  FF_REFUND_LINKED_POLICY_VERIFIED:'true',FF_REFUND_UNLINKED_CREDITS_EXCLUDED:'true',
  AUTHNET_API_LOGIN_ID:'synthetic',AUTHNET_TRANSACTION_KEY:'fixture-only',
  AUTHNET_API_URL:'https://apitest.authorize.net/xml/v1/request.api'};
const ledgerFactory=()=>({routerOptions:{providerScope:'synthetic_sandbox',refundLedger:{}},close:async()=>{}});
test('history remains disconnected by default and explicitly false',()=>{
  for(const env of [{},{FF_REFUND_HISTORY_ENABLED:'false'}]) {
    const runtime=createRefundServiceRuntime({env,ledgerFactory,historyFactory:()=>assert.fail('no history')});
    assert.equal(runtime.routerOptions.verifyRefundHistoryFn,undefined);
  }
});
test('verified server configuration wires history and preserves ledger close',async()=>{
  let config,closed=false;
  const runtime=createRefundServiceRuntime({env:{...enabled},historyFactory:c=>{config=c;return async op=>op;},
    ledgerFactory:()=>({...ledgerFactory(),close:async()=>{closed=true;}})});
  assert.deepEqual(config,{providerScope:'synthetic_sandbox',currency:'USD',linkedRefundPolicyVerified:true,unlinkedCreditsExcluded:true});
  assert.deepEqual(await runtime.routerOptions.verifyRefundHistoryFn({transactionId:'123'}),{transactionId:'123'});
  await runtime.close();assert.equal(closed,true);
});
test('missing or invalid attestations cannot open a database pool',()=>{
  for(const patch of [{FF_REFUND_HISTORY_ENABLED:'TRUE'},{FF_REFUND_LINKED_POLICY_VERIFIED:'false'},
    {FF_REFUND_UNLINKED_CREDITS_EXCLUDED:undefined},{FF_REFUND_LEDGER_MODE:'single'},
    {FF_REFUND_CURRENCY:'EUR'},{FF_REFUND_LEDGER_DATABASE_URL:''},{FF_REFUND_PROVIDER_SCOPE:''},
    {AUTHNET_API_LOGIN_ID:''},{AUTHNET_TRANSACTION_KEY:''},{AUTHNET_API_URL:'https://example.invalid'}]) {
    assert.throws(()=>createRefundServiceRuntime({env:{...enabled,...patch},
      ledgerFactory:()=>assert.fail('no pool'),historyFactory:()=>assert.fail('no history')}),/refund history|Refund history/);
  }
});
test('provider identity and policy drift blocks before history access without leaking secrets',async()=>{
  for(const key of ['AUTHNET_API_URL','AUTHNET_API_LOGIN_ID','AUTHNET_TRANSACTION_KEY','FF_REFUND_PROVIDER_SCOPE',
    'FF_REFUND_CURRENCY','FF_REFUND_LINKED_POLICY_VERIFIED','FF_REFUND_UNLINKED_CREDITS_EXCLUDED','FF_REFUND_HISTORY_ENABLED']) {
    const env={...enabled};let calls=0;
    const runtime=createRefundServiceRuntime({env,ledgerFactory,historyFactory:()=>async()=>{calls++;}});
    env[key]='SECRET-CHANGED';
    await assert.rejects(runtime.routerOptions.verifyRefundHistoryFn({}),e=>/configuration changed/.test(e.message)&&!e.message.includes('SECRET'));
    assert.equal(calls,0);
  }
});
test('configuration change during history collection discards the evidence',async()=>{
  const env={...enabled};
  const runtime=createRefundServiceRuntime({env,ledgerFactory,historyFactory:()=>async()=>{
    env.AUTHNET_API_LOGIN_ID='changed';return {complete:true};
  }});
  await assert.rejects(runtime.routerOptions.verifyRefundHistoryFn({}),/configuration changed/);
});
test('configuration drift while acquiring ledger claim cannot dispatch to a different provider account',async()=>{
  const {createGuardedRefundProcessor}=require('../src/features/billingRefunds/refundGuard');
  const env={...enabled};let calls=0;const finishes=[];
  const ledger={requiresRequestId:true,claim:async()=>{env.AUTHNET_API_LOGIN_ID='changed';return {};},
    finish:async(_claim,state)=>{finishes.push(state);}};
  const runtime=createRefundServiceRuntime({env,
    ledgerFactory:()=>({routerOptions:{refundLedger:ledger,providerScope:'synthetic_sandbox',refundCurrency:'USD'},close:async()=>{}}),
    historyFactory:()=>async op=>({...op,complete:true,checkedAtMs:Date.now(),originalAmount:'10.00',remainingAmount:'10.00',
      hasUncertainRefunds:false,linkedRefundPolicyVerified:true,unlinkedCreditsExcluded:true}),
    refundTransactionFn:async()=>{calls++;assert.fail('provider must not run');}});
  const run=createGuardedRefundProcessor({...runtime.routerOptions,ledger,
    processRefundFn:async args=>{try{await args.refundTransactionFn({refTransId:'123',amount:'1.00'});}catch{}return {ok:false};}});
  const result=await run({requestId:'00000000-0000-4000-8000-000000000001'});
  assert.equal(calls,0);assert.equal(result.requires_reconciliation,true);
  assert.deepEqual(finishes,['needs_reconciliation']);
});
test('unchanged configuration dispatches through the server-owned wrapper exactly once',async()=>{
  let calls=0;
  const runtime=createRefundServiceRuntime({env:{...enabled},ledgerFactory,historyFactory:()=>async()=>({}),
    refundTransactionFn:async request=>{calls++;return {id:request.refTransId};}});
  assert.deepEqual(await runtime.routerOptions.refundTransactionFn({refTransId:'123'}),{id:'123'});
  assert.equal(calls,1);
});
