const test=require('node:test');
const assert=require('node:assert/strict');
const {EventEmitter}=require('node:events');
const {createRefundLedgerRuntime}=require('../src/core/refundLedgerRuntime');
const {createGuardedRefundProcessor}=require('../src/features/billingRefunds/refundGuard');
const id='00000000-0000-4000-8000-000000000001';

test('partial mode requires explicit supported server currency before pool construction',()=>{
  for(const patch of [{FF_REFUND_LEDGER_MODE:'other'},{FF_REFUND_LEDGER_MODE:'partial'},
    {FF_REFUND_LEDGER_MODE:'partial',FF_REFUND_CURRENCY:'JPY'}]) {
    assert.throws(()=>createRefundLedgerRuntime({env:{FF_REFUND_LEDGER_DATABASE_URL:'postgres://fixture:fixture@db.example.invalid/test',
      FF_REFUND_PROVIDER_SCOPE:'synthetic',...patch},PoolClass:class{constructor(){assert.fail('pool');}}}),/mode or currency/);
  }
});

test('runtime partial mode uses checked durable query adapter and server scope/currency',async()=>{
  let released=false;
  class Pool extends EventEmitter {
    async connect(){return {getTransactionStatus:()=> 'I',release:()=>{released=true;},query:async(sql,args)=>{
      if(sql.includes('pg_is_in_recovery'))return {rows:[{recovery:false,synchronous_commit:'on',read_only:'off',fsync:'on',original_role:true,restricted_role:true}]};
      assert.match(sql,/ff_claim_partial_refund/);
      assert.equal(args[3],'29');
      return {rowCount:1,rows:[{attempt_id:args[5]}]};
    }};}
    async end(){}
  }
  const r=createRefundLedgerRuntime({env:{FF_REFUND_LEDGER_DATABASE_URL:'postgres://fixture:fixture@db.example.invalid/test',
    FF_REFUND_PROVIDER_SCOPE:'synthetic',FF_REFUND_LEDGER_MODE:'partial',FF_REFUND_CURRENCY:'USD'},PoolClass:Pool});
  assert.equal(r.routerOptions.refundCurrency,'USD');
  const claim=await r.routerOptions.refundLedger.claim({providerScope:'synthetic',transactionId:'123',requestId:id,currency:'USD',amount:'0.29'});
  assert.equal(claim.requestId,id);assert.equal(released,true);await r.close();
});

function processor({claim=async()=>({}),provider=async()=>({transactionResponse:{responseCode:'1',transId:'456'}}),preflight}={}) {
  return createGuardedRefundProcessor({providerScope:'synthetic',refundCurrency:'USD',
    verifyRefundHistoryFn:async op=>({...op,complete:true,linkedRefundPolicyVerified:true,
      unlinkedCreditsExcluded:true,hasUncertainRefunds:false,checkedAtMs:Date.now(),originalAmount:'100.00',remainingAmount:'100.00'}),
    ledger:{requiresRequestId:true,claim,finish:async()=>{}},refundTransactionFn:provider,
    processRefundFn:preflight || (async args=>{
      assert.equal(args.refundRequestId,args.requestId);
      try {await args.refundTransactionFn({refTransId:'123',amount:'10.00'});return {ok:true,status:'REFUNDED'};}
      catch{return {ok:false};}
    })});
}
test('missing stable request ID blocks before preflight or provider',async()=>{
  const run=processor({preflight:async()=>assert.fail('preflight')});
  assert.equal((await run({})).code,'REFUND_REQUEST_ID_REQUIRED');
  assert.equal((await run({requestId:'row-5'})).code,'REFUND_REQUEST_ID_REQUIRED');
});
test('guard propagates stable identity while ignoring request-supplied account and currency',async()=>{
  let operation;
  const run=processor({claim:async op=>{operation=op;return {};}});
  const result=await run({requestId:id,providerScope:'forged',currency:'EUR',refundRequestId:'forged'});
  assert.equal(result.requestId,id);assert.equal(result.ok,true);
  assert.deepEqual(operation,{providerScope:'synthetic',transactionId:'123',amount:'10.00',requestId:id,currency:'USD'});
});
test('unknown provider outcome carries matching original payment and request identity',async()=>{
  const run=processor({provider:async()=>{throw new Error('timeout');}});
  const result=await run({requestId:id});
  assert.equal(result.requestId,id);assert.equal(result.originalTransactionId,'123');
  assert.equal(result.refundAmount,'10.00');assert.equal(result.requires_reconciliation,true);
});
