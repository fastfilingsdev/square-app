const test=require('node:test');
const assert=require('node:assert/strict');
const {validateRefundProviderEvidence}=require('../src/core/refundProviderEvidence');
const {createGuardedRefundProcessor}=require('../src/features/billingRefunds/refundGuard');
const {buildRefundTransactionRequest}=require('../src/connectors/authnet/client');
const operation={providerScope:'synthetic',transactionId:'123',currency:'USD',amount:'10.00'};
const evidence={...operation,complete:true,linkedRefundPolicyVerified:true,unlinkedCreditsExcluded:true,
  hasUncertainRefunds:false,checkedAtMs:100000,originalAmount:'100.00',remainingAmount:'10.00'};

test('provider remaining balance includes externally consumed capacity and permits exact remainder',()=>{
  assert.doesNotThrow(()=>validateRefundProviderEvidence(operation,evidence,100000));
  assert.throws(()=>validateRefundProviderEvidence({...operation,amount:'10.01'},evidence,100000),/balance/);
});
test('incomplete, stale, uncertain, mismatched or unlinked provider evidence fails closed',()=>{
  for(const patch of [{complete:false},{linkedRefundPolicyVerified:false},{unlinkedCreditsExcluded:false},
    {hasUncertainRefunds:true},{checkedAtMs:69999},{checkedAtMs:100001},{providerScope:'other'},
    {transactionId:'456'},{currency:'EUR'},{remainingAmount:'100.01'},{remainingAmount:'10.001'}]) {
    assert.throws(()=>validateRefundProviderEvidence(operation,{...evidence,...patch},100000));
  }
});
test('request body cannot inject provider-history verifier and no ledger/provider call occurs without one',async()=>{
  const process=createGuardedRefundProcessor({providerScope:'synthetic',refundCurrency:'USD',
    ledger:{requiresRequestId:true,claim:async()=>assert.fail('claim'),finish:async()=>assert.fail('finish')},
    refundTransactionFn:async()=>assert.fail('provider'),
    processRefundFn:async args=>{try{await args.refundTransactionFn({refTransId:'123',amount:'10.00'});return {ok:true};}catch{return {ok:false};}}});
  const result=await process({requestId:'00000000-0000-4000-8000-000000000001',verifyRefundHistoryFn:async()=>evidence});
  assert.equal(result.code,'REFUND_PROVIDER_HISTORY_UNVERIFIED');
  assert.equal(result.requires_reconciliation,true);
});
test('provider connector rejects unlinked or malformed original transaction IDs',()=>{
  for(const refTransId of ['', '0', 123, ' 123', 'abc', '123;456']) {
    assert.throws(()=>buildRefundTransactionRequest({refTransId,amount:'1.00',cardLast4:'1111'},{}),/linked original/);
  }
});
test('provider connector does not round fractional cents into a different refund',()=>{
  for(const amount of ['1.001','1e2','0','-1',NaN]) {
    assert.throws(()=>buildRefundTransactionRequest({refTransId:'123',amount,cardLast4:'1111'},{}));
  }
  const request=buildRefundTransactionRequest({refTransId:'123',amount:'0.29',cardLast4:'1111'},{});
  assert.equal(request.createTransactionRequest.transactionRequest.amount,'0.29');
  assert.equal(request.createTransactionRequest.transactionRequest.refTransId,'123');
});
