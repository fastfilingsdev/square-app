const test = require('node:test');
const assert = require('node:assert/strict');
const { createPartialRefundLedger } = require('../src/core/partialRefundLedger');
const { executeLedgerRefund } = require('../src/core/refundLedger');
const op = {providerScope:'synthetic',transactionId:'123',requestId:'00000000-0000-4000-8000-000000000001',currency:'USD',amount:'0.29'};

test('partial claim sends exact minor units and stable request ID as parameters', async () => {
  let params;
  const ledger=createPartialRefundLedger({query:async (sql,values) => {
    assert.match(sql,/ff_claim_partial_refund/); params=values;
    return {rowCount:1,rows:[{attempt_id:values[5]}]};
  }});
  const claim=await ledger.claim(op);
  assert.deepEqual(params.slice(0,5),['synthetic','123',op.requestId,'29','USD']);
  assert.equal(claim.amount,'0.29');
  assert.equal(claim.requestId,op.requestId);
});

test('invalid caller identity, currency or money never reaches query', async () => {
  const ledger=createPartialRefundLedger({query:async()=>assert.fail('query')});
  for (const patch of [{requestId:''},{currency:'usd'},{transactionId:'0'},{amount:'1.001'},{amount:'0'},{providerScope:"x';--"}]) {
    await assert.rejects(ledger.claim({...op,...patch}),/Invalid/);
  }
});

test('claim rejection and response loss never permit provider dispatch', async () => {
  for (const query of [async()=>({rowCount:0,rows:[]}),async()=>{throw new Error('SECRET');},async()=>({rowCount:1,rows:[{attempt_id:'wrong'}]})]) {
    const result=await executeLedgerRefund({ledger:createPartialRefundLedger({query}),operation:op,
      refund:async()=>assert.fail('provider'),parseRefundId:x=>x});
    assert.equal(result.ok,false);
    assert.equal(result.automatic_retry_allowed,false);
    assert.equal(JSON.stringify(result).includes('SECRET'),false);
  }
});

test('finish is bound to exact owned claim and is single-use', async () => {
  let calls=0;
  const ledger=createPartialRefundLedger({query:async (sql,values) => {
    calls++;
    return {rowCount:1,rows:[{attempt_id:values[sql.includes('ff_claim')?5:4]}]};
  }});
  const claim=await ledger.claim(op);
  await assert.rejects(ledger.finish({...claim},'succeeded','456'),/Invalid/);
  await assert.rejects(ledger.finish(claim,'released'),/Invalid/);
  await ledger.finish(claim,'succeeded','456');
  await assert.rejects(ledger.finish(claim,'succeeded','456'),/Invalid/);
  assert.equal(calls,2);
});

test('lost completion acknowledgement returns known receipt without authorizing retry', async () => {
  const ledger=createPartialRefundLedger({query:async (sql,values) => {
    if(sql.includes('ff_finish')) throw new Error('SECRET');
    return {rowCount:1,rows:[{attempt_id:values[5]}]};
  }});
  const result=await executeLedgerRefund({ledger,operation:op,refund:async()=>'456',parseRefundId:x=>x});
  assert.equal(result.providerRefundId,'456');
  assert.equal(result.code,'REFUND_LEDGER_COMPLETION_UNCONFIRMED');
  assert.equal(result.automatic_retry_allowed,false);
});
