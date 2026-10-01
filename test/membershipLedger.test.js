'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const {createMembershipLedger,membershipKey}=require('../src/core/membershipLedger');
const op={providerScope:'synthetic',transactionId:'101',fingerprint:'a'.repeat(64),customerEmail:'test@example.test'};
test('membership claim binds normalized customer, original charge, previous subscription and fingerprint before provider',async()=>{
  const sequence=[];
  const ledger=createMembershipLedger({query:async(sql,args)=>{
    assert.equal(args[2],membershipKey(' TEST@example.test '));
    assert.equal(args[4],null);
    sequence.push(sql.includes('ff_claim_')?'claim':'save');
    return {rows:[sql.includes('ff_claim_')?{outcome:'claimed'}:{saved:true}]};
  }});
  assert.deepEqual(await ledger.execute({...op,create:async()=>{sequence.push('provider');return {subscriptionId:'999'};}}),
    {subscriptionId:'999',replayed:false});
  assert.deepEqual(sequence,['claim','provider','save']);
});
test('invalid identity never queries or dispatches',async()=>{
  const ledger=createMembershipLedger({query:()=>assert.fail('query')});
  for(const change of [{customerEmail:''},{customerEmail:'bad'},{previousSubscriptionId:'null'},
    {transactionId:'0'},{providerScope:''},{fingerprint:'short'}]){
    await assert.rejects(ledger.execute({...op,...change,create:()=>assert.fail('dispatch')}));
  }
});
test('membership held, ambiguous and lost claim replies cannot dispatch',async()=>{
  for(const response of [{rows:[]},{rows:[{outcome:'held'}]},{rows:[{outcome:'succeeded',subscription_id:'bad'}]},
    {rows:[{outcome:'claimed'},{outcome:'claimed'}]}]){
    await assert.rejects(createMembershipLedger({query:async()=>response}).execute({...op,create:()=>assert.fail('dispatch')}));
  }
  await assert.rejects(createMembershipLedger({query:async()=>{throw Error('lost');}}).execute({...op,create:()=>assert.fail('dispatch')}));
});
test('receipt replay does not dispatch or finish again',async()=>{
  let queries=0;
  const ledger=createMembershipLedger({query:async()=>{queries++;return {rows:[{outcome:'succeeded',subscription_id:'999'}]};}});
  assert.equal((await ledger.execute({...op,create:()=>assert.fail('dispatch')})).replayed,true);
  assert.equal(queries,1);
});
test('provider and finish uncertainty never retries',async()=>{
  for(const kind of ['provider','receipt','finish']){
    let calls=0,queries=0;
    const ledger=createMembershipLedger({query:async()=>{queries++;return {rows:[queries===1?{outcome:'claimed'}:{saved:false}]};}});
    await assert.rejects(ledger.execute({...op,create:async()=>{
      calls++;if(kind==='provider')throw Error('timeout');return {subscriptionId:kind==='receipt'?'':'999'};
    }}));
    assert.equal(calls,1);assert.equal(queries,kind==='finish'?2:1);
  }
});
