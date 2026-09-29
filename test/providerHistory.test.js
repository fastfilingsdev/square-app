const test=require('node:test');
const assert=require('node:assert/strict');
const {readRefundProviderHistory,createRefundHistoryVerifier}=require('../src/features/billingRefunds/providerHistory');
const {validateRefundProviderEvidence}=require('../src/core/refundProviderEvidence');
const {createGuardedRefundProcessor}=require('../src/features/billingRefunds/refundGuard');
const NOW=Date.parse('2026-09-28T10:00:00Z');
const operation={providerScope:'synthetic',transactionId:'100',currency:'USD',amount:'10.00'};
const success = payload=>({messages:{resultCode:'Ok',message:[{code:'I00001'}]},...payload});
function fixture({pending=true}={}) {
  const original={transId:'100',transactionType:'authCaptureTransaction',transactionStatus:'settledSuccessfully',
    settleAmount:'100.00',submitTimeUTC:'2026-07-01T10:00:00'};
  const details={100:original,
    101:{transId:'101',transactionType:'refundTransaction',transactionStatus:'refundSettledSuccessfully',refTransId:'100',settleAmount:'20.00'},
    102:{transId:'102',transactionType:'refundTransaction',transactionStatus:'refundPendingSettlement',refTransId:'100',authAmount:'10.00'},
    103:{transId:'103',transactionType:'authCaptureTransaction',transactionStatus:'settledSuccessfully',settleAmount:'50.00'},
    104:{transId:'104',transactionType:'refundTransaction',transactionStatus:'refundSettledSuccessfully',refTransId:'999',settleAmount:'90.00'}};
  const batch={batchId:'500',settlementTimeUTC:'2026-08-10T10:00:00Z',settlementState:'settledSuccessfully'};
  const settled=['100','101','103','104'];
  const pendingIds=pending?['102']:[];
  const calls=[];
  function page(ids,{offset,limit}) {return success({totalNumInResultSet:ids.length,transactions:ids.slice((offset-1)*limit,offset*limit).map(transId=>({transId}))});}
  const provider={
    getTransactionDetails:async id=>{calls.push(['detail',id]);return success({transaction:{...details[id]}});},
    getSettledBatchList:async range=>{calls.push(['batches',range]);const stamp=Date.parse(batch.settlementTimeUTC);
      return success({batchList:stamp>=Date.parse(range.firstSettlementDate)&&stamp<=Date.parse(range.lastSettlementDate)?[{...batch}]:[]});},
    getTransactionListForBatch:async (id,paging)=>{calls.push(['settled',id,paging]);return page(settled,paging);},
    getUnsettledTransactionList:async paging=>{calls.push(['pending',paging]);return page(pendingIds,paging);}
  };
  return {provider,details,batch,settled,pendingIds,calls,page,
    run:options=>readRefundProviderHistory(operation,{provider,now:()=>NOW,pageSize:2,...options})};
}

test('all settled pages and pending refunds are counted exactly, unrelated originals excluded',async()=>{
  const f=fixture();const result=await f.run();
  assert.equal(result.remainingAmount,'70.00');assert.equal(result.hasUncertainRefunds,true);
  assert.equal(result.refunds.length,2);assert.equal(result.complete,true);
  assert.deepEqual(f.calls.filter(x=>x[0]==='settled').map(x=>x[2].offset),[1,2]);
  for(const [,range] of f.calls.filter(x=>x[0]==='batches')) assert.ok(Date.parse(range.lastSettlementDate)-Date.parse(range.firstSettlementDate)<=30*86400000);
  assert.ok(f.calls.filter(x=>x[0]==='batches').length>=6);
});
test('confirmed voids do not consume balance, unknown linked statuses demand reconciliation',async()=>{
  const f=fixture();f.details[102].transactionStatus='voided';
  assert.equal((await f.run()).remainingAmount,'80.00');
  f.details[102].transactionStatus='unrecognizedProviderState';
  assert.equal((await f.run()).hasUncertainRefunds,true);
});
test('provider errors never return partial history or expose error secrets',async()=>{
  for(const method of ['getTransactionDetails','getSettledBatchList','getTransactionListForBatch','getUnsettledTransactionList']) {
    const f=fixture();f.provider[method]=async()=>{throw new Error('SECRET token');};
    await assert.rejects(f.run(),err=>/incomplete/.test(err.message)&&!err.message.includes('SECRET'));
  }
});
test('changed counts, duplicate pages, missing totals and truncated pages fail closed',async()=>{
  for(const mode of ['changed','duplicate','missing','truncated']) {
    const f=fixture();
    f.provider.getTransactionListForBatch=async (_id,p)=>{
      const result=f.page(f.settled,p);
      if(mode==='changed'&&p.offset===2) result.totalNumInResultSet++;
      if(mode==='duplicate'&&p.offset===2) result.transactions=f.page(f.settled,{...p,offset:1}).transactions;
      if(mode==='missing') delete result.totalNumInResultSet;
      if(mode==='truncated') result.transactions.pop();
      return result;
    };
    await assert.rejects(f.run(),/incomplete/);
  }
});
test('work and history limits reject instead of silently truncating',async()=>{
  for(const options of [{maxPages:1},{maxCalls:2},{maxHistoryDays:30}]) await assert.rejects(fixture().run(options),/incomplete/);
  const f=fixture();let tick=NOW;
  await assert.rejects(f.run({now:()=>tick+=1000,maxDurationMs:100}),/incomplete/);
});
test('pending inventory drift and settlement movement are detected',async()=>{
  const f=fixture();let calls=0;
  f.provider.getUnsettledTransactionList=async p=>f.page(++calls===1?['102']:[],p);
  await assert.rejects(f.run(),/incomplete/);
  const overlap=fixture();overlap.pendingIds.push('101');
  await assert.rejects(overlap.run(),/incomplete/);
});
test('newly settled batch after collection invalidates the snapshot',async()=>{
  const f=fixture();const get=f.provider.getSettledBatchList;let windows=0;
  f.provider.getSettledBatchList=async range=>{
    const data=await get(range);
    if(++windows===6)data.batchList.push({batchId:'501',settlementState:'settledSuccessfully',settlementTimeUTC:'2026-09-20T10:00:00Z'});
    return data;
  };
  await assert.rejects(f.run(),/incomplete/);
});
test('exact provider IDs and original settled amount are mandatory',async()=>{
  for(const patch of [{transId:'999'},{settleAmount:'100oops'},{transactionStatus:'unsettled'},{currencyCode:'EUR'},{submitTimeUTC:'not-a-date'}]) {
    const f=fixture();Object.assign(f.details[100],patch);await assert.rejects(f.run(),/incomplete/);
  }
});
test('unlinked credits, over-refunds and invalid fractional cents cannot form approval evidence',async()=>{
  for(const patch of [{refTransId:'0'},{refTransId:undefined},{settleAmount:'101.00'},{settleAmount:'20.001'}]) {
    const f=fixture();Object.assign(f.details[101],patch);await assert.rejects(f.run(),/incomplete/);
  }
});
test('explicit empty-result response is supported, but malformed absence is not assumed empty',async()=>{
  const f=fixture({pending:false});
  f.provider.getUnsettledTransactionList=async()=>({messages:{resultCode:'Ok',message:[{code:'I00004'}]}});
  assert.equal((await f.run()).remainingAmount,'80.00');
  f.provider.getUnsettledTransactionList=async()=>success({});
  await assert.rejects(f.run(),/incomplete/);
});
test('trusted factory validates account policy and produces guard-compatible evidence',async()=>{
  assert.throws(()=>createRefundHistoryVerifier({}),/incomplete/);
  const f=fixture({pending:false});
  const verify=createRefundHistoryVerifier({providerScope:'synthetic',currency:'USD',linkedRefundPolicyVerified:true,
    unlinkedCreditsExcluded:true,provider:f.provider,now:()=>NOW,pageSize:2});
  const evidence=await verify(operation);
  assert.doesNotThrow(()=>validateRefundProviderEvidence(operation,evidence,NOW));
  await assert.rejects(verify({...operation,providerScope:'other'}),/incomplete/);
});
test('actual guard refuses pending refund evidence before ledger claim or provider dispatch',async()=>{
  const f=fixture();
  const verify=createRefundHistoryVerifier({providerScope:'synthetic',currency:'USD',linkedRefundPolicyVerified:true,
    unlinkedCreditsExcluded:true,provider:f.provider,now:()=>NOW,pageSize:2});
  const process=createGuardedRefundProcessor({providerScope:'synthetic',refundCurrency:'USD',verifyRefundHistoryFn:async op=>({...await verify(op),checkedAtMs:Date.now()}),
    ledger:{requiresRequestId:true,claim:async()=>assert.fail('claim'),finish:async()=>assert.fail('finish')},
    refundTransactionFn:async()=>assert.fail('provider'),processRefundFn:async args=>{
      try{await args.refundTransactionFn({refTransId:'100',amount:'10.00'});return {ok:true};}catch{return {ok:false};}
    }});
  const result=await process({requestId:'00000000-0000-4000-8000-000000000001'});
  assert.equal(result.code,'REFUND_PROVIDER_HISTORY_UNVERIFIED');
});

test('only positive settled-charge summaries avoid redundant details',async()=>{
  const f=fixture();const get=f.provider.getTransactionListForBatch;
  f.provider.getTransactionListForBatch=async(b,p)=>{
    const result=await get(b,p);
    result.transactions=result.transactions.map(r=>r.transId==='103'?{...r,transactionStatus:'settledSuccessfully',settleAmount:'50.00'}:r);
    return result;
  };
  const result=await f.run();
  assert.equal(result.remainingAmount,'70.00');
  assert.equal(f.calls.some(x=>x[0]==='detail'&&x[1]==='103'),false);
  for(const id of ['100','101','102','104']) assert.ok(f.calls.some(x=>x[0]==='detail'&&x[1]===id));
});
test('ambiguous, malformed or credit-hinted summaries still require details',async()=>{
  for(const patch of [{transactionStatus:undefined},{transactionStatus:'SETTLEDSUCCESSFULLY'},
    {transactionStatus:'refundSettledSuccessfully'},{settleAmount:undefined},{settleAmount:'0.00'},
    {settleAmount:'-1.00'},{settleAmount:'1.001'},{transactionType:'refundTransaction'},
    {transactionType:'unknown'},{refTransId:'100'},{refTransID:'100'}]) {
    const f=fixture();const get=f.provider.getTransactionListForBatch;
    f.provider.getTransactionListForBatch=async(b,p)=>{
      const result=await get(b,p);
      result.transactions=result.transactions.map(r=>r.transId==='103'?{...r,transactionStatus:'settledSuccessfully',settleAmount:'50.00',...patch}:r);
      return result;
    };
    await f.run();assert.ok(f.calls.some(x=>x[0]==='detail'&&x[1]==='103'));
  }
});
test('pending rows never use settled-summary fast path',async()=>{
  const f=fixture();
  f.provider.getUnsettledTransactionList=async()=>success({totalNumInResultSet:1,
    transactions:[{transId:'102',transactionStatus:'settledSuccessfully',settleAmount:'10.00'}]});
  const result=await f.run();
  assert.equal(result.hasUncertainRefunds,true);
  assert.ok(f.calls.some(x=>x[0]==='detail'&&x[1]==='102'));
});
test('explicit historical declined and voided summaries cannot consume refundable capacity',async()=>{
  for(const status of ['declined','voided'])for(const amount of ['0.00','50.00']) {
    const f=fixture();const get=f.provider.getTransactionListForBatch;
    f.provider.getTransactionListForBatch=async(b,p)=>{
      const d=await get(b,p);d.transactions=d.transactions.map(r=>r.transId==='103'?{...r,transactionStatus:status,settleAmount:amount}:r);return d;
    };
    assert.equal((await f.run()).remainingAmount,'70.00');
    assert.equal(f.calls.some(x=>x[0]==='detail'&&x[1]==='103'),false);
  }
});
test('errors, review states and malformed or credit-hinted terminal summaries retain detail checks',async()=>{
  for(const patch of [{transactionStatus:'generalError'},{transactionStatus:'communicationError'},
    {transactionStatus:'couldNotVoid'},{transactionStatus:'expired'},{transactionStatus:'failedReview'},
    {transactionStatus:'Declined'},{settleAmount:undefined},{settleAmount:'-1.00'},
    {transactionType:'refundTransaction'},{transactionType:'unknown'},{refTransId:'100'}]) {
    const f=fixture();const get=f.provider.getTransactionListForBatch;
    f.provider.getTransactionListForBatch=async(b,p)=>{
      const d=await get(b,p);d.transactions=d.transactions.map(r=>r.transId==='103'?{...r,transactionStatus:'declined',settleAmount:'50.00',...patch}:r);return d;
    };
    await f.run();assert.ok(f.calls.some(x=>x[0]==='detail'&&x[1]==='103'));
  }
});
test('read concurrency is bounded and invalid configurations fail before reads',async()=>{
  for(const concurrency of [0,5,1.5,NaN]) {
    const f=fixture();await assert.rejects(f.run({concurrency}),/incomplete/);assert.equal(f.calls.length,0);
  }
  const f=fixture();let active=0,peak=0;
  for(const key of Object.keys(f.provider)) {
    const read=f.provider[key];f.provider[key]=async(...args)=>{
      peak=Math.max(peak,++active);await new Promise(resolve=>setImmediate(resolve));
      try{return await read(...args);}finally{active--;}
    };
  }
  assert.equal((await f.run()).remainingAmount,'70.00');
  assert.ok(peak>1&&peak<=4);assert.equal(active,0);
});
test('failed parallel read stops new work and drains in-flight requests before rejection',async()=>{
  const f=fixture({pending:false});let active=0,finished=0,started=0;
  const get=f.provider.getTransactionDetails;
  f.provider.getTransactionDetails=async id=>{
    if(id==='100')return get(id);
    started++;active++;
    await new Promise(resolve=>setImmediate(resolve));
    active--;finished++;
    if(id==='101')throw Error('SECRET');
    return get(id);
  };
  await assert.rejects(f.run(),err=>/incomplete/.test(err.message)&&!err.message.includes('SECRET'));
  assert.equal(active,0);assert.equal(started,finished);
  const finishedAtReturn=finished;await new Promise(resolve=>setImmediate(resolve));assert.equal(finished,finishedAtReturn);
});
test('merchant-scale complete scan stays fresh without truncating pages or credit evidence',async()=>{
  let clock=NOW,queue=[],scheduled=false,peak=0,detailCount=0,batchPages=0;
  function read(payload){return new Promise(resolve=>{
    queue.push(()=>resolve(success(payload)));peak=Math.max(peak,queue.length);
    if(!scheduled){scheduled=true;setImmediate(()=>{const wave=queue;queue=[];scheduled=false;clock+=180;wave.forEach(done=>done());});}
  });}
  const from=NOW-116*86400000;
  const batches=Array.from({length:116},(_,i)=>({batchId:String(500+i),
    settlementTimeUTC:new Date(from+(i+1)*86400000-1000).toISOString(),settlementState:'settledSuccessfully'}));
  const provider={
    getTransactionDetails:id=>{
      detailCount++;
      if(id==='100')return read({transaction:{transId:id,transactionType:'authCaptureTransaction',
        transactionStatus:'settledSuccessfully',settleAmount:'20.00',submitTimeUTC:new Date(from).toISOString()}});
      assert.equal(id,'101');return read({transaction:{transId:id,transactionType:'refundTransaction',
        transactionStatus:'refundSettledSuccessfully',refTransId:'100',settleAmount:'20.00'}});
    },
    getSettledBatchList:range=>read({batchList:batches.filter(b=>Date.parse(b.settlementTimeUTC)>=Date.parse(range.firstSettlementDate)&&Date.parse(b.settlementTimeUTC)<=Date.parse(range.lastSettlementDate))}),
    getTransactionListForBatch:(b,p)=>{
      batchPages++;assert.equal(p.offset,1);
      const transactions=Array.from({length:100},(_,i)=>({transId:String(Number(b)*1000+i),
        transactionStatus:i<75?'settledSuccessfully':i<95?'declined':'voided',settleAmount:'20.00'}));
      if(b==='500')transactions.push({transId:'101',transactionStatus:'refundSettledSuccessfully',settleAmount:'20.00'});
      return read({totalNumInResultSet:transactions.length,transactions});
    },
    getUnsettledTransactionList:()=>read({totalNumInResultSet:0,transactions:[]})
  };
  const evidence=await readRefundProviderHistory(operation,{provider,now:()=>clock});
  assert.equal(evidence.complete,true);assert.equal(evidence.remainingAmount,'0.00');
  assert.equal(evidence.refunds.length,1);assert.equal(batchPages,116);assert.equal(detailCount,3);
  assert.ok(clock-NOW<10000);assert.ok(peak<=4);assert.equal(evidence.checkedAtMs,NOW);
  assert.throws(()=>validateRefundProviderEvidence(operation,{...evidence,linkedRefundPolicyVerified:true,unlinkedCreditsExcluded:true},clock),/exceeds verified provider balance/);
});
