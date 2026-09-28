const client = require('../../connectors/authnet/client');
const {minorUnits,decimalAmount} = require('../../core/refundBudget');
const DAY = 86400000;
const id = value => typeof value === 'string' && /^[1-9][0-9]{0,29}$/.test(value);
const fail = () => { throw new Error('Refund provider history incomplete or changed; reconciliation required'); };
function timestamp(value) {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?$/.test(value)) fail();
  const ms = Date.parse(value.endsWith('Z') ? value : value+'Z');
  if (!Number.isFinite(ms)) fail();
  return ms;
}
function ok(data) { if (data?.messages?.resultCode !== 'Ok') fail(); }
function rows(data,key) {
  ok(data);
  if (Array.isArray(data[key])) return data[key];
  const messages = data.messages.message;
  if (data[key] == null && Array.isArray(messages) && messages.some(x=>x.code==='I00004')) return [];
  fail();
}
function count(data, items) {
  const value=data.totalNumInResultSet;
  if (value == null && items.length === 0 && data.messages.message?.some(x=>x.code==='I00004')) return 0;
  if (!/^(0|[1-9][0-9]*)$/.test(String(value))) fail();
  const n=Number(value); if (!Number.isSafeInteger(n)) fail(); return n;
}
function fingerprint(items) { return items.map(x=>x.transId).sort().join(','); }

// Read-only full-coverage collector. No invoice/email heuristics, truncation,
// retries, credential logging, or writes. Work limits FAIL rather than returning
// an incomplete balance. Inventory rechecks detect settlement/page movement;
// this is not an atomic provider snapshot or a lock against manual refunds.
async function readRefundProviderHistory(operation, {
  provider=client, now=Date.now, pageSize=1000, maxPages=1000, maxCalls=10000,
  maxDurationMs=25000, maxHistoryDays=366
} = {}) {
  try {
    if (!id(operation?.transactionId) || operation.currency !== 'USD') fail();
    for(const n of [pageSize,maxPages,maxCalls,maxDurationMs,maxHistoryDays]) if(!Number.isSafeInteger(n)||n<=0) fail();
    if(pageSize>1000 || maxPages>100000 || maxHistoryDays>366) fail();
    const started=now(); if(!Number.isSafeInteger(started)) fail();
    let calls=0;
    async function call(fn,...args) {
      if(typeof fn!=='function' || ++calls>maxCalls || now()-started>maxDurationMs) fail();
      const result=await fn(...args); ok(result);
      if(now()-started>maxDurationMs) fail();
      return result;
    }
    async function detail(txId) {
      const response=await call(provider.getTransactionDetails,txId);
      if(response.transaction?.transId!==txId) fail();
      return response.transaction;
    }
    function originalValues(tx) {
      if(tx.transactionStatus!=='settledSuccessfully' ||
          !['authCaptureTransaction','priorAuthCaptureTransaction','captureOnlyTransaction'].includes(tx.transactionType)) fail();
      if(tx.currencyCode && tx.currencyCode!==operation.currency) fail();
      const amount=minorUnits(tx.settleAmount);
      const from=timestamp(tx.submitTimeUTC);
      if(amount<=0n || from>started || started-from>maxHistoryDays*DAY) fail();
      return {amount,from};
    }
    const original=await detail(operation.transactionId);
    const initial=originalValues(original);
    async function inventory(end) {
      const batches=new Map();
      for(let start=initial.from;start<=end;) {
        const until=Math.min(start+30*DAY,end);
        const result=await call(provider.getSettledBatchList,{includeStatistics:false,
          firstSettlementDate:new Date(start).toISOString(),lastSettlementDate:new Date(until).toISOString()});
        for(const batch of rows(result,'batchList')) {
          if(!id(batch.batchId) || batch.settlementState!=='settledSuccessfully') fail();
          const stamp=timestamp(batch.settlementTimeUTC);
          if(stamp<start || stamp>until) fail();
          if(batches.has(batch.batchId) && batches.get(batch.batchId)!==stamp) fail();
          batches.set(batch.batchId,stamp);
        }
        if(until===end) break;
        start=until; // overlap boundary, deduplicated by provider batch ID
      }
      return batches;
    }
    async function pages(fetch) {
      const collected=new Map(); let total;
      // Authorize.Net offset is a PAGE NUMBER, not a row offset.
      for(let offset=1;offset<=maxPages;offset++) {
        const data=await call(fetch,{limit:pageSize,offset});
        const entries=rows(data,'transactions'); const expected=count(data,entries);
        if(total==null) total=expected;
        if(total!==expected || entries.length>pageSize) fail();
        for(const item of entries) {
          if(!id(item.transId) || collected.has(item.transId)) fail();
          collected.set(item.transId,item);
        }
        if(collected.size===total) return [...collected.values()];
        if(collected.size>total || entries.length<pageSize) fail();
      }
      fail();
    }
    const pendingBefore=await pages(provider.getUnsettledTransactionList);
    const batches=await inventory(started);
    const transactions=new Map();
    for(const batchId of batches.keys()) {
      for(const row of await pages(paging=>provider.getTransactionListForBatch(batchId,paging))) {
        if(transactions.has(row.transId)) fail();
        transactions.set(row.transId,row);
      }
    }
    for(const row of pendingBefore) {
      if(transactions.has(row.transId)) fail(); // settlement moved during scan
      transactions.set(row.transId,row);
    }
    const refunds=[]; let used=0n; let uncertain=false;
    for(const txId of transactions.keys()) {
      const tx=await detail(txId);
      if(tx.transactionType!=='refundTransaction') {
        if(!['authCaptureTransaction','authOnlyTransaction','priorAuthCaptureTransaction','captureOnlyTransaction','voidTransaction'].includes(tx.transactionType) ||
            /refund|credit/i.test(tx.transactionStatus||'')) fail();
        continue;
      }
      const ref=tx.refTransId ?? tx.refTransID;
      if(!id(ref)) fail(); // unlinked credit cannot be assigned safely
      if(ref!==operation.transactionId) continue;
      if(tx.transactionStatus==='voided') continue; // confirmed void retains no capacity
      if(tx.currencyCode && tx.currencyCode!==operation.currency) fail();
      const amount=minorUnits(tx.settleAmount ?? tx.authAmount);
      if(amount<=0n) fail();
      const succeeded=tx.transactionStatus==='refundSettledSuccessfully';
      if(!succeeded) uncertain=true; // pending, failed or unfamiliar states require review
      used+=amount;
      refunds.push({providerRefundId:txId,amount:decimalAmount(amount),state:succeeded?'succeeded':'needs_reconciliation'});
    }
    const pendingAfter=await pages(provider.getUnsettledTransactionList);
    if(fingerprint(pendingBefore)!==fingerprint(pendingAfter)) fail();
    const batchesAfter=await inventory(now());
    if(batches.size!==batchesAfter.size || [...batches].some(([key,value])=>batchesAfter.get(key)!==value)) fail();
    const final=originalValues(await detail(operation.transactionId));
    if(final.amount!==initial.amount || final.from!==initial.from || used>initial.amount) fail();
    return {providerScope:operation.providerScope,transactionId:operation.transactionId,currency:operation.currency,
      complete:true,checkedAtMs:started,originalAmount:decimalAmount(initial.amount),
      remainingAmount:decimalAmount(initial.amount-used),hasUncertainRefunds:uncertain,refunds};
  } catch { fail(); } // never return partial history or secret-bearing provider errors
}

function createRefundHistoryVerifier({providerScope,currency,linkedRefundPolicyVerified,unlinkedCreditsExcluded,...deps}={}) {
  if(typeof providerScope!=='string' || !/^[a-zA-Z0-9_-]{1,80}$/.test(providerScope) || currency!=='USD' ||
      linkedRefundPolicyVerified!==true || unlinkedCreditsExcluded!==true) fail();
  return async operation=>{
    if(operation.providerScope!==providerScope || operation.currency!==currency) fail();
    return {...await readRefundProviderHistory(operation,deps),linkedRefundPolicyVerified:true,unlinkedCreditsExcluded:true};
  };
}
module.exports={readRefundProviderHistory,createRefundHistoryVerifier};
