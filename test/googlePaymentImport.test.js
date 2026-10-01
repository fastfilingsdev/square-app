'use strict';
const test=require('node:test');const assert=require('node:assert/strict');const vm=require('node:vm');const fs=require('node:fs');const path=require('node:path');
const source=fs.readFileSync(path.join(__dirname,'../docs/google-payment-import-replacement.gs'),'utf8');
function fixture(respond){
  const calls=[],writes=[];const context={LOOKBACK_DAYS:30,Logger:{log:()=>{}},FF_paymentRead_:(op,parameters)=>{
    calls.push({op,parameters});return respond(op,parameters);},getBillingSheet_:clear=>{writes.push({clear});return {getRange:(...range)=>({setValues:values=>writes.push({range,values})})};}};
  vm.createContext(context);vm.runInContext(source,context);return {context,calls,writes};
}
test('import preserves columns and does not clear report until all reads succeed',()=>{
  const f=fixture(op=>op==='getSettledBatchListRequest'?{batchList:[{batchId:'1'}]}:{transactions:[{transId:'2',settleAmount:'20.00',customer:{email:'synthetic@example.test'},submitTimeUTC:'2026-09-30T12:00:00Z'}]});
  f.context.fetchAuthorizeNetTransactions();assert.equal(f.writes.length,2);assert.equal(f.writes[1].values[0].length,13);
  assert.equal(f.writes[1].values[0][1],'2');assert.equal(f.writes[1].values[0][4],'synthetic@example.test');
  assert.equal(f.calls[1].parameters.paging.offset,1);assert.equal(f.calls[1].parameters.paging.limit,1000);
});
test('missing response, failed page and duplicate transaction preserve old report',()=>{
  for(const response of [{},null,{transactions:[{transId:'2'},{transId:'2'}]}]){
    const f=fixture(op=>{if(op==='getSettledBatchListRequest')return {batchList:[{batchId:'1'}]};
      if(response===null)throw Error('network');return response;});
    assert.throws(()=>f.context.fetchAuthorizeNetTransactions());assert.equal(f.writes.length,0);
  }
});
test('full page is followed by next numbered page, not truncated to first 1000',()=>{
  const f=fixture((op,p)=>op==='getSettledBatchListRequest'?{batchList:[{batchId:'1'}]}:
    {transactions:p.paging.offset===1?Array.from({length:1000},(_,i)=>({transId:String(i+1)})):[{transId:'1001'}]});
  f.context.fetchAuthorizeNetTransactions();assert.equal(f.writes[1].values.length,1001);assert.equal(f.calls[2].parameters.paging.offset,2);
});
