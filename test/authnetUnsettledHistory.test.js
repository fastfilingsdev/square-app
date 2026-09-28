const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const {createRequire}=require('node:module');
const file=require.resolve('../src/connectors/authnet/client');
const nativeRequire=createRequire(file);
function connector() {
  const sent=[];
  const sandbox={module:{exports:{}},exports:{},process:{env:{}},Buffer,console,
    require:name=>name==='axios'?{post:async(url,payload)=>{sent.push({url,payload});return {data:{messages:{resultCode:'Ok'},transactions:[],totalNumInResultSet:0}};}}:nativeRequire(name)};
  vm.runInNewContext(fs.readFileSync(file,'utf8'),sandbox,{filename:file});
  return {sent,api:sandbox.module.exports};
}
const config={apiLoginId:'synthetic',transactionKey:'fixture-only',apiUrl:'https://example.invalid/authnet'};
test('unsettled connector sends explicit page number and does not filter out pending refunds',async()=>{
  const h=connector();await h.api.getUnsettledTransactionList({limit:1000,offset:2},config);
  const request=h.sent[0].payload.getUnsettledTransactionListRequest;
  assert.equal(request.paging.offset,2);assert.equal(request.paging.limit,1000);
  assert.equal(request.sorting.orderBy,'id');assert.equal(request.sorting.orderDescending,false);
  assert.equal(Object.hasOwn(request,'status'),false);
  assert.equal(h.sent.length,1);
});
test('unsettled connector rejects fractional and out-of-range paging without any request',async()=>{
  const h=connector();
  for(const paging of [{limit:0},{limit:1001},{offset:0},{offset:1.5},{offset:100001},{offset:'2'}])
    await assert.rejects(h.api.getUnsettledTransactionList(paging,config),/pagination/);
  assert.equal(h.sent.length,0);
});
