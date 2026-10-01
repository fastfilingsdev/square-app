'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const vm=require('node:vm');
const fs=require('node:fs');
const path=require('node:path');
const crypto=require('node:crypto');
const axios=require('axios');
const {createBillingRefundsRouter,verifyGoogleAccessToken}=require('../src/features/billingRefunds/routes');
const {verifySqConnection}=require('../src/core/sqCustomerSync');
const gs=fs.readFileSync(path.join(__dirname,'../integrations/sales-tax/07_Webhook.gs'),'utf8');
const secret='synthetic-connection-secret-never-production';
function fixture(enabled='false') {
  const props={SQ_CUSTOMER_SYNC_ENABLED:enabled,SQ_CUSTOMER_SYNC_SECRET:secret};let writes=0;
  const ctx=vm.createContext({Date,PropertiesService:{getScriptProperties:()=>({
    getProperty:k=>props[k],setProperty:(k,v)=>{props[k]=v},deleteProperty:k=>{delete props[k]},getProperties:()=>({...props})
  })},Utilities:{computeHmacSha256Signature:(s,k)=>[...crypto.createHmac('sha256',k).update(s).digest()]},
  LockService:{getScriptLock:()=>({tryLock:()=>true,releaseLock:()=>{}})},
  ContentService:{MimeType:{JSON:'json'},createTextOutput:s=>({setMimeType:()=>JSON.parse(s)})},
  syncConnectedCustomersToSQ:()=>{writes++;return {success:true}}});
  vm.runInContext(gs,ctx);
  return {run:b=>ctx.doPost({postData:{contents:JSON.stringify(b)}}),writes:()=>writes,props};
}
test('production-shaped signed probe accepts once while sync is disabled, then rejects replay',async()=>{
  const f=fixture();let body;let posts=0,gets=0;
  const result=await verifySqConnection({env:{SQ_CUSTOMER_SYNC_SECRET:secret,SQ_CUSTOMER_SYNC_URL:'https://script.google.com/macros/s/test/exec'},
    http:{post:async(u,b,o)=>{posts++;body=b;assert.equal(o.maxRedirects,0);return {status:302,headers:{location:'https://script.googleusercontent.com/macros/echo?fixture=1'}}},
    get:async(u,o)=>{gets++;assert.equal(o.headers,undefined);return {status:200,data:f.run(body)}}}});
  assert.equal(result.success,true);assert.equal(f.run(body).success,false);
  assert.equal(posts,1);assert.equal(gets,1);assert.equal(f.writes(),0);
  assert.equal(f.props.SQ_CUSTOMER_SYNC_ENABLED,'false');
});
test('probe action cannot be switched to sync and cannot bypass the disabled gate',()=>{
  const f=fixture();const b={action:'verifyConnection',timestamp:Date.now(),nonce:crypto.randomUUID()};
  b.signature=crypto.createHmac('sha256',secret).update([b.action,b.timestamp,b.nonce].join('\n')).digest('hex');
  b.action='syncCustomers';assert.equal(f.run(b).success,false);
  b.signature=crypto.createHmac('sha256',secret).update([b.action,b.timestamp,b.nonce].join('\n')).digest('hex');
  assert.equal(f.run(b).success,false);assert.equal(f.writes(),0);
});
test('probe rejects tampered signature and expired body without writing a nonce',()=>{
  for(const change of [{signature:'0'.repeat(64)},{timestamp:Date.now()-180000}]) {
    const f=fixture();const b={action:'verifyConnection',timestamp:Date.now(),nonce:crypto.randomUUID()};
    b.signature=crypto.createHmac('sha256',secret).update([b.action,b.timestamp,b.nonce].join('\n')).digest('hex');
    Object.assign(b,change);assert.equal(f.run(b).success,false);assert.equal(f.writes(),0);
    assert.equal(Object.keys(f.props).length,2);
  }
});
test('probe refuses generic success or nonzero write receipt',async()=>{
  for(const data of [{success:true},{success:true,connectionVerified:true,customerWrites:1}]) {
    await assert.rejects(verifySqConnection({env:{SQ_CUSTOMER_SYNC_SECRET:secret,SQ_CUSTOMER_SYNC_URL:'https://script.google.com/macros/s/test/exec'},http:{post:async()=>({status:200,data})}}));
  }
});
function res(){return {headers:{},set(k,v){this.headers[k]=v;return this},status(c){this.code=c;return this},json(b){this.body=b;return this}};}
test('actual auth-check route accepts admin or allowed verified OAuth and rejects anonymous',async()=>{
  const before=process.env.FF_SYNC_ADMIN_TOKEN;const beforeEmails=process.env.FF_BILLING_REFUNDS_ALLOWED_GOOGLE_EMAILS;
  const original=axios.get;
  try {
    process.env.FF_SYNC_ADMIN_TOKEN='synthetic-admin';process.env.FF_BILLING_REFUNDS_ALLOWED_GOOGLE_EMAILS='returns@fastfilings.com';
    const router=createBillingRefundsRouter({refundTransactionFn:()=>assert.fail('no provider call')});
    const route=router.stack.find(l=>l.route?.path==='/refunds/auth-check').route;
    assert.deepEqual(Object.keys(route.methods),['get']);
    const fn=route.stack[0].handle;
    let oauthCalls=0;
    axios.get=async(u,o)=>{oauthCalls++;assert.equal(o.maxRedirects,0);return {data:{email:'returns@fastfilings.com',email_verified:true}}};
    for(const headers of [{'x-ff-sync-token':'synthetic-admin'},{authorization:'Bearer synthetic-oauth'}]) {
      const r=res();await fn({get:k=>headers[k]},r);assert.deepEqual(r.body,{ok:true,authenticated:true,operationStarted:false});assert.equal(r.headers['Cache-Control'],'no-store');
    }
    assert.equal(oauthCalls,1);
    const r=res();await fn({get:()=>''},r);assert.equal(r.code,401);assert.equal(oauthCalls,1);
    for(const data of [{email:'outside@example.test',email_verified:true},{email:'returns@fastfilings.com',email_verified:false}]) {
      axios.get=async()=>({data});const r=res();await fn({get:k=>k==='authorization'?'Bearer synthetic-oauth':''},r);assert.equal(r.code,401);
    }
    axios.get=async()=>{throw Error('secret-bearing URL synthetic-token')};
    assert.equal((await verifyGoogleAccessToken('synthetic')).error,'Google authorization could not be verified');
  } finally {
    axios.get=original;
    if(before===undefined)delete process.env.FF_SYNC_ADMIN_TOKEN;else process.env.FF_SYNC_ADMIN_TOKEN=before;
    if(beforeEmails===undefined)delete process.env.FF_BILLING_REFUNDS_ALLOWED_GOOGLE_EMAILS;else process.env.FF_BILLING_REFUNDS_ALLOWED_GOOGLE_EMAILS=beforeEmails;
  }
});
