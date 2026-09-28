const test = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { triggerSqCustomerSync } = require('../src/core/sqCustomerSync');
const source = fs.readFileSync(path.join(__dirname, '../integrations/sales-tax/07_Webhook.gs'), 'utf8');
const secret = 'synthetic-only-signing-key-1234567890';
const env = { SQ_CUSTOMER_SYNC_SECRET: secret, SQ_CUSTOMER_SYNC_URL: 'https://script.google.com/macros/s/synthetic/exec' };
function fixture(options = {}) {
  const props = { SQ_CUSTOMER_SYNC_SECRET: secret, SQ_CUSTOMER_SYNC_ENABLED: 'true', ...options.props };
  let writes = 0;
  const ctx = vm.createContext({ Date, PropertiesService: { getScriptProperties: () => ({
    getProperty: k => props[k], getProperties: () => ({...props}), setProperty: (k,v) => { props[k]=v; }, deleteProperty: k => { delete props[k]; }
  }) }, Utilities: { computeHmacSha256Signature: (s,k) => [...crypto.createHmac('sha256',k).update(s).digest()] },
  LockService: { getScriptLock: () => ({ tryLock: () => !options.busy, releaseLock: () => {} }) },
  ContentService: { MimeType: {JSON:'json'}, createTextOutput: s => ({ setMimeType: () => JSON.parse(s) }) },
  syncConnectedCustomersToSQ: () => { writes++; if(options.failure) throw new Error('secret'); return options.noResult ? undefined : {success:true}; }
  });
  vm.runInContext(source,ctx);
  return {ctx,props,writes:()=>writes, run: body => ctx.doPost({postData:{contents:JSON.stringify(body)}})};
}
async function signedBody() {
  let body;
  await triggerSqCustomerSync({env,http:{post:async (u,b)=>{body=b;return {status:200,data:{success:true}};}}});
  return body;
}
test('backend signature accepted by native-compatible handler once, replay blocked',async()=>{
  const f=fixture(), body=await signedBody();
  assert.equal(f.run(body).success,true); assert.equal(f.run(body).success,false); assert.equal(f.writes(),1);
});
test('GET cannot sync',()=>{const f=fixture();assert.equal(f.ctx.doGet().success,false);assert.equal(f.writes(),0);});
for (const field of ['action','timestamp','nonce','signature']) test('tampered '+field+' cannot write',async()=>{
  const body=await signedBody(); body[field]=field==='timestamp'?Date.now()-120000:'tampered';
  const f=fixture(); assert.equal(f.run(body).success,false); assert.equal(f.writes(),0);
});
for (const options of [{busy:true},{props:{SQ_CUSTOMER_SYNC_ENABLED:'false'}},{props:{SQ_CUSTOMER_SYNC_SECRET:''}}]) test('disabled/misconfigured/locked rejects '+JSON.stringify(options),async()=>{
  const f=fixture(options);assert.equal(f.run(await signedBody()).success,false);assert.equal(f.writes(),0);
});
test('uncertain write consumes nonce and never reports success',async()=>{
  for(const options of [{failure:true},{noResult:true}]) {const f=fixture(options),body=await signedBody();assert.equal(f.run(body).success,false);assert.equal(f.run(body).success,false);assert.equal(f.writes(),1);}
});
test('only trusted response redirect is fetched without signed body',async()=>{
  const calls=[];
  await triggerSqCustomerSync({env,http:{post:async(u,b,o)=>{calls.push(o);return {status:302,headers:{location:'https://script.googleusercontent.com/macros/echo?synthetic=1'}};},get:async(u,o)=>{calls.push(o);return {status:200,data:{success:true}};}}});
  assert.equal(calls.length,2); assert.equal(calls[0].maxRedirects,0);assert.equal(calls[1].maxRedirects,0);assert.equal(calls[1].headers,undefined);
});
test('untrusted redirect, malformed success and transport failure never retry',async()=>{
  for(const response of [{status:302,headers:{location:'https://evil.invalid/'}},{status:200,data:{success:false}},null]) {
    let calls=0;
    await assert.rejects(triggerSqCustomerSync({env,http:{post:async()=>{calls++;if(!response)throw new Error(secret);return response;},get:async()=>{throw new Error('must not GET');}}}), /unconfirmed/);
    assert.equal(calls,1);
  }
});
test('missing signing secret or unsafe destination fails before HTTP',async()=>{
  for(const change of [{SQ_CUSTOMER_SYNC_SECRET:''},{SQ_CUSTOMER_SYNC_URL:'https://evil.invalid/macros/s/test/exec'}]) await assert.rejects(triggerSqCustomerSync({env:{...env,...change},http:{post:()=>assert.fail('must not call')}}));
});
test('nonce storage capacity fails closed before writes',async()=>{
  const f=fixture();
  for(let i=0;i<100;i++) f.props['FF_SQ_NONCE_'+i]=String(Date.now());
  assert.equal(f.run(await signedBody()).success,false); assert.equal(f.writes(),0);
});
test('only expired nonce records are cleaned; unrelated properties survive',async()=>{
  const f=fixture();f.props.FF_SQ_NONCE_old=String(Date.now()-180000);f.props.unrelated='preserve';
  assert.equal(f.run(await signedBody()).success,true); assert.equal(f.props.FF_SQ_NONCE_old,undefined); assert.equal(f.props.unrelated,'preserve');
});
test('coercible array nonce and signature are rejected',async()=>{
  for(const field of ['nonce','signature']) {
    const f=fixture(),body=await signedBody();body[field]=[body[field]];
    assert.equal(f.run(body).success,false);assert.equal(f.writes(),0);
  }
});
