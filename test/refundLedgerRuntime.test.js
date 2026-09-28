const test=require('node:test');
const assert=require('node:assert/strict');
const {EventEmitter}=require('node:events');
const {createRefundLedgerRuntime,ledgerPoolConfig}=require('../src/core/refundLedgerRuntime');
const env={FF_REFUND_LEDGER_DATABASE_URL:'postgres://synthetic:fixture-only@db.example.invalid/ledger_test',FF_REFUND_PROVIDER_SCOPE:'synthetic_sandbox'};
const op={providerScope:'synthetic_sandbox',transactionId:'123456',amount:'10.00'};
function fixture({session={},status='I',failure='',statusAfterWrite='I'}={}) {
  const calls=[],releases=[]; let pool; let txStatus=status;
  class Pool extends EventEmitter {
    constructor(config){super();this.config=config;pool=this;this.ends=0;}
    async connect(){
      if(failure==='connect')throw new Error('SECRET');
      return {getTransactionStatus:()=>txStatus,release:discard=>releases.push(discard),query:async(sql,args)=>{
        calls.push({sql,args});
        if(sql.startsWith('SELECT')){
          if(failure==='check')throw new Error('SECRET');
          return {rows:[{recovery:false,synchronous_commit:'on',read_only:'off',fsync:'on',original_role:true,restricted_role:true,...session}]};
        }
        if(failure==='write')throw new Error('SECRET');
        txStatus=statusAfterWrite;
        return {rowCount:1,rows:[{attempt_id:args[3]}]};
      }};
    }
    async end(){this.ends++;}
  }
  const logs=[];
  const runtime=createRefundLedgerRuntime({env,PoolClass:Pool,onPoolError:m=>logs.push(m)});
  return {runtime,pool,calls,releases,logs};
}
test('missing dedicated config does not construct a pool or use generic DATABASE_URL',()=>{
  const r=createRefundLedgerRuntime({env:{DATABASE_URL:'postgres://unrelated'},PoolClass:class{constructor(){assert.fail('no pool');}}});
  assert.deepEqual(r.routerOptions,{});
});
test('pool has bounded concurrency/timeouts and verified TLS by default',()=>{
  const config=ledgerPoolConfig(env);
  assert.equal(config.max,4);assert.equal(config.connectionTimeoutMillis,5000);
  assert.equal(config.query_timeout,7000);assert.equal(config.statement_timeout,5000);
  assert.deepEqual(config.ssl,{rejectUnauthorized:true,minVersion:'TLSv1.2'});
  assert.match(config.options,/synchronous_commit=on/);assert.equal(config.pipeline,false);
});
test('configuration rejects partial config, URI overrides and unsafe TLS without leaking URL',()=>{
  for(const patch of [
    {FF_REFUND_PROVIDER_SCOPE:''},{FF_REFUND_LEDGER_DATABASE_URL:''},
    {FF_REFUND_LEDGER_DATABASE_URL:env.FF_REFUND_LEDGER_DATABASE_URL+'?sslmode=disable'},
    {FF_REFUND_LEDGER_DATABASE_URL:'https://synthetic:SECRET@db.example.invalid/ledger_test'},
    {FF_REFUND_LEDGER_DATABASE_URL:'postgres://synthetic:SECRET@db.example.invalid:0/ledger_test'},
    {FF_REFUND_LEDGER_TLS_MODE:'render-internal'},{FF_REFUND_LEDGER_TLS_MODE:'disable'}
  ])assert.throws(()=>ledgerPoolConfig({...env,...patch}),e=>e.message==='Invalid refund ledger configuration');
});
test('explicit Render-internal mode requires an internal-form hostname and still encrypts',()=>{
  const config=ledgerPoolConfig({...env,FF_REFUND_LEDGER_DATABASE_URL:'postgres://synthetic:fixture-only@dpg-synthetic-a/ledger_test',FF_REFUND_LEDGER_TLS_MODE:'render-internal'});
  assert.deepEqual(config.ssl,{rejectUnauthorized:false,minVersion:'TLSv1.2'});
});
test('claim checks primary/durability/autocommit before write and releases connection',async()=>{
  const f=fixture();const claim=await f.runtime.routerOptions.refundLedger.claim(op);
  assert.ok(claim);assert.equal(f.calls.length,2);assert.match(f.calls[0].sql,/pg_is_in_recovery/);
  assert.match(f.calls[1].sql,/^INSERT/);assert.deepEqual(f.releases,[false]);
});
for(const [label,session] of [['replica',{recovery:true}],['async commit',{synchronous_commit:'off'}],
  ['read-only',{read_only:'on'}],['fsync disabled',{fsync:'off'}],
  ['elevated role',{restricted_role:false}],['switched role',{original_role:false}],
  ['unknown role permissions',{restricted_role:null}],['missing role permissions',{restricted_role:undefined}],
  ['missing original role',{original_role:undefined}]]){
  test(`unsafe ${label} session blocks before SQL mutation`,async()=>{
    const f=fixture({session});await assert.rejects(f.runtime.routerOptions.refundLedger.claim(op),/unconfirmed/);
    assert.equal(f.calls.length,1);assert.deepEqual(f.releases,[true]);
  });
}
test('session query checks all prohibited role attributes using the catalog',async()=>{
  const f=fixture();await f.runtime.routerOptions.refundLedger.claim(op);
  for(const flag of ['rolsuper','rolcreaterole','rolcreatedb','rolreplication','rolbypassrls']) {
    assert.ok(f.calls[0].sql.includes('r.'+flag));
  }
  assert.match(f.calls[0].sql,/FROM pg_catalog\.pg_roles/);
  assert.match(f.calls[0].sql,/current_user = session_user/);
});
test('already-open transaction is discarded before any query',async()=>{
  const f=fixture({status:'T'});await assert.rejects(f.runtime.routerOptions.refundLedger.claim(op));
  assert.equal(f.calls.length,0);assert.deepEqual(f.releases,[true]);
});
test('write acknowledgement within an open transaction is not treated as committed',async()=>{
  const f=fixture({statusAfterWrite:'T'});await assert.rejects(f.runtime.routerOptions.refundLedger.claim(op));
  assert.equal(f.calls.length,2);assert.deepEqual(f.releases,[true]);
});
for(const failure of ['connect','check','write'])test(`${failure} failure is sanitized and never retried`,async()=>{
  const f=fixture({failure});await assert.rejects(f.runtime.routerOptions.refundLedger.claim(op),e=>!e.message.includes('SECRET'));
  assert.equal(f.calls.filter(c=>c.sql.startsWith('INSERT')).length,failure==='write'?1:0);
  assert.deepEqual(f.releases,failure==='connect'?[]:[true]);
});
test('pool idle errors are sanitized and close blocks further claims',async()=>{
  const f=fixture();f.pool.emit('error',new Error('SECRET'));
  assert.deepEqual(f.logs,['Refund ledger connection unavailable']);
  await f.runtime.close();await f.runtime.close();assert.equal(f.pool.ends,1);
  await assert.rejects(f.runtime.routerOptions.refundLedger.claim(op));assert.equal(f.calls.length,0);
});
