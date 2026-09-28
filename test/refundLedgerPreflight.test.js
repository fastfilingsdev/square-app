const test=require('node:test');
const assert=require('node:assert/strict');
const {EventEmitter}=require('node:events');
const {inspectRefundLedgerConnection}=require('../src/core/refundLedgerPreflight');
const env={FF_REFUND_LEDGER_DATABASE_URL:'postgres://fixture:secret@db.example.invalid/ledger',
  FF_REFUND_PROVIDER_SCOPE:'fixture',FF_REFUND_LEDGER_MODE:'partial',FF_REFUND_CURRENCY:'USD'};
const keys=['original_role','restricted_role','primary_server','durable_storage','durable_commit',
  'read_only_probe','encrypted_connection','no_schema_create','no_executor_membership',
  'no_legacy_table_access','no_budget_table_access','no_partial_table_access','claim_available','finish_available'];
function fixture({patch={},failure='',initial='I'}={}) {
  const calls=[],releases=[]; let ended=0,status=initial,config;
  class Pool extends EventEmitter {
    constructor(value){super();config=value;}
    async connect(){
      if(failure==='connect')throw new Error('SECRET');
      return {getTransactionStatus:()=>status,release:value=>releases.push(value),
        query:async sql=>{
          calls.push(sql);
          if(failure===sql || (failure==='inspect' && sql.startsWith('SELECT')))throw new Error('SECRET');
          if(sql==='BEGIN READ ONLY')status='T';
          if(sql==='ROLLBACK')status='I';
          return {rows:[{...Object.fromEntries(keys.map(key=>[key,true])),...patch}]};
        }};
    }
    async end(){ended++;}
  }
  return {Pool,calls,releases,get ended(){return ended;},get config(){return config;}};
}
test('operator probe uses application config with one connection and read-only SQL',async()=>{
  const f=fixture();const result=await inspectRefundLedgerConnection({env,PoolClass:f.Pool});
  assert.equal(result.ok,true);assert.equal(result.financialFunctionsCalled,0);
  assert.equal(f.config.max,1);assert.equal(f.config.ssl.rejectUnauthorized,true);
  assert.equal(f.calls[0],'BEGIN READ ONLY');assert.equal(f.calls[2],'ROLLBACK');
  assert.equal(f.calls.length,3);assert.match(f.calls[1],/^SELECT/);
  assert.doesNotMatch(f.calls[1],/SELECT\s+(?:public\.)?ff_(?:claim|finish)/i);
  assert.deepEqual(f.releases,[false]);assert.equal(f.ended,1);
});
for(const key of keys) test(`operator probe rejects false or absent ${key}`,async()=>{
  for(const value of [false,undefined]) {
    const f=fixture({patch:{[key]:value}});
    await assert.rejects(inspectRefundLedgerConnection({env,PoolClass:f.Pool}),{message:'Refund ledger connection preflight failed'});
    assert.deepEqual(f.releases,[true]);assert.equal(f.ended,1);
  }
});
for(const failure of ['connect','BEGIN READ ONLY','inspect','ROLLBACK']) test(`probe sanitizes ${failure} and closes without retry`,async()=>{
  const f=fixture({failure});
  await assert.rejects(inspectRefundLedgerConnection({env,PoolClass:f.Pool}),{message:'Refund ledger connection preflight failed'});
  assert.equal(f.ended,1);assert.deepEqual(f.releases,failure==='connect'?[]:[true]);
  assert.equal(f.calls.filter(sql=>sql===failure).length,failure==='connect'||failure==='inspect'?0:1);
});
test('probe rejects existing transaction before queries',async()=>{
  const f=fixture({initial:'T'});
  await assert.rejects(inspectRefundLedgerConnection({env,PoolClass:f.Pool}));
  assert.deepEqual(f.calls,[]);assert.deepEqual(f.releases,[true]);
});
test('probe requires dedicated partial USD configuration before pool creation',async()=>{
  for(const value of [{},{...env,FF_REFUND_LEDGER_MODE:'single'},{...env,FF_REFUND_CURRENCY:'EUR'},
    {...env,FF_REFUND_LEDGER_DATABASE_URL:''}]) {
    await assert.rejects(inspectRefundLedgerConnection({env:value,PoolClass:class{constructor(){assert.fail('no pool');}}}));
  }
});
