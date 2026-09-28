const test = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');
const path = require('node:path');
const dir = path.join(__dirname, '../integrations/sales-tax');
const source = fs.readFileSync(path.join(dir, '04_ConnectionsSync.gs'), 'utf8');
function fixture() {
  const con = [['customer id','square merchant id','connected'],['AZ-1','M1','Yes']];
  const cfg = [['state','spreadsheet id','customers tab','active'],['AZ','synthetic','Customers','yes']];
  const sq = [[],[],['customer id','square merchant id','state','name','business name','filing frequency','status','square connected','date added','last sync','notes','effective from'],['AZ-1','M1','AZ','Old name','Biz','Monthly','Active','Yes','original date','filing stamp','manual note','keep']];
  const states = {AZ:[[],['customer id','platform access','name','business name','filing frequency','status','sales platform'],['AZ-1','No','New name','Biz','Monthly','Active','']]};
  const ctx = vm.createContext({}); vm.runInContext(source,ctx);
  return {ctx,con,cfg,sq,states,plan:toStates=>ctx.ffSqSyncPlan_(con,cfg,sq,states,toStates)};
}
test('consolidated customer plan never overwrites filing stamp, manual note or original date',()=>{
  const f=fixture(), plan=f.plan(false); assert.equal(plan.length,1);
  const columns=plan[0].cells.map(c=>c.col);
  for(const col of [9,10,11,12]) assert.ok(!columns.includes(col));
  assert.ok(plan[0].cells.some(c=>c.col===4&&c.value==='New name'));
});
test('new customer receives date but no false filing timestamp',()=>{
  const f=fixture();f.sq.pop();const entry=f.plan(false)[0];assert.equal(entry.row,null);assert.ok(entry.cells.some(c=>c.col===9));assert.ok(!entry.cells.some(c=>c.col===10));
});
for(const target of ['con','cfg','sq','state']) test('duplicate '+target+' identity rejected before any write plan',()=>{
  const f=fixture(), rows=target==='state'?f.states.AZ:f[target];rows.push(rows.at(-1).slice());assert.throws(()=>f.plan(false),/Duplicate/);
});
test('merchant remapping cannot silently replace customer identity',()=>{
  const f=fixture();f.sq[3][1]='OTHER';assert.throws(()=>f.plan(false),/mapping/);
});
test('missing state customer rejects rather than overwriting another row',()=>{
  const f=fixture();f.states.AZ.pop();assert.throws(()=>f.plan(false),/missing/);
});
test('state sync touches only platform fields, not status or notes',()=>{
  const f=fixture(),entry=f.plan(true)[0];assert.equal(entry.row,3);assert.deepEqual(Array.from(entry.cells,c=>c.col),[2,7]);
});
test('combined candidate files define every global function only once',()=>{
  const all=fs.readdirSync(dir).filter(n=>n.endsWith('.gs')).map(n=>fs.readFileSync(path.join(dir,n),'utf8')).join('\n');
  const names=Array.from(all.matchAll(/^function\s+(\w+)\(/gm),m=>m[1]);assert.equal(new Set(names).size,names.length);
  const ctx=vm.createContext({});vm.runInContext(all,ctx);
  for(const name of ['syncConnectedCustomersToSQ','syncConnectionsToStates','runSalesDataSQ','runSalesDataSQSelectedRow','doPost','doGet']) assert.equal(typeof ctx[name],'function');
});
test('executor completes with explicit acknowledgement and releases lock; writes retain protected cells',()=>{
  const f=fixture();let released=0;
  function sheet(values){return {getDataRange:()=>({getValues:()=>values}),getRange:(r,c)=>({getValue:()=>values[r-1][c-1],getFormula:()=>'',setValue:v=>{values[r-1][c-1]=v;}}),appendRow:r=>values.push(r)};}
  const tabs={'Connections':sheet(f.con),'Config - States':sheet(f.cfg),'Customers':sheet(f.sq)};
  f.ctx.SpreadsheetApp={getActiveSpreadsheet:()=>({getSheetByName:n=>tabs[n]}),openById:()=>({getSheetByName:()=>sheet(f.states.AZ)})};
  f.ctx.LockService={getScriptLock:()=>({tryLock:()=>true,releaseLock:()=>released++})};
  assert.equal(f.ctx.syncConnectedCustomersToSQ().success,true);assert.equal(released,1);
  assert.equal(f.sq[3][3],'New name');assert.equal(f.sq[3][9],'filing stamp');assert.equal(f.sq[3][10],'manual note');
});
test('busy executor throws instead of falsely acknowledging success',()=>{
  const f=fixture();f.ctx.LockService={getScriptLock:()=>({tryLock:()=>false})};assert.throws(()=>f.ctx.syncConnectedCustomersToSQ(),/busy/);
});
test('signed backend request runs actual consolidated job; replay cannot write again',async()=>{
  const crypto=require('node:crypto');
  const {triggerSqCustomerSync}=require('../src/core/sqCustomerSync');
  const f=fixture();let writes=0;
  function sheet(values){return {getDataRange:()=>({getValues:()=>values}),getRange:(r,c)=>({getValue:()=>values[r-1][c-1],getFormula:()=>'',setValue:v=>{writes++;values[r-1][c-1]=v;}}),appendRow:r=>{writes++;values.push(r);}};}
  const tabs={'Connections':sheet(f.con),'Config - States':sheet(f.cfg),'Customers':sheet(f.sq)};
  f.ctx.SpreadsheetApp={getActiveSpreadsheet:()=>({getSheetByName:n=>tabs[n]}),openById:()=>({getSheetByName:()=>sheet(f.states.AZ)})};
  f.ctx.LockService={getScriptLock:()=>({tryLock:()=>true,releaseLock:()=>{}})};
  const props={SQ_CUSTOMER_SYNC_SECRET:'synthetic-signing-secret-1234567890',SQ_CUSTOMER_SYNC_ENABLED:'true'};
  f.ctx.PropertiesService={getScriptProperties:()=>({getProperty:k=>props[k],getProperties:()=>({...props}),setProperty:(k,v)=>{props[k]=v;},deleteProperty:k=>delete props[k]})};
  f.ctx.Utilities={computeHmacSha256Signature:(s,k)=>[...crypto.createHmac('sha256',k).update(s).digest()]};
  f.ctx.ContentService={MimeType:{JSON:'json'},createTextOutput:s=>({setMimeType:()=>JSON.parse(s)})};
  vm.runInContext(fs.readFileSync(path.join(dir,'07_Webhook.gs'),'utf8'),f.ctx);
  let event;
  const result=await triggerSqCustomerSync({env:{SQ_CUSTOMER_SYNC_URL:'https://script.google.com/macros/s/synthetic/exec',SQ_CUSTOMER_SYNC_SECRET:props.SQ_CUSTOMER_SYNC_SECRET},http:{post:async(u,b)=>{event={postData:{contents:JSON.stringify(b)}};return {status:200,data:f.ctx.doPost(event)};}}});
  assert.equal(result.success,true);assert.equal(writes,1);assert.equal(f.ctx.doPost(event).success,false);assert.equal(writes,1);assert.equal(f.sq[3][9],'filing stamp');
});
