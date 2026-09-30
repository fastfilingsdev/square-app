'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const { createMaintenance } = require('../src/core/maintenance');
function response() {
  return Object.assign(new EventEmitter(), {
    writableFinished: false, headers: {},
    set(values) { Object.assign(this.headers, values); return this; },
    status(code) { this.code = code; return this; },
    json(body) { this.body = body; return this; }
  });
}
test('maintenance starts held with enabled or invalid nonempty flag; absent flag preserves behavior', () => {
  for (const value of ['true','1','yes',' on ', 'tru', 'invalid']) {
    const m = createMaintenance({env:{FF_FOUNDATION_MAINTENANCE:value}});
    assert.equal(m.enter(), null); assert.equal(m.status().drained, true);
  }
  assert.equal(createMaintenance({env:{}}).status().paused, false);
});
test('HTTP server failure prevents a clean drain claim', () => {
  const m=createMaintenance({env:{}}),res=response();
  m.middleware({method:'POST',path:'/synthetic'},res,()=>{});
  m.pause();res.statusCode=500;res.writableFinished=true;res.emit('finish');
  assert.equal(m.status().drained,false);assert.equal(m.status().uncertain,1);
});
test('pause refuses new work but waits for the existing lease to finish', () => {
  const m = createMaintenance({env:{}}), done = m.enter();
  assert.equal(m.pause().drained, false); assert.equal(m.enter(), null);
  done(); done(); assert.deepEqual(m.status(), {paused:true,active:0,uncertain:0,drained:true});
});
test('uncertain work permanently prevents clean-drain claim in this process', () => {
  const m = createMaintenance({env:{}}), done = m.enter();
  m.pause(); done(true); done();
  assert.deepEqual(m.status(), {paused:true,active:0,uncertain:1,drained:false});
});
test('held mode blocks both mutating GET routes and POST requests before handlers', () => {
  const m=createMaintenance({env:{FF_FOUNDATION_MAINTENANCE:'true'}});
  for(const method of ['GET','POST','PUT','DELETE']) {
    const res=response(); let calls=0;
    m.middleware({method,path:'/push-to-sheets'},res,()=>calls++);
    assert.equal(calls,0); assert.equal(res.code,503);
    assert.equal(res.body.operationStarted,false); assert.equal(res.headers['Cache-Control'],'no-store');
  }
});
test('only exact read-only health paths are admitted while paused', () => {
  const m=createMaintenance({env:{FF_FOUNDATION_MAINTENANCE:'true'}});
  for(const route of ['/authnet/health','/foundation-maintenance/health','/billing/refunds/auth-check']) {
    let calls=0; m.middleware({method:'GET',path:route},response(),()=>calls++); assert.equal(calls,1);
    calls=0; m.middleware({method:'POST',path:route},response(),()=>calls++); assert.equal(calls,0);
    m.middleware({method:'GET',path:route+'/other'},response(),()=>calls++); assert.equal(calls,0);
  }
});
test('successful HTTP completion releases once across finish and close', () => {
  const m=createMaintenance({env:{}}),res=response();
  m.middleware({method:'POST',path:'/synthetic'},res,()=>{});
  assert.equal(m.pause().active,1); res.writableFinished=true;res.emit('finish');res.emit('close');
  assert.equal(m.status().drained,true);
});
test('client disconnect cannot masquerade as safe completion', () => {
  const m=createMaintenance({env:{}}),res=response();
  m.middleware({method:'POST',path:'/synthetic'},res,()=>{});
  m.pause();res.emit('close');res.emit('finish');assert.equal(m.status().drained,false);
  assert.equal(m.status().uncertain,1);
});
test('synchronous handler failure retains uncertainty', () => {
  const m=createMaintenance({env:{}}),res=response();
  assert.throws(()=>m.middleware({method:'POST',path:'/synthetic'},res,()=>{throw Error('synthetic');}));
  assert.equal(m.pause().drained,false);
});
