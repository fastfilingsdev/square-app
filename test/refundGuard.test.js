const test = require('node:test');
const assert = require('node:assert/strict');
const { createGuardedRefundProcessor } = require('../src/features/billingRefunds/refundGuard');

function harness({ provider, finish, processor } = {}) {
  let claimed = false;
  const calls = [];
  const process = createGuardedRefundProcessor({
    providerScope: 'synthetic_sandbox',
    ledger: { claim: async op => { calls.push(op); if (claimed) return null; claimed = true; return {}; },
      finish: finish || (async () => {}) },
    refundTransactionFn: provider || (async () => ({ transactionResponse: { responseCode:'1',transId:'987' } })),
    processRefundFn: processor || (async args => {
      try { return { ok: true, response: await args.refundTransactionFn({refTransId:'123',amount:'10.00'}) }; }
      catch (err) { return {ok:false,issues:[err.message]}; }
    })
  });
  return { process, calls };
}
test('unconfigured server fails closed even with request-supplied ledger and scope', async () => {
  const process = createGuardedRefundProcessor({processRefundFn:async () => assert.fail('preflight must not execute')});
  assert.equal((await process({ledger:{},providerScope:'attacker'})).code, 'REFUND_LEDGER_UNAVAILABLE');
});
test('provider adapter and scope cannot be overridden by request fields', async () => {
  const h = harness();
  const result = await h.process({providerScope:'attacker',refundTransactionFn:async () => assert.fail('untrusted')});
  assert.equal(result.ok, true); assert.equal(h.calls[0].providerScope, 'synthetic_sandbox');
  assert.equal((await h.process({})).code, 'REFUND_OPERATION_ALREADY_RECORDED');
});
test('failed preflight never claims or invokes provider', async () => {
  const h = harness({processor:async () => ({ok:false,status:'BLOCKED / ERROR'}),provider:async () => assert.fail('provider')});
  assert.equal((await h.process({})).ok, false); assert.equal(h.calls.length, 0);
});
test('provider timeout translates legacy error into non-retryable reconciliation result', async () => {
  const h = harness({provider:async () => {throw new Error('SECRET');}});
  const result = await h.process({});
  assert.equal(result.status, 'RECONCILIATION REQUIRED'); assert.equal(result.automatic_retry_allowed, false);
  assert.equal(JSON.stringify(result).includes('SECRET'), false);
});
test('missing provider approval code becomes reconciliation, never ledger success', async () => {
  const states = [];
  const h = harness({provider:async () => ({transactionResponse:{transId:'987'}}),
    finish:async (_claim,state) => states.push(state)});
  const result = await h.process({});
  assert.equal(result.code,'REFUND_PROVIDER_OUTCOME_UNCONFIRMED');
  assert.equal(result.automatic_retry_allowed,false);
  assert.deepEqual(states,['needs_reconciliation']);
  assert.equal((await h.process({})).code,'REFUND_OPERATION_ALREADY_RECORDED');
});
test('approved provider receipt retained when recording completion fails', async () => {
  const h = harness({finish:async () => {throw new Error('database unavailable');}});
  const result = await h.process({});
  assert.equal(result.code, 'REFUND_LEDGER_COMPLETION_UNCONFIRMED');
  assert.equal(result.refundTransactionId, '987'); assert.equal(result.ok, false);
});

test('actual legacy preflight and processor execute behind durable guard with synthetic providers', async () => {
  const events = [];
  const process = createGuardedRefundProcessor({
    providerScope:'synthetic_sandbox',
    ledger:{claim:async op => { events.push('claim'); assert.equal(op.transactionId, '555777001'); return {}; },
      finish:async (_claim,state,id) => { events.push(state); assert.equal(id, '987'); }},
    refundTransactionFn:async request => {
      events.push('provider'); assert.equal(request.emailCustomer, false);
      return {transactionResponse:{responseCode:'1',transId:'987'}};
    }
  });
  const result = await process({
    lookup:'555777001', transactionId:'555777001', refundType:'FULL',
    reason:'Synthetic fixture only', approvedBy:'Synthetic reviewer', liveConfirm:'PROCESS LIVE REFUND',
    subscriptionsSpreadsheetId:'',
    sheets:{spreadsheets:{values:{get:async () => ({data:{values:[]}})}}},
    getTransactionDetailsFn:async () => ({transaction:{transId:'555777001',transactionType:'authCaptureTransaction',
      transactionStatus:'settledSuccessfully',settleAmount:'10.00',payment:{creditCard:{cardNumber:'XXXX1111'}}}}),
    getSubscriptionFn:async () => {throw new Error('unexpected');},
    getTransactionListForCustomerFn:async () => ({transactions:[]}),
    getSettledBatchListFn:async () => ({batchList:[]})
  });
  assert.equal(result.ok, true, JSON.stringify(result));
  assert.deepEqual(events, ['claim','provider','succeeded']);
});

test('actual refund route blocks authenticated mutation until server ledger is configured', async () => {
  const { createBillingRefundsRouter } = require('../src/features/billingRefunds/routes');
  const previous = process.env.FF_SYNC_ADMIN_TOKEN;
  process.env.FF_SYNC_ADMIN_TOKEN = 'synthetic-admin';
  try {
    const router = createBillingRefundsRouter();
    const handler = router.stack.find(layer => layer.route?.path === '/refunds/process').route.stack[0].handle;
    let status, body;
    const req = {body:{providerScope:'request-cannot-configure',refundLedger:{}}, get:name => name === 'x-ff-sync-token' ? 'synthetic-admin' : ''};
    const res = {set(){return this;},status(value){status=value;return this;},json(value){body=value;return this;}};
    await handler(req,res);
    assert.equal(status,409); assert.equal(body.code,'REFUND_LEDGER_UNAVAILABLE');
  } finally {
    if (previous === undefined) delete process.env.FF_SYNC_ADMIN_TOKEN; else process.env.FF_SYNC_ADMIN_TOKEN = previous;
  }
});
