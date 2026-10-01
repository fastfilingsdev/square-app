const test = require('node:test');
const assert = require('node:assert/strict');
const { createRefundLedger, executeLedgerRefund } = require('../src/core/refundLedger');

const operation = { providerScope: 'synthetic_sandbox', transactionId: '123456', amount: '10.00' };
// Contract double only: this does NOT prove PostgreSQL syntax, durability or
// isolation. Native concurrent connections and migration tests are release gates.
function database() {
  const records = new Map();
  const calls = [];
  async function query(sql, values) {
    calls.push({ sql, values });
    const [scope, tx, amount, attempt, state, providerId] = values;
    const key = `${scope}|${tx}`;
    if (sql.startsWith('INSERT')) {
      if (records.has(key)) return { rowCount: 0, rows: [] };
      records.set(key, { amount, attempt, state: 'dispatching' });
      return { rowCount: 1, rows: [{ attempt_id: attempt }] };
    }
    const row = records.get(key);
    if (!row || row.attempt !== attempt || row.amount !== amount || row.state !== 'dispatching') return { rowCount: 0, rows: [] };
    Object.assign(row, { state, providerId });
    return { rowCount: 1, rows: [{ attempt_id: attempt }] };
  }
  return { query, records, calls };
}
function run(ledger, overrides = {}) {
  return executeLedgerRefund({ ledger, operation, refund: async () => '987654', parseRefundId: x => x, ...overrides });
}

test('ledger requires a persistent query adapter', () => {
  assert.throws(() => createRefundLedger(), /Persistent/);
});
test('claim uses bound parameters and original-transaction uniqueness, not amount uniqueness', async () => {
  const db = database();
  const ledger = createRefundLedger(db);
  const claim = await ledger.claim(operation);
  assert.ok(claim.attemptId);
  assert.match(db.calls[0].sql, /ON CONFLICT \(provider_scope, original_transaction_id\) DO NOTHING/);
  assert.equal(db.calls[0].sql.includes(operation.providerScope), false);
  assert.deepEqual(db.calls[0].values.slice(0, 3), Object.values(operation));
  assert.equal(await ledger.claim({ ...operation, amount: '9.00' }), null);
});
test('invalid identities and non-canonical money fail before query', async () => {
  const db = database(); const ledger = createRefundLedger(db);
  for (const patch of [{providerScope:''}, {providerScope:"x';DROP"}, {transactionId:'0'}, {transactionId:' 123'},
    {amount:10}, {amount:'1e2'}, {amount:'0.00'}, {amount:'1.001'}, {amount:'01.00'}, {amount:'10000000000.00'}]) {
    await assert.rejects(ledger.claim({ ...operation, ...patch }), /Invalid/);
  }
  assert.equal(db.calls.length, 0);
});
test('simultaneous callers through shared contract store dispatch only once', async () => {
  const db = database(); let count = 0;
  const outcomes = await Promise.all(Array.from({length: 12}, () => run(createRefundLedger(db), {
    refund: async () => { count++; return '987654'; }
  })));
  assert.equal(count, 1); assert.equal(outcomes.filter(x => x.ok).length, 1);
  assert.equal(outcomes.filter(x => x.code === 'REFUND_OPERATION_ALREADY_RECORDED').length, 11);
});
test('new ledger object cannot re-dispatch a completed operation', async () => {
  const db = database(); assert.equal((await run(createRefundLedger(db))).ok, true);
  const result = await run(createRefundLedger(db), { refund: async () => assert.fail('duplicate') });
  assert.equal(result.code, 'REFUND_OPERATION_ALREADY_RECORDED');
});
test('provider timeout preserves a reconciliation record and never retries', async () => {
  const db = database(); let count = 0;
  const result = await run(createRefundLedger(db), { refund: async () => { count++; throw new Error('SECRET provider URL'); } });
  assert.equal(result.code, 'REFUND_PROVIDER_OUTCOME_UNCONFIRMED');
  assert.equal([...db.records.values()][0].state, 'needs_reconciliation');
  assert.equal(JSON.stringify(result).includes('SECRET'), false);
  assert.equal((await run(createRefundLedger(db))).ok, false); assert.equal(count, 1);
});
test('committed claim with lost response cannot invoke provider', async () => {
  const db = database();
  const ledger = createRefundLedger({ query: async (...args) => { await db.query(...args); throw new Error('SECRET'); } });
  const result = await run(ledger, { refund: async () => assert.fail('must not dispatch') });
  assert.equal(result.code, 'REFUND_LEDGER_CLAIM_UNCONFIRMED');
  assert.equal((await run(createRefundLedger(db))).code, 'REFUND_OPERATION_ALREADY_RECORDED');
});
test('lost completion response preserves provider receipt and blocks replay', async () => {
  const db = database();
  const ledger = createRefundLedger({ query: async (sql, args) => {
    const result = await db.query(sql, args); if (sql.startsWith('UPDATE')) throw new Error('SECRET'); return result;
  } });
  const result = await run(ledger);
  assert.equal(result.code, 'REFUND_LEDGER_COMPLETION_UNCONFIRMED');
  assert.equal(result.providerRefundId, '987654'); assert.equal(result.automatic_retry_allowed, false);
  assert.equal((await run(createRefundLedger(db))).code, 'REFUND_OPERATION_ALREADY_RECORDED');
});
test('failed reconciliation write leaves the original dispatch claim blocking', async () => {
  const db = database();
  const ledger = createRefundLedger({ query: async (sql, args) => {
    if (sql.startsWith('UPDATE')) throw new Error('unavailable'); return db.query(sql, args);
  } });
  await run(ledger, { refund: async () => { throw new Error('timeout'); } });
  assert.equal([...db.records.values()][0].state, 'dispatching');
  assert.equal((await run(createRefundLedger(db))).ok, false);
});
test('missing ledger blocks before provider', async () => {
  const result = await run(null, { refund: async () => assert.fail('must not dispatch') });
  assert.equal(result.code, 'REFUND_LEDGER_UNAVAILABLE');
});
test('unrecognized claim acknowledgement fails closed', async () => {
  for (const response of [null, {rowCount:1, rows:[]}, {rowCount:1, rows:[{attempt_id:'wrong'}]}, {rowCount:0}]) {
    const result = await run(createRefundLedger({query:async () => response}), {refund:async () => assert.fail('must not dispatch')});
    assert.equal(result.code, 'REFUND_LEDGER_CLAIM_UNCONFIRMED');
  }
});
test('completion requires exact single-use ownership object and approved state', async () => {
  const db = database(); const ledger = createRefundLedger(db); const claim = await ledger.claim(operation);
  await assert.rejects(ledger.finish({...claim}, 'succeeded', '987'), /Invalid/);
  await assert.rejects(ledger.finish(claim, 'released'), /Invalid/);
  await ledger.finish(claim, 'succeeded', '987');
  await assert.rejects(ledger.finish(claim, 'succeeded', '987'), /Invalid/);
  assert.equal(db.calls.length, 2);
});
test('malformed provider receipt is unconfirmed, not succeeded', async () => {
  for (const receipt of [undefined, '0', 123, 'not-an-id']) {
    const db = database(); const result = await run(createRefundLedger(db), {refund:async () => receipt});
    assert.equal(result.code, 'REFUND_PROVIDER_OUTCOME_UNCONFIRMED');
    assert.equal([...db.records.values()][0].state, 'needs_reconciliation');
  }
});
test('provider account/environment scopes remain distinct', async () => {
  const db = database(); assert.equal((await run(createRefundLedger(db))).ok, true);
  assert.equal((await run(createRefundLedger(db), {operation:{...operation,providerScope:'other_sandbox'}})).ok, true);
});
test('lost process after claim leaves no expiry/release takeover path', async () => {
  const db = database(); await createRefundLedger(db).claim(operation);
  assert.equal((await run(createRefundLedger(db))).code, 'REFUND_OPERATION_ALREADY_RECORDED');
  assert.deepEqual(Object.keys(createRefundLedger(db)), ['claim', 'finish']);
  assert.equal(db.calls.some(x => /DELETE|expires|lease/i.test(x.sql)), false);
});
