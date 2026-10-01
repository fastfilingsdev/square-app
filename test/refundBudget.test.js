const test = require('node:test');
const assert = require('node:assert/strict');
const { minorUnits, decimalAmount, reconcileRefundBudget } = require('../src/core/refundBudget');
const { validateRefundSelection } = require('../src/features/billingRefunds/refundLookup');

test('money is exact, accepts normal numeric sheet cells, never rounds', () => {
  for (const [input, output] of [['0.29',29n], [0.29,29n], ['10',1000n], ['10.1',1010n], ['9999999999.99',999999999999n]]) {
    assert.equal(minorUnits(input),output);
    assert.equal(minorUnits(decimalAmount(output)),output);
  }
  for (const input of ['1e2', '10oops', '$10', '1,000', '0.001', '-1', '01', true, null, {}, Infinity, NaN, 0.1 + 0.2]) {
    assert.throws(() => minorUnits(input), /Invalid/);
  }
});

test('actual refund selection rejects malformed partial amounts and one-cent overflow', () => {
  const candidate = {refundable:true,refundableAmount:'0.30'};
  for (const refundAmount of ['0.301', '0.31', '0.1e2', '0.20oops', '0.00', '$0.20']) {
    assert.equal(validateRefundSelection({candidate,refundType:'PARTIAL',refundAmount}).ok,false);
  }
  assert.equal(validateRefundSelection({candidate,refundType:'PARTIAL',refundAmount:0.29}).refundAmount,'0.29');
  assert.equal(validateRefundSelection({candidate,refundType:'FULL'}).refundAmount,'0.30');
});

const success = (id,amount,requestId=id) => ({state:'succeeded',providerRefundId:id,amount,requestId});
const budget = (providerRefunds=[],ledgerRefunds=[],originalAmount='100.00') =>
  reconcileRefundBudget({originalAmount,providerRefunds,ledgerRefunds});

test('multiple distinct partial refunds consume one original payment up to exactly 100 percent', () => {
  assert.equal(budget([], [success('1','25.00'),success('2','75.00')]).remainingAmount,'0.00');
  assert.throws(() => budget([], [success('1','25.00'),success('2','75.01')]),/exceeds/);
});

test('provider and ledger receipts are unioned, not summed twice or combined using max', () => {
  const result = budget([success('1','20.00'),success('2','30.00')], [success('2','30.00'),success('3','10.00')]);
  assert.equal(result.consumedAmount,'60.00');
  assert.equal(result.remainingAmount,'40.00');
});

test('uncertain operations retain capacity and require reconciliation', () => {
  for (const state of ['dispatching','needs_reconciliation']) {
    const result = budget([success('1','20.00')],[{requestId:'pending',state,amount:'30.00'}]);
    assert.equal(result.remainingAmount,'50.00');
    assert.equal(result.requiresReconciliation,true);
  }
});

test('conflicting receipts, duplicated requests and invalid evidence fail closed', () => {
  assert.throws(() => budget([success('1','20.00')],[success('1','21.00')]),/Conflicting/);
  assert.throws(() => budget([],[success('1','20.00','same'),success('2','20.00','same')]),/identity/);
  assert.throws(() => budget([{state:'pending',amount:'10.00'}]),/Unresolved/);
  assert.throws(() => budget([],[{state:'released',requestId:'x',amount:'10.00'}]),/state/);
  assert.throws(() => budget([success('0','10.00')]),/receipt/);
  assert.throws(() => budget([success('1','0.00')]),/amount/);
  assert.throws(() => reconcileRefundBudget({originalAmount:'100.00'}),/reconciliation/);
});

test('many tiny partial refunds have no floating-point drift', () => {
  const rows=Array.from({length:100},(_,i)=>success(String(i+1),'0.01'));
  assert.equal(budget([],rows,'1.00').remainingAmount,'0.00');
});
