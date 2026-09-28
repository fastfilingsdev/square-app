const test = require('node:test');
const assert = require('node:assert/strict');
const { parseRefundTransactionId } = require('../src/features/billingRefunds/refundProcess');

for (const [label,tx] of [
  ['missing approval code',{transId:'987654'}],
  ['blank approval code',{responseCode:'',transId:'987654'}],
  ['zero transaction ID',{responseCode:'1',transId:'0'}],
  ['nonnumeric transaction ID',{responseCode:'1',transId:'not-a-receipt'}],
  ['unsafe numeric transaction ID',{responseCode:'1',transId:9007199254740992}],
  ['declined',{responseCode:'2',transId:'987654'}],
  ['error',{responseCode:'3',transId:'987654'}],
  ['held for review',{responseCode:'4',transId:'987654'}]
]) test(`refund receipt rejects ${label}`,()=>{
  assert.throws(()=>parseRefundTransactionId({transactionResponse:tx}));
});
test('refund receipt accepts explicit approved code and nonzero string ID',()=>{
  assert.equal(parseRefundTransactionId({transactionResponse:{responseCode:'1',transId:'987654'}}),'987654');
});
test('refund receipt retains supported nested response and numeric approval code',()=>{
  assert.equal(parseRefundTransactionId({createTransactionResponse:{transactionResponse:{responseCode:1,transId:'987654'}}}),'987654');
});
test('refund receipt refuses error details even with an approved code',()=>{
  assert.throws(()=>parseRefundTransactionId({transactionResponse:{responseCode:'1',transId:'987654',errors:{error:{errorCode:'33',errorText:'synthetic error'}}}}));
});
