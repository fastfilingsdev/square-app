const test=require('node:test');
const assert=require('node:assert/strict');
const {__billingRefundsTestHooks:h}=require('../src/features/billingRefunds/refundLookup');
const original={transId:'123',order:{invoiceNumber:'shared-invoice'},customer:{email:'fixture@example.invalid'}};
const refund={transactionType:'refundTransaction',order:original.order,customer:original.customer,settleAmount:'1.00'};
test('same invoice or email cannot override a conflicting original transaction reference',()=>{
  const other={...refund,refTransId:'456'};
  assert.equal(h.refundCreditLikelyMatchesOriginal(other,original),false);
  assert.equal(h.refundCreditMatchesOriginal(other,original),false);
});
test('invoice can select a candidate for detail lookup but is never authoritative refund linkage',()=>{
  assert.equal(h.refundCreditLikelyMatchesOriginal(refund,original),true);
  assert.equal(h.refundCreditMatchesOriginal(refund,original),false);
  assert.equal(h.refundCreditMatchesOriginal({...refund,refTransId:'123'},original),true);
  assert.equal(h.refundCreditMatchesOriginal(refund,{transId:''}),false);
});
test('only explicit successful settlement is eligible, not a status containing settled',()=>{
  for(const transactionStatus of ['unsettled','notSettled','settlementError','settled','settledPendingSettlement','failedSettled']) {
    assert.equal(h.isSettledForRefund({transactionStatus,transactionType:'authCaptureTransaction'}),false);
  }
  assert.equal(h.isSettledForRefund({transactionStatus:'settledSuccessfully',transactionType:'authCaptureTransaction'}),true);
});
