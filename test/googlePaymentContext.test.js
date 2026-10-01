'use strict';
const test=require('node:test');const assert=require('node:assert/strict');
const {createGooglePaymentContext}=require('../src/features/googlePaymentReads/context');
const workbook='synthetic-workbook-id-only-0001';
function fixture(overrides={}){
  const calls=[];
  const ranges={"'Payment Update'!A1:W1":[['Payment Update Type','Customer ID','Email','Subscription ID','Payment Update Status','Stop / Suppressed']],
    "'Payment Update'!A2:W2":[['SUB RECAPTURE C - Terminated','SYN-1','test@example.test','700','','']],
    "'Active Subscriptions'!A1:AZ10000":[['Email','Subscription ID'],['other@example.test','123']],
    "'Cancellations'!A1:N1":[['Customer Name','Email','Subscription ID','Cancel Requested At','Processed At','Auth.Net Result']],
    "'Cancellations'!A2:N2":[['Synthetic','test@example.test','700','2026-10-01','','']],...overrides};
  const context=createGooglePaymentContext({env:{FF_SUBSCRIPTIONS_SPREADSHEET_ID:workbook},sheetsClient:async()=>({spreadsheets:{values:{get:async input=>{
    calls.push(input);return {data:{values:ranges[input.range]}};}}}})});
  return {context,calls};
}
test('context binds only configured workbook, fixed tabs and bounded ranges; no write methods',async()=>{
  const f=fixture();assert.deepEqual((await f.context.recovery(2)).activeEmails,['other@example.test']);
  assert.equal((await f.context.cancellation(2))['Subscription ID'],'700');
  assert.equal(f.calls.length,5);for(const c of f.calls)assert.equal(c.spreadsheetId,workbook);
});
test('unknown/duplicate headers, partial active identity, unbounded scan and invalid rows fail closed',async()=>{
  for(const active of [[['Email','Subscription ID'],['','123']],[['Email','Email','Subscription ID']],
    [['Unknown']],Array.from({length:10000},()=>['Email','Subscription ID'])]){
    await assert.rejects(fixture({"'Active Subscriptions'!A1:AZ10000":active}).context.recovery(2));
  }
  const f=fixture();for(const row of [0,1,10001,'2',2.5])await assert.rejects(f.context.recovery(row));
  assert.equal(f.calls.length,0);
});
