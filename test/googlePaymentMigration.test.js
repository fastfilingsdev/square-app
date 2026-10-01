'use strict';
const test=require('node:test');const assert=require('node:assert/strict');
const migration=import('../tools/prepare-google-payment-migration.mjs');
test('replacement preserves adjacent functions and nested callbacks',async()=>{
  const {replaceFunction}=await migration;
  const source='// before\nfunction alpha() {\n  [1].map(function(x) { return x; });\n}\n\nfunction beta() {\n return 2;\n}\n';
  const result=replaceFunction(source,'alpha','function alpha() {\n return 1;\n}');
  assert.ok(result.startsWith('// before\nfunction alpha()'));assert.ok(result.endsWith('function beta() {\n return 2;\n}\n'));
  assert.throws(()=>replaceFunction(source+'\nfunction alpha() {\n}\n','alpha','x'));
  assert.throws(()=>replaceFunction(source,'missing','x'));
});
test('function replacement does not truncate column-zero nested braces or template strings',async()=>{
  const {replaceFunction}=await migration;
  const source='function alpha() {\n const template = `literal\n}\ntext`;\n if (true) {\n return template;\n}\n}\nfunction beta() {\n return 2;\n}\n';
  const result=replaceFunction(source,'alpha','function alpha() {\n return 1;\n}');
  assert.equal(result,'function alpha() {\n return 1;\n}\nfunction beta() {\n return 2;\n}\n');
  assert.throws(()=>replaceFunction(source,'alpha','function alpha() { invalid syntax }'));
});
test('complete migration removes synthetic legacy keys and preserves the local recovery journal wrapper',async()=>{
  const {prepareGooglePaymentMigration}=await migration;
  const fn=name=>'function '+name+'() {\n return "synthetic";\n}\n';
  const groups={
    'ActiveSubscriptionsWorkflow.gs':['activeWorkflowAuthNetPost_','activeWorkflowFetchSubscriptionDetails_','activeWorkflowMerchantAuthentication_'],
    'TerminationCRecovery.gs':['terminationCAuthNetPost_'],
    'DirectAuthNetCancellations.gs':['directAuthNetRequest_','directAuthNetCancelSubscription_','directAuthNetMerchantAuthentication_'],
    'Code.gs':['fetchAuthorizeNetTransactions','doPost','computeAuthNetSignature_'],
    'webhook.gs':['fetchAuthNetTransactionDetailsForWebhook_','doPost','computeAuthNetSignature_'],
    'TerminationCAutoRecovery.gs':['terminationCAutoCreatePaymentProfileFromTransaction_','terminationCAutoCreateArbSubscription_'],
    'testWebhook.gs':['testWebhook_doPost'],
    'subscriptionOps.gs':['authNetCancelSubscription_LIVE_DISABLED_']
  };
  const sources=Object.fromEntries(Object.entries(groups).map(([file,names])=>[file,names.map(fn).join('\n')]));
  sources['Code.gs']='const AUTHNET_TRANSACTION_KEY = "synthetic-only-key";\n'+sources['Code.gs'];
  sources['DirectAuthNetCancellations.gs']+='function caller() {\n return directAuthNetCancelSubscription_(request.subscriptionId);\n}\n';
  sources['TerminationCAutoRecovery.gs']+='function recover() {\n return FF_claimedTerminationCreate_(function() {\n const paymentProfile = terminationCAutoCreatePaymentProfileFromTransaction_(txId, customerProfileId, tx);\n return terminationCAutoCreateArbSubscription_(customerProfileId, paymentProfile.customerPaymentProfileId, oldSub, checkedEvidence.amount, startDate);\n });\n}\n';
  const replacements=Object.fromEntries(Object.values(groups).flat().map(name=>[name,fn(name)]));
  const prepared=prepareGooglePaymentMigration(sources,replacements);
  assert.ok(prepared['TerminationCAutoRecovery.gs'].includes('FF_claimedTerminationCreate_'));
  assert.ok(prepared['TerminationCAutoRecovery.gs'].includes('FF_recoverTerminationOnRender_(target.rowNumber'));
  assert.ok(prepared['DirectAuthNetCancellations.gs'].includes('request.subscriptionId, request.row'));
  assert.ok(!Object.values(prepared).join('').includes('synthetic-only-key'));
  assert.ok(prepared['testWebhook.gs'].includes('retired'));
  assert.ok(prepared['subscriptionOps.gs'].includes('retired'));
  assert.throws(()=>prepareGooglePaymentMigration({...sources,'unknown.gs':'const secret = AUTHNET_TRANSACTION_KEY;'},replacements),/Unmigrated/);
});
