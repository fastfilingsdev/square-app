import {Script} from 'node:vm';
// Pure transformation only. Never fetch, save, install or print live source.
// Caller must provide exact reviewed source and later compare saved readback.
function replaceFunction(source,name,replacement){
  const marker=new RegExp('^function '+name+'\\s*\\(','gm');
  const matches=[...source.matchAll(marker)];
  if(matches.length!==1)throw Error('Unexpected function count: '+name);
  const start=matches[0].index;
  // Parse, but never execute, each candidate boundary. A column-zero brace
  // inside a callback, comment or template string is not a function boundary.
  let end=-1;
  for(let candidate=source.indexOf('\n}',start);candidate>=0;candidate=source.indexOf('\n}',candidate+2)){
    try{new Script('('+source.slice(start,candidate+2)+'\n)');end=candidate;break;}catch(_){}
  }
  if(end<0)throw Error('Unterminated or unsupported function: '+name);
  new Script('('+replacement.trim()+'\n)');
  return source.slice(0,start)+replacement.trim()+source.slice(end+2);
}
function prepareGooglePaymentMigration(sources,replacements){
  const out={...sources};
  const replace=(file,name,text)=>{if(typeof out[file]!=='string')throw Error('Missing source: '+file);
    out[file]=replaceFunction(out[file],name,text);};
  for(const [file,names]of Object.entries({
    'ActiveSubscriptionsWorkflow.gs':['activeWorkflowAuthNetPost_','activeWorkflowFetchSubscriptionDetails_'],
    'TerminationCRecovery.gs':['terminationCAuthNetPost_'],
    'DirectAuthNetCancellations.gs':['directAuthNetRequest_','directAuthNetCancelSubscription_'],
    'Code.gs':['fetchAuthorizeNetTransactions'],
    'webhook.gs':['fetchAuthNetTransactionDetailsForWebhook_']
  }))for(const name of names){if(!replacements[name])throw Error('Missing replacement: '+name);replace(file,name,replacements[name]);}
  const cancel=out['DirectAuthNetCancellations.gs'];
  const oldCall='directAuthNetCancelSubscription_(request.subscriptionId)';
  if(cancel.split(oldCall).length!==2)throw Error('Unexpected cancellation caller count');
  out['DirectAuthNetCancellations.gs']=cancel.replace(oldCall,'directAuthNetCancelSubscription_(request.subscriptionId, request.row)');
  const block=/const paymentProfile = terminationCAutoCreatePaymentProfileFromTransaction_\(txId, customerProfileId, tx\);\s*return terminationCAutoCreateArbSubscription_\(customerProfileId, paymentProfile.customerPaymentProfileId, oldSub, checkedEvidence.amount, startDate\);/g;
  const recovery=out['TerminationCAutoRecovery.gs'];
  if(typeof recovery!=='string'||!recovery.includes('FF_claimedTerminationCreate_')||[...recovery.matchAll(block)].length!==1){
    throw Error('Recovery guard or creation block changed');
  }
  out['TerminationCAutoRecovery.gs']=recovery.replace(block,
    'return FF_recoverTerminationOnRender_(target.rowNumber, oldSub, txId, terminationCAutoMoney_(checkedEvidence.amount), startDate);');
  for(const name of ['terminationCAutoCreatePaymentProfileFromTransaction_','terminationCAutoCreateArbSubscription_']){
    replace('TerminationCAutoRecovery.gs',name,'function '+name+'() {\n  throw new Error("Direct provider creation retired; use the guarded Render recovery entry");\n}');
  }
  for(const file of ['Code.gs','webhook.gs']){
    replace(file,'doPost','function doPost() {\n  throw new Error("Legacy Google payment webhook retired; use the verified Render webhook");\n}');
    replace(file,'computeAuthNetSignature_','function computeAuthNetSignature_() {\n  throw new Error("Payment signature verification is owned by Render");\n}');
  }
  replace('testWebhook.gs','testWebhook_doPost','function testWebhook_doPost() {\n  throw new Error("Legacy production webhook test retired; use isolated synthetic tests");\n}');
  replace('subscriptionOps.gs','authNetCancelSubscription_LIVE_DISABLED_',
    'function authNetCancelSubscription_LIVE_DISABLED_() {\n  throw new Error("Direct cancellation retired; use the guarded Render cancellation entry");\n}');
  for(const file of Object.keys(out)){
    let text=out[file];
    // Remove only named legacy credential declarations; do not touch unrelated
    // values or upload these source strings to logs/errors/review comments.
    text=text.replace(/^\s*(?:const|let|var)\s+(?:AUTHNET_API_LOGIN_ID|AUTHNET_TRANSACTION_KEY|AUTHNET_SIGNATURE_KEY_HEX|API_LOGIN_ID|TRANSACTION_KEY)\s*=\s*[^;\n]+;\s*$/gm,'');
    text=text.replace(/\s*merchantAuthentication:\s*\{\s*name:\s*AUTHNET_API_LOGIN_ID,\s*transactionKey:\s*AUTHNET_TRANSACTION_KEY,?\s*\},?/g,'');
    text=text.replace(/\s*merchantAuthentication:\s*(?:activeWorkflowMerchantAuthentication_|directAuthNetMerchantAuthentication_)\(\),?/g,'');
    for(const name of ['activeWorkflowMerchantAuthentication_','directAuthNetMerchantAuthentication_']){
      if(new RegExp('^function '+name+'\\s*\\(','m').test(text))text=replaceFunction(text,name,
        'function '+name+'() {\n  throw new Error("Google payment credentials retired");\n}');
    }
    if(/\b(?:AUTHNET_API_LOGIN_ID|AUTHNET_TRANSACTION_KEY|AUTHNET_SIGNATURE_KEY_HEX|API_LOGIN_ID|TRANSACTION_KEY)\b/.test(text)){
      throw Error('Unmigrated credential reference in '+file);
    }
    if(/https:\/\/(?:api2?|apitest)\.authorize\.net\/(?:xml|rest|soap)/.test(text)){
      throw Error('Unmigrated direct provider transport in '+file);
    }
    out[file]=text;
  }
  return out;
}
export {prepareGooglePaymentMigration,replaceFunction};
