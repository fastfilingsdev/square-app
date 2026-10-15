'use strict';
// Owner-authorized retirement of the legacy billing backend only.
// No provider calls, sheet writes, subscription changes, or email delivery.
const prefixes=['/payment-update','/billing','/subscriptions','/google-payments','/authnet','/clover/hosted-checkout'];
function legacyBillingRetired(req,res,next){
 const path=req.path.toLowerCase();
 if(!prefixes.some(p=>path===p||path.startsWith(p+'/')))return next();
 res.set({'Cache-Control':'no-store, max-age=0','Referrer-Policy':'no-referrer','X-Robots-Tag':'noindex, nofollow'});
 const status={ok:true,legacyBillingRetired:true,billingTimersEnabled:false,hostedSessionsEnabled:false,subscriptionMutationsEnabled:false,customerEmailsEnabled:false};
 if((req.method==='GET'||req.method==='HEAD')&&(path.endsWith('/health')||path==='/payment-update/test'))return res.status(200).json(status);
 if(req.method!=='GET'&&req.method!=='HEAD')return res.status(410).json({ok:false,code:'legacy_billing_retired',error:'Legacy billing is no longer available.'});
 return res.status(410).type('html').send('<!doctype html><html lang="en"><meta charset="utf-8"><title>Link retired | Fast Filings</title><body><h1>This billing link is no longer available.</h1><p>Please contact returns@fastfilings.com. Do not submit another payment using an old link.</p><p>This page does not confirm payment or change your membership.</p></body></html>');
}
module.exports={legacyBillingRetired};
