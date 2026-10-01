'use strict';
const express=require('express');
const {verifyGoogleAccessToken}=require('../billingRefunds/routes');
const {createRecoveryProcessor}=require('./recovery');
const {createProviderTransport}=require('./provider');
const {createGooglePaymentContext}=require('./context');
const {createCancellationProcessor}=require('./cancellation');

function createRecoveryHandler({env=process.env,verify=verifyGoogleAccessToken,ledger,providerScope,
  readContext,provider,kind='recovery'}={}){
  const cancellation=kind==='cancellation';
  const flag=cancellation?'FF_GOOGLE_CANCELLATIONS_ENABLED':'FF_GOOGLE_RECOVERY_ENABLED';
  const context=readContext||createGooglePaymentContext({env})[cancellation?'cancellation':'recovery'];
  const processRecovery=(cancellation?createCancellationProcessor:createRecoveryProcessor)({env,ledger,providerScope,readContext:context,
    provider:provider||createProviderTransport({env})});
  let inFlight=0;
  return async({authorization,body})=>{
    if(env[flag]!=='true')return {status:503,body:{ok:false,error:'Payment operation disabled',operationStarted:false}};
    if(inFlight>=4)return {status:503,body:{ok:false,error:'Payment operation capacity reached',operationStarted:false}};
    inFlight++;
    const token=typeof authorization==='string'&&/^Bearer ([^\s]{1,4096})$/i.exec(authorization);
    try{
      const user=token&&await verify(token[1]);
      if(user?.ok!==true||user.verified!==true||user.email!=='returns@fastfilings.com'){
        return {status:401,body:{ok:false,error:'Unauthorized',operationStarted:false}};
      }
      const receipt=await processRecovery(body);
      return {status:200,body:{ok:true,...receipt}};
    }catch(_){
      // Do not imply that no mutation happened: profile or ARB acknowledgement
      // may have been lost. Keep durable hold and never suggest a blind retry.
      return {status:409,body:{ok:false,error:'Payment operation not confirmed; review required',retryAutomatically:false}};
    }finally{inFlight--;}
  };
}
function createGooglePaymentMutationsRouter(options={}){
  const router=express.Router();
  const recover=createRecoveryHandler(options);
  const cancel=createRecoveryHandler({...options,ledger:options.cancellationLedger,kind:'cancellation'});
  router.post('/recover-terminated',express.json({limit:'4kb'}),async(req,res)=>{
    res.set({'Cache-Control':'no-store',Pragma:'no-cache'});
    const result=await recover({authorization:req.get('authorization'),body:req.body});
    return res.status(result.status).json(result.body);
  });
  router.post('/cancel-subscription',express.json({limit:'4kb'}),async(req,res)=>{
    res.set({'Cache-Control':'no-store',Pragma:'no-cache'});
    const result=await cancel({authorization:req.get('authorization'),body:req.body});
    return res.status(result.status).json(result.body);
  });
  return router;
}
module.exports={createRecoveryHandler,createGooglePaymentMutationsRouter};
