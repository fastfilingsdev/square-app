'use strict';
const axios=require('axios');
const ALLOWED=new Set(['getTransactionDetailsRequest','ARBGetSubscriptionRequest','getCustomerProfileRequest',
  'createCustomerProfileFromTransactionRequest','ARBCreateSubscriptionRequest','ARBCancelSubscriptionRequest']);
// Internal only: parameters are built by validated handlers, never forwarded from
// a public generic mutation endpoint. Render remains the sole credential owner.
function createProviderTransport({env=process.env,post=axios.post}={}){
  return async(operation,parameters)=>{
    if(!ALLOWED.has(operation)||!env.AUTHNET_API_LOGIN_ID||!env.AUTHNET_TRANSACTION_KEY||
      Object.hasOwn(parameters,'merchantAuthentication'))throw Error('Provider operation unavailable');
    try{
      const response=await post('https://api2.authorize.net/xml/v1/request.api',{
        [operation]:{merchantAuthentication:{name:env.AUTHNET_API_LOGIN_ID,transactionKey:env.AUTHNET_TRANSACTION_KEY},...parameters}
      },{timeout:45000,maxRedirects:0,maxContentLength:4*1024*1024,maxBodyLength:16384,headers:{'Content-Type':'application/json'}});
      if(response.status!==200||response.data?.messages?.resultCode!=='Ok')throw Error('Unconfirmed response');
      return response.data;
    }catch(_){throw Error('Provider operation unconfirmed; no automatic retry');}
  };
}
module.exports={createProviderTransport};
