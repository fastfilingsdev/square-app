'use strict';
const {getSheetsClient}=require('../../core/googleSheets');
// No spreadsheet ID fallback: configure the actual Subscriptions workbook.
function createGooglePaymentContext({env=process.env,sheetsClient=getSheetsClient}={}){
  async function values(range){
    const spreadsheetId=env.FF_SUBSCRIPTIONS_SPREADSHEET_ID||env.SUBSCRIPTIONS_SPREADSHEET_ID;
    if(!spreadsheetId||!/^[a-zA-Z0-9_-]{20,100}$/.test(spreadsheetId))throw Error('Subscriptions workbook not configured');
    const sheets=await sheetsClient();
    const result=await sheets.spreadsheets.values.get({spreadsheetId,range,valueRenderOption:'FORMATTED_VALUE'});
    return result.data.values||[];
  }
  function mapRow(headers,row,required){
    const names=headers.map(x=>String(x).trim());
    if(new Set(names.filter(Boolean)).size!==names.filter(Boolean).length||required.some(x=>!names.includes(x))){
      throw Error('Payment workbook schema mismatch');
    }
    return Object.fromEntries(names.filter(Boolean).map(name=>[name,row[names.indexOf(name)]??'']));
  }
  return {
    async recovery(rowNumber){
      if(!Number.isInteger(rowNumber)||rowNumber<2||rowNumber>10000)throw Error('Invalid recovery row');
      const header=(await values("'Payment Update'!A1:W1"))[0]||[];
      const row=mapRow(header,(await values(`'Payment Update'!A${rowNumber}:W${rowNumber}`))[0]||[],
        ['Payment Update Type','Customer ID','Email','Subscription ID','Payment Update Status','Stop / Suppressed']);
      const active=await values("'Active Subscriptions'!A1:AZ10000");
      if(active.length>=10000||!active.length)throw Error('Active membership scan incomplete');
      const headers=active[0];mapRow(headers,[],['Email','Subscription ID']);
      const activeEmails=active.slice(1).filter(r=>r.some(x=>String(x).trim())).map(r=>{
        const item=mapRow(headers,r,['Email','Subscription ID']);
        if(!String(item.Email).trim()||!String(item['Subscription ID']).trim())throw Error('Active membership identity incomplete');
        return item.Email;
      });
      return {row,activeEmails,complete:true};
    },
    async cancellation(rowNumber){
      if(!Number.isInteger(rowNumber)||rowNumber<2||rowNumber>10000)throw Error('Invalid cancellation row');
      return mapRow((await values("'Cancellations'!A1:N1"))[0]||[],
        (await values(`'Cancellations'!A${rowNumber}:N${rowNumber}`))[0]||[],
        ['Customer Name','Email','Subscription ID','Cancel Requested At','Processed At','Auth.Net Result']);
    }
  };
}
module.exports={createGooglePaymentContext};
