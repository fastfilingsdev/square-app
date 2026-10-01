// Replace the provider-creation block in terminationCAutoRecoverOne_ with this
// call, retaining its existing local persistent claim and sheet journaling.
// Do not install before the shared Render ledger and handler are verified.
function FF_recoverTerminationOnRender_(rowNumber, oldSubscriptionId, transactionId, amount, startDate) {
  var response = UrlFetchApp.fetch('https://fastfilings-api.onrender.com/google-payments/recover-terminated', {
    method: 'post', contentType: 'application/json', followRedirects: false,
    muteHttpExceptions: true,
    headers: {Authorization: 'Bearer ' + ScriptApp.getOAuthToken()},
    payload: JSON.stringify({rowNumber: rowNumber, oldSubscriptionId: String(oldSubscriptionId),
      transactionId: String(transactionId), amount: String(amount), startDate: String(startDate)})
  });
  if (response.getResponseCode() !== 200) throw new Error('Render recovery not confirmed; keep claim and review. Do not retry.');
  var data = JSON.parse(response.getContentText());
  if (data.ok !== true || !/^[1-9][0-9]{0,29}$/.test(String(data.subscriptionId || ''))) {
    throw new Error('Render recovery receipt unconfirmed; keep claim and review.');
  }
  return {subscriptionId: String(data.subscriptionId), replayed: data.replayed === true};
}
