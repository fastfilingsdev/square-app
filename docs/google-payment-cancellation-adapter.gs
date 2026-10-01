// Replace directAuthNetCancelSubscription_ and pass request.row from its sole
// caller in processDirectCancellationRow_. Server re-reads the fixed workbook.
function directAuthNetCancelSubscription_(subscriptionId, rowNumber) {
  if (!Number.isInteger(rowNumber) || rowNumber < 2 || rowNumber > 10000) {
    throw new Error('Cancellation requires its verified sheet row');
  }
  var response = UrlFetchApp.fetch('https://fastfilings-api.onrender.com/google-payments/cancel-subscription', {
    method: 'post', contentType: 'application/json', followRedirects: false, muteHttpExceptions: true,
    headers: {Authorization: 'Bearer ' + ScriptApp.getOAuthToken()},
    payload: JSON.stringify({rowNumber: rowNumber, subscriptionId: String(subscriptionId)})
  });
  if (response.getResponseCode() !== 200) throw new Error('Cancellation not confirmed; review before any retry.');
  var data = JSON.parse(response.getContentText());
  if (data.ok !== true || String(data.subscriptionId) !== String(subscriptionId)) {
    throw new Error('Cancellation receipt does not match the requested subscription');
  }
  // Preserve the existing UI/journal response contract without returning raw
  // provider data, keys, customer identity or payment details to the sheet.
  return {messages: {resultCode: 'Ok', message: [{code: 'I00001',
    text: data.alreadyEnded ? 'Subscription already ended; no provider cancellation sent' :
      data.replayed ? 'Previously confirmed cancellation; no repeat request sent' : 'Cancellation confirmed'}]}};
}
