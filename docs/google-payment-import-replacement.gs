// Preserves the existing reporting columns. Collect and validate every page
// before clearing the previous report; no provider secrets or direct API calls.
function fetchAuthorizeNetTransactions() {
  if (!Number.isInteger(LOOKBACK_DAYS) || LOOKBACK_DAYS < 1 || LOOKBACK_DAYS > 31) throw new Error('Invalid reporting lookback');
  var end = new Date(), start = new Date(end.getTime() - LOOKBACK_DAYS * 86400000);
  var batchData = FF_paymentRead_('getSettledBatchListRequest', {
    firstSettlementDate: start.toISOString(), lastSettlementDate: end.toISOString()
  });
  var batches = batchData.batchList || [];
  if (!Array.isArray(batches)) throw new Error('Invalid settled batch response');
  if (!batches.length) return;
  var allRows = [], seen = Object.create(null);
  batches.forEach(function (batch) {
    if (!/^[1-9][0-9]{0,29}$/.test(String(batch.batchId || ''))) throw new Error('Invalid batch ID');
    var complete = false;
    for (var page = 1; page <= 100; page++) {
      var data = FF_paymentRead_('getTransactionListRequest', {
        batchId: String(batch.batchId), sorting: {orderBy: 'submitTimeUTC', orderDescending: true},
        paging: {limit: 1000, offset: page}
      });
      var txns = Array.isArray(data.transactions) ? data.transactions :
        data.transactions && Array.isArray(data.transactions.transaction) ? data.transactions.transaction :
        Array.isArray(data.transactionList) ? data.transactionList :
        data.transactionList && Array.isArray(data.transactionList.transaction) ? data.transactionList.transaction : null;
      if (!txns) {
        if (Number(data.totalNumInResultSet) === 0) txns = [];
        else throw new Error('Missing transaction list; previous report preserved');
      }
      txns.forEach(function (tx) {
        var id = String(tx.transId || '');
        if (!/^[1-9][0-9]{0,29}$/.test(id) || seen[id]) throw new Error('Duplicate or missing transaction ID; report not replaced');
        seen[id] = true;
        allRows.push([end, id, tx.firstName || (tx.customer && tx.customer.firstName) || '',
          tx.lastName || (tx.customer && tx.customer.lastName) || '',
          (tx.customer && (tx.customer.email || tx.customer.emailAddress)) || '',
          tx.settleAmount || tx.authAmount || '', tx.transactionStatus || '', tx.responseCode || '',
          tx.submitTimeUTC || '', tx.authCode || '', (tx.order && tx.order.invoiceNumber) || tx.invoiceNumber || '',
          (tx.subscription && tx.subscription.id) || tx.subscriptionId || '', JSON.stringify(tx).substring(0, 4000)]);
      });
      if (txns.length < 1000) { complete = true; break; }
    }
    if (!complete) throw new Error('Transaction scan exceeded bound; previous report preserved');
  });
  if (!allRows.length) return;
  allRows.sort(function (a, b) { return new Date(b[8]).getTime() - new Date(a[8]).getTime(); });
  var sheet = getBillingSheet_(true);
  sheet.getRange(2, 1, allRows.length, allRows[0].length).setValues(allRows);
  Logger.log('Imported ' + allRows.length + ' transactions through Render.');
}
