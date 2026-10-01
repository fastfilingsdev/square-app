/** Candidate adapter. No payment credentials, no direct-provider fallback. */
function FF_paymentReadRequest_(operation, parameters) {
  const allowed = ['ARBGetSubscriptionListRequest', 'ARBGetSubscriptionRequest',
    'getUnsettledTransactionListRequest', 'getSettledBatchListRequest',
    'getTransactionListRequest', 'getTransactionDetailsRequest', 'getCustomerProfileRequest'];
  if (allowed.indexOf(operation) < 0 || !parameters || typeof parameters !== 'object' || Array.isArray(parameters)) {
    throw new Error('Unsupported payment read request');
  }
  if (Object.prototype.hasOwnProperty.call(parameters, 'merchantAuthentication')) {
    throw new Error('Payment credentials must not be supplied by Google');
  }
  // Existing reporting callers use the strings 'true'/'false'; the backend
  // deliberately accepts only booleans. Normalize only that known field and
  // leave the caller's original object untouched.
  const normalized = JSON.parse(JSON.stringify(parameters));
  if (normalized.sorting && (normalized.sorting.orderDescending === 'true' || normalized.sorting.orderDescending === 'false')) {
    normalized.sorting.orderDescending = normalized.sorting.orderDescending === 'true';
  }
  return {
    url: 'https://fastfilings-api.onrender.com/google-payments/read',
    method: 'post', contentType: 'application/json',
    headers: { Authorization: 'Bearer ' + ScriptApp.getOAuthToken() },
    payload: JSON.stringify({ operation: operation, parameters: normalized }),
    muteHttpExceptions: true, followRedirects: false
  };
}

function FF_paymentReadResponse_(response) {
  // Never expose an HTML login page or raw provider/transport error in logs.
  if (response.getResponseCode() !== 200) throw new Error('Render payment read blocked or unavailable; no automatic retry');
  let data;
  try { data = JSON.parse(response.getContentText().replace(/^\uFEFF/, '')); }
  catch (_) { throw new Error('Invalid Render payment read response'); }
  if (!data || !data.messages || data.messages.resultCode !== 'Ok') {
    throw new Error('Payment provider read failed; no automatic retry');
  }
  return data;
}

function FF_paymentRead_(operation, parameters) {
  const request = FF_paymentReadRequest_(operation, parameters);
  return FF_paymentReadResponse_(UrlFetchApp.fetch(request.url, request));
}

/** Ordered results for existing subscription-detail fetchAll consumers. */
function FF_paymentReadBatch_(requests) {
  if (!Array.isArray(requests) || requests.length > 1000) throw new Error('Invalid payment read batch');
  const results = [];
  for (let start = 0; start < requests.length; start += 4) {
    const chunk = requests.slice(start, start + 4);
    const fetches = chunk.map(function (r) { return FF_paymentReadRequest_(r.operation, r.parameters); });
    const responses = UrlFetchApp.fetchAll(fetches);
    if (!responses || responses.length !== chunk.length) throw new Error('Incomplete payment read batch; no automatic retry');
    responses.forEach(function (response) {
      try { results.push({ data: FF_paymentReadResponse_(response) }); }
      catch (_) { results.push({ error: 'Payment read failed; no automatic retry' }); }
    });
  }
  return results;
}
