// Candidate replacements for existing functions. Do not append duplicates.
// Install only after the Render gateway and all request builders are migrated.
// Request builders must omit merchantAuthentication entirely. Legacy credentials
// cause a local rejection rather than transmission to the backend.
function FF_paymentReadPayload_(payload) {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) throw new Error('Invalid payment read payload');
  const operations = Object.keys(payload);
  if (operations.length !== 1) throw new Error('Exactly one payment read is required');
  return FF_paymentRead_(operations[0], payload[operations[0]]);
}

function activeWorkflowAuthNetPost_(payload) {
  return FF_paymentReadPayload_(payload);
}

function terminationCAuthNetPost_(payload) {
  return FF_paymentReadPayload_(payload);
}

function directAuthNetRequest_(payload) {
  return FF_paymentReadPayload_(payload);
}

function activeWorkflowFetchSubscriptionDetails_(subscriptionIds) {
  const uniqueIds = [];
  const seen = Object.create(null);
  subscriptionIds.forEach(function (id) {
    const sid = String(id || '').trim();
    if (!sid || seen[sid]) return;
    if (!/^[1-9][0-9]{0,29}$/.test(sid)) throw new Error('Invalid subscription ID');
    seen[sid] = true;
    uniqueIds.push(sid);
  });
  const out = {};
  // Preserve the existing outer batch size and pacing; the shared adapter
  // further bounds simultaneous network requests and returns ordered results.
  for (let start = 0; start < uniqueIds.length; start += ACTIVE_SUBSCRIPTIONS_DETAIL_BATCH_SIZE) {
    const chunk = uniqueIds.slice(start, start + ACTIVE_SUBSCRIPTIONS_DETAIL_BATCH_SIZE);
    const responses = FF_paymentReadBatch_(chunk.map(function (sid) {
      return { operation: 'ARBGetSubscriptionRequest', parameters: { subscriptionId: sid, includeTransactions: true } };
    }));
    if (responses.length !== chunk.length) throw new Error('Incomplete subscription detail read');
    responses.forEach(function (response, index) { out[chunk[index]] = response; });
    Utilities.sleep(150);
  }
  return out;
}

function fetchAuthNetTransactionDetailsForWebhook_(transId) {
  const id = String(transId || '').trim();
  if (!id) return {};
  try {
    const data = FF_paymentRead_('getTransactionDetailsRequest', { transId: id });
    return data.transaction || {};
  } catch (_) {
    Logger.log('Transaction detail lookup failed; no automatic retry');
    return {};
  }
}
