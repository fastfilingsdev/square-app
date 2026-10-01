// Candidate replacement; requires consolidated sync returning {success:true}.
// Default disabled. Never deploy separately from the signed backend caller.
function doGet() { return ffSqWebhookOutput_({ success: false, error: 'POST required' }); }
function doPost(e) {
  let lock;
  try {
    const properties = PropertiesService.getScriptProperties();
    const secret = properties.getProperty('SQ_CUSTOMER_SYNC_SECRET') || '';
    if (secret.length < 32 || !e || !e.postData || e.postData.contents.length > 2048) throw new Error();
    const body = JSON.parse(e.postData.contents);
    if (!['syncCustomers', 'verifyConnection'].includes(body.action) || !Number.isSafeInteger(body.timestamp) || Math.abs(Date.now() - body.timestamp) > 60000 || typeof body.nonce !== 'string' || typeof body.signature !== 'string' || !/^[a-f0-9-]{36}$/.test(body.nonce) || !/^[a-f0-9]{64}$/.test(body.signature)) throw new Error();
    if (body.action === 'syncCustomers' && properties.getProperty('SQ_CUSTOMER_SYNC_ENABLED') !== 'true') throw new Error();
    const signed = [body.action, body.timestamp, body.nonce].join('\n');
    const expected = Utilities.computeHmacSha256Signature(signed, secret).map(function (v) { return ('0' + ((v + 256) % 256).toString(16)).slice(-2); }).join('');
    let difference = 0;
    for (let i = 0; i < 64; i++) difference |= expected.charCodeAt(i) ^ body.signature.charCodeAt(i);
    if (difference !== 0) throw new Error();
    lock = LockService.getScriptLock();
    if (!lock.tryLock(1000)) throw new Error();
    const key = 'FF_SQ_NONCE_' + body.nonce;
    if (properties.getProperty(key)) throw new Error();
    const all = properties.getProperties();
    let retained = 0;
    Object.keys(all).filter(function (k) { return k.indexOf('FF_SQ_NONCE_') === 0; }).forEach(function (k) {
      if (Number(all[k]) < Date.now() - 120000) properties.deleteProperty(k); else retained++;
    });
    if (retained >= 100) throw new Error();
    // Claim before invoking any writes; a failure leaves the nonce consumed.
    properties.setProperty(key, String(Date.now()));
    lock.releaseLock(); lock = null;
    // Same authentication/nonce path, but never enter customer-sync code.
    if (body.action === 'verifyConnection') return ffSqWebhookOutput_({ success: true, connectionVerified: true, customerWrites: 0 });
    const result = syncConnectedCustomersToSQ();
    if (!result || result.success !== true) throw new Error();
    return ffSqWebhookOutput_({ success: true });
  } catch (_) {
    return ffSqWebhookOutput_({ success: false, error: 'Sync unconfirmed; reconcile before rerun' });
  } finally { if (lock) lock.releaseLock(); }
}
function ffSqWebhookOutput_(value) {
  return ContentService.createTextOutput(JSON.stringify(value)).setMimeType(ContentService.MimeType.JSON);
}
