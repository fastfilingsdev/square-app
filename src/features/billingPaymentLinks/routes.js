const express = require('express');
const { getSheetsClient } = require('../../core/googleSheets');
const { escapeHtml } = require('../../core/html');
const { hasValidBillingAccess } = require('../billingRefunds/routes');
const {
  LINK_TYPE_CERTIFICATE_CANCELLATION,
  LINK_TYPE_MULTIPLE_ITEMS,
  LINK_TYPE_PAST_PERIOD_FILINGS,
  PAYMENT_LINK_HEADERS,
  buildHostedPaymentTransactionRequest,
  createHostedPaymentLinkSession,
  findPaymentLink,
  generatePaymentLinkId,
  getBillingSpreadsheetId,
  hostedPaymentSettings,
  paymentLinkReturnUrl,
  paymentLinkUrl,
  totalLineItems,
  updatePaymentLinkRow,
  validatePaymentLink
} = require('./paymentLinks');

function noStore(res) {
  res.set({
    'Cache-Control': 'no-store, max-age=0',
    Pragma: 'no-cache',
    'X-Robots-Tag': 'noindex, nofollow'
  });
}

function renderItems(items) {
  return items.map(item => {
    const qty = Number(item.quantity || 1);
    const amount = Number(item.amount || 0);
    const total = qty * amount;
    return `<li><span>${escapeHtml(item.name)}</span><strong>$${total.toFixed(2).replace(/\.00$/, '')}</strong></li>`;
  }).join('');
}

function renderPaymentLinkHtml({ link, testOnly = false }) {
  const amount = totalLineItems(link.items);
  const safeAuthorization = escapeHtml(link.authorizationText || 'I authorize Fast Filings to process this payment for the service item(s) listed on this page.');

  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta name="robots" content="noindex,nofollow" />
  <title>Fast Filings Secure Payment Link</title>
  <style>
    :root { color-scheme: light; --ink:#0f172a; --muted:#64748b; --line:#dbe4ef; --blue:#1d4ed8; --teal:#0f766e; --green:#047857; }
    * { box-sizing:border-box; }
    body { margin:0; min-height:100vh; font-family:Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color:var(--ink); background:linear-gradient(145deg, #0f172a 0%, #1d4ed8 52%, #0f766e 100%); padding:16px; display:grid; place-items:center; }
    main { width:min(100%, 760px); }
    .card { background:rgba(255,255,255,.98); border:1px solid rgba(255,255,255,.75); border-radius:26px; padding:28px; box-shadow:0 24px 70px rgba(15,23,42,.24); }
    .brand-row { display:flex; align-items:center; justify-content:space-between; gap:16px; flex-wrap:wrap; margin:0 0 18px; }
    .ff-logo { display:block; width:190px; max-width:58vw; height:auto; }
    .authnet-badge { display:inline-flex; align-items:center; gap:9px; padding:9px 12px; border:1px solid rgba(31,95,153,.18); background:rgba(255,255,255,.88); border-radius:999px; box-shadow:0 8px 24px rgba(16,32,51,.08); color:#52627a; font-size:12px; font-weight:850; white-space:nowrap; }
    .authnet-logo { display:block; width:132px; max-width:36vw; height:auto; }
    h1 { margin:0 0 8px; font-size:clamp(32px, 6vw, 50px); line-height:1.02; letter-spacing:-.05em; }
    .intro { margin:0 0 18px; color:#475569; font-size:16px; line-height:1.5; }
    .amount { display:flex; align-items:flex-end; justify-content:space-between; gap:16px; flex-wrap:wrap; padding:18px 0; border-top:1px solid var(--line); border-bottom:1px solid var(--line); margin:16px 0; }
    .amount p { margin:0; }
    .label { color:var(--muted); font-size:12px; font-weight:900; letter-spacing:.08em; text-transform:uppercase; }
    .price { font-size:48px; line-height:.95; font-weight:950; letter-spacing:-.05em; }
    ul { list-style:none; padding:0; margin:12px 0 0; border:1px solid var(--line); border-radius:18px; overflow:hidden; }
    li { display:flex; justify-content:space-between; gap:16px; padding:13px 15px; border-top:1px solid var(--line); color:#334155; }
    li:first-child { border-top:0; }
    .consent { display:flex; gap:11px; align-items:flex-start; margin:14px 0; color:#0f172a; background:#fff; border:1px solid #cbd5e1; border-radius:16px; padding:13px; font-size:14px; line-height:1.4; cursor:pointer; }
    .consent input { width:20px; height:20px; margin:0; flex:0 0 auto; accent-color:var(--blue); }
    button { width:100%; border:0; border-radius:16px; padding:15px 16px; color:white; background:linear-gradient(135deg, #111827, #1d4ed8); font-size:16px; font-weight:950; cursor:pointer; box-shadow:0 14px 26px rgba(29,78,216,.25); }
    button:disabled { cursor:wait; opacity:.72; }
    .status-line { min-height:18px; margin:9px 0 0; color:#52627a; font-size:12px; line-height:1.35; }
    .status-line.error { color:#b42318; }
    .fine { margin:12px 0 0; color:var(--muted); font-size:12px; line-height:1.45; text-align:center; }
    @media (max-width:520px) { .brand-row { align-items:flex-start; } .ff-logo { width:160px; } .authnet-badge { width:100%; justify-content:space-between; } }
  </style>
</head>
<body>
<main>
  <section class="card" aria-label="Fast Filings secure payment link">
    <div class="brand-row"><img class="ff-logo" src="/assets/payment-update/fast-filings-logo.png" alt="Fast Filings" /><div class="authnet-badge" aria-label="Secured by Authorize.Net"><span>Secured by</span><img class="authnet-logo" src="/assets/payment-update/authorize-net-logo.svg" alt="Authorize.Net" /></div></div>
    <h1>Complete your payment securely.</h1>
    <div class="amount"><div><p class="label">Amount due now</p><p class="price">$${amount.replace(/\.00$/, '')}</p></div><div><p class="label">Payment items</p><p>${link.items.length} item${link.items.length === 1 ? '' : 's'}</p></div></div>
    <ul aria-label="Payment item list">${renderItems(link.items)}</ul>
    <form id="authnet-session-form">
      <label class="consent"><input id="authorization-checkbox" type="checkbox" required /><span>${safeAuthorization}</span></label>
      <button id="continue-button" type="submit">Continue to secure checkout</button>
      <div id="status-line" class="status-line" aria-live="polite"></div>
    </form>
    <p class="fine">Checkout is hosted by Authorize.Net. Fast Filings verifies approved payments before marking the service as paid/completed.</p>
  </section>
</main>
<script>
(function () {
  const form = document.getElementById('authnet-session-form');
  const button = document.getElementById('continue-button');
  const statusLine = document.getElementById('status-line');
  const checkbox = document.getElementById('authorization-checkbox');
  function setStatus(message, isError) { statusLine.textContent = message || ''; statusLine.classList.toggle('error', Boolean(isError)); }
  function appendHidden(targetForm, name, value) { if (value == null || value === '') return; const input = document.createElement('input'); input.type = 'hidden'; input.name = name; input.value = value; targetForm.appendChild(input); }
  form.addEventListener('submit', async function (event) {
    event.preventDefault();
    if (!checkbox.checked) { setStatus('Please check the authorization box before continuing.', true); return; }
    button.disabled = true; button.textContent = 'Opening secure Authorize.Net checkout…'; setStatus('Creating a one-time secure checkout session…', false);
    try {
      const currentPath = window.location.pathname.endsWith('/') ? window.location.pathname.slice(0, -1) : window.location.pathname;
      const response = await fetch(currentPath + '/session', { method: 'POST', headers: { Accept: 'application/json', 'Content-Type': 'application/json' }, cache: 'no-store', body: JSON.stringify({ authorizationAccepted: true }) });
      const payload = await response.json().catch(function () { return {}; });
      if (!response.ok || !payload.ok) throw new Error(payload.error || 'Unable to create the secure Authorize.Net checkout session.');
      const formActionUrl = payload.formActionUrl || payload.acceptHostedPaymentUrl;
      if (!formActionUrl || !payload.hostedToken) throw new Error('Authorize.Net did not return a complete hosted checkout session.');
      const authnetForm = document.createElement('form'); authnetForm.method = 'post'; authnetForm.action = formActionUrl; appendHidden(authnetForm, 'token', payload.hostedToken); document.body.appendChild(authnetForm); authnetForm.submit();
    } catch (err) {
      button.disabled = false; button.textContent = 'Continue to secure checkout'; setStatus(err.message || 'Unable to open secure checkout. Please try again.', true);
    }
  });
})();
</script>
</body>
</html>`;
}

async function renderPaymentLink(req, res, { testOnly }) {
  try {
    const linkId = String(req.params.linkId || '').trim();
    const sheets = await getSheetsClient();
    const link = validatePaymentLink(await findPaymentLink(sheets, linkId), { testOnly });
    noStore(res);
    res.set({ 'Content-Type': 'text/html; charset=utf-8' });
    return res.status(200).send(renderPaymentLinkHtml({ link, testOnly }));
  } catch (err) {
    console.error(testOnly ? 'FF BILLING TEST PAYMENT LINK ERROR:' : 'FF BILLING PAYMENT LINK ERROR:', err.message);
    noStore(res);
    return res.status(err.statusCode || 500).send(`Payment link error: ${escapeHtml(err.message)}`);
  }
}

async function createSession(req, res, { testOnly }) {
  try {
    const linkId = String(req.params.linkId || '').trim();
    const sheets = await getSheetsClient();
    const link = validatePaymentLink(await findPaymentLink(sheets, linkId), { testOnly });
    const result = await createHostedPaymentLinkSession({ req, sheets, link, authorizationAccepted: req.body?.authorizationAccepted === true, testOnly });
    noStore(res);
    return res.status(200).json(result);
  } catch (err) {
    console.error(testOnly ? 'FF BILLING TEST PAYMENT LINK SESSION ERROR:' : 'FF BILLING PAYMENT LINK SESSION ERROR:', err.message);
    noStore(res);
    return res.status(err.statusCode || 500).json({ ok: false, error: err.message || 'Unable to create secure checkout session' });
  }
}

async function returnFromCheckout(req, res) {
  try {
    const linkId = String(req.params.linkId || '').trim();
    const sheets = await getSheetsClient();
    const link = await findPaymentLink(sheets, linkId);
    await updatePaymentLinkRow(sheets, link, { 'Checkout Returned At': new Date().toISOString() });
  } catch (err) {
    console.error('FF BILLING PAYMENT LINK RETURN STAMP ERROR:', err.message);
  }

  noStore(res);
  res.set({ 'Content-Type': 'text/html; charset=utf-8' });
  return res.status(200).send(`<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Payment Submitted | Fast Filings</title><style>body{font-family:Inter,system-ui,sans-serif;background:#f8fafc;color:#102033;min-height:100vh;display:grid;place-items:center;margin:0;padding:24px}main{max-width:620px;background:#fff;border:1px solid #dbe4ef;border-radius:24px;padding:34px;box-shadow:0 20px 60px rgba(16,32,51,.12)}h1{margin:0 0 12px;font-size:38px;letter-spacing:-.04em}p{color:#607089;line-height:1.6;font-size:17px}</style></head><body><main><h1>Payment submitted.</h1><p>If you completed the secure Authorize.Net checkout, Fast Filings will verify the approved payment before marking this payment link as paid/completed.</p><p>You can safely close this page.</p></main></body></html>`);
}

function createBillingPaymentLinksRouter() {
  const router = express.Router();

  router.get('/payment-links/health', (req, res) => {
    res.json({
      ok: true,
      route: '/billing/payment-links',
      billingSpreadsheetConfigured: Boolean(getBillingSpreadsheetId()),
      authnetConfigured: Boolean(process.env.AUTHNET_API_LOGIN_ID && process.env.AUTHNET_TRANSACTION_KEY),
      rawTokensStored: false,
      customerEmailsSentByRoute: false,
      authNetMutationBeforeCustomerSubmit: false,
      supportedLinkTypes: [LINK_TYPE_CERTIFICATE_CANCELLATION, LINK_TYPE_PAST_PERIOD_FILINGS, LINK_TYPE_MULTIPLE_ITEMS],
      canonicalHeaders: PAYMENT_LINK_HEADERS,
      safety: 'Payment links are sheet-backed. Hosted Authorize.Net tokens are generated just-in-time after the customer checks the authorization box; no raw tokens/card data are stored and no customer emails are sent by this route.'
    });
  });

  router.post('/payment-links/prepare', async (req, res) => {
    if (!await hasValidBillingAccess(req, res)) return;
    const live = req.body?.live !== false;
    const linkId = req.body?.linkId || generatePaymentLinkId({ live });
    res.json({
      ok: true,
      linkId,
      paymentLink: paymentLinkUrl(req, linkId, { testOnly: !live }),
      returnUrl: paymentLinkReturnUrl(req, linkId),
      note: 'Prepare helper returns IDs/URLs only. Sheet row writing is owned by Apps Script or local API tooling.'
    });
  });

  router.get('/payment-links/return/:linkId', returnFromCheckout);
  router.get('/payment-links/test/:linkId', (req, res) => renderPaymentLink(req, res, { testOnly: true }));
  router.post('/payment-links/test/:linkId/session', (req, res) => createSession(req, res, { testOnly: true }));
  router.get('/payment-links/:linkId', (req, res) => renderPaymentLink(req, res, { testOnly: false }));
  router.post('/payment-links/:linkId/session', (req, res) => createSession(req, res, { testOnly: false }));

  return router;
}

module.exports = {
  createBillingPaymentLinksRouter,
  __billingPaymentLinksTestHooks: {
    buildHostedPaymentTransactionRequest,
    hostedPaymentSettings,
    renderPaymentLinkHtml,
    totalLineItems
  }
};
