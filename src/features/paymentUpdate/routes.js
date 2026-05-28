const express = require('express');
const { getSheetsClient } = require('../../core/googleSheets');
const { escapeHtml } = require('../../core/html');
const {
  getAuthNetConfig,
  getHostedPaymentPageToken,
  getHostedProfilePageToken,
  getSubscription
} = require('../../connectors/authnet/client');
const {
  findPaymentUpdateTicket,
  getPaymentUpdateSpreadsheetId,
  updatePaymentUpdateTicketAudit,
  validatePaymentUpdateTicket
} = require('./tickets');

const PAYMENT_UPDATE_TYPE_A = 'SUB RECAPTURE A - New Order';
const PAYMENT_UPDATE_TYPE_B = 'SUB RECAPTURE B - Payment on Hold';
const PAYMENT_UPDATE_TYPE_C = 'SUB RECAPTURE C - Terminated';

function getBaseUrl(req) {
  const configured = process.env.PAYMENT_UPDATE_BASE_URL || '';
  if (configured) return configured.replace(/\/$/, '');

  const protocol = req.get('x-forwarded-proto') || req.protocol || 'https';
  return `${protocol}://${req.get('host')}`.replace(/\/$/, '');
}

function normalizePaymentUpdateType(value) {
  return String(value || '').trim().toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
}

function paymentFlowForTicket(ticket) {
  const normalized = normalizePaymentUpdateType(ticket.paymentUpdateType);
  if (normalized === 'sub recapture a new order' || normalized === 'new order') return 'new-order';
  if (normalized === 'sub recapture c terminated' || normalized === 'terminated') return 'terminated';
  return 'payment-update';
}

function quoteSheetName(title) {
  return String(title || '').replace(/'/g, "''");
}

function normalizeHeader(value) {
  return String(value || '').trim().toLowerCase().replace(/[^a-z0-9]+/g, '');
}

function firstNonEmpty(...values) {
  for (const value of values) {
    const text = String(value == null ? '' : value).trim();
    if (text) return text;
  }
  return '';
}

function splitFullName(fullName) {
  const parts = String(fullName == null ? '' : fullName).trim().split(/\s+/).filter(Boolean);
  if (!parts.length) return { firstName: '', lastName: '' };
  if (parts.length === 1) return { firstName: parts[0], lastName: '' };
  return { firstName: parts[0], lastName: parts.slice(1).join(' ') };
}

function valueByHeader(sourceRow, headerNames) {
  const names = Array.isArray(headerNames) ? headerNames : [headerNames];
  for (const name of names) {
    const value = sourceRow[normalizeHeader(name)];
    if (String(value || '').trim()) return String(value).trim();
  }
  return '';
}

function safeAmount(value) {
  const raw = String(value == null ? '' : value).trim();
  if (!raw) return '';
  const cleaned = raw.replace(/[$,]/g, '').match(/\d+(?:\.\d+)?/);
  if (!cleaned) return '';
  const n = Number(cleaned[0]);
  if (!Number.isFinite(n) || n <= 0) return '';
  return n.toFixed(2);
}

function displayAmount(amount) {
  const normalized = safeAmount(amount);
  if (!normalized) return '';
  return `$${normalized.replace(/\.00$/, '')}`;
}

function hostedReturnUrl(req, ticket, flow) {
  // Authorize.Net Accept Hosted can render a blank "Order Summary" page when
  // return/cancel URLs contain query strings. Keep the return context in path
  // segments instead of `?ticket=...&flow=...`.
  return `${getBaseUrl(req)}/payment-update/return/${encodeURIComponent(flow)}/${encodeURIComponent(ticket.ticketId)}`;
}

function paymentUpdateSettings(req, ticket) {
  const returnUrl = hostedReturnUrl(req, ticket, 'payment-update');
  return [
    { settingName: 'hostedProfileReturnUrl', settingValue: returnUrl },
    { settingName: 'hostedProfileReturnUrlText', settingValue: 'Return to Fast Filings' },
    { settingName: 'hostedProfilePageBorderVisible', settingValue: 'false' },
    { settingName: 'hostedProfileBillingAddressRequired', settingValue: 'true' },
    { settingName: 'hostedProfileCardCodeRequired', settingValue: 'true' }
  ];
}

function hostedPaymentSetting(settingName, settingValue) {
  return {
    settingName,
    settingValue: JSON.stringify(settingValue)
  };
}

function hostedPaymentSettings(req, ticket, amount, flow) {
  const returnUrl = hostedReturnUrl(req, ticket, flow);
  return [
    hostedPaymentSetting('hostedPaymentReturnOptions', {
      showReceipt: true,
      url: returnUrl,
      urlText: 'Return to Fast Filings',
      cancelUrl: returnUrl,
      cancelUrlText: 'Cancel'
    }),
    hostedPaymentSetting('hostedPaymentButtonOptions', { text: `Pay ${displayAmount(amount) || '$0.00'}` }),
    hostedPaymentSetting('hostedPaymentStyleOptions', { bgColor: '#1d4ed8' }),
    hostedPaymentSetting('hostedPaymentOrderOptions', { show: true, merchantName: 'Fast Filings' }),
    hostedPaymentSetting('hostedPaymentPaymentOptions', {
      cardCodeRequired: true,
      showCreditCard: true,
      showBankAccount: false
    }),
    hostedPaymentSetting('hostedPaymentBillingAddressOptions', { show: true, required: true }),
    hostedPaymentSetting('hostedPaymentCustomerOptions', {
      showEmail: true,
      requiredEmail: true,
      addPaymentProfile: false
    }),
    hostedPaymentSetting('hostedPaymentSecurityOptions', { captcha: false })
  ];
}

function paymentSessionScript({ buttonDefaultText = 'Continue to Authorize.Net' } = {}) {
  const safeButtonText = JSON.stringify(buttonDefaultText);
  return `<script>
      (function () {
        const form = document.getElementById('authnet-session-form');
        const button = document.getElementById('continue-button');
        const statusLine = document.getElementById('status-line');

        function setStatus(message, isError) {
          statusLine.textContent = message || '';
          statusLine.classList.toggle('error', Boolean(isError));
        }

        function appendHidden(targetForm, name, value) {
          if (value == null || value === '') return;
          const input = document.createElement('input');
          input.type = 'hidden';
          input.name = name;
          input.value = value;
          targetForm.appendChild(input);
        }

        form.addEventListener('submit', async function (event) {
          event.preventDefault();
          button.disabled = true;
          button.textContent = 'Opening secure Authorize.Net form…';
          setStatus('Creating a one-time secure session with Authorize.Net…', false);

          try {
            const currentPath = window.location.pathname.endsWith('/')
              ? window.location.pathname.slice(0, -1)
              : window.location.pathname;
            const sessionUrl = currentPath + '/session';
            const response = await fetch(sessionUrl, {
              method: 'POST',
              headers: { Accept: 'application/json' },
              cache: 'no-store'
            });
            const payload = await response.json().catch(function () { return {}; });
            if (!response.ok || !payload.ok) {
              throw new Error(payload.error || 'Unable to create the secure Authorize.Net session.');
            }

            const formActionUrl = payload.formActionUrl || payload.acceptEditPaymentUrl || payload.acceptHostedPaymentUrl;
            if (!formActionUrl || !payload.hostedToken) {
              throw new Error('Authorize.Net did not return a complete hosted session.');
            }

            const authnetForm = document.createElement('form');
            authnetForm.method = 'post';
            authnetForm.action = formActionUrl;
            appendHidden(authnetForm, 'token', payload.hostedToken);
            appendHidden(authnetForm, 'paymentProfileId', payload.paymentProfileId);
            document.body.appendChild(authnetForm);
            authnetForm.submit();
          } catch (err) {
            button.disabled = false;
            button.textContent = ${safeButtonText};
            setStatus(err.message || 'Unable to open the secure Authorize.Net form. Please try again.', true);
          }
        });
      })();
    </script>`;
}

function renderPaymentUpdateHtml({ ticket, testMode = false }) {
  const safePaymentType = escapeHtml(ticket.paymentUpdateType || PAYMENT_UPDATE_TYPE_B);
  const safeTicketId = escapeHtml(ticket.ticketId);
  const footerLabel = testMode ? `Test ticket: ${safeTicketId}` : `Secure link: ${safeTicketId}`;

  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <meta name="robots" content="noindex,nofollow" />
    <title>Fast Filings Secure Payment Update</title>
    <style>
      :root {
        color-scheme: light;
        --navy: #102033;
        --muted: #607089;
        --line: #dbe5f1;
        --paper: #ffffff;
        --soft: #f5f8fc;
        --teal: #0f766e;
        --teal-2: #14b8a6;
        --blue: #1f5f99;
        --shadow: 0 24px 70px rgba(16, 32, 51, 0.16);
      }
      * { box-sizing: border-box; }
      body {
        margin: 0;
        min-height: 100vh;
        display: flex;
        align-items: center;
        justify-content: center;
        padding: 28px;
        font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, Helvetica, sans-serif;
        background:
          radial-gradient(circle at 20% 0%, rgba(20, 184, 166, 0.22) 0%, transparent 34%),
          radial-gradient(circle at 90% 20%, rgba(31, 95, 153, 0.16) 0%, transparent 32%),
          linear-gradient(135deg, #eef7f5 0%, #f7f9fc 52%, #edf3fb 100%);
        color: var(--navy);
      }
      .shell { width: 100%; max-width: 920px; }
      .trust-row { display: flex; justify-content: space-between; align-items: center; gap: 14px; margin-bottom: 16px; color: #41516a; font-size: 13px; }
      .brand-lockup { display: inline-flex; align-items: center; gap: 12px; font-weight: 800; letter-spacing: -0.02em; }
      .ff-logo { display: block; width: min(260px, 54vw); height: auto; }
      .brand-text .small { margin-top: 6px; color: var(--muted); font-size: 12px; font-weight: 700; letter-spacing: 0.08em; text-transform: uppercase; }
      .authnet-badge { display: inline-flex; align-items: center; gap: 10px; padding: 10px 13px; border: 1px solid rgba(31, 95, 153, 0.18); background: rgba(255, 255, 255, 0.82); border-radius: 999px; box-shadow: 0 8px 24px rgba(16, 32, 51, 0.08); white-space: nowrap; }
      .authnet-logo { display: block; width: 156px; max-width: 36vw; height: auto; }
      .card { overflow: hidden; background: rgba(255, 255, 255, 0.92); border: 1px solid rgba(219, 229, 241, 0.95); border-radius: 28px; box-shadow: var(--shadow); backdrop-filter: blur(12px); }
      .topbar { height: 9px; background: linear-gradient(90deg, var(--teal) 0%, var(--teal-2) 48%, var(--blue) 100%); }
      .hero { padding: 42px; }
      .eyebrow { display: inline-flex; align-items: center; gap: 8px; padding: 8px 11px; border-radius: 999px; background: #e9fbf8; color: #0b665f; font-size: 13px; font-weight: 800; margin-bottom: 18px; }
      .lock { font-size: 15px; }
      h1 { margin: 0 0 14px; font-size: clamp(34px, 5vw, 54px); line-height: 0.98; letter-spacing: -0.055em; }
      .lead { margin: 0; color: var(--muted); font-size: 18px; line-height: 1.62; max-width: 590px; }
      .notice { margin: 26px 0 0; padding: 16px 18px; border-radius: 18px; background: #f8fafc; border: 1px solid var(--line); color: #4d5d75; line-height: 1.55; }
      .notice strong { color: var(--navy); }
      .action-stack { margin-top: 24px; max-width: 520px; }
      button { width: 100%; background: linear-gradient(135deg, var(--teal) 0%, #0b5f73 100%); color: #fff; border: 0; border-radius: 15px; padding: 15px 18px; font-weight: 900; font-size: 16px; cursor: pointer; box-shadow: 0 14px 28px rgba(15, 118, 110, 0.25); }
      button:hover { filter: brightness(1.03); transform: translateY(-1px); }
      button:disabled { cursor: wait; opacity: 0.74; transform: none; filter: none; }
      .status-line { min-height: 20px; margin: 11px 0 0; color: #52627a; font-size: 13px; line-height: 1.45; }
      .status-line.error { color: #b42318; }
      .checklist { list-style: none; padding: 0; margin: 20px 0 0; display: grid; gap: 12px; }
      .checklist li { display: flex; gap: 10px; color: #52627a; font-size: 14px; line-height: 1.45; }
      .check { flex: 0 0 auto; width: 20px; height: 20px; border-radius: 50%; background: #e9fbf8; color: var(--teal); display: inline-grid; place-items: center; font-weight: 900; }
      .footer-note { border-top: 1px solid var(--line); padding: 15px 42px 17px; display: flex; justify-content: space-between; gap: 16px; color: #7a8798; font-size: 12px; background: rgba(248, 250, 252, 0.7); }
      @media (max-width: 760px) {
        body { padding: 18px; align-items: flex-start; }
        .trust-row { align-items: flex-start; flex-direction: column; }
        .hero { padding: 30px 24px; }
        .action-stack { max-width: none; }
        .footer-note { padding: 14px 24px; flex-direction: column; }
        .authnet-badge { white-space: normal; }
      }
    </style>
  </head>
  <body>
    <div class="shell">
      <div class="trust-row" aria-label="Payment update trust indicators">
        <div class="brand-lockup" aria-label="Fast Filings"><div class="brand-text"><img class="ff-logo" src="/assets/payment-update/fast-filings-logo.png" alt="Fast Filings" /><div class="small">Secure payment update</div></div></div>
        <div class="authnet-badge" aria-label="Secured by Authorize.Net"><span>Secured by</span><img class="authnet-logo" src="/assets/payment-update/authorize-net-logo.svg" alt="Authorize.Net" /></div>
      </div>
      <main class="card">
        <div class="topbar"></div>
        <section class="content"><div class="hero">
          <div class="eyebrow"><span class="lock">🔒</span> Encrypted hosted payment session</div>
          <h1>Update your payment method securely.</h1>
          <p class="lead">You’ll leave this Fast Filings page and open the secure Authorize.Net payment update form.</p>
          <div class="action-stack" aria-label="Continue to Authorize.Net">
            <form id="authnet-session-form"><button id="continue-button" type="submit">Continue to Authorize.Net</button><div id="status-line" class="status-line" aria-live="polite"></div></form>
            <ul class="checklist" aria-label="Security details"><li><span class="check">✓</span><span>One-time secure session generated after you click Continue.</span></li><li><span class="check">✓</span><span>Hosted by Authorize.Net for card entry and update.</span></li><li><span class="check">✓</span><span>Fast Filings verifies the update after Authorize.Net processes it.</span></li></ul>
          </div>
          <div class="notice"><strong>Your card details stay with Authorize.Net.</strong> Fast Filings does not see or store your full card number, CVV, or banking details on this page.</div>
        </div></section>
        <div class="footer-note"><span>${safePaymentType}</span><span>${footerLabel}</span></div>
      </main>
    </div>
    ${paymentSessionScript({ buttonDefaultText: 'Continue to Authorize.Net' })}
  </body>
</html>`;
}

function recaptureCopy(flow) {
  if (flow === 'new-order') {
    return {
      small: 'Secure setup payment',
      title: 'Complete your Fast Filings setup',
      intro: 'Your Fast Filings Sales Tax service is almost ready. Complete this secure payment so we can activate your membership.',
      refPrefix: 'ORD',
      refLabel: 'Setup payment',
      consent: amount => amount
        ? `I authorize Fast Filings to process this ${amount} payment and begin my recurring sales tax filing membership.`
        : 'I authorize Fast Filings to process this secure payment and begin my recurring sales tax filing membership.',
      button: 'Continue to secure payment',
      typeFallback: PAYMENT_UPDATE_TYPE_A
    };
  }

  return {
    small: 'Secure restart payment',
    title: 'Restart your Fast Filings service',
    intro: 'Complete this secure restart payment so Fast Filings can restart your sales tax filing membership.',
    refPrefix: 'RST',
    refLabel: 'One-time restart',
    consent: amount => amount
      ? `I authorize Fast Filings to process this ${amount} restart payment and use the approved payment method to resume my recurring monthly membership.`
      : 'I authorize Fast Filings to process this secure restart payment and use the approved payment method to resume my recurring monthly membership.',
    button: 'Continue to secure payment',
    typeFallback: PAYMENT_UPDATE_TYPE_C
  };
}

function renderRecapturePaymentHtml({ ticket, sourceContext = {}, testMode = false }) {
  const flow = paymentFlowForTicket(ticket);
  const copy = recaptureCopy(flow);
  const amount = safeAmount(sourceContext.amountDue || sourceContext.amount || '');
  const amountDisplay = displayAmount(amount);
  const amountText = amountDisplay || 'Secure payment';
  const safeAmountText = escapeHtml(amountText);
  const safeTicketId = escapeHtml(ticket.ticketId);
  const safePaymentType = escapeHtml(ticket.paymentUpdateType || copy.typeFallback);
  const safeRef = escapeHtml(`${copy.refPrefix}-${String(ticket.subscriptionId || '').slice(0, 16)}`);
  const footerLabel = testMode ? `Test ticket: ${safeTicketId}` : `Secure link: ${safeTicketId}`;
  const safeConsent = escapeHtml(copy.consent(amountDisplay));

  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta name="robots" content="noindex,nofollow" />
  <title>${escapeHtml(copy.title)} | Fast Filings</title>
  <style>
    :root { color-scheme: light; --ink:#0f172a; --muted:#64748b; --line:#dbe4ef; --blue:#1d4ed8; --green:#047857; --teal:#0f766e; }
    * { box-sizing:border-box; }
    body { margin:0; min-height:100vh; font-family:Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color:var(--ink); background:linear-gradient(145deg, #0f172a 0%, #1d4ed8 52%, #0f766e 100%); padding:14px; display:grid; place-items:center; overflow-x:hidden; }
    main { width:calc(100vw - 28px); max-width:440px; }
    .card { width:100%; background:rgba(255,255,255,.97); border:1px solid rgba(255,255,255,.72); border-radius:26px; padding:20px; box-shadow:0 24px 70px rgba(15,23,42,.24); }
    .top { display:flex; align-items:center; justify-content:center; margin-bottom:14px; }
    .ff-logo { display:block; width:170px; max-width:100%; height:auto; }
    .small { margin:0 0 8px; color:#0f766e; font-size:12px; font-weight:950; letter-spacing:.08em; text-transform:uppercase; }
    h1 { margin:6px 0 8px; font-size:30px; line-height:1.04; letter-spacing:-.04em; }
    .intro { margin:0 0 14px; color:#475569; font-size:14px; line-height:1.42; }
    .amount-row { display:flex; align-items:flex-end; justify-content:space-between; gap:10px; flex-wrap:wrap; padding:14px 0; border-top:1px solid var(--line); border-bottom:1px solid var(--line); margin:12px 0; }
    .amount-label { margin:0; color:var(--muted); font-size:12px; font-weight:900; letter-spacing:.08em; text-transform:uppercase; }
    .price { margin:0; font-size:42px; line-height:.95; font-weight:950; letter-spacing:-.05em; }
    .ref { margin:0 0 2px; color:#334155; font-weight:850; text-align:right; }
    .mini { margin:0; color:var(--muted); font-size:12px; text-align:right; }
    .explain { margin:12px 0; color:#1e3a8a; background:#eff6ff; border:1px solid #bfdbfe; border-radius:16px; padding:12px; font-size:13px; line-height:1.38; }
    .consent { display:flex; gap:10px; align-items:flex-start; margin:12px 0; color:#0f172a; background:#fff; border:1px solid #cbd5e1; border-radius:16px; padding:12px; font-size:13px; line-height:1.34; cursor:pointer; }
    .consent input { width:19px; height:19px; margin:0; flex:0 0 auto; accent-color:var(--blue); }
    button { width:100%; border:0; border-radius:16px; padding:15px 16px; color:white; background:linear-gradient(135deg, #111827, #1d4ed8); font-size:16px; font-weight:950; cursor:pointer; box-shadow:0 14px 26px rgba(29,78,216,.25); }
    button:disabled { cursor:wait; opacity:.72; }
    .status-line { min-height:18px; margin:9px 0 0; color:#52627a; font-size:12px; line-height:1.35; }
    .status-line.error { color:#b42318; }
    .powered { display:flex; align-items:center; justify-content:center; gap:10px; margin-top:12px; color:#065f46; font-size:12px; font-weight:800; }
    .authnet-logo { display:block; width:118px; max-width:44%; height:auto; }
    .fine { margin:9px 0 0; color:var(--muted); font-size:11px; line-height:1.35; text-align:center; }
    .footer { margin-top:10px; display:flex; justify-content:space-between; gap:12px; color:#94a3b8; font-size:10.5px; overflow-wrap:anywhere; }
    @media (min-width:760px) { main { width:min(100%, 680px); max-width:680px; } .card { padding:28px; } .ff-logo { width:220px; } h1 { font-size:42px; max-width:560px; } .intro { font-size:16px; } .explain, .consent { font-size:14px; } }
    @media (max-height:760px) { body { padding:10px; } .card { padding:15px; border-radius:22px; } .top { margin-bottom:8px; } .ff-logo { width:150px; } h1 { font-size:25px; margin:4px 0 6px; } .intro { font-size:12.5px; margin-bottom:8px; } .amount-row { margin:8px 0; padding:10px 0; } .price { font-size:36px; } .explain, .consent { margin:8px 0; padding:10px; font-size:12px; } button { padding:13px; } .fine { display:none; } }
  </style>
</head>
<body>
<main>
  <section class="card" aria-label="Fast Filings secure payment bridge">
    <div class="top"><img class="ff-logo" src="/assets/payment-update/fast-filings-logo.png" alt="Fast Filings" /></div>
    <p class="small">${escapeHtml(copy.small)}</p>
    <h1>${escapeHtml(copy.title)}</h1>
    <p class="intro">${escapeHtml(copy.intro)}</p>
    <div class="amount-row">
      <div><p class="amount-label">Amount due now</p><p class="price">${safeAmountText}</p></div>
      <div><p class="ref">${safeRef}</p><p class="mini">${escapeHtml(copy.refLabel)}</p></div>
    </div>
    <p class="explain"><strong>Next:</strong> you’ll continue to Authorize.Net to enter payment details securely. Fast Filings will not see your full card number.</p>
    <form id="authnet-session-form">
      <label class="consent"><input type="checkbox" required /><span>${safeConsent}</span></label>
      <button id="continue-button" type="submit">${escapeHtml(copy.button)}</button>
      <div id="status-line" class="status-line" aria-live="polite"></div>
    </form>
    <div class="powered"><span>Checkout powered by</span><img class="authnet-logo" src="/assets/payment-update/authorize-net-logo.svg" alt="Authorize.Net" /></div>
    <p class="fine">This hosted checkout is payment-only. Fast Filings verifies approved payments before activating or restarting service.</p>
    <div class="footer"><span>${safePaymentType}</span><span>${footerLabel}</span></div>
  </section>
</main>
${paymentSessionScript({ buttonDefaultText: copy.button })}
</body>
</html>`;
}

async function loadValidatedPaymentUpdateTicket(ticketId, { testOnly = true } = {}) {
  const sheets = await getSheetsClient();
  const spreadsheetId = getPaymentUpdateSpreadsheetId();
  const ticket = await findPaymentUpdateTicket(sheets, ticketId, spreadsheetId);
  validatePaymentUpdateTicket(ticket, { testOnly });
  return { sheets, spreadsheetId, ticket };
}

async function loadSheetRowContext(sheets, spreadsheetId, tabName, rowNumber) {
  const out = {
    amountDue: '',
    amount: '',
    customerId: '',
    name: '',
    billingZip: '',
    email: '',
    notes: ''
  };

  const sourceTab = String(tabName || '').trim();
  const sourceRow = Number(rowNumber || 0);
  if (!sourceTab || !Number.isInteger(sourceRow) || sourceRow < 2 || sourceRow > 100000) return out;

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: `'${quoteSheetName(sourceTab)}'!A1:AZ${sourceRow}`,
    valueRenderOption: 'FORMATTED_VALUE'
  });
  const rows = response.data.values || [];
  if (!rows.length || !rows[sourceRow - 1]) return out;

  const headers = rows[0] || [];
  const row = rows[sourceRow - 1] || [];
  const source = {};
  headers.forEach((header, i) => {
    const key = normalizeHeader(header);
    if (key) source[key] = String(row[i] || '').trim();
  });

  out.amountDue = firstNonEmpty(
    valueByHeader(source, 'Amount Due'),
    valueByHeader(source, 'Amount'),
    valueByHeader(source, 'Amount to Collect')
  );
  out.amount = out.amountDue;
  out.customerId = valueByHeader(source, ['Customer ID', 'Customer ID(s)']);
  out.name = firstNonEmpty(valueByHeader(source, 'Name'), valueByHeader(source, 'Full Name'));
  out.billingZip = firstNonEmpty(
    valueByHeader(source, 'Billing Zip'),
    valueByHeader(source, 'Billing ZIP'),
    valueByHeader(source, 'Zip'),
    valueByHeader(source, 'Postal Code')
  );
  out.email = firstNonEmpty(valueByHeader(source, 'Email'), valueByHeader(source, 'Alt Email'));
  out.notes = firstNonEmpty(valueByHeader(source, 'Notes'), valueByHeader(source, 'Note'));
  return out;
}

async function loadPaymentUpdateSourceContext(sheets, spreadsheetId, ticket) {
  const [sourceContext, paymentRowContext] = await Promise.all([
    loadSheetRowContext(sheets, spreadsheetId, ticket.sourceTab, ticket.sourceRow),
    loadSheetRowContext(sheets, spreadsheetId, 'Payment Update', ticket.paymentUpdateRow)
  ]);

  return {
    amountDue: firstNonEmpty(sourceContext.amountDue, paymentRowContext.amountDue),
    amount: firstNonEmpty(sourceContext.amount, paymentRowContext.amount),
    // A/New Order source rows do not always carry Customer ID, but the Payment Update
    // queue can be backfilled from state Customer/Returns routing. Prefer the source
    // row when present and fall back to the queue row so hosted recapture payments keep
    // their state/customer route context without exposing or storing card data.
    customerId: firstNonEmpty(sourceContext.customerId, paymentRowContext.customerId),
    email: firstNonEmpty(sourceContext.email, paymentRowContext.email),
    notes: firstNonEmpty(sourceContext.notes, paymentRowContext.notes)
  };
}

function buildRecaptureTransactionRequest({ ticket, sourceContext, subscriptionData, amount, flow }) {
  const subscription = subscriptionData.subscription || {};
  const profile = subscription.profile || {};
  const { firstName, lastName } = splitFullName(firstNonEmpty(sourceContext.name, profile.name));

  const customerId = sourceContext.customerId || '';
  const statePrefix = customerId.includes('-') ? customerId.split('-', 1)[0].trim().toUpperCase() : '';
  const statePart = statePrefix ? ` ${statePrefix}` : '';
  const invoicePrefix = flow === 'new-order' ? 'ORD' : 'RST';
  const invoiceNumber = `${invoicePrefix}-${ticket.subscriptionId}`.slice(0, 20);
  const description = `Fast Filings${statePart} Sales Tax Filing`;

  return {
    transactionType: 'authCaptureTransaction',
    amount,
    order: {
      invoiceNumber,
      description: description.slice(0, 255)
    },
    ...(firstName || lastName || sourceContext.billingZip ? {
      billTo: {
        ...(firstName ? { firstName } : {}),
        ...(lastName ? { lastName } : {}),
        ...(sourceContext.billingZip ? { zip: sourceContext.billingZip } : {})
      }
    } : {}),
    customer: {
      email: firstNonEmpty(sourceContext.email, profile.email)
    }
  };
}

async function createHostedRecapturePaymentSession(req, sheets, spreadsheetId, ticket, flow) {
  await updatePaymentUpdateTicketAudit(sheets, ticket, { lastClickAt: true }, spreadsheetId);

  const [sourceContext, subscriptionData] = await Promise.all([
    loadPaymentUpdateSourceContext(sheets, spreadsheetId, ticket),
    getSubscription(ticket.subscriptionId)
  ]);

  const amount = safeAmount(sourceContext.amountDue || subscriptionData.subscription?.amount || '');
  if (!amount) {
    const err = new Error('Payment amount was not available for this recapture link');
    err.statusCode = 502;
    throw err;
  }

  const transactionRequest = buildRecaptureTransactionRequest({
    ticket,
    sourceContext,
    subscriptionData,
    amount,
    flow
  });

  const refPrefix = flow === 'new-order' ? 'new-order' : 'restart';
  const hostedToken = await getHostedPaymentPageToken(
    transactionRequest,
    hostedPaymentSettings(req, ticket, amount, flow),
    `${refPrefix}-${ticket.subscriptionId}`
  );
  await updatePaymentUpdateTicketAudit(sheets, ticket, { lastTokenGeneratedAt: true }, spreadsheetId);

  const config = getAuthNetConfig();
  return {
    formActionUrl: config.acceptHostedPaymentUrl,
    acceptHostedPaymentUrl: config.acceptHostedPaymentUrl,
    hostedToken,
    amount,
    paymentMode: flow,
    rawTokenStored: false,
    customerEmailSent: false,
    authNetMutation: false
  };
}

async function createHostedPaymentUpdateSession(req, sheets, spreadsheetId, ticket) {
  await updatePaymentUpdateTicketAudit(sheets, ticket, { lastClickAt: true }, spreadsheetId);

  const subscriptionData = await getSubscription(ticket.subscriptionId);
  const profile = subscriptionData.subscription?.profile || {};
  const paymentProfile = profile.paymentProfile || {};
  const customerProfileId = String(profile.customerProfileId || '');
  const paymentProfileId = String(paymentProfile.customerPaymentProfileId || '');
  if (!customerProfileId || !paymentProfileId) {
    const err = new Error('Authorize.Net profile/payment profile was not available for this subscription');
    err.statusCode = 502;
    throw err;
  }

  const hostedToken = await getHostedProfilePageToken(customerProfileId, paymentUpdateSettings(req, ticket));
  await updatePaymentUpdateTicketAudit(sheets, ticket, { lastTokenGeneratedAt: true }, spreadsheetId);

  const config = getAuthNetConfig();
  return {
    formActionUrl: config.acceptEditPaymentUrl,
    acceptEditPaymentUrl: config.acceptEditPaymentUrl,
    hostedToken,
    paymentProfileId,
    paymentMode: 'payment-update',
    rawTokenStored: false,
    customerEmailSent: false,
    authNetMutation: false
  };
}

async function createHostedSessionForTicket(req, sheets, spreadsheetId, ticket) {
  const flow = paymentFlowForTicket(ticket);
  if (flow === 'new-order' || flow === 'terminated') {
    return createHostedRecapturePaymentSession(req, sheets, spreadsheetId, ticket, flow);
  }
  return createHostedPaymentUpdateSession(req, sheets, spreadsheetId, ticket);
}

function renderReturnHtml(req) {
  const flow = String(req.params.flow || req.query.flow || 'payment-update');
  const title = flow === 'new-order'
    ? 'Payment submitted.'
    : (flow === 'terminated' ? 'Restart payment submitted.' : 'Thank you.');
  const message = flow === 'new-order'
    ? 'If you completed the secure Authorize.Net checkout, Fast Filings will verify the approved payment before activating your service.'
    : (flow === 'terminated'
      ? 'If you completed the secure Authorize.Net checkout, Fast Filings will verify the approved payment before restarting your membership.'
      : 'If you completed the secure Authorize.Net form, Fast Filings will verify the subscription/payment status before marking this payment update as complete.');

  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Payment Received | Fast Filings</title>
    <style>
      body { margin: 0; min-height: 100vh; display: flex; align-items: center; justify-content: center; padding: 24px; font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, Helvetica, sans-serif; background: linear-gradient(135deg, #eef7f5 0%, #f7f9fc 100%); color: #102033; }
      main { width: 100%; max-width: 640px; background: #fff; border: 1px solid #dbe5f1; border-radius: 24px; box-shadow: 0 20px 60px rgba(16, 32, 51, 0.12); overflow: hidden; }
      .topbar { height: 8px; background: linear-gradient(90deg, #0f766e 0%, #14b8a6 48%, #1f5f99 100%); }
      .content { padding: 34px; }
      .brand { display: inline-flex; align-items: center; margin-bottom: 22px; }
      .return-logo { display: block; width: min(250px, 70vw); height: auto; }
      h1 { margin: 0 0 12px; font-size: clamp(30px, 5vw, 42px); letter-spacing: -0.04em; }
      p { color: #607089; line-height: 1.65; font-size: 17px; }
    </style>
  </head>
  <body>
    <main>
      <div class="topbar"></div>
      <section class="content">
        <div class="brand"><img class="return-logo" src="/assets/payment-update/fast-filings-logo.png" alt="Fast Filings" /></div>
        <h1>${escapeHtml(title)}</h1>
        <p>${escapeHtml(message)}</p>
        <p>You can safely close this page.</p>
      </section>
    </main>
  </body>
</html>`;
}

async function renderTicketHtml(req, res, { testOnly }) {
  try {
    const ticketId = String(req.params.ticketId || '').trim();
    const { sheets, spreadsheetId, ticket } = await loadValidatedPaymentUpdateTicket(ticketId, { testOnly });
    const flow = paymentFlowForTicket(ticket);
    let sourceContext = null;
    if (flow === 'new-order' || flow === 'terminated') {
      sourceContext = await loadPaymentUpdateSourceContext(sheets, spreadsheetId, ticket);
      if (!safeAmount(sourceContext.amountDue || sourceContext.amount || '')) {
        const subscriptionData = await getSubscription(ticket.subscriptionId);
        sourceContext = {
          ...sourceContext,
          amountDue: subscriptionData.subscription?.amount || sourceContext.amountDue || '',
          amount: subscriptionData.subscription?.amount || sourceContext.amount || ''
        };
      }
    }
    const html = flow === 'new-order' || flow === 'terminated'
      ? renderRecapturePaymentHtml({ ticket, sourceContext, testMode: testOnly })
      : renderPaymentUpdateHtml({ ticket, testMode: testOnly });

    res.set({
      'Content-Type': 'text/html; charset=utf-8',
      'Cache-Control': 'no-store, max-age=0',
      Pragma: 'no-cache',
      'X-Robots-Tag': 'noindex, nofollow'
    });
    return res.status(200).send(html);
  } catch (err) {
    console.error(testOnly ? 'PAYMENT UPDATE TEST ROUTE ERROR:' : 'PAYMENT UPDATE LIVE ROUTE ERROR:', err.message);
    return res.status(err.statusCode || 500).send(`Payment update link error: ${escapeHtml(err.message)}`);
  }
}

async function createSessionResponse(req, res, { testOnly }) {
  try {
    const ticketId = String(req.params.ticketId || '').trim();
    const { sheets, spreadsheetId, ticket } = await loadValidatedPaymentUpdateTicket(ticketId, { testOnly });
    const session = await createHostedSessionForTicket(req, sheets, spreadsheetId, ticket);

    res.set({
      'Cache-Control': 'no-store, max-age=0',
      Pragma: 'no-cache',
      'X-Robots-Tag': 'noindex, nofollow'
    });
    return res.status(200).json({
      ok: true,
      ...session,
      rawTokenStored: false,
      customerEmailSent: false,
      authNetMutation: false
    });
  } catch (err) {
    console.error(testOnly ? 'PAYMENT UPDATE TEST SESSION ERROR:' : 'PAYMENT UPDATE LIVE SESSION ERROR:', err.message);
    res.set({
      'Cache-Control': 'no-store, max-age=0',
      Pragma: 'no-cache',
      'X-Robots-Tag': 'noindex, nofollow'
    });
    return res.status(err.statusCode || 500).json({
      ok: false,
      error: err.message || 'Unable to create the secure Authorize.Net session'
    });
  }
}

function createPaymentUpdateRouter() {
  const router = express.Router();

  router.get('/test', (req, res) => {
    res.json({
      ok: true,
      route: '/payment-update/test/:ticketId',
      liveRoute: '/payment-update/:ticketId',
      modes: {
        [PAYMENT_UPDATE_TYPE_A]: 'hosted-payment-charge-new-order',
        [PAYMENT_UPDATE_TYPE_B]: 'hosted-profile-payment-update',
        [PAYMENT_UPDATE_TYPE_C]: 'hosted-payment-charge-restart'
      },
      spreadsheet_configured: Boolean(process.env.PAYMENT_UPDATE_SPREADSHEET_ID || process.env.GOOGLE_SHEETS_SPREADSHEET_ID),
      authnet_configured: Boolean(process.env.AUTHNET_API_LOGIN_ID && process.env.AUTHNET_TRANSACTION_KEY),
      raw_tokens_stored: false,
      customer_emails_sent: false,
      authnet_mutations_without_customer_submit: false,
      hosted_session_timing: 'generated_after_continue_click',
      hosted_return_url_format: 'path-no-query',
      accept_hosted_order_summary_guard: true,
      sample_recapture_return_url: hostedReturnUrl(req, { ticketId: 'pu_test_healthcheck' }, 'new-order')
    });
  });

  router.get('/test/:ticketId', (req, res) => renderTicketHtml(req, res, { testOnly: true }));
  router.post('/test/:ticketId/session', (req, res) => createSessionResponse(req, res, { testOnly: true }));

  router.get('/return', (req, res) => {
    res.set({
      'Content-Type': 'text/html; charset=utf-8',
      'Cache-Control': 'no-store, max-age=0',
      Pragma: 'no-cache',
      'X-Robots-Tag': 'noindex, nofollow'
    });
    return res.status(200).send(renderReturnHtml(req));
  });

  router.get('/return/:flow/:ticketId', (req, res) => {
    res.set({
      'Content-Type': 'text/html; charset=utf-8',
      'Cache-Control': 'no-store, max-age=0',
      Pragma: 'no-cache',
      'X-Robots-Tag': 'noindex, nofollow'
    });
    return res.status(200).send(renderReturnHtml(req));
  });

  router.post('/:ticketId/session', (req, res) => createSessionResponse(req, res, { testOnly: false }));
  router.get('/:ticketId', (req, res) => renderTicketHtml(req, res, { testOnly: false }));

  return router;
}

module.exports = {
  createPaymentUpdateRouter,
  __paymentUpdateTestHooks: {
    hostedReturnUrl,
    hostedPaymentSettings,
    paymentUpdateSettings,
    paymentFlowForTicket,
    safeAmount,
    displayAmount
  }
};
