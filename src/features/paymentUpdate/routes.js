const express = require('express');
const { getSheetsClient } = require('../../core/googleSheets');
const { escapeHtml } = require('../../core/html');
const { getAuthNetConfig, getHostedProfilePageToken, getSubscription } = require('../../connectors/authnet/client');
const {
  findPaymentUpdateTicket,
  getPaymentUpdateSpreadsheetId,
  updatePaymentUpdateTicketAudit,
  validatePaymentUpdateTicket
} = require('./tickets');

function getBaseUrl(req) {
  const configured = process.env.PAYMENT_UPDATE_BASE_URL || '';
  if (configured) return configured.replace(/\/$/, '');

  const protocol = req.get('x-forwarded-proto') || req.protocol || 'https';
  return `${protocol}://${req.get('host')}`.replace(/\/$/, '');
}

function paymentUpdateSettings(req, ticket) {
  const returnUrl = `${getBaseUrl(req)}/payment-update/return?ticket=${encodeURIComponent(ticket.ticketId)}`;
  return [
    { settingName: 'hostedProfileReturnUrl', settingValue: returnUrl },
    { settingName: 'hostedProfileReturnUrlText', settingValue: 'Return to Fast Filings' },
    { settingName: 'hostedProfilePageBorderVisible', settingValue: 'false' },
    { settingName: 'hostedProfileBillingAddressRequired', settingValue: 'true' },
    { settingName: 'hostedProfileCardCodeRequired', settingValue: 'true' }
  ];
}

function renderPaymentUpdateHtml({ acceptEditPaymentUrl, hostedToken, paymentProfileId, ticket }) {
  const safePaymentType = escapeHtml(ticket.paymentUpdateType || 'Payment Update');
  const safeTicketId = escapeHtml(ticket.ticketId);

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
        --gold: #f2b84b;
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
      .trust-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 14px;
        margin-bottom: 16px;
        color: #41516a;
        font-size: 13px;
      }
      .brand-lockup { display: inline-flex; align-items: center; gap: 12px; font-weight: 800; letter-spacing: -0.02em; }
      .ff-logo { display: block; width: min(260px, 54vw); height: auto; }
      .brand-text .small { margin-top: 6px; color: var(--muted); font-size: 12px; font-weight: 700; letter-spacing: 0.08em; text-transform: uppercase; }
      .authnet-badge {
        display: inline-flex;
        align-items: center;
        gap: 10px;
        padding: 10px 13px;
        border: 1px solid rgba(31, 95, 153, 0.18);
        background: rgba(255, 255, 255, 0.82);
        border-radius: 999px;
        box-shadow: 0 8px 24px rgba(16, 32, 51, 0.08);
        white-space: nowrap;
      }
      .authnet-logo { display: block; width: 156px; max-width: 36vw; height: auto; }
      .card {
        overflow: hidden;
        background: rgba(255, 255, 255, 0.92);
        border: 1px solid rgba(219, 229, 241, 0.95);
        border-radius: 28px;
        box-shadow: var(--shadow);
        backdrop-filter: blur(12px);
      }
      .topbar { height: 9px; background: linear-gradient(90deg, var(--teal) 0%, var(--teal-2) 48%, var(--blue) 100%); }
      .content {
        display: grid;
        grid-template-columns: minmax(0, 1.15fr) minmax(280px, 0.85fr);
        gap: 0;
      }
      .hero { padding: 42px 42px 38px; }
      .eyebrow {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        padding: 8px 11px;
        border-radius: 999px;
        background: #e9fbf8;
        color: #0b665f;
        font-size: 13px;
        font-weight: 800;
        margin-bottom: 18px;
      }
      .lock { font-size: 15px; }
      h1 { margin: 0 0 14px; font-size: clamp(34px, 5vw, 54px); line-height: 0.98; letter-spacing: -0.055em; }
      .lead { margin: 0; color: var(--muted); font-size: 18px; line-height: 1.62; max-width: 590px; }
      .notice {
        margin: 26px 0 0;
        padding: 16px 18px;
        border-radius: 18px;
        background: #f8fafc;
        border: 1px solid var(--line);
        color: #4d5d75;
        line-height: 1.55;
      }
      .notice strong { color: var(--navy); }
      .action-panel {
        border-left: 1px solid var(--line);
        background:
          linear-gradient(180deg, rgba(248, 250, 252, 0.9) 0%, rgba(255, 255, 255, 0.95) 100%);
        padding: 42px 34px;
        display: flex;
        flex-direction: column;
        justify-content: center;
      }
      .panel-title { margin: 0 0 10px; font-size: 18px; font-weight: 900; }
      .panel-copy { margin: 0 0 22px; color: var(--muted); line-height: 1.55; }
      button {
        width: 100%;
        background: linear-gradient(135deg, var(--teal) 0%, #0b5f73 100%);
        color: #fff;
        border: 0;
        border-radius: 15px;
        padding: 15px 18px;
        font-weight: 900;
        font-size: 16px;
        cursor: pointer;
        box-shadow: 0 14px 28px rgba(15, 118, 110, 0.25);
      }
      button:hover { filter: brightness(1.03); transform: translateY(-1px); }
      .checklist { list-style: none; padding: 0; margin: 24px 0 0; display: grid; gap: 12px; }
      .checklist li { display: flex; gap: 10px; color: #52627a; font-size: 14px; line-height: 1.45; }
      .check { flex: 0 0 auto; width: 20px; height: 20px; border-radius: 50%; background: #e9fbf8; color: var(--teal); display: inline-grid; place-items: center; font-weight: 900; }
      .footer-note {
        border-top: 1px solid var(--line);
        padding: 15px 42px 17px;
        display: flex;
        justify-content: space-between;
        gap: 16px;
        color: #7a8798;
        font-size: 12px;
        background: rgba(248, 250, 252, 0.7);
      }
      @media (max-width: 760px) {
        body { padding: 18px; align-items: flex-start; }
        .trust-row { align-items: flex-start; flex-direction: column; }
        .content { grid-template-columns: 1fr; }
        .hero, .action-panel { padding: 30px 24px; }
        .action-panel { border-left: 0; border-top: 1px solid var(--line); }
        .footer-note { padding: 14px 24px; flex-direction: column; }
        .authnet-badge { white-space: normal; }
      }
    </style>
  </head>
  <body>
    <div class="shell">
      <div class="trust-row" aria-label="Payment update trust indicators">
        <div class="brand-lockup" aria-label="Fast Filings">
          <div class="brand-text">
            <img class="ff-logo" src="/assets/payment-update/fast-filings-logo.png" alt="Fast Filings" />
            <div class="small">Secure payment update</div>
          </div>
        </div>
        <div class="authnet-badge" aria-label="Secured by Authorize.Net">
          <span>Secured by</span>
          <img class="authnet-logo" src="/assets/payment-update/authorize-net-logo.svg" alt="Authorize.Net" />
        </div>
      </div>

      <main class="card">
        <div class="topbar"></div>
        <section class="content">
          <div class="hero">
            <div class="eyebrow"><span class="lock">🔒</span> Encrypted hosted payment session</div>
            <h1>Update your payment method securely.</h1>
            <p class="lead">You’ll leave this Fast Filings page and open the secure Authorize.Net payment update form.</p>
            <div class="notice"><strong>Your card details stay with Authorize.Net.</strong> Fast Filings does not see or store your full card number, CVV, or banking details on this page.</div>
          </div>

          <aside class="action-panel" aria-label="Continue to Authorize.Net">
            <p class="panel-title">Ready to continue?</p>
            <p class="panel-copy">You’ll leave this Fast Filings bridge page and open the secure Authorize.Net payment update form.</p>
            <form method="post" action="${escapeHtml(acceptEditPaymentUrl)}">
              <input type="hidden" name="token" value="${escapeHtml(hostedToken)}" />
              <input type="hidden" name="paymentProfileId" value="${escapeHtml(paymentProfileId)}" />
              <button type="submit">Continue to Authorize.Net</button>
            </form>
            <ul class="checklist" aria-label="Security details">
              <li><span class="check">✓</span><span>One-time secure session generated when you click the link.</span></li>
              <li><span class="check">✓</span><span>Hosted by Authorize.Net for card entry and update.</span></li>
              <li><span class="check">✓</span><span>Fast Filings verifies the update after Authorize.Net processes it.</span></li>
            </ul>
          </aside>
        </section>
        <div class="footer-note">
          <span>${safePaymentType}</span>
          <span>Test ticket: ${safeTicketId}</span>
        </div>
      </main>
    </div>
  </body>
</html>`;
}

function renderReturnHtml() {
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Payment Update Received | Fast Filings</title>
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
        <h1>Thank you.</h1>
        <p>If you completed the secure Authorize.Net form, Fast Filings will verify the subscription/payment status before marking this payment update as complete.</p>
        <p>You can safely close this page.</p>
      </section>
    </main>
  </body>
</html>`;
}

function createPaymentUpdateRouter() {
  const router = express.Router();

  router.get('/test', (req, res) => {
    res.json({
      ok: true,
      route: '/payment-update/test/:ticketId',
      mode: 'test-only',
      spreadsheet_configured: Boolean(process.env.PAYMENT_UPDATE_SPREADSHEET_ID || process.env.GOOGLE_SHEETS_SPREADSHEET_ID),
      authnet_configured: Boolean(process.env.AUTHNET_API_LOGIN_ID && process.env.AUTHNET_TRANSACTION_KEY),
      raw_tokens_stored: false,
      customer_emails_sent: false,
      authnet_mutations: false
    });
  });

  router.get('/test/:ticketId', async (req, res) => {
    try {
      const sheets = await getSheetsClient();
      const spreadsheetId = getPaymentUpdateSpreadsheetId();
      const ticketId = String(req.params.ticketId || '').trim();
      const ticket = await findPaymentUpdateTicket(sheets, ticketId, spreadsheetId);
      validatePaymentUpdateTicket(ticket, { testOnly: true });
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
      const html = renderPaymentUpdateHtml({
        acceptEditPaymentUrl: config.acceptEditPaymentUrl,
        hostedToken,
        paymentProfileId,
        ticket
      });

      res.set({
        'Content-Type': 'text/html; charset=utf-8',
        'Cache-Control': 'no-store, max-age=0',
        Pragma: 'no-cache',
        'X-Robots-Tag': 'noindex, nofollow'
      });
      return res.status(200).send(html);
    } catch (err) {
      console.error('PAYMENT UPDATE TEST ROUTE ERROR:', err.message);
      return res.status(err.statusCode || 500).send(`Payment update link error: ${escapeHtml(err.message)}`);
    }
  });

  router.get('/return', (req, res) => {
    res.set({
      'Content-Type': 'text/html; charset=utf-8',
      'Cache-Control': 'no-store, max-age=0',
      Pragma: 'no-cache',
      'X-Robots-Tag': 'noindex, nofollow'
    });
    return res.status(200).send(renderReturnHtml());
  });

  return router;
}

module.exports = {
  createPaymentUpdateRouter
};
