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
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <meta name="robots" content="noindex,nofollow" />
    <title>Fast Filings Secure Payment Update</title>
    <style>
      body { margin: 0; min-height: 100vh; display: flex; align-items: center; justify-content: center; padding: 24px; font-family: Inter, Arial, Helvetica, sans-serif; background: #f5f7fb; color: #14213d; }
      .card { width: 100%; max-width: 640px; background: #fff; border: 1px solid #d9e2f1; border-radius: 24px; box-shadow: 0 20px 60px rgba(20, 33, 61, 0.12); overflow: hidden; }
      .topbar { height: 8px; background: linear-gradient(90deg, #0f766e 0%, #14b8a6 100%); }
      .content { padding: 36px 30px 30px; }
      .brand { font-size: 14px; font-weight: 700; letter-spacing: 0.08em; text-transform: uppercase; color: #0f766e; margin-bottom: 18px; }
      h1 { margin: 0 0 12px; font-size: clamp(28px, 5vw, 40px); line-height: 1.08; letter-spacing: -0.03em; }
      p { color: #5f6c86; font-size: 17px; line-height: 1.6; }
      .notice { margin: 22px 0; padding: 14px 16px; border-radius: 14px; background: #f8fafc; border: 1px solid #d9e2f1; color: #5f6c86; }
      button { background: #0f766e; color: #fff; border: 0; border-radius: 12px; padding: 13px 18px; font-weight: 800; font-size: 16px; cursor: pointer; }
      .muted { margin-top: 18px; font-size: 13px; color: #7b879c; }
    </style>
  </head>
  <body>
    <main class="card">
      <div class="topbar"></div>
      <section class="content">
        <div class="brand">Fast Filings</div>
        <h1>Secure payment update</h1>
        <p>Use the button below to open the secure Authorize.Net payment update page.</p>
        <div class="notice">Fast Filings will not see or store your full card number. This page only creates a short-lived secure Authorize.Net session for this payment profile.</div>
        <form method="post" action="${escapeHtml(acceptEditPaymentUrl)}">
          <input type="hidden" name="token" value="${escapeHtml(hostedToken)}" />
          <input type="hidden" name="paymentProfileId" value="${escapeHtml(paymentProfileId)}" />
          <button type="submit">Continue to secure card update</button>
        </form>
        <p class="muted">Test ticket: ${escapeHtml(ticket.ticketId)} · ${escapeHtml(ticket.paymentUpdateType || 'Payment Update')}</p>
      </section>
    </main>
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
      body { margin: 0; min-height: 100vh; display: flex; align-items: center; justify-content: center; padding: 24px; font-family: Inter, Arial, Helvetica, sans-serif; background: #f5f7fb; color: #14213d; }
      main { width: 100%; max-width: 620px; background: #fff; border: 1px solid #d9e2f1; border-radius: 24px; box-shadow: 0 20px 60px rgba(20, 33, 61, 0.12); padding: 34px; }
      h1 { margin: 0 0 12px; }
      p { color: #5f6c86; line-height: 1.6; }
    </style>
  </head>
  <body>
    <main>
      <h1>Thank you.</h1>
      <p>If you completed the secure Authorize.Net form, Fast Filings will verify the subscription/payment status before marking this payment update as complete.</p>
      <p>You can safely close this page.</p>
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
