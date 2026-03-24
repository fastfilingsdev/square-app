require('dotenv').config();
const express = require('express');
const axios = require('axios');
const crypto = require('crypto');
const { google } = require('googleapis');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const CLIENT_ID = process.env.SQUARE_CLIENT_ID;
const CLIENT_SECRET = process.env.SQUARE_CLIENT_SECRET;
const ACCESS_TOKEN = process.env.SQUARE_ACCESS_TOKEN || '';
const REDIRECT_URI = process.env.SQUARE_REDIRECT_URI || 'http://localhost:3000/callback';
const SQUARE_VERSION = process.env.SQUARE_VERSION || '2025-02-20';
const SQUARE_BASE_URL = process.env.SQUARE_BASE_URL || 'https://connect.squareup.com';
const GOOGLE_SHEETS_SPREADSHEET_ID = process.env.GOOGLE_SHEETS_SPREADSHEET_ID;
const GOOGLE_SERVICE_ACCOUNT_EMAIL = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
const GOOGLE_PRIVATE_KEY = process.env.GOOGLE_PRIVATE_KEY;
const oauthStates = new Map();

function getSheetsAuth() {
  const email = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
  let key = process.env.GOOGLE_PRIVATE_KEY;
  const spreadsheetId = process.env.GOOGLE_SHEETS_SPREADSHEET_ID;

  if (!email || !key || !spreadsheetId) {
    throw new Error('Missing Google Sheets environment variables in .env');
  }

  if (key.startsWith('"') && key.endsWith('"')) {
    key = key.slice(1, -1);
  }

  key = key.replace(/\\n/g, '\n');

  return new google.auth.GoogleAuth({
    credentials: {
      client_email: email,
      private_key: key,
    },
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
}

async function getSheetsClient() {
  const auth = getSheetsAuth();
  const client = await auth.getClient();
  return google.sheets({ version: 'v4', auth: client });
}

async function getSquareAccessTokenForCustomer(customerId) {
  return refreshConnectionIfNeeded(customerId);
}

async function getSquareHeaders(customerId) {
  const accessToken = await getSquareAccessTokenForCustomer(customerId);

  return {
    Authorization: `Bearer ${accessToken}`,
    'Square-Version': SQUARE_VERSION,
  };
}

function resolveDateRange(query) {
  let { start, end, period } = query;

  if ((!start || !end) && period) {
    const monthlyMatch = /^(\d{4})-(\d{2})$/.exec(period) || /^(\d{2})\.(\d{2})$/.exec(period);

    if (monthlyMatch) {
      let year;
      let month;

      if (period.includes('-')) {
        year = Number(monthlyMatch[1]);
        month = Number(monthlyMatch[2]);
      } else {
        month = Number(monthlyMatch[1]);
        year = 2000 + Number(monthlyMatch[2]);
      }

      const startDate = new Date(Date.UTC(year, month - 1, 1));
      const endDate = new Date(Date.UTC(year, month, 0));
      start = `${year}-${String(month).padStart(2, '0')}-01`;
      end = `${year}-${String(month).padStart(2, '0')}-${String(endDate.getUTCDate()).padStart(2, '0')}`;
    }
  }

  return { start, end, period: period || null };
}


function buildPeriodLabel(start, end, period) {
  if (period) {
    const isoMonthlyMatch = /^(\d{4})-(\d{2})$/.exec(period);
    const shortMonthlyMatch = /^(\d{2})\.(\d{2})$/.exec(period);
    const quarterMatch =
      /^Q([1-4])\s*(\d{4})$/i.exec(period) ||
      /^(\d{4})-Q([1-4])$/i.exec(period);

    if (isoMonthlyMatch) {
      return `${isoMonthlyMatch[1]}-${isoMonthlyMatch[2]}`;
    }

    if (shortMonthlyMatch) {
      const month = shortMonthlyMatch[1];
      const year = `20${shortMonthlyMatch[2]}`;
      return `${year}-${month}`;
    }

    if (quarterMatch) {
      if (/^Q/i.test(period)) {
        const quarter = quarterMatch[1];
        const year = quarterMatch[2];
        return `${year}-Q${quarter}`;
      }
      return `${quarterMatch[1]}-Q${quarterMatch[2]}`;
    }

    return period;
  }

  if (start && end) return `${start} to ${end}`;
  return start || end || new Date().toISOString().slice(0, 7);
}

// Helper to fetch all Square payments with date range and pagination
async function listSquarePayments(customerId, { start, end, locationId } = {}) {
  const headers = await getSquareHeaders(customerId);
  const payments = [];
  let cursor;

  do {
    const params = {
      sort_order: 'ASC'
    };

    if (start) {
      params.begin_time = `${start}T00:00:00Z`;
    }

    if (end) {
      params.end_time = `${end}T23:59:59Z`;
    }

    if (locationId) {
      params.location_id = locationId;
    }

    if (cursor) {
      params.cursor = cursor;
    }

    const response = await axios.get(
      `${SQUARE_BASE_URL}/v2/payments`,
      {
        headers,
        params
      }
    );

    payments.push(...(response.data.payments || []));
    cursor = response.data.cursor;
  } while (cursor);

  return payments;
}

async function exchangeSquareAuthorizationCode(code) {
  if (!CLIENT_ID || !CLIENT_SECRET) {
    throw new Error('Missing SQUARE_CLIENT_ID or SQUARE_CLIENT_SECRET in .env');
  }

  const response = await axios.post(
    `${SQUARE_BASE_URL}/oauth2/token`,
    {
      client_id: CLIENT_ID,
      client_secret: CLIENT_SECRET,
      code,
      grant_type: 'authorization_code',
      redirect_uri: REDIRECT_URI
    },
    {
      headers: {
        'Content-Type': 'application/json'
      }
    }
  );

  return response.data;
}

async function refreshSquareAccessToken(refreshToken) {
  if (!CLIENT_ID || !CLIENT_SECRET) {
    throw new Error('Missing SQUARE_CLIENT_ID or SQUARE_CLIENT_SECRET in .env');
  }

  if (!refreshToken) {
    throw new Error('Missing refresh token for Square token refresh');
  }

  const response = await axios.post(
    `${SQUARE_BASE_URL}/oauth2/token`,
    {
      client_id: CLIENT_ID,
      client_secret: CLIENT_SECRET,
      grant_type: 'refresh_token',
      refresh_token: refreshToken
    },
    {
      headers: {
        'Content-Type': 'application/json'
      }
    }
  );

  return response.data;
}

async function saveSquareConnection(sheets, connectionData) {
  const rowValues = [
    connectionData.customerId || '',
    connectionData.squareMerchantId || '',
    connectionData.accessToken || '',
    connectionData.refreshToken || '',
    connectionData.expiresAt || '',
    connectionData.connected ? 'TRUE' : 'FALSE',
    connectionData.connectedOn || '',
    connectionData.environment || 'Sandbox'
  ];

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: process.env.GOOGLE_SHEETS_SPREADSHEET_ID,
    range: 'Connections!A:H'
  });

  const rows = response.data.values || [];
  let existingRowNumber = null;

  for (let i = 1; i < rows.length; i += 1) {
    const row = rows[i] || [];
    const existingCustomerId = String(row[0] || '').trim();
    if (existingCustomerId === connectionData.customerId) {
      existingRowNumber = i + 1;
      break;
    }
  }

  if (existingRowNumber) {
    await sheets.spreadsheets.values.update({
      spreadsheetId: process.env.GOOGLE_SHEETS_SPREADSHEET_ID,
      range: `Connections!A${existingRowNumber}:H${existingRowNumber}`,
      valueInputOption: 'USER_ENTERED',
      requestBody: {
        values: [rowValues]
      }
    });

    return 'updated';
  }

  await sheets.spreadsheets.values.append({
    spreadsheetId: process.env.GOOGLE_SHEETS_SPREADSHEET_ID,
    range: 'Connections!A:H',
    valueInputOption: 'USER_ENTERED',
    requestBody: {
      values: [rowValues]
    }
  });

  return 'appended';
}


// Helper to sync the Square merchant ID into the Customers sheet
async function syncCustomerSquareMerchantId(sheets, customerId, squareMerchantId) {
  if (!customerId || !squareMerchantId) {
    return 'skipped';
  }

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: process.env.GOOGLE_SHEETS_SPREADSHEET_ID,
    range: 'Customers!A:Z'
  });

  const rows = response.data.values || [];
  if (rows.length < 4) {
    return 'skipped';
  }

  const headerRowIndex = 2;
  const headers = (rows[headerRowIndex] || []).map(header => String(header || '').trim().toLowerCase());
  const getIndex = (...names) => {
    const match = names.find(name => headers.includes(name));
    return match ? headers.indexOf(match) : -1;
  };

  const customerIdIndex = getIndex('customer id', 'internal customer id', 'id');
  const squareCustomerIdIndex = getIndex('square merchant id', 'square customer id', 'square id', 'square_customer_id');
  const squareConnectedIndex = getIndex('square connected', 'squareconnected');
  const lastSyncIndex = getIndex('last sync', 'lastsync');

  if (customerIdIndex === -1 || squareCustomerIdIndex === -1) {
    return 'skipped';
  }

  for (let i = headerRowIndex + 1; i < rows.length; i += 1) {
    const row = rows[i] || [];
    const existingCustomerId = String(row[customerIdIndex] || '').trim();

    if (existingCustomerId !== String(customerId).trim()) {
      continue;
    }

    const rowNumber = i + 1;
    await sheets.spreadsheets.values.update({
      spreadsheetId: process.env.GOOGLE_SHEETS_SPREADSHEET_ID,
      range: `Customers!${String.fromCharCode(65 + squareCustomerIdIndex)}${rowNumber}`,
      valueInputOption: 'USER_ENTERED',
      requestBody: {
        values: [[squareMerchantId]]
      }
    });

    if (squareConnectedIndex > -1) {
      await sheets.spreadsheets.values.update({
        spreadsheetId: process.env.GOOGLE_SHEETS_SPREADSHEET_ID,
        range: `Customers!${String.fromCharCode(65 + squareConnectedIndex)}${rowNumber}`,
        valueInputOption: 'USER_ENTERED',
        requestBody: {
          values: [['Yes']]
        }
      });
    }

    if (lastSyncIndex > -1) {
      await sheets.spreadsheets.values.update({
        spreadsheetId: process.env.GOOGLE_SHEETS_SPREADSHEET_ID,
        range: `Customers!${String.fromCharCode(65 + lastSyncIndex)}${rowNumber}`,
        valueInputOption: 'USER_ENTERED',
        requestBody: {
          values: [[new Date().toISOString().slice(0, 10)]]
        }
      });
    }

    return 'updated';
  }

  return 'not_found';
}

// === Inserted helper functions ===
async function syncCustomerLastSyncDate(sheets, customerId, syncDate = new Date()) {
  if (!customerId) {
    return 'skipped';
  }

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: process.env.GOOGLE_SHEETS_SPREADSHEET_ID,
    range: 'Customers!A:Z'
  });

  const rows = response.data.values || [];
  if (rows.length < 4) {
    return 'skipped';
  }

  const headerRowIndex = 2;
  const headers = (rows[headerRowIndex] || []).map(header => String(header || '').trim().toLowerCase());
  const getIndex = (...names) => {
    const match = names.find(name => headers.includes(name));
    return match ? headers.indexOf(match) : -1;
  };

  const customerIdIndex = getIndex('customer id', 'internal customer id', 'id');
  const lastSyncIndex = getIndex('last sync', 'lastsync');

  if (customerIdIndex === -1 || lastSyncIndex === -1) {
    return 'skipped';
  }

  for (let i = headerRowIndex + 1; i < rows.length; i += 1) {
    const row = rows[i] || [];
    const existingCustomerId = String(row[customerIdIndex] || '').trim();

    if (existingCustomerId !== String(customerId).trim()) {
      continue;
    }

    const rowNumber = i + 1;
    await sheets.spreadsheets.values.update({
      spreadsheetId: process.env.GOOGLE_SHEETS_SPREADSHEET_ID,
      range: `Customers!${String.fromCharCode(65 + lastSyncIndex)}${rowNumber}`,
      valueInputOption: 'USER_ENTERED',
      requestBody: {
        values: [[syncDate]]
      }
    });

    return 'updated';
  }

  return 'not_found';
}

async function removeExistingReviewRowsForPeriod(sheets, customerId, periodValue) {
  if (!customerId || !periodValue) {
    return 0;
  }

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: process.env.GOOGLE_SHEETS_SPREADSHEET_ID,
    range: 'Review Queue!A:N'
  });

  const rows = response.data.values || [];
  if (rows.length <= 1) {
    return 0;
  }

  const rowsToKeep = [rows[0]];
  let removedCount = 0;

  for (let i = 1; i < rows.length; i += 1) {
    const row = rows[i] || [];
    const existingPeriod = String(row[1] || '').trim();
    const existingCustomerId = String(row[4] || '').trim();

    if (existingPeriod === String(periodValue).trim() && existingCustomerId === String(customerId).trim()) {
      removedCount += 1;
      continue;
    }

    rowsToKeep.push(row);
  }

  if (removedCount > 0) {
    await sheets.spreadsheets.values.clear({
      spreadsheetId: process.env.GOOGLE_SHEETS_SPREADSHEET_ID,
      range: 'Review Queue!A2:N'
    });

    if (rowsToKeep.length > 1) {
      await sheets.spreadsheets.values.update({
        spreadsheetId: process.env.GOOGLE_SHEETS_SPREADSHEET_ID,
        range: `Review Queue!A2:N${rowsToKeep.length}`,
        valueInputOption: 'USER_ENTERED',
        requestBody: {
          values: rowsToKeep.slice(1)
        }
      });
    }
  }

  return removedCount;
}

function isTokenExpiringSoon(expiresAt) {
  if (!expiresAt) return false;

  const expiryMs = new Date(expiresAt).getTime();
  if (Number.isNaN(expiryMs)) return false;

  const bufferMs = 10 * 60 * 1000; // 10 minutes
  return expiryMs <= Date.now() + bufferMs;
}

async function refreshConnectionIfNeeded(customerId) {
  if (!customerId) {
    if (!ACCESS_TOKEN) {
      throw new Error('Missing customer_id and no fallback SQUARE_ACCESS_TOKEN in .env');
    }
    return ACCESS_TOKEN;
  }

  const sheets = await getSheetsClient();
  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: process.env.GOOGLE_SHEETS_SPREADSHEET_ID,
    range: 'Connections!A:H'
  });

  const rows = response.data.values || [];
  for (let i = 1; i < rows.length; i += 1) {
    const row = rows[i] || [];
    const existingCustomerId = String(row[0] || '').trim();
    const existingAccessToken = String(row[2] || '').trim();
    const existingRefreshToken = String(row[3] || '').trim();
    const existingExpiresAt = String(row[4] || '').trim();
    const connected = String(row[5] || '').trim().toUpperCase();
    const environment = String(row[7] || '').trim() || 'Sandbox';

    if (existingCustomerId !== String(customerId).trim()) {
      continue;
    }

    if (connected && connected !== 'TRUE') {
      throw new Error(`Customer ${customerId} is not marked connected in Connections`);
    }

    if (!existingAccessToken) {
      throw new Error(`No access token found in Connections for customer ${customerId}`);
    }

    if (!isTokenExpiringSoon(existingExpiresAt)) {
      return existingAccessToken;
    }

    if (!existingRefreshToken) {
      throw new Error(`Token for customer ${customerId} is expiring or expired and no refresh token is stored`);
    }

    const refreshedTokenData = await refreshSquareAccessToken(existingRefreshToken);

    await saveSquareConnection(sheets, {
      customerId: existingCustomerId,
      squareMerchantId: row[1] || '',
      accessToken: refreshedTokenData.access_token || existingAccessToken,
      refreshToken: refreshedTokenData.refresh_token || existingRefreshToken,
      expiresAt: refreshedTokenData.expires_at || existingExpiresAt,
      connected: true,
      connectedOn: row[6] || new Date().toISOString(),
      environment
    });

    console.log('SQUARE TOKEN REFRESH SUCCESS:', {
      customer_id: existingCustomerId,
      expires_at: refreshedTokenData.expires_at || existingExpiresAt,
      access_token_prefix: refreshedTokenData.access_token
        ? refreshedTokenData.access_token.slice(0, 12)
        : existingAccessToken.slice(0, 12)
    });

    return refreshedTokenData.access_token || existingAccessToken;
  }

  if (!ACCESS_TOKEN) {
    throw new Error(`No connection found for customer ${customerId} and no fallback SQUARE_ACCESS_TOKEN in .env`);
  }

  return ACCESS_TOKEN;
}


app.get('/connect', (req, res) => {
  if (!CLIENT_ID || !CLIENT_SECRET) {
    return res.status(500).send('Missing SQUARE_CLIENT_ID or SQUARE_CLIENT_SECRET in .env');
  }

  const { customer_id } = req.query;

  if (!customer_id) {
    return res.status(400).send('Missing customer_id. Use /connect?customer_id=CUS-0001');
  }

  const state = crypto.randomBytes(24).toString('hex');
  oauthStates.set(state, {
    createdAt: Date.now(),
    customerId: String(customer_id).trim()
  });

  const params = new URLSearchParams({
    client_id: CLIENT_ID,
    scope: 'ORDERS_READ PAYMENTS_READ ITEMS_READ',
    session: 'false',
    state,
    redirect_uri: REDIRECT_URI,
  });

  const url = `${SQUARE_BASE_URL}/oauth2/authorize?${params.toString()}`;
  console.log('AUTH URL GENERATED FOR CUSTOMER:', customer_id);
  res.redirect(url);
});

app.get('/callback', async (req, res) => {
  const { code, state, error, error_description } = req.query;

  if (error) {
    console.log('OAUTH ERROR:', error, error_description || '');
    return res.status(400).send(`Square error: ${error}${error_description ? ` - ${error_description}` : ''}`);
  }

  if (!state || !oauthStates.has(state)) {
    return res.status(400).send('Invalid OAuth state. Please try connecting again.');
  }

  const stateData = oauthStates.get(state);
  oauthStates.delete(state);

  if (!code) {
    return res.status(400).send('Missing authorization code from Square.');
  }

  try {
    const tokenData = await exchangeSquareAuthorizationCode(code);
    const sheets = await getSheetsClient();
    const connectionAction = await saveSquareConnection(sheets, {
      customerId: stateData?.customerId || '',
      squareMerchantId: tokenData.merchant_id || '',
      accessToken: tokenData.access_token || '',
      refreshToken: tokenData.refresh_token || '',
      expiresAt: tokenData.expires_at || '',
      connected: true,
      connectedOn: new Date().toISOString(),
      environment: SQUARE_BASE_URL.includes('squareupsandbox.com') ? 'Sandbox' : 'Production'
    });
    const customerSheetAction = await syncCustomerSquareMerchantId(
      sheets,
      stateData?.customerId || '',
      tokenData.merchant_id || ''
    );

    console.log('SQUARE TOKEN EXCHANGE SUCCESS:', {
      customer_id: stateData?.customerId || null,
      merchant_id: tokenData.merchant_id || null,
      access_token_prefix: tokenData.access_token ? tokenData.access_token.slice(0, 12) : null,
      refresh_token_present: !!tokenData.refresh_token,
      expires_at: tokenData.expires_at || null,
      connections_sheet_action: connectionAction,
      customers_sheet_action: customerSheetAction
    });

    res.json({
      success: true,
      message: 'Square connected, authorization code exchanged, and connection saved successfully.',
      customer_id: stateData?.customerId || null,
      merchant_id: tokenData.merchant_id || null,
      expires_at: tokenData.expires_at || null,
      token_type: tokenData.token_type || null,
      short_lived: tokenData.short_lived || null,
      connections_sheet_action: connectionAction,
      customers_sheet_action: customerSheetAction
    });
  } catch (err) {
    console.error('SQUARE TOKEN EXCHANGE ERROR:', err.response?.data || err.message);
    return res.status(500).json(err.response?.data || { error: err.message });
  }
});

app.get('/', (req, res) => {
  res.send('Server is working');
});

app.get('/pull-sales', async (req, res) => {
  try {
    const { customer_id } = req.query;
    const payments = await listSquarePayments(customer_id, {
      start: req.query.start || null,
      end: req.query.end || null,
      locationId: req.query.location_id || null
    });

    res.json({ payments });
  } catch (err) {
    console.error('SQUARE ERROR:', err.response?.data || err.message);
    res.status(500).json(err.response?.data || { error: err.message });
  }
});

app.get('/locations', async (req, res) => {
  try {
    const response = await axios.get(
      `${SQUARE_BASE_URL}/v2/locations`,
      {
        headers: await getSquareHeaders(req.query.customer_id)
      }
    );

    res.json(response.data);
  } catch (err) {
    console.error('SQUARE LOCATIONS ERROR:', err.response?.data || err.message);
    res.status(500).json(err.response?.data || { error: err.message });
  }
});

app.get('/sales-summary', async (req, res) => {
  try {
    const { customer_id } = req.query;
    const payments = await listSquarePayments(customer_id, {
      start: req.query.start || null,
      end: req.query.end || null,
      locationId: req.query.location_id || null
    });

    let grossSales = 0;
    let processingFees = 0;
    let completedCount = 0;
    let paymentsMissingFee = 0;

    payments.forEach(payment => {
      if (payment.status === 'COMPLETED') {
        completedCount += 1;
        grossSales += payment.amount_money?.amount || 0;

        if (Array.isArray(payment.processing_fee) && payment.processing_fee.length > 0) {
          payment.processing_fee.forEach(fee => {
            processingFees += fee.amount_money?.amount || 0;
          });
        } else {
          paymentsMissingFee += 1;
        }
      }
    });

    const netAfterFees = grossSales - processingFees;

    res.json({
      total_payments: completedCount,
      gross_sales: grossSales / 100,
      processing_fees: processingFees / 100,
      payments_missing_processing_fee: paymentsMissingFee,
      processing_fees_status: paymentsMissingFee > 0 ? 'partial_or_pending' : 'final',
      net_after_fees: netAfterFees / 100
    });
  } catch (err) {
    console.error('SQUARE SUMMARY ERROR:', err.response?.data || err.message);
    res.status(500).json(err.response?.data || { error: err.message });
  }
});

app.get('/sales-tax-ready', async (req, res) => {
  try {
    const { location_id, customer_id } = req.query;
    const sheets = await getSheetsClient();
    const customerRecord = await getCustomerRecordByInternalId(sheets, customer_id);
    const { start, end, period } = resolveDateRange(req.query);

    const payments = await listSquarePayments(customer_id, {
      start,
      end,
      locationId: location_id || null
    });

    const filteredPayments = payments.filter(payment => {
      if (payment.status !== 'COMPLETED') return false;
      if (location_id && payment.location_id !== location_id) return false;
      return true;
    });

    let grossSales = 0;
    let processingFees = 0;
    let refunds = 0;
    let taxCollected = 0;

    filteredPayments.forEach(payment => {
      grossSales += payment.amount_money?.amount || 0;

      if (Array.isArray(payment.processing_fee)) {
        payment.processing_fee.forEach(fee => {
          processingFees += fee.amount_money?.amount || 0;
        });
      }

      if (Array.isArray(payment.refunds)) {
        payment.refunds.forEach(refund => {
          refunds += refund.amount_money?.amount || 0;
        });
      }

      if (payment.total_money && payment.approved_money) {
        const total = payment.total_money.amount || 0;
        const approved = payment.approved_money.amount || 0;
        const possibleTax = total - approved;
        if (possibleTax > 0) {
          taxCollected += possibleTax;
        }
      }
    });

    const netSales = grossSales - refunds;

    res.json({
      period: {
        start: start || null,
        end: end || null,
        period: period || null,
        location_id: location_id || null,
        customer_id: customer_id || null
      },
      source: 'payments-api-v1',
      assumptions: [
        'taxable_sales currently assumes all completed sales are taxable',
        'tax_collected is not reliable from Payments alone and will be upgraded with Orders API next',
        'product/service categorization is not included yet'
      ],
      totals: {
        payment_count: filteredPayments.length,
        gross_sales: grossSales / 100,
        refunds: refunds / 100,
        net_sales: netSales / 100,
        processing_fees: processingFees / 100,
        taxable_sales: netSales / 100,
        tax_collected_estimate: taxCollected / 100
      },
      payments: filteredPayments.map(payment => ({
        id: payment.id,
        created_at: payment.created_at,
        amount: (payment.amount_money?.amount || 0) / 100,
        status: payment.status,
        source_type: payment.source_type || null,
        location_id: payment.location_id || null,
        note: payment.note || '',
        order_id: payment.order_id || null
      }))
    });
  } catch (err) {
    console.error('SQUARE TAX ENGINE ERROR:', err.response?.data || err.message);
    res.status(500).json(err.response?.data || { error: err.message });
  }
});

app.get('/orders-tax-engine', async (req, res) => {
  try {
    const { location_id, customer_id } = req.query;
    const sheets = await getSheetsClient();
    const customerRecord = await getCustomerRecordByInternalId(sheets, customer_id);
    const { start, end, period } = resolveDateRange(req.query);

    const payments = await listSquarePayments(customer_id, {
      start,
      end,
      locationId: location_id || null
    });

    const filteredPayments = payments.filter(payment => {
      if (payment.status !== 'COMPLETED') return false;
      if (!payment.order_id) return false;
      if (location_id && payment.location_id !== location_id) return false;
      return true;
    });

    const orderIds = [...new Set(filteredPayments.map(payment => payment.order_id).filter(Boolean))];

    if (orderIds.length === 0) {
      return res.json({
        period: {
          start: start || null,
          end: end || null,
          period: period || null,
          location_id: location_id || null,
          customer_id: customer_id || null
        },
        source: 'orders-api-v1',
        assumptions: [
          'no completed payments with order_id matched this filter',
          'tax_collected comes from Orders API total_tax_money',
          'product/service categorization is not included yet'
        ],
        totals: {
          payment_count: 0,
          order_count: 0,
          gross_sales: 0,
          discounts: 0,
          refunds: 0,
          net_sales_before_tax: 0,
          taxable_sales_estimate: 0,
          non_taxable_sales_estimate: 0,
          tax_collected: 0,
          processing_fees: 0
        },
        orders: []
      });
    }

    const batchBody = {
      order_ids: orderIds
    };

    if (location_id) {
      batchBody.location_id = location_id;
    }

    const ordersResponse = await axios.post(
      `${SQUARE_BASE_URL}/v2/orders/batch-retrieve`,
      batchBody,
      {
        headers: {
          ...(await getSquareHeaders(customer_id)),
          'Content-Type': 'application/json'
        }
      }
    );

    const orders = ordersResponse.data.orders || [];

    let grossSales = 0;
    let discounts = 0;
    let refunds = 0;
    let netSalesBeforeTax = 0;
    let taxableSalesEstimate = 0;
    let nonTaxableSalesEstimate = 0;
    let taxCollected = 0;
    let processingFees = 0;
    let paymentsMissingFee = 0;

    filteredPayments.forEach(payment => {
      if (Array.isArray(payment.processing_fee) && payment.processing_fee.length > 0) {
        payment.processing_fee.forEach(fee => {
          processingFees += fee.amount_money?.amount || 0;
        });
      } else {
        paymentsMissingFee += 1;
      }

      if (Array.isArray(payment.refunds)) {
        payment.refunds.forEach(refund => {
          refunds += refund.amount_money?.amount || 0;
        });
      }
    });

    const orderPaymentMap = new Map(filteredPayments.map(payment => [payment.order_id, payment]));

    const detailedOrders = orders.map(order => {
      const lineItems = Array.isArray(order.line_items) ? order.line_items : [];
      const payment = orderPaymentMap.get(order.id);

      let orderGross = 0;
      let orderDiscounts = 0;
      let orderTaxableEstimate = 0;
      let orderNonTaxableEstimate = 0;

      const detailedLineItems = lineItems.map(item => {
        const gross = item.gross_sales_money?.amount || 0;
        const discount = item.total_discount_money?.amount || 0;
        const tax = item.total_tax_money?.amount || 0;
        const total = item.total_money?.amount || 0;

        orderGross += gross;
        orderDiscounts += discount;

        if (tax > 0) {
          orderTaxableEstimate += total;
        } else {
          orderNonTaxableEstimate += total;
        }

        return {
          name: item.name || 'Unnamed item',
          quantity: item.quantity || '1',
          gross_sales: gross / 100,
          discount: discount / 100,
          tax: tax / 100,
          total: total / 100,
          taxable_estimate: tax > 0
        };
      });

      const orderTax = order.total_tax_money?.amount || 0;
      const orderServiceCharges = order.total_service_charge_money?.amount || 0;
      const orderNetBeforeTax = orderGross - orderDiscounts + orderServiceCharges;

      grossSales += orderGross;
      discounts += orderDiscounts;
      taxableSalesEstimate += orderTaxableEstimate;
      nonTaxableSalesEstimate += orderNonTaxableEstimate;
      taxCollected += orderTax;
      netSalesBeforeTax += orderNetBeforeTax;

      return {
        order_id: order.id,
        created_at: order.created_at || payment?.created_at || null,
        location_id: order.location_id || payment?.location_id || null,
        payment_id: payment?.id || null,
        square_customer_id: payment?.customer_id || null,
        source_type: payment?.source_type || null,
        note: payment?.note || '',
        totals: {
          gross_sales: orderGross / 100,
          discounts: orderDiscounts / 100,
          service_charges: orderServiceCharges / 100,
          net_sales_before_tax: orderNetBeforeTax / 100,
          tax_collected: orderTax / 100,
          taxable_sales_estimate: orderTaxableEstimate / 100,
          non_taxable_sales_estimate: orderNonTaxableEstimate / 100
        },
        line_items: detailedLineItems
      };
    });

    res.json({
      period: {
        start: start || null,
        end: end || null,
        period: period || null,
        location_id: location_id || null,
        customer_id: customer_id || null
      },
      source: 'orders-api-v1',
      assumptions: [
        'tax_collected comes from Orders API total_tax_money',
        'taxable_sales_estimate treats line items with tax > 0 as taxable',
        'non_taxable_sales_estimate treats line items with tax = 0 as non-taxable',
        'product/service categorization is not included yet'
      ],
      totals: {
        payment_count: filteredPayments.length,
        order_count: detailedOrders.length,
        gross_sales: grossSales / 100,
        discounts: discounts / 100,
        refunds: refunds / 100,
        net_sales_before_tax: netSalesBeforeTax / 100,
        taxable_sales_estimate: taxableSalesEstimate / 100,
        non_taxable_sales_estimate: nonTaxableSalesEstimate / 100,
        tax_collected: taxCollected / 100,
        processing_fees: processingFees / 100,
        payments_missing_processing_fee: paymentsMissingFee,
        processing_fees_status: paymentsMissingFee > 0 ? 'partial_or_pending' : 'final'
      },
      orders: detailedOrders
    });
  } catch (err) {
    console.error('SQUARE ORDERS TAX ENGINE ERROR:', err.response?.data || err.message);
    res.status(500).json(err.response?.data || { error: err.message });
  }
});

app.get('/catalog-sync', async (req, res) => {
  try {
    const { customer_id } = req.query;
    const response = await axios.post(
      `${SQUARE_BASE_URL}/v2/catalog/search`,
      {
        object_types: ['ITEM', 'CATEGORY', 'TAX']
      },
      {
        headers: {
          ...(await getSquareHeaders(customer_id)),
          'Content-Type': 'application/json'
        }
      }
    );

    const objects = response.data.objects || [];

    const categories = new Map();
    const taxes = new Map();
    const items = [];

    objects.forEach(obj => {
      if (obj.type === 'CATEGORY') {
        categories.set(obj.id, {
          id: obj.id,
          name: obj.category_data?.name || 'Unnamed category'
        });
      }

      if (obj.type === 'TAX') {
        taxes.set(obj.id, {
          id: obj.id,
          name: obj.tax_data?.name || 'Unnamed tax',
          percentage: obj.tax_data?.percentage || null,
          enabled: obj.present_at_all_locations ?? true
        });
      }
    });

    objects.forEach(obj => {
      if (obj.type !== 'ITEM') return;

      const itemData = obj.item_data || {};
      const category = itemData.category_id ? categories.get(itemData.category_id) : null;
      const itemTaxes = Array.isArray(itemData.tax_ids)
        ? itemData.tax_ids.map(id => taxes.get(id)).filter(Boolean)
        : [];

      const variations = Array.isArray(itemData.variations)
        ? itemData.variations.map(variation => ({
            id: variation.id,
            name: variation.item_variation_data?.name || 'Default',
            pricing_type: variation.item_variation_data?.pricing_type || null,
            price: variation.item_variation_data?.price_money
              ? {
                  amount: (variation.item_variation_data.price_money.amount || 0) / 100,
                  currency: variation.item_variation_data.price_money.currency || null
                }
              : null,
            sellable: variation.present_at_all_locations ?? true
          }))
        : [];

      items.push({
        id: obj.id,
        name: itemData.name || 'Unnamed item',
        description: itemData.description || '',
        product_type: itemData.product_type || null,
        category: category ? category.name : null,
        category_id: itemData.category_id || null,
        tax_ids: Array.isArray(itemData.tax_ids) ? itemData.tax_ids : [],
        taxes: itemTaxes,
        variations
      });
    });

    res.json({
      source: 'catalog-api-v1',
      counts: {
        items: items.length,
        categories: categories.size,
        taxes: taxes.size
      },
      categories: Array.from(categories.values()),
      taxes: Array.from(taxes.values()),
      items
    });
  } catch (err) {
    console.error('SQUARE CATALOG SYNC ERROR:', err.response?.data || err.message);
    res.status(500).json(err.response?.data || { error: err.message });
  }
});

app.get('/catalog-enriched-orders', async (req, res) => {
  try {
    const { location_id, customer_id } = req.query;
    const sheets = await getSheetsClient();
    const customerRecord = await getCustomerRecordByInternalId(sheets, customer_id);
    const { start, end, period } = resolveDateRange(req.query);

    const payments = await listSquarePayments(customer_id, {
      start,
      end,
      locationId: location_id || null
    });

    const filteredPayments = payments.filter(payment => {
      if (payment.status !== 'COMPLETED') return false;
      if (!payment.order_id) return false;
      if (location_id && payment.location_id !== location_id) return false;
      return true;
    });

    const orderIds = [...new Set(filteredPayments.map(payment => payment.order_id).filter(Boolean))];

    const catalogResponse = await axios.post(
      `${SQUARE_BASE_URL}/v2/catalog/search`,
      {
        object_types: ['ITEM', 'CATEGORY', 'TAX']
      },
      {
        headers: {
          ...(await getSquareHeaders(customer_id)),
          'Content-Type': 'application/json'
        }
      }
    );

    const catalogObjects = catalogResponse.data.objects || [];
    const categories = new Map();
    const taxes = new Map();
    const itemsByName = new Map();
    const itemsByVariationName = new Map();
    const catalogItems = [];

    catalogObjects.forEach(obj => {
      if (obj.type === 'CATEGORY') {
        categories.set(obj.id, {
          id: obj.id,
          name: obj.category_data?.name || 'Unnamed category'
        });
      }

      if (obj.type === 'TAX') {
        taxes.set(obj.id, {
          id: obj.id,
          name: obj.tax_data?.name || 'Unnamed tax',
          percentage: obj.tax_data?.percentage || null
        });
      }
    });

    catalogObjects.forEach(obj => {
      if (obj.type !== 'ITEM') return;

      const itemData = obj.item_data || {};
      const variations = Array.isArray(itemData.variations) ? itemData.variations : [];
      const resolvedTaxes = Array.isArray(itemData.tax_ids)
        ? itemData.tax_ids.map(id => taxes.get(id)).filter(Boolean)
        : [];

      const enrichedItem = {
        id: obj.id,
        name: itemData.name || 'Unnamed item',
        description: itemData.description || '',
        product_type: itemData.product_type || null,
        category_id: itemData.category_id || null,
        category: itemData.category_id ? categories.get(itemData.category_id)?.name || null : null,
        tax_ids: Array.isArray(itemData.tax_ids) ? itemData.tax_ids : [],
        taxes: resolvedTaxes,
        variations: variations.map(variation => ({
          id: variation.id,
          name: variation.item_variation_data?.name || 'Default',
          pricing_type: variation.item_variation_data?.pricing_type || null,
          price: variation.item_variation_data?.price_money
            ? {
                amount: (variation.item_variation_data.price_money.amount || 0) / 100,
                currency: variation.item_variation_data.price_money.currency || null
              }
            : null
        }))
      };

      catalogItems.push(enrichedItem);
      itemsByName.set(enrichedItem.name.toLowerCase(), enrichedItem);
      enrichedItem.variations.forEach(variation => {
        itemsByVariationName.set(`${enrichedItem.name.toLowerCase()}::${variation.name.toLowerCase()}`, enrichedItem);
        itemsByVariationName.set(variation.name.toLowerCase(), enrichedItem);
      });
    });

    if (orderIds.length === 0) {
      return res.json({
        period: {
          start: start || null,
          end: end || null,
          period: period || null,
          location_id: location_id || null,
          customer_id: customer_id || null
        },
        source: 'catalog-enriched-orders-v1',
        counts: {
          payments: 0,
          orders: 0,
          catalog_items: itemsByName.size
        },
        orders: []
      });
    }

    const batchBody = { order_ids: orderIds };
    if (location_id) batchBody.location_id = location_id;

    const ordersResponse = await axios.post(
      `${SQUARE_BASE_URL}/v2/orders/batch-retrieve`,
      batchBody,
      {
        headers: {
          ...(await getSquareHeaders(customer_id)),
          'Content-Type': 'application/json'
        }
      }
    );

    const orders = ordersResponse.data.orders || [];
    const orderPaymentMap = new Map(filteredPayments.map(payment => [payment.order_id, payment]));

    const enrichedOrders = orders.map(order => {
      const payment = orderPaymentMap.get(order.id);
      const lineItems = Array.isArray(order.line_items) ? order.line_items : [];

      const enrichedLineItems = lineItems.map(item => {
        const itemName = (item.name || 'Unnamed item').trim();
        const variationName = (item.variation_name || '').trim();

        let catalogItem = itemsByVariationName.get(`${itemName.toLowerCase()}::${variationName.toLowerCase()}`)
          || itemsByName.get(itemName.toLowerCase())
          || itemsByVariationName.get(variationName.toLowerCase())
          || null;

        if (!catalogItem && itemName.toLowerCase() === 'unnamed item' && catalogItems.length === 1) {
          catalogItem = catalogItems[0];
        }

        const tax = item.total_tax_money?.amount || 0;
        const total = item.total_money?.amount || 0;

        return {
          order_name: itemName,
          variation_name: variationName || null,
          quantity: item.quantity || '1',
          gross_sales: (item.gross_sales_money?.amount || 0) / 100,
          discount: (item.total_discount_money?.amount || 0) / 100,
          tax: tax / 100,
          total: total / 100,
          taxable_estimate: tax > 0,
          catalog_match_found: !!catalogItem,
          catalog: catalogItem ? {
            id: catalogItem.id,
            name: catalogItem.name,
            description: catalogItem.description,
            product_type: catalogItem.product_type,
            category: catalogItem.category,
            category_id: catalogItem.category_id,
            tax_ids: catalogItem.tax_ids,
            taxes: catalogItem.taxes,
            variations: catalogItem.variations
          } : null
        };
      });

      return {
        order_id: order.id,
        created_at: order.created_at || payment?.created_at || null,
        location_id: order.location_id || payment?.location_id || null,
        payment_id: payment?.id || null,
        square_customer_id: payment?.customer_id || null,
        source_type: payment?.source_type || null,
        note: payment?.note || '',
        tax_collected: (order.total_tax_money?.amount || 0) / 100,
        line_items: enrichedLineItems
      };
    });

    res.json({
      period: {
        start: start || null,
        end: end || null,
        period: period || null,
        location_id: location_id || null,
        customer_id: customer_id || null
      },
      source: 'catalog-enriched-orders-v1',
      counts: {
        payments: filteredPayments.length,
        orders: enrichedOrders.length,
        catalog_items: itemsByName.size
      },
      assumptions: [
        'catalog matching is currently based on order line item name and variation name',
        'if a line item name does not match the catalog, a fallback match is used when there is exactly one catalog item and the order line is Unnamed item',
        'next step is to build classification rules on top of these catalog-enriched lines'
      ],
      orders: enrichedOrders
    });
  } catch (err) {
    console.error('SQUARE CATALOG ENRICHED ORDERS ERROR:', err.response?.data || err.message);
    res.status(500).json(err.response?.data || { error: err.message });
  }
});

app.get('/classification-layer', async (req, res) => {
  try {
    const { location_id, customer_id } = req.query;
    const sheets = await getSheetsClient();
    const customerRecord = await getCustomerRecordByInternalId(sheets, customer_id);
    const { start, end, period } = resolveDateRange(req.query);

    const payments = await listSquarePayments(customer_id, {
      start,
      end,
      locationId: location_id || null
    });

    const filteredPayments = payments.filter(payment => {
      if (payment.status !== 'COMPLETED') return false;
      if (!payment.order_id) return false;
      if (location_id && payment.location_id !== location_id) return false;
      return true;
    });

    const orderIds = [...new Set(filteredPayments.map(payment => payment.order_id).filter(Boolean))];

    const catalogResponse = await axios.post(
      `${SQUARE_BASE_URL}/v2/catalog/search`,
      {
        object_types: ['ITEM', 'CATEGORY', 'TAX']
      },
      {
        headers: {
          ...(await getSquareHeaders(customer_id)),
          'Content-Type': 'application/json'
        }
      }
    );

    const catalogObjects = catalogResponse.data.objects || [];
    const categories = new Map();
    const taxes = new Map();
    const itemsByName = new Map();
    const itemsByVariationName = new Map();
    const catalogItems = [];

    catalogObjects.forEach(obj => {
      if (obj.type === 'CATEGORY') {
        categories.set(obj.id, {
          id: obj.id,
          name: obj.category_data?.name || 'Unnamed category'
        });
      }

      if (obj.type === 'TAX') {
        taxes.set(obj.id, {
          id: obj.id,
          name: obj.tax_data?.name || 'Unnamed tax',
          percentage: obj.tax_data?.percentage || null
        });
      }
    });

    catalogObjects.forEach(obj => {
      if (obj.type !== 'ITEM') return;

      const itemData = obj.item_data || {};
      const variations = Array.isArray(itemData.variations) ? itemData.variations : [];
      const resolvedTaxes = Array.isArray(itemData.tax_ids)
        ? itemData.tax_ids.map(id => taxes.get(id)).filter(Boolean)
        : [];

      const enrichedItem = {
        id: obj.id,
        name: itemData.name || 'Unnamed item',
        description: itemData.description || '',
        product_type: itemData.product_type || null,
        category_id: itemData.category_id || null,
        category: itemData.category_id ? categories.get(itemData.category_id)?.name || null : null,
        tax_ids: Array.isArray(itemData.tax_ids) ? itemData.tax_ids : [],
        taxes: resolvedTaxes,
        variations: variations.map(variation => ({
          id: variation.id,
          name: variation.item_variation_data?.name || 'Default',
          pricing_type: variation.item_variation_data?.pricing_type || null,
          price: variation.item_variation_data?.price_money
            ? {
                amount: (variation.item_variation_data.price_money.amount || 0) / 100,
                currency: variation.item_variation_data.price_money.currency || null
              }
            : null
        }))
      };

      catalogItems.push(enrichedItem);
      itemsByName.set(enrichedItem.name.toLowerCase(), enrichedItem);
      enrichedItem.variations.forEach(variation => {
        itemsByVariationName.set(`${enrichedItem.name.toLowerCase()}::${variation.name.toLowerCase()}`, enrichedItem);
        itemsByVariationName.set(variation.name.toLowerCase(), enrichedItem);
      });
    });

    if (orderIds.length === 0) {
      return res.json({
        period: {
          start: start || null,
          end: end || null,
          period: period || null,
          location_id: location_id || null,
          customer_id: customer_id || null
        },
        source: 'classification-layer-v1',
        counts: {
          payments: 0,
          orders: 0,
          classified_lines: 0,
          needs_review: 0
        },
        summary: {
          taxable: 0,
          non_taxable: 0,
          needs_review: 0
        },
        orders: []
      });
    }

    const batchBody = { order_ids: orderIds };
    if (location_id) batchBody.location_id = location_id;

    const ordersResponse = await axios.post(
      `${SQUARE_BASE_URL}/v2/orders/batch-retrieve`,
      batchBody,
      {
        headers: {
          ...(await getSquareHeaders(customer_id)),
          'Content-Type': 'application/json'
        }
      }
    );

    const orders = ordersResponse.data.orders || [];
    const orderPaymentMap = new Map(filteredPayments.map(payment => [payment.order_id, payment]));

    let taxableCount = 0;
    let nonTaxableCount = 0;
    let needsReviewCount = 0;

    const classifiedOrders = orders.map(order => {
      const payment = orderPaymentMap.get(order.id);
      const lineItems = Array.isArray(order.line_items) ? order.line_items : [];

      const classifiedLineItems = lineItems.map(item => {
        const itemName = (item.name || 'Unnamed item').trim();
        const variationName = (item.variation_name || '').trim();

        let catalogItem = itemsByVariationName.get(`${itemName.toLowerCase()}::${variationName.toLowerCase()}`)
          || itemsByName.get(itemName.toLowerCase())
          || itemsByVariationName.get(variationName.toLowerCase())
          || null;

        if (!catalogItem && itemName.toLowerCase() === 'unnamed item' && catalogItems.length === 1) {
          catalogItem = catalogItems[0];
        }

        const observedTax = (item.total_tax_money?.amount || 0) / 100;
        const normalizedItemName = itemName.toLowerCase();
        const hasCatalogMatch = !!catalogItem;
        const isUnnamedItem = normalizedItemName === 'unnamed item';
        const isServiceChargeLike = normalizedItemName.includes('service charge') || normalizedItemName.includes('fee');
        const observedTaxable = observedTax > 0;
        const configuredTaxable = !!(catalogItem && Array.isArray(catalogItem.tax_ids) && catalogItem.tax_ids.length > 0);

        let classificationStatus = 'needs_review';
        let reason = 'No rule matched.';

        if (observedTaxable && isUnnamedItem) {
          classificationStatus = 'taxable';
          reason = 'Tax was charged on an unnamed/custom Square line item, so it is treated as taxable.';
          taxableCount += 1;
        } else if (observedTaxable && isServiceChargeLike) {
          classificationStatus = 'taxable';
          reason = 'Tax was charged on a service-charge/fee line item, so it is treated as taxable.';
          taxableCount += 1;
        } else if (observedTaxable && configuredTaxable) {
          classificationStatus = 'taxable';
          reason = 'Tax was charged on the order and the catalog item has tax configured in Square.';
          taxableCount += 1;
        } else if (observedTaxable && !hasCatalogMatch) {
          classificationStatus = 'taxable';
          reason = 'Tax was charged on a line item with no catalog match, so it is treated as taxable.';
          taxableCount += 1;
        } else if (!observedTaxable && isUnnamedItem) {
          classificationStatus = 'non_taxable';
          reason = 'No tax was charged on an unnamed/custom Square line item, so it is treated as non-taxable.';
          nonTaxableCount += 1;
        } else if (!observedTaxable && isServiceChargeLike) {
          classificationStatus = 'non_taxable';
          reason = 'No tax was charged on a service-charge/fee line item, so it is treated as non-taxable.';
          nonTaxableCount += 1;
        } else if (!observedTaxable && hasCatalogMatch && !configuredTaxable) {
          classificationStatus = 'non_taxable';
          reason = 'No tax was charged on the order and the catalog item has no tax configured in Square.';
          nonTaxableCount += 1;
        } else if (!observedTaxable && !hasCatalogMatch) {
          classificationStatus = 'non_taxable';
          reason = 'No tax was charged on a line item with no catalog match, so it is treated as non-taxable.';
          nonTaxableCount += 1;
        } else if (!observedTaxable && configuredTaxable) {
          classificationStatus = 'needs_review';
          reason = 'The catalog item is configured as taxable in Square, but no tax was charged on this order.';
          needsReviewCount += 1;
        } else if (observedTaxable && !configuredTaxable) {
          classificationStatus = 'needs_review';
          reason = 'Tax was charged on the order, but the catalog item has no tax configured in Square.';
          needsReviewCount += 1;
        } else {
          needsReviewCount += 1;
        }

        return {
          order_name: itemName,
          variation_name: variationName || null,
          quantity: item.quantity || '1',
          gross_sales: (item.gross_sales_money?.amount || 0) / 100,
          tax: observedTax,
          total: (item.total_money?.amount || 0) / 100,
          configured_taxable_in_square: configuredTaxable,
          observed_taxable_in_order: observedTaxable,
          classification_status: classificationStatus,
          reason,
          catalog_match_found: hasCatalogMatch,
          catalog: catalogItem ? {
            id: catalogItem.id,
            name: catalogItem.name,
            description: catalogItem.description,
            product_type: catalogItem.product_type,
            category: catalogItem.category,
            category_id: catalogItem.category_id,
            tax_ids: catalogItem.tax_ids,
            taxes: catalogItem.taxes,
            variations: catalogItem.variations
          } : null
        };
      });

      return {
        order_id: order.id,
        created_at: order.created_at || payment?.created_at || null,
        location_id: order.location_id || payment?.location_id || null,
        payment_id: payment?.id || null,
        square_customer_id: payment?.customer_id || null,
        note: payment?.note || '',
        tax_collected: (order.total_tax_money?.amount || 0) / 100,
        line_items: classifiedLineItems
      };
    });

    res.json({
      period: {
        start: start || null,
        end: end || null,
        period: period || null,
        location_id: location_id || null,
        customer_id: customer_id || null
      },
      source: 'classification-layer-v1',
      assumptions: [
        'configured_taxable_in_square is based on whether the matched catalog item has at least one tax_id',
        'observed_taxable_in_order is based on whether line item tax is greater than zero',
        'needs_review means Square configuration and observed order tax behavior do not match',
        'next step is to aggregate this into a filing-ready summary by state and period'
      ],
      counts: {
        payments: filteredPayments.length,
        orders: classifiedOrders.length,
        classified_lines: taxableCount + nonTaxableCount + needsReviewCount,
        needs_review: needsReviewCount
      },
      summary: {
        taxable: taxableCount,
        non_taxable: nonTaxableCount,
        needs_review: needsReviewCount
      },
      orders: classifiedOrders
    });
  } catch (err) {
    console.error('SQUARE CLASSIFICATION LAYER ERROR:', err.response?.data || err.message);
    res.status(500).json(err.response?.data || { error: err.message });
  }
});


app.get('/filing-summary', async (req, res) => {
  try {
    const { location_id, customer_id } = req.query;
    const { start, end, period } = resolveDateRange(req.query);

    const classificationResponse = await axios.get(`http://localhost:${PORT}/classification-layer`, {
      params: { start, end, period, location_id, customer_id }
    });

    const data = classificationResponse.data;
    const orders = data.orders || [];

    let grossSalesIncludingTax = 0;
    let grossSalesBeforeTax = 0;
    let taxableSales = 0;
    let nonTaxableSales = 0;
    let needsReviewSales = 0;
    let taxCollected = 0;
    let reviewCount = 0;

    orders.forEach(order => {
      taxCollected += order.tax_collected || 0;

      (order.line_items || []).forEach(item => {
        const total = item.total || 0;
        const tax = item.tax || 0;
        const beforeTax = total - tax;

        grossSalesIncludingTax += total;
        grossSalesBeforeTax += beforeTax;

        if (item.classification_status === 'taxable') {
          taxableSales += total;
        } else if (item.classification_status === 'non_taxable') {
          nonTaxableSales += total;
        } else {
          needsReviewSales += total;
          reviewCount += 1;
        }
      });
    });

    res.json({
      period: {
        start: start || null,
        end: end || null,
        period: period || null,
        location_id: location_id || null,
        customer_id: customer_id || null
      },
      source: 'filing-summary-v1',
      assumptions: [
        'taxable_sales are based on classification layer results',
        'needs_review should be reviewed before filing',
        'gross_sales_including_tax uses line item total amounts',
        'gross_sales_before_tax subtracts line item tax from line item totals'
      ],
      totals: {
        gross_sales_before_tax: grossSalesBeforeTax,
        gross_sales_including_tax: grossSalesIncludingTax,
        taxable_sales: taxableSales,
        non_taxable_sales: nonTaxableSales,
        needs_review_sales: needsReviewSales,
        tax_collected: taxCollected,
        review_count: reviewCount
      }
    });
  } catch (err) {
    console.error('FILING SUMMARY ERROR:', err.response?.data || err.message);
    res.status(500).json(err.response?.data || { error: err.message });
  }
});

async function getCustomerMapping(sheets, squareCustomerId) {
  const fallback = {
    internalCustomerId: squareCustomerId || 'UNKNOWN',
    squareCustomerId: squareCustomerId || '',
    state: 'UNKNOWN',
    name: '',
    businessName: '',
    filingFrequency: 'Monthly'
  };

  if (!squareCustomerId) {
    return fallback;
  }

  try {
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: process.env.GOOGLE_SHEETS_SPREADSHEET_ID,
      range: 'Customers!A:Z'
    });

    const rows = response.data.values || [];
    if (rows.length < 4) {
      return fallback;
    }

    const headerRowIndex = 2;
    const headers = (rows[headerRowIndex] || []).map(header => String(header || '').trim().toLowerCase());
    const getIndex = (...names) => {
      const match = names.find(name => headers.includes(name));
      return match ? headers.indexOf(match) : -1;
    };

    const stateIndex = getIndex('state');
    const internalCustomerIdIndex = getIndex('customer id', 'internal customer id', 'id');
    const squareCustomerIdIndex = getIndex('square merchant id', 'square customer id', 'square id', 'square_customer_id');
    const nameIndex = getIndex('name', 'customer name');
    const businessNameIndex = getIndex('business name', 'business', 'business nm.', 'business nm');
    const filingFrequencyIndex = getIndex('filing frequency', 'frequency', 'period type');

    if (squareCustomerIdIndex === -1) {
      return fallback;
    }

    for (let i = headerRowIndex + 1; i < rows.length; i += 1) {
      const row = rows[i] || [];
      const existingSquareCustomerId = String(row[squareCustomerIdIndex] || '').trim();

      if (existingSquareCustomerId === squareCustomerId) {
        return {
          internalCustomerId: row[internalCustomerIdIndex] || squareCustomerId,
          squareCustomerId,
          state: row[stateIndex] || 'UNKNOWN',
          name: row[nameIndex] || '',
          businessName: row[businessNameIndex] || '',
          filingFrequency: row[filingFrequencyIndex] || 'Monthly'
        };
      }
    }

    return fallback;
  } catch (error) {
    console.error('CUSTOMER MAPPING ERROR:', error.response?.data || error.message);
    return fallback;
  }
}

async function getCustomerRecordByInternalId(sheets, internalCustomerId) {
  const fallback = {
    internalCustomerId: internalCustomerId || '',
    squareCustomerId: '',
    state: 'UNKNOWN',
    name: '',
    businessName: '',
    filingFrequency: 'Monthly'
  };

  if (!internalCustomerId) {
    return fallback;
  }

  try {
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: process.env.GOOGLE_SHEETS_SPREADSHEET_ID,
      range: 'Customers!A:Z'
    });

    const rows = response.data.values || [];
    if (rows.length < 4) {
      return fallback;
    }

    const headerRowIndex = 2;
    const headers = (rows[headerRowIndex] || []).map(header => String(header || '').trim().toLowerCase());
    const getIndex = (...names) => {
      const match = names.find(name => headers.includes(name));
      return match ? headers.indexOf(match) : -1;
    };

    const stateIndex = getIndex('state');
    const internalCustomerIdIndex = getIndex('customer id', 'internal customer id', 'id');
    const squareCustomerIdIndex = getIndex('square merchant id', 'square customer id', 'square id', 'square_customer_id');
    const nameIndex = getIndex('name', 'customer name');
    const businessNameIndex = getIndex('business name', 'business', 'business nm.', 'business nm');
    const filingFrequencyIndex = getIndex('filing frequency', 'frequency', 'period type');

    if (internalCustomerIdIndex === -1) {
      return fallback;
    }

    for (let i = headerRowIndex + 1; i < rows.length; i += 1) {
      const row = rows[i] || [];
      const existingInternalCustomerId = String(row[internalCustomerIdIndex] || '').trim();

      if (existingInternalCustomerId === String(internalCustomerId).trim()) {
        return {
          internalCustomerId: existingInternalCustomerId || internalCustomerId,
          squareCustomerId: row[squareCustomerIdIndex] || '',
          state: row[stateIndex] || 'UNKNOWN',
          name: row[nameIndex] || '',
          businessName: row[businessNameIndex] || '',
          filingFrequency: row[filingFrequencyIndex] || 'Monthly'
        };
      }
    }

    return fallback;
  } catch (error) {
    console.error('CUSTOMER RECORD LOOKUP ERROR:', error.response?.data || error.message);
    return fallback;
  }
}


app.get('/push-to-sheets', async (req, res) => {
  try {
    const { location_id, customer_id } = req.query;
    const { start, end, period } = resolveDateRange(req.query);

    if (!process.env.GOOGLE_SHEETS_SPREADSHEET_ID || !process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL || !process.env.GOOGLE_PRIVATE_KEY) {
      return res.status(500).json({
        error: 'Missing Google Sheets environment variables. Add GOOGLE_SHEETS_SPREADSHEET_ID, GOOGLE_SERVICE_ACCOUNT_EMAIL, and GOOGLE_PRIVATE_KEY to .env'
      });
    }

    const filingResponse = await axios.get(`http://localhost:${PORT}/filing-summary`, {
      params: { start, end, period, location_id, customer_id }
    });

    const filingData = filingResponse.data;
    const totals = filingData.totals || {};

    const classificationResponse = await axios.get(`http://localhost:${PORT}/classification-layer`, {
      params: { start, end, period, location_id, customer_id }
    });

    const classificationData = classificationResponse.data;
    const classificationCounts = classificationData.counts || {};
    const orders = classificationData.orders || [];
    const firstOrder = orders[0] || {};
    const firstLine = (firstOrder.line_items || [])[0] || {};
    const periodValue = buildPeriodLabel(start, end, period);
    const periodStart = start || '';
    const periodEnd = end || '';
    const sheets = await getSheetsClient();
    const customerRecord = await getCustomerRecordByInternalId(sheets, customer_id);
    const squareCustomerId = customerRecord.squareCustomerId || firstOrder.square_customer_id || 'UNKNOWN';
    const customerMapping = customerRecord.squareCustomerId
      ? customerRecord
      : await getCustomerMapping(sheets, squareCustomerId);
    const customerId = customerMapping.internalCustomerId || squareCustomerId;
    const businessName = customerMapping.businessName || firstLine.catalog?.name || firstLine.order_name || 'Unknown Business';
    const customerName = customerMapping.name || '';
    const state = customerMapping.state || 'UNKNOWN';
    const periodType = customerMapping.filingFrequency || 'Monthly';
    const status = totals.review_count > 0 ? 'Needs Review' : 'Ready';
    const syncTimestamp = new Date();

    const filingsRow = [
      state,
      periodValue,
      periodType,
      customerId,
      customerName,
      businessName,
      totals.gross_sales_before_tax || 0,
      totals.gross_sales_including_tax || 0,
      totals.taxable_sales || 0,
      totals.non_taxable_sales || 0,
      totals.needs_review_sales || 0,
      totals.tax_collected || 0,
      totals.review_count || 0,
      classificationCounts.payments || 0,
      classificationCounts.orders || 0,
      status,
      '',
      '',
      '',
      ''
    ];

    const filingsReadResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: process.env.GOOGLE_SHEETS_SPREADSHEET_ID,
      range: 'Filings!A:T'
    });

    const existingFilingsRows = filingsReadResponse.data.values || [];
    let filingsRowNumber = null;

    for (let i = 1; i < existingFilingsRows.length; i += 1) {
      const row = existingFilingsRows[i] || [];
      const existingPeriod = row[1] || '';
      const existingCustomerId = row[3] || '';

      if (existingPeriod === periodValue && existingCustomerId === customerId) {
        filingsRowNumber = i + 1;
        break;
      }
    }

    // === LOCK CHECK ===
    if (filingsRowNumber) {
      const lockedValue = existingFilingsRows[filingsRowNumber - 1][16]; // column Q = Locked

      if (String(lockedValue).toLowerCase() === 'true') {
        return res.status(400).json({
          success: false,
          error: 'This filing is LOCKED and cannot be overwritten.',
          period: periodValue,
          customer_id: customerId
        });
      }
    }

    let filingsAction = 'appended';

    if (filingsRowNumber) {
      await sheets.spreadsheets.values.update({
        spreadsheetId: process.env.GOOGLE_SHEETS_SPREADSHEET_ID,
        range: `Filings!A${filingsRowNumber}:T${filingsRowNumber}`,
        valueInputOption: 'USER_ENTERED',
        requestBody: {
          values: [filingsRow]
        }
      });
      filingsAction = 'updated';
    } else {
      await sheets.spreadsheets.values.append({
        spreadsheetId: process.env.GOOGLE_SHEETS_SPREADSHEET_ID,
        range: 'Filings!A:T',
        valueInputOption: 'USER_ENTERED',
        requestBody: {
          values: [filingsRow]
        }
      });
    }

    const removedReviewRows = await removeExistingReviewRowsForPeriod(sheets, customerId, periodValue);

    const reviewRows = [];
    orders.forEach(order => {
      // Only include orders for this customer (prevents UNKNOWN duplicates)
      if (order.square_customer_id && order.square_customer_id !== squareCustomerId) {
        return;
      }
      (order.line_items || []).forEach(item => {
        if (item.classification_status === 'needs_review') {
          reviewRows.push([
            state,
            periodValue,
            periodStart,
            periodEnd,
            customerId,
            squareCustomerId,
            businessName,
            order.order_id || '',
            item.catalog?.name || item.order_name || 'Unnamed item',
            item.reason || 'Needs review',
            item.total || 0,
            'Review Square tax setup before filing.',
            false,
            ''
          ]);
        }
      });
    });

    let reviewRowsToWrite = reviewRows;

    if (reviewRowsToWrite.length > 0) {
      await sheets.spreadsheets.values.append({
        spreadsheetId: process.env.GOOGLE_SHEETS_SPREADSHEET_ID,
        range: 'Review Queue!A:N',
        valueInputOption: 'USER_ENTERED',
        requestBody: {
          values: reviewRowsToWrite
        }
      });
    }
    const customerLastSyncAction = await syncCustomerLastSyncDate(sheets, customerId, syncTimestamp);

    res.json({
      success: true,
      message: 'Pushed filing summary to Google Sheets.',
      filings_row_written: true,
      filings_action: filingsAction,
      review_rows_written: reviewRowsToWrite.length,
      review_rows_removed: removedReviewRows,
      customer_last_sync_action: customerLastSyncAction
    });
  } catch (err) {
    console.error('SHEETS PUSH ERROR:', err.response?.data || err.message);
    res.status(500).json(err.response?.data || { error: err.message });
  }
});

app.get('/debug-env', (req, res) => {
  res.json({
    hasClientId: !!process.env.SQUARE_CLIENT_ID,
    hasClientSecret: !!process.env.SQUARE_CLIENT_SECRET,
    hasAccessToken: !!process.env.SQUARE_ACCESS_TOKEN,
    clientIdPrefix: process.env.SQUARE_CLIENT_ID?.slice(0, 18) || null,
    accessTokenPrefix: process.env.SQUARE_ACCESS_TOKEN?.slice(0, 8) || null,
    squareBaseUrl: SQUARE_BASE_URL,
    hasGoogleSpreadsheetId: !!process.env.GOOGLE_SHEETS_SPREADSHEET_ID,
    hasGoogleServiceAccountEmail: !!process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL,
    hasGooglePrivateKey: !!process.env.GOOGLE_PRIVATE_KEY,
    googleServiceAccountEmail: process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL || null,
    googlePrivateKeyPrefix: process.env.GOOGLE_PRIVATE_KEY ? process.env.GOOGLE_PRIVATE_KEY.slice(0, 30) : null,
    sampleManualPullExample: '/push-to-sheets?customer_id=CUS-0001&period=03.26'
  });
});

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});