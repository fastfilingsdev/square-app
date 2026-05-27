const PAYMENT_UPDATE_TICKET_HEADERS = [
  'Ticket ID',
  'Payment Update Row',
  'Payment Update Type',
  'Source Tab',
  'Source Row',
  'Subscription ID',
  'Ticket Status',
  'Link Created At',
  'Link Expires At',
  'Last Token Generated At',
  'Last Click At',
  'Completed At',
  'Revoked At',
  'AuthNet Customer Profile Found?',
  'AuthNet Payment Profile Found?',
  'Decline Category',
  'Notes'
];

const TICKETS_TAB = 'Payment Update Link Tickets';

const TEST_READY_STATUSES = new Set(['Draft/Test', 'Test Link Ready']);
const LIVE_READY_STATUSES = new Set(['Live Link Ready', 'Link Ready', 'Payment Update Link Ready']);

function getPaymentUpdateSpreadsheetId() {
  const id = process.env.PAYMENT_UPDATE_SPREADSHEET_ID || process.env.GOOGLE_SHEETS_SPREADSHEET_ID || '';
  if (!id) {
    throw new Error('Missing PAYMENT_UPDATE_SPREADSHEET_ID');
  }
  return id;
}

function rowValue(row, index, name) {
  const i = index[name];
  return i > -1 ? String(row[i] || '').trim() : '';
}

function validateTicketHeaders(headers) {
  const expectedHeaderPrefix = PAYMENT_UPDATE_TICKET_HEADERS.join('\u0001');
  const actualHeaderPrefix = headers.slice(0, PAYMENT_UPDATE_TICKET_HEADERS.length).join('\u0001');
  if (actualHeaderPrefix !== expectedHeaderPrefix) {
    throw new Error('Payment Update Link Tickets headers do not match expected schema');
  }
}

async function findPaymentUpdateTicket(sheets, ticketId, spreadsheetId = getPaymentUpdateSpreadsheetId()) {
  const response = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: `'${TICKETS_TAB}'!A:Q`
  });

  const rows = response.data.values || [];
  if (!rows.length) return null;

  const headers = rows[0].map(header => String(header || '').trim());
  validateTicketHeaders(headers);
  const index = Object.fromEntries(headers.map((header, i) => [header, i]));

  for (let i = 1; i < rows.length; i += 1) {
    const row = rows[i] || [];
    if (rowValue(row, index, 'Ticket ID') !== ticketId) continue;

    return {
      rowNumber: i + 1,
      ticketId: rowValue(row, index, 'Ticket ID'),
      paymentUpdateRow: rowValue(row, index, 'Payment Update Row'),
      paymentUpdateType: rowValue(row, index, 'Payment Update Type'),
      sourceTab: rowValue(row, index, 'Source Tab'),
      sourceRow: rowValue(row, index, 'Source Row'),
      subscriptionId: rowValue(row, index, 'Subscription ID'),
      ticketStatus: rowValue(row, index, 'Ticket Status'),
      linkCreatedAt: rowValue(row, index, 'Link Created At'),
      linkExpiresAt: rowValue(row, index, 'Link Expires At'),
      completedAt: rowValue(row, index, 'Completed At'),
      revokedAt: rowValue(row, index, 'Revoked At')
    };
  }

  return null;
}

function httpError(message, statusCode) {
  const err = new Error(message);
  err.statusCode = statusCode;
  return err;
}

function validatePaymentUpdateTicket(ticket, { testOnly = true } = {}) {
  if (!ticket) throw httpError('Payment update link was not found', 404);
  if (testOnly && !String(ticket.ticketId || '').startsWith('pu_test_')) {
    throw httpError('This test route only accepts test payment-update tickets', 403);
  }
  if (!testOnly && String(ticket.ticketId || '').startsWith('pu_test_')) {
    throw httpError('This live route does not accept test payment-update tickets', 403);
  }
  if (ticket.revokedAt) throw httpError('This payment update link has been revoked', 410);
  if (ticket.completedAt) throw httpError('This payment update link has already been completed', 410);
  const readyStatuses = testOnly ? TEST_READY_STATUSES : LIVE_READY_STATUSES;
  if (!readyStatuses.has(ticket.ticketStatus)) {
    throw httpError(testOnly ? 'This payment update link is not ready for testing' : 'This payment update link is not ready', 403);
  }
  if (ticket.linkExpiresAt) {
    const expiresAt = new Date(ticket.linkExpiresAt);
    if (Number.isNaN(expiresAt.getTime())) {
      throw httpError('This payment update link has a malformed expiration date', 500);
    }
    if (expiresAt.getTime() < Date.now()) {
      throw httpError('This payment update link has expired', 410);
    }
  }
  if (!ticket.subscriptionId) throw httpError('This payment update ticket is missing a subscription ID', 500);
}

async function updatePaymentUpdateTicketAudit(sheets, ticket, fields, spreadsheetId = getPaymentUpdateSpreadsheetId()) {
  if (!ticket?.rowNumber) return;

  const now = new Date().toISOString();
  const values = {
    lastTokenGeneratedAt: fields.lastTokenGeneratedAt || '',
    lastClickAt: fields.lastClickAt || ''
  };

  const data = [];
  if (values.lastTokenGeneratedAt) {
    data.push({ range: `'${TICKETS_TAB}'!J${ticket.rowNumber}`, values: [[values.lastTokenGeneratedAt === true ? now : values.lastTokenGeneratedAt]] });
  }
  if (values.lastClickAt) {
    data.push({ range: `'${TICKETS_TAB}'!K${ticket.rowNumber}`, values: [[values.lastClickAt === true ? now : values.lastClickAt]] });
  }

  if (!data.length) return;

  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId,
    requestBody: {
      valueInputOption: 'RAW',
      data
    }
  });
}

module.exports = {
  PAYMENT_UPDATE_TICKET_HEADERS,
  TICKETS_TAB,
  findPaymentUpdateTicket,
  getPaymentUpdateSpreadsheetId,
  updatePaymentUpdateTicketAudit,
  validatePaymentUpdateTicket
};
