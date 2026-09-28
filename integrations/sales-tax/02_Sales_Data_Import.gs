// Candidate replacement for 02_Sales_Data_Import.gs; NOT installed in production.
// Roll out only alongside the authenticated POST backend. Configure the matching
// FF_SYNC_ADMIN_TOKEN in Script Properties, never in a sheet cell or source file.
function runSalesDataSQ() { ffSqRunImport_(false); }
function runSalesDataSQSelectedRow() { ffSqRunImport_(true); }

function ffSqRunImport_(selected) {
  const ui = SpreadsheetApp.getUi();
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheet = ss.getSheetByName('Customers');
    if (!sheet) throw new Error('Missing Customers tab.');
    if (selected && ss.getActiveSheet().getSheetId() !== sheet.getSheetId()) {
      throw new Error('Select a row on the Customers tab first.');
    }
    const initial = sheet.getDataRange().getValues();
    const columns = ffSqColumns_(initial);
    let id;
    if (selected) {
      const rowNumber = sheet.getActiveCell().getRow();
      if (rowNumber < 4 || rowNumber > initial.length) throw new Error('Select a customer row below the headers.');
      id = String(initial[rowNumber - 1][columns.id] || '').trim();
    } else {
      const answer = ui.prompt('Run Sales Data (SQ)', 'Enter Customer ID', ui.ButtonSet.OK_CANCEL);
      if (answer.getSelectedButton() !== ui.Button.OK) return;
      id = String(answer.getResponseText() || '').trim();
    }
    const customer = ffSqCustomer_(sheet.getDataRange().getValues(), id);
    if (customer.status && customer.status.toLowerCase() !== 'active') {
      if (ui.alert('Customer not Active', 'Continue for this customer?', ui.ButtonSet.YES_NO) !== ui.Button.YES) return;
    }
    if (customer.connected && !['yes', 'true', '1'].includes(customer.connected.toLowerCase())) {
      if (ui.alert('Square not marked connected', 'Continue for this customer?', ui.ButtonSet.YES_NO) !== ui.Button.YES) return;
    }
    const answer = ui.prompt('Select Period', 'Monthly: 03.26 or 2026-03. Quarterly: Q1 2026 or 2026-Q1.', ui.ButtonSet.OK_CANCEL);
    if (answer.getSelectedButton() !== ui.Button.OK) return;
    const period = String(answer.getResponseText() || '').trim();
    // Prompts suspend execution: re-resolve identity, not a remembered row number.
    const current = ffSqCustomer_(sheet.getDataRange().getValues(), id);
    if (current.merchant !== customer.merchant || current.status !== customer.status || current.connected !== customer.connected) {
      throw new Error('Customer changed while the dialog was open. Review the row before starting again.');
    }
    ffSqPush_(id, period);
    // The backend atomically stamps Last Sync. Never write a stale local row here.
    ui.alert('Run complete', 'Backend confirmed the filing update for ' + id + '.', ui.ButtonSet.OK);
  } catch (error) {
    // Only locally generated, non-secret errors leave this adapter.
    ui.alert('Run stopped', error.message, ui.ButtonSet.OK);
  }
}

function ffSqNormalize_(value) { return String(value || '').toLowerCase().replace(/[^a-z0-9]/g, ''); }
function ffSqColumns_(values) {
  if (values.length < 4) throw new Error('Customers tab is empty.');
  const headers = values[2].map(ffSqNormalize_);
  function column(aliases, required) {
    const matches = [];
    headers.forEach(function (h, i) { if (aliases.indexOf(h) !== -1) matches.push(i); });
    if (matches.length > 1 || (required && matches.length !== 1)) throw new Error('Missing or ambiguous customer headers.');
    return matches.length ? matches[0] : -1;
  }
  return {
    id: column(['customerid', 'internalcustomerid', 'id'], true),
    merchant: column(['squaremerchantid', 'squarecustomerid', 'squareid'], true),
    status: column(['status'], false), connected: column(['squareconnected'], false)
  };
}
function ffSqCustomer_(values, id) {
  if (!id) throw new Error('Customer ID is required.');
  const cols = ffSqColumns_(values);
  const rows = values.slice(3).filter(function (row) { return String(row[cols.id] || '').trim() === id; });
  if (rows.length !== 1) throw new Error('Customer ID must match exactly one row.');
  const row = rows[0];
  const merchant = String(row[cols.merchant] || '').trim();
  if (!merchant) throw new Error('Square Merchant ID is required.');
  return { merchant: merchant, status: String(row[cols.status] || '').trim(), connected: String(row[cols.connected] || '').trim() };
}

function ffSqPush_(id, period) {
  if (!id || !period) throw new Error('Customer ID and period are required.');
  const token = String(PropertiesService.getScriptProperties().getProperty('FF_SYNC_ADMIN_TOKEN') || '').trim();
  if (!token) throw new Error('Reporting authentication is not configured. No request was sent.');
  const url = 'https://fastfilings-api.onrender.com/push-to-sheets?customer_id=' + encodeURIComponent(id) + '&period=' + encodeURIComponent(period);
  let response;
  try {
    response = UrlFetchApp.fetch(url, {
      method: 'post', muteHttpExceptions: true, followRedirects: false,
      headers: { Accept: 'application/json', 'x-ff-sync-token': token }
    });
  } catch (_) {
    throw new Error('Request outcome unknown. Check backend and sheet results before any rerun; do not retry automatically.');
  }
  const code = response.getResponseCode();
  let data;
  try { data = JSON.parse(response.getContentText()); } catch (_) { data = null; }
  if (code >= 200 && code < 300 && data && data.success === true) return;
  if (code === 401 || code === 403 || code === 405) {
    throw new Error('Reporting access or backend version mismatch (HTTP ' + code + '). No automatic retry.');
  }
  throw new Error('Backend did not confirm success (HTTP ' + code + '). Reconcile sheet results before any rerun; no automatic retry.');
}
