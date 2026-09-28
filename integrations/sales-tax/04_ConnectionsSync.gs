// Candidate replacement for BOTH old04/05 state-sync files and old06 customer-sync.
// Remove those three definitions from an isolated copy before adding this file.
// All public menu/trigger names are retained; helpers are uniquely prefixed.
function syncConnectedCustomersToSQ() { return ffSqSyncExecute_(false); }
function syncConnectionsToStates() { return ffSqSyncExecute_(true); }

function ffSqSyncTable_(values, headerRow, required) {
  if (!values[headerRow - 1]) throw new Error('Missing sync headers.');
  const map = Object.create(null);
  values[headerRow - 1].forEach(function (value, index) {
    const key = String(value || '').trim().toLowerCase().replace(/\s+/g, ' ');
    if (!key) return;
    if (map[key] !== undefined) throw new Error('Duplicate sync header.');
    map[key] = index;
  });
  required.forEach(function (key) { if (map[key] === undefined) throw new Error('Missing sync header: ' + key); });
  return { map: map, rows: values.slice(headerRow), start: headerRow + 1, width: values[headerRow - 1].length };
}
function ffSqSyncIndex_(table, key) {
  const out = Object.create(null);
  table.rows.forEach(function (row, index) {
    const id = String(row[table.map[key]] || '').trim();
    if (!id) return;
    if (out[id]) throw new Error('Duplicate sync identity; no writes started.');
    out[id] = { row: row, number: table.start + index };
  });
  return out;
}
function ffSqSyncPlan_(connectionsValues, configValues, customersValues, stateValues, toStates) {
  const con = ffSqSyncTable_(connectionsValues, 1, ['customer id', 'square merchant id', 'connected']);
  const cfg = ffSqSyncTable_(configValues, 1, ['state', 'spreadsheet id', 'customers tab', 'active']);
  const sq = ffSqSyncTable_(customersValues, 3, ['customer id', 'square merchant id', 'state', 'name', 'business name', 'filing frequency', 'status', 'square connected', 'date added', 'last sync', 'notes']);
  const connections = ffSqSyncIndex_(con, 'customer id');
  const configs = ffSqSyncIndex_(cfg, 'state');
  const customers = ffSqSyncIndex_(sq, 'customer id');
  const changes = [];
  Object.keys(connections).forEach(function (id) {
    const connection = connections[id].row;
    if (!['yes', 'true', '1'].includes(String(connection[con.map.connected] || '').trim().toLowerCase())) return;
    const match = id.match(/^([A-Z]{2})-/);
    if (!match || !configs[match[1]]) throw new Error('Connected customer lacks unique state configuration.');
    const state = match[1], config = configs[state].row;
    if (String(config[cfg.map.active] || '').trim().toLowerCase() !== 'yes') return;
    const merchant = String(connection[con.map['square merchant id']] || '').trim();
    if (!merchant) throw new Error('Connected customer lacks merchant identity.');
    const stateTable = ffSqSyncTable_(stateValues[state] || [], 2, ['customer id', 'platform access', 'name', 'business name', 'filing frequency', 'status']);
    const entry = ffSqSyncIndex_(stateTable, 'customer id')[id];
    if (!entry) throw new Error('Connected customer missing in state sheet; review before sync.');
    if (toStates) {
      changes.push({ state: state, id: id, row: entry.number, idCol: stateTable.map['customer id'] + 1,
        cells: [{ col: stateTable.map['platform access'] + 1, value: 'Yes' }].concat(stateTable.map['sales platform'] === undefined ? [] : [{ col: stateTable.map['sales platform'] + 1, value: 'Square' }]) });
      return;
    }
    const existing = customers[id];
    if (existing) {
      const oldMerchant = String(existing.row[sq.map['square merchant id']] || '').trim();
      if (oldMerchant && oldMerchant !== merchant) throw new Error('Merchant mapping changed; reconcile before sync.');
      const oldState = String(existing.row[sq.map.state] || '').trim();
      if (oldState && oldState !== state) throw new Error('Customer state mapping mismatch.');
    }
    const fields = { state: state, 'customer id': id, 'square merchant id': merchant, 'square connected': 'Yes' };
    ['name', 'business name', 'filing frequency', 'status'].forEach(function (key) {
      const value = entry.row[stateTable.map[key]];
      if (value !== '' && value !== null && value !== undefined) fields[key] = value;
    });
    const cells = Object.keys(fields).map(function (key) { return { col: sq.map[key] + 1, value: fields[key] }; });
    // Last Sync belongs to confirmed filing import, never connection sync.
    // Existing notes, Date Added, frequency-change metadata and formulas survive.
    if (!existing) cells.push({ col: sq.map['date added'] + 1, value: new Date() });
    changes.push({ state: null, id: id, row: existing ? existing.number : null, idCol: sq.map['customer id'] + 1, width: sq.width, cells: cells });
  });
  return changes;
}

function ffSqSyncExecute_(toStates) {
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(1000)) throw new Error('Sync busy; no automatic retry.');
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    function local(name) { const sh = ss.getSheetByName(name); if (!sh) throw new Error('Missing sync tab.'); return sh; }
    const con = local('Connections'), cfg = local('Config - States'), sq = local('Customers');
    const conValues = con.getDataRange().getValues(), cfgValues = cfg.getDataRange().getValues(), sqValues = sq.getDataRange().getValues();
    const config = ffSqSyncTable_(cfgValues, 1, ['state', 'spreadsheet id', 'customers tab', 'active']);
    const configIndex = ffSqSyncIndex_(config, 'state');
    const states = {}, stateValues = {};
    Object.keys(configIndex).forEach(function (state) {
      const row = configIndex[state].row;
      if (String(row[config.map.active] || '').trim().toLowerCase() !== 'yes') return;
      const book = String(row[config.map['spreadsheet id']] || '').trim(), tab = String(row[config.map['customers tab']] || '').trim();
      if (!book || !tab) throw new Error('Incomplete state configuration.');
      const sheet = SpreadsheetApp.openById(book).getSheetByName(tab);
      if (!sheet) throw new Error('Missing state customer tab.');
      states[state] = sheet; stateValues[state] = sheet.getDataRange().getValues();
    });
    const changes = ffSqSyncPlan_(conValues, cfgValues, sqValues, stateValues, toStates);
    // Detect changes since planning before first mutation. Apps Script locks do
    // not lock manual edits or the backend; native rollout still needs controls.
    [[con, conValues], [cfg, cfgValues], [sq, sqValues]].concat(Object.keys(states).map(function (state) { return [states[state], stateValues[state]]; })).forEach(function (pair) {
      if (JSON.stringify(pair[0].getDataRange().getValues()) !== JSON.stringify(pair[1])) throw new Error('Sync inputs changed; no writes started.');
    });
    // Validate every existing target before starting any mutation.
    changes.filter(function (change) { return change.row !== null; }).forEach(function (change) {
      const sh = change.state ? states[change.state] : sq;
      if (String(sh.getRange(change.row, change.idCol).getValue() || '').trim() !== change.id) throw new Error('Sync identity changed; no writes started.');
      change.cells.forEach(function (cell) {
        if (sh.getRange(change.row, cell.col).getFormula()) throw new Error('Sync target has formula; no writes started.');
      });
    });
    changes.forEach(function (change) {
      const sh = change.state ? states[change.state] : sq;
      if (change.row === null) {
        const row = new Array(change.width).fill('');
        change.cells.forEach(function (cell) { row[cell.col - 1] = cell.value; });
        sh.appendRow(row);
      } else {
        if (String(sh.getRange(change.row, change.idCol).getValue() || '').trim() !== change.id) throw new Error('Sync identity changed; reconcile partial result.');
        change.cells.forEach(function (cell) {
          const range = sh.getRange(change.row, cell.col);
          if (range.getFormula()) throw new Error('Sync target has formula; reconcile partial result.');
          if (range.getValue() !== cell.value) range.setValue(cell.value);
        });
      }
    });
    return { success: true, customers: changes.length };
  } finally { lock.releaseLock(); }
}

function installSyncConnectedCustomersToSQTrigger() { ffSqSyncInstall_('syncConnectedCustomersToSQ'); }
function removeSyncConnectedCustomersToSQTrigger() { ffSqSyncRemove_('syncConnectedCustomersToSQ'); }
function installSyncConnectionsTrigger() { ffSqSyncInstall_('syncConnectionsToStates'); }
function removeSyncConnectionsTrigger() { ffSqSyncRemove_('syncConnectionsToStates'); }
function ffSqSyncRemove_(handler) {
  ScriptApp.getProjectTriggers().forEach(function (trigger) { if (trigger.getHandlerFunction() === handler) ScriptApp.deleteTrigger(trigger); });
}
function ffSqSyncInstall_(handler) {
  ffSqSyncRemove_(handler);
  ScriptApp.newTrigger(handler).timeBased().everyMinutes(5).create();
}
