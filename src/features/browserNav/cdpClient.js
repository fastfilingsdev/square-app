const WebSocket = require('ws');

function trimTrailingSlash(value) {
  return String(value || '').replace(/\/+$/, '');
}

async function resolveCdpWebSocketUrl(profile = {}) {
  const direct = String(profile.cdpUrl || '').trim();
  if (direct.startsWith('ws://') || direct.startsWith('wss://')) return direct;

  const httpBase = trimTrailingSlash(profile.cdpHttpUrl || direct);
  if (!httpBase) throw new Error('Browser profile missing cdpUrl/cdpHttpUrl');
  if (!/^https?:\/\//i.test(httpBase)) throw new Error('Browser CDP endpoint must be ws(s) or http(s)');

  const response = await fetch(`${httpBase}/json/version`, { method: 'GET' });
  if (!response.ok) {
    throw new Error(`Unable to resolve CDP websocket URL: HTTP ${response.status}`);
  }
  const json = await response.json();
  const wsUrl = String(json.webSocketDebuggerUrl || '').trim();
  if (!wsUrl) throw new Error('CDP /json/version response missing webSocketDebuggerUrl');
  return wsUrl;
}

class CdpConnection {
  constructor(wsUrl, { WebSocketImpl = WebSocket } = {}) {
    this.wsUrl = wsUrl;
    this.WebSocketImpl = WebSocketImpl;
    this.ws = null;
    this.nextId = 1;
    this.pending = new Map();
    this.eventWaiters = [];
  }

  async connect(timeoutMs = 15000) {
    if (this.ws) return this;
    await new Promise((resolve, reject) => {
      const ws = new this.WebSocketImpl(this.wsUrl);
      this.ws = ws;
      const timer = setTimeout(() => {
        reject(new Error('Timed out connecting to browser CDP endpoint'));
        try { ws.close(); } catch (err) {}
      }, timeoutMs);
      ws.once('open', () => {
        clearTimeout(timer);
        resolve();
      });
      ws.once('error', err => {
        clearTimeout(timer);
        reject(err);
      });
      ws.on('message', data => this._handleMessage(data));
      ws.on('close', () => this._rejectAll(new Error('Browser CDP websocket closed')));
    });
    return this;
  }

  _rejectAll(err) {
    for (const pending of this.pending.values()) {
      clearTimeout(pending.timer);
      pending.reject(err);
    }
    this.pending.clear();
    for (const waiter of this.eventWaiters.splice(0)) {
      clearTimeout(waiter.timer);
      waiter.reject(err);
    }
  }

  _handleMessage(data) {
    let msg;
    try {
      msg = JSON.parse(String(data));
    } catch (err) {
      return;
    }

    if (msg.id && this.pending.has(msg.id)) {
      const pending = this.pending.get(msg.id);
      this.pending.delete(msg.id);
      clearTimeout(pending.timer);
      if (msg.error) pending.reject(new Error(msg.error.message || JSON.stringify(msg.error)));
      else pending.resolve(msg.result || {});
      return;
    }

    if (msg.method) {
      const waiters = this.eventWaiters.slice();
      for (const waiter of waiters) {
        if (waiter.method !== msg.method) continue;
        if (waiter.sessionId && waiter.sessionId !== msg.sessionId) continue;
        if (waiter.predicate && !waiter.predicate(msg)) continue;
        this.eventWaiters = this.eventWaiters.filter(item => item !== waiter);
        clearTimeout(waiter.timer);
        waiter.resolve(msg);
      }
    }
  }

  send(method, params = {}, sessionId = null, timeoutMs = 15000) {
    if (!this.ws || this.ws.readyState !== this.WebSocketImpl.OPEN) {
      return Promise.reject(new Error('Browser CDP websocket is not open'));
    }
    const id = this.nextId++;
    const payload = { id, method, params };
    if (sessionId) payload.sessionId = sessionId;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`Timed out waiting for CDP response: ${method}`));
      }, timeoutMs);
      this.pending.set(id, { resolve, reject, timer });
      this.ws.send(JSON.stringify(payload), err => {
        if (err) {
          this.pending.delete(id);
          clearTimeout(timer);
          reject(err);
        }
      });
    });
  }

  waitForEvent(method, { sessionId = null, timeoutMs = 15000, predicate = null } = {}) {
    return new Promise((resolve, reject) => {
      const waiter = { method, sessionId, predicate, resolve, reject, timer: null };
      waiter.timer = setTimeout(() => {
        this.eventWaiters = this.eventWaiters.filter(item => item !== waiter);
        reject(new Error(`Timed out waiting for CDP event: ${method}`));
      }, timeoutMs);
      this.eventWaiters.push(waiter);
    });
  }

  close() {
    try {
      if (this.ws) this.ws.close();
    } catch (err) {}
    this.ws = null;
  }
}

async function openCdpPage(profile = {}, { WebSocketImpl = WebSocket } = {}) {
  const wsUrl = await resolveCdpWebSocketUrl(profile);
  const connection = new CdpConnection(wsUrl, { WebSocketImpl });
  await connection.connect();

  let targetId = null;
  let sessionId = null;
  try {
    const created = await connection.send('Target.createTarget', { url: profile.defaultUrl || 'about:blank' });
    targetId = created.targetId || null;
    const attached = await connection.send('Target.attachToTarget', { targetId, flatten: true });
    sessionId = attached.sessionId || null;
  } catch (err) {
    // Some CDP URLs point directly at a page target instead of a browser target. In that case use the root session.
    targetId = null;
    sessionId = null;
  }

  await connection.send('Page.enable', {}, sessionId).catch(() => {});
  await connection.send('Runtime.enable', {}, sessionId).catch(() => {});
  await connection.send('DOM.enable', {}, sessionId).catch(() => {});

  return {
    connection,
    targetId,
    sessionId,
    async close() {
      if (targetId) {
        await connection.send('Target.closeTarget', { targetId }, null, 5000).catch(() => {});
      }
      connection.close();
    }
  };
}

module.exports = {
  CdpConnection,
  openCdpPage,
  resolveCdpWebSocketUrl
};
