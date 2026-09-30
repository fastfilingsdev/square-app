'use strict';

// Process-local admission control, not a distributed lock or payment retry tool.
// Set the saved flag before a controlled restart: a signal alone is not durable.
function createMaintenance({ env = process.env } = {}) {
  const flag = String(env.FF_FOUNDATION_MAINTENANCE || '').trim();
  let paused = flag !== '' && !/^(0|false|no|off)$/i.test(flag);
  let active = 0, uncertain = 0;
  function enter() {
    if (paused) return null;
    active++;
    let released = false;
    return (abandoned = false) => {
      if (released) return;
      released = true; active--;
      if (abandoned) uncertain++;
    };
  }
  function status() {
    return { paused, active, uncertain, drained: paused && active === 0 && uncertain === 0 };
  }
  function pause() { paused = true; return status(); }
  const health = new Set(['/authnet/health', '/foundation-maintenance/health']);
  function middleware(req, res, next) {
    if ((req.method === 'GET' || req.method === 'HEAD') && health.has(req.path)) return next();
    const release = enter();
    if (!release) {
      res.set({ 'Cache-Control': 'no-store', 'Retry-After': '60' });
      return res.status(503).json({ ok: false, error: 'maintenance', operationStarted: false });
    }
    // A disconnected request may still be doing work. Never claim it drained.
    res.once('finish', () => release(res.statusCode >= 500));
    res.once('close', () => release(!res.writableFinished));
    try { return next(); } catch (error) { release(true); throw error; }
  }
  return { enter, pause, status, middleware };
}

const maintenance = createMaintenance();
module.exports = { createMaintenance, maintenance };
