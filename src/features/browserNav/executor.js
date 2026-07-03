const { openCdpPage } = require('./cdpClient');

function jsString(value) {
  return JSON.stringify(String(value ?? ''));
}

async function evaluate(page, expression, { timeoutMs = 15000, returnByValue = true } = {}) {
  const result = await page.connection.send('Runtime.evaluate', {
    expression,
    awaitPromise: true,
    returnByValue,
    userGesture: true,
    timeout: timeoutMs
  }, page.sessionId, timeoutMs + 1000);

  if (result.exceptionDetails) {
    const description = result.exceptionDetails.exception?.description || result.exceptionDetails.text || 'Runtime evaluation failed';
    throw new Error(description);
  }
  return result.result?.value;
}

async function waitForSelector(page, selector, timeoutMs = 15000) {
  const expression = `(() => new Promise((resolve) => {
    const selector = ${jsString(selector)};
    const deadline = Date.now() + ${Number(timeoutMs) || 15000};
    const check = () => {
      const el = document.querySelector(selector);
      if (el) return resolve({ ok: true, selector });
      if (Date.now() >= deadline) return resolve({ ok: false, error: 'selector not found', selector });
      setTimeout(check, 100);
    };
    check();
  }))()`;
  const result = await evaluate(page, expression, { timeoutMs: timeoutMs + 1000 });
  if (!result?.ok) throw new Error(result?.error || `Selector not found: ${selector}`);
  return result;
}

async function clickSelector(page, selector, timeoutMs) {
  await waitForSelector(page, selector, timeoutMs);
  const expression = `(() => {
    const el = document.querySelector(${jsString(selector)});
    if (!el) return { ok: false, error: 'selector not found' };
    el.scrollIntoView({ block: 'center', inline: 'center' });
    el.click();
    return { ok: true, tagName: el.tagName, text: (el.innerText || el.value || '').slice(0, 200) };
  })()`;
  const result = await evaluate(page, expression, { timeoutMs });
  if (!result?.ok) throw new Error(result?.error || `Click failed: ${selector}`);
  return { ok: true, action: 'click', selector, tagName: result.tagName || null, textPreview: result.text || '' };
}

async function fillSelector(page, selector, text, timeoutMs) {
  await waitForSelector(page, selector, timeoutMs);
  const expression = `(() => {
    const el = document.querySelector(${jsString(selector)});
    if (!el) return { ok: false, error: 'selector not found' };
    el.scrollIntoView({ block: 'center', inline: 'center' });
    el.focus();
    const value = ${jsString(text)};
    el.value = value;
    el.dispatchEvent(new Event('input', { bubbles: true }));
    el.dispatchEvent(new Event('change', { bubbles: true }));
    return { ok: true, tagName: el.tagName, valueLength: value.length };
  })()`;
  const result = await evaluate(page, expression, { timeoutMs });
  if (!result?.ok) throw new Error(result?.error || `Fill failed: ${selector}`);
  return { ok: true, action: 'fill', selector, tagName: result.tagName || null, valueLength: result.valueLength || 0 };
}

const KEY_DEFS = {
  Enter: { key: 'Enter', code: 'Enter', windowsVirtualKeyCode: 13 },
  Tab: { key: 'Tab', code: 'Tab', windowsVirtualKeyCode: 9 },
  Escape: { key: 'Escape', code: 'Escape', windowsVirtualKeyCode: 27 },
  Backspace: { key: 'Backspace', code: 'Backspace', windowsVirtualKeyCode: 8 }
};

async function pressKey(page, key, timeoutMs) {
  const def = KEY_DEFS[key] || { key, code: key, text: key.length === 1 ? key : undefined, windowsVirtualKeyCode: key.length === 1 ? key.toUpperCase().charCodeAt(0) : 0 };
  await page.connection.send('Input.dispatchKeyEvent', { type: 'keyDown', ...def }, page.sessionId, timeoutMs);
  await page.connection.send('Input.dispatchKeyEvent', { type: 'keyUp', ...def }, page.sessionId, timeoutMs);
  return { ok: true, action: 'press', key };
}

async function navigate(page, url, timeoutMs) {
  const waitForLoad = page.connection.waitForEvent('Page.loadEventFired', { sessionId: page.sessionId, timeoutMs }).catch(err => ({ timeout: true, error: err.message }));
  const result = await page.connection.send('Page.navigate', { url }, page.sessionId, timeoutMs);
  const load = await waitForLoad;
  if (result.errorText) throw new Error(result.errorText);
  return { ok: true, action: 'navigate', url, frameId: result.frameId || null, loaded: !load?.timeout };
}

async function snapshot(page, maxChars = 4000, timeoutMs = 15000) {
  const expression = `(() => ({
    ok: true,
    url: location.href,
    title: document.title || '',
    text: (document.body ? document.body.innerText : '').replace(/\n{3,}/g, '\\n\\n').slice(0, ${Number(maxChars) || 4000})
  }))()`;
  const result = await evaluate(page, expression, { timeoutMs });
  return { ok: true, action: 'snapshot', url: result?.url || '', title: result?.title || '', text: result?.text || '' };
}

async function screenshot(page, { fullPage = false, timeoutMs = 15000 } = {}) {
  const params = { format: 'png', fromSurface: true };
  if (fullPage) {
    const metrics = await page.connection.send('Page.getLayoutMetrics', {}, page.sessionId, timeoutMs).catch(() => null);
    const clip = metrics?.contentSize;
    if (clip) {
      params.clip = { x: 0, y: 0, width: Math.max(1, clip.width), height: Math.max(1, clip.height), scale: 1 };
    }
  }
  const result = await page.connection.send('Page.captureScreenshot', params, page.sessionId, timeoutMs);
  return { ok: true, action: 'screenshot', mimeType: 'image/png', dataBase64: result.data || '', byteLengthApprox: result.data ? Math.floor(result.data.length * 0.75) : 0 };
}

async function executeStep(page, step) {
  if (step.action === 'navigate') return navigate(page, step.url, step.timeoutMs);
  if (step.action === 'waitForSelector') {
    await waitForSelector(page, step.selector, step.timeoutMs);
    return { ok: true, action: 'waitForSelector', selector: step.selector };
  }
  if (step.action === 'click') return clickSelector(page, step.selector, step.timeoutMs);
  if (step.action === 'fill') return fillSelector(page, step.selector, step.text || '', step.timeoutMs);
  if (step.action === 'press') return pressKey(page, step.key, step.timeoutMs);
  if (step.action === 'snapshot') return snapshot(page, step.maxChars, step.timeoutMs);
  if (step.action === 'screenshot') return screenshot(page, { fullPage: step.fullPage, timeoutMs: step.timeoutMs });
  throw new Error(`Unsupported browser nav action: ${step.action}`);
}

async function executeBrowserNavRun(normalized, { openPage = openCdpPage } = {}) {
  const startedAt = Date.now();
  let page;
  const results = [];
  try {
    page = await openPage(normalized.profile);
    for (const step of normalized.run.steps) {
      const stepStartedAt = Date.now();
      try {
        const result = await executeStep(page, step);
        results.push({ index: step.index, ok: true, durationMs: Date.now() - stepStartedAt, result });
      } catch (err) {
        results.push({ index: step.index, ok: false, durationMs: Date.now() - stepStartedAt, error: err.message });
        return {
          ok: false,
          status: 'failed',
          durationMs: Date.now() - startedAt,
          failedStep: step.index,
          error: err.message,
          results
        };
      }
    }
    return { ok: true, status: 'completed', durationMs: Date.now() - startedAt, results };
  } finally {
    if (page?.close) await page.close().catch(() => {});
  }
}

module.exports = {
  evaluate,
  executeBrowserNavRun,
  executeStep,
  waitForSelector
};
