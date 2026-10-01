'use strict';
const express = require('express');
const { createPaymentReadHandler } = require('./gateway');

// Candidate only: mount behind the existing maintenance middleware after caller
// acceptance. No maintenance exception, admin-token bypass or financial route.
function createGooglePaymentReadsRouter(options) {
  const router = express.Router();
  const handle = createPaymentReadHandler(options);
  router.post('/read', express.json({ limit: '16kb' }), async (req, res) => {
    res.set({ 'Cache-Control': 'no-store', Pragma: 'no-cache' });
    const result = await handle({ authorization: req.get('authorization'), body: req.body });
    return res.status(result.status).json(result.body);
  });
  return router;
}

module.exports = { createGooglePaymentReadsRouter };
