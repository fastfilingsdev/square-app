'use strict';

// Deliberately read-only. Never extend this allowlist with financial mutations.
const axios = require('axios');
const { verifyGoogleAccessToken } = require('../billingRefunds/routes');
const ENDPOINT = 'https://api2.authorize.net/xml/v1/request.api';
const plain = v => v !== null && typeof v === 'object' && !Array.isArray(v) &&
  [Object.prototype, null].includes(Object.getPrototypeOf(v));
const id = v => typeof v === 'string' && /^[1-9][0-9]{0,29}$/.test(v);
const bool = v => typeof v === 'boolean';
const oneOf = values => v => values.includes(v);
function fields(value, rules, required = []) {
  return plain(value) && Object.keys(value).every(k => Object.hasOwn(rules, k) && rules[k](value[k])) &&
    required.every(k => Object.hasOwn(value, k));
}
const paging = v => fields(v, {
  limit: n => Number.isInteger(n) && n >= 1 && n <= 1000,
  offset: n => Number.isInteger(n) && n >= 1 && n <= 100000
}, ['limit', 'offset']);
const sorting = orders => v => fields(v, { orderBy: oneOf(orders), orderDescending: bool }, ['orderBy', 'orderDescending']);
const date = v => typeof v === 'string' && /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,3})?Z$/.test(v) && Number.isFinite(Date.parse(v));
const transactionSort = sorting(['id', 'submitTimeUTC']);
const validators = {
  ARBGetSubscriptionListRequest: v => fields(v, {
    searchType: oneOf(['cardExpiringThisMonth', 'subscriptionActive', 'subscriptionExpiringThisMonth', 'subscriptionInactive']),
    sorting: sorting(['id', 'name', 'status', 'createTimeStampUTC', 'lastName', 'firstName', 'accountNumber', 'amount', 'pastOccurrences']), paging
  }, ['searchType']),
  ARBGetSubscriptionRequest: v => fields(v, { subscriptionId: id, includeTransactions: bool }, ['subscriptionId']),
  getUnsettledTransactionListRequest: v => fields(v, { sorting: transactionSort, paging }),
  getSettledBatchListRequest: v => fields(v, { includeStatistics: bool, firstSettlementDate: date, lastSettlementDate: date },
    ['firstSettlementDate', 'lastSettlementDate']) &&
    Date.parse(v.lastSettlementDate) >= Date.parse(v.firstSettlementDate) &&
    Date.parse(v.lastSettlementDate) - Date.parse(v.firstSettlementDate) <= 31 * 86400000,
  getTransactionListRequest: v => fields(v, { batchId: id, sorting: transactionSort, paging }, ['batchId']),
  getTransactionDetailsRequest: v => fields(v, { transId: id }, ['transId']),
  getCustomerProfileRequest: v => fields(v, { customerProfileId: id, unmaskExpirationDate: v => v === false }, ['customerProfileId'])
};

function validateRequest(body) {
  return fields(body, { operation: v => typeof v === 'string' && Object.hasOwn(validators, v), parameters: plain },
    ['operation', 'parameters']) && validators[body.operation](body.parameters);
}

// Authorize.Net's JSON endpoint is backed by an ordered XML schema. In
// particular sorting must precede paging; caller insertion order is not safe.
// Match the official SDK request constructors, without changing the allowlist.
const parameterOrder = {
  ARBGetSubscriptionListRequest: ['searchType', 'sorting', 'paging'],
  ARBGetSubscriptionRequest: ['subscriptionId', 'includeTransactions'],
  getUnsettledTransactionListRequest: ['sorting', 'paging'],
  getSettledBatchListRequest: ['includeStatistics', 'firstSettlementDate', 'lastSettlementDate'],
  getTransactionListRequest: ['batchId', 'sorting', 'paging'],
  getTransactionDetailsRequest: ['transId'],
  getCustomerProfileRequest: ['customerProfileId', 'unmaskExpirationDate']
};
function buildProviderReadRequest(body, env) {
  if (!validateRequest(body)) throw Error('Unsupported payment read request');
  const parameters = {};
  for (const key of parameterOrder[body.operation]) {
    if (!Object.hasOwn(body.parameters, key)) continue;
    const value = body.parameters[key];
    parameters[key] = key === 'sorting' ? {orderBy: value.orderBy, orderDescending: value.orderDescending} :
      key === 'paging' ? {limit: value.limit, offset: value.offset} : value;
  }
  return {[body.operation]: {
    merchantAuthentication: {name: env.AUTHNET_API_LOGIN_ID, transactionKey: env.AUTHNET_TRANSACTION_KEY},
    ...parameters
  }};
}

// Preserve the provider's read-response shape, but never pass credentials, full
// account numbers, security codes or expiration dates back to Google.
function sanitize(value, secrets = [], depth = 0) {
  if (depth > 30) throw Error('Invalid provider response');
  if (Array.isArray(value)) return value.map(v => sanitize(v, secrets, depth + 1));
  if (plain(value)) {
    const result = Object.create(null);
    for (const [key, item] of Object.entries(value)) {
      const lower = key.toLowerCase();
      if (['__proto__', 'constructor', 'prototype', 'merchantauthentication', 'transactionkey', 'signaturekey',
        'apiloginid', 'cardcode', 'cardsecuritycode', 'cvv', 'cvc', 'expirationdate', 'routingnumber'].includes(lower)) continue;
      if (['cardnumber', 'accountnumber'].includes(lower)) {
        const digits = String(item).replace(/\D/g, '');
        result[key] = digits.length >= 4 ? 'XXXX' + digits.slice(-4) : 'MASKED';
      } else result[key] = sanitize(item, secrets, depth + 1);
    }
    return result;
  }
  if (typeof value === 'string') {
    let text = value;
    for (const secret of secrets.filter(Boolean)) text = text.split(secret).join('[REDACTED]');
    return text;
  }
  return value;
}

function createPaymentReadHandler({ env = process.env, verify = verifyGoogleAccessToken, post = axios.post } = {}) {
  let inFlight = 0;
  const reply = (status, error) => ({ status, body: { ok: false, error, retryAutomatically: false } });
  return async function handle({ authorization, body }) {
    if (env.FF_GOOGLE_PAYMENT_READS_ENABLED !== 'true') return reply(503, 'Payment read bridge disabled');
    // Bound authentication as well as provider work. No queue or automatic retry.
    if (inFlight >= 8) return reply(429, 'Payment read bridge busy');
    inFlight++;
    try {
      const match = typeof authorization === 'string' && /^Bearer ([^\s]{1,4096})$/i.exec(authorization);
      if (!match) return reply(401, 'Unauthorized');
      const identity = await verify(match[1]);
      if (identity?.ok !== true || identity.verified !== true || identity.email !== 'returns@fastfilings.com') {
        return reply(401, 'Unauthorized');
      }
      if (!validateRequest(body)) return reply(400, 'Unsupported payment read request');
      if (!env.AUTHNET_API_LOGIN_ID || !env.AUTHNET_TRANSACTION_KEY) return reply(503, 'Payment read bridge not configured');
      const request = buildProviderReadRequest(body, env);
      const response = await post(ENDPOINT, request, {
        timeout: 45000, maxRedirects: 0, maxContentLength: 4 * 1024 * 1024, maxBodyLength: 16384,
        headers: { 'Content-Type': 'application/json' }
      });
      if (response.status !== 200 || !plain(response.data) ||
          !['Ok', 'Error'].includes(response.data.messages?.resultCode)) return reply(502, 'Invalid provider read response');
      return { status: 200, body: sanitize(response.data,
        [env.AUTHNET_API_LOGIN_ID, env.AUTHNET_TRANSACTION_KEY, env.AUTHNET_SIGNATURE_KEY]) };
    } catch (_) {
      // Axios errors can contain the complete credential-bearing request. Do not
      // return or log errors, request objects, access tokens, or raw responses.
      return reply(502, 'Payment read could not be completed');
    } finally { inFlight--; }
  };
}

module.exports = { createPaymentReadHandler, validateRequest, buildProviderReadRequest, sanitize };
