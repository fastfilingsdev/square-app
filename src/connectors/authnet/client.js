const axios = require('axios');

function asArray(value) {
  if (!value) return [];
  return Array.isArray(value) ? value : [value];
}

function normalizeString(value) {
  return String(value == null ? '' : value).trim();
}

function safeText(value, max = 300) {
  const text = normalizeString(value);
  return text ? text.slice(0, max) : '';
}

function safeMessage(item) {
  if (!item || typeof item !== 'object') return null;
  const code = safeText(item.code || item.errorCode || item.responseCode, 80);
  const text = safeText(item.text || item.errorText || item.description || item.message, 500);
  if (!code && !text) return null;
  return {
    ...(code ? { code } : {}),
    ...(text ? { text } : {})
  };
}

function maskAccountNumber(value) {
  const text = safeText(value, 80);
  if (!text) return '';
  const digits = text.replace(/\D/g, '');
  if (digits.length >= 4) return `XXXX${digits.slice(-4)}`;
  if (/^X+/i.test(text)) return text.slice(-12);
  return 'MASKED';
}

function sanitizedMessages(value) {
  return asArray(value).map(safeMessage).filter(Boolean);
}

function sanitizeTransactionResponse(tx) {
  if (!tx || typeof tx !== 'object') return null;
  const errors = sanitizedMessages(tx.errors?.error || tx.errors);
  const messages = sanitizedMessages(tx.messages?.message || tx.messages);
  const sanitized = {
    responseCode: safeText(tx.responseCode, 40),
    rawResponseCode: safeText(tx.rawResponseCode, 40),
    transId: safeText(tx.transId || tx.transactionId, 80),
    refTransId: safeText(tx.refTransID || tx.refTransId, 80),
    accountType: safeText(tx.accountType, 80),
    accountNumberMasked: maskAccountNumber(tx.accountNumber || tx.cardNumber),
    avsResultCode: safeText(tx.avsResultCode || tx.AVSResponse, 40),
    cvvResultCode: safeText(tx.cvvResultCode, 40),
    cavvResultCode: safeText(tx.cavvResultCode, 40),
    errors,
    messages,
    authCodePresent: Boolean(tx.authCode),
    networkTransIdPresent: Boolean(tx.networkTransId),
    fieldsPresent: Object.keys(tx).filter(key => ![
      'authCode', 'transHash', 'transHashSha2', 'networkTransId', 'profile', 'userFields'
    ].includes(key)).sort()
  };
  return Object.fromEntries(Object.entries(sanitized).filter(([, value]) => (
    value !== '' && value != null && !(Array.isArray(value) && !value.length)
  )));
}

function sanitizeAuthNetFailureDetail(data) {
  if (!data || typeof data !== 'object') return null;
  const tx = data.transactionResponse || data.createTransactionResponse?.transactionResponse || null;
  const topMessages = sanitizedMessages(data.messages?.message || data.messages);
  const transactionResponse = sanitizeTransactionResponse(tx);
  return {
    resultCode: safeText(data.messages?.resultCode || data.resultCode, 80),
    messages: topMessages,
    ...(transactionResponse ? { transactionResponse } : {}),
    topLevelKeysPresent: Object.keys(data).filter(key => !['merchantAuthentication'].includes(key)).sort()
  };
}

class AuthNetApiError extends Error {
  constructor(message, data) {
    super(message || 'Authorize.Net returned an error');
    this.name = 'AuthNetApiError';
    this.authNetFailure = sanitizeAuthNetFailureDetail(data);
  }
}

function compactAuthNetMessage(item) {
  if (!item || typeof item !== 'object') return '';
  const code = item.code || item.errorCode || item.responseCode || '';
  const text = item.text || item.errorText || item.description || item.message || '';
  return `${code || ''} ${text || ''}`.trim();
}

function authNetErrorMessages(data) {
  const messages = [];
  asArray(data?.messages?.message).forEach(item => {
    const text = compactAuthNetMessage(item);
    if (text) messages.push(text);
  });

  const txResponses = [
    data?.transactionResponse,
    data?.createTransactionResponse?.transactionResponse
  ].filter(Boolean);

  txResponses.forEach(tx => {
    asArray(tx?.errors?.error).forEach(item => {
      const text = compactAuthNetMessage(item);
      if (text) messages.push(text);
    });
    asArray(tx?.messages?.message).forEach(item => {
      const text = compactAuthNetMessage(item);
      if (text) messages.push(text);
    });
  });

  return Array.from(new Set(messages));
}

function formatAuthNetErrorMessage(data) {
  const messages = authNetErrorMessages(data);
  return messages.length ? messages.join('; ') : 'Authorize.Net returned an error';
}

function getAuthNetConfig() {
  return {
    apiLoginId: process.env.AUTHNET_API_LOGIN_ID || '',
    transactionKey: process.env.AUTHNET_TRANSACTION_KEY || '',
    apiUrl: process.env.AUTHNET_API_URL || 'https://api2.authorize.net/xml/v1/request.api',
    restUrl: process.env.AUTHNET_REST_URL || 'https://api.authorize.net/rest/v1',
    acceptEditPaymentUrl: process.env.AUTHNET_ACCEPT_EDIT_PAYMENT_URL || 'https://accept.authorize.net/customer/editPayment',
    acceptHostedPaymentUrl: process.env.AUTHNET_ACCEPT_HOSTED_PAYMENT_URL || 'https://accept.authorize.net/payment/payment'
  };
}

function getMerchantAuthentication(config = getAuthNetConfig()) {
  if (!config.apiLoginId || !config.transactionKey) {
    throw new Error('Missing AUTHNET_API_LOGIN_ID or AUTHNET_TRANSACTION_KEY');
  }

  return {
    name: config.apiLoginId,
    transactionKey: config.transactionKey
  };
}

async function authNetPost(payload, config = getAuthNetConfig()) {
  let response;
  try {
    response = await axios.post(config.apiUrl, payload, {
      headers: { 'Content-Type': 'application/json' },
      timeout: 45000
    });
  } catch (err) {
    const data = err?.response?.data;
    if (data && typeof data === 'object') {
      throw new AuthNetApiError(formatAuthNetErrorMessage(data), data);
    }
    throw err;
  }

  const data = response.data || {};
  if (data.messages?.resultCode === 'Error') {
    throw new AuthNetApiError(formatAuthNetErrorMessage(data), data);
  }

  return data;
}

function authNetRestAuthHeader(config = getAuthNetConfig()) {
  const authConfig = getMerchantAuthentication(config);
  return `Basic ${Buffer.from(`${authConfig.name}:${authConfig.transactionKey}`, 'utf8').toString('base64')}`;
}

async function authNetRestRequest(method, path, body = null, config = getAuthNetConfig()) {
  const baseUrl = String(config.restUrl || 'https://api.authorize.net/rest/v1').replace(/\/$/, '');
  const response = await axios({
    method,
    url: `${baseUrl}${path}`,
    data: body || undefined,
    headers: {
      Authorization: authNetRestAuthHeader(config),
      Accept: 'application/json',
      'Content-Type': 'application/json'
    },
    timeout: 45000
  });
  return response.data || null;
}

async function getAuthNetWebhook(webhookId, config = getAuthNetConfig()) {
  if (!webhookId) throw new Error('Missing Authorize.Net webhook ID');
  return authNetRestRequest('GET', `/webhooks/${encodeURIComponent(String(webhookId))}`, null, config);
}

async function updateAuthNetWebhook(webhookId, patch, config = getAuthNetConfig()) {
  if (!webhookId) throw new Error('Missing Authorize.Net webhook ID');
  if (!patch || typeof patch !== 'object') throw new Error('Missing Authorize.Net webhook update patch');
  return authNetRestRequest('PUT', `/webhooks/${encodeURIComponent(String(webhookId))}`, patch, config);
}

async function getSubscription(subscriptionId, config = getAuthNetConfig()) {
  const merchantAuthentication = getMerchantAuthentication(config);
  return authNetPost({
    ARBGetSubscriptionRequest: {
      merchantAuthentication,
      subscriptionId: String(subscriptionId),
      includeTransactions: true
    }
  }, config);
}

async function getTransactionDetails(transId, config = getAuthNetConfig()) {
  const merchantAuthentication = getMerchantAuthentication(config);
  return authNetPost({
    getTransactionDetailsRequest: {
      merchantAuthentication,
      transId: String(transId)
    }
  }, config);
}

async function getTransactionListForCustomer(customerProfileId, config = getAuthNetConfig()) {
  const merchantAuthentication = getMerchantAuthentication(config);
  return authNetPost({
    getTransactionListForCustomerRequest: {
      merchantAuthentication,
      customerProfileId: String(customerProfileId)
    }
  }, config);
}

function buildRefundTransactionRequest({
  refTransId,
  amount,
  cardLast4,
  customerProfileId = '',
  customerPaymentProfileId = '',
  customer = null,
  billTo = null,
  invoiceNumber = '',
  description = 'Fast Filings refund',
  emailCustomer = false,
  refId = ''
}, merchantAuthentication) {
  const normalizedAmount = Number(amount);
  const last4 = String(cardLast4 || '').replace(/\D/g, '').slice(-4);
  if (!refTransId) throw new Error('Missing original Authorize.Net transaction ID for refund');
  if (!Number.isFinite(normalizedAmount) || normalizedAmount <= 0) throw new Error('Invalid Authorize.Net refund amount');
  const profileRefund = Boolean(customerProfileId && customerPaymentProfileId);
  if (!profileRefund && last4.length !== 4) throw new Error('Missing card last4 required for Authorize.Net refund');
  // Linked Authorize.Net refunds require the original transaction plus the
  // original payment card's last four digits with expirationDate=XXXX. Keep
  // the request aligned with Authorize.Net's refundTransaction examples; do
  // not send a current ARB payment profile unless no original last4 exists.
  const refundCardNumber = last4;
  const paymentOrProfile = profileRefund ? {
    profile: {
      customerProfileId: String(customerProfileId),
      paymentProfile: {
        paymentProfileId: String(customerPaymentProfileId)
      }
    }
  } : {
    payment: {
      creditCard: {
        cardNumber: refundCardNumber,
        expirationDate: 'XXXX'
      }
    }
  };

  const cleanField = value => String(value == null ? '' : value).trim();
  const allowedBillTo = ['firstName', 'lastName', 'company', 'address', 'city', 'state', 'zip', 'country', 'phoneNumber'];
  const billToPayload = billTo && typeof billTo === 'object'
    ? Object.fromEntries(allowedBillTo
      .map(key => [key, cleanField(billTo[key])])
      .filter(([, value]) => value))
    : null;
  const customerPayload = customer && typeof customer === 'object'
    ? Object.fromEntries(['id', 'email']
      .map(key => [key, cleanField(customer[key])])
      .filter(([, value]) => value))
    : null;

  return {
    createTransactionRequest: {
      merchantAuthentication,
      ...(refId ? { refId: String(refId).slice(0, 20) } : {}),
      transactionRequest: {
        transactionType: 'refundTransaction',
        amount: normalizedAmount.toFixed(2),
        ...paymentOrProfile,
        refTransId: String(refTransId),
        ...(invoiceNumber || description ? {
          order: {
            ...(invoiceNumber ? { invoiceNumber: String(invoiceNumber).slice(0, 20) } : {}),
            ...(description ? { description: String(description).slice(0, 255) } : {})
          }
        } : {}),
        ...(customerPayload && Object.keys(customerPayload).length ? { customer: customerPayload } : {}),
        ...(billToPayload && Object.keys(billToPayload).length ? { billTo: billToPayload } : {}),
        transactionSettings: {
          setting: [{ settingName: 'emailCustomer', settingValue: emailCustomer ? 'true' : 'false' }]
        }
      }
    }
  };
}

async function refundTransaction({
  refTransId,
  amount,
  cardLast4,
  customerProfileId = '',
  customerPaymentProfileId = '',
  customer = null,
  billTo = null,
  invoiceNumber = '',
  description = 'Fast Filings refund',
  emailCustomer = false,
  refId = ''
}, config = getAuthNetConfig()) {
  const merchantAuthentication = getMerchantAuthentication(config);
  return authNetPost(buildRefundTransactionRequest({
    refTransId,
    amount,
    cardLast4,
    customerProfileId,
    customerPaymentProfileId,
    customer,
    billTo,
    invoiceNumber,
    description,
    emailCustomer,
    refId
  }, merchantAuthentication), config);
}

async function getHostedProfilePageToken(customerProfileId, hostedProfileSettings = [], config = getAuthNetConfig()) {
  const merchantAuthentication = getMerchantAuthentication(config);
  const data = await authNetPost({
    getHostedProfilePageRequest: {
      merchantAuthentication,
      customerProfileId: String(customerProfileId),
      hostedProfileSettings: {
        setting: hostedProfileSettings
      }
    }
  }, config);

  const token = String(data.token || '');
  if (!token) {
    throw new Error('Authorize.Net did not return a hosted-profile token');
  }

  return token;
}

async function getHostedPaymentPageToken(transactionRequest, hostedPaymentSettings = [], refId = '', config = getAuthNetConfig()) {
  const merchantAuthentication = getMerchantAuthentication(config);
  const data = await authNetPost({
    getHostedPaymentPageRequest: {
      merchantAuthentication,
      ...(refId ? { refId: String(refId).slice(0, 20) } : {}),
      transactionRequest,
      hostedPaymentSettings: {
        setting: hostedPaymentSettings
      }
    }
  }, config);

  const token = String(data.token || '');
  if (!token) {
    throw new Error('Authorize.Net did not return a hosted-payment token');
  }

  return token;
}

function buildCustomerPaymentProfileChargeRequest({
  customerProfileId,
  customerPaymentProfileId,
  amount,
  invoiceNumber,
  description = 'Fast Filings Sales Tax Filing',
  customerEmail = '',
  emailCustomer = false,
  refId = ''
}, merchantAuthentication) {
  const normalizedAmount = Number(amount);
  if (!customerProfileId || !customerPaymentProfileId) {
    throw new Error('Missing Authorize.Net customer/payment profile IDs for profile charge');
  }
  if (!Number.isFinite(normalizedAmount) || normalizedAmount <= 0) {
    throw new Error('Invalid Authorize.Net profile charge amount');
  }

  return {
    createTransactionRequest: {
      merchantAuthentication,
      ...(refId ? { refId: String(refId).slice(0, 20) } : {}),
      transactionRequest: {
        transactionType: 'authCaptureTransaction',
        amount: normalizedAmount.toFixed(2),
        profile: {
          customerProfileId: String(customerProfileId),
          paymentProfile: {
            paymentProfileId: String(customerPaymentProfileId)
          }
        },
        order: {
          invoiceNumber: String(invoiceNumber || '').slice(0, 20),
          description: String(description || 'Fast Filings Sales Tax Filing').slice(0, 255)
        },
        transactionSettings: {
          setting: [{ settingName: 'emailCustomer', settingValue: emailCustomer ? 'true' : 'false' }]
        },
        ...(customerEmail && emailCustomer ? { customer: { email: String(customerEmail).slice(0, 255) } } : {})
      }
    }
  };
}

async function chargeCustomerPaymentProfile({
  customerProfileId,
  customerPaymentProfileId,
  amount,
  invoiceNumber,
  description = 'Fast Filings Sales Tax Filing',
  customerEmail = '',
  emailCustomer = false,
  refId = ''
}, config = getAuthNetConfig()) {
  const merchantAuthentication = getMerchantAuthentication(config);
  return authNetPost(buildCustomerPaymentProfileChargeRequest({
    customerProfileId,
    customerPaymentProfileId,
    amount,
    invoiceNumber,
    description,
    customerEmail,
    emailCustomer,
    refId
  }, merchantAuthentication), config);
}

module.exports = {
  AuthNetApiError,
  authNetPost,
  authNetRestAuthHeader,
  authNetRestRequest,
  buildCustomerPaymentProfileChargeRequest,
  buildRefundTransactionRequest,
  chargeCustomerPaymentProfile,
  formatAuthNetErrorMessage,
  getAuthNetConfig,
  getAuthNetWebhook,
  getHostedPaymentPageToken,
  getHostedProfilePageToken,
  getMerchantAuthentication,
  refundTransaction,
  sanitizeAuthNetFailureDetail,
  updateAuthNetWebhook,
  getSubscription,
  getTransactionDetails,
  getTransactionListForCustomer
};
