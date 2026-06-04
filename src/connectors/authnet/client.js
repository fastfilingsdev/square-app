const axios = require('axios');

function getAuthNetConfig() {
  return {
    apiLoginId: process.env.AUTHNET_API_LOGIN_ID || '',
    transactionKey: process.env.AUTHNET_TRANSACTION_KEY || '',
    apiUrl: process.env.AUTHNET_API_URL || 'https://api2.authorize.net/xml/v1/request.api',
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
  const response = await axios.post(config.apiUrl, payload, {
    headers: { 'Content-Type': 'application/json' },
    timeout: 45000
  });

  const data = response.data || {};
  if (data.messages?.resultCode === 'Error') {
    const message = Array.isArray(data.messages?.message)
      ? data.messages.message.map(item => `${item.code || ''} ${item.text || ''}`.trim()).join('; ')
      : 'Authorize.Net returned an error';
    throw new Error(message);
  }

  return data;
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

async function chargeCustomerPaymentProfile({
  customerProfileId,
  customerPaymentProfileId,
  amount,
  invoiceNumber,
  description = 'Fast Filings Sales Tax Filing',
  customerEmail = '',
  refId = ''
}, config = getAuthNetConfig()) {
  const merchantAuthentication = getMerchantAuthentication(config);
  const normalizedAmount = Number(amount);
  if (!customerProfileId || !customerPaymentProfileId) {
    throw new Error('Missing Authorize.Net customer/payment profile IDs for profile charge');
  }
  if (!Number.isFinite(normalizedAmount) || normalizedAmount <= 0) {
    throw new Error('Invalid Authorize.Net profile charge amount');
  }

  return authNetPost({
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
        ...(customerEmail ? { customer: { email: String(customerEmail).slice(0, 255) } } : {})
      }
    }
  }, config);
}

module.exports = {
  authNetPost,
  chargeCustomerPaymentProfile,
  getAuthNetConfig,
  getHostedPaymentPageToken,
  getHostedProfilePageToken,
  getMerchantAuthentication,
  getSubscription
};
