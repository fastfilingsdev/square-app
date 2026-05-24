const axios = require('axios');

function getAuthNetConfig() {
  return {
    apiLoginId: process.env.AUTHNET_API_LOGIN_ID || '',
    transactionKey: process.env.AUTHNET_TRANSACTION_KEY || '',
    apiUrl: process.env.AUTHNET_API_URL || 'https://api2.authorize.net/xml/v1/request.api',
    acceptEditPaymentUrl: process.env.AUTHNET_ACCEPT_EDIT_PAYMENT_URL || 'https://accept.authorize.net/customer/editPayment'
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

module.exports = {
  authNetPost,
  getAuthNetConfig,
  getHostedProfilePageToken,
  getMerchantAuthentication,
  getSubscription
};
