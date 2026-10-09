'use strict';

// Only profile READS may be retried. Never retry a payment or ARB create here.
async function verifyProfileReady({ profileIds, post, wait = ms => new Promise(resolve => setTimeout(resolve, ms)) }) {
  const { customerProfileId, customerPaymentProfileId } = profileIds;
  if (![customerProfileId, customerPaymentProfileId].every(id => /^\d{1,30}$/.test(id || ''))) {
    throw Error('Invalid customer/payment profile references');
  }
  for (let attempt = 0; attempt < 4; attempt++) {
    if (attempt) await wait(5000);
    let data;
    try {
      data = await post({ getCustomerPaymentProfileRequest: {
        customerProfileId, customerPaymentProfileId, unmaskExpirationDate: false
      } });
    } catch (error) {
      const messages = error?.authNetFailure?.messages || [];
      if (attempt < 3 && messages.some(message => message.code === 'E00040')) continue;
      throw error;
    }
    const profile = data?.paymentProfile;
    if (String(profile?.customerPaymentProfileId || '') !== customerPaymentProfileId) {
      throw Error('Payment profile readback does not match requested reference');
    }
    if (!profile?.billTo?.firstName?.trim() || !profile?.billTo?.lastName?.trim()) {
      throw Error('Payment profile is missing the name required for a subscription');
    }
    return { customerProfileId, customerPaymentProfileId };
  }
  throw Error('Payment profile readiness unconfirmed');
}
module.exports = { verifyProfileReady };
