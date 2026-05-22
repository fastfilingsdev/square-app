const { google } = require('googleapis');

function normalizePrivateKey(rawKey) {
  let key = String(rawKey || '');
  if (key.startsWith('"') && key.endsWith('"')) {
    key = key.slice(1, -1);
  }
  return key.replace(/\\n/g, '\n');
}

function getSheetsAuth() {
  const email = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
  const rawKey = process.env.GOOGLE_PRIVATE_KEY;

  if (!email || !rawKey) {
    throw new Error('Missing Google service account environment variables');
  }

  return new google.auth.GoogleAuth({
    credentials: {
      client_email: email,
      private_key: normalizePrivateKey(rawKey)
    },
    scopes: ['https://www.googleapis.com/auth/spreadsheets']
  });
}

async function getSheetsClient() {
  const auth = getSheetsAuth();
  const client = await auth.getClient();
  return google.sheets({ version: 'v4', auth: client });
}

module.exports = {
  getSheetsClient,
  normalizePrivateKey
};
