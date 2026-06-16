'use strict';

const crypto = require('crypto');

const TOKEN_PREFIX = 'enc:v1:';

function normalizeKey(rawKey) {
  const value = String(rawKey || '').trim();
  if (!value) return null;

  const base64Match = /^[A-Za-z0-9+/=]+$/.test(value) ? Buffer.from(value, 'base64') : null;
  if (base64Match && base64Match.length === 32) {
    return base64Match;
  }

  const hexMatch = /^[a-f0-9]{64}$/i.test(value) ? Buffer.from(value, 'hex') : null;
  if (hexMatch && hexMatch.length === 32) {
    return hexMatch;
  }

  return crypto.createHash('sha256').update(value, 'utf8').digest();
}

function hasCloverTokenEncryptionKey(env = process.env) {
  return Boolean(normalizeKey(
    env.CLOVER_TOKEN_ENCRYPTION_KEY ||
    env.PLATFORM_CONNECTIONS_ENCRYPTION_KEY ||
    env.FF_PLATFORM_CONNECTIONS_ENCRYPTION_KEY ||
    ''
  ));
}

function encryptCloverTokenForStorage(token, env = process.env) {
  const value = String(token || '');
  if (!value) return '';

  const key = normalizeKey(
    env.CLOVER_TOKEN_ENCRYPTION_KEY ||
    env.PLATFORM_CONNECTIONS_ENCRYPTION_KEY ||
    env.FF_PLATFORM_CONNECTIONS_ENCRYPTION_KEY ||
    ''
  );
  if (!key) {
    throw new Error('Missing Clover token encryption key. Configure CLOVER_TOKEN_ENCRYPTION_KEY before storing production Clover tokens.');
  }

  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
  const ciphertext = Buffer.concat([cipher.update(value, 'utf8'), cipher.final()]);
  const tag = cipher.getAuthTag();

  return `${TOKEN_PREFIX}${iv.toString('base64')}:${tag.toString('base64')}:${ciphertext.toString('base64')}`;
}

function decryptCloverTokenFromStorage(storedValue, env = process.env) {
  const value = String(storedValue || '');
  if (!value) return '';
  if (!value.startsWith(TOKEN_PREFIX)) {
    return value;
  }

  const key = normalizeKey(
    env.CLOVER_TOKEN_ENCRYPTION_KEY ||
    env.PLATFORM_CONNECTIONS_ENCRYPTION_KEY ||
    env.FF_PLATFORM_CONNECTIONS_ENCRYPTION_KEY ||
    ''
  );
  if (!key) {
    throw new Error('Missing Clover token encryption key. Cannot decrypt stored Clover token.');
  }

  const parts = value.slice(TOKEN_PREFIX.length).split(':');
  if (parts.length !== 3) {
    throw new Error('Invalid encrypted Clover token format.');
  }

  const [ivB64, tagB64, ciphertextB64] = parts;
  const decipher = crypto.createDecipheriv('aes-256-gcm', key, Buffer.from(ivB64, 'base64'));
  decipher.setAuthTag(Buffer.from(tagB64, 'base64'));
  const plaintext = Buffer.concat([
    decipher.update(Buffer.from(ciphertextB64, 'base64')),
    decipher.final()
  ]);
  return plaintext.toString('utf8');
}

function prepareCloverTokenForStorage(token, { environment = 'Production', env = process.env } = {}) {
  const value = String(token || '');
  if (!value) return '';

  if (hasCloverTokenEncryptionKey(env)) {
    return encryptCloverTokenForStorage(value, env);
  }

  if (String(environment || '').toLowerCase() === 'production') {
    throw new Error('Refusing to store plaintext production Clover token. Configure CLOVER_TOKEN_ENCRYPTION_KEY first.');
  }

  return value;
}

function getCloverTokenStorageStatus(env = process.env, environment = 'Production') {
  const encryptionConfigured = hasCloverTokenEncryptionKey(env);
  const productionReady = String(environment || '').toLowerCase() !== 'production' || encryptionConfigured;
  return {
    encryptionConfigured,
    productionReady,
    storageMode: encryptionConfigured ? 'encrypted-aes-256-gcm' : 'plaintext-sandbox-only'
  };
}

module.exports = {
  TOKEN_PREFIX,
  decryptCloverTokenFromStorage,
  encryptCloverTokenForStorage,
  getCloverTokenStorageStatus,
  hasCloverTokenEncryptionKey,
  prepareCloverTokenForStorage
};
