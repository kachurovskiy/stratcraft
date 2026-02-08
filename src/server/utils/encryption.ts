import crypto from 'crypto';

const ENCRYPTION_PREFIX = 'enc:v1:';
const IV_LENGTH = 12;
const KEY_ENV_VAR = 'DATABASE_KEY';

let cachedKey: Buffer | null | undefined;

function loadEncryptionKey(): Buffer {
  if (cachedKey !== undefined) {
    if (cachedKey === null) {
      throw new Error(`${KEY_ENV_VAR} is required to encrypt and decrypt secrets.`);
    }
    return cachedKey;
  }

  const raw = process.env[KEY_ENV_VAR];
  if (!raw || raw.trim().length === 0) {
    cachedKey = null;
    throw new Error(
      `${KEY_ENV_VAR} is required to encrypt and decrypt secrets. ` +
      'Generate one with "openssl rand -hex 32".'
    );
  }

  const trimmed = raw.trim();
  let key: Buffer | null = null;

  if (/^[0-9a-fA-F]{64}$/.test(trimmed)) {
    key = Buffer.from(trimmed, 'hex');
  } else {
    const decoded = Buffer.from(trimmed, 'base64');
    if (decoded.length === 32) {
      key = decoded;
    }
  }

  if (!key || key.length !== 32) {
    cachedKey = null;
    throw new Error(
      `${KEY_ENV_VAR} must be a 32-byte key encoded as 64 hex characters or base64. Key length ${key ? key.length : 0}`
    );
  }

  cachedKey = key;
  return key;
}

export function isEncryptedValue(value: string): boolean {
  return typeof value === 'string' && value.startsWith(ENCRYPTION_PREFIX);
}

export function encryptValue(value: string): string {
  if (typeof value !== 'string' || value.length === 0) {
    return value;
  }
  if (isEncryptedValue(value)) {
    return value;
  }

  const key = loadEncryptionKey();
  const iv = crypto.randomBytes(IV_LENGTH);
  const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
  const ciphertext = Buffer.concat([cipher.update(value, 'utf8'), cipher.final()]);
  const tag = cipher.getAuthTag();

  return [
    ENCRYPTION_PREFIX,
    iv.toString('base64'),
    ':',
    ciphertext.toString('base64'),
    ':',
    tag.toString('base64')
  ].join('');
}

export function decryptValue(value: string): string {
  if (typeof value !== 'string' || value.length === 0) {
    return value;
  }
  if (!isEncryptedValue(value)) {
    return value;
  }

  const key = loadEncryptionKey();
  const payload = value.slice(ENCRYPTION_PREFIX.length);
  const parts = payload.split(':');
  if (parts.length !== 3) {
    throw new Error('Encrypted value has an invalid format.');
  }

  const [ivB64, dataB64, tagB64] = parts;
  const iv = Buffer.from(ivB64, 'base64');
  const data = Buffer.from(dataB64, 'base64');
  const tag = Buffer.from(tagB64, 'base64');

  if (iv.length !== IV_LENGTH || tag.length === 0) {
    throw new Error('Encrypted value payload is invalid.');
  }

  const decipher = crypto.createDecipheriv('aes-256-gcm', key, iv);
  decipher.setAuthTag(tag);
  const plaintext = Buffer.concat([decipher.update(data), decipher.final()]);
  return plaintext.toString('utf8');
}
