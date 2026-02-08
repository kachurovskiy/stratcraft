const KEY_HEX = '0123456789abcdef'.repeat(4);

const loadEncryptionModule = async (key?: string) => {
  jest.resetModules();
  delete process.env.DATABASE_KEY;
  if (key) {
    process.env.DATABASE_KEY = key;
  }
  return await import('./encryption');
};

describe('encryption utils', () => {
  test('encrypts and decrypts using DATABASE_KEY', async () => {
    const { encryptValue, decryptValue, isEncryptedValue } = await loadEncryptionModule(KEY_HEX);

    const ciphertext = encryptValue('super-secret');

    expect(isEncryptedValue(ciphertext)).toBe(true);
    expect(ciphertext).not.toBe('super-secret');
    expect(decryptValue(ciphertext)).toBe('super-secret');
  });

  test('decryptValue returns plaintext unchanged when not encrypted', async () => {
    const { decryptValue, isEncryptedValue } = await loadEncryptionModule();

    const plaintext = 'already-plain';
    expect(isEncryptedValue(plaintext)).toBe(false);
    expect(decryptValue(plaintext)).toBe(plaintext);
  });

  test('encryptValue throws when DATABASE_KEY is missing', async () => {
    const { encryptValue } = await loadEncryptionModule();

    expect(() => encryptValue('secret')).toThrow(/DATABASE_KEY/);
  });
});
