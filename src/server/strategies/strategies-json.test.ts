import fs from 'fs';
import path from 'path';

describe('strategy JSON definitions', () => {
  const strategiesDir = __dirname;
  const jsonFiles = fs
    .readdirSync(strategiesDir)
    .filter((file) => file.endsWith('.json'));

  it('are all valid JSON', () => {
    expect(jsonFiles.length).toBeGreaterThan(0);

    for (const file of jsonFiles) {
      const filePath = path.join(strategiesDir, file);
      const fileContents = fs.readFileSync(filePath, 'utf8');

      expect(() => JSON.parse(fileContents)).not.toThrow();
    }
  });
});
