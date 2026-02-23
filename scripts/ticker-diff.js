#!/usr/bin/env node

const fs = require('fs/promises');
const path = require('path');

async function readTickers(filePath) {
  const absolutePath = path.resolve(process.cwd(), filePath);
  const data = await fs.readFile(absolutePath, 'utf8');
  return data
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(line => line.length > 0 && !line.startsWith('#'));
}

async function main() {
  const [, , firstFile, secondFile] = process.argv;

  if (!firstFile || !secondFile) {
    console.error('Usage: node scripts/ticker-diff.js <firstFile> <secondFile>');
    process.exit(1);
  }

  try {
    const [firstTickers, secondTickers] = await Promise.all([
      readTickers(firstFile),
      readTickers(secondFile),
    ]);

    const secondSet = new Set(secondTickers);
    const seen = new Set();
    const diff = [];

    for (const ticker of firstTickers) {
      if (!secondSet.has(ticker) && !seen.has(ticker)) {
        diff.push(ticker);
        seen.add(ticker);
      }
    }

    console.log(diff.join(' '));
  } catch (error) {
    console.error(`Failed to diff tickers: ${error.message}`);
    process.exit(1);
  }
}

main();
