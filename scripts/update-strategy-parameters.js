#!/usr/bin/env node

/**
 * Syncs each strategy JSON definition under src/server/strategies
 * with the best-performing parameter set stored in best.txt.
 *
 * Usage:
 *   node scripts/update-strategy-parameters.js [--dry-run]
 */

const fs = require('node:fs');
const path = require('node:path');

const ROOT_DIR = path.join(__dirname, '..');
const STRATEGIES_DIR = path.join(
  ROOT_DIR,
  'src',
  'server',
  'strategies'
);
const BEST_FILE = path.join(STRATEGIES_DIR, 'best.txt');

const args = process.argv.slice(2);
const dryRun = args.includes('--dry-run');

function exitWithError(message) {
  console.error(message);
  process.exit(1);
}

function parseBestFile() {
  if (!fs.existsSync(BEST_FILE)) {
    exitWithError(`Missing ${BEST_FILE}`);
  }

  const content = fs.readFileSync(BEST_FILE, 'utf8');
  const lines = content.split(/\r?\n/).map((line) => line.trim());
  const entries = new Map();

  lines.forEach((line, idx) => {
    if (!line || line.startsWith('#')) {
      return;
    }

    const firstPipe = line.indexOf('|');
    const lastPipe = line.lastIndexOf('|');
    if (firstPipe === -1 || lastPipe === -1 || firstPipe === lastPipe) {
      console.warn(
        `Skipping malformed line ${idx + 1} in best.txt: ${line}`
      );
      return;
    }

    const templateId = line.slice(0, firstPipe).trim();
    const paramsJson = line.slice(firstPipe + 1, lastPipe).trim();

    if (!templateId) {
      console.warn(
        `Skipping line ${idx + 1}: missing template id (${line})`
      );
      return;
    }

    let params;
    try {
      params = JSON.parse(paramsJson);
    } catch (err) {
      console.warn(
        `Skipping line ${idx + 1}: cannot parse parameters JSON (${err.message})`
      );
      return;
    }

    entries.set(templateId, params);
  });

  return entries;
}

function updateStrategyFile(filePath, params) {
  const original = fs.readFileSync(filePath, 'utf8');
  let strategy;
  try {
    strategy = JSON.parse(original);
  } catch (err) {
    console.warn(`Skipping ${path.basename(filePath)}: ${err.message}`);
    return { modified: false, updatedNames: [] };
  }

  if (!Array.isArray(strategy.parameters)) {
    console.warn(
      `Skipping ${path.basename(filePath)}: parameters is not an array`
    );
    return { modified: false, updatedNames: [] };
  }

  const updatedNames = [];
  strategy.parameters.forEach((paramDef) => {
    const name = paramDef?.name;
    if (!name || !(name in params)) {
      return;
    }
    const newValue = params[name];
    if (!Object.is(paramDef.default, newValue)) {
      paramDef.default = newValue;
      updatedNames.push(name);
    }
  });

  if (updatedNames.length === 0) {
    return { modified: false, updatedNames };
  }

  if (!dryRun) {
    fs.writeFileSync(
      filePath,
      `${JSON.stringify(strategy, null, 2)}\n`,
      'utf8'
    );
  }

  return { modified: true, updatedNames };
}

function main() {
  const bestEntries = parseBestFile();
  if (bestEntries.size === 0) {
    exitWithError('No valid entries found in best.txt');
  }

  let filesModified = 0;
  let totalParamsUpdated = 0;

  bestEntries.forEach((params, templateId) => {
    const strategyFile = path.join(STRATEGIES_DIR, `${templateId}.json`);
    if (!fs.existsSync(strategyFile)) {
      console.warn(
        `No strategy file found for template "${templateId}", skipping.`
      );
      return;
    }
    const { modified, updatedNames } = updateStrategyFile(
      strategyFile,
      params
    );
    if (!modified) {
      console.log(
        `No parameter changes needed for ${templateId}`
      );
      return;
    }
    filesModified += 1;
    totalParamsUpdated += updatedNames.length;
    console.log(
      `${dryRun ? '[dry-run] ' : ''}Updated ${templateId}: ${updatedNames.join(
        ', '
      )}`
    );
  });

  const summary = dryRun
    ? `Dry run complete: ${filesModified} strategies would be updated (${totalParamsUpdated} parameters).`
    : `Done: updated ${filesModified} strategy files (${totalParamsUpdated} parameter defaults).`;

  console.log(summary);
}

main();
