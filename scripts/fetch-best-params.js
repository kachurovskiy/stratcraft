#!/usr/bin/env node

/**
 * Downloads the best parameter set for every strategy template from the
 * configured API host and writes the values into each template's JSON
 * definition (parameters[].default).
 *
 * Usage:
 *   node scripts/fetch-best-params.js [--dry-run]
 *
 * Reads DATABASE_URL from .env in the repo root.
 */

const fs = require('node:fs/promises');
const path = require('node:path');
const https = require('node:https');
const { URL } = require('node:url');
const { Client } = require('pg');

const ROOT_DIR = path.join(__dirname, '..');
const STRATEGIES_DIR = path.join(ROOT_DIR, 'src', 'server', 'strategies');
const LOCAL_DOMAIN_PREFIXES = ['localhost', '127.0.0.1', '[::1]'];
const SETTINGS_KEYS = {
  DOMAIN: 'DOMAIN',
  BACKTEST_API_SECRET: 'BACKTEST_API_SECRET',
};

const args = process.argv.slice(2);
const dryRun = args.includes('--dry-run');

const DECIMAL_PLACES = 8;
const FLOAT_TOLERANCE = 1e-12;
let apiSecret = '';
let apiBaseUrl = '';

async function loadEnvValue(targetKey) {
  const envPath = path.join(ROOT_DIR, '.env');
  let content = '';
  try {
    content = await fs.readFile(envPath, 'utf8');
  } catch (error) {
    return '';
  }

  for (const line of content.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) {
      continue;
    }
    const [key, ...rest] = trimmed.split('=');
    if (key !== targetKey) {
      continue;
    }
    let value = rest.join('=').trim();
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }
    return value.trim();
  }

  return '';
}

function normalizeDomain(value) {
  const trimmed = typeof value === 'string' ? value.trim() : '';
  if (!trimmed) return '';
  if (trimmed.includes('://') || trimmed.includes('/') || trimmed.includes('?') || trimmed.includes('#')) {
    return '';
  }
  if (trimmed.includes(':')) {
    return '';
  }
  if (!/^[A-Za-z0-9.-]+$/.test(trimmed)) {
    return '';
  }
  return trimmed;
}

function buildBaseUrl(domain) {
  const lowered = domain.toLowerCase();
  const isLocal = LOCAL_DOMAIN_PREFIXES.some(prefix => lowered.startsWith(prefix));
  return `${isLocal ? 'http' : 'https'}://${domain}`;
}

function buildHeaders() {
  const headers = { Accept: 'application/json' };
  if (apiSecret) {
    headers['x-backtest-secret'] = apiSecret;
  }
  return headers;
}

async function loadSettingsFromDatabase(keys) {
  const databaseUrl = process.env.DATABASE_URL || (await loadEnvValue('DATABASE_URL'));
  if (!databaseUrl) {
    console.warn('DATABASE_URL is not set in .env or environment; settings lookup will be skipped.');
    return {};
  }

  const client = new Client({ connectionString: databaseUrl });
  try {
    await client.connect();
    const result = await client.query(
      'SELECT setting_key, value FROM settings WHERE setting_key = ANY($1)',
      [keys]
    );
    return result.rows.reduce((acc, row) => {
      if (row && typeof row.setting_key === 'string') {
        acc[row.setting_key] = typeof row.value === 'string' ? row.value.trim() : '';
      }
      return acc;
    }, {});
  } catch (error) {
    console.warn(`Failed to load settings: ${error.message}`);
    return {};
  } finally {
    try {
      await client.end();
    } catch {
      // ignore disconnect errors
    }
  }
}

function normalizeNumericValue(value) {
  if (
    typeof value !== 'number' ||
    Number.isNaN(value) ||
    !Number.isFinite(value)
  ) {
    return value;
  }

  if (value === 0) {
    return 0;
  }

  if (Math.abs(value) >= 1e21) {
    return value;
  }

  const rounded = Number(value.toFixed(DECIMAL_PLACES));
  const tolerance = FLOAT_TOLERANCE * Math.max(1, Math.abs(value));

  if (Math.abs(rounded - value) <= tolerance) {
    return Object.is(rounded, -0) ? 0 : rounded;
  }

  return value;
}

function normalizeParameters(params) {
  return Object.entries(params).reduce((acc, [key, value]) => {
    acc[key] =
      typeof value === 'number' ? normalizeNumericValue(value) : value;
    return acc;
  }, {});
}

async function listStrategyTemplates() {
  const entries = await fs.readdir(STRATEGIES_DIR, { withFileTypes: true });

  return entries
    .filter(
      (entry) => entry.isFile() && entry.name.toLowerCase().endsWith('.json')
    )
    .map((entry) => ({
      filePath: path.join(STRATEGIES_DIR, entry.name),
      templateId: entry.name.slice(0, -'.json'.length),
    }))
    .sort((a, b) => a.templateId.localeCompare(b.templateId));
}

async function fetchBestParameters(templateId) {
  const url = `${apiBaseUrl}/${encodeURIComponent(templateId)}`;
  const payload = await getJson(url);

  if (!payload || typeof payload !== 'object') {
    throw new Error('Unexpected API response shape.');
  }

  if (
    !payload.parameters ||
    typeof payload.parameters !== 'object' ||
    Array.isArray(payload.parameters)
  ) {
    throw new Error('API response is missing the parameters object.');
  }

  return normalizeParameters(payload.parameters);
}

async function getJson(rawUrl) {
  if (typeof globalThis.fetch === 'function') {
    const response = await globalThis.fetch(rawUrl, {
      headers: buildHeaders(),
    });

    const text = await response.text();
    if (!response.ok) {
      const snippet = text ? ` - ${text.slice(0, 200)}` : '';
      throw new Error(
        `Request failed (${response.status} ${response.statusText})${snippet}`
      );
    }

    if (!text) {
      return {};
    }

    try {
      return JSON.parse(text);
    } catch (error) {
      throw new Error(`Invalid JSON from ${rawUrl}: ${error.message}`);
    }
  }

  return requestJsonViaHttps(rawUrl);
}

function requestJsonViaHttps(rawUrl) {
  const url = new URL(rawUrl);

  return new Promise((resolve, reject) => {
    const req = https.request(
      url,
      {
        method: 'GET',
        headers: buildHeaders(),
      },
      (res) => {
        let body = '';
        res.setEncoding('utf8');
        res.on('data', (chunk) => {
          body += chunk;
        });
        res.on('end', () => {
          if (res.statusCode && res.statusCode >= 400) {
            return reject(
              new Error(
                `Request failed (${res.statusCode})${
                  body ? ` - ${body.slice(0, 200)}` : ''
                }`
              )
            );
          }

          if (!body) {
            resolve({});
            return;
          }

          try {
            resolve(JSON.parse(body));
          } catch (error) {
            reject(new Error(`Invalid JSON from ${rawUrl}: ${error.message}`));
          }
        });
      }
    );

    req.on('error', reject);
    req.end();
  });
}

async function updateStrategyFile(filePath, params) {
  let strategy;
  try {
    const content = await fs.readFile(filePath, 'utf8');
    strategy = JSON.parse(content);
  } catch (error) {
    console.warn(`Skipping ${path.basename(filePath)}: ${error.message}`);
    return { updated: false, count: 0 };
  }

  if (!Array.isArray(strategy.parameters)) {
    console.warn(
      `Skipping ${path.basename(filePath)}: "parameters" is not an array.`
    );
    return { updated: false, count: 0 };
  }

  let changed = 0;
  strategy.parameters.forEach((parameter) => {
    if (!parameter || typeof parameter !== 'object') {
      return;
    }
    const name = parameter.name;
    if (
      !name ||
      !Object.prototype.hasOwnProperty.call(params, name)
    ) {
      return;
    }
    const newValue = normalizeNumericValue(params[name]);
    if (!Object.is(parameter.default, newValue)) {
      parameter.default = newValue;
      changed += 1;
    }
  });

  if (changed > 0 && !dryRun) {
    await fs.writeFile(
      filePath,
      `${JSON.stringify(strategy, null, 2)}\n`,
      'utf8'
    );
  }

  return { updated: changed > 0, count: changed };
}

async function main() {
  const settings = await loadSettingsFromDatabase([
    SETTINGS_KEYS.BACKTEST_API_SECRET,
    SETTINGS_KEYS.DOMAIN,
  ]);
  apiSecret = settings[SETTING_KEYS.BACKTEST_API_SECRET] || '';
  const envDomain = process.env.DOMAIN || (await loadEnvValue('DOMAIN'));
  const domain = normalizeDomain(envDomain || settings[SETTING_KEYS.DOMAIN]);
  if (!domain) {
    console.error('DOMAIN (domain only, no port) must be configured before fetching best params.');
    process.exit(1);
  }
  const baseUrl = buildBaseUrl(domain);
  apiBaseUrl = `${baseUrl}/api/backtest/best`;

  const templates = await listStrategyTemplates();
  if (templates.length === 0) {
    console.log('No strategy templates found.');
    return;
  }

  let filesUpdated = 0;
  let paramsUpdated = 0;

  for (const template of templates) {
    const { templateId, filePath } = template;
    try {
      const bestParams = await fetchBestParameters(templateId);
      const { updated, count } = await updateStrategyFile(
        filePath,
        bestParams
      );

      if (updated) {
        filesUpdated += 1;
        paramsUpdated += count;
        console.log(
          `${dryRun ? '[dry-run] ' : ''}Updated ${templateId}: ${count} parameter${
            count === 1 ? '' : 's'
          }`
        );
      } else {
        console.log(`No changes needed for ${templateId}`);
      }
    } catch (error) {
      console.error(`Failed to update ${templateId}: ${error.message}`);
    }
  }

  const summary = dryRun
    ? `Dry run complete: ${filesUpdated} file(s) would be updated (${paramsUpdated} parameter defaults).`
    : `Done: updated ${filesUpdated} file(s) (${paramsUpdated} parameter defaults).`;

  console.log(summary);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
