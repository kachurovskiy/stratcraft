import { createHash } from 'crypto';
import type { TickerAssetType } from '../database/types';
import { normalizeUppercaseString as normalizeAssetName } from './stringNormalization';

export type LeveragedExpenseRatios = Record<2 | 3 | 5, number>;

export type AssetClassificationSettings = {
  etfBaseExpenseRatio: number;
  inverseEtfExpenseRatio: number;
  commodityTrustExpenseRatio: number;
  bondEtfExpenseRatio: number;
  incomeEtfExpenseRatio: number;
  leveragedExpenseRatios: LeveragedExpenseRatios;
};

export type AssetClassification = {
  assetType: TickerAssetType;
  expenseRatio: number | null;
};

type NumericLeverageDetection = { multiplier: 2 | 3 | 5; isInverse: boolean };

const METAL_KEYWORDS = ['GOLD', 'SILVER', 'PLATINUM', 'PALLADIUM', 'PRECIOUS', 'METALS', 'COPPER'];
const BOND_KEYWORDS = ['BOND', 'TREASURY', 'MUNICIPAL', 'MUNI', 'CORPORATE', 'FIXED INCOME', 'AGGREGATE'];
const INCOME_ETF_KEYWORDS = [
  'BUYWRITE',
  'BUY-WRITE',
  'COVERED CALL',
  'COVEREDCALL',
  'ENHANCED INCOME',
  'PREMIUM INCOME',
  'INCOME BUILDER',
  'INCOME ETF',
  'YIELD ENHANCED'
];

function detectNumericLeverage(normalizedName: string): NumericLeverageDetection | null {
  if (!normalizedName) {
    return null;
  }
  const match = normalizedName.match(/([+-]?)([235])\s*X\b/);
  if (match) {
    const sign = match[1] || '';
    const value = Number(match[2]);
    if (value === 2 || value === 3 || value === 5) {
      return {
        multiplier: value as 2 | 3 | 5,
        isInverse: sign.trim() === '-'
      };
    }
  }
  return null;
}

function isEtfLike(normalizedName: string): boolean {
  if (!normalizedName) {
    return false;
  }
  if (
    normalizedName.includes('ETF') ||
    normalizedName.includes('ETN') ||
    normalizedName.includes('EXCHANGE TRADED')
  ) {
    return true;
  }
  const hasFund = normalizedName.includes(' FUND');
  const hasIndex = normalizedName.includes(' INDEX');
  const hasTrust = normalizedName.includes(' TRUST');
  const hasSeries = normalizedName.includes(' SERIES');
  if (hasTrust && (hasSeries || normalizedName.includes(' ETF'))) {
    return true;
  }
  if (hasFund && hasIndex) {
    return true;
  }
  return false;
}

function detectTextualLeverage(normalizedName: string): 2 | 3 | 5 | null {
  if (!normalizedName) {
    return null;
  }
  if (normalizedName.includes('ULTRAPRO')) {
    return 3;
  }
  if (normalizedName.includes('PROSHARES') && normalizedName.includes('ULTRA')) {
    return 2;
  }
  if (normalizedName.includes('TRIPLE')) {
    return 3;
  }
  return null;
}

function toLeverageAssetType(multiplier: 2 | 3 | 5): TickerAssetType {
  switch (multiplier) {
    case 2:
      return 'leveraged_2x';
    case 3:
      return 'leveraged_3x';
    case 5:
      return 'leveraged_5x';
    default:
      return 'equity';
  }
}

function toInverseLeverageAssetType(multiplier: 2 | 3 | 5): TickerAssetType {
  switch (multiplier) {
    case 2:
      return 'inverse_leveraged_2x';
    case 3:
      return 'inverse_leveraged_3x';
    case 5:
      return 'inverse_leveraged_5x';
    default:
      return 'inverse_etf';
  }
}

function hasInverseKeywords(normalizedName: string): boolean {
  if (!normalizedName) {
    return false;
  }
  if (normalizedName.includes('INVERSE') || normalizedName.includes(' BEAR') || normalizedName.includes('ULTRASHORT')) {
    return true;
  }
  const standaloneShort = /\bSHORT\b/.test(normalizedName);
  if (standaloneShort) {
    if (/\bSHORT[-\s]+TERM\b/.test(normalizedName) || /\bSHORT[-\s]+DURATION\b/.test(normalizedName)) {
      return false;
    }
    return true;
  }
  if (normalizedName.includes('-1X') || normalizedName.includes('(-1X')) {
    return true;
  }
  return false;
}

function isCommodityTrustName(normalizedName: string): boolean {
  if (!normalizedName.includes('TRUST')) {
    return false;
  }
  if (normalizedName.includes('PHYSICAL')) {
    return true;
  }
  return METAL_KEYWORDS.some((keyword) => normalizedName.includes(keyword));
}

function isBondEtfName(normalizedName: string): boolean {
  return BOND_KEYWORDS.some((keyword) => normalizedName.includes(keyword));
}

function isIncomeEtfName(normalizedName: string): boolean {
  return INCOME_ETF_KEYWORDS.some((keyword) => normalizedName.includes(keyword));
}

export function classifyAssetFromName(
  name: string | null | undefined,
  settings: AssetClassificationSettings
): AssetClassification {
  const normalized = normalizeAssetName(name);
  const inverseKeywords = hasInverseKeywords(normalized);
  const numericLeverage = detectNumericLeverage(normalized);
  if (numericLeverage) {
    const assetType = inverseKeywords || numericLeverage.isInverse
      ? toInverseLeverageAssetType(numericLeverage.multiplier)
      : toLeverageAssetType(numericLeverage.multiplier);
    return {
      assetType,
      expenseRatio: settings.leveragedExpenseRatios[numericLeverage.multiplier]
    };
  }

  if (isCommodityTrustName(normalized)) {
    return {
      assetType: 'commodity_trust',
      expenseRatio: settings.commodityTrustExpenseRatio
    };
  }

  if (isEtfLike(normalized)) {
    const textualLeverage = detectTextualLeverage(normalized);
    if (textualLeverage) {
      return {
        assetType: inverseKeywords ? toInverseLeverageAssetType(textualLeverage) : toLeverageAssetType(textualLeverage),
        expenseRatio: settings.leveragedExpenseRatios[textualLeverage]
      };
    }
    if (inverseKeywords) {
      return {
        assetType: 'inverse_etf',
        expenseRatio: settings.inverseEtfExpenseRatio
      };
    }
    if (isBondEtfName(normalized)) {
      return {
        assetType: 'bond_etf',
        expenseRatio: settings.bondEtfExpenseRatio
      };
    }
    if (isIncomeEtfName(normalized)) {
      return {
        assetType: 'income_etf',
        expenseRatio: settings.incomeEtfExpenseRatio
      };
    }
    return {
      assetType: 'etf',
      expenseRatio: settings.etfBaseExpenseRatio
    };
  }

  return {
    assetType: 'equity',
    expenseRatio: null
  };
}

export function isTrainingTicker(
  symbol: string,
  alwaysValidationTickers: Set<string>,
  trainingAllocationRatio: number
): boolean {
  const normalized = normalizeAssetName(symbol);
  if (alwaysValidationTickers.has(normalized)) {
    return false;
  }
  const hash = createHash('sha256').update(normalized).digest();
  const value = hash.readUInt32BE(0) / 0xffffffff;
  return value < trainingAllocationRatio;
}
