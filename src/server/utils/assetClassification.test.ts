import {
  type AssetClassificationSettings,
  classifyAssetFromName
} from './assetClassification';

const SETTINGS: AssetClassificationSettings = {
  etfBaseExpenseRatio: 0.2,
  inverseEtfExpenseRatio: 0.9,
  commodityTrustExpenseRatio: 0.4,
  bondEtfExpenseRatio: 0.15,
  incomeEtfExpenseRatio: 0.35,
  leveragedExpenseRatios: {
    2: 0.95,
    3: 1.05,
    5: 1.25
  }
};

describe('classifyAssetFromName', () => {
  it('classifies numeric leverage before ETF-like detection', () => {
    expect(classifyAssetFromName('Direxion Daily TSLA Bear 3X Shares', SETTINGS)).toEqual({
      assetType: 'inverse_leveraged_3x',
      expenseRatio: 1.05
    });
  });

  it('classifies textual leveraged ETFs', () => {
    expect(classifyAssetFromName('ProShares Ultra Technology ETF', SETTINGS)).toEqual({
      assetType: 'leveraged_2x',
      expenseRatio: 0.95
    });
  });

  it('classifies commodity trusts', () => {
    expect(classifyAssetFromName('SPDR Gold Trust', SETTINGS)).toEqual({
      assetType: 'commodity_trust',
      expenseRatio: 0.4
    });
  });

  it('does not treat short duration bond ETFs as inverse', () => {
    expect(classifyAssetFromName('iShares Short Duration Bond ETF', SETTINGS)).toEqual({
      assetType: 'bond_etf',
      expenseRatio: 0.15
    });
  });

  it('classifies income ETFs from name keywords', () => {
    expect(classifyAssetFromName('Global X Nasdaq 100 Covered Call ETF', SETTINGS)).toEqual({
      assetType: 'income_etf',
      expenseRatio: 0.35
    });
  });

  it('falls back to equity when no ETF traits are present', () => {
    expect(classifyAssetFromName('Apple Inc.', SETTINGS)).toEqual({
      assetType: 'equity',
      expenseRatio: null
    });
  });
});
