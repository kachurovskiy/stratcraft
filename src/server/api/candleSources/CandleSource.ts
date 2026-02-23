import { Candle } from '../../../shared/types/StrategyTemplate';

export type CandleSourceResult = {
  candles: Candle[];
  noData: boolean;
};

export interface CandleSource {
  getHistoricalCandles(symbol: string, startDate: Date, endDate: Date): Promise<CandleSourceResult>;
  getSourceName(): string;
}
