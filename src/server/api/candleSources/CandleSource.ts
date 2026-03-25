import { Candle } from '../../types/StrategyTemplate';

export type CandleSourceResult = {
  candles: Candle[];
  noData: boolean;
};

export interface CandleSource {
  getHistoricalCandles(
    symbol: string,
    startDate: Date,
    endDate: Date,
    abortSignal?: AbortSignal
  ): Promise<CandleSourceResult>;
  getSourceName(): string;
}
