# Core ideas

These are working assumptions and observations that motivated the project (not guarantees):

- Over long horizons, holding mostly cash and nominal bonds can lose purchasing power after inflation
- Discretionary stock/crypto/metal picking and frequent trading tend to underperform for many retail investors
- Broad equities have historically outperformed high-quality bonds over long horizons (with higher volatility)
- Concentration risk (single country/sector/asset) is a major failure mode; diversification is preferred
- Public equities offer liquid proxies/tilts to many themes (commodities, real estate, crypto-related)
- Stock returns are highly skewed: many names underperform, few drive most of the gains
- For most retail participants, intraday edges are hard to sustain after spreads, slippage and fees
- Reduce human bias with backtests, out-of-sample validation, and automation

## Preliminary conclusions after optimizing dozens of strategies

These are empirical observations from my dataset, assumptions, and execution model; treat them as hypotheses.

- Complex strategies overfit easily: great performance in-sample, poor performance out-of-sample
- In backtests, beating QQQ appeared possible by diversifying over many independent outperformers (implementation-dependent)
- Trend-following that holds rising assets with wide stops over sufficient holding periods can outperform
- More stable strategies (high Sharpe) are typically easier to stick with than maximum-return ones (high CAGR)
- Equity markets tend to drift upward over time, and robust short strategies are hard to come by
- "Sell everything" logic during drawdowns did not outperform in my tests
- Filtering tickers by minimum volume did not outperform in my tests
- Selecting tickers where a strategy performed well in the past did not outperform in my tests
- Raising stops did not outperform in my tests
