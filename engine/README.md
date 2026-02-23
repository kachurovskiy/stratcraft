# engine (Rust)

Fast strategy optimization + backtesting engine for StratCraft. Use it to train/verify parameter sets, refresh signals, and run backtests. Most commands work directly against the database; `optimize` and `verify` can also run against a market data snapshot file via `--data-file` (generate one with `export-market-data`).

## Build

Build the Rust engine binary:
```bash
cd engine
cargo build --release
```

Other useful Rust commands:
```bash
cargo fmt
cargo check
cargo test
```

## Run

```bash
./target/release/engine <command> [options]
```

## Commands

Optimize parameters (auto-detects tunables, training tickers 2021-2024):
```bash
./target/release/engine optimize atr
./target/release/engine optimize atr --data-file ../data/market-data.bin
```

Verify cached parameter sets on 2025 data (all tickers):
```bash
./target/release/engine verify atr
./target/release/engine verify atr --data-file ../data/market-data.bin
```

Generate missing signals for active strategies:
```bash
./target/release/engine generate-signals
```

Backtest active strategies for the given month windows (comma or space separated, scope: validation|training|all):
```bash
./target/release/engine backtest-active 6,12
./target/release/engine backtest-active --scope training 3 6 12
```

Backtest strategies linked to live accounts (all tickers):
```bash
./target/release/engine backtest-accounts
```

Plan account operations for strategies with accounts + start dates:
```bash
./target/release/engine plan-operations
```

Reconcile live trades with broker order state:
```bash
./target/release/engine reconcile-trades
```

Export a market data snapshot (default `../data/market-data.bin`):
```bash
./target/release/engine export-market-data
./target/release/engine export-market-data --output ..\\data\\market-data.bin
```

Train the LightGBM model:
```bash
./target/release/engine train-lightgbm
./target/release/engine train-lightgbm --output engine\\src\\models\\lightgbm_model.txt
./target/release/engine train-lightgbm --num-iterations 800 --learning-rate 0.05
```

Notes:
- Training requires the LightGBM CLI (`lightgbm`).
- On Windows, StratCraft uses the vendored binary at `engine/vendor/lightgbm.exe` (no `PATH` changes needed).
- On Linux/macOS, install `lightgbm` via your package manager (or put it on `PATH`).
