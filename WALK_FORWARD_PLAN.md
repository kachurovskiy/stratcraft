# Walk-Forward Introduction Plan

## Goals
- Add rolling, time-based walk-forward evaluation without disrupting the current optimize/verify/balance pipeline.
- Keep all results isolated from the existing `backtest_cache` to avoid mixing windowed runs with global caches.
- Use server-side rendering and standard form posts only (no new REST APIs).

## Current Baseline (Summary)
- Optimization runs on training tickers within `OPTIMIZER_TRAINING_START_DATE` → `OPTIMIZER_TRAINING_END_DATE`.
- Verification runs on all tickers within `VERIFY_WINDOW_START_DATE` → `VERIFY_WINDOW_END_DATE`.
- Balance runs on training vs validation tickers within `BALANCE_WINDOW_START_DATE` → `BALANCE_WINDOW_END_DATE`.
- The optimize job (`src/server/jobs/handlers/optimizeHandler.ts`) chains optimize → verify → balance.

## Walk-Forward Definition
Define a rolling set of windows built from settings and data availability:
- `training_months` (length of the optimization window)
- `forward_months` (length of the out-of-sample window immediately following training)
- `step_months` (how far to advance each window)
- `max_windows` (cap for runtime control; `0` means no cap)

For each window:
1. Optimize parameters on training tickers within the training window.
2. Evaluate the best parameters on the forward window (default: all tickers).
3. Persist per-window metrics and parameters to walk-forward tables.

## Data Model
Add two new tables to keep walk-forward separate from the existing cache:

1. `walk_forward_runs`
   - `id` (UUID PK)
   - `template_id`
   - `status` (queued/running/succeeded/failed/cancelled)
   - `started_at`, `finished_at`
   - `training_months`, `forward_months`, `step_months`, `max_windows`
   - `window_count`
   - `settings_snapshot` (JSONB, optional)
   - `error_message` (nullable)

2. `walk_forward_windows`
   - `id` (UUID PK)
   - `run_id` (FK to `walk_forward_runs`)
   - `window_index`
   - `training_start`, `training_end`
   - `forward_start`, `forward_end`
   - `best_parameters` (JSONB)
   - `train_metrics` (JSONB)
   - `forward_metrics` (JSONB)
   - `balance_training_metrics` (JSONB, optional)
   - `balance_validation_metrics` (JSONB, optional)

Indexes:
- `walk_forward_runs(template_id, started_at DESC)`
- `walk_forward_windows(run_id, window_index)`

Add types and repos:
- `src/server/database/types.ts`
- `src/server/database/repos/WalkForwardRepo.ts`
- `src/server/database/Database.ts`
- `src/server/database/pg.sql`

## Settings
Add new settings with defaults in `src/server/database/pg.sql`, `src/server/constants.ts`, and `src/server/routes/settings.ts`:
- `WALK_FORWARD_TRAINING_MONTHS` (default `36`)
- `WALK_FORWARD_FORWARD_MONTHS` (default `12`)
- `WALK_FORWARD_STEP_MONTHS` (default `6`)
- `WALK_FORWARD_MAX_WINDOWS` (default `0`)
- `WALK_FORWARD_ENABLED` (default `false`)

Also include these keys in `engine/src/data_context.rs` under `SNAPSHOT_ALLOWED_SETTINGS` so snapshot runs use the same configuration.

## Engine Changes
Add a new CLI command `walk-forward`:
- File: `engine/src/commands/walk_forward.rs`
- Wire into `engine/src/commands/mod.rs` and `engine/src/main.rs`.

Implementation outline:
1. Load settings and market data (same pattern as `optimize.rs` / `verify.rs`).
2. Build window list from settings and available dates.
3. For each window:
   - Build a training context with `TickerScope::TrainingOnly` and `MarketDataFilters` set to the training window.
   - Run optimization using a new method that **does not write** to `backtest_cache`.
     - Option A: Add `optimize_local_search_ephemeral(...) -> OptimizationResult`.
     - Option B: Add a flag to `optimize_local_search` to skip cache writes and DB updates.
   - Evaluate the best parameters on the forward window with `TickerScope::AllTickers` using `run_parameter_batch(..., use_cache = false)`.
   - Optionally compute balance metrics for the forward window (training vs validation) using the same batch evaluation and store results.
   - Persist per-window rows in `walk_forward_windows`.
4. Update `walk_forward_runs` status and window count on completion.

Key constraints:
- Avoid writing to `backtest_cache`, `backtest_best_params`, or template optimization versions.
- Keep `--data-file` support consistent with other commands.

## Server Changes (SSR + Forms Only)
Add a new job type and handler:
- Add `walk-forward` to `JobType` in `src/server/jobs/JobScheduler.ts`.
- Implement `src/server/jobs/handlers/walkForwardHandler.ts` to:
  - Iterate templates (or accept a template id from job metadata).
  - Call `engineCli.run('walk-forward', [templateId])`.
  - Record job summaries.

Add form-driven routes (no REST JSON):
- In `src/server/routes/templates.ts`, add:
  - `POST /templates/:templateId/walk-forward` (admin only)
  - Schedule job and redirect back to the template page with a success message.

Add SSR display:
- Add a Walk-Forward section to `src/views/pages/template.hbs`.
- Fetch latest run + window rows in `templates.ts` and pass data to the view.
- Show:
  - Last run status and settings
  - Per-window table with training/forward dates and forward metrics
  - Summary aggregates (mean/median forward CAGR, Sharpe, drawdown)

## Scoring & Ranking (Phase 2)
Initial phase: display-only, no impact on template ranking.
Optional later:
- Add aggregate walk-forward score to `templateScore.ts` with new weights.
- Controlled by a setting (e.g., `TEMPLATE_SCORE_WALK_FORWARD_WEIGHT`).

## Tests
Engine:
- Add a walk-forward integration test in `engine/tests/pipeline.rs` using a small snapshot.
- Assert that `walk_forward_runs` and `walk_forward_windows` rows are persisted and non-empty.

Server:
- Add minimal repo mapping tests for `WalkForwardRepo`.
- Manual UI test: run walk-forward from a template page and verify SSR output.

## Rollout
1. Ship behind `WALK_FORWARD_ENABLED = false`.
2. Add admin-only trigger button.
3. Validate performance and runtime on a single template.
4. Enable on a subset of templates or via a manual schedule.

## Open Decisions
- Forward evaluation ticker scope: all tickers vs validation-only (default recommendation: all tickers).
- Whether to compute balance metrics per window.
- Default window sizes and max window cap based on typical runtime.
