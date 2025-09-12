# Architecture overview

This project is organized into modular layers for clarity and extensibility.

- CLI: `database_swap/cli/interface.py` – user entry point and argument parsing
- Config: `database_swap/config/settings.py` – YAML loading, defaults, overrides
- Core: `database_swap/core/` – `migrator.py`, `rate_limiter.py`, `validator.py`
- Adapters: `database_swap/adapters/` – DB-specific implementations
- Utils: `database_swap/utils/` – logging, helpers, progress, timers

## Data flow

1) CLI parses args and loads config
2) Migrator is created with config
3) Migrator resolves source/target adapters and connects
4) For each table/collection:
   - Read in batches from source
   - Validate and convert data types (`utils.helpers.convert_data_types`)
   - Rate limit writes (`core.rate_limiter.AdaptiveRateLimiter`)
   - Write to target and track stats
5) Final stats and logs are emitted

## Key components

- DatabaseMigrator: Orchestrates migration, batching, and stats
- RateLimiter / AdaptiveRateLimiter: Applies delays based on error rate
- DataValidator / SchemaValidator: Optional validation of rows and schemas
- MigrationStats: Aggregates counters and durations

## Error handling

- Retries for failed batch writes (configurable `max_retries`)
- Exponential backoff per batch attempt
- Non-fatal errors are collected and surfaced at the end

## Logging

- File and colored console logging via `utils.logger.setup_logging`
- Progress logs per table via `ProgressLogger`
