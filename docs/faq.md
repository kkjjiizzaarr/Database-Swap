# FAQ

## Does it support PostgreSQL?

Not out of the box. See docs/extending.md to add a new adapter.

## Do I need to create tables first?

No. The migrator will attempt to create target tables/collections using the source schema (or inferred schema for MongoDB). You can also pre-create with your own DDL if you prefer.

## How are data types handled?

`utils.helpers.convert_data_types` converts values between engines (e.g., JSON objects to strings for SQL, booleans to 0/1 for SQL).

## Can I migrate only a few tables?

Yes. Set `migration.tables` in YAML or pass `--tables "a,b,c"` on the CLI.

## How do I speed up migrations?

- Increase `batch_size`
- Lower `rate_limit_delay` (Adaptive limiter may increase it during errors)
- Ensure indexes and hardware resources are sufficient on the target

## What happens on errors?

Batches are retried up to `max_retries` with exponential backoff. Failures are logged and summarized at the end.
