# Recipes

## SQLite -> MySQL

```powershell
database-swap migrate `
  --source-type sqlite --source-database .\app.db `
  --target-type mysql --target-host localhost --target-database app_migrated --target-username root --target-password `
  --batch-size 500 --rate-limit-delay 0.2
```

## MySQL -> MongoDB (subset of tables)

```powershell
database-swap migrate `
  --source-type mysql --source-host mysql.local --source-database ecommerce --source-username reader --source-password `
  --target-type mongodb --target-host mongo.local --target-database ecommerce_nosql `
  --tables "users,orders,products"
```

## Using a YAML config

```powershell
database-swap migrate --config .\migration.yaml
```

Example config snippets are in the `examples/` directory.

## Analyze before migrating

```powershell
database-swap analyze --db-type mysql --host localhost --database ecommerce --username reader
```

## Dry run to validate

```powershell
database-swap migrate --dry-run --config .\migration.yaml
```
