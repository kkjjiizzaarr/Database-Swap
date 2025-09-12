# Adapters

Database adapters implement a common interface (`DatabaseAdapter`) to interact with each database engine.

Built-in adapters

- SQLite (`database_swap.adapters.sqlite.SQLiteAdapter`)
- MySQL (`database_swap.adapters.mysql.MySQLAdapter`) – requires mysql-connector-python
- MongoDB (`database_swap.adapters.mongodb.MongoDBAdapter`) – requires pymongo

The adapter factory (`database_swap.adapters.get_adapter`) resolves a type string to the adapter class if its dependency is available.

## Required connection fields

- SQLite: database (file path)
- MySQL: host, database, username (password typically required)
- MongoDB: host, database (username/password optional)

## Common capabilities

- connect(), disconnect(), test_connection()
- get_tables(), get_table_schema(name), get_table_count(name)
- read_data(name, batch_size): yields batches of dict rows
- write_data(name, data, create_table)
- create_table(name, schema), drop_table(name)

Schemas are lightweight dicts with a `columns` map, optional `primary_keys`, and `indexes`.

```python
schema = {
  'columns': {
    'id': { 'type': 'INTEGER', 'nullable': False },
    'name': { 'type': 'TEXT', 'nullable': True }
  },
  'primary_keys': ['id'],
  'indexes': []
}
```

MongoDB schemas are inferred from sample documents and are only advisory.
