# Extending Database Swap

You can add support for a new database by implementing `DatabaseAdapter` and registering it.

## 1) Create an adapter class

Create `database_swap/adapters/postgresql.py` (as an example):

```python
from .base import DatabaseAdapter

class PostgreSQLAdapter(DatabaseAdapter):
    def connect(self) -> bool: ...
    def disconnect(self) -> None: ...
    def test_connection(self) -> bool: ...
    def get_tables(self) -> list[str]: ...
    def get_table_schema(self, table_name: str) -> dict: ...
    def get_table_count(self, table_name: str) -> int: ...
    def read_data(self, table_name: str, batch_size: int = 1000, offset: int = 0): ...
    def write_data(self, table_name: str, data: list[dict], create_table: bool = True) -> bool: ...
    def create_table(self, table_name: str, schema: dict) -> bool: ...
    def drop_table(self, table_name: str) -> bool: ...
```

Reuse patterns from `sqlite.py`, `mysql.py`, and `mongodb.py`.

## 2) Register in the factory

Edit `database_swap/adapters/__init__.py` to import conditionally and expose your adapter when its dependency is installed. Follow the MySQL/MongoDB examples.

## 3) Map types and conversions

If your engine needs special conversions, update `utils.helpers.convert_value` as needed to ensure cross-engine safety.

## 4) Tests and docs

- Add sample connection details and minimal test DB
- Document required fields in `docs/adapters.md`

## 5) Packaging

- Add the dependency to `requirements.txt` (or make it optional and document installation)
