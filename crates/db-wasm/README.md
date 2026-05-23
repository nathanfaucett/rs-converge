# @athena/wasm

WASM bridge for Athena DB in browser environments.

## Build

```bash
just wasm
```

This runs `wasm-pack` and writes the generated package directly to `pkg/`.

## Imports

```ts
import init from "@aicacia/db-wasm";
import { BrowserDatabase, type DatabaseEngineOptions } from "@aicacia/db-wasm";
import {
  type DatabaseTransaction,
  type EngineKey,
  type IndexRangeRequest,
  type PrimaryKeyEntry,
  type PrimaryKey,
  type PrimaryKeyRangeRequest,
  type RowBytes,
  translate_sql_to_query,
  translate_sql_to_query_with_params,
  translate_sql_to_statement,
  translate_sql_to_statement_with_params,
} from "@aicacia/db-wasm";
import type {
  EngineQuery,
  QualifiedPredicate,
  TableSchema,
  EngineValue,
} from "@aicacia/db-wasm";
```

## Minimal Usage

```ts
import init from "@aicacia/db-wasm";
import { BrowserDatabase } from "@aicacia/db-wasm";

await init();

const db = BrowserDatabase.open();

await db.executeSql("CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)");
await db.executeSql("INSERT INTO users (id, name) VALUES (1, 'Ada')");
const result = await db.executeSql("SELECT name FROM users WHERE id = 1");
console.log(result.rows);
```

## SQL Params

```ts
import init from "@aicacia/db-wasm";
import { BrowserDatabase } from "@aicacia/db-wasm";

await init();

const db = BrowserDatabase.open();

await db.executeSql("CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)");
await db.executeSql("INSERT INTO users (id, name) VALUES (1, 'Ada')");

const byPositional = await db.executeSqlWithParams(
  "SELECT name FROM users WHERE id = $1",
  [1],
);

const byNamed = await db.executeSqlWithParams(
  "SELECT name FROM users WHERE id = :id",
  { id: 1 },
);

const insertRecipe = await db.executeSqlWithParams(
  "INSERT INTO recipes (id, ingredients) VALUES (:id, :ingredients)",
  { id: "r-1", ingredients: ["salt", "pepper"] },
);

console.log(byPositional.rows, byNamed.rows);
```

Param conversion rules in WASM:

- `number` -> `INTEGER`/`FLOAT`
- `string` -> `TEXT`
- `null`/`undefined` -> `NULL`
- `Uint8Array` -> `BLOB` (or `UUID` when 16 bytes)
- array/object/boolean (JSON-serializable) -> `JSON` string
- numeric arrays containing only bytes (`0..=255`) are treated as binary bytes

You can also translate with params:

```ts
const query = translate_sql_to_query_with_params(
  "SELECT name FROM users WHERE id = :id",
  schemas,
  { id: 1 },
);

const statement = translate_sql_to_statement_with_params(
  "SELECT name FROM users WHERE id = $1",
  schemas,
  [1],
);
```

## Notes

- This package exports a flat root API from the generated wasm bundle.
- `BrowserDatabase.open()` uses the built-in in-memory store.

## StoreAdapter

`DatabaseEngineOptions` now requires a single ACID boundary: `beginTransaction()` must return a transaction object with row, index, commit, and rollback methods.

```ts
import {
  BrowserDatabase,
  type DatabaseEngineOptions,
  type DatabaseTransaction,
  type EngineKey,
  type IndexEntry,
  type IndexRangeRequest,
  type PrimaryKey,
  type PrimaryKeyEntry,
  type PrimaryKeyRangeRequest,
  type RowBytes,
} from "@aicacia/db-wasm";

const primaryKeyToString = (primaryKey: PrimaryKey): string => primaryKey.join(":");
const copyBytes = (bytes: RowBytes): RowBytes => new Uint8Array(bytes);

class InMemoryTx implements DatabaseTransaction {
  constructor(
    private readonly rowsByTable: Map<string, Map<string, RowBytes>>,
    private readonly entriesByIndex: Map<string, IndexEntry[]>,
    private readonly tableSchemas: Map<string, RowBytes>,
    private readonly indexSchemas: Map<string, RowBytes>,
  ) {}

  async getRow(table: string, primaryKey: PrimaryKey): Promise<RowBytes | undefined> {
    const row = this.rowsByTable.get(table)?.get(primaryKeyToString(primaryKey));
    return row ? copyBytes(row) : undefined;
  }

  async putRow(table: string, primaryKey: PrimaryKey, row: RowBytes): Promise<void> {
    let tableRows = this.rowsByTable.get(table);
    if (!tableRows) {
      tableRows = new Map<string, RowBytes>();
      this.rowsByTable.set(table, tableRows);
    }
    tableRows.set(primaryKeyToString(primaryKey), copyBytes(row));
  }

  async deleteRow(table: string, primaryKey: PrimaryKey): Promise<RowBytes | undefined> {
    const tableRows = this.rowsByTable.get(table);
    const encodedPk = primaryKeyToString(primaryKey);
    const previous = tableRows?.get(encodedPk);
    tableRows?.delete(encodedPk);
    return previous ? copyBytes(previous) : undefined;
  }

  async rangeRows(table: string, _range: PrimaryKeyRangeRequest): Promise<PrimaryKeyEntry[]> {
    const tableRows = this.rowsByTable.get(table);
    if (!tableRows) {
      return [];
    }
    return Array.from(tableRows.entries()).map(([encodedPk, row]) => ({
      primaryKey: encodedPk.split(":").map((part) => Number.parseInt(part, 10)) as PrimaryKey,
      row: copyBytes(row),
    }));
  }

  async addIndex(index: string, indexKey: EngineKey, rowPrimaryKey: PrimaryKey): Promise<void> {
    const entries = this.entriesByIndex.get(index) ?? [];
    entries.push({ indexKey, rowPrimaryKey });
    this.entriesByIndex.set(index, entries);
  }

  async removeIndex(index: string, indexKey: EngineKey, rowPrimaryKey: PrimaryKey): Promise<void> {
    const entries = this.entriesByIndex.get(index) ?? [];
    this.entriesByIndex.set(
      index,
      entries.filter(
        (entry) =>
          JSON.stringify(entry.indexKey) !== JSON.stringify(indexKey) ||
          primaryKeyToString(entry.rowPrimaryKey) !== primaryKeyToString(rowPrimaryKey),
      ),
    );
  }

  async rangeIndex(index: string, _range: IndexRangeRequest): Promise<IndexEntry[]> {
    return this.entriesByIndex.get(index) ?? [];
  }

  async getTableSchema(table: string): Promise<RowBytes | undefined> {
    const row = this.tableSchemas.get(table);
    return row ? copyBytes(row) : undefined;
  }

  async putTableSchema(table: string, row: RowBytes): Promise<void> {
    this.tableSchemas.set(table, copyBytes(row));
  }

  async deleteTableSchema(table: string): Promise<RowBytes | undefined> {
    const previous = this.tableSchemas.get(table);
    this.tableSchemas.delete(table);
    return previous ? copyBytes(previous) : undefined;
  }

  async rangeTableSchemas(): Promise<Array<{ table: string; row: RowBytes }>> {
    return Array.from(this.tableSchemas.entries()).map(([table, row]) => ({
      table,
      row: copyBytes(row),
    }));
  }

  async getIndexSchema(index: string): Promise<RowBytes | undefined> {
    const row = this.indexSchemas.get(index);
    return row ? copyBytes(row) : undefined;
  }

  async putIndexSchema(index: string, row: RowBytes): Promise<void> {
    this.indexSchemas.set(index, copyBytes(row));
  }

  async deleteIndexSchema(index: string): Promise<RowBytes | undefined> {
    const previous = this.indexSchemas.get(index);
    this.indexSchemas.delete(index);
    return previous ? copyBytes(previous) : undefined;
  }

  async rangeIndexSchemas(): Promise<Array<{ index: string; row: RowBytes }>> {
    return Array.from(this.indexSchemas.entries()).map(([index, row]) => ({
      index,
      row: copyBytes(row),
    }));
  }

  async commit(): Promise<void> {}

  async rollback(): Promise<void> {}
}

const rowsByTable = new Map<string, Map<string, RowBytes>>();
const entriesByIndex = new Map<string, IndexEntry[]>();
const tableSchemas = new Map<string, RowBytes>();
const indexSchemas = new Map<string, RowBytes>();

const options: DatabaseEngineOptions = {
  async beginTransaction(_mode) {
    return new InMemoryTx(rowsByTable, entriesByIndex, tableSchemas, indexSchemas);
  },
};

const db = await BrowserDatabase.openWithBackend(options);
```

### Required Guarantees

- All adapter methods execute through a transaction object returned by `beginTransaction()`.
- Schema operations are transaction-bound and mandatory.
- `commit()` and `rollback()` are mandatory.
- There are no legacy tree callbacks and no non-transactional fallback paths.

### EngineKey Ordering Contract

- `EngineKey` values are opaque encoded bytes.
- Compare `EngineKey` values using bytewise lexicographic ordering.
- Do not decode, normalize, or reinterpret `EngineKey` bytes in adapter code.
- `rangeIndex` must apply bounds and return results in ascending bytewise key order.
