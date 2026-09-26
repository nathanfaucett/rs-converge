# Plan: Remove Table/Column/Index Generation UUIDs

## Overview
Remove the generation UUID system for tables, columns, and indexes. Use name-based identity instead of UUIDs. Keep existing row-level tombstones and conflict resolution.

## Goals
- Simplify identity system
- Eliminate redundant generation UUID layer
- Keep row-level tombstones for deletion semantics
- Maintain replication and conflict resolution

## Scope
- Remove `TableGenerationId`, `ColumnGenerationId`, `IndexGenerationId`
- Replace with name-based lookups
- Update all related code and docs
- No backwards compatibility

## Files to Modify
- `ofdb/crates/engine/src/id.rs` – delete generation macro and types
- `ofdb/crates/engine/src/schema.rs` – replace `table_id`, `column_id`, `index_id` with name lookups
- `ofdb/crates/engine/src/executor.rs` – remove generation functions, use name directly
- `ofdb/crates/engine/src/engine.rs` – remove public generation methods
- `ofdb/crates/engine/src/change.rs` – update `ChangeKey::Row` to use table name
- `ofdb/crates/engine/src/codec.rs` – change `table: Uuid` to `table: String`
- `ofdb/crates/engine/src/catalog.rs` – verify internal table schemas
- `ofdb/docs/design.md` – update “Generations and Tombstones” section
- `ofdb/docs/goal.md` – update generation identity requirement
- `ofdb/docs/grpc-protocol-plan.md` – adjust any generation ID references

## Actionable Todos

### Phase 1: Delete Generation Types
1. Delete `ofdb/crates/engine/src/id.rs`
2. Remove `TableGenerationId`, `ColumnGenerationId`, `IndexGenerationId` from all imports and uses

### Phase 2: Simplify Schema Lookups
3. Rewrite `schema::table_id()` → `schema::table()` returning `String`
4. Rewrite `schema::column_id()` → `schema::column()` returning column info by name
5. Rewrite `index::index_generation_id()` → `index::index()` returning index info by name
6. Remove `table_deleted()`, `column_deleted()`, `index_deleted()` – use row tombstone checks

### Phase 3: Update Executor
7. Update `create_table()` to use table name directly, no generation ID
8. Update `add_column()` to use column name, no generation ID
9. Update `create_index()` to use index name, no generation ID
10. Update `drop_table()`, `drop_index()`, `alter_table()` to use name lookups
11. Update `insert_row()`, `update_rows()`, `delete_rows()`, `select_rows()` to use table name

### Phase 4: Update Engine API
12. Remove `Engine::table_generation_id()`, `column_generation_id()`, `index_generation_id()`
13. Replace with simple name lookup or inline logic

### Phase 5: Update Codec and Change
14. Change `RowCodec` trait to use `table: String` instead of `table: Uuid`
15. Update `Change::row()` and `ChangeKey::Row` to use `table: String`
16. Update all `RowCodec` implementations

### Phase 6: Update Design Docs
17. Rewrite “Generations and Tombstones” in `design.md`
18. Update generation identity requirement in `goal.md`
19. Adjust any generation ID references in `grpc-protocol-plan.md`

### Phase 7: Tests
20. Run `cargo hack test --feature-powerset --all-targets`
21. Fix compilation errors
22. Verify all tests pass

## Key Design Decisions
- Table identity = name (String)
- Column identity = (table name, column name)
- Index identity = name
- Tombstones stay at row level in internal tables
- No backwards compatibility – delete old code, no migration

## Estimated Files Changed
~20-25 files across `crates/engine/src/` and `docs/`
