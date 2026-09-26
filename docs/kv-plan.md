# KV crate plan

## Goal and scope

Add `crates/kv`: an async, transactional key/value store with UTF-8 `String` keys, Automerge values, optional expiration, and generation-scoped tombstones. Use `redb` for durable storage and the existing `btree` traits for an in-memory test backend. Keep it separate from the SQL Engine and its row identity; do not add a new storage trait or replication protocol just for KV.

**Done when:** both backends pass the same public API tests; reopening redb preserves data and tombstones; two replicas that acquire different generations for the same key converge on the greatest UUIDv7 after exchanging their retained changes; and expired/tombstoned entries never appear in ordinary reads or scans.

## Domain and behavior

- A **logical key** is arbitrary UTF-8 text, including empty strings, colons, Unicode, and embedded NULs. It may have multiple immutable **generations**, each identified by a UUIDv7 and represented by one Automerge document. A generation's UUID never changes during normal updates.
- A **value** is opaque bytes (`Vec<u8>`), not a row or a serialized Rust type. Empty bytes are a valid live value. The document root stores `value` (Automerge bytes), `expires_at` (optional Unix milliseconds as a signed integer), and `tombstone` (boolean). No value and `tombstone = true` means a deleted generation; an absent expiry is distinct from zero. Reject malformed documents rather than treating them as missing keys.
- `get(key, now)` considers all generations for `key`, picks the generation with the greatest UUID bytes, and then checks _that generation only_ for tombstone and expiry. It does not fall back to an older live generation. Expiry is exclusive of visibility: `now >= expires_at` is expired. Scans return at most one visible value per logical key in UTF-8 byte order and apply the same rule. Pass `now` explicitly or use one injectable clock read per operation so tests and scans have consistent results.
- `set` on a live, non-tombstoned latest generation updates its Automerge document with an incremental change (including expiry changes). `delete` tombstones the latest generation by clearing its value and expiry and setting `tombstone = true`; deleting a missing/already tombstoned key is a no-op. `set` after a tombstone creates a new UUIDv7 generation and removes records for older generations in the same transaction. The new generation becomes the durable high-water mark: peers receiving it replace their older generations as well. A later generation wins even if it is itself deleted or expired. A delete without a newer generation must retain its tombstone so peers can learn about the deletion.
- A concurrently created generation on another machine competes by UUIDv7 byte ordering; the greater UUID wins regardless of whether either generation is tombstoned. UUIDv7 is an ordering policy, **not** a causal clock: clocks can disagree, so a later real-world write may have a smaller UUID. Equal UUIDs with incompatible key/document identities are an error, not an arbitrary winner. Do not use Automerge's per-field conflict selection to choose between generations.

## Storage layout

Use one underlying `BTree<Vec<u8>, Vec<u8>>` per KV store, backed by `btree_redb::RedbBTree<Vec<u8>, Vec<u8>>` in production and `btree::InMemoryBTree<Vec<u8>, Vec<u8>>` in tests. Wrap it with `btree_automerge::AutomergeChangeStore` and reuse `DocumentChangeKey` for snapshot/incremental encoding and document-ID range bounds. Do not create a parallel change-key format.

The conceptual record is `KEY:UUIDv7:change-hash -> Automerge snapshot/incremental bytes`. The _actual_ ordered binary representation must be unambiguous for arbitrary strings: `DocumentId = escaped UTF-8 key + terminator + 16 UUID bytes`, where each zero byte in the key is escaped as `[0, 255]` and the key terminator is `[0, 0]`. `DocumentChangeKey::encode_ordered()` then appends its existing escaped-ID terminator, `DocumentType`, and 32-byte hash. Decode the document ID strictly: valid UTF-8, correctly terminated key, exactly 16 UUID bytes of version 7, and no trailing bytes. Never parse a colon-separated key; colons are valid key content. Verify that the byte ordering groups records by logical key, sorts generations by UUID bytes, and sorts each generation's changes as expected by `btree-automerge`.

For example, `a:b` and `a\0b` must remain distinct; `get("a")` must not include `ab`; scanning `["a", "b")` includes all generations of `a` but not `b`. Use bounds derived from the encoded logical-key prefix or a bounded document-ID range; do not scan the whole database for each `get`.

Automerge's `DocumentType::Metadata` is **not** the KV tombstone: `btree-automerge` reconstruction treats that record as removing the document. Store the KV tombstone in the Automerge document. Snapshots/incrementals retain the existing `DocumentChangeKey` type and 32-byte hash conventions; do not pretend that `hash_heads` is necessarily a single Automerge `ChangeHash`.

## API and transaction boundary

- Provide a small generic store over an existing `BTree<Vec<u8>, Vec<u8>>`, and a transaction over its `BTreeTransaction` rather than inventing another backend trait. Expose `get`, `scan`, `set`, `delete`, `commit`, and `rollback`; keep read/write visibility within a transaction. Consider a separate read-only handle only if the existing `BTreeRead` abstraction makes it useful.
- `set(key, value, expires_at)` and `delete(key)` must read the current winning generation and write its Automerge change in **one** storage transaction. Compute the next UUIDv7 when creating a generation, and ensure it is greater than all known generations for that key; fail explicitly if a suitable UUID cannot be generated. Recheck the winner inside the write transaction so competing local writers cannot silently overwrite a newer generation. Do not overwrite a tombstone in place.
- Merge/import of remote changes is a separate entry point only when replication is implemented: validate document ID, change type, payload, and hash; write idempotently; discard older generations when a greater UUIDv7 arrives, but retain a winning tombstone until superseded. Never accept a delayed older generation over the persisted winner. Do not make `set` a disguised import API. If importing more than one record, commit them atomically. The first version need not implement network transport, peer state, or background expiry sweeps.
- Keep the redb open/create and table initialization in a small adapter or example; `kv` logic should depend on the trait, not on redb types. Add a convenience constructor only if it does not force redb on in-memory users. Avoid changing `ofdb` root API until a concrete caller needs it.

## Implementation checklist

### 1. Verify and, where needed, repair shared storage primitives

- [ ] Write focused tests in `btree-automerge` for multiple incrementals, concurrent changes, snapshot compaction, metadata records, and scans over a bounded ID range. Confirm that hash-ordered increments reconstruct correctly; fix the shared reconstruction/compaction path if not, instead of hiding a KV-only workaround.
- [x] Verify `AutomergeChangeStore::range` passes encoded bounds to its underlying tree. Replaced the full-table scan and in-memory filtering with the encoded bounds; focused tests cover included and excluded bounds. Malformed encoded keys within a requested range still return an error through strict decoding.
- [ ] Check `btree-redb` can store `Vec<u8>` keys/values and open the same named table after process restart. Verify its transactional `range`, commit, and rollback behavior with the encoded key ordering.

### 2. Build `crates/kv`

- [ ] Add the workspace member and a minimal crate manifest using existing dependencies (`btree`, `btree-automerge`, `automerge`, `uuid`; `btree-redb` only where needed). Keep `lib.rs` thin and implementations in dedicated modules. Follow the repo's dependency/feature conventions; require `std` only for clock, UUID generation, or redb paths that need it.
- [ ] Implement reversible, lexicographically ordered `String + UUIDv7 <-> DocumentId` encoding and bounded logical-key lookup/scan. Test Unicode, empty strings, colons, embedded NULs, prefix keys, and malformed IDs.
- [ ] Implement value-document encode/decode and mutation. Validate missing/wrong-type fields, invalid timestamps, and tombstones with an unexpected value. Test empty byte values, absent expiry, and exact expiry boundary.
- [ ] Implement generic transactional `get`, `scan`, `set`, and `delete` using the existing Automerge change-store APIs. Reuse a generation for live updates and allocate a new one only after tombstone; atomically insert the replacement and remove superseded generations. Keep a delete-only tombstone until replaced. Test read-your-writes, rollback, and atomic commit.
- [ ] Add integration tests against both `InMemoryBTree` and `RedbBTree`, including reopen/restart. Inject deterministic UUIDv7s and time in tests; assert that the stored keys are decodable by `DocumentChangeKey` and store Automerge snapshots/incrementals.

### 3. Convergence and retention checks

- [ ] Simulate two independent stores creating/deleting/recreating the same key; exchange raw changes in both orders and assert the highest UUIDv7 generation has the same visible result on both. Include winning tombstones, expired winners, and later changes to losing generations.
- [ ] Decide and implement a safe import route before claiming cross-machine convergence; local key ordering alone does not synchronize databases. Document how missing snapshot/incremental dependencies are handled (queue/retry or explicit error). Test duplicate imports and out-of-order payloads.
- [ ] On local recreation and remote import of a newer UUIDv7, remove older generation records atomically. On import of an older generation, leave the newer generation untouched. A still-winning tombstone remains until a newer generation supersedes it; expiry alone does not remove the winner.
- [ ] Specify the synchronization contract: export the current winning generation (including its snapshot or reconstructible changes), not only an incremental diff against old generations. Confirm that a peer which missed an old tombstone still converges after receiving the replacement, and that a delayed old generation cannot resurrect after pruning.

### 4. Validate

- [ ] Run `cargo test -p kv` and focused `btree-automerge` / `btree-redb` tests, then `cargo hack test --feature-powerset --all-targets`.
- [ ] Run `cargo fmt --all --check`, relevant `cargo clippy` checks, and `just crap`; address complexity in newly changed paths rather than refactoring unrelated modules.
- [ ] Document the public contract with a short in-memory and redb usage example once the API is settled.

## Known risks and explicit limits

- UUIDv7 winner selection is deterministic but not globally chronological under clock skew; a local `set` must never silently create a losing UUID. Whether to wait, use a supplied generator, or return an error is an implementation choice, but silent success is not.
- `btree-automerge` currently orders snapshots before increments and changes within a type by hash, not by causal order. Reconstruction/compaction must be proven against that layout before relying on it for synced KV changes.
- Replacing an old tombstone is safe for eventual _current-state_ convergence only if the new winning generation is durable and propagated. A peer that has not yet received the replacement can temporarily show its old value. A delete with no replacement cannot discard its tombstone. Until the import/export contract above exists, do not advertise the store as fully syncing across machines.
