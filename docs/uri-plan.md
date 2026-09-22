# URI Plan: Unified Database Open

## Goal

Add a single public entry point `Database::open_uri(uri: &str)` that can open any
ofdb URI, dispatching to the appropriate kernel based on the URI scheme. This
replaces the two separate constructors (`Database::open` and
`Database::in_memory`) with one ergonomic API.

## URI Scheme

| URI | Meaning | Kernel | Required Features |
| --- | --- | --- | --- |
| `:in_memory:` | In-memory database | `InMemoryKernel` | `in-memory`, `automerge` |
| `ofdb://./local.db` | File database, relative path | `RedbKernel` | `redb`, `automerge` |
| `ofdb://tmp/data.db` | File database, relative path | `RedbKernel` | `redb`, `automerge` |
| `ofdb:///absolute/path.db` | File database, absolute path | `RedbKernel` | `redb`, `automerge` |

### Parsing Rules

- The URI is parsed with a simple, dependency-free parser (no external crate).
- `:in_memory:` is recognized by its leading `:` and trailing `:` — it is not a
  true RFC 3986 URI, but a sentinel string.
- `ofdb://` is the scheme prefix. The authority component is empty. The path
  follows the `//` and is interpreted as a filesystem path:
  - `ofdb://./local.db` → `./local.db`
  - `ofdb://tmp/data.db` → `tmp/data.db`
  - `ofdb:///abs/path.db` → `/abs/path.db`
- An empty path after `ofdb://` (e.g. `ofdb://`) is an error.
- Unknown schemes return an `EngineError` variant `UnsupportedScheme`.

## API Changes

### `src/database.rs`

Add a single constructor:

```rust
impl Database {
    /// Open a database from a URI string.
    ///
    /// Supports `:in_memory:` and `ofdb://<path>` schemes. The path is
    /// interpreted as a filesystem path relative to the current working
    /// directory when it does not begin with `/`.
    pub fn open_uri(uri: &str) -> Result<Self, EngineError> {
        // parse and dispatch
    }
}
```

The existing `Database::open` and `Database::in_memory` constructors remain as
private helpers (or are inlined) so the public surface is just `open_uri`.

### Feature-Gated Dispatch

`open_uri` must compile under all feature combinations. When the requested
scheme's required features are not enabled, it returns an error at runtime
(not a compile error), so that a single binary can support multiple URIs by
enabling the right features at build time.

```rust
match scheme {
    InMemory => {
        #[cfg(all(feature = "automerge", feature = "in-memory"))]
        { Ok(Self::in_memory()) }
        #[cfg(not(all(feature = "automerge", feature = "in-memory")))]
        { Err(EngineError::custom("in-memory kernel not available")) }
    }
    File => {
        #[cfg(all(feature = "automerge", feature = "redb"))]
        { Self::open(path) }
        #[cfg(not(all(feature = "automerge", feature = "redb")))]
        { Err(EngineError::custom("redb kernel not available")) }
    }
}
```

## Implementation Steps

1. **Add URI parser** — create `src/uri.rs` with a `parse_uri(uri: &str) ->
   Result<UriScheme, UriError>` function. Keep it `no_std`-compatible (uses
   `alloc` only).

2. **Add `Database::open_uri`** — wire the parser to the existing constructors.

3. **Update examples** — switch `examples/*.rs` to use `open_uri` where
   appropriate, demonstrating both schemes.

4. **Tests** — add unit tests in `src/database.rs` (or `tests/uri.rs`) covering:
   - `:in_memory:` parses correctly
   - `ofdb://./local.db` parses to `./local.db`
   - `ofdb://tmp/data.db` parses to `tmp/data.db`
   - `ofdb:///abs/path.db` parses to `/abs/path.db`
   - Unknown scheme returns an error
   - Empty path returns an error

## Out of Scope

- URI query parameters (e.g. `ofdb://path?mode=ro`) — reserved for future.
- Scheme registration / custom schemes — not needed.
- URL encoding/decoding — paths are taken literally.
