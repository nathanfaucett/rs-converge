#![cfg(target_arch = "wasm32")]

use db_wasm::BrowserDatabase;
use db_wasm::DatabaseEngineOptions;
use db_wasm::types::{ColumnSchema, EngineType, TableSchema};
use wasm_bindgen::JsCast;
use wasm_bindgen_test::wasm_bindgen_test;

const ADAPTER_SCRIPT: &str = r#"
(() => {
  const state = {
    trees: new Map(),
  };

  const toBytes = (value) => Array.from(value ?? []);
  const keyString = (key) => JSON.stringify(toBytes(key));
  const compareBytes = (left, right) => {
    const a = toBytes(left);
    const b = toBytes(right);
    const limit = Math.min(a.length, b.length);
    for (let index = 0; index < limit; index += 1) {
      if (a[index] !== b[index]) {
        return a[index] - b[index];
      }
    }
    return a.length - b.length;
  };
  const treeStore = (tree) => {
    let store = state.trees.get(tree);
    if (!store) {
      store = new Map();
      state.trees.set(tree, store);
    }
    return store;
  };

  return {
    beginTransaction(_mode) {
      return Promise.resolve({
        get(tree, key) {
          const store = state.trees.get(tree);
          const row = store?.get(keyString(key));
          return Promise.resolve(row ? [...row] : undefined);
        },
        put(tree, key, value) {
          const store = treeStore(tree);
          store.set(keyString(key), [...value]);
          return Promise.resolve();
        },
        delete(tree, key) {
          const store = state.trees.get(tree);
          const encodedKey = keyString(key);
          const previous = store?.get(encodedKey);
          store?.delete(encodedKey);
          return Promise.resolve(previous !== undefined ? [...previous] : undefined);
        },
        range(tree, range) {
          const store = state.trees.get(tree);
          if (!store) {
            return Promise.resolve([]);
          }
          const start = range.start;
          const end = range.end;
          const startInclusive = range.startInclusive;
          const endInclusive = range.endInclusive;
          const out = Array.from(store.entries())
            .map(([encodedKey, value]) => ({ key: JSON.parse(encodedKey), value: [...value] }))
            .filter(({ key }) => {
              if (start) {
                const cmp = compareBytes(key, start);
                if (cmp < 0 || (cmp === 0 && !startInclusive)) {
                  return false;
                }
              }
              if (end) {
                const cmp = compareBytes(key, end);
                if (cmp > 0 || (cmp === 0 && !endInclusive)) {
                  return false;
                }
              }
              return true;
            })
            .sort((left, right) => compareBytes(left.key, right.key));
          return Promise.resolve(out);
        },
        commit() {
          return Promise.resolve();
        },
        rollback() {
          return Promise.resolve(undefined);
        },
      });
    },
  };
})()
"#;

const DELAYED_ADAPTER_SCRIPT: &str = r#"
(() => {
  const delay = (value) => new Promise((resolve) => setTimeout(() => resolve(value), 5));
  const state = {
    trees: new Map(),
  };

  const toBytes = (value) => Array.from(value ?? []);
  const keyString = (key) => JSON.stringify(toBytes(key));
  const treeStore = (tree) => {
    let store = state.trees.get(tree);
    if (!store) {
      store = new Map();
      state.trees.set(tree, store);
    }
    return store;
  };

  return {
    beginTransaction(_mode) {
      return delay({
        get(tree, key) {
          const store = state.trees.get(tree);
          const row = store?.get(keyString(key));
          return delay(row ? [...row] : undefined);
        },
        put(tree, key, value) {
          treeStore(tree).set(keyString(key), [...value]);
          return delay(undefined);
        },
        delete(tree, key) {
          const store = state.trees.get(tree);
          const encodedKey = keyString(key);
          const previous = store?.get(encodedKey);
          store?.delete(encodedKey);
          return delay(previous !== undefined ? [...previous] : undefined);
        },
        range(tree, _range) {
          const store = state.trees.get(tree);
          if (!store) {
            return delay([]);
          }
          return delay(Array.from(store.entries()).map(([encodedKey, value]) => ({ key: JSON.parse(encodedKey), value: [...value] })));
        },
        commit() {
          return delay(undefined);
        },
        rollback() {
          return delay(undefined);
        },
      });
    },
  };
})()
"#;

#[wasm_bindgen_test]
async fn open_with_strict_transaction_adapter_supports_schema_lifecycle() {
  let adapter_value = js_sys::eval(ADAPTER_SCRIPT).expect("adapter eval should succeed");
  let options: DatabaseEngineOptions = adapter_value.unchecked_into();
  let db = BrowserDatabase::open_with_backend(options)
    .await
    .expect("open_with_backend should succeed");

  let schema = TableSchema {
    name: "users".to_string(),
    columns: vec![
      ColumnSchema {
        name: "id".to_string(),
        data_type: EngineType::Uuid,
      },
      ColumnSchema {
        name: "name".to_string(),
        data_type: EngineType::Text,
      },
    ],
    primary_key: vec![0],
  };

  db.register_table(schema.clone())
    .await
    .expect("register_table should succeed");

  let described = db.describe_table("users");
  assert_eq!(described, Some(schema));

  db.drop_table("users")
    .await
    .expect("drop_table should succeed");

  let described_after_drop = db.describe_table("users");
  assert_eq!(described_after_drop, None);
}

#[wasm_bindgen_test]
async fn open_with_delayed_promise_adapter_supports_schema_lifecycle() {
  let adapter_value = js_sys::eval(DELAYED_ADAPTER_SCRIPT).expect("adapter eval should succeed");
  let options: DatabaseEngineOptions = adapter_value.unchecked_into();
  let db = BrowserDatabase::open_with_backend(options)
    .await
    .expect("open_with_backend should succeed");

  let schema = TableSchema {
    name: "delayed_users".to_string(),
    columns: vec![
      ColumnSchema {
        name: "id".to_string(),
        data_type: EngineType::Uuid,
      },
      ColumnSchema {
        name: "name".to_string(),
        data_type: EngineType::Text,
      },
    ],
    primary_key: vec![0],
  };

  db.register_table(schema.clone())
    .await
    .expect("register_table should succeed");

  let described = db.describe_table("delayed_users");
  assert_eq!(described, Some(schema));
}
