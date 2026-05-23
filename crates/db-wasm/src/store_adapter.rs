use async_stream::stream;
use core::fmt;
use core::ops::{Bound, RangeBounds};
use db_core::{BTreeError, BTreeResult, MaybeSend, NamedTreeProvider, NamedTreeTransaction};
use db_engine::{EngineKey, PrimaryKey};
use db_types::key_encoding::{DefaultEncoding, KeyEncoding, RowEncoding};
use db_types::persistence::decode_index_schema_row;
use futures::Stream;
use js_sys::{Function, JSON, Promise, Reflect};
use serde::de::{DeserializeOwned, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

#[wasm_bindgen(typescript_custom_section)]
const STORE_ADAPTER_TS: &str = r#"
export type PrimaryKeyTuple = [
  number, number, number, number,
  number, number, number, number,
  number, number, number, number,
  number, number, number, number,
];

export type PrimaryKey = Uint8Array | PrimaryKeyTuple;

/**
 * Opaque encoded key bytes.
 *
 * Ordering is bytewise lexicographic and is already made semantic by the Rust
 * key encoder. Adapters must not reinterpret these bytes.
 */
export type EngineKey = Uint8Array;

export interface PrimaryKeyRangeRequest {
  start?: PrimaryKey;
  startInclusive: boolean;
  end?: PrimaryKey;
  endInclusive: boolean;
}

export interface IndexRangeRequest {
  /** Lower bound in EngineKey byte order. */
  start?: EngineKey;
  startInclusive: boolean;
  /** Upper bound in EngineKey byte order. */
  end?: EngineKey;
  endInclusive: boolean;
}

export type RowBytes = Uint8Array;

export type PrimaryKeyEntry = {
  primaryKey: PrimaryKey;
  row: RowBytes;
};

export type IndexEntry = {
  indexKey: EngineKey;
  rowPrimaryKey: PrimaryKey;
};

export type TableSchemaEntry = {
  table: string;
  row: RowBytes;
};

export type IndexSchemaEntry = {
  index: string;
  row: RowBytes;
};

export interface DatabaseTransaction {
  getRow(table: string, primaryKey: PrimaryKey): Promise<RowBytes | null | undefined> | RowBytes | null | undefined;
  putRow(table: string, primaryKey: PrimaryKey, row: RowBytes): Promise<void> | void;
  deleteRow(table: string, primaryKey: PrimaryKey): Promise<RowBytes | null | undefined> | RowBytes | null | undefined;
  rangeRows(table: string, range: PrimaryKeyRangeRequest): Promise<PrimaryKeyEntry[]> | PrimaryKeyEntry[];
  addIndex(index: string, indexKey: EngineKey, rowPrimaryKey: PrimaryKey): Promise<void> | void;
  removeIndex(index: string, indexKey: EngineKey, rowPrimaryKey: PrimaryKey): Promise<void> | void;
  /**
   * Returns index entries sorted by ascending EngineKey byte order.
   * Apply start/end bounds using bytewise lexicographic comparison.
   */
  rangeIndex(index: string, range: IndexRangeRequest): Promise<IndexEntry[]> | IndexEntry[];
  getTableSchema(table: string): Promise<RowBytes | null | undefined> | RowBytes | null | undefined;
  putTableSchema(table: string, row: RowBytes): Promise<void> | void;
  deleteTableSchema(table: string): Promise<RowBytes | null | undefined> | RowBytes | null | undefined;
  rangeTableSchemas(): Promise<TableSchemaEntry[]> | TableSchemaEntry[];
  getIndexSchema(index: string): Promise<RowBytes | null | undefined> | RowBytes | null | undefined;
  putIndexSchema(index: string, row: RowBytes): Promise<void> | void;
  deleteIndexSchema(index: string): Promise<RowBytes | null | undefined> | RowBytes | null | undefined;
  rangeIndexSchemas(): Promise<IndexSchemaEntry[]> | IndexSchemaEntry[];
  commit(): Promise<void> | void;
  rollback(): Promise<void> | void;
}

export interface DatabaseEngineOptions {
  beginTransaction(mode: "readonly" | "readwrite"): Promise<DatabaseTransaction> | DatabaseTransaction;
}
"#;

#[wasm_bindgen]
extern "C" {
  #[wasm_bindgen(typescript_type = "DatabaseEngineOptions")]
  pub type DatabaseEngineOptions;
}

#[derive(Debug)]
struct StoreAdapterError(String);

impl fmt::Display for StoreAdapterError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}", self.0)
  }
}

impl std::error::Error for StoreAdapterError {}

fn js_error(value: JsValue) -> BTreeError {
  let message = value
    .as_string()
    .or_else(|| {
      value
        .dyn_ref::<js_sys::Error>()
        .map(|error| String::from(error.message()))
    })
    .or_else(|| {
      Reflect::get(&value, &JsValue::from_str("message"))
        .ok()
        .and_then(|message| message.as_string())
    })
    .or_else(|| {
      JSON::stringify(&value)
        .ok()
        .and_then(|serialized| serialized.as_string())
    })
    .unwrap_or_else(|| "store adapter callback error".to_string());
  BTreeError::other(StoreAdapterError(message))
}

fn serde_error(message: impl fmt::Display) -> BTreeError {
  BTreeError::other(StoreAdapterError(message.to_string()))
}

fn to_js<T: Serialize + ?Sized>(value: &T) -> BTreeResult<JsValue> {
  value
    .serialize(&serde_wasm_bindgen::Serializer::new().serialize_bytes_as_arrays(false))
    .map_err(serde_error)
}

fn from_js<T: DeserializeOwned>(value: JsValue) -> BTreeResult<T> {
  serde_wasm_bindgen::from_value(value).map_err(serde_error)
}

async fn resolve_js(value: JsValue) -> BTreeResult<JsValue> {
  if value.is_instance_of::<Promise>() {
    let promise: Promise = value.unchecked_into();
    JsFuture::from(promise).await.map_err(js_error)
  } else {
    Ok(value)
  }
}

async fn call_method0(function: Function, this: JsValue) -> BTreeResult<JsValue> {
  let value = function.call0(&this).map_err(js_error)?;
  resolve_js(value).await
}

async fn call_method1(function: Function, this: JsValue, arg0: JsValue) -> BTreeResult<JsValue> {
  let value = function.call1(&this, &arg0).map_err(js_error)?;
  resolve_js(value).await
}

async fn call_method2(
  function: Function,
  this: JsValue,
  arg0: JsValue,
  arg1: JsValue,
) -> BTreeResult<JsValue> {
  let value = function.call2(&this, &arg0, &arg1).map_err(js_error)?;
  resolve_js(value).await
}

async fn call_method3(
  function: Function,
  this: JsValue,
  arg0: JsValue,
  arg1: JsValue,
  arg2: JsValue,
) -> BTreeResult<JsValue> {
  let value = function
    .call3(&this, &arg0, &arg1, &arg2)
    .map_err(js_error)?;
  resolve_js(value).await
}

fn load_required_function(adapter: &JsValue, name: &str) -> Result<Function, String> {
  let key = JsValue::from_str(name);
  let value =
    Reflect::get(adapter, &key).map_err(|_| format!("invalid adapter property: {name}"))?;
  if value.is_null() || value.is_undefined() {
    return Err(format!("missing required adapter function: {name}"));
  }
  value
    .dyn_into::<Function>()
    .map_err(|_| format!("adapter property is not a function: {name}"))
}

#[derive(Clone)]
struct CallbackRegistry {
  adapter: JsValue,
  begin_transaction: Function,
}

#[derive(Clone)]
struct BackendTransactionHandles {
  value: JsValue,
  get_row: Function,
  put_row: Function,
  delete_row: Function,
  range_rows: Function,
  add_index: Function,
  remove_index: Function,
  range_index: Function,
  get_table_schema: Function,
  put_table_schema: Function,
  delete_table_schema: Function,
  range_table_schemas: Function,
  get_index_schema: Function,
  put_index_schema: Function,
  delete_index_schema: Function,
  range_index_schemas: Function,
  commit: Function,
  rollback: Function,
}

struct BackendTransaction {
  handles: Rc<RefCell<BackendTransactionHandles>>,
}

impl BackendTransaction {
  async fn commit(self) -> BTreeResult<()> {
    let (commit, value) = {
      let handles = self.handles.borrow();
      (handles.commit.clone(), handles.value.clone())
    };
    call_method0(commit, value).await.map(|_| ())
  }

  async fn rollback(self) -> BTreeResult<()> {
    let (rollback, value) = {
      let handles = self.handles.borrow();
      (handles.rollback.clone(), handles.value.clone())
    };
    call_method0(rollback, value).await.map(|_| ())
  }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PrimaryKeyRangeRequest {
  #[serde(deserialize_with = "deserialize_optional_primary_key")]
  start: Option<PrimaryKey>,
  start_inclusive: bool,
  #[serde(deserialize_with = "deserialize_optional_primary_key")]
  end: Option<PrimaryKey>,
  end_inclusive: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IndexRangeRequest {
  start: Option<EngineKey>,
  start_inclusive: bool,
  end: Option<EngineKey>,
  end_inclusive: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PrimaryKeyEntry {
  #[serde(deserialize_with = "deserialize_primary_key")]
  primary_key: PrimaryKey,
  row: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IndexEntry {
  index_key: EngineKey,
  #[serde(deserialize_with = "deserialize_primary_key")]
  row_primary_key: PrimaryKey,
}

fn pk_from_bytes<E: serde::de::Error>(bytes: &[u8]) -> Result<PrimaryKey, E> {
  let array: [u8; 16] = bytes
    .try_into()
    .map_err(|_| E::custom("primary key must be exactly 16 bytes"))?;
  Ok(PrimaryKey::new(array))
}

fn deserialize_primary_key<'de, D>(deserializer: D) -> Result<PrimaryKey, D::Error>
where
  D: serde::Deserializer<'de>,
{
  struct PrimaryKeyVisitor;

  impl<'de> Visitor<'de> for PrimaryKeyVisitor {
    type Value = PrimaryKey;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
      formatter.write_str("a 16-byte primary key as Uint8Array or 16-number array")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
      E: serde::de::Error,
    {
      pk_from_bytes(v)
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
      E: serde::de::Error,
    {
      pk_from_bytes(&v)
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
      A: SeqAccess<'de>,
    {
      let mut out = [0u8; 16];
      for (index, slot) in out.iter_mut().enumerate() {
        *slot = seq.next_element::<u8>()?.ok_or_else(|| {
          serde::de::Error::custom(format!("primary key missing byte at index {index}"))
        })?;
      }
      if seq.next_element::<u8>()?.is_some() {
        return Err(serde::de::Error::custom(
          "primary key must be exactly 16 bytes",
        ));
      }
      Ok(PrimaryKey::new(out))
    }
  }

  deserializer.deserialize_any(PrimaryKeyVisitor)
}

fn deserialize_optional_primary_key<'de, D>(deserializer: D) -> Result<Option<PrimaryKey>, D::Error>
where
  D: serde::Deserializer<'de>,
{
  struct OptionalPrimaryKeyVisitor;

  impl<'de> Visitor<'de> for OptionalPrimaryKeyVisitor {
    type Value = Option<PrimaryKey>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
      formatter.write_str("an optional primary key")
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
      E: serde::de::Error,
    {
      Ok(None)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
      E: serde::de::Error,
    {
      Ok(None)
    }

    fn visit_some<D2>(self, deserializer: D2) -> Result<Self::Value, D2::Error>
    where
      D2: serde::Deserializer<'de>,
    {
      deserialize_primary_key(deserializer).map(Some)
    }
  }

  deserializer.deserialize_option(OptionalPrimaryKeyVisitor)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TableSchemaEntry {
  table: String,
  row: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IndexSchemaEntry {
  index: String,
  row: Vec<u8>,
}

#[derive(Clone)]
pub struct StoreAdapterCallbacks {
  callbacks: CallbackRegistry,
  index_key_widths: Arc<Mutex<HashMap<String, usize>>>,
}

impl TryFrom<JsValue> for StoreAdapterCallbacks {
  type Error = String;

  fn try_from(value: JsValue) -> Result<Self, Self::Error> {
    let callbacks = CallbackRegistry {
      begin_transaction: load_required_function(&value, "beginTransaction")?,
      adapter: value,
    };

    Ok(Self {
      callbacks,
      index_key_widths: Arc::new(Mutex::new(HashMap::new())),
    })
  }
}

#[derive(Clone)]
pub struct StoreAdapterTree {
  adapter: StoreAdapterCallbacks,
  tree: String,
}

pub struct StoreAdapterTransaction {
  adapter: StoreAdapterCallbacks,
  backend_tx: BackendTransaction,
}

const TABLE_SCHEMAS_TREE: &str = "sys:table_schemas";
const INDEX_SCHEMAS_TREE: &str = "sys:index_schemas";

enum StoreTree<'a> {
  TableSchemas,
  IndexSchemas,
  RowTable(&'a str),
  IndexTable(&'a str),
}

fn key_in_range<R>(key: &EngineKey, range: &R) -> bool
where
  R: RangeBounds<EngineKey>,
{
  let start_ok = match range.start_bound() {
    Bound::Included(start) => key >= start,
    Bound::Excluded(start) => key > start,
    Bound::Unbounded => true,
  };

  let end_ok = match range.end_bound() {
    Bound::Included(end) => key <= end,
    Bound::Excluded(end) => key < end,
    Bound::Unbounded => true,
  };

  start_ok && end_ok
}

impl StoreAdapterCallbacks {
  fn row_table_name<'a>(&self, tree: &'a str) -> Option<&'a str> {
    if self.index_name(tree).is_some() {
      None
    } else {
      Some(tree.strip_prefix("t:").unwrap_or(tree))
    }
  }

  fn index_name<'a>(&self, tree: &'a str) -> Option<&'a str> {
    tree.strip_prefix("i:")
  }

  fn tree_kind<'a>(&'a self, tree: &'a str) -> StoreTree<'a> {
    if tree == TABLE_SCHEMAS_TREE {
      StoreTree::TableSchemas
    } else if tree == INDEX_SCHEMAS_TREE {
      StoreTree::IndexSchemas
    } else if let Some(index_name) = self.index_name(tree) {
      StoreTree::IndexTable(index_name)
    } else {
      StoreTree::RowTable(tree.strip_prefix("t:").unwrap_or(tree))
    }
  }

  fn table_schema_name_from_engine_key(&self, key: &EngineKey) -> BTreeResult<String> {
    let values = <DefaultEncoding as KeyEncoding>::decode_values(key)
      .map_err(|e| serde_error(format!("decode key error: {e}")))?;
    match values.as_slice() {
      [db_engine::EngineValue::Text(name)] => Ok(name.clone()),
      _ => Err(serde_error("table schema key must be text scalar")),
    }
  }

  fn index_schema_name_from_engine_key(&self, key: &EngineKey) -> BTreeResult<String> {
    let values = <DefaultEncoding as KeyEncoding>::decode_values(key)
      .map_err(|e| serde_error(format!("decode key error: {e}")))?;
    match values.as_slice() {
      [db_engine::EngineValue::Text(name)] => Ok(name.clone()),
      _ => Err(serde_error("index schema key must be text scalar")),
    }
  }

  fn primary_key_from_engine_key(&self, key: &EngineKey) -> BTreeResult<PrimaryKey> {
    let values = <DefaultEncoding as KeyEncoding>::decode_values(key)
      .map_err(|e| serde_error(format!("decode key error: {e}")))?;
    match values.as_slice() {
      [db_engine::EngineValue::Uuid(bytes)] => Ok(PrimaryKey::new(*bytes)),
      _ => Err(serde_error("row primary key must be UUID scalar")),
    }
  }

  fn split_composite_index_key(
    &self,
    index_name: &str,
    composite: &EngineKey,
  ) -> BTreeResult<(EngineKey, PrimaryKey)> {
    let width = self
      .index_key_widths
      .lock()
      .map_err(|_| serde_error("index schema cache lock poisoned"))?
      .get(index_name)
      .copied()
      .ok_or_else(|| serde_error(format!("missing index schema width for {index_name}")))?;

    let values = <DefaultEncoding as KeyEncoding>::decode_values(composite)
      .map_err(|e| serde_error(format!("decode composite key error: {e}")))?;
    if values.len() < width + 1 {
      return Err(serde_error("malformed composite index key"));
    }

    let index_key = <DefaultEncoding as KeyEncoding>::encode_values(&values[..width]);
    let row_pk_key = <DefaultEncoding as KeyEncoding>::encode_values(&values[width..]);
    let row_pk = self.primary_key_from_engine_key(&row_pk_key)?;
    Ok((index_key, row_pk))
  }

  fn compose_composite_index_key(&self, index_key: &EngineKey, row_pk: PrimaryKey) -> EngineKey {
    let mut values = <DefaultEncoding as KeyEncoding>::decode_values(index_key).unwrap_or_default();
    values.push(db_engine::EngineValue::Uuid(*row_pk.as_bytes()));
    <DefaultEncoding as KeyEncoding>::encode_values(&values)
  }

  fn maybe_update_index_schema_widths(&self, tree: &str, key: &EngineKey, row: &[u8]) {
    if tree != "sys:index_schemas" {
      return;
    }
    let decoded_key = <DefaultEncoding as KeyEncoding>::decode_values(key);
    let decoded_row = <DefaultEncoding as RowEncoding>::decode_values(row);
    if let (Ok([db_engine::EngineValue::Text(index_name)]), Ok(row_values)) =
      (decoded_key.as_deref(), decoded_row.as_deref())
      && let Ok(schema) = decode_index_schema_row(row_values)
      && let Ok(mut guard) = self.index_key_widths.lock()
    {
      guard.insert(index_name.clone(), schema.column_indices.len());
    }
  }

  fn maybe_remove_index_schema_width(&self, tree: &str, key: &EngineKey) {
    if tree != "sys:index_schemas" {
      return;
    }
    if let Ok([db_engine::EngineValue::Text(index_name)]) =
      <DefaultEncoding as KeyEncoding>::decode_values(key).as_deref()
      && let Ok(mut guard) = self.index_key_widths.lock()
    {
      let _ = guard.remove(index_name);
    }
  }

  fn load_backend_transaction_handles(
    &self,
    value: JsValue,
  ) -> BTreeResult<BackendTransactionHandles> {
    macro_rules! load_fn {
      ($name:expr) => {
        load_required_function(&value, $name).map_err(serde_error)?
      };
    }

    Ok(BackendTransactionHandles {
      value,
      get_row: load_fn!("getRow"),
      put_row: load_fn!("putRow"),
      delete_row: load_fn!("deleteRow"),
      range_rows: load_fn!("rangeRows"),
      add_index: load_fn!("addIndex"),
      remove_index: load_fn!("removeIndex"),
      range_index: load_fn!("rangeIndex"),
      get_table_schema: load_fn!("getTableSchema"),
      put_table_schema: load_fn!("putTableSchema"),
      delete_table_schema: load_fn!("deleteTableSchema"),
      range_table_schemas: load_fn!("rangeTableSchemas"),
      get_index_schema: load_fn!("getIndexSchema"),
      put_index_schema: load_fn!("putIndexSchema"),
      delete_index_schema: load_fn!("deleteIndexSchema"),
      range_index_schemas: load_fn!("rangeIndexSchemas"),
      commit: load_fn!("commit"),
      rollback: load_fn!("rollback"),
    })
  }

  async fn tx_get_table_schema(
    &self,
    tx: &BackendTransaction,
    key: &EngineKey,
  ) -> BTreeResult<Option<Vec<u8>>> {
    let table = self.table_schema_name_from_engine_key(key)?;
    let table_js = JsValue::from_str(&table);
    let (get_table_schema, tx_value) = {
      let handles = tx.handles.borrow();
      (handles.get_table_schema.clone(), handles.value.clone())
    };
    let value = call_method1(get_table_schema, tx_value, table_js).await?;
    if value.is_null() || value.is_undefined() {
      return Ok(None);
    }
    from_js(value)
  }

  async fn tx_get_index_schema(
    &self,
    tx: &BackendTransaction,
    key: &EngineKey,
  ) -> BTreeResult<Option<Vec<u8>>> {
    let index = self.index_schema_name_from_engine_key(key)?;
    let index_js = JsValue::from_str(&index);
    let (get_index_schema, tx_value) = {
      let handles = tx.handles.borrow();
      (handles.get_index_schema.clone(), handles.value.clone())
    };
    let value = call_method1(get_index_schema, tx_value, index_js).await?;
    if value.is_null() || value.is_undefined() {
      return Ok(None);
    }
    from_js(value)
  }

  async fn tx_get_row(
    &self,
    tx: &BackendTransaction,
    table: &str,
    key: &EngineKey,
  ) -> BTreeResult<Option<Vec<u8>>> {
    let table_js = JsValue::from_str(table);
    let pk = self.primary_key_from_engine_key(key)?;
    let pk_js = to_js(&pk)?;
    let (get_row, tx_value) = {
      let handles = tx.handles.borrow();
      (handles.get_row.clone(), handles.value.clone())
    };
    let value = call_method2(get_row, tx_value, table_js, pk_js).await?;
    if value.is_null() || value.is_undefined() {
      return Ok(None);
    }
    from_js(value)
  }

  async fn tx_get_index_entry(
    &self,
    tx: &BackendTransaction,
    index_name: &str,
    key: &EngineKey,
  ) -> BTreeResult<Option<Vec<u8>>> {
    let (index_key, row_pk) = self.split_composite_index_key(index_name, key)?;
    let request = IndexRangeRequest {
      start: Some(index_key.clone()),
      start_inclusive: true,
      end: Some(index_key),
      end_inclusive: true,
    };
    let index_js = JsValue::from_str(index_name);
    let req_js = to_js(&request)?;
    let (range_index, tx_value) = {
      let handles = tx.handles.borrow();
      (handles.range_index.clone(), handles.value.clone())
    };
    let value = call_method2(range_index, tx_value, index_js, req_js).await?;
    let entries: Vec<IndexEntry> = from_js(value)?;
    let found = entries
      .into_iter()
      .any(|entry| entry.row_primary_key == row_pk);
    Ok(found.then(Vec::new))
  }

  async fn tx_insert_table_schema(
    &self,
    tx: &BackendTransaction,
    key: &EngineKey,
    row: &[u8],
  ) -> BTreeResult<()> {
    let table = self.table_schema_name_from_engine_key(key)?;
    let table_js = JsValue::from_str(&table);
    let row_js = to_js(row)?;
    let (put_table_schema, tx_value) = {
      let handles = tx.handles.borrow();
      (handles.put_table_schema.clone(), handles.value.clone())
    };
    let _ = call_method2(put_table_schema, tx_value, table_js, row_js).await?;
    Ok(())
  }

  async fn tx_insert_index_schema(
    &self,
    tx: &BackendTransaction,
    key: &EngineKey,
    row: &[u8],
  ) -> BTreeResult<()> {
    let index = self.index_schema_name_from_engine_key(key)?;
    let index_js = JsValue::from_str(&index);
    let row_js = to_js(row)?;
    let (put_index_schema, tx_value) = {
      let handles = tx.handles.borrow();
      (handles.put_index_schema.clone(), handles.value.clone())
    };
    let _ = call_method2(put_index_schema, tx_value, index_js, row_js).await?;
    self.maybe_update_index_schema_widths(INDEX_SCHEMAS_TREE, key, row);
    Ok(())
  }

  async fn tx_insert_row(
    &self,
    tx: &BackendTransaction,
    table: &str,
    key: &EngineKey,
    row: &[u8],
  ) -> BTreeResult<()> {
    let table_js = JsValue::from_str(table);
    let pk = self.primary_key_from_engine_key(key)?;
    let pk_js = to_js(&pk)?;
    let row_js = to_js(row)?;
    let (put_row, tx_value) = {
      let handles = tx.handles.borrow();
      (handles.put_row.clone(), handles.value.clone())
    };
    let _ = call_method3(put_row, tx_value, table_js, pk_js, row_js).await?;
    Ok(())
  }

  async fn tx_insert_index_entry(
    &self,
    tx: &BackendTransaction,
    index_name: &str,
    key: &EngineKey,
  ) -> BTreeResult<()> {
    let (index_key, row_pk) = self.split_composite_index_key(index_name, key)?;
    let index_js = JsValue::from_str(index_name);
    let index_key_js = to_js(&index_key)?;
    let row_pk_js = to_js(&row_pk)?;
    let (add_index, tx_value) = {
      let handles = tx.handles.borrow();
      (handles.add_index.clone(), handles.value.clone())
    };
    let _ = call_method3(add_index, tx_value, index_js, index_key_js, row_pk_js).await?;
    Ok(())
  }

  async fn tx_remove_table_schema(
    &self,
    tx: &BackendTransaction,
    key: &EngineKey,
  ) -> BTreeResult<Option<Vec<u8>>> {
    let table = self.table_schema_name_from_engine_key(key)?;
    let table_js = JsValue::from_str(&table);
    let (delete_table_schema, tx_value) = {
      let handles = tx.handles.borrow();
      (handles.delete_table_schema.clone(), handles.value.clone())
    };
    let value = call_method1(delete_table_schema, tx_value, table_js).await?;
    if value.is_null() || value.is_undefined() {
      return Ok(None);
    }
    from_js(value)
  }

  async fn tx_remove_index_schema(
    &self,
    tx: &BackendTransaction,
    key: &EngineKey,
  ) -> BTreeResult<Option<Vec<u8>>> {
    let index = self.index_schema_name_from_engine_key(key)?;
    let index_js = JsValue::from_str(&index);
    let (delete_index_schema, tx_value) = {
      let handles = tx.handles.borrow();
      (handles.delete_index_schema.clone(), handles.value.clone())
    };
    let value = call_method1(delete_index_schema, tx_value, index_js).await?;
    self.maybe_remove_index_schema_width(INDEX_SCHEMAS_TREE, key);
    if value.is_null() || value.is_undefined() {
      return Ok(None);
    }
    from_js(value)
  }

  async fn tx_remove_row(
    &self,
    tx: &BackendTransaction,
    table: &str,
    key: &EngineKey,
  ) -> BTreeResult<Option<Vec<u8>>> {
    let table_js = JsValue::from_str(table);
    let pk = self.primary_key_from_engine_key(key)?;
    let pk_js = to_js(&pk)?;
    let (delete_row, tx_value) = {
      let handles = tx.handles.borrow();
      (handles.delete_row.clone(), handles.value.clone())
    };
    let value = call_method2(delete_row, tx_value, table_js, pk_js).await?;
    if value.is_null() || value.is_undefined() {
      return Ok(None);
    }
    from_js(value)
  }

  async fn tx_remove_index_entry(
    &self,
    tx: &BackendTransaction,
    index_name: &str,
    key: &EngineKey,
  ) -> BTreeResult<Option<Vec<u8>>> {
    let (index_key, row_pk) = self.split_composite_index_key(index_name, key)?;
    let index_js = JsValue::from_str(index_name);
    let index_key_js = to_js(&index_key)?;
    let row_pk_js = to_js(&row_pk)?;
    let (remove_index, tx_value) = {
      let handles = tx.handles.borrow();
      (handles.remove_index.clone(), handles.value.clone())
    };
    let _ = call_method3(remove_index, tx_value, index_js, index_key_js, row_pk_js).await?;
    Ok(None)
  }

  async fn tx_range_table_schemas(
    &self,
    tx: &BackendTransaction,
  ) -> BTreeResult<Vec<(EngineKey, Vec<u8>)>> {
    let (range_table_schemas, tx_value) = {
      let handles = tx.handles.borrow();
      (handles.range_table_schemas.clone(), handles.value.clone())
    };
    let value = call_method0(range_table_schemas, tx_value).await?;
    let entries: Vec<TableSchemaEntry> = from_js(value)?;
    Ok(
      entries
        .into_iter()
        .map(|entry| {
          (
            <DefaultEncoding as KeyEncoding>::encode_values(&[db_engine::EngineValue::Text(
              entry.table,
            )]),
            entry.row,
          )
        })
        .collect(),
    )
  }

  async fn tx_range_index_schemas(
    &self,
    tx: &BackendTransaction,
  ) -> BTreeResult<Vec<(EngineKey, Vec<u8>)>> {
    let (range_index_schemas, tx_value) = {
      let handles = tx.handles.borrow();
      (handles.range_index_schemas.clone(), handles.value.clone())
    };
    let value = call_method0(range_index_schemas, tx_value).await?;
    let entries: Vec<IndexSchemaEntry> = from_js(value)?;
    let out = entries
      .into_iter()
      .map(|entry| {
        (
          <DefaultEncoding as KeyEncoding>::encode_values(&[db_engine::EngineValue::Text(
            entry.index,
          )]),
          entry.row,
        )
      })
      .collect::<Vec<_>>();
    for (key, row) in &out {
      self.maybe_update_index_schema_widths(INDEX_SCHEMAS_TREE, key, row);
    }
    Ok(out)
  }

  async fn tx_range_rows<R>(
    &self,
    tx: &BackendTransaction,
    table: &str,
    range: &R,
  ) -> BTreeResult<Vec<(EngineKey, Vec<u8>)>>
  where
    R: RangeBounds<EngineKey>,
  {
    let request = PrimaryKeyRangeRequest {
      start: match range.start_bound() {
        Bound::Included(key) | Bound::Excluded(key) => {
          let pk = self.primary_key_from_engine_key(key)?;
          Some(pk)
        }
        Bound::Unbounded => None,
      },
      start_inclusive: matches!(range.start_bound(), Bound::Included(_)),
      end: match range.end_bound() {
        Bound::Included(key) | Bound::Excluded(key) => {
          let pk = self.primary_key_from_engine_key(key)?;
          Some(pk)
        }
        Bound::Unbounded => None,
      },
      end_inclusive: matches!(range.end_bound(), Bound::Included(_)),
    };

    let table_js = JsValue::from_str(table);
    let req_js = to_js(&request)?;
    let (range_rows, tx_value) = {
      let handles = tx.handles.borrow();
      (handles.range_rows.clone(), handles.value.clone())
    };
    let value = call_method2(range_rows, tx_value, table_js, req_js).await?;
    let rows: Vec<PrimaryKeyEntry> = from_js(value)?;
    Ok(
      rows
        .into_iter()
        .map(|entry| (entry.primary_key.to_engine_key(), entry.row))
        .collect(),
    )
  }

  async fn tx_range_index_entries<R>(
    &self,
    tx: &BackendTransaction,
    index_name: &str,
    range: &R,
  ) -> BTreeResult<Vec<(EngineKey, Vec<u8>)>>
  where
    R: RangeBounds<EngineKey>,
  {
    let request = IndexRangeRequest {
      start: match range.start_bound() {
        Bound::Included(key) | Bound::Excluded(key) => self
          .split_composite_index_key(index_name, key)
          .ok()
          .map(|(index_key, _)| index_key),
        Bound::Unbounded => None,
      },
      start_inclusive: matches!(range.start_bound(), Bound::Included(_)),
      end: match range.end_bound() {
        Bound::Included(key) | Bound::Excluded(key) => self
          .split_composite_index_key(index_name, key)
          .ok()
          .map(|(index_key, _)| index_key),
        Bound::Unbounded => None,
      },
      end_inclusive: matches!(range.end_bound(), Bound::Included(_)),
    };

    let index_js = JsValue::from_str(index_name);
    let req_js = to_js(&request)?;
    let (range_index, tx_value) = {
      let handles = tx.handles.borrow();
      (handles.range_index.clone(), handles.value.clone())
    };
    let value = call_method2(range_index, tx_value, index_js, req_js).await?;
    let entries: Vec<IndexEntry> = from_js(value)?;
    Ok(
      entries
        .into_iter()
        .map(|entry| {
          (
            self.compose_composite_index_key(&entry.index_key, entry.row_primary_key),
            Vec::new(),
          )
        })
        .collect(),
    )
  }

  async fn begin_backend_transaction(
    &self,
    commit_on_success: bool,
  ) -> BTreeResult<BackendTransaction> {
    let mode = if commit_on_success {
      "readwrite"
    } else {
      "readonly"
    };
    let value = call_method1(
      self.callbacks.begin_transaction.clone(),
      self.callbacks.adapter.clone(),
      JsValue::from_str(mode),
    )
    .await?;

    if value.is_null() || value.is_undefined() {
      return Err(serde_error("beginTransaction returned null or undefined"));
    }

    let handles = self.load_backend_transaction_handles(value)?;

    Ok(BackendTransaction {
      handles: Rc::new(RefCell::new(handles)),
    })
  }

  async fn tx_get(
    &self,
    tx: &BackendTransaction,
    tree: &str,
    key: &EngineKey,
  ) -> BTreeResult<Option<Vec<u8>>> {
    match self.tree_kind(tree) {
      StoreTree::TableSchemas => self.tx_get_table_schema(tx, key).await,
      StoreTree::IndexSchemas => self.tx_get_index_schema(tx, key).await,
      StoreTree::RowTable(table) => self.tx_get_row(tx, table, key).await,
      StoreTree::IndexTable(index_name) => self.tx_get_index_entry(tx, index_name, key).await,
    }
  }

  async fn tx_insert(
    &self,
    tx: &BackendTransaction,
    tree: &str,
    key: &EngineKey,
    row: &[u8],
  ) -> BTreeResult<()> {
    match self.tree_kind(tree) {
      StoreTree::TableSchemas => self.tx_insert_table_schema(tx, key, row).await,
      StoreTree::IndexSchemas => self.tx_insert_index_schema(tx, key, row).await,
      StoreTree::RowTable(table) => self.tx_insert_row(tx, table, key, row).await,
      StoreTree::IndexTable(index_name) => self.tx_insert_index_entry(tx, index_name, key).await,
    }
  }

  async fn tx_remove(
    &self,
    tx: &BackendTransaction,
    tree: &str,
    key: &EngineKey,
  ) -> BTreeResult<Option<Vec<u8>>> {
    match self.tree_kind(tree) {
      StoreTree::TableSchemas => self.tx_remove_table_schema(tx, key).await,
      StoreTree::IndexSchemas => self.tx_remove_index_schema(tx, key).await,
      StoreTree::RowTable(table) => self.tx_remove_row(tx, table, key).await,
      StoreTree::IndexTable(index_name) => self.tx_remove_index_entry(tx, index_name, key).await,
    }
  }

  async fn tx_range<R>(
    &self,
    tx: &BackendTransaction,
    tree: &str,
    range: &R,
  ) -> BTreeResult<Vec<(EngineKey, Vec<u8>)>>
  where
    R: RangeBounds<EngineKey>,
  {
    match self.tree_kind(tree) {
      StoreTree::TableSchemas => self.tx_range_table_schemas(tx).await,
      StoreTree::IndexSchemas => self.tx_range_index_schemas(tx).await,
      StoreTree::RowTable(table) => self.tx_range_rows(tx, table, range).await,
      StoreTree::IndexTable(index_name) => self.tx_range_index_entries(tx, index_name, range).await,
    }
  }

  async fn callback_get(&self, tree: &str, key: &EngineKey) -> BTreeResult<Option<Vec<u8>>> {
    let tx = self.begin_backend_transaction(false).await?;
    match self.tx_get(&tx, tree, key).await {
      Ok(value) => {
        tx.rollback().await?;
        Ok(value)
      }
      Err(err) => {
        let _ = tx.rollback().await;
        Err(err)
      }
    }
  }

  async fn callback_insert(&self, tree: &str, key: &EngineKey, row: &[u8]) -> BTreeResult<()> {
    let tx = self.begin_backend_transaction(true).await?;
    match self.tx_insert(&tx, tree, key, row).await {
      Ok(()) => tx.commit().await,
      Err(err) => {
        let _ = tx.rollback().await;
        Err(err)
      }
    }
  }

  async fn callback_remove(&self, tree: &str, key: &EngineKey) -> BTreeResult<Option<Vec<u8>>> {
    let tx = self.begin_backend_transaction(true).await?;
    match self.tx_remove(&tx, tree, key).await {
      Ok(value) => {
        tx.commit().await?;
        Ok(value)
      }
      Err(err) => {
        let _ = tx.rollback().await;
        Err(err)
      }
    }
  }

  async fn callback_range<R>(&self, tree: &str, range: &R) -> BTreeResult<Vec<(EngineKey, Vec<u8>)>>
  where
    R: RangeBounds<EngineKey>,
  {
    let tx = self.begin_backend_transaction(false).await?;
    match self.tx_range(&tx, tree, range).await {
      Ok(value) => {
        tx.rollback().await?;
        Ok(value)
      }
      Err(err) => {
        let _ = tx.rollback().await;
        Err(err)
      }
    }
  }
}

impl NamedTreeProvider<EngineKey, Vec<u8>> for StoreAdapterCallbacks {
  type Tree = StoreAdapterTree;
  type Transaction = StoreAdapterTransaction;

  fn get_tree(
    &self,
    name: &str,
  ) -> impl core::future::Future<Output = BTreeResult<Self::Tree>> + '_ {
    let tree = StoreAdapterTree {
      adapter: self.clone(),
      tree: name.to_string(),
    };
    async move { Ok(tree) }
  }

  async fn begin_transaction(&self) -> BTreeResult<Self::Transaction> {
    let backend_tx = self.begin_backend_transaction(true).await?;
    Ok(StoreAdapterTransaction {
      adapter: self.clone(),
      backend_tx,
    })
  }
}

impl NamedTreeTransaction<EngineKey, Vec<u8>> for StoreAdapterTransaction {
  fn get<'a>(
    &'a mut self,
    tree: &'a str,
    key: &'a EngineKey,
  ) -> impl core::future::Future<Output = BTreeResult<Option<Vec<u8>>>> + 'a
  where
    EngineKey: Ord,
  {
    async move { self.adapter.tx_get(&self.backend_tx, tree, key).await }
  }

  fn insert<'a>(
    &'a mut self,
    tree: &'a str,
    key: EngineKey,
    value: Vec<u8>,
  ) -> impl core::future::Future<Output = BTreeResult<()>> + 'a
  where
    EngineKey: Ord,
  {
    async move {
      self
        .adapter
        .tx_insert(&self.backend_tx, tree, &key, &value)
        .await
    }
  }

  fn remove<'a>(
    &'a mut self,
    tree: &'a str,
    key: &'a EngineKey,
  ) -> impl core::future::Future<Output = BTreeResult<Option<Vec<u8>>>> + 'a
  where
    EngineKey: Ord,
  {
    async move { self.adapter.tx_remove(&self.backend_tx, tree, key).await }
  }

  fn range<'a, R>(
    &'a self,
    tree: &'a str,
    range: R,
  ) -> impl Stream<Item = BTreeResult<(EngineKey, Vec<u8>)>> + 'a
  where
    EngineKey: Ord,
    R: RangeBounds<EngineKey> + MaybeSend + 'a,
  {
    stream! {
      match self.adapter.tx_range(&self.backend_tx, tree, &range).await {
        Ok(pairs) => {
          for (key, value) in pairs {
            if key_in_range(&key, &range) {
              yield Ok((key, value));
            }
          }
        }
        Err(err) => yield Err(err),
      }
    }
  }

  fn commit(self) -> impl core::future::Future<Output = BTreeResult<()>>
  where
    Self: Sized,
  {
    async move { self.backend_tx.commit().await }
  }

  fn rollback(self) -> impl core::future::Future<Output = BTreeResult<()>>
  where
    Self: Sized,
  {
    async move { self.backend_tx.rollback().await }
  }
}

impl db_core::BTreeExecutor<EngineKey, Vec<u8>> for StoreAdapterTree {
  fn get<'a, Q>(
    &'a self,
    key: Q,
  ) -> impl core::future::Future<Output = BTreeResult<Option<Vec<u8>>>> + 'a
  where
    EngineKey: Ord,
    Q: core::borrow::Borrow<EngineKey> + MaybeSend + 'a,
  {
    async move { self.adapter.callback_get(&self.tree, key.borrow()).await }
  }

  fn insert<'a>(
    &'a mut self,
    key: EngineKey,
    value: Vec<u8>,
  ) -> impl core::future::Future<Output = BTreeResult<()>> + 'a
  where
    EngineKey: Ord,
  {
    async move { self.adapter.callback_insert(&self.tree, &key, &value).await }
  }

  fn remove<'a, Q>(
    &'a mut self,
    key: Q,
  ) -> impl core::future::Future<Output = BTreeResult<Option<Vec<u8>>>> + 'a
  where
    EngineKey: Ord,
    Q: core::borrow::Borrow<EngineKey> + MaybeSend + 'a,
  {
    async move { self.adapter.callback_remove(&self.tree, key.borrow()).await }
  }

  fn range<'a, R>(&'a self, range: R) -> impl Stream<Item = BTreeResult<(EngineKey, Vec<u8>)>> + 'a
  where
    EngineKey: Ord,
    R: RangeBounds<EngineKey> + MaybeSend + 'a,
  {
    stream! {
      match self.adapter.callback_range(&self.tree, &range).await {
        Ok(pairs) => {
          for (key, value) in pairs {
            if key_in_range(&key, &range) {
              yield Ok((key, value));
            }
          }
        }
        Err(err) => yield Err(err),
      }
    }
  }
}

impl db_core::BTreeExecutor<EngineKey, Vec<u8>> for StoreAdapterTransaction {
  fn get<'a, Q>(
    &'a self,
    _key: Q,
  ) -> impl core::future::Future<Output = BTreeResult<Option<Vec<u8>>>> + 'a
  where
    EngineKey: Ord,
    Q: core::borrow::Borrow<EngineKey> + MaybeSend + 'a,
  {
    async move { Err(BTreeError::UnsupportedOperation) }
  }

  fn insert<'a>(
    &'a mut self,
    _key: EngineKey,
    _value: Vec<u8>,
  ) -> impl core::future::Future<Output = BTreeResult<()>> + 'a
  where
    EngineKey: Ord,
  {
    async move { Err(BTreeError::UnsupportedOperation) }
  }

  fn remove<'a, Q>(
    &'a mut self,
    _key: Q,
  ) -> impl core::future::Future<Output = BTreeResult<Option<Vec<u8>>>> + 'a
  where
    EngineKey: Ord,
    Q: core::borrow::Borrow<EngineKey> + MaybeSend + 'a,
  {
    async move { Err(BTreeError::UnsupportedOperation) }
  }

  fn range<'a, R>(&'a self, _range: R) -> impl Stream<Item = BTreeResult<(EngineKey, Vec<u8>)>> + 'a
  where
    EngineKey: Ord,
    R: RangeBounds<EngineKey> + MaybeSend + 'a,
  {
    futures::stream::iter(Vec::new())
  }
}

impl db_core::BTreeTransaction<EngineKey, Vec<u8>> for StoreAdapterTransaction {
  fn commit(self) -> impl core::future::Future<Output = BTreeResult<()>>
  where
    Self: Sized,
  {
    <Self as NamedTreeTransaction<EngineKey, Vec<u8>>>::commit(self)
  }

  fn rollback(self) -> impl core::future::Future<Output = BTreeResult<()>>
  where
    Self: Sized,
  {
    <Self as NamedTreeTransaction<EngineKey, Vec<u8>>>::rollback(self)
  }
}

impl db_core::BTree<EngineKey, Vec<u8>> for StoreAdapterTree {
  type Transaction = StoreAdapterTransaction;

  async fn transaction(&self) -> BTreeResult<Self::Transaction> {
    let backend_tx = self.adapter.begin_backend_transaction(true).await?;
    Ok(StoreAdapterTransaction {
      adapter: self.adapter.clone(),
      backend_tx,
    })
  }
}
