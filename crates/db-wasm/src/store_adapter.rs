use async_stream::stream;
use core::fmt;
use core::ops::{Bound, RangeBounds};
use db_core::{BTree, BTreeError, BTreeResult, MaybeSend, NamedBTreeMap};
use db_engine::{EngineKey, EngineNamedTreeBackend, EngineNamedTreeTransaction};
use futures::Stream;
use futures::StreamExt;
use js_sys::{Function, JSON, Promise, Reflect};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::rc::Rc;
use std::string::String;
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

export type EngineKey = Uint8Array;

export type RowBytes = Uint8Array;

export interface ByteRangeRequest {
  start?: Uint8Array;
  startInclusive: boolean;
  end?: Uint8Array;
  endInclusive: boolean;
}

export type ByteEntry = {
  key: EngineKey;
  value: RowBytes;
};

export interface DatabaseTransaction {
  get(tree: string, key: Uint8Array): Promise<RowBytes | null | undefined> | RowBytes | null | undefined;
  put(tree: string, key: Uint8Array, value: RowBytes): Promise<void> | void;
  delete(tree: string, key: Uint8Array): Promise<RowBytes | null | undefined> | RowBytes | null | undefined;
  range(tree: string, range: ByteRangeRequest): Promise<ByteEntry[]> | ByteEntry[];
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
  get: Function,
  put: Function,
  delete: Function,
  range: Function,
  commit: Function,
  rollback: Function,
}

impl BackendTransactionHandles {
  fn load_handles<const N: usize>(value: &JsValue, names: [&str; N]) -> BTreeResult<[Function; N]> {
    let mut handles = Vec::with_capacity(N);
    for name in names {
      handles.push(load_required_function(value, name).map_err(serde_error)?);
    }
    handles
      .try_into()
      .map_err(|_| serde_error("internal handle loading error"))
  }

  fn load_tx_handles(value: &JsValue) -> BTreeResult<[Function; 2]> {
    Self::load_handles(value, ["commit", "rollback"])
  }

  fn load_from_js(value: JsValue) -> BTreeResult<Self> {
    let [get, put, delete, range] = Self::load_handles(&value, ["get", "put", "delete", "range"])?;
    let [commit, rollback] = Self::load_tx_handles(&value)?;

    Ok(Self {
      value,
      get,
      put,
      delete,
      range,
      commit,
      rollback,
    })
  }
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
struct ByteRangeRequest {
  start: Option<EngineKey>,
  start_inclusive: bool,
  end: Option<EngineKey>,
  end_inclusive: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ByteEntry {
  key: EngineKey,
  value: Vec<u8>,
}

#[derive(Clone)]
pub struct StoreAdapterCallbacks {
  callbacks: CallbackRegistry,
}

impl TryFrom<JsValue> for StoreAdapterCallbacks {
  type Error = String;

  fn try_from(value: JsValue) -> Result<Self, Self::Error> {
    let callbacks = CallbackRegistry {
      begin_transaction: load_required_function(&value, "beginTransaction")?,
      adapter: value,
    };

    Ok(Self { callbacks })
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

fn key_in_range<R>(key: &EngineKey, range: &R) -> bool
where
  R: RangeBounds<EngineKey>,
{
  start_bound_matches(key, range.start_bound()) && end_bound_matches(key, range.end_bound())
}

fn start_bound_matches(key: &EngineKey, bound: Bound<&EngineKey>) -> bool {
  match bound {
    Bound::Included(start) => key >= start,
    Bound::Excluded(start) => key > start,
    Bound::Unbounded => true,
  }
}

fn end_bound_matches(key: &EngineKey, bound: Bound<&EngineKey>) -> bool {
  match bound {
    Bound::Included(end) => key <= end,
    Bound::Excluded(end) => key < end,
    Bound::Unbounded => true,
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use db_engine::EngineValue;

  #[test]
  fn key_in_range_inclusive_and_exclusive_bounds() {
    let key = <DefaultEncoding as KeyEncoding>::encode_values(&[EngineValue::Text("m".into())]);
    let start = <DefaultEncoding as KeyEncoding>::encode_values(&[EngineValue::Text("a".into())]);
    let end = <DefaultEncoding as KeyEncoding>::encode_values(&[EngineValue::Text("z".into())]);

    assert!(key_in_range(&key, &(start.clone()..=end.clone())));
    assert!(key_in_range(&key, &(start.clone()..)));
    assert!(!key_in_range(&key, &(..end.clone())));
  }
}

impl StoreAdapterCallbacks {
  fn transaction_mode(commit_on_success: bool) -> &'static str {
    if commit_on_success {
      "readwrite"
    } else {
      "readonly"
    }
  }

  async fn begin_backend_transaction(
    &self,
    commit_on_success: bool,
  ) -> BTreeResult<BackendTransaction> {
    let mode = Self::transaction_mode(commit_on_success);
    let value = call_method1(
      self.callbacks.begin_transaction.clone(),
      self.callbacks.adapter.clone(),
      JsValue::from_str(mode),
    )
    .await?;

    if value.is_null() || value.is_undefined() {
      return Err(serde_error("beginTransaction returned null or undefined"));
    }

    let handles = BackendTransactionHandles::load_from_js(value)?;

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
    let (get, tx_value) = {
      let handles = tx.handles.borrow();
      (handles.get.clone(), handles.value.clone())
    };
    let value = call_method2(get, tx_value, JsValue::from_str(tree), to_js(key)?).await?;
    if value.is_null() || value.is_undefined() {
      return Ok(None);
    }
    from_js(value)
  }

  async fn tx_insert(
    &self,
    tx: &BackendTransaction,
    tree: &str,
    key: &EngineKey,
    row: &[u8],
  ) -> BTreeResult<()> {
    let (put, tx_value) = {
      let handles = tx.handles.borrow();
      (handles.put.clone(), handles.value.clone())
    };
    let _ = call_method3(
      put,
      tx_value,
      JsValue::from_str(tree),
      to_js(key)?,
      to_js(row)?,
    )
    .await?;
    Ok(())
  }

  async fn tx_remove(
    &self,
    tx: &BackendTransaction,
    tree: &str,
    key: &EngineKey,
  ) -> BTreeResult<Option<Vec<u8>>> {
    let (delete, tx_value) = {
      let handles = tx.handles.borrow();
      (handles.delete.clone(), handles.value.clone())
    };
    let value = call_method2(delete, tx_value, JsValue::from_str(tree), to_js(key)?).await?;
    if value.is_null() || value.is_undefined() {
      return Ok(None);
    }
    from_js(value)
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
    let request = byte_range_request(range);

    let (range_fn, tx_value) = {
      let handles = tx.handles.borrow();
      (handles.range.clone(), handles.value.clone())
    };
    let value = call_method2(
      range_fn,
      tx_value,
      JsValue::from_str(tree),
      to_js(&request)?,
    )
    .await?;
    decode_byte_entries(value)
  }

  async fn callback_get(&self, tree: &str, key: &EngineKey) -> BTreeResult<Option<Vec<u8>>> {
    let tx = self.begin_backend_transaction(false).await?;
    let result = self.tx_get(&tx, tree, key).await;
    let _ = tx.rollback().await;
    result
  }

  async fn callback_insert(&self, tree: &str, key: &EngineKey, row: &[u8]) -> BTreeResult<()> {
    let tx = self.begin_backend_transaction(true).await?;
    let result = self.tx_insert(&tx, tree, key, row).await;
    match result {
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

  async fn callback_remove(&self, tree: &str, key: &EngineKey) -> BTreeResult<Option<Vec<u8>>> {
    let tx = self.begin_backend_transaction(true).await?;
    let result = self.tx_remove(&tx, tree, key).await;
    match result {
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
    let result = self.tx_range(&tx, tree, range).await;
    let _ = tx.rollback().await;
    result
  }
}

fn byte_range_request<R>(range: &R) -> ByteRangeRequest
where
  R: RangeBounds<EngineKey>,
{
  ByteRangeRequest {
    start: bound_key(range.start_bound()),
    start_inclusive: matches!(range.start_bound(), Bound::Included(_)),
    end: bound_key(range.end_bound()),
    end_inclusive: matches!(range.end_bound(), Bound::Included(_)),
  }
}

fn bound_key(bound: Bound<&EngineKey>) -> Option<EngineKey> {
  match bound {
    Bound::Included(key) | Bound::Excluded(key) => Some(key.clone()),
    Bound::Unbounded => None,
  }
}

fn decode_byte_entries(value: JsValue) -> BTreeResult<Vec<(EngineKey, Vec<u8>)>> {
  let rows: Vec<ByteEntry> = from_js(value)?;
  Ok(
    rows
      .into_iter()
      .map(|entry| (entry.key, entry.value))
      .collect(),
  )
}

impl NamedBTreeMap<EngineKey, Vec<u8>> for StoreAdapterCallbacks {
  type Tree = StoreAdapterTree;

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

  fn insert_tree(
    &self,
    name: &str,
    tree: Self::Tree,
  ) -> impl core::future::Future<Output = BTreeResult<()>> + '_ {
    let name = name.to_string();
    async move {
      let mut tx = EngineNamedTreeBackend::begin_transaction(&tree.adapter).await?;
      let source = tree.tree;
      let range_stream = tx.range(&source, ..);
      pin_mut!(range_stream);
      while let Some(item) = range_stream.next().await {
        let (key, value) = item?;
        tx.insert(&name, key, value).await?;
      }
      tx.commit().await
    }
  }

  fn delete_tree(&self, name: &str) -> impl core::future::Future<Output = BTreeResult<()>> + '_ {
    let adapter = self.clone();
    let name = name.to_string();
    async move {
      let mut tx = adapter.begin_transaction().await?;
      let range_stream = tx.range(&name, ..);
      pin_mut!(range_stream);
      while let Some(item) = range_stream.next().await {
        let (key, _) = item?;
        tx.remove(&name, &key).await?;
      }
      tx.commit().await
    }
  }

  fn list_names(&self) -> impl core::future::Future<Output = Vec<String>> + '_ {
    async move { Vec::new() }
  }
}

impl EngineNamedTreeBackend<EngineKey, Vec<u8>> for StoreAdapterCallbacks {
  type Transaction = StoreAdapterTransaction;

  async fn begin_transaction(&self) -> BTreeResult<Self::Transaction> {
    let backend_tx = self.begin_backend_transaction(true).await?;
    Ok(StoreAdapterTransaction {
      adapter: self.clone(),
      backend_tx,
    })
  }
}

impl EngineNamedTreeTransaction<EngineKey, Vec<u8>> for StoreAdapterTransaction {
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

impl db_core::BTreeWriteExecutor<EngineKey, Vec<u8>> for StoreAdapterTree {
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

impl db_core::BTreeWriteExecutor<EngineKey, Vec<u8>> for StoreAdapterTransaction {
  fn get<'a, Q>(
    &'a self,
    key: Q,
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
    <Self as EngineNamedTreeTransaction<EngineKey, Vec<u8>>>::commit(self)
  }

  fn rollback(self) -> impl core::future::Future<Output = BTreeResult<()>>
  where
    Self: Sized,
  {
    <Self as EngineNamedTreeTransaction<EngineKey, Vec<u8>>>::rollback(self)
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
