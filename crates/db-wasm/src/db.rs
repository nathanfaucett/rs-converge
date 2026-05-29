use db_engine::{EngineQuery, EngineResult, IndexSchema, Subscriber, SubscriptionId, TableSchema};
use db_facade::Database;

use futures::lock::Mutex;
use js_sys::Promise;
use serde::Serialize;
use std::sync::Arc;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;

use crate::params::{parse_sql_params, to_js_error};
use crate::pluggable_store::PluggableBackendStore;
use crate::store_adapter::{DatabaseEngineOptions, StoreAdapterCallbacks};

/// Strongly-typed callback for query subscriptions.
/// TypeScript sees: `(error: Error | null, result: EngineResult | null) => void`
#[wasm_bindgen]
extern "C" {
  #[wasm_bindgen(typescript_type = "EngineResult")]
  pub type JsEngineResult;

  #[wasm_bindgen(typescript_type = "(error: Error | null, result: EngineResult | null) => void")]
  pub type SubscribeCallback;
}

fn serialize_output<T: Serialize + ?Sized>(value: &T) -> Result<JsValue, JsValue> {
  value
    .serialize(&serde_wasm_bindgen::Serializer::json_compatible())
    .map_err(to_js_error)
}

fn serialize_engine_result(value: &EngineResult) -> Result<JsEngineResult, JsValue> {
  serialize_output(value).map(|value| value.unchecked_into())
}

fn map_backend_open_error(error: impl core::fmt::Display) -> JsValue {
  let message = error.to_string();
  let lower = message.to_ascii_lowercase();

  if lower.contains("invalid magic bytes")
    || lower.contains("failed to parse header")
    || lower.contains("unable to parse chunk")
  {
    return to_js_error(format!(
      "{}. Backend adapter returned bytes that do not match the expected row encoding. Ensure adapter methods return raw Uint8Array bytes (not JSON/base64 strings), avoid mixing old/new storage formats, and isolate keyspace by schema version.",
      message
    ));
  }

  to_js_error(message)
}

#[wasm_bindgen]
pub struct BrowserDatabase {
  inner: Arc<Mutex<Database<PluggableBackendStore>>>,
}

#[wasm_bindgen]
impl BrowserDatabase {
  #[wasm_bindgen(js_name = open)]
  pub fn open() -> BrowserDatabase {
    let store = PluggableBackendStore::InMemory(InMemoryNamedBTree::new());
    let inner = Database::from_store(store);

    BrowserDatabase {
      inner: Arc::new(Mutex::new(inner)),
    }
  }

  pub async fn open_with_backend(
    options: DatabaseEngineOptions,
  ) -> Result<BrowserDatabase, JsValue> {
    let adapter_value: JsValue = options.into();
    let adapter = StoreAdapterCallbacks::try_from(adapter_value).map_err(to_js_error)?;
    let store = PluggableBackendStore::External(adapter);
    let inner = Database::open_with_store(store)
      .await
      .map_err(map_backend_open_error)?;
    Ok(BrowserDatabase {
      inner: Arc::new(Mutex::new(inner)),
    })
  }

  #[wasm_bindgen(js_name = openWithBackend)]
  pub fn open_with_backend_js(options: DatabaseEngineOptions) -> Promise {
    let mut options = Some(options);

    Promise::new(&mut move |resolve, reject| {
      let resolve = resolve.clone();
      let reject = reject.clone();
      let Some(options) = options.take() else {
        let error = js_sys::Error::new("openWithBackend promise executor called multiple times");
        let _ = reject.call1(&JsValue::UNDEFINED, &error.into());
        return;
      };

      spawn_local(async move {
        let result = BrowserDatabase::open_with_backend(options)
          .await
          .map(JsValue::from);

        match result {
          Ok(value) => {
            let _ = resolve.call1(&JsValue::UNDEFINED, &value);
          }
          Err(error) => {
            let _ = reject.call1(&JsValue::UNDEFINED, &error);
          }
        }
      });
    })
  }

  #[wasm_bindgen(js_name = registerTable)]
  pub async fn register_table(&self, schema: TableSchema) -> Result<(), JsValue> {
    self
      .inner
      .lock()
      .await
      .register_table(schema, false)
      .await
      .map_err(to_js_error)
  }

  #[wasm_bindgen(js_name = dropTable)]
  pub async fn drop_table(&self, table_name: &str) -> Result<(), JsValue> {
    self
      .inner
      .lock()
      .await
      .drop_table(table_name, false)
      .await
      .map_err(to_js_error)
  }

  #[wasm_bindgen(js_name = registerIndex)]
  pub async fn register_index(&self, schema: IndexSchema) -> Result<(), JsValue> {
    self
      .inner
      .lock()
      .await
      .register_index(schema)
      .await
      .map_err(to_js_error)
  }

  #[wasm_bindgen(js_name = dropIndex)]
  pub async fn drop_index(&self, index_name: &str) -> Result<(), JsValue> {
    self
      .inner
      .lock()
      .await
      .drop_index(index_name)
      .await
      .map_err(to_js_error)
  }

  #[wasm_bindgen(js_name = describeTable)]
  pub fn describe_table(&self, table_name: &str) -> Option<TableSchema> {
    let inner = self.inner.try_lock()?;
    inner.describe_table(table_name)
  }

  #[wasm_bindgen(js_name = executeQuery)]
  pub async fn execute_query(&self, query: EngineQuery) -> Result<JsEngineResult, JsValue> {
    let result = self
      .inner
      .lock()
      .await
      .execute_query(query)
      .await
      .map_err(to_js_error)?;
    serialize_engine_result(&result)
  }

  #[wasm_bindgen(js_name = executeSql)]
  pub async fn execute_sql(&self, sql: &str) -> Result<JsEngineResult, JsValue> {
    let result = self
      .inner
      .lock()
      .await
      .execute_sql(sql)
      .await
      .map_err(to_js_error)?;
    serialize_engine_result(&result)
  }

  #[wasm_bindgen(js_name = executeSqlWithParams)]
  pub async fn execute_sql_with_params(
    &self,
    sql: &str,
    params: JsValue,
  ) -> Result<JsEngineResult, JsValue> {
    let params = parse_sql_params(params)?;
    let result = self
      .inner
      .lock()
      .await
      .execute_sql_with_params(sql, &params)
      .await
      .map_err(to_js_error)?;
    serialize_engine_result(&result)
  }

  #[wasm_bindgen(js_name = selectSimple)]
  pub fn select_simple(
    table: String,
    projection: Vec<usize>,
    predicate: Option<db_engine::QualifiedPredicate>,
  ) -> EngineQuery {
    EngineQuery::select_simple(table, projection, predicate)
  }

  #[wasm_bindgen(js_name = subscribeQuery)]
  pub async fn subscribe_query(
    &self,
    query: EngineQuery,
    callback: SubscribeCallback,
  ) -> Result<SubscriptionId, JsValue> {
    let callback: js_sys::Function = callback.unchecked_into();
    let sub_id = self
      .inner
      .lock()
      .await
      .subscribe_query(query, Arc::new(WasmSubscriber { callback }), None)
      .await
      .map_err(to_js_error)?;
    Ok(sub_id)
  }

  #[wasm_bindgen(js_name = subscribeSql)]
  pub async fn subscribe_sql(
    &self,
    sql: &str,
    callback: SubscribeCallback,
  ) -> Result<SubscriptionId, JsValue> {
    let callback: js_sys::Function = callback.unchecked_into();
    let sub_id = self
      .inner
      .lock()
      .await
      .subscribe_sql(sql, Arc::new(WasmSubscriber { callback }), None)
      .await
      .map_err(to_js_error)?;
    Ok(sub_id)
  }

  #[wasm_bindgen(js_name = subscribeSqlWithParams)]
  pub async fn subscribe_sql_with_params(
    &self,
    sql: &str,
    params: JsValue,
    callback: SubscribeCallback,
  ) -> Result<SubscriptionId, JsValue> {
    let params = parse_sql_params(params)?;
    let callback: js_sys::Function = callback.unchecked_into();
    let sub_id = self
      .inner
      .lock()
      .await
      .subscribe_sql_with_params(sql, &params, Arc::new(WasmSubscriber { callback }), None)
      .await
      .map_err(to_js_error)?;
    Ok(sub_id)
  }

  #[wasm_bindgen(js_name = unsubscribe)]
  pub async fn unsubscribe(&self, id: SubscriptionId) -> Result<(), JsValue> {
    self
      .inner
      .lock()
      .await
      .unsubscribe(id)
      .await
      .map_err(to_js_error)
  }
}

/// WASM subscriber wrapper that bridges Rust callbacks to JS functions
struct WasmSubscriber {
  callback: js_sys::Function,
}

impl Subscriber for WasmSubscriber {
  fn on_results(&self, result: Result<EngineResult, db_engine::EngineError>) {
    let this = JsValue::null();
    match result {
      Ok(results) => {
        let results_js = serialize_output(&results).unwrap_or_else(|error| error);
        let _ = self.callback.call2(&this, &JsValue::null(), &results_js);
      }
      Err(e) => {
        let error = js_sys::Error::new(&e.to_string());
        let _ = self.callback.call2(&this, &error.into(), &JsValue::null());
      }
    }
  }
}
