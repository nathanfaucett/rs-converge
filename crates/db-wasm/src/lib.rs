#![cfg(target_arch = "wasm32")]

mod db;
mod params;
mod pluggable_store;
mod sql;
mod store_adapter;

pub use db_core as core;
pub use db_engine as engine;
pub use db_engine as types;
pub use db_sql_to_engine as sql_to_engine;

pub use db::BrowserDatabase;
pub use sql::{
  translate_sql_to_query, translate_sql_to_query_with_params, translate_sql_to_statement,
  translate_sql_to_statement_with_params,
};
pub use store_adapter::DatabaseEngineOptions;
