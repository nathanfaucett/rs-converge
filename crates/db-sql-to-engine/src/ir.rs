#[cfg(not(feature = "std"))]
use alloc::string::String;
#[cfg(feature = "wasm")]
#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, format, string::ToString};

#[derive(Clone, Debug)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct CanonicalQuery {
  pub engine_query: db_engine::EngineQuery,
}

impl From<db_engine::EngineQuery> for CanonicalQuery {
  fn from(eq: db_engine::EngineQuery) -> Self {
    Self { engine_query: eq }
  }
}

#[derive(Clone, Debug)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub enum DdlOp {
  CreateTable(db_engine::TableSchema, bool),
  DropTable(String, bool),
  CreateIndex(db_engine::IndexSchema, bool),
  DropIndex(String, bool),
}

#[derive(Clone, Debug)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub enum CanonicalStatement {
  Query(db_engine::EngineQuery),
  Ddl(DdlOp),
}

impl From<db_engine::EngineQuery> for CanonicalStatement {
  fn from(eq: db_engine::EngineQuery) -> Self {
    CanonicalStatement::Query(eq)
  }
}

impl From<DdlOp> for CanonicalStatement {
  fn from(op: DdlOp) -> Self {
    CanonicalStatement::Ddl(op)
  }
}
