use crate::translate::{SchemaResolver, TranslateError, parse_and_translate_to_ir};

/// Lightweight translator facade and helpers.
pub struct Translator {}

impl Default for Translator {
  fn default() -> Self {
    Self::new()
  }
}

impl Translator {
  pub fn new() -> Self {
    Self {}
  }

  /// Parse SQL and return the target engine query representation.
  pub fn sql_to_engine_query(
    &self,
    sql: &str,
    resolver: &dyn SchemaResolver,
  ) -> Result<db_engine::EngineQuery, TranslateError> {
    self.sql_to_adapter(sql, resolver, &crate::engine_adapter::DbEngineAdapter)
  }

  /// Parse SQL and lower the canonical IR using the provided `EngineAdapter`.
  pub fn sql_to_adapter<A: crate::engine_adapter::EngineAdapter>(
    &self,
    sql: &str,
    resolver: &dyn SchemaResolver,
    adapter: &A,
  ) -> Result<<A as crate::engine_adapter::EngineAdapter>::Output, TranslateError> {
    let cq = parse_and_translate_to_ir(sql, resolver)?;
    adapter.lower(&cq)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::translate::SchemaResolver;
  #[cfg(not(feature = "std"))]
  use alloc::string::String;
  #[cfg(not(feature = "std"))]
  use hashbrown::HashMap;
  #[cfg(feature = "std")]
  use std::collections::HashMap;

  struct DummyResolver {
    tables: HashMap<String, db_engine::TableSchema>,
  }

  impl SchemaResolver for DummyResolver {
    fn describe_table(&self, name: &str) -> Option<db_engine::TableSchema> {
      self.tables.get(name).cloned()
    }
  }

  #[test]
  fn translator_translates_to_engine_query() {
    let mut tables = HashMap::new();
    tables.insert(
      "users".into(),
      db_engine::TableSchema {
        name: "users".into(),
        columns: vec![
          db_engine::ColumnSchema {
            name: "id".into(),
            data_type: db_engine::EngineType::Integer,
          },
          db_engine::ColumnSchema {
            name: "name".into(),
            data_type: db_engine::EngineType::Text,
          },
        ],
        primary_key: vec![0],
      },
    );

    let resolver = DummyResolver { tables };
    let t = Translator::new();
    let eq = t
      .sql_to_engine_query("SELECT id, name FROM users WHERE id = 1;", &resolver)
      .expect("translate");

    match eq {
      db_engine::EngineQuery::Select { table, .. } => {
        assert_eq!(table, "users");
      }
      _ => panic!("unexpected variant"),
    }
  }

  #[test]
  fn translator_sql_to_adapter_uses_adapter_lower() {
    let mut tables = HashMap::new();
    tables.insert(
      "users".into(),
      db_engine::TableSchema {
        name: "users".into(),
        columns: vec![
          db_engine::ColumnSchema {
            name: "id".into(),
            data_type: db_engine::EngineType::Integer,
          },
          db_engine::ColumnSchema {
            name: "name".into(),
            data_type: db_engine::EngineType::Text,
          },
        ],
        primary_key: vec![0],
      },
    );

    let resolver = DummyResolver { tables };
    let t = Translator::new();
    let adapter = crate::engine_adapter::DbEngineAdapter;

    let out = t
      .sql_to_adapter("SELECT id FROM users WHERE id = 2;", &resolver, &adapter)
      .expect("lower");

    // lower with DbEngineAdapter returns an EngineQuery matching the parse
    match out {
      db_engine::EngineQuery::Select { table, .. } => assert_eq!(table, "users"),
      _ => panic!("unexpected lowered query"),
    }
  }
}
