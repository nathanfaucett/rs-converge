use alloc::{
  string::{String, ToString},
  vec::Vec,
};
use hashbrown::{HashMap, HashSet};

use crate::query::{JoinClause, JoinKind, JoinOn, QualifiedColumn};
use crate::store_backend::{EngineStoreBackend, collect_table_rows};
use crate::{EngineError, EngineRow};

use super::operators::{
  JoinAlgorithm, NestedLoopFull, NestedLoopInner, NestedLoopLeft, NestedLoopRight,
};

pub(crate) type JoinedRowState = HashMap<String, Option<EngineRow>>;
pub(crate) type JoinedRowStates = Vec<JoinedRowState>;

const MAX_JOIN_STATES: usize = 100_000;

fn ensure_join_state_limit(len: usize) -> Result<(), EngineError> {
  if len > MAX_JOIN_STATES {
    return Err(EngineError::QueryLimitExceeded(format!(
      "join state size {len} exceeded max {MAX_JOIN_STATES}; refine joins/predicates"
    )));
  }
  Ok(())
}

pub(crate) fn collect_tables(
  base_table: &str,
  from_tables: &[String],
  joins: &[JoinClause],
) -> HashSet<String> {
  let mut tables: HashSet<String> = HashSet::new();
  tables.insert(base_table.to_string());
  for table in from_tables {
    tables.insert(table.clone());
  }
  for join in joins {
    tables.insert(join.left_table.clone());
    tables.insert(join.right_table.clone());
  }
  tables
}

pub(crate) fn build_join_template(tables: &HashSet<String>) -> JoinedRowState {
  let mut template: JoinedRowState = HashMap::new();
  for table in tables {
    template.insert(table.clone(), None);
  }
  template
}

pub(crate) async fn collect_table_rows_map<S>(
  tx: &mut super::NamedTreeEngineTransaction<S>,
  tables: &HashSet<String>,
) -> Result<HashMap<String, Vec<EngineRow>>, EngineError>
where
  S: EngineStoreBackend,
{
  let mut table_rows_map: HashMap<String, Vec<EngineRow>> = HashMap::new();
  for table in tables {
    let rows_with_pk = collect_table_rows(tx, table, None).await?;
    let rows = rows_with_pk
      .into_iter()
      .map(|(_pk, row)| row)
      .collect::<Vec<_>>();
    table_rows_map.insert(table.clone(), rows);
  }
  Ok(table_rows_map)
}

pub(crate) fn seed_joined_row_states(
  base_table: &str,
  table_rows_map: &HashMap<String, Vec<EngineRow>>,
  template: &JoinedRowState,
) -> JoinedRowStates {
  let mut partial_results: JoinedRowStates = Vec::new();
  if let Some(base_rows) = table_rows_map.get(base_table) {
    for row in base_rows {
      let mut state = template.clone();
      state.insert(base_table.to_string(), Some(row.clone()));
      partial_results.push(state);
    }
  }
  partial_results
}

pub(crate) fn expand_with_from_tables(
  mut partial_results: JoinedRowStates,
  from_tables: &[String],
  table_rows_map: &HashMap<String, Vec<EngineRow>>,
) -> Result<JoinedRowStates, EngineError> {
  for from_table in from_tables {
    let source_rows = table_rows_map.get(from_table).cloned().unwrap_or_default();
    if source_rows.is_empty() {
      partial_results.clear();
      break;
    }

    let mut expanded: JoinedRowStates = Vec::new();
    for partial in &partial_results {
      for source_row in &source_rows {
        let mut next = partial.clone();
        next.insert(from_table.clone(), Some(source_row.clone()));
        expanded.push(next);
      }
    }
    ensure_join_state_limit(expanded.len())?;
    partial_results = expanded;
  }

  Ok(partial_results)
}

pub(crate) fn apply_join_clauses(
  joins: &[JoinClause],
  table_rows_map: &HashMap<String, Vec<EngineRow>>,
  template: &JoinedRowState,
  mut partial_results: JoinedRowStates,
) -> Result<JoinedRowStates, EngineError> {
  for join in joins {
    let right_table = &join.right_table;

    let join_pairs: Vec<(QualifiedColumn, QualifiedColumn)> = match &join.on {
      JoinOn::ColumnEq { left, right } => vec![(left.clone(), right.clone())],
      JoinOn::ColumnEqList { pairs } => pairs.clone(),
    };

    let right_rows = table_rows_map.get(right_table).cloned().unwrap_or_default();

    let algorithm: &dyn JoinAlgorithm = match join.kind {
      JoinKind::Inner => &NestedLoopInner,
      JoinKind::Left => &NestedLoopLeft,
      JoinKind::Right => &NestedLoopRight,
      JoinKind::Full => &NestedLoopFull,
    };
    partial_results = algorithm.apply(
      &partial_results,
      &right_rows,
      right_table,
      &join_pairs,
      template,
    );

    ensure_join_state_limit(partial_results.len())?;
  }

  Ok(partial_results)
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::EngineValue;

  fn row(id: i64) -> EngineRow {
    vec![EngineValue::Integer(id)]
  }

  fn row_const() -> EngineRow {
    vec![EngineValue::Integer(1)]
  }

  #[test]
  fn expand_with_from_tables_builds_cartesian_product() {
    let mut base: JoinedRowState = HashMap::new();
    base.insert("users".into(), Some(row(1)));
    let partial_results = vec![base.clone(), base];

    let mut table_rows_map: HashMap<String, Vec<EngineRow>> = HashMap::new();
    table_rows_map.insert("sources".into(), vec![row(10), row(11), row(12)]);

    let expanded = expand_with_from_tables(partial_results, &["sources".into()], &table_rows_map)
      .expect("expand from tables");

    assert_eq!(expanded.len(), 6);
    for state in expanded {
      assert!(state.contains_key("users"));
      assert!(state.contains_key("sources"));
      assert!(
        state
          .get("sources")
          .and_then(|value| value.as_ref())
          .is_some()
      );
    }
  }

  #[test]
  fn expand_with_from_tables_empty_source_clears_results() {
    let mut base: JoinedRowState = HashMap::new();
    base.insert("users".into(), Some(row(1)));
    let partial_results = vec![base];

    let table_rows_map: HashMap<String, Vec<EngineRow>> = HashMap::new();

    let expanded = expand_with_from_tables(partial_results, &["sources".into()], &table_rows_map)
      .expect("expand from tables");

    assert!(expanded.is_empty());
  }

  #[test]
  fn apply_join_clauses_rejects_excessive_state_growth() {
    let mut template: JoinedRowState = HashMap::new();
    template.insert("left".into(), None);
    template.insert("right".into(), None);

    let partial_results: JoinedRowStates = (0..500)
      .map(|_| {
        let mut state = template.clone();
        state.insert("left".into(), Some(row_const()));
        state
      })
      .collect();

    let right_rows: Vec<EngineRow> = (0..500).map(|_| row_const()).collect();

    let mut table_rows_map: HashMap<String, Vec<EngineRow>> = HashMap::new();
    table_rows_map.insert("right".into(), right_rows);

    let joins = vec![JoinClause {
      kind: JoinKind::Left,
      left_table: "left".into(),
      right_table: "right".into(),
      on: JoinOn::ColumnEq {
        left: crate::query::QualifiedColumn {
          table: "left".into(),
          column_index: 0,
        },
        right: crate::query::QualifiedColumn {
          table: "right".into(),
          column_index: 0,
        },
      },
    }];

    let result = apply_join_clauses(&joins, &table_rows_map, &template, partial_results);

    assert!(matches!(result, Err(EngineError::QueryLimitExceeded(_))));
  }
}
