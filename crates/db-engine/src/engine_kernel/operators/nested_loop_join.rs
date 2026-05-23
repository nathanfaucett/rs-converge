use alloc::{
  string::{String, ToString},
  vec::Vec,
};
use hashbrown::HashMap;

use crate::{EngineRow, EngineValue, query::QualifiedColumn};

fn get_partial_column_value(
  partial: &HashMap<String, Option<EngineRow>>,
  qc: &QualifiedColumn,
) -> Option<EngineValue> {
  match partial.get(&qc.table) {
    Some(Some(row)) => row.get(qc.column_index).cloned(),
    _ => None,
  }
}

fn right_column_value(row: &EngineRow, qc: &QualifiedColumn) -> Option<EngineValue> {
  row.get(qc.column_index).cloned()
}

fn values_match(left: Option<&EngineValue>, right: Option<&EngineValue>) -> bool {
  matches!((left, right), (Some(left), Some(right)) if *left == *right)
}

fn join_pair_values_match(
  partial: &HashMap<String, Option<EngineRow>>,
  row: &EngineRow,
  pairs: &[(QualifiedColumn, QualifiedColumn)],
) -> bool {
  pairs.iter().all(|(left_qc, right_qc)| {
    let left_val = get_partial_column_value(partial, left_qc);
    let right_val = right_column_value(row, right_qc);
    values_match(left_val.as_ref(), right_val.as_ref())
  })
}

fn push_joined(
  results: &mut Vec<HashMap<String, Option<EngineRow>>>,
  partial: &HashMap<String, Option<EngineRow>>,
  right_table: &str,
  right_row: &EngineRow,
) {
  let mut joined = partial.clone();
  joined.insert(right_table.to_string(), Some(right_row.clone()));
  results.push(joined);
}

fn push_unmatched(
  results: &mut Vec<HashMap<String, Option<EngineRow>>>,
  partial: &HashMap<String, Option<EngineRow>>,
  right_table: &str,
) {
  let mut joined = partial.clone();
  joined.insert(right_table.to_string(), None);
  results.push(joined);
}

pub fn apply_inner_join(
  partial_results: &[HashMap<String, Option<EngineRow>>],
  right_rows: &[EngineRow],
  right_table: &str,
  join_pairs: &[(QualifiedColumn, QualifiedColumn)],
) -> Vec<HashMap<String, Option<EngineRow>>> {
  let mut new_results = Vec::new();
  for partial in partial_results {
    for rr in right_rows {
      if join_pair_values_match(partial, rr, join_pairs) {
        push_joined(&mut new_results, partial, right_table, rr);
      }
    }
  }
  new_results
}

pub fn apply_left_join(
  partial_results: &[HashMap<String, Option<EngineRow>>],
  right_rows: &[EngineRow],
  right_table: &str,
  join_pairs: &[(QualifiedColumn, QualifiedColumn)],
) -> Vec<HashMap<String, Option<EngineRow>>> {
  let mut new_results = Vec::new();
  for partial in partial_results {
    let mut matched = false;
    for rr in right_rows {
      if join_pair_values_match(partial, rr, join_pairs) {
        push_joined(&mut new_results, partial, right_table, rr);
        matched = true;
      }
    }

    if !matched {
      push_unmatched(&mut new_results, partial, right_table);
    }
  }
  new_results
}

pub fn apply_right_join(
  partial_results: &[HashMap<String, Option<EngineRow>>],
  right_rows: &[EngineRow],
  right_table: &str,
  join_pairs: &[(QualifiedColumn, QualifiedColumn)],
  template: &HashMap<String, Option<EngineRow>>,
) -> Vec<HashMap<String, Option<EngineRow>>> {
  let mut new_results = Vec::new();

  for rr in right_rows {
    let mut any = false;
    for partial in partial_results {
      if join_pair_values_match(partial, rr, join_pairs) {
        push_joined(&mut new_results, partial, right_table, rr);
        any = true;
      }
    }

    if !any {
      let mut np = template.clone();
      np.insert(right_table.to_string(), Some(rr.clone()));
      new_results.push(np);
    }
  }

  new_results
}

pub fn apply_full_join(
  partial_results: &[HashMap<String, Option<EngineRow>>],
  right_rows: &[EngineRow],
  right_table: &str,
  join_pairs: &[(QualifiedColumn, QualifiedColumn)],
  template: &HashMap<String, Option<EngineRow>>,
) -> Vec<HashMap<String, Option<EngineRow>>> {
  let mut new_results = Vec::new();
  let mut matched = vec![false; right_rows.len()];

  for partial in partial_results {
    let mut any = false;
    for (ri, rr) in right_rows.iter().enumerate() {
      if join_pair_values_match(partial, rr, join_pairs) {
        push_joined(&mut new_results, partial, right_table, rr);
        matched[ri] = true;
        any = true;
      }
    }

    if !any {
      push_unmatched(&mut new_results, partial, right_table);
    }
  }

  for (ri, rr) in right_rows.iter().enumerate() {
    if !matched[ri] {
      let mut np = template.clone();
      np.insert(right_table.to_string(), Some(rr.clone()));
      new_results.push(np);
    }
  }

  new_results
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::EngineValue;

  fn make_partial(table: &str, row: Option<EngineRow>) -> HashMap<String, Option<EngineRow>> {
    let mut m = HashMap::new();
    m.insert(table.to_string(), row);
    m
  }

  fn qc(table: &str, idx: usize) -> QualifiedColumn {
    QualifiedColumn {
      table: table.into(),
      column_index: idx,
    }
  }

  #[test]
  fn inner_join_matching_rows() {
    let left = vec![make_partial("a", Some(vec![EngineValue::Integer(1)]))];
    let right = vec![vec![EngineValue::Integer(1)]];
    let join_pairs = vec![(qc("a", 0), qc("b", 0))];
    let result = apply_inner_join(&left, &right, "b", &join_pairs);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0]["b"], Some(vec![EngineValue::Integer(1)]));
  }

  #[test]
  fn inner_join_no_match_returns_empty() {
    let left = vec![make_partial("a", Some(vec![EngineValue::Integer(1)]))];
    let right = vec![vec![EngineValue::Integer(2)]];
    let join_pairs = vec![(qc("a", 0), qc("b", 0))];
    let result = apply_inner_join(&left, &right, "b", &join_pairs);
    assert!(result.is_empty());
  }

  #[test]
  fn left_join_no_right_match_preserves_left() {
    let left = vec![make_partial("a", Some(vec![EngineValue::Integer(1)]))];
    let right: Vec<EngineRow> = vec![];
    let join_pairs = vec![(qc("a", 0), qc("b", 0))];
    let result = apply_left_join(&left, &right, "b", &join_pairs);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0]["b"], None);
  }

  #[test]
  fn right_join_no_left_match_preserves_right() {
    let left: Vec<HashMap<String, Option<EngineRow>>> = vec![];
    let right = vec![vec![EngineValue::Integer(99)]];
    let template = make_partial("a", None);
    let join_pairs = vec![(qc("a", 0), qc("b", 0))];
    let result = apply_right_join(&left, &right, "b", &join_pairs, &template);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0]["b"], Some(vec![EngineValue::Integer(99)]));
    assert_eq!(result[0]["a"], None);
  }

  #[test]
  fn full_join_includes_unmatched_from_both_sides() {
    let left = vec![make_partial("a", Some(vec![EngineValue::Integer(1)]))];
    let right = vec![vec![EngineValue::Integer(2)]];
    let template = make_partial("a", None);
    let join_pairs = vec![(qc("a", 0), qc("b", 0))];
    let result = apply_full_join(&left, &right, "b", &join_pairs, &template);
    // left row unmatched → appears with None right
    // right row unmatched → appears with None left
    assert_eq!(result.len(), 2);
  }
}
