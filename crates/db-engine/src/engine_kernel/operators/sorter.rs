use alloc::vec::Vec;
use core::cmp::Ordering;
use hashbrown::HashSet;

use crate::{
  EngineRow, EngineValue,
  query::{OrderBy, SortDirection},
};

/// Sorts, deduplicates, and slices a set of projected rows.
/// Each entry is `(sort_keys, projected_row)` where sort keys are pre-extracted
/// by the caller (they may reference columns outside the projection).
pub struct Sorter {
  order_by: Vec<OrderBy>,
  distinct: bool,
  limit: Option<usize>,
  offset: Option<usize>,
  input: Vec<(Vec<EngineValue>, EngineRow)>,
}

impl Sorter {
  pub fn new(
    order_by: Vec<OrderBy>,
    distinct: bool,
    limit: Option<usize>,
    offset: Option<usize>,
    input: Vec<(Vec<EngineValue>, EngineRow)>,
  ) -> Self {
    Self {
      order_by,
      distinct,
      limit,
      offset,
      input,
    }
  }

  pub fn execute(self) -> Vec<EngineRow> {
    let Self {
      order_by,
      distinct,
      limit,
      offset,
      mut input,
    } = self;

    if distinct {
      let mut seen: HashSet<EngineRow> = HashSet::new();
      input.retain(|(_, row)| seen.insert(row.clone()));
    }

    if !order_by.is_empty() {
      input.sort_by(|(a_keys, _), (b_keys, _)| {
        for (i, ord) in order_by.iter().enumerate() {
          let av = a_keys.get(i).unwrap_or(&EngineValue::Null);
          let bv = b_keys.get(i).unwrap_or(&EngineValue::Null);
          let cmp = av.cmp(bv);
          let cmp = match ord.direction {
            SortDirection::Asc => cmp,
            SortDirection::Desc => cmp.reverse(),
          };
          if cmp != Ordering::Equal {
            return cmp;
          }
        }
        Ordering::Equal
      });
    }

    let rows = input
      .into_iter()
      .skip(offset.unwrap_or(0))
      .map(|(_, row)| row);

    match limit {
      Some(limit) => rows.take(limit).collect(),
      None => rows.collect(),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn sort_ascending() {
    let input = vec![
      (
        vec![EngineValue::Integer(3)],
        vec![EngineValue::Text("c".into())],
      ),
      (
        vec![EngineValue::Integer(1)],
        vec![EngineValue::Text("a".into())],
      ),
      (
        vec![EngineValue::Integer(2)],
        vec![EngineValue::Text("b".into())],
      ),
    ];
    let order_by = vec![OrderBy {
      expr: crate::query::QualifiedColumn {
        table: "t".into(),
        column_index: 0,
      },
      direction: SortDirection::Asc,
    }];
    let out = Sorter::new(order_by, false, None, None, input).execute();
    assert_eq!(
      out,
      vec![
        vec![EngineValue::Text("a".into())],
        vec![EngineValue::Text("b".into())],
        vec![EngineValue::Text("c".into())],
      ]
    );
  }

  #[test]
  fn sort_descending() {
    let input = vec![
      (vec![EngineValue::Integer(1)], vec![EngineValue::Integer(1)]),
      (vec![EngineValue::Integer(3)], vec![EngineValue::Integer(3)]),
      (vec![EngineValue::Integer(2)], vec![EngineValue::Integer(2)]),
    ];
    let order_by = vec![OrderBy {
      expr: crate::query::QualifiedColumn {
        table: "t".into(),
        column_index: 0,
      },
      direction: SortDirection::Desc,
    }];
    let out = Sorter::new(order_by, false, None, None, input).execute();
    assert_eq!(
      out,
      vec![
        vec![EngineValue::Integer(3)],
        vec![EngineValue::Integer(2)],
        vec![EngineValue::Integer(1)],
      ]
    );
  }

  #[test]
  fn limit_and_offset() {
    let input: Vec<(Vec<EngineValue>, EngineRow)> = (1..=10)
      .map(|i| (vec![], vec![EngineValue::Integer(i)]))
      .collect();
    let out = Sorter::new(vec![], false, Some(3), Some(2), input).execute();
    assert_eq!(
      out,
      vec![
        vec![EngineValue::Integer(3)],
        vec![EngineValue::Integer(4)],
        vec![EngineValue::Integer(5)],
      ]
    );
  }

  #[test]
  fn distinct_deduplicates() {
    let input = vec![
      (vec![], vec![EngineValue::Integer(1)]),
      (vec![], vec![EngineValue::Integer(2)]),
      (vec![], vec![EngineValue::Integer(1)]),
    ];
    let out = Sorter::new(vec![], true, None, None, input).execute();
    assert_eq!(out.len(), 2);
  }
}
