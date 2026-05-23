use alloc::string::String;
use alloc::vec::Vec;
use hashbrown::HashMap;

use crate::{EngineRow, query::QualifiedColumn};

use super::nested_loop_join::{
  apply_full_join, apply_inner_join, apply_left_join, apply_right_join,
};

pub(crate) type JoinedRowState = HashMap<String, Option<EngineRow>>;
pub(crate) type JoinedRowStates = Vec<JoinedRowState>;

/// Seam for join execution algorithms. All join variants receive the same
/// parameters; implementations that do not need the `template` (e.g., inner
/// join) simply ignore it.
pub(crate) type QualifiedColumnPairs = Vec<(QualifiedColumn, QualifiedColumn)>;

pub(crate) trait JoinAlgorithm {
  fn apply(
    &self,
    partial_results: &[JoinedRowState],
    right_rows: &[EngineRow],
    right_table: &str,
    join_pairs: &QualifiedColumnPairs,
    template: &JoinedRowState,
  ) -> JoinedRowStates;
}

/// Nested-loop inner join: only rows with a matching right-side value survive.
pub(crate) struct NestedLoopInner;

impl JoinAlgorithm for NestedLoopInner {
  fn apply(
    &self,
    partial_results: &[JoinedRowState],
    right_rows: &[EngineRow],
    right_table: &str,
    join_pairs: &QualifiedColumnPairs,
    _template: &JoinedRowState,
  ) -> JoinedRowStates {
    apply_inner_join(partial_results, right_rows, right_table, join_pairs)
  }
}

/// Nested-loop left join: unmatched left rows kept with a NULL right side.
pub(crate) struct NestedLoopLeft;

impl JoinAlgorithm for NestedLoopLeft {
  fn apply(
    &self,
    partial_results: &[JoinedRowState],
    right_rows: &[EngineRow],
    right_table: &str,
    join_pairs: &QualifiedColumnPairs,
    _template: &JoinedRowState,
  ) -> JoinedRowStates {
    apply_left_join(partial_results, right_rows, right_table, join_pairs)
  }
}

/// Nested-loop right join: unmatched right rows kept with a NULL left side.
pub(crate) struct NestedLoopRight;

impl JoinAlgorithm for NestedLoopRight {
  fn apply(
    &self,
    partial_results: &[JoinedRowState],
    right_rows: &[EngineRow],
    right_table: &str,
    join_pairs: &QualifiedColumnPairs,
    template: &JoinedRowState,
  ) -> JoinedRowStates {
    apply_right_join(
      partial_results,
      right_rows,
      right_table,
      join_pairs,
      template,
    )
  }
}

/// Nested-loop full outer join: unmatched rows from both sides kept with NULLs.
pub(crate) struct NestedLoopFull;

impl JoinAlgorithm for NestedLoopFull {
  fn apply(
    &self,
    partial_results: &[JoinedRowState],
    right_rows: &[EngineRow],
    right_table: &str,
    join_pairs: &QualifiedColumnPairs,
    template: &JoinedRowState,
  ) -> JoinedRowStates {
    apply_full_join(
      partial_results,
      right_rows,
      right_table,
      join_pairs,
      template,
    )
  }
}
