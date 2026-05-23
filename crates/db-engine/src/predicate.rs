use alloc::{string::String, vec::Vec};
use hashbrown::{HashMap, HashSet};

use crate::{
  EngineKey, EngineQuery, EngineRow, EngineValue, IndexSchema,
  query::{HavingPredicate, QualifiedColumn, QualifiedOperand, QualifiedPredicate, RefOrAgg},
};
use db_types::key_encoding::{DefaultEncoding, KeyEncoding};

pub trait RowContext {
  fn get_value(&self, table: &str, col_index: usize) -> Option<&EngineValue>;
}

pub struct SingleRowContext<'a> {
  pub table: &'a str,
  pub row: &'a EngineRow,
}

impl<'a> RowContext for SingleRowContext<'a> {
  fn get_value(&self, table: &str, col_index: usize) -> Option<&EngineValue> {
    if table != self.table {
      return None;
    }
    self.row.get(col_index)
  }
}

pub struct JoinedRowContext<'a> {
  pub partial: &'a HashMap<String, Option<EngineRow>>,
}

impl<'a> RowContext for JoinedRowContext<'a> {
  fn get_value(&self, table: &str, col_index: usize) -> Option<&EngineValue> {
    match self.partial.get(table) {
      Some(Some(row)) => row.get(col_index),
      _ => None,
    }
  }
}

pub struct GroupRowContext<'a> {
  pub row: &'a EngineRow,
  pub group_by: &'a [QualifiedColumn],
}

impl<'a> RowContext for GroupRowContext<'a> {
  fn get_value(&self, table: &str, col_index: usize) -> Option<&EngineValue> {
    self
      .group_by
      .iter()
      .position(|g| g.table == table && g.column_index == col_index)
      .and_then(|pos| self.row.get(pos))
  }
}

pub struct EvalContext {
  pub subquery_cache: HashMap<String, HashSet<EngineValue>>,
}

impl EvalContext {
  pub fn empty() -> Self {
    Self {
      subquery_cache: HashMap::new(),
    }
  }

  pub fn with_cache(subquery_cache: HashMap<String, HashSet<EngineValue>>) -> Self {
    Self { subquery_cache }
  }
}

pub struct PredicateEvaluator<'a> {
  eval_ctx: &'a EvalContext,
}

impl<'a> PredicateEvaluator<'a> {
  pub fn new(eval_ctx: &'a EvalContext) -> Self {
    Self { eval_ctx }
  }

  pub fn matches_row(&self, pred: &QualifiedPredicate, table: &str, row: &EngineRow) -> bool {
    let ctx = SingleRowContext { table, row };
    eval_predicate(pred, &ctx, self.eval_ctx)
  }

  pub fn matches_joined_row(
    &self,
    pred: &QualifiedPredicate,
    partial: &HashMap<String, Option<EngineRow>>,
  ) -> bool {
    let ctx = JoinedRowContext { partial };
    eval_predicate(pred, &ctx, self.eval_ctx)
  }
}

fn resolve_operand(op: &QualifiedOperand, ctx: &dyn RowContext) -> Option<EngineValue> {
  match op {
    QualifiedOperand::Value(v) => Some(v.clone()),
    QualifiedOperand::Column(qc) => ctx.get_value(&qc.table, qc.column_index).cloned(),
    QualifiedOperand::Lower(inner) => match resolve_operand(inner, ctx) {
      Some(EngineValue::Text(s)) => Some(EngineValue::Text(s.to_lowercase())),
      other => other,
    },
  }
}

fn like_matches(text: &str, pattern: &str) -> bool {
  let text: Vec<char> = text.chars().collect();
  let pat: Vec<char> = pattern.chars().collect();
  like_match_inner(&text, &pat)
}

fn like_match_inner(text: &[char], pat: &[char]) -> bool {
  match pat.first() {
    None => text.is_empty(),
    Some('%') => (0..=text.len()).any(|i| like_match_inner(&text[i..], &pat[1..])),
    Some('_') => !text.is_empty() && like_match_inner(&text[1..], &pat[1..]),
    Some(&c) => !text.is_empty() && text[0] == c && like_match_inner(&text[1..], &pat[1..]),
  }
}

/// Shared binary operator evaluation for both WHERE and HAVING predicates.
/// Compares two values using the given operator; returns false if either is None.
#[derive(Debug, Clone, Copy)]
enum ComparisonOp {
  Eq,
  Ne,
  Lt,
  Le,
  Gt,
  Ge,
}

fn eval_binary_op(op: ComparisonOp, left: Option<EngineValue>, right: Option<EngineValue>) -> bool {
  match (left, right) {
    (Some(a), Some(b)) => match op {
      ComparisonOp::Eq => a == b,
      ComparisonOp::Ne => a != b,
      ComparisonOp::Lt => a < b,
      ComparisonOp::Le => a <= b,
      ComparisonOp::Gt => a > b,
      ComparisonOp::Ge => a >= b,
    },
    _ => false,
  }
}

fn eval_comparison_predicate(
  op: ComparisonOp,
  left: &QualifiedOperand,
  right: &QualifiedOperand,
  ctx: &dyn RowContext,
) -> bool {
  eval_binary_op(op, resolve_operand(left, ctx), resolve_operand(right, ctx))
}

fn eval_null_predicate(qc: &QualifiedColumn, negated: bool, ctx: &dyn RowContext) -> bool {
  let is_null = matches!(
    ctx.get_value(&qc.table, qc.column_index),
    Some(EngineValue::Null)
  );

  if negated { !is_null } else { is_null }
}

fn eval_in_list_predicate(
  expr: &QualifiedColumn,
  list: &[EngineValue],
  negated: bool,
  ctx: &dyn RowContext,
) -> bool {
  let found = match ctx.get_value(&expr.table, expr.column_index) {
    Some(v) => list.iter().any(|x| x == v),
    None => false,
  };

  if negated { !found } else { found }
}

fn eval_in_subquery_predicate(
  expr: &QualifiedColumn,
  subquery: &EngineQuery,
  negated: bool,
  ctx: &dyn RowContext,
  eval_ctx: &EvalContext,
) -> bool {
  let lv = ctx.get_value(&expr.table, expr.column_index).cloned();
  let key = format!("{:?}", subquery);
  let found = match (lv, eval_ctx.subquery_cache.get(&key)) {
    (Some(v), Some(s)) => s.contains(&v),
    _ => false,
  };

  if negated { !found } else { found }
}

fn eval_like_predicate(
  expr: &QualifiedOperand,
  pattern: &QualifiedOperand,
  negated: bool,
  ctx: &dyn RowContext,
) -> bool {
  let text = match resolve_operand(expr, ctx) {
    Some(EngineValue::Text(s)) => s,
    Some(EngineValue::Null) | None => return false,
    Some(v) => format!("{:?}", v),
  };

  let pat = match resolve_operand(pattern, ctx) {
    Some(EngineValue::Text(s)) => s,
    Some(EngineValue::Null) | None => return false,
    Some(v) => format!("{:?}", v),
  };

  let matched = like_matches(&text, &pat);
  if negated { !matched } else { matched }
}

pub fn eval_predicate(
  pred: &QualifiedPredicate,
  ctx: &dyn RowContext,
  eval_ctx: &EvalContext,
) -> bool {
  match pred {
    QualifiedPredicate::Equals(l, r) => eval_comparison_predicate(ComparisonOp::Eq, l, r, ctx),
    QualifiedPredicate::NotEquals(l, r) => eval_comparison_predicate(ComparisonOp::Ne, l, r, ctx),
    QualifiedPredicate::LessThan(l, r) => eval_comparison_predicate(ComparisonOp::Lt, l, r, ctx),
    QualifiedPredicate::LessThanOrEquals(l, r) => {
      eval_comparison_predicate(ComparisonOp::Le, l, r, ctx)
    }
    QualifiedPredicate::GreaterThan(l, r) => eval_comparison_predicate(ComparisonOp::Gt, l, r, ctx),
    QualifiedPredicate::GreaterThanOrEquals(l, r) => {
      eval_comparison_predicate(ComparisonOp::Ge, l, r, ctx)
    }
    QualifiedPredicate::IsNull(qc) => eval_null_predicate(qc, false, ctx),
    QualifiedPredicate::IsNotNull(qc) => eval_null_predicate(qc, true, ctx),
    QualifiedPredicate::InList {
      expr,
      list,
      negated,
    } => eval_in_list_predicate(expr, list, *negated, ctx),
    QualifiedPredicate::InSubquery {
      expr,
      subquery,
      negated,
    } => eval_in_subquery_predicate(expr, subquery, *negated, ctx, eval_ctx),
    QualifiedPredicate::And(l, r) => {
      eval_predicate(l, ctx, eval_ctx) && eval_predicate(r, ctx, eval_ctx)
    }
    QualifiedPredicate::Or(l, r) => {
      eval_predicate(l, ctx, eval_ctx) || eval_predicate(r, ctx, eval_ctx)
    }
    QualifiedPredicate::Not(p) => !eval_predicate(p, ctx, eval_ctx),
    QualifiedPredicate::Like {
      expr,
      pattern,
      negated,
    } => eval_like_predicate(expr, pattern, *negated, ctx),
  }
}

fn resolve_having_ref(r: &RefOrAgg, ctx: &GroupRowContext<'_>) -> Option<EngineValue> {
  match r {
    RefOrAgg::Column(qc) => ctx
      .group_by
      .iter()
      .position(|g| g == qc)
      .and_then(|pos| ctx.row.get(pos).cloned()),
    RefOrAgg::AggregateIndex(i) => ctx.row.get(ctx.group_by.len() + *i).cloned(),
  }
}

fn having_cmp(op: ComparisonOp, r: &RefOrAgg, v: &EngineValue, ctx: &GroupRowContext<'_>) -> bool {
  eval_binary_op(op, resolve_having_ref(r, ctx), Some(v.clone()))
}

pub fn eval_having_predicate(h: &HavingPredicate, ctx: &GroupRowContext<'_>) -> bool {
  match h {
    HavingPredicate::Equals(r, v) => having_cmp(ComparisonOp::Eq, r, v, ctx),
    HavingPredicate::NotEquals(r, v) => having_cmp(ComparisonOp::Ne, r, v, ctx),
    HavingPredicate::LessThan(r, v) => having_cmp(ComparisonOp::Lt, r, v, ctx),
    HavingPredicate::LessThanOrEquals(r, v) => having_cmp(ComparisonOp::Le, r, v, ctx),
    HavingPredicate::GreaterThan(r, v) => having_cmp(ComparisonOp::Gt, r, v, ctx),
    HavingPredicate::GreaterThanOrEquals(r, v) => having_cmp(ComparisonOp::Ge, r, v, ctx),
    HavingPredicate::IsNull(r) => {
      matches!(resolve_having_ref(r, ctx), Some(EngineValue::Null))
    }
    HavingPredicate::IsNotNull(r) => match resolve_having_ref(r, ctx) {
      Some(EngineValue::Null) | None => false,
      Some(_) => true,
    },
    HavingPredicate::And(l, r) => eval_having_predicate(l, ctx) && eval_having_predicate(r, ctx),
    HavingPredicate::Or(l, r) => eval_having_predicate(l, ctx) || eval_having_predicate(r, ctx),
    HavingPredicate::Not(p) => !eval_having_predicate(p, ctx),
  }
}

impl QualifiedPredicate {
  pub fn matches_row(&self, table: &str, row: &EngineRow) -> bool {
    PredicateEvaluator::new(&EvalContext::empty()).matches_row(self, table, row)
  }

  pub fn matches_row_with_ctx(&self, table: &str, row: &EngineRow, eval_ctx: &EvalContext) -> bool {
    PredicateEvaluator::new(eval_ctx).matches_row(self, table, row)
  }

  pub fn matches_joined_row(
    &self,
    partial: &HashMap<String, Option<EngineRow>>,
    eval_ctx: &EvalContext,
  ) -> bool {
    PredicateEvaluator::new(eval_ctx).matches_joined_row(self, partial)
  }

  pub fn index_key_for(&self, index: &IndexSchema) -> Option<EngineKey> {
    let mut values = vec![None; index.column_indices.len()];
    self.fill_index_key_values(index, &mut values)?;
    if values.iter().all(Option::is_some) {
      let values = values.into_iter().map(Option::unwrap).collect::<Vec<_>>();
      Some(<DefaultEncoding as KeyEncoding>::encode_values(&values))
    } else {
      None
    }
  }

  fn fill_index_key_values(
    &self,
    index: &IndexSchema,
    values: &mut [Option<EngineValue>],
  ) -> Option<()> {
    match self {
      QualifiedPredicate::Equals(QualifiedOperand::Column(qc), QualifiedOperand::Value(value))
      | QualifiedPredicate::Equals(QualifiedOperand::Value(value), QualifiedOperand::Column(qc)) => {
        if let Some((slot, _)) = index
          .column_indices
          .iter()
          .enumerate()
          .find(|&(_, &col)| col == qc.column_index)
        {
          if let Some(existing) = &values[slot]
            && existing != value
          {
            return None;
          }
          values[slot] = Some(value.clone());
        }
        Some(())
      }
      QualifiedPredicate::And(left, right) => {
        left.fill_index_key_values(index, values)?;
        right.fill_index_key_values(index, values)?;
        Some(())
      }
      _ => None,
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{EngineValue, query::QualifiedOperand};
  use alloc::{boxed::Box, string::ToString};

  #[test]
  fn single_row_equals() {
    let row = vec![EngineValue::Integer(42)];
    let ctx = SingleRowContext {
      table: "t",
      row: &row,
    };
    let pred = QualifiedPredicate::Equals(
      QualifiedOperand::Column(QualifiedColumn {
        table: "t".into(),
        column_index: 0,
      }),
      QualifiedOperand::Value(EngineValue::Integer(42)),
    );
    assert!(eval_predicate(&pred, &ctx, &EvalContext::empty()));
  }

  #[test]
  fn single_row_wrong_table_returns_false() {
    let row = vec![EngineValue::Integer(42)];
    let ctx = SingleRowContext {
      table: "t",
      row: &row,
    };
    let pred = QualifiedPredicate::Equals(
      QualifiedOperand::Column(QualifiedColumn {
        table: "other".into(),
        column_index: 0,
      }),
      QualifiedOperand::Value(EngineValue::Integer(42)),
    );
    assert!(!eval_predicate(&pred, &ctx, &EvalContext::empty()));
  }

  #[test]
  fn joined_context_multi_table() {
    let row_a = vec![EngineValue::Integer(1)];
    let row_b = vec![EngineValue::Integer(1)];
    let mut partial = HashMap::new();
    partial.insert("a".to_string(), Some(row_a));
    partial.insert("b".to_string(), Some(row_b));
    let ctx = JoinedRowContext { partial: &partial };

    let pred = QualifiedPredicate::Equals(
      QualifiedOperand::Column(QualifiedColumn {
        table: "a".into(),
        column_index: 0,
      }),
      QualifiedOperand::Column(QualifiedColumn {
        table: "b".into(),
        column_index: 0,
      }),
    );
    assert!(eval_predicate(&pred, &ctx, &EvalContext::empty()));
  }

  #[test]
  fn and_short_circuits() {
    let row = vec![EngineValue::Null];
    let ctx = SingleRowContext {
      table: "t",
      row: &row,
    };
    let pred = QualifiedPredicate::And(
      Box::new(QualifiedPredicate::IsNull(QualifiedColumn {
        table: "t".into(),
        column_index: 0,
      })),
      Box::new(QualifiedPredicate::IsNotNull(QualifiedColumn {
        table: "t".into(),
        column_index: 0,
      })),
    );
    assert!(!eval_predicate(&pred, &ctx, &EvalContext::empty()));
  }

  #[test]
  fn in_list_negated() {
    let row = vec![EngineValue::Integer(5)];
    let ctx = SingleRowContext {
      table: "t",
      row: &row,
    };
    let pred = QualifiedPredicate::InList {
      expr: QualifiedColumn {
        table: "t".into(),
        column_index: 0,
      },
      list: vec![EngineValue::Integer(1), EngineValue::Integer(2)],
      negated: true,
    };
    assert!(eval_predicate(&pred, &ctx, &EvalContext::empty()));
  }

  #[test]
  fn group_context_aggregate_index() {
    let group_by = vec![QualifiedColumn {
      table: "t".into(),
      column_index: 0,
    }];
    let row = vec![EngineValue::Integer(10), EngineValue::Integer(99)];
    let ctx = GroupRowContext {
      row: &row,
      group_by: &group_by,
    };

    let pred = HavingPredicate::GreaterThan(RefOrAgg::AggregateIndex(0), EngineValue::Integer(50));
    assert!(eval_having_predicate(&pred, &ctx));
  }

  #[test]
  fn in_subquery_matches_prepopulated_cache() {
    use crate::query::{EngineQuery, SelectOptions};

    let row = vec![EngineValue::Integer(7)];
    let ctx = SingleRowContext {
      table: "t",
      row: &row,
    };

    let subquery = EngineQuery::Select {
      table: "other".into(),
      projection: vec![],
      predicate: None,
      options: Box::new(SelectOptions::default()),
    };
    let key = format!("{:?}", &subquery);
    let mut cache: HashMap<String, HashSet<EngineValue>> = HashMap::new();
    cache.insert(key, [EngineValue::Integer(7)].into());

    let pred = QualifiedPredicate::InSubquery {
      expr: QualifiedColumn {
        table: "t".into(),
        column_index: 0,
      },
      subquery: Box::new(subquery),
      negated: false,
    };

    assert!(eval_predicate(&pred, &ctx, &EvalContext::with_cache(cache)));
  }

  #[test]
  fn in_subquery_empty_cache_returns_false() {
    use crate::query::{EngineQuery, SelectOptions};

    let row = vec![EngineValue::Integer(7)];
    let ctx = SingleRowContext {
      table: "t",
      row: &row,
    };
    let subquery = EngineQuery::Select {
      table: "other".into(),
      projection: vec![],
      predicate: None,
      options: Box::new(SelectOptions::default()),
    };
    let pred = QualifiedPredicate::InSubquery {
      expr: QualifiedColumn {
        table: "t".into(),
        column_index: 0,
      },
      subquery: Box::new(subquery),
      negated: false,
    };

    assert!(!eval_predicate(&pred, &ctx, &EvalContext::empty()));
  }

  #[test]
  fn not_in_subquery_with_cache() {
    use crate::query::{EngineQuery, SelectOptions};

    let row = vec![EngineValue::Integer(99)];
    let ctx = SingleRowContext {
      table: "t",
      row: &row,
    };
    let subquery = EngineQuery::Select {
      table: "other".into(),
      projection: vec![],
      predicate: None,
      options: Box::new(SelectOptions::default()),
    };
    let key = format!("{:?}", &subquery);
    let mut cache: HashMap<String, HashSet<EngineValue>> = HashMap::new();
    cache.insert(
      key,
      [EngineValue::Integer(1), EngineValue::Integer(2)].into(),
    );

    let pred = QualifiedPredicate::InSubquery {
      expr: QualifiedColumn {
        table: "t".into(),
        column_index: 0,
      },
      subquery: Box::new(subquery),
      negated: true,
    };

    assert!(eval_predicate(&pred, &ctx, &EvalContext::with_cache(cache)));
  }

  #[test]
  fn eval_predicate_qualified() {
    let row = vec![EngineValue::Integer(42)];
    let pred = QualifiedPredicate::Equals(
      QualifiedOperand::Column(QualifiedColumn {
        table: "t".into(),
        column_index: 0,
      }),
      QualifiedOperand::Value(EngineValue::Integer(42)),
    );

    let ctx = SingleRowContext {
      table: "t",
      row: &row,
    };
    assert!(eval_predicate(&pred, &ctx, &EvalContext::empty()));
  }
}
