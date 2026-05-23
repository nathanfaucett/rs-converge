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
  resolve_operand_impl(op, ctx)
}

fn resolve_operand_impl(op: &QualifiedOperand, ctx: &dyn RowContext) -> Option<EngineValue> {
  match op {
    QualifiedOperand::Value(v) => Some(v.clone()),
    QualifiedOperand::Column(qc) => ctx.get_value(&qc.table, qc.column_index).cloned(),
    QualifiedOperand::Lower(inner) => resolve_lower_operand(inner, ctx),
  }
}

fn resolve_lower_operand(inner: &QualifiedOperand, ctx: &dyn RowContext) -> Option<EngineValue> {
  match resolve_operand(inner, ctx) {
    Some(EngineValue::Text(s)) => Some(EngineValue::Text(s.to_lowercase())),
    other => other,
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
    QualifiedPredicate::And(l, r) => eval_and_predicate(l, r, ctx, eval_ctx),
    QualifiedPredicate::Or(l, r) => eval_or_predicate(l, r, ctx, eval_ctx),
    QualifiedPredicate::Not(p) => !eval_predicate(p, ctx, eval_ctx),
    _ => eval_leaf_predicate(pred, ctx, eval_ctx),
  }
}

fn eval_and_predicate(
  left: &QualifiedPredicate,
  right: &QualifiedPredicate,
  ctx: &dyn RowContext,
  eval_ctx: &EvalContext,
) -> bool {
  eval_predicate(left, ctx, eval_ctx) && eval_predicate(right, ctx, eval_ctx)
}

fn eval_or_predicate(
  left: &QualifiedPredicate,
  right: &QualifiedPredicate,
  ctx: &dyn RowContext,
  eval_ctx: &EvalContext,
) -> bool {
  eval_predicate(left, ctx, eval_ctx) || eval_predicate(right, ctx, eval_ctx)
}

fn eval_leaf_predicate(
  pred: &QualifiedPredicate,
  ctx: &dyn RowContext,
  eval_ctx: &EvalContext,
) -> bool {
  pred.eval_leaf(ctx, eval_ctx)
}

impl QualifiedPredicate {
  fn eval_leaf(&self, ctx: &dyn RowContext, eval_ctx: &EvalContext) -> bool {
    if let Some((op, left, right)) = self.as_comparison() {
      return eval_comparison_predicate(op, left, right, ctx);
    }

    match self {
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
      QualifiedPredicate::Like {
        expr,
        pattern,
        negated,
      } => eval_like_predicate(expr, pattern, *negated, ctx),
      _ => false,
    }
  }

  fn as_comparison(&self) -> Option<(ComparisonOp, &QualifiedOperand, &QualifiedOperand)> {
    match self {
      QualifiedPredicate::Equals(l, r) => Some((ComparisonOp::Eq, l, r)),
      QualifiedPredicate::NotEquals(l, r) => Some((ComparisonOp::Ne, l, r)),
      QualifiedPredicate::LessThan(l, r) => Some((ComparisonOp::Lt, l, r)),
      QualifiedPredicate::LessThanOrEquals(l, r) => Some((ComparisonOp::Le, l, r)),
      QualifiedPredicate::GreaterThan(l, r) => Some((ComparisonOp::Gt, l, r)),
      QualifiedPredicate::GreaterThanOrEquals(l, r) => Some((ComparisonOp::Ge, l, r)),
      _ => None,
    }
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
  h.matches(ctx)
}

impl HavingPredicate {
  pub fn matches(&self, ctx: &GroupRowContext<'_>) -> bool {
    match self {
      HavingPredicate::And(l, r) => l.matches(ctx) && r.matches(ctx),
      HavingPredicate::Or(l, r) => l.matches(ctx) || r.matches(ctx),
      HavingPredicate::Not(p) => !p.matches(ctx),
      _ => self.matches_leaf(ctx),
    }
  }

  fn matches_leaf(&self, ctx: &GroupRowContext<'_>) -> bool {
    if let Some((op, r, v)) = self.as_comparison() {
      return self.matches_comparison(op, r, v, ctx);
    }

    match self {
      HavingPredicate::IsNull(_) => self.matches_null(true, ctx),
      HavingPredicate::IsNotNull(_) => self.matches_null(false, ctx),
      _ => false,
    }
  }

  fn matches_comparison(
    &self,
    op: ComparisonOp,
    r: &RefOrAgg,
    v: &EngineValue,
    ctx: &GroupRowContext<'_>,
  ) -> bool {
    having_cmp(op, r, v, ctx)
  }

  fn matches_null(&self, null_expected: bool, ctx: &GroupRowContext<'_>) -> bool {
    let r = self.ref_or_agg();
    match resolve_having_ref(r, ctx) {
      Some(EngineValue::Null) => null_expected,
      Some(_) => !null_expected,
      None => false,
    }
  }

  fn as_comparison(&self) -> Option<(ComparisonOp, &RefOrAgg, &EngineValue)> {
    match self {
      HavingPredicate::Equals(r, v) => Some((ComparisonOp::Eq, r, v)),
      HavingPredicate::NotEquals(r, v) => Some((ComparisonOp::Ne, r, v)),
      HavingPredicate::LessThan(r, v) => Some((ComparisonOp::Lt, r, v)),
      HavingPredicate::LessThanOrEquals(r, v) => Some((ComparisonOp::Le, r, v)),
      HavingPredicate::GreaterThan(r, v) => Some((ComparisonOp::Gt, r, v)),
      HavingPredicate::GreaterThanOrEquals(r, v) => Some((ComparisonOp::Ge, r, v)),
      _ => None,
    }
  }

  fn ref_or_agg(&self) -> &RefOrAgg {
    match self {
      HavingPredicate::Equals(r, _)
      | HavingPredicate::NotEquals(r, _)
      | HavingPredicate::LessThan(r, _)
      | HavingPredicate::LessThanOrEquals(r, _)
      | HavingPredicate::GreaterThan(r, _)
      | HavingPredicate::GreaterThanOrEquals(r, _)
      | HavingPredicate::IsNull(r)
      | HavingPredicate::IsNotNull(r) => r,
      _ => unreachable!(),
    }
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

  #[test]
  fn like_match_patterns() {
    assert!(like_matches("hello", "h%o"));
    assert!(like_matches("hello", "h_llo"));
    assert!(like_matches("hello", "%llo"));
    assert!(!like_matches("hello", "h_oo"));
    assert!(like_matches("", "%"));
    assert!(!like_matches("", "_"));
  }

  #[test]
  fn eval_like_predicate_for_column_text() {
    let row = vec![EngineValue::Text("hello".into())];
    let ctx = SingleRowContext {
      table: "t",
      row: &row,
    };

    let pred = QualifiedPredicate::Like {
      expr: QualifiedOperand::Column(QualifiedColumn {
        table: "t".into(),
        column_index: 0,
      }),
      pattern: QualifiedOperand::Value(EngineValue::Text("h%o".into())),
      negated: false,
    };

    assert!(eval_predicate(&pred, &ctx, &EvalContext::empty()));
  }
}
