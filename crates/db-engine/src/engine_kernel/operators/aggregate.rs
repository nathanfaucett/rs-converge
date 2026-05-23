use alloc::{string::String, vec::Vec};
use core::cmp::Ordering;
use hashbrown::HashMap;

use crate::{
  EngineError, EngineKey, EngineRow, EngineValue,
  predicate::{GroupRowContext, eval_having_predicate},
  query::{Aggregate, HavingPredicate, OrderBy, QualifiedColumn, SortDirection},
};
use db_types::key_encoding::{DefaultEncoding, KeyEncoding};

pub type PartialRow = HashMap<String, Option<EngineRow>>;

#[derive(Clone)]
enum AggState {
  Count(u64),
  Sum(f64),
  Min(Option<EngineValue>),
  Max(Option<EngineValue>),
  Avg { sum: f64, count: u64 },
}

impl AggState {
  fn new_for(agg: &Aggregate) -> Self {
    match agg {
      Aggregate::Count(_) => AggState::Count(0),
      Aggregate::Sum(_) => AggState::Sum(0.0),
      Aggregate::Min(_) => AggState::Min(None),
      Aggregate::Max(_) => AggState::Max(None),
      Aggregate::Avg(_) => AggState::Avg { sum: 0.0, count: 0 },
    }
  }

  fn update(&mut self, agg: &Aggregate, value: Option<EngineValue>) {
    agg.apply_to_state(self, value)
  }

  fn update_count(&mut self, col: &Option<QualifiedColumn>, value: Option<EngineValue>) {
    if let AggState::Count(c) = self {
      if col.is_none() {
        *c += 1;
      } else if let Some(v) = value
        && !matches!(v, EngineValue::Null)
      {
        *c += 1;
      }
    }
  }

  fn update_sum(&mut self, value: Option<EngineValue>) {
    if let AggState::Sum(s) = self
      && let Some(v) = value
      && let Some(n) = to_f64(&v)
    {
      *s += n;
    }
  }

  fn update_min(&mut self, value: Option<EngineValue>) {
    if let AggState::Min(opt) = self
      && let Some(value) = value
    {
      update_best(opt, value, |value, current| value < current);
    }
  }

  fn update_max(&mut self, value: Option<EngineValue>) {
    if let AggState::Max(opt) = self
      && let Some(value) = value
    {
      update_best(opt, value, |value, current| value > current);
    }
  }

  fn update_avg(&mut self, value: Option<EngineValue>) {
    if let AggState::Avg { sum, count } = self
      && let Some(v) = value
      && let Some(n) = to_f64(&v)
    {
      *sum += n;
      *count += 1;
    }
  }

  fn finish(&self) -> EngineValue {
    match self {
      AggState::Count(c) => EngineValue::Integer(*c as i64),
      AggState::Sum(s) => EngineValue::Float(*s),
      AggState::Min(Some(v)) => v.clone(),
      AggState::Min(None) => EngineValue::Null,
      AggState::Max(Some(v)) => v.clone(),
      AggState::Max(None) => EngineValue::Null,
      AggState::Avg { sum, count } => {
        if *count == 0 {
          EngineValue::Null
        } else {
          EngineValue::Float(*sum / (*count as f64))
        }
      }
    }
  }
}

impl Aggregate {
  fn apply_to_state(&self, state: &mut AggState, value: Option<EngineValue>) {
    match self {
      Aggregate::Count(col) => state.update_count(col, value),
      Aggregate::Sum(_) => state.update_sum(value),
      Aggregate::Min(_) => state.update_min(value),
      Aggregate::Max(_) => state.update_max(value),
      Aggregate::Avg(_) => state.update_avg(value),
    }
  }
}

fn update_best<F>(current: &mut Option<EngineValue>, value: EngineValue, is_better: F)
where
  F: FnOnce(&EngineValue, &EngineValue) -> bool,
{
  if current
    .as_ref()
    .is_none_or(|current| is_better(&value, current))
  {
    *current = Some(value);
  }
}

fn to_f64(value: &EngineValue) -> Option<f64> {
  match value {
    EngineValue::Integer(i) => Some(*i as f64),
    EngineValue::Float(f) => Some(*f),
    _ => None,
  }
}

pub struct Aggregator {
  group_by: Vec<QualifiedColumn>,
  aggregates: Vec<Aggregate>,
  having: Option<HavingPredicate>,
  order_by: Vec<OrderBy>,
  limit: Option<usize>,
  offset: Option<usize>,
  input: Vec<PartialRow>,
}

impl Aggregator {
  pub fn new(
    group_by: Vec<QualifiedColumn>,
    aggregates: Vec<Aggregate>,
    having: Option<HavingPredicate>,
    order_by: Vec<OrderBy>,
    limit: Option<usize>,
    offset: Option<usize>,
    input: Vec<PartialRow>,
  ) -> Self {
    Self {
      group_by,
      aggregates,
      having,
      order_by,
      limit,
      offset,
      input,
    }
  }

  pub fn execute(self) -> Result<Vec<EngineRow>, EngineError> {
    let mut out_rows = self.build_grouped_rows()?;
    out_rows = self.apply_having(out_rows);
    self.apply_order_by(&mut out_rows)?;
    Ok(self.apply_limit_offset(out_rows))
  }

  fn build_grouped_rows(&self) -> Result<Vec<EngineRow>, EngineError> {
    let groups = self.build_groups()?;
    self.finish_group_rows(groups)
  }

  fn build_groups(&self) -> Result<HashMap<EngineKey, Vec<AggState>>, EngineError> {
    let mut groups: HashMap<EngineKey, Vec<AggState>> = HashMap::new();

    for partial in &self.input {
      let mut key_vals: Vec<EngineValue> = Vec::with_capacity(self.group_by.len());
      for gc in &self.group_by {
        match partial.get(&gc.table) {
          Some(Some(row)) => key_vals.push(
            row
              .get(gc.column_index)
              .cloned()
              .unwrap_or(EngineValue::Null),
          ),
          Some(None) => key_vals.push(EngineValue::Null),
          None => {
            return Err(EngineError::SchemaMismatch(format!(
              "group_by references unknown table {}",
              gc.table
            )));
          }
        }
      }

      let key = <DefaultEncoding as KeyEncoding>::encode_values(&key_vals);
      let entry = groups
        .entry(key)
        .or_insert_with(|| self.aggregates.iter().map(AggState::new_for).collect());

      for (i, agg) in self.aggregates.iter().enumerate() {
        let val = match agg {
          Aggregate::Count(Some(qc))
          | Aggregate::Sum(qc)
          | Aggregate::Min(qc)
          | Aggregate::Max(qc)
          | Aggregate::Avg(qc) => match partial.get(&qc.table) {
            Some(Some(row)) => row.get(qc.column_index).cloned(),
            _ => None,
          },
          Aggregate::Count(None) => None,
        };
        entry[i].update(agg, val);
      }
    }

    Ok(groups)
  }

  fn finish_group_rows(
    &self,
    groups: HashMap<EngineKey, Vec<AggState>>,
  ) -> Result<Vec<EngineRow>, EngineError> {
    let mut out_rows: Vec<EngineRow> = Vec::with_capacity(groups.len());
    for (key_bytes, agg_states) in groups {
      let key_vals = <DefaultEncoding as KeyEncoding>::decode_values(&key_bytes)
        .map_err(|e| EngineError::SchemaMismatch(format!("decode group key: {}", e)))?;
      let mut row: EngineRow = key_vals;
      for st in agg_states {
        row.push(st.finish());
      }
      out_rows.push(row);
    }
    Ok(out_rows)
  }

  fn apply_having(&self, mut out_rows: Vec<EngineRow>) -> Vec<EngineRow> {
    if let Some(having) = &self.having {
      out_rows.retain(|r| {
        let ctx = GroupRowContext {
          row: r,
          group_by: &self.group_by,
        };
        eval_having_predicate(having, &ctx)
      });
    }
    out_rows
  }

  fn apply_order_by(&self, out_rows: &mut [EngineRow]) -> Result<(), EngineError> {
    if self.order_by.is_empty() {
      return Ok(());
    }

    let mut orders_idx: Vec<(usize, SortDirection)> = Vec::new();
    for ord in &self.order_by {
      if let Some(pos) = self.group_by.iter().position(|gc| gc == &ord.expr) {
        orders_idx.push((pos, ord.direction.clone()));
        continue;
      }
      if let Some(pos) = self.aggregates.iter().position(|agg| match agg {
        Aggregate::Count(None) => false,
        Aggregate::Count(Some(qc)) => qc == &ord.expr,
        Aggregate::Sum(qc) => qc == &ord.expr,
        Aggregate::Min(qc) => qc == &ord.expr,
        Aggregate::Max(qc) => qc == &ord.expr,
        Aggregate::Avg(qc) => qc == &ord.expr,
      }) {
        orders_idx.push((self.group_by.len() + pos, ord.direction.clone()));
        continue;
      }
      return Err(EngineError::SchemaMismatch(
        "ORDER BY references unknown group or aggregate column".into(),
      ));
    }

    out_rows.sort_by(|a, b| {
      for (idx, dir) in &orders_idx {
        let av = a.get(*idx).unwrap_or(&EngineValue::Null);
        let bv = b.get(*idx).unwrap_or(&EngineValue::Null);
        let cmp = av.cmp(bv);
        let cmp = match dir {
          SortDirection::Asc => cmp,
          SortDirection::Desc => cmp.reverse(),
        };
        if cmp != Ordering::Equal {
          return cmp;
        }
      }
      Ordering::Equal
    });

    Ok(())
  }

  fn apply_limit_offset(&self, rows: Vec<EngineRow>) -> Vec<EngineRow> {
    match self.limit {
      Some(limit) => rows
        .into_iter()
        .skip(self.offset.unwrap_or(0))
        .take(limit)
        .collect(),
      None => rows.into_iter().skip(self.offset.unwrap_or(0)).collect(),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use alloc::string::ToString;

  fn make_partial(table: &str, row: EngineRow) -> PartialRow {
    let mut m = HashMap::new();
    m.insert(table.to_string(), Some(row));
    m
  }

  fn col(index: usize) -> QualifiedColumn {
    QualifiedColumn {
      table: "t".into(),
      column_index: index,
    }
  }

  fn aggregate_rows(aggregates: Vec<Aggregate>, input: Vec<PartialRow>) -> Vec<EngineRow> {
    Aggregator::new(vec![], aggregates, None, vec![], None, None, input)
      .execute()
      .unwrap()
  }

  #[test]
  fn count_star_all_rows() {
    let input = vec![
      make_partial("t", vec![EngineValue::Integer(1)]),
      make_partial("t", vec![EngineValue::Integer(2)]),
      make_partial("t", vec![EngineValue::Integer(3)]),
    ];
    let rows = aggregate_rows(vec![Aggregate::Count(None)], input);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], EngineValue::Integer(3));
  }

  #[test]
  fn sum_column() {
    let input = vec![
      make_partial("t", vec![EngineValue::Integer(10)]),
      make_partial("t", vec![EngineValue::Integer(20)]),
      make_partial("t", vec![EngineValue::Integer(30)]),
    ];
    let rows = aggregate_rows(vec![Aggregate::Sum(col(0))], input);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], EngineValue::Float(60.0));
  }

  #[test]
  fn avg_column() {
    let input = vec![
      make_partial("t", vec![EngineValue::Integer(10)]),
      make_partial("t", vec![EngineValue::Integer(20)]),
    ];
    let rows = aggregate_rows(vec![Aggregate::Avg(col(0))], input);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], EngineValue::Float(15.0));
  }

  #[test]
  fn min_and_max() {
    let col_min = QualifiedColumn {
      table: "t".into(),
      column_index: 0,
    };
    let col_max = QualifiedColumn {
      table: "t".into(),
      column_index: 0,
    };
    let input = vec![
      make_partial("t", vec![EngineValue::Integer(5)]),
      make_partial("t", vec![EngineValue::Integer(1)]),
      make_partial("t", vec![EngineValue::Integer(9)]),
    ];
    let agg = Aggregator::new(
      vec![],
      vec![Aggregate::Min(col_min), Aggregate::Max(col_max)],
      None,
      vec![],
      None,
      None,
      input,
    );
    let rows = agg.execute().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], EngineValue::Integer(1));
    assert_eq!(rows[0][1], EngineValue::Integer(9));
  }

  #[test]
  fn group_by_two_groups() {
    let group_col = QualifiedColumn {
      table: "t".into(),
      column_index: 0,
    };
    let count_col = QualifiedColumn {
      table: "t".into(),
      column_index: 1,
    };
    let input = vec![
      make_partial(
        "t",
        vec![EngineValue::Text("a".into()), EngineValue::Integer(1)],
      ),
      make_partial(
        "t",
        vec![EngineValue::Text("a".into()), EngineValue::Integer(2)],
      ),
      make_partial(
        "t",
        vec![EngineValue::Text("b".into()), EngineValue::Integer(3)],
      ),
    ];
    let agg = Aggregator::new(
      vec![group_col],
      vec![Aggregate::Count(Some(count_col))],
      None,
      vec![],
      None,
      None,
      input,
    );
    let mut rows = agg.execute().unwrap();
    rows.sort_by_key(|r| match &r[0] {
      EngineValue::Text(s) => s.clone(),
      _ => String::new(),
    });
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], EngineValue::Text("a".into()));
    assert_eq!(rows[0][1], EngineValue::Integer(2));
    assert_eq!(rows[1][0], EngineValue::Text("b".into()));
    assert_eq!(rows[1][1], EngineValue::Integer(1));
  }

  #[test]
  fn having_filters_groups() {
    use crate::query::{HavingPredicate, RefOrAgg};

    let group_col = QualifiedColumn {
      table: "t".into(),
      column_index: 0,
    };
    let input = vec![
      make_partial("t", vec![EngineValue::Text("a".into())]),
      make_partial("t", vec![EngineValue::Text("a".into())]),
      make_partial("t", vec![EngineValue::Text("b".into())]),
    ];
    let having = HavingPredicate::GreaterThan(RefOrAgg::AggregateIndex(0), EngineValue::Integer(1));
    let agg = Aggregator::new(
      vec![group_col],
      vec![Aggregate::Count(None)],
      Some(having),
      vec![],
      None,
      None,
      input,
    );
    let rows = agg.execute().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], EngineValue::Text("a".into()));
    assert_eq!(rows[0][1], EngineValue::Integer(2));
  }

  #[test]
  fn limit_and_offset() {
    let input = vec![
      make_partial("t", vec![EngineValue::Integer(1)]),
      make_partial("t", vec![EngineValue::Integer(2)]),
      make_partial("t", vec![EngineValue::Integer(3)]),
      make_partial("t", vec![EngineValue::Integer(4)]),
      make_partial("t", vec![EngineValue::Integer(5)]),
    ];
    let agg = Aggregator::new(
      vec![],
      vec![Aggregate::Count(None)],
      None,
      vec![],
      Some(1),
      Some(0),
      input,
    );
    let rows = agg.execute().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], EngineValue::Integer(5));
  }
}
