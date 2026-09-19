use btree::{BTreeKey, BTreeValue};

use crate::{key::Key, value::Value};

pub trait RedbKey: BTreeKey + Clone + reconverge::Key + 'static
where
    for<'a> Self: reconverge::Value<SelfType<'a> = Self>,
{
}

impl<T> RedbKey for T
where
    T: BTreeKey + Clone + reconverge::Key + 'static,
    for<'a> T: reconverge::Value<SelfType<'a> = T>,
{
}

pub trait RedbValue: BTreeValue + reconverge::Value + 'static
where
    for<'a> Self: reconverge::Value<SelfType<'a> = Self>,
{
}

impl<T> RedbValue for T
where
    T: BTreeValue + reconverge::Value + 'static,
    for<'a> T: reconverge::Value<SelfType<'a> = T>,
{
}

pub fn table_definition<'a, 'b: 'a, K, V>(
    name: &'b str,
) -> reconverge::TableDefinition<'a, Key<K>, Value<V>>
where
    K: RedbKey + 'a,
    V: RedbValue + 'a,
{
    reconverge::TableDefinition::new(name)
}
