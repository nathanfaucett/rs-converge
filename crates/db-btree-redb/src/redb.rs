use db_btree::{BTreeKey, BTreeValue};

use crate::{key::Key, value::Value};

pub trait RedbKey: BTreeKey + Clone + redb::Key + 'static
where
    for<'a> Self: redb::Value<SelfType<'a> = Self>,
{
}

impl<T> RedbKey for T
where
    T: BTreeKey + Clone + redb::Key + 'static,
    for<'a> T: redb::Value<SelfType<'a> = T>,
{
}

pub trait RedbValue: BTreeValue + redb::Value + 'static
where
    for<'a> Self: redb::Value<SelfType<'a> = Self>,
{
}

impl<T> RedbValue for T
where
    T: BTreeValue + redb::Value + 'static,
    for<'a> T: redb::Value<SelfType<'a> = T>,
{
}

pub fn table_definition<'a, 'b: 'a, K, V>(
    name: &'b str,
) -> redb::TableDefinition<'a, Key<K>, Value<V>>
where
    K: RedbKey + 'a,
    V: RedbValue + 'a,
{
    redb::TableDefinition::new(name)
}
