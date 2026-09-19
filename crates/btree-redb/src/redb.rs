use btree::{BTreeKey, BTreeValue};
use redb::{Key as RedbKeyTrait, Value as RedbValueTrait};

use crate::{key::Key, value::Value};

pub trait RedbKey: BTreeKey + Clone + RedbKeyTrait + 'static
where
    for<'a> Self: RedbValueTrait<SelfType<'a> = Self>,
{
}

impl<T> RedbKey for T
where
    T: BTreeKey + Clone + RedbKeyTrait + 'static,
    for<'a> T: RedbValueTrait<SelfType<'a> = T>,
{
}

pub trait RedbValue: BTreeValue + RedbValueTrait + 'static
where
    for<'a> Self: RedbValueTrait<SelfType<'a> = Self>,
{
}

impl<T> RedbValue for T
where
    T: BTreeValue + RedbValueTrait + 'static,
    for<'a> T: RedbValueTrait<SelfType<'a> = T>,
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
