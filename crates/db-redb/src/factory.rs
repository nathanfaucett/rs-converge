use std::sync::Arc;

use dashmap::DashMap;

use db_engine::{BTreeDefinition, BTreeFactory, BTreeKey, BTreeResult, BTreeValue};
use redb::TableDefinition;

use crate::RedbBTree;

pub struct RedbBTreeFactory {
  db: Arc<redb::Database>,
  names: DashMap<String, &'static str>,
}

impl RedbBTreeFactory {
  pub fn new(db: redb::Database) -> Self {
    Self {
      db: Arc::new(db),
      names: DashMap::new(),
    }
  }

  fn static_id(&self, id: &str) -> &'static str {
    if let Some(entry) = self.names.get(id) {
      return entry.value();
    }

    let static_id: &'static str = Box::leak(id.to_owned().into_boxed_str());
    self.names.insert(id.to_string(), static_id);
    static_id
  }
}

impl BTreeFactory for RedbBTreeFactory {
  type BTree<K, V>
    = RedbBTree<K, V>
  where
    K: BTreeKey,
    V: BTreeValue;

  async fn create<D>(&self, definition: &D) -> BTreeResult<Self::BTree<D::Key, D::Value>>
  where
    D: BTreeDefinition,
  {
    let static_id = self.static_id(definition.id());
    let table_def = TableDefinition::<&'static [u8], &'static [u8]>::new(static_id);
    let btree = RedbBTree::<D::Key, D::Value>::new(self.db.clone(), definition.id(), table_def);
    Ok(btree)
  }
}
