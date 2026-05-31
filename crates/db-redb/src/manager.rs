use std::sync::Arc;

use dashmap::DashMap;

use db_engine::{
  BTreeDefinition, BTreeKey, BTreeManager, BTreeResult, BTreeValue, MaybeSend, MaybeSync,
};

use crate::btree::RedbBTree;
use redb::TableDefinition;

trait RedbBTreeManagerValue: core::any::Any + MaybeSend + MaybeSync + 'static {}
impl<T> RedbBTreeManagerValue for T where T: core::any::Any + MaybeSend + MaybeSync + 'static {}

pub struct RedbBTreeManager {
  db: Arc<redb::Database>,
  inner: DashMap<String, Box<dyn RedbBTreeManagerValue>>,
  names: DashMap<String, &'static str>,
}

impl RedbBTreeManager {
  pub fn new(db: redb::Database) -> Self {
    Self {
      db: Arc::new(db),
      inner: DashMap::new(),
      names: DashMap::new(),
    }
  }

  fn intern_name(&self, id: &str) -> &'static str {
    if let Some(entry) = self.names.get(id) {
      return *entry.value();
    }

    // Create a per-definition table name and leak it once. We store the
    // leaked &'static str in `names` to guarantee deduplication for the same
    // id.
    let name = format!("aicacia_btree_{}", id);
    let static_name: &'static str = Box::leak(name.into_boxed_str());
    self.names.insert(id.to_string(), static_name);
    static_name
  }
}

impl BTreeManager for RedbBTreeManager {
  type BTree<K, V>
    = RedbBTree<K, V>
  where
    K: BTreeKey + serde::Serialize + serde::de::DeserializeOwned,
    V: BTreeValue + serde::Serialize + serde::de::DeserializeOwned;

  async fn get<D>(&self, definition: &D) -> BTreeResult<Self::BTree<D::Key, D::Value>>
  where
    D: BTreeDefinition,
  {
    let id = definition.id().to_string();
    let db = self.db.clone();
    let inner = &self.inner;
    let names = &self.names;

    if let Some(entry) = inner.get(&id) {
      let any_ref = entry.value().as_ref() as &dyn core::any::Any;
      if let Some(typed) = any_ref.downcast_ref::<RedbBTree<D::Key, D::Value>>() {
        return Ok(typed.clone());
      } else {
        return Err(db_engine::BTreeError::TypeMismatch);
      }
    }

    // Intern the table name (creates/returns a &'static str)
    let static_name: &'static str = if let Some(entry) = names.get(&id) {
      *entry.value()
    } else {
      // We can't call self.intern_name() from async move because `self`
      // isn't moved into the async block. Recreate the name here and
      // perform a similar intern-insert as above.
      let name = format!("aicacia_btree_{}", id);
      let static_name: &'static str = Box::leak(name.into_boxed_str());
      names.insert(id.clone(), static_name);
      static_name
    };

    let table_def = TableDefinition::<&'static [u8], &'static [u8]>::new(static_name);

    let btree = RedbBTree::<D::Key, D::Value>::new(db.clone(), &id, table_def.clone());
    let boxed: Box<dyn RedbBTreeManagerValue> = Box::new(btree.clone());
    inner.insert(id.clone(), boxed);

    let entry = inner.get(&id).unwrap();
    let any_ref = entry.value().as_ref() as &dyn core::any::Any;
    if let Some(typed) = any_ref.downcast_ref::<RedbBTree<D::Key, D::Value>>() {
      Ok(typed.clone())
    } else {
      Err(db_engine::BTreeError::TypeMismatch)
    }
  }

  async fn insert<D>(&self, definition: &D) -> BTreeResult<Self::BTree<D::Key, D::Value>>
  where
    D: BTreeDefinition,
  {
    self.get(definition).await
  }

  async fn remove<D>(&self, definition: &D) -> BTreeResult<()>
  where
    D: BTreeDefinition,
  {
    let id = definition.id().to_string();
    let inner = &self.inner;
    inner.remove(&id);
    Ok(())
  }
}
