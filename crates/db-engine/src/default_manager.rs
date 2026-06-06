#[cfg(not(feature = "std"))]
use alloc::{
  boxed::Box,
  collections::BTreeMap,
  string::{String, ToString},
  sync::Arc,
};
#[cfg(feature = "std")]
use std::{collections::BTreeMap, sync::Arc};

use async_lock::RwLock;
use core::any::Any;

use crate::{
  BTreeDefinition, BTreeError, BTreeManager, BTreeResult,
  btree::{BTreeFactory, BTreeManagerAnyBTree},
};

use crate::btree::{BTreeKey, BTreeValue};

pub struct DefaultBTreeManager<F> {
  // TODO: find a concurrent hashmap that not supports no_std envs
  inner: Arc<RwLock<BTreeMap<String, Box<dyn BTreeManagerAnyBTree>>>>,
  factory: Arc<F>,
}

impl<F> DefaultBTreeManager<F> {
  pub fn new(factory: F) -> Self {
    Self {
      inner: Arc::new(RwLock::new(BTreeMap::new())),
      factory: Arc::new(factory),
    }
  }
}

impl<F> Default for DefaultBTreeManager<F>
where
  F: Default,
{
  fn default() -> Self {
    Self::new(Default::default())
  }
}

#[cfg(feature = "in-memory")]
impl DefaultBTreeManager<crate::InMemoryBTreeFactory> {
  pub fn with_in_memory_factory() -> Self {
    Self::new(crate::InMemoryBTreeFactory::new())
  }
}

impl<F> BTreeManager<F> for DefaultBTreeManager<F>
where
  F: BTreeFactory,
{
  async fn entry<D>(&self, definition: &D) -> BTreeResult<F::BTree<D::Key, D::Value>>
  where
    D: BTreeDefinition,
    <D as BTreeDefinition>::Key: BTreeKey,
    <D as BTreeDefinition>::Value: BTreeValue,
  {
    if let Some(btree_box) = self.inner.read().await.get(definition.id()) {
      let btree_any = btree_box.as_ref() as &dyn Any;

      if let Some(typed) = btree_any.downcast_ref::<F::BTree<D::Key, D::Value>>() {
        Ok(typed.clone())
      } else {
        Err(BTreeError::TypeMismatch)
      }
    } else {
      let btree = self.factory.create(definition).await?;
      let btree_any: Box<dyn BTreeManagerAnyBTree> = Box::new(btree.clone());

      self
        .inner
        .write()
        .await
        .insert(definition.id().to_string(), btree_any);

      Ok(btree)
    }
  }

  async fn remove<D>(&self, definition: &D) -> BTreeResult<()>
  where
    D: BTreeDefinition,
    <D as BTreeDefinition>::Key: BTreeKey,
    <D as BTreeDefinition>::Value: BTreeValue,
  {
    self.inner.write().await.remove(definition.id());
    Ok(())
  }
}

#[cfg(all(test, feature = "in-memory"))]
mod tests {
  use super::*;
  use futures::executor::block_on;

  use crate::btree::{
    BTree, BTreeManager, BTreeReadExecutor, BTreeTransaction, BTreeWriteExecutor,
  };
  use crate::{BTreeDefinition, DefaultBTreeManager};

  #[derive(Clone)]
  struct TestDefinition(String);

  impl BTreeDefinition for TestDefinition {
    type Key = i32;
    type Value = i32;

    fn id(&self) -> &str {
      &self.0
    }
  }

  #[test]
  fn entry_reuses_existing_tree_for_same_definition_types() {
    let manager = DefaultBTreeManager::with_in_memory_factory();
    let definition = TestDefinition("test-tree".to_string());

    let btree1 = block_on(manager.entry(&definition)).expect("first entry");
    let btree2 = block_on(manager.entry(&definition)).expect("second entry");

    let mut tx = block_on(btree1.transaction()).expect("transaction");
    block_on(tx.insert(1, 42)).expect("insert");
    block_on(tx.commit()).expect("commit");

    let got = block_on(btree2.get(1)).expect("get");
    assert_eq!(got, Some(42));
  }
}
