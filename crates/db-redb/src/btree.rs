use std::{borrow::Borrow, marker::PhantomData, ops::RangeBounds, sync::Arc};

use async_stream::stream;
use automerge::AutoCommit;
use futures::{Stream, StreamExt};
use redb::{Database, ReadableDatabase, TableDefinition};
use uuid::Uuid;

use db_btree::{BTreeError, BTreeKey, BTreeReadExecutor, BTreeResult, BTreeValue};
use db_core::MaybeSend;

#[derive(Clone)]
pub struct RedbBTree<K, V> {
  db: Arc<Database>,
  table_definition: TableDefinition<&'_ [u8], &'_ [u8]>,
  _phantom_marker: PhantomData<(K, V)>,
}

impl<K, V> RedbBTree<K, V> {
  pub fn new(db: Database, table_definition: TableDefinition<&'_ [u8], &'_ [u8]>) -> Self {
    Self {
      db: Arc::new(db),
      table_definition: table_definition,
      _phantom_marker: PhantomData,
    }
  }
}

impl<K, V> BTreeReadExecutor<Uuid, AutoCommit> for RedbBTree<K, V>
where
  K: BTreeKey,
  V: BTreeValue,
{
  async fn get<'a, Q>(&'a self, key: Q) -> BTreeResult<Option<AutoCommit>>
  where
    Q: Borrow<Uuid> + MaybeSend + 'a,
  {
    let tx = self.db.begin_read().map_err(BTreeError::custom)?;
    let table = tx
      .open_table(self.table_definition)
      .map_err(BTreeError::custom)?;

    let value = table.get(key.borrow().as_bytes())?.map(|value| {
      AutoCommit::load(value.value())
        .map_err(|e| BTreeError::custom(format!("Failed to load AutoCommit: {}", e)))
    });

    unimplemented!()
  }

  fn range<'a, R>(&'a self, range: R) -> impl Stream<Item = BTreeResult<(Uuid, AutoCommit)>> + 'a
  where
    R: RangeBounds<Uuid> + MaybeSend + 'a,
  {
    stream! {
        unimplemented!()
    }
  }
}
