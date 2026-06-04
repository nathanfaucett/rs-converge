use std::{borrow::Borrow, marker::PhantomData, sync::Arc};

use async_stream::stream;
use std::ops::RangeBounds;

use db_engine::{
  BTree, BTreeKey, BTreeReadExecutor, BTreeResult, BTreeValue, BTreeWriteExecutor, MaybeSend,
  MaybeSendStream,
};

use crate::transaction::RedbTransaction;
use redb::{ReadTransaction, ReadableDatabase, ReadableTable, TableDefinition};

fn is_table_missing(error: &redb::TableError) -> bool {
  matches!(error, redb::TableError::TableDoesNotExist(_))
}

pub struct RedbBTree<K, V> {
  pub(crate) db: Arc<redb::Database>,
  pub(crate) id: String,
  pub(crate) table_def: TableDefinition<'static, &'static [u8], &'static [u8]>,
  _marker: PhantomData<(K, V)>,
}

impl<K, V> Clone for RedbBTree<K, V> {
  fn clone(&self) -> Self {
    Self {
      db: self.db.clone(),
      id: self.id.clone(),
      table_def: self.table_def,
      _marker: PhantomData,
    }
  }
}

impl<K, V> RedbBTree<K, V> {
  pub fn new(
    db: Arc<redb::Database>,
    id: &str,
    table_def: TableDefinition<'static, &'static [u8], &'static [u8]>,
  ) -> Self {
    Self {
      db,
      id: id.to_string(),
      table_def,
      _marker: PhantomData,
    }
  }
}

impl<K, V> BTreeReadExecutor<K, V> for RedbBTree<K, V>
where
  K: BTreeKey,
  V: BTreeValue,
{
  async fn get<'a, Q>(&'a self, key: Q) -> BTreeResult<Option<V>>
  where
    K: Ord,
    Q: Borrow<K> + MaybeSend + 'a,
  {
    let key_ref = key.borrow();
    let key_bin = key_ref.encode()?;

    let rt: ReadTransaction = self.db.begin_read().map_err(db_engine::BTreeError::other)?;
    let table = match rt.open_table(self.table_def) {
      Ok(table) => table,
      Err(error) if is_table_missing(&error) => return Ok(None),
      Err(error) => return Err(db_engine::BTreeError::other(error)),
    };
    let mut full_key = self.id.as_bytes().to_vec();
    full_key.push(0u8);
    full_key.extend_from_slice(&key_bin);

    match table
      .get(full_key.as_slice())
      .map_err(db_engine::BTreeError::other)?
    {
      Some(val_guard) => {
        let v = V::decode(val_guard.value())?;
        Ok(Some(v))
      }
      None => Ok(None),
    }
  }

  fn range<'a, R>(&'a self, range: R) -> impl MaybeSendStream<Item = BTreeResult<(K, V)>> + 'a
  where
    K: Ord,
    R: RangeBounds<K> + MaybeSend + 'a,
  {
    let db = self.db.clone();
    let id = self.id.clone();
    let table_def = self.table_def;

    stream! {
      let rt = db.begin_read().map_err(db_engine::BTreeError::other)?;
      let table = match rt.open_table(table_def) {
        Ok(table) => table,
        Err(error) if is_table_missing(&error) => {
          return;
        }
        Err(error) => {
          yield Err(db_engine::BTreeError::other(error));
          return;
        }
      };

      let mut prefix = id.into_bytes();
      prefix.push(0u8);

      let iter = table.iter().map_err(db_engine::BTreeError::other)?;
      for item in iter {
        let (key_guard, val_guard) = item.map_err(db_engine::BTreeError::other)?;
        let key_bytes: &[u8] = key_guard.value();
        let val: &[u8] = val_guard.value();
        if !key_bytes.starts_with(&prefix) {
          continue;
        }
        let user_key_bytes = &key_bytes[prefix.len()..];
        let k: K = K::decode(user_key_bytes)?;

        let mut in_range = true;
        use core::ops::Bound;
        match range.start_bound() {
          Bound::Included(s) => { if k < *s { in_range = false; } }
          Bound::Excluded(s) => { if k <= *s { in_range = false; } }
          Bound::Unbounded => {}
        }
        match range.end_bound() {
          Bound::Included(e) => { if k > *e { in_range = false; } }
          Bound::Excluded(e) => { if k >= *e { in_range = false; } }
          Bound::Unbounded => {}
        }

        if !in_range { continue; }

        let v: V = V::decode(val)?;
        yield Ok((k, v));
      }
    }
  }
}

impl<K, V> BTreeWriteExecutor<K, V> for RedbBTree<K, V>
where
  K: BTreeKey,
  V: BTreeValue,
{
  async fn insert<'a>(&'a mut self, key: K, value: V) -> BTreeResult<()>
  where
    K: Ord,
  {
    let wt = self
      .db
      .begin_write()
      .map_err(db_engine::BTreeError::other)?;
    let mut table = wt
      .open_table(self.table_def)
      .map_err(db_engine::BTreeError::other)?;

    let key_bin = key.encode()?;
    let mut full_key = self.id.as_bytes().to_vec();
    full_key.push(0u8);
    full_key.extend_from_slice(&key_bin);
    let val_bin = value.encode()?;

    table
      .insert(full_key.as_slice(), val_bin.as_slice())
      .map_err(db_engine::BTreeError::other)?;

    drop(table);
    wt.commit().map_err(db_engine::BTreeError::other)?;
    Ok(())
  }

  async fn remove<'a, Q>(&'a mut self, key: Q) -> BTreeResult<Option<V>>
  where
    K: Ord,
    Q: Borrow<K> + MaybeSend + 'a,
  {
    let wt = self
      .db
      .begin_write()
      .map_err(db_engine::BTreeError::other)?;
    let mut table = wt
      .open_table(self.table_def)
      .map_err(db_engine::BTreeError::other)?;

    let key_bin = key.borrow().encode()?;
    let mut full_key = self.id.as_bytes().to_vec();
    full_key.push(0u8);
    full_key.extend_from_slice(&key_bin);

    let prev = match table
      .remove(full_key.as_slice())
      .map_err(db_engine::BTreeError::other)?
    {
      Some(removed_guard) => {
        let v = V::decode(removed_guard.value())?;
        Some(v)
      }
      None => None,
    };

    drop(table);
    wt.commit().map_err(db_engine::BTreeError::other)?;
    Ok(prev)
  }
}

impl<K, V> BTree<K, V> for RedbBTree<K, V>
where
  K: BTreeKey,
  V: BTreeValue,
{
  type Transaction = RedbTransaction<K, V>;

  async fn transaction<'a>(&'a self) -> BTreeResult<Self::Transaction> {
    RedbTransaction::new_write(self.db.clone(), &self.id, self.table_def)
  }
}
