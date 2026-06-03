use std::{borrow::Borrow, collections::BTreeMap, marker::PhantomData, sync::Arc};

use async_stream::stream;
use core::ops::Bound;
use db_engine::BTreeDefinition;
use futures::Stream;
use std::ops::RangeBounds;

use async_lock::RwLock;
use db_engine::{
  BTreeError, BTreeKey, BTreeReadExecutor, BTreeResult, BTreeTransaction, BTreeValue,
  BTreeWriteExecutor, MaybeSend,
};
use postcard::{from_bytes, to_stdvec};
use redb::{ReadableDatabase, ReadableTable, TableDefinition};

#[derive(Debug, Clone)]
pub enum TransactionPatchEntry<V> {
  Present(V),
  Deleted,
}

#[derive(Debug, Clone)]
pub struct TransactionPatch<K, V>(pub BTreeMap<K, TransactionPatchEntry<V>>);

impl<K, V> Default for TransactionPatch<K, V> {
  fn default() -> Self {
    TransactionPatch(BTreeMap::new())
  }
}

pub struct RedbTransaction<K, V> {
  pub(crate) db: Arc<redb::Database>,
  pub(crate) id: String,
  pub(crate) table_def: TableDefinition<'static, &'static [u8], &'static [u8]>,
  pub(crate) write: bool,
  pub(crate) patch: Arc<RwLock<TransactionPatch<K, V>>>,
  _k: PhantomData<K>,
  _v: PhantomData<V>,
}

impl<K, V> RedbTransaction<K, V>
where
  K: BTreeKey,
  V: BTreeValue,
{
  pub fn new_read(
    db: Arc<redb::Database>,
    id: &str,
    table_def: TableDefinition<'static, &'static [u8], &'static [u8]>,
  ) -> Self {
    Self {
      db,
      id: id.to_string(),
      table_def,
      write: false,
      patch: Arc::new(RwLock::new(TransactionPatch::default())),
      _k: PhantomData,
      _v: PhantomData,
    }
  }

  pub fn new_write(
    db: Arc<redb::Database>,
    id: &str,
    table_def: TableDefinition<'static, &'static [u8], &'static [u8]>,
  ) -> Result<Self, BTreeError> {
    // validate we can start a write transaction
    let _ = db.begin_write().map_err(BTreeError::other)?;
    Ok(Self {
      db,
      id: id.to_string(),
      table_def,
      write: true,
      patch: Arc::new(RwLock::new(TransactionPatch::default())),
      _k: PhantomData,
      _v: PhantomData,
    })
  }
}

impl<K, V> BTreeTransaction<K, V> for RedbTransaction<K, V>
where
  K: BTreeKey,
  V: BTreeValue,
{
  fn commit(self) -> impl core::future::Future<Output = BTreeResult<()>>
  where
    Self: Sized,
  {
    async move {
      if !self.write {
        return Ok(());
      }

      // take ownership of the patch contents
      let patch_map = {
        let mut guard = self.patch.write().await;
        std::mem::take(&mut guard.0)
      };

      if patch_map.is_empty() {
        return Ok(());
      }

      let db = self.db.clone();
      let table_def = self.table_def;
      let id = self.id.clone();

      let wt = db.begin_write().map_err(BTreeError::other)?;
      let mut table = wt.open_table(table_def).map_err(BTreeError::other)?;

      for (k, entry) in patch_map {
        let key_bin = to_stdvec(&k)
          .map_err(|e| BTreeError::Custom(format!("postcard key ser error: {}", e)))?;
        let mut full_key = id.as_bytes().to_vec();
        full_key.push(0u8);
        full_key.extend_from_slice(&key_bin);

        match entry {
          TransactionPatchEntry::Present(v) => {
            let val_bin = crate::encode_value(&v)?;
            table
              .insert(full_key.as_slice(), val_bin.as_slice())
              .map_err(BTreeError::other)?;
          }
          TransactionPatchEntry::Deleted => {
            let _ = table
              .remove(full_key.as_slice())
              .map_err(BTreeError::other)?;
          }
        }
      }

      drop(table);
      wt.commit().map_err(BTreeError::other)?;
      Ok(())
    }
  }

  fn rollback(self) -> impl core::future::Future<Output = BTreeResult<()>>
  where
    Self: Sized,
  {
    async move {
      // discard the patch
      let _ = self.patch.write().await;
      Ok(())
    }
  }
}

impl<K, V> BTreeReadExecutor<K, V> for RedbTransaction<K, V>
where
  K: BTreeKey,
  V: BTreeValue,
{
  async fn get<'a, Q>(&'a self, key: Q) -> BTreeResult<Option<V>>
  where
    K: Ord,
    Q: core::borrow::Borrow<K> + MaybeSend + 'a,
  {
    // Check in-memory patch first
    {
      let guard = self.patch.read().await;
      if let Some(entry) = guard.0.get(key.borrow()) {
        match entry {
          TransactionPatchEntry::Present(v) => return Ok(Some(v.clone())),
          TransactionPatchEntry::Deleted => return Ok(None),
        }
      }
    }

    // Fallback to persistent storage
    let key_bin = to_stdvec(key.borrow())
      .map_err(|e| BTreeError::Custom(format!("postcard key ser error: {}", e)))?;
    let rt = self.db.begin_read().map_err(BTreeError::other)?;
    let table = rt.open_table(self.table_def).map_err(BTreeError::other)?;
    let mut full_key = self.id.as_bytes().to_vec();
    full_key.push(0u8);
    full_key.extend_from_slice(&key_bin);

    match table.get(full_key.as_slice()).map_err(BTreeError::other)? {
      Some(val) => Ok(Some(crate::decode_value(val.value())?)),
      None => Ok(None),
    }
  }

  fn range<'a, R>(&'a self, range: R) -> impl Stream<Item = BTreeResult<(K, V)>> + 'a
  where
    K: Ord + Clone,
    R: core::ops::RangeBounds<K> + MaybeSend + 'a,
  {
    let table_def = self.table_def;
    let db = self.db.clone();
    let id = self.id.clone();

    stream! {
      // Snapshot patch
      let patch_snapshot = self.patch.read().await.clone();

      let rt = db.begin_read().map_err(BTreeError::other)?;
      let table = rt.open_table(table_def).map_err(BTreeError::other)?;

      let mut prefix = id.as_bytes().to_vec();
      prefix.push(0u8);

      let iter = table.iter().map_err(BTreeError::other)?;

      // Base map collects persisted entries within range that are not deleted by the patch
      let mut merged: BTreeMap<K, V> = BTreeMap::new();

      for item in iter {
        let (key_guard, val_guard) = item.map_err(BTreeError::other)?;
        let key_bytes: &[u8] = key_guard.value();
        let val: &[u8] = val_guard.value();
        if !key_bytes.starts_with(&prefix) {
          continue;
        }
        let user_key_bytes = &key_bytes[prefix.len()..];
        let k: K = from_bytes(user_key_bytes).map_err(|e| BTreeError::Custom(format!("postcard key de error: {}", e)))?;

        // Range check
        let mut in_range = true;
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

        // If patch marks this key as deleted, skip; if patch has a present override, skip (it will be applied later)
        match patch_snapshot.0.get(&k) {
          Some(TransactionPatchEntry::Deleted) => continue,
          Some(TransactionPatchEntry::Present(_)) => continue,
          None => {
            let v: V = crate::decode_value(val)?;
            merged.insert(k, v);
          }
        }
      }

      // Apply patch entries (present/deleted) that are within range
      for (k, entry) in patch_snapshot.0.into_iter() {
        // Range check
        let mut in_range = true;
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

        match entry {
          TransactionPatchEntry::Present(v) => {
            merged.insert(k, v);
          }
          TransactionPatchEntry::Deleted => {
            merged.remove(&k);
          }
        }
      }

      // Yield merged entries in order
      for (k, v) in merged {
        yield Ok((k, v));
      }
    }
  }
}

impl<K, V> BTreeWriteExecutor<K, V> for RedbTransaction<K, V>
where
  K: BTreeKey,
  V: BTreeValue,
{
  async fn insert<'a>(&'a mut self, key: K, value: V) -> BTreeResult<()>
  where
    K: Ord,
  {
    if self.write {
      let mut guard = self.patch.write().await;
      guard.0.insert(key, TransactionPatchEntry::Present(value));
      Ok(())
    } else {
      Err(BTreeError::UnsupportedOperation)
    }
  }

  async fn remove<'a, Q>(&'a mut self, key: Q) -> BTreeResult<Option<V>>
  where
    K: Ord + Clone,
    Q: core::borrow::Borrow<K> + MaybeSend + 'a,
  {
    if self.write {
      // Fast-path if patch already contains entry
      let mut guard = self.patch.write().await;
      let key_owned = key.borrow().clone();

      match guard.0.get(&key_owned) {
        Some(TransactionPatchEntry::Present(v)) => {
          let removed = v.clone();
          guard.0.insert(key_owned, TransactionPatchEntry::Deleted);
          return Ok(Some(removed));
        }
        Some(TransactionPatchEntry::Deleted) => return Ok(None),
        None => {}
      }

      // Not in patch; read from DB to find previous value (if any) and record deletion in patch
      let key_bin = to_stdvec(&key_owned)
        .map_err(|e| BTreeError::Custom(format!("postcard key ser error: {}", e)))?;
      let rt = self.db.begin_read().map_err(BTreeError::other)?;
      let table = rt.open_table(self.table_def).map_err(BTreeError::other)?;
      let mut full_key = self.id.as_bytes().to_vec();
      full_key.push(0u8);
      full_key.extend_from_slice(&key_bin);

      let prev = match table.get(full_key.as_slice()).map_err(BTreeError::other)? {
        Some(removed_guard) => {
          let v = crate::decode_value::<V>(removed_guard.value())?;
          Some(v)
        }
        None => None,
      };

      guard.0.insert(key_owned, TransactionPatchEntry::Deleted);
      Ok(prev)
    } else {
      Err(BTreeError::UnsupportedOperation)
    }
  }
}
