use std::{borrow::Borrow, ops::RangeBounds};

use async_stream::stream;
use automerge::AutoCommit;
use db_core::MaybeSend;
use db_core::{BTree, BTreeError, BTreeExecutor, BTreeTransaction};
use futures::{Stream, StreamExt};
use uuid::Uuid;

mod codec;
mod compaction;
mod hash;
mod key;
mod lifecycle;
mod reconstruction;
mod scan;
mod transaction;

use crate::automerge_btree::hash::hash_heads;

use self::codec::encode_doc_key_range;
#[cfg(test)]
use self::codec::uuid_prefix_range;
pub use self::codec::{DocumentChangeKeyCodec, VecBytesCodec};
use self::compaction::{CompactionPolicy, ThresholdPolicy, run_compaction};
pub use self::key::{AutomergeEntry, DocumentChangeKey, DocumentType};
use self::key::{all_document_bounds, document_entry_bounds};
use self::lifecycle::build_lifecycle_write;
use self::scan::{
  ReconstructionAccumulator, collect_range_keys, flush_reconstructed_doc, load_document,
  scan_document_entries, uuid_in_range,
};
use self::transaction::AutomergeTransaction;

/// Backend-agnostic Automerge wrapper. Parameterized over any `BTree` backend.
pub struct AutomergeBTree<B> {
  inner: B,
  policy: Box<dyn CompactionPolicy>,
}

impl<B> AutomergeBTree<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  pub fn new(inner: B) -> Self {
    Self {
      inner,
      policy: Box::new(ThresholdPolicy::default()),
    }
  }

  pub fn with_compaction(
    inner: B,
    compaction_threshold_count: usize,
    compaction_threshold_bytes: usize,
  ) -> Self {
    Self {
      inner,
      policy: Box::new(ThresholdPolicy {
        threshold_count: compaction_threshold_count,
        threshold_bytes: compaction_threshold_bytes,
      }),
    }
  }

  pub fn new_with_policy(inner: B, policy: Box<dyn CompactionPolicy>) -> Self {
    Self { inner, policy }
  }

  /// Reconstruct the latest document state for `doc_id`.
  /// If the compaction policy triggers, perform inline compaction (atomic snapshot + cleanup).
  async fn get_document(&self, doc_id: Uuid) -> Option<Vec<u8>> {
    let (start, end) = document_entry_bounds(doc_id);

    let scan = scan_document_entries(self.inner.range(start.clone()..=end.clone())).await;

    if !scan.has_entries {
      return None;
    }

    let state = scan.accumulator.finish();

    if self
      .policy
      .should_compact(scan.delta_count, scan.delta_bytes)
    {
      match self.inner.transaction().await {
        Ok(tx) => {
          if let Err(err) =
            run_compaction(tx, start.clone(), end.clone(), doc_id, state.clone()).await
          {
            eprintln!("Automerge compaction failed: {err}");
          }
        }
        Err(err) => {
          eprintln!("Automerge compaction transaction failed: {err}");
        }
      }
    }

    Some(state)
  }

  async fn remove_document_keys_atomic(
    &mut self,
    start: DocumentChangeKey,
    end: DocumentChangeKey,
  ) -> Result<(), BTreeError> {
    let mut tx = self.inner.transaction().await?;
    let keys_to_remove = collect_range_keys(tx.range(start.clone()..=end.clone())).await?;
    for key in keys_to_remove {
      tx.remove(&key).await?;
    }
    tx.commit().await
  }

  async fn remove_document_keys_fallback(
    &mut self,
    start: DocumentChangeKey,
    end: DocumentChangeKey,
  ) -> Result<(), BTreeError> {
    let keys_to_remove = collect_range_keys(self.inner.range(start..=end)).await?;
    for key in keys_to_remove {
      let _ = self.inner.remove(&key).await;
    }
    Ok(())
  }
}

#[allow(clippy::needless_lifetimes)]
impl<B> BTreeExecutor<Uuid, AutoCommit> for AutomergeBTree<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  async fn get<'a, Q>(&'a self, key: Q) -> Result<Option<AutoCommit>, BTreeError>
  where
    Uuid: Ord,
    Q: Borrow<Uuid> + MaybeSend + 'a,
  {
    let doc_id = *key.borrow();

    match self.get_document(doc_id).await {
      None => Ok(None),
      Some(bytes) => load_document(&bytes).map(Some),
    }
  }

  async fn insert<'a>(&'a mut self, key: Uuid, value: AutoCommit) -> Result<(), BTreeError>
  where
    Uuid: Ord,
  {
    let existing = self.get(key).await?;
    if let Some((internal_key, bytes)) = build_lifecycle_write(key, value, existing) {
      self.inner.insert(internal_key, bytes).await
    } else {
      Ok(())
    }
  }

  async fn remove<'a, Q>(&'a mut self, key: Q) -> Result<Option<AutoCommit>, BTreeError>
  where
    Uuid: Ord,
    Q: Borrow<Uuid> + MaybeSend + 'a,
  {
    let doc_id = *key.borrow();
    let prev = self.get_document(doc_id).await;
    let (start, end) = document_entry_bounds(doc_id);

    if self
      .remove_document_keys_atomic(start.clone(), end.clone())
      .await
      .is_err()
    {
      let _ = self.remove_document_keys_fallback(start, end).await;
    }

    match prev {
      None => Ok(None),
      Some(bytes) => load_document(&bytes).map(Some),
    }
  }

  fn range<'a, R>(
    &'a self,
    range: R,
  ) -> impl Stream<Item = Result<(Uuid, AutoCommit), BTreeError>> + 'a
  where
    Uuid: Ord,
    R: RangeBounds<Uuid> + MaybeSend + 'a,
  {
    stream! {
      let (start_doc, end_doc) = all_document_bounds();

      let inner_stream = self.inner.range(start_doc.clone()..=end_doc.clone());
      futures::pin_mut!(inner_stream);

      let mut current_doc: Option<Uuid> = None;
      let mut accumulator = ReconstructionAccumulator::new();

      while let Some(item) = inner_stream.next().await {
        let (k, v) = item?;
        if !uuid_in_range(&range, &k.doc_id) {
          continue;
        }

        if current_doc.is_none() {
          current_doc = Some(k.doc_id);
        } else if current_doc.as_ref().expect("doc id present") != &k.doc_id {
          if let Some(item) = flush_reconstructed_doc(&mut current_doc, &mut accumulator) {
            match item {
              Ok(pair) => yield Ok(pair),
              Err(e) => yield Err(e),
            }
          }
          current_doc = Some(k.doc_id);
        }

        accumulator.apply(k.doc_type, v.clone());
      }

      if let Some(item) = flush_reconstructed_doc(&mut current_doc, &mut accumulator) {
        match item {
          Ok(pair) => yield Ok(pair),
          Err(e) => yield Err(e),
        }
      }
    }
  }
}

impl<B> BTree<Uuid, AutoCommit> for AutomergeBTree<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  type Transaction = AutomergeTransaction<B::Transaction>;

  async fn transaction(&self) -> Result<Self::Transaction, BTreeError> {
    let inner_tx = self.inner.transaction().await?;
    Ok(AutomergeTransaction::new(inner_tx))
  }
}

/// Encoded-backed Automerge wrapper: stores encoded keys/values in `B` and
/// uses provided codecs to convert to/from `DocumentChangeKey`/`AutomergeEntry`.
#[allow(dead_code)]
pub struct AutomergeBTreeEncoded<B, KC = DocumentChangeKeyCodec, VC = VecBytesCodec>
where
  B: BTree<Vec<u8>, Vec<u8>> + Clone + Send + Sync + 'static,
  KC: db_core::ValueCodec<DocumentChangeKey> + Clone + Send + Sync + 'static,
  VC: db_core::ValueCodec<AutomergeEntry> + Clone + Send + Sync + 'static,
{
  inner: B,
  key_codec: KC,
  val_codec: VC,
  compaction_threshold_count: usize,
  compaction_threshold_bytes: usize,
}

impl<B> AutomergeBTreeEncoded<B>
where
  B: BTree<Vec<u8>, Vec<u8>> + Clone + Send + Sync + 'static,
{
  pub fn new(inner: B) -> Self {
    Self::with_codecs_generic(inner, DocumentChangeKeyCodec, VecBytesCodec)
  }

  pub fn with_codecs_generic<KC, VC>(
    inner: B,
    key_codec: KC,
    val_codec: VC,
  ) -> AutomergeBTreeEncoded<B, KC, VC>
  where
    KC: db_core::KeyCodec<DocumentChangeKey> + Clone + Send + Sync + 'static,
    VC: db_core::ValueCodec<AutomergeEntry> + Clone + Send + Sync + 'static,
  {
    AutomergeBTreeEncoded {
      inner,
      key_codec,
      val_codec,
      compaction_threshold_count: 100,
      compaction_threshold_bytes: 1024 * 1024,
    }
  }

  #[allow(dead_code)]
  fn decode_key<KC2: db_core::ValueCodec<DocumentChangeKey>>(
    data: &[u8],
  ) -> Result<DocumentChangeKey, db_core::DecodeError> {
    <KC2 as db_core::ValueCodec<DocumentChangeKey>>::decode_checked(data)
  }
}

impl<B, KC, VC> AutomergeBTreeEncoded<B, KC, VC>
where
  B: BTree<Vec<u8>, Vec<u8>> + Clone + Send + Sync + 'static,
  KC: db_core::FastKeyCodec<DocumentChangeKey> + Clone + Send + Sync + 'static,
  VC: db_core::ValueCodec<AutomergeEntry> + Clone + Send + Sync + 'static,
{
  fn decode_entry_bytes(data: &[u8]) -> Vec<u8> {
    <VC as db_core::ValueCodec<AutomergeEntry>>::decode_checked(data)
      .unwrap_or_else(|_| data.to_vec())
  }

  fn doc_scan_bounds(&self, doc_id: Uuid) -> (Vec<u8>, Vec<u8>) {
    encode_doc_key_range(doc_id, &self.key_codec)
  }

  fn full_scan_bounds(&self) -> (Vec<u8>, Vec<u8>) {
    let (start_bound, end_bound) = all_document_bounds();
    let start = start_bound.doc_id;
    let end = end_bound.doc_id;
    let (start_enc, _) = encode_doc_key_range(start, &self.key_codec);
    let (_, end_enc) = encode_doc_key_range(end, &self.key_codec);
    (start_enc, end_enc)
  }

  fn decode_scanned_entry(k_enc: &[u8], entry_enc: &[u8]) -> Option<(DocumentChangeKey, Vec<u8>)> {
    let key = <KC as db_core::ValueCodec<DocumentChangeKey>>::decode_checked(k_enc).ok()?;
    Some((key, Self::decode_entry_bytes(entry_enc)))
  }

  async fn contains_identical_entry(
    &self,
    start_enc: Vec<u8>,
    end_enc: Vec<u8>,
    bytes: &[u8],
  ) -> Result<bool, BTreeError> {
    let stream = self.inner.range(start_enc..=end_enc);
    futures::pin_mut!(stream);
    while let Some(item) = stream.next().await {
      let (_k_enc, v_enc) = item?;
      if Self::decode_entry_bytes(&v_enc) == bytes {
        return Ok(true);
      }
    }
    Ok(false)
  }

  async fn collect_doc_keys(&self, doc_id: Uuid) -> Result<Vec<Vec<u8>>, BTreeError> {
    let (start_enc, end_enc) = self.doc_scan_bounds(doc_id);
    collect_range_keys(self.inner.range(start_enc..=end_enc)).await
  }

  async fn scan_document_state(
    &self,
    doc_id: Uuid,
  ) -> Result<(ReconstructionAccumulator, bool), BTreeError> {
    let (start_enc, end_enc) = self.doc_scan_bounds(doc_id);
    let scanned = scan_document_entries(self.inner.range(start_enc..=end_enc).filter_map(
      |item| async move {
        match item {
          Ok((k_enc, entry_enc)) => Self::decode_scanned_entry(&k_enc, &entry_enc).map(Ok),
          Err(_) => None,
        }
      },
    ))
    .await;

    Ok((scanned.accumulator, scanned.has_entries))
  }
}

#[allow(clippy::needless_lifetimes)]
impl<B, KC, VC> BTreeExecutor<Uuid, AutoCommit> for AutomergeBTreeEncoded<B, KC, VC>
where
  B: BTree<Vec<u8>, Vec<u8>> + Clone + Send + Sync + 'static,
  KC: db_core::FastKeyCodec<DocumentChangeKey> + Clone + Send + Sync + 'static,
  VC: db_core::ValueCodec<AutomergeEntry> + Clone + Send + Sync + 'static,
{
  async fn get<'a, Q>(&'a self, key: Q) -> Result<Option<AutoCommit>, BTreeError>
  where
    Uuid: Ord,
    Q: Borrow<Uuid> + MaybeSend + 'a,
  {
    let doc_id = *key.borrow();
    let (accumulator, _) = self.scan_document_state(doc_id).await?;

    if accumulator.is_empty() {
      return Ok(None);
    }

    let state = accumulator.finish();
    load_document(&state).map(Some)
  }

  async fn insert<'a>(&'a mut self, key: Uuid, value: AutoCommit) -> Result<(), BTreeError>
  where
    Uuid: Ord,
  {
    let mut doc = value;
    let change_hash = hash_heads(&doc.get_heads());
    let bytes = doc.save();

    let (start_enc, end_enc) = self.doc_scan_bounds(key);

    let found_identical = self
      .contains_identical_entry(start_enc.clone(), end_enc.clone(), &bytes)
      .await?;
    if found_identical {
      return Ok(());
    }

    let internal_key = DocumentChangeKey {
      doc_id: key,
      doc_type: DocumentType::Snapshot,
      change_hash,
    };
    let mut key_scratch = db_core::KeyScratch::with_capacity(49);
    <KC as db_core::FastKeyCodec<DocumentChangeKey>>::encode_into(
      &self.key_codec,
      &internal_key,
      &mut key_scratch,
    );
    let val_enc: Vec<u8> = <VC as db_core::ValueCodec<AutomergeEntry>>::encode(&bytes)
      .as_ref()
      .to_vec();
    self.inner.insert(key_scratch.buf, val_enc).await
  }

  async fn remove<'a, Q>(&'a mut self, key: Q) -> Result<Option<AutoCommit>, BTreeError>
  where
    Uuid: Ord,
    Q: Borrow<Uuid> + MaybeSend + 'a,
  {
    let doc_id = *key.borrow();
    let (accumulator, has_entries) = self.scan_document_state(doc_id).await?;
    let prev = if !has_entries {
      None
    } else {
      Some(accumulator.finish())
    };

    let keys_to_remove = self.collect_doc_keys(doc_id).await?;
    for k in keys_to_remove {
      let _ = self.inner.remove(&k).await;
    }

    match prev {
      None => Ok(None),
      Some(bytes) => load_document(&bytes).map(Some),
    }
  }

  fn range<'a, R>(
    &'a self,
    range: R,
  ) -> impl Stream<Item = Result<(Uuid, AutoCommit), BTreeError>> + 'a
  where
    Uuid: Ord,
    R: RangeBounds<Uuid> + MaybeSend + 'a,
  {
    stream! {
      let (start_enc, end_enc) = self.full_scan_bounds();

      let inner_stream = self.inner.range(start_enc.clone()..=end_enc.clone());
      futures::pin_mut!(inner_stream);

      let mut current_doc: Option<Uuid> = None;
      let mut accumulator = ReconstructionAccumulator::new();

      while let Some(item) = inner_stream.next().await {
        let (k_enc, v_enc) = item?;
        let k = match <KC as db_core::ValueCodec<DocumentChangeKey>>::decode_checked(&k_enc) {
          Ok(kd) => kd,
          Err(_) => continue,
        };
        if !uuid_in_range(&range, &k.doc_id) { continue; }

        if current_doc.is_none() {
          current_doc = Some(k.doc_id);
        } else if current_doc.as_ref().expect("doc id present") != &k.doc_id {
          if let Some(item) = flush_reconstructed_doc(&mut current_doc, &mut accumulator) {
            match item {
              Ok(pair) => yield Ok(pair),
              Err(e) => yield Err(e),
            }
          }
          current_doc = Some(k.doc_id);
        }

        let value = Self::decode_entry_bytes(&v_enc);
        accumulator.apply(k.doc_type, value);
      }

      if let Some(item) = flush_reconstructed_doc(&mut current_doc, &mut accumulator) {
        match item {
          Ok(pair) => yield Ok(pair),
          Err(e) => yield Err(e),
        }
      }
    }
  }
}

impl<B, KC, VC> BTree<Uuid, AutoCommit> for AutomergeBTreeEncoded<B, KC, VC>
where
  B: BTree<Vec<u8>, Vec<u8>> + Clone + Send + Sync + 'static,
  KC: db_core::FastKeyCodec<DocumentChangeKey> + Clone + Send + Sync + 'static,
  VC: db_core::ValueCodec<AutomergeEntry> + Clone + Send + Sync + 'static,
{
  type Transaction =
    crate::automerge_btree::transaction::AutomergeEncodedTransaction<B::Transaction, KC, VC>;

  async fn transaction(&self) -> Result<Self::Transaction, BTreeError> {
    let inner_tx = self.inner.transaction().await?;
    Ok(
      crate::automerge_btree::transaction::AutomergeEncodedTransaction::new(
        inner_tx,
        self.key_codec.clone(),
        self.val_codec.clone(),
      ),
    )
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use futures::{StreamExt, future};

  use automerge::transaction::Transactable;
  use db_core::{BTreeTransaction, block_on};
  use db_in_memory::InMemoryBTree;
  use std::fs;

  fn tmp_path(name: &str) -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("db-automerge-{}.db", name));
    path
  }

  #[test]
  fn insert_and_get_latest() {
    let _ = fs::remove_file(tmp_path("basic"));

    let underlying = InMemoryBTree::<DocumentChangeKey, AutomergeEntry>::new();
    let mut store = AutomergeBTree::new(underlying);

    let doc_id = Uuid::new_v4();
    let doc = AutoCommit::new();
    let mut expected = doc.clone();

    block_on(store.insert(doc_id, doc)).expect("insert");
    let mut got = block_on(store.get(&doc_id)).expect("get").expect("missing");
    let expected_bytes = expected.save();
    let got_bytes = got.save();
    assert_eq!(got_bytes, expected_bytes);
  }

  #[test]
  fn range_ordering() {
    let underlying = InMemoryBTree::<DocumentChangeKey, AutomergeEntry>::new();
    let mut store = AutomergeBTree::new(underlying);

    let mut ids: Vec<Uuid> = Vec::new();
    for i in 0..3 {
      let id = Uuid::new_v4();
      ids.push(id);
      let mut doc = AutoCommit::new();
      doc
        .put(&automerge::ROOT, "v", format!("v{}", i))
        .expect("put");
      block_on(store.insert(id, doc)).expect("insert");
      std::thread::sleep(std::time::Duration::from_millis(1));
    }

    let start = &Uuid::nil();
    let end = &Uuid::from_u128(u128::MAX);

    let s = store.range(start..=end);
    let items = block_on(async move {
      let mut collected = Vec::new();
      futures::pin_mut!(s);
      while let Some(item) = s.next().await {
        let (k, v) = item.expect("range failed");
        collected.push((k, v));
      }
      collected
    });

    assert_eq!(items.len(), 3);
  }

  #[test]
  fn compaction_with_concurrent_writer() {
    block_on(async {
      let underlying = InMemoryBTree::<DocumentChangeKey, AutomergeEntry>::new();
      let automerge = AutomergeBTree::with_compaction(underlying.clone(), 1, 1);
      let doc_id = Uuid::new_v4();

      let mut base_doc = AutoCommit::new();
      base_doc
        .put(&automerge::ROOT, "message", "hello")
        .expect("put base value");
      let base_changes = base_doc.get_changes(&[]);
      let mut base_delta = Vec::new();
      for change in &base_changes {
        base_delta.extend_from_slice(change.raw_bytes());
      }

      let delta_key = DocumentChangeKey {
        doc_id,
        doc_type: DocumentType::Incremental,
        change_hash: super::hash::hash_hashes(base_changes.iter().map(|c| c.hash().0)),
      };

      // insert initial delta directly into underlying storage
      {
        let mut tx = underlying.transaction().await.expect("start tx");
        tx.insert(delta_key.clone(), base_delta.clone())
          .await
          .expect("insert delta");
        tx.commit().await.expect("commit tx");
      }

      let reader = automerge;
      let writer_store = underlying.clone();
      let mut writer_doc = base_doc.clone();
      writer_doc
        .put(&automerge::ROOT, "tail", "!")
        .expect("put writer value");
      let writer_changes = writer_doc.get_changes(&base_doc.get_heads());
      let mut writer_delta = Vec::new();
      for change in &writer_changes {
        writer_delta.extend_from_slice(change.raw_bytes());
      }
      let writer_key = DocumentChangeKey {
        doc_id,
        doc_type: DocumentType::Incremental,
        change_hash: super::hash::hash_hashes(writer_changes.iter().map(|c| c.hash().0)),
      };
      let writer_key_clone = writer_key.clone();
      let writer_delta_clone = writer_delta.clone();

      let expected_read_state = base_delta.clone();

      let read_future = reader.get_document(doc_id);
      let write_future = async move {
        let mut tx = writer_store.transaction().await.expect("start writer tx");
        tx.insert(writer_key_clone, writer_delta_clone)
          .await
          .expect("insert writer delta");
        tx.commit().await.expect("commit writer tx");
      };

      let (read_state, ()) = future::join(read_future, write_future).await;
      assert_eq!(read_state.unwrap(), expected_read_state.clone());

      let final_store = AutomergeBTree::with_compaction(underlying.clone(), 1, 1);
      let final_state = final_store
        .get_document(doc_id)
        .await
        .expect("final get_document");
      // After compaction, the store contains a Snapshot (compacted from the
      // concurrent read state) plus the writer's Incremental delta inserted
      // after compaction. Reconstruction yields snapshot + delta.
      let mut expected_final_state = expected_read_state.clone();
      expected_final_state.extend_from_slice(&writer_delta);
      assert_eq!(final_state, expected_final_state);

      let actual_writer_value = underlying
        .get(&writer_key)
        .await
        .expect("get writer key failed")
        .expect("writer key missing");
      assert_eq!(actual_writer_value, writer_delta);
    });
  }

  #[test]
  fn uuid_prefix_vs_short_prefix() {
    let _ = fs::remove_file(tmp_path("prefix_test"));

    let underlying = InMemoryBTree::<Vec<u8>, Vec<u8>>::new();

    let id = Uuid::new_v4();
    let key = DocumentChangeKey {
      doc_id: id,
      doc_type: DocumentType::Snapshot,
      change_hash: [1u8; 32],
    };
    let mut key_scratch = db_core::KeyScratch::with_capacity(49);
    <DocumentChangeKeyCodec as db_core::FastKeyCodec<DocumentChangeKey>>::encode_into(
      &DocumentChangeKeyCodec,
      &key,
      &mut key_scratch,
    );
    let key_enc = key_scratch.buf;

    // insert encoded key into underlying storage via a transaction
    block_on(async {
      let mut tx = underlying.transaction().await.expect("start tx");
      tx.insert(key_enc.clone(), b"v".to_vec())
        .await
        .expect("insert");
      tx.commit().await.expect("commit");
    });

    // short (16-byte) prefix bounds - should NOT match encoded entries
    let short_start = id.as_bytes().to_vec();
    let short_end = id.as_bytes().to_vec();
    let short_count = block_on(async {
      let s = underlying.range(short_start.clone()..=short_end.clone());
      futures::pin_mut!(s);
      let mut cnt = 0usize;
      while let Some(item) = s.next().await {
        let (_k, _v) = item.expect("range");
        cnt += 1;
      }
      cnt
    });
    assert_eq!(short_count, 0);

    // full-encoded bounds should include the entry
    let (start_enc, end_enc) = uuid_prefix_range(id);
    let full_count = block_on(async {
      let s = underlying.range(start_enc.clone()..=end_enc.clone());
      futures::pin_mut!(s);
      let mut cnt = 0usize;
      while let Some(item) = s.next().await {
        let (_k, _v) = item.expect("range");
        cnt += 1;
      }
      cnt
    });
    assert_eq!(full_count, 1);
  }

  #[test]
  fn encoded_remove_only_deletes_target_document() {
    block_on(async {
      let underlying = InMemoryBTree::<Vec<u8>, Vec<u8>>::new();
      let mut store = AutomergeBTreeEncoded::new(underlying);

      let doc_a = Uuid::from_u128(1);
      let doc_b = Uuid::from_u128(2);

      let mut first = AutoCommit::new();
      first.put(&automerge::ROOT, "v", "a").expect("put a");
      let mut second = AutoCommit::new();
      second.put(&automerge::ROOT, "v", "b").expect("put b");

      store.insert(doc_a, first.clone()).await.expect("insert a");
      store.insert(doc_b, second.clone()).await.expect("insert b");

      let mut removed = store
        .remove(&doc_a)
        .await
        .expect("remove a")
        .expect("removed doc");
      assert_eq!(removed.save(), first.save());

      assert!(store.get(&doc_a).await.expect("get removed").is_none());
      let mut remaining = store
        .get(&doc_b)
        .await
        .expect("get remaining")
        .expect("remaining doc");
      assert_eq!(remaining.save(), second.save());
    });
  }
}
