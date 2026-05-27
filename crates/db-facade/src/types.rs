#[cfg(not(feature = "std"))]
extern crate alloc;

#[cfg(not(feature = "std"))]
use alloc::{format, string::String, vec::Vec};
use core::fmt;

#[cfg(feature = "automerge")]
use db_automerge::AutomergeFormatAdapter;
#[cfg(all(feature = "automerge", feature = "redb"))]
use db_automerge::DocumentType;
#[cfg(feature = "automerge")]
use db_automerge::{AutomergeEntry, DocumentChangeKey};
#[cfg(all(feature = "automerge", feature = "redb"))]
use db_core::BufferSink;
use db_core::{MaybeSend, MaybeSync, NamedBTreeMap};
#[cfg(feature = "redb")]
use db_engine::EngineKeyCodec;
use db_engine::{EngineDatabase, EngineKey, EngineStoreBackend, EngineValue};
use db_in_memory::InMemoryNamedBTree;
#[cfg(feature = "redb")]
use db_redb::REDBNamedBTree;

/// Public facade error type.
#[derive(Debug)]
pub enum DatabaseError {
  Engine(String),
  Other(String),
}

impl fmt::Display for DatabaseError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      DatabaseError::Engine(s) => write!(f, "engine error: {s}"),
      DatabaseError::Other(s) => write!(f, "{s}"),
    }
  }
}

impl From<db_engine::EngineError> for DatabaseError {
  fn from(e: db_engine::EngineError) -> Self {
    DatabaseError::Engine(format!("{e}"))
  }
}

/// Simple row type reusing EngineValue
pub type Row = Vec<EngineValue>;

/// In-memory named-tree layout backend for raw engine key/value storage.
pub type InMemoryEngineStore = InMemoryNamedBTree<EngineKey, Vec<u8>>;
/// Redb named-tree layout backend for raw engine key/value storage.
#[cfg(feature = "redb")]
pub type RedbEngineStore = REDBNamedBTree<EngineKey, Vec<u8>, EngineKeyCodec>;

/// Redb named-tree layout backend for Automerge document change storage.
#[cfg(all(feature = "automerge", feature = "redb"))]
pub type RedbAutomergeLayoutBackend = REDBNamedBTree<
  DocumentChangeKey,
  AutomergeEntry,
  FacadeDocumentChangeKeyCodec,
  FacadeVecBytesCodec,
>;
/// In-memory named-tree layout backend for Automerge document change storage.
#[cfg(feature = "automerge")]
pub type InMemoryAutomergeLayoutBackend = InMemoryNamedBTree<DocumentChangeKey, AutomergeEntry>;

/// Facade store: Automerge format adapter over a redb layout backend.
#[cfg(all(feature = "automerge", feature = "redb"))]
pub type RedbAutomergeStore = AutomergeFormatAdapter<RedbAutomergeLayoutBackend>;
/// Facade store: Automerge format adapter over an in-memory layout backend.
#[cfg(feature = "automerge")]
pub type InMemoryAutomergeStore = AutomergeFormatAdapter<InMemoryAutomergeLayoutBackend>;

pub trait FacadeStore: Clone + MaybeSend + MaybeSync + 'static {
  type EngineStore: db_engine::EngineStoreBackend;

  fn into_engine_store(self) -> Self::EngineStore;
}

impl<T> FacadeStore for T
where
  T: Clone + NamedBTreeMap<EngineKey, Vec<u8>> + MaybeSend + MaybeSync + 'static,
{
  type EngineStore = Self;

  fn into_engine_store(self) -> Self::EngineStore {
    self
  }
}

/// Opaque database handle.
pub struct Database<S>
where
  S: FacadeStore,
{
  pub(crate) engine: EngineDatabase<S::EngineStore>,
}

/// Transaction wrapper delegating to EngineTransaction.
pub struct Transaction<'db, S>
where
  S: FacadeStore,
{
  pub(crate) inner: db_engine::EngineTransaction<'db, S::EngineStore>,
}

/// Read-only transaction wrapper delegating to engine query execution.
pub struct ReadTransaction<'db, S>
where
  S: FacadeStore,
{
  pub(crate) db: &'db Database<S>,
}

#[cfg(all(feature = "automerge", feature = "redb"))]
#[derive(Clone, Copy, Debug, Default)]
pub struct FacadeDocumentChangeKeyCodec;

#[cfg(all(feature = "automerge", feature = "redb"))]
impl db_core::ValueCodec<DocumentChangeKey> for FacadeDocumentChangeKeyCodec {
  type Bytes<'a>
    = Vec<u8>
  where
    Self: 'a,
    DocumentChangeKey: 'a;

  fn encode<'a>(value: &'a DocumentChangeKey) -> Self::Bytes<'a> {
    let mut out = Vec::with_capacity(49);
    out.extend_from_slice(value.doc_id.as_bytes());
    out.push(match value.doc_type {
      DocumentType::Incremental => 0u8,
      DocumentType::Snapshot => 1u8,
    });
    out.extend_from_slice(&value.change_hash);
    out
  }

  fn decode(data: &[u8]) -> DocumentChangeKey {
    if data.len() < 49 {
      panic!("invalid DocumentChangeKey encoding");
    }
    let id = uuid::Uuid::from_slice(&data[0..16]).expect("uuid decode");
    let doc_type = match data[16] {
      0 => DocumentType::Incremental,
      1 => DocumentType::Snapshot,
      _ => panic!("invalid doc_type"),
    };
    let mut change_hash = [0u8; 32];
    change_hash.copy_from_slice(&data[17..49]);
    DocumentChangeKey {
      doc_id: id,
      doc_type,
      change_hash,
    }
  }

  fn decode_checked(data: &[u8]) -> Result<DocumentChangeKey, db_core::DecodeError> {
    if data.len() < 49 {
      return Err(db_core::DecodeError::Truncated);
    }
    Ok(Self::decode(data))
  }
}

#[cfg(all(feature = "automerge", feature = "redb"))]
impl db_core::KeyCodec<DocumentChangeKey> for FacadeDocumentChangeKeyCodec {
  fn compare(left: &[u8], right: &[u8]) -> core::cmp::Ordering {
    left.cmp(right)
  }
}

#[cfg(all(feature = "automerge", feature = "redb"))]
impl db_core::FastKeyCodec<DocumentChangeKey> for FacadeDocumentChangeKeyCodec {
  fn encode_into(&self, value: &DocumentChangeKey, scratch: &mut db_core::KeyScratch) {
    scratch.push_bytes(value.doc_id.as_bytes());
    let dt = match value.doc_type {
      DocumentType::Incremental => 0u8,
      DocumentType::Snapshot => 1u8,
    };
    scratch.push_bytes(&[dt]);
    scratch.push_bytes(&value.change_hash);
  }

  fn compare_encoded(&self, left: &[u8], right: &[u8]) -> core::cmp::Ordering {
    <Self as db_core::KeyCodec<DocumentChangeKey>>::compare(left, right)
  }
}

#[cfg(all(feature = "automerge", feature = "redb"))]
#[derive(Clone, Copy, Debug, Default)]
pub struct FacadeVecBytesCodec;

#[cfg(all(feature = "automerge", feature = "redb"))]
impl db_core::ValueCodec<AutomergeEntry> for FacadeVecBytesCodec {
  type Bytes<'a>
    = Vec<u8>
  where
    Self: 'a,
    AutomergeEntry: 'a;

  fn encode<'a>(value: &'a AutomergeEntry) -> Self::Bytes<'a> {
    value.clone()
  }

  fn decode(data: &[u8]) -> AutomergeEntry {
    data.to_vec()
  }

  fn decode_checked(data: &[u8]) -> Result<AutomergeEntry, db_core::DecodeError> {
    Ok(data.to_vec())
  }
}
