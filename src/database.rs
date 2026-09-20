use core::ops::Deref;

use engine::Engine;
#[cfg(all(feature = "automerge", feature = "redb"))]
use engine::EngineError;

#[cfg(all(feature = "automerge", feature = "in-memory"))]
use engine::InMemoryKernel;
#[cfg(all(feature = "automerge", any(feature = "in-memory", feature = "redb")))]
use engine_automerge::AutomergeRowCodec;
#[cfg(all(feature = "automerge", feature = "redb"))]
use engine_redb::{RedbKernel, redb};

/// An ergonomic database handle backed by an engine and row codec.
#[derive(Clone)]
pub struct Database<K, R> {
    engine: Engine<K, R>,
}

impl<K, R> Database<K, R> {
    pub fn from_engine(engine: Engine<K, R>) -> Self {
        Self { engine }
    }

    pub fn into_inner(self) -> Engine<K, R> {
        self.engine
    }

    pub fn as_engine(&self) -> &Engine<K, R> {
        &self.engine
    }
}

impl<K, R> From<Engine<K, R>> for Database<K, R> {
    fn from(engine: Engine<K, R>) -> Self {
        Self::from_engine(engine)
    }
}

impl<K, R> Deref for Database<K, R> {
    type Target = Engine<K, R>;

    fn deref(&self) -> &Self::Target {
        &self.engine
    }
}

#[cfg(all(feature = "automerge", feature = "redb"))]
impl Database<RedbKernel, AutomergeRowCodec> {
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, EngineError> {
        let database = redb::Database::create(path).map_err(EngineError::custom)?;
        Ok(Self::from_engine(Engine::new(
            RedbKernel::new(std::sync::Arc::new(database)),
            AutomergeRowCodec::new(),
        )))
    }
}

#[cfg(all(feature = "automerge", feature = "redb"))]
pub type FileDatabase = Database<RedbKernel, AutomergeRowCodec>;

#[cfg(all(feature = "automerge", feature = "in-memory"))]
impl Database<InMemoryKernel, AutomergeRowCodec> {
    pub fn in_memory() -> Self {
        Self::from_engine(Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new()))
    }
}

#[cfg(all(feature = "automerge", feature = "in-memory"))]
pub type InMemoryDatabase = Database<InMemoryKernel, AutomergeRowCodec>;
