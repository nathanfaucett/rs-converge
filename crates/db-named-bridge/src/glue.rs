use db_core::NamedBTreeMap;
use db_engine::{EngineKey, EngineNamedTreeBackend};

/// Engine-facing bridge built from a layout backend plus a format handler.
///
/// Implementors expose the layout backend for catalog/sync and implement
/// [`NamedBTreeMap`] and [`EngineNamedTreeBackend`] for `EngineKey` / `Vec<u8>` engine trees.
pub trait LayoutFormatBridge:
  Clone + NamedBTreeMap<EngineKey, Vec<u8>> + EngineNamedTreeBackend<EngineKey, Vec<u8>> + Send + Sync
{
  type LayoutBackend: Clone + Send + Sync;

  fn layout_backend(&self) -> &Self::LayoutBackend;
}
