use db_core::NamedTreeProvider;
use db_engine::EngineKey;

/// Engine-facing bridge built from a layout backend plus a format handler.
///
/// Implementors expose the layout backend for catalog/sync and implement
/// [`NamedTreeProvider`] for `EngineKey` / `Vec<u8>` engine trees.
pub trait LayoutFormatBridge: Clone + NamedTreeProvider<EngineKey, Vec<u8>> + Send + Sync {
  type LayoutBackend: Clone + Send + Sync;

  fn layout_backend(&self) -> &Self::LayoutBackend;
}
