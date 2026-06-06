#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

use core::ops::{Deref, DerefMut};

use serde::{Deserialize, Serialize};

/// Wrapper around `automerge::AutoCommit` that provides `Serialize`/`Deserialize`
/// by converting to/from bytes using `save` and `load`.
#[derive(Clone, Debug)]
pub struct AutoCommit(automerge::AutoCommit);

impl Default for AutoCommit {
  fn default() -> Self {
    Self::new()
  }
}

impl AutoCommit {
  pub fn load(bytes: &[u8]) -> Result<Self, automerge::AutomergeError> {
    automerge::AutoCommit::load(bytes).map(AutoCommit)
  }

  pub fn new() -> Self {
    AutoCommit(automerge::AutoCommit::new())
  }

  /// Save consumes or mutably borrows the inner commit; expose a mutable
  /// method for callers that have ownership/mutable access.
  pub fn save(&mut self) -> Vec<u8> {
    self.0.save()
  }
}

impl From<automerge::AutoCommit> for AutoCommit {
  fn from(inner: automerge::AutoCommit) -> Self {
    AutoCommit(inner)
  }
}

impl From<AutoCommit> for automerge::AutoCommit {
  fn from(wrapper: AutoCommit) -> automerge::AutoCommit {
    wrapper.0
  }
}

impl Deref for AutoCommit {
  type Target = automerge::AutoCommit;

  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

impl DerefMut for AutoCommit {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.0
  }
}

impl Serialize for AutoCommit {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: serde::Serializer,
  {
    // `save` requires &mut self on the inner type. Clone the inner value if
    // possible and save from the clone so this method can take `&self`.
    let mut clone = self.0.clone();
    let bytes = clone.save();
    serializer.serialize_bytes(&bytes)
  }
}

impl<'de> Deserialize<'de> for AutoCommit {
  fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
  where
    D: serde::Deserializer<'de>,
  {
    let bytes: Vec<u8> = serde::Deserialize::deserialize(deserializer)?;
    automerge::AutoCommit::load(&bytes)
      .map(AutoCommit)
      .map_err(serde::de::Error::custom)
  }
}
