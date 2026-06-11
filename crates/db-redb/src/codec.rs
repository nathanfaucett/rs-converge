use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CodecError {
  #[error("Error: {0}")]
  Custom(String),
}

impl CodecError {
  pub fn custom<T>(error: T) -> Self
  where
    T: ToString,
  {
    Self::Custom(error.to_string())
  }
}

pub type CodecResult<T> = Result<T, CodecError>;

pub trait Codec: Sized {
  fn from_bytes(bytes: &[u8]) -> CodecResult<Self>;
  fn to_bytes(&self) -> CodecResult<Vec<u8>>;
}

impl<T> Codec for T
where
  T: Serialize + DeserializeOwned,
{
  fn from_bytes(bytes: &[u8]) -> CodecResult<Self> {
    postcard::from_bytes(bytes).map_err(CodecError::custom)
  }

  fn to_bytes(&self) -> CodecResult<Vec<u8>> {
    postcard::to_stdvec(self).map_err(CodecError::custom)
  }
}
