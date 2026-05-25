use alloc::vec::Vec;
use core::cmp::Ordering;

/// Encodes and decodes a value for a storage backend.
pub trait ValueCodec<T>: Send + Sync + 'static {
  type Bytes<'a>: AsRef<[u8]> + 'a
  where
    Self: 'a,
    T: 'a;

  /// Returns the fixed byte width for the encoded value when known.
  fn fixed_width() -> Option<usize> {
    None
  }

  fn encode<'a>(value: &'a T) -> Self::Bytes<'a>;

  fn decode(data: &[u8]) -> T;

  fn decode_checked(data: &[u8]) -> Result<T, crate::DecodeError> {
    Ok(Self::decode(data))
  }

  fn encode_to_vec(value: &T) -> Vec<u8> {
    Self::encode(value).as_ref().to_vec()
  }
}

pub trait KeyCodec<T>: ValueCodec<T> {
  fn compare(left: &[u8], right: &[u8]) -> Ordering;
}

pub const CURRENT_CODEC_VERSION: u8 = 1;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum DecodeError {
  #[error("invalid version: {0}")]
  InvalidVersion(u8),

  #[error("truncated data")]
  Truncated,

  #[error("malformed data")]
  Malformed,
}

pub struct KeyScratch {
  pub buf: Vec<u8>,
}

impl KeyScratch {
  pub fn with_capacity(cap: usize) -> Self {
    Self {
      buf: Vec::with_capacity(cap),
    }
  }

  pub fn clear(&mut self) {
    self.buf.clear();
  }

  pub fn as_slice(&self) -> &[u8] {
    &self.buf
  }

  pub fn len(&self) -> usize {
    self.buf.len()
  }

  pub fn is_empty(&self) -> bool {
    self.buf.is_empty()
  }
}

pub trait BufferSink {
  /// Append bytes to the sink.
  fn push_bytes(&mut self, bytes: &[u8]);
}

impl BufferSink for KeyScratch {
  fn push_bytes(&mut self, bytes: &[u8]) {
    self.buf.extend_from_slice(bytes);
  }
}

#[cfg(not(feature = "std"))]
impl BufferSink for Vec<u8> {
  fn push_bytes(&mut self, bytes: &[u8]) {
    self.extend_from_slice(bytes);
  }
}

#[cfg(feature = "std")]
impl<T: std::io::Write> BufferSink for T {
  fn push_bytes(&mut self, bytes: &[u8]) {
    let _ = self.write_all(bytes);
  }
}

pub trait FastKeyCodec<T>: KeyCodec<T> {
  /// Append an encoded representation of `value` into `scratch`.
  fn encode_into(&self, value: &T, scratch: &mut KeyScratch) {
    // Default fallback: allocate via `ValueCodec::encode` and append.
    let bytes = <Self as ValueCodec<T>>::encode(value);
    scratch.buf.extend_from_slice(bytes.as_ref());
  }

  fn compare_encoded(&self, left: &[u8], right: &[u8]) -> Ordering {
    <Self as KeyCodec<T>>::compare(left, right)
  }
}

use alloc::string::String;

pub fn encode_version_into_sink<S: BufferSink>(sink: &mut S) {
  sink.push_bytes(&[CURRENT_CODEC_VERSION]);
}

fn encode_be_bytes_into_sink<S: BufferSink, const N: usize>(sink: &mut S, bytes: [u8; N]) {
  sink.push_bytes(&bytes);
}

pub fn encode_u32_into_sink<S: BufferSink>(sink: &mut S, value: u32) {
  encode_be_bytes_into_sink(sink, value.to_be_bytes());
}

pub fn encode_u64_into_sink<S: BufferSink>(sink: &mut S, value: u64) {
  encode_be_bytes_into_sink(sink, value.to_be_bytes());
}

pub fn encode_i64_into_sink<S: BufferSink>(sink: &mut S, value: i64) {
  encode_be_bytes_into_sink(sink, value.to_be_bytes());
}

pub fn encode_len_into_sink<S: BufferSink>(sink: &mut S, len: usize) {
  let len = u32::try_from(len)
    .unwrap_or_else(|_| panic!("invalid store codec payload: length exceeds u32"));
  encode_u32_into_sink(sink, len);
}

pub fn encode_string_into_sink<S: BufferSink>(sink: &mut S, value: &str) {
  encode_len_into_sink(sink, value.len());
  sink.push_bytes(value.as_bytes());
}

pub fn encode_bytes_into_sink<S: BufferSink>(sink: &mut S, value: &[u8]) {
  encode_len_into_sink(sink, value.len());
  sink.push_bytes(value);
}

pub fn encode_usize_into_sink<S: BufferSink>(sink: &mut S, value: usize) {
  let value = u64::try_from(value)
    .unwrap_or_else(|_| panic!("invalid store codec payload: usize exceeds u64"));
  encode_u64_into_sink(sink, value);
}

pub fn canonical_f64_bits_into_sink<S: BufferSink>(sink: &mut S, value: f64) {
  let bits = canonical_f64_bits(value);
  encode_u64_into_sink(sink, bits);
}

pub fn encode_bool_into_sink<S: BufferSink>(sink: &mut S, value: bool) {
  sink.push_bytes(&[u8::from(value)]);
}

pub struct Cursor<'a> {
  data: &'a [u8],
  position: usize,
}

impl<'a> Cursor<'a> {
  pub fn new(data: &'a [u8]) -> Self {
    Self { data, position: 0 }
  }

  pub fn finish(self) -> Result<(), DecodeError> {
    if self.position != self.data.len() {
      return Err(DecodeError::Malformed);
    }
    Ok(())
  }

  pub fn read_u8(&mut self) -> Result<u8, DecodeError> {
    let s = self.read_exact(1)?;
    Ok(s[0])
  }

  fn read_array<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
    let mut bytes = [0_u8; N];
    bytes.copy_from_slice(self.read_exact(N)?);
    Ok(bytes)
  }

  pub fn read_u32(&mut self) -> Result<u32, DecodeError> {
    Ok(u32::from_be_bytes(self.read_array()?))
  }

  pub fn read_u64(&mut self) -> Result<u64, DecodeError> {
    Ok(u64::from_be_bytes(self.read_array()?))
  }

  pub fn read_i64(&mut self) -> Result<i64, DecodeError> {
    Ok(i64::from_be_bytes(self.read_array()?))
  }

  pub fn read_exact(&mut self, len: usize) -> Result<&'a [u8], DecodeError> {
    let end = self
      .position
      .checked_add(len)
      .filter(|end| *end <= self.data.len())
      .ok_or(DecodeError::Truncated)?;
    let slice = &self.data[self.position..end];
    self.position = end;
    Ok(slice)
  }
}

pub fn decode_version(cursor: &mut Cursor<'_>) -> Result<(), DecodeError> {
  let version = cursor.read_u8()?;
  if version != CURRENT_CODEC_VERSION {
    return Err(DecodeError::InvalidVersion(version));
  }
  Ok(())
}

pub fn decode_bool(cursor: &mut Cursor<'_>) -> Result<bool, DecodeError> {
  match cursor.read_u8()? {
    0 => Ok(false),
    1 => Ok(true),
    _ => Err(DecodeError::Malformed),
  }
}

pub fn decode_len(cursor: &mut Cursor<'_>) -> Result<usize, DecodeError> {
  let v = cursor.read_u32()?;
  usize::try_from(v).map_err(|_| DecodeError::Malformed)
}

pub fn decode_string(cursor: &mut Cursor<'_>) -> Result<String, DecodeError> {
  let len = decode_len(cursor)?;
  let bytes = cursor.read_exact(len)?;
  String::from_utf8(bytes.to_vec()).map_err(|_| DecodeError::Malformed)
}

pub fn decode_bytes(cursor: &mut Cursor<'_>) -> Result<Vec<u8>, DecodeError> {
  let len = decode_len(cursor)?;
  Ok(cursor.read_exact(len)?.to_vec())
}

pub fn decode_usize(cursor: &mut Cursor<'_>) -> Result<usize, DecodeError> {
  let v = cursor.read_u64()?;
  usize::try_from(v).map_err(|_| DecodeError::Malformed)
}

pub fn canonical_f64_bits(value: f64) -> u64 {
  if value == 0.0 {
    0
  } else if value.is_nan() {
    f64::NAN.to_bits()
  } else {
    value.to_bits()
  }
}

pub fn encode_with_version<F>(buffer: &mut Vec<u8>, f: F)
where
  F: FnOnce(&mut Vec<u8>),
{
  encode_version_into_sink(buffer);
  f(buffer);
}

pub fn decode_from_slice<T, F>(data: &[u8], f: F) -> Result<T, DecodeError>
where
  F: FnOnce(&mut Cursor<'_>) -> Result<T, DecodeError>,
{
  let mut c = Cursor::new(data);
  let out = f(&mut c)?;
  c.finish()?;
  Ok(out)
}

pub fn decode_with_version<T, F>(data: &[u8], f: F) -> Result<T, DecodeError>
where
  F: FnOnce(&mut Cursor<'_>) -> Result<T, DecodeError>,
{
  decode_from_slice(data, |cursor| {
    decode_version(cursor)?;
    f(cursor)
  })
}

#[cfg(test)]
mod tests {
  use super::*;
  use core::cmp::Ordering;

  #[derive(Debug, Clone, Copy, Default)]
  struct TestI64Codec;

  impl ValueCodec<i64> for TestI64Codec {
    type Bytes<'a>
      = [u8; 8]
    where
      Self: 'a,
      i64: 'a;

    fn fixed_width() -> Option<usize> {
      Some(8)
    }

    fn encode<'a>(value: &'a i64) -> Self::Bytes<'a> {
      let bits = (*value as u64) ^ 0x8000_0000_0000_0000_u64;
      bits.to_be_bytes()
    }

    fn decode(data: &[u8]) -> i64 {
      let mut bytes = [0_u8; 8];
      bytes.copy_from_slice(&data[..8]);
      let encoded = u64::from_be_bytes(bytes);
      let bits = encoded ^ 0x8000_0000_0000_0000_u64;
      i64::from_be_bytes(bits.to_be_bytes())
    }
  }

  impl KeyCodec<i64> for TestI64Codec {
    fn compare(left: &[u8], right: &[u8]) -> Ordering {
      left.cmp(right)
    }
  }

  impl FastKeyCodec<i64> for TestI64Codec {}

  #[test]
  fn fast_key_codec_encode_into_matches_value_codec() {
    let codec = TestI64Codec;
    let encoded = <TestI64Codec as ValueCodec<i64>>::encode(&42);

    let mut s = KeyScratch::with_capacity(32);
    <TestI64Codec as FastKeyCodec<i64>>::encode_into(&codec, &42, &mut s);

    assert_eq!(s.as_slice(), encoded.as_slice());
  }

  #[test]
  fn decode_with_version_round_trips_payload() {
    let mut encoded = Vec::new();
    encode_with_version(&mut encoded, |sink| encode_i64_into_sink(sink, -10));

    let decoded = decode_with_version(&encoded, |cursor| cursor.read_i64()).expect("decode failed");

    assert_eq!(decoded, -10);
  }

  #[test]
  fn key_codec_compare_matches_domain_ordering() {
    let left = <TestI64Codec as ValueCodec<i64>>::encode(&-10);
    let right = <TestI64Codec as ValueCodec<i64>>::encode(&10);

    assert_eq!(
      <TestI64Codec as KeyCodec<i64>>::compare(left.as_ref(), right.as_ref()),
      (-10i64).cmp(&10i64)
    );
  }
}
