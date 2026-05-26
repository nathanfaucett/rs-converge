// Codec roundtrip example: encode an engine row and decode it back.
use db_engine::{EngineRowCodec, EngineValue};

fn main() {
  let value = vec![EngineValue::Integer(1), EngineValue::Text("Alice".into())];

  // Use the fully-qualified trait methods for encoding/decoding
  let encoded = <EngineRowCodec as db_core::ValueCodec<Vec<EngineValue>>>::encode_to_vec(&value);
  let decoded = <EngineRowCodec as db_core::ValueCodec<Vec<EngineValue>>>::decode_checked(&encoded)
    .expect("decode failed");

  println!("original: {:?}", value);
  println!("decoded:  {:?}", decoded);

  assert_eq!(decoded, value);
  println!("Codec roundtrip succeeded");
}
