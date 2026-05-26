use db_core::KeyCodec;
use proptest::prelude::*;

use db_engine::key_encoding::{DefaultEncoding, KeyEncoding};
use db_engine::{EngineKey, EngineKeyCodec, EngineRowCodec, EngineValue};

/// Use the fully-qualified trait methods for encoding/decoding to avoid
/// needing to import the trait into the test scope.
fn engine_value_strategy() -> impl Strategy<Value = EngineValue> {
  prop_oneof![
    Just(EngineValue::Null),
    any::<i64>().prop_map(EngineValue::Integer),
    proptest::num::f64::ANY.prop_map(EngineValue::Float),
    any::<String>().prop_map(EngineValue::Text),
    any::<Vec<u8>>().prop_map(EngineValue::Blob),
  ]
}

proptest! {
  #[test]
  fn engine_row_roundtrip(values in prop::collection::vec(engine_value_strategy(), 0..6)) {
    let value = values;

    let encoded = <EngineRowCodec as db_core::ValueCodec<Vec<EngineValue>>>::encode_to_vec(&value);
    let decoded = <EngineRowCodec as db_core::ValueCodec<Vec<EngineValue>>>::decode_checked(&encoded)
      .expect("decode failed");

    prop_assert_eq!(decoded, value);
  }

  #[test]
  fn engine_key_ordering(a in engine_value_strategy(), b in engine_value_strategy()) {
    let left_key = <DefaultEncoding as KeyEncoding>::encode_values(std::slice::from_ref(&a));
    let right_key = <DefaultEncoding as KeyEncoding>::encode_values(std::slice::from_ref(&b));

    let left_encoded = <EngineKeyCodec as db_core::ValueCodec<EngineKey>>::encode_to_vec(&left_key);
    let right_encoded = <EngineKeyCodec as db_core::ValueCodec<EngineKey>>::encode_to_vec(&right_key);

    let cmp_encoded = EngineKeyCodec::compare(&left_encoded, &right_encoded);
    let cmp_keys = left_key.cmp(&right_key);

    prop_assert_eq!(cmp_encoded, cmp_keys);
  }
}
