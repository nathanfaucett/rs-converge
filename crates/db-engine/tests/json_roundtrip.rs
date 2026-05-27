//! Integration tests for JSON type codec and operations roundtrip.

use db_engine::EngineValue;
use db_engine::key_encoding::KeyEncoding;

#[test]
fn test_json_type_codec_roundtrip() {
  let json_str = r#"{"name": "Alice", "age": 30, "active": true}"#;
  let value = EngineValue::Json(json_str.to_string());

  // Encode
  use db_engine::key_encoding::DefaultEncoding;
  let encoded = DefaultEncoding::encode_values(std::slice::from_ref(&value));

  // Decode
  let decoded = DefaultEncoding::decode_values(&encoded).expect("failed to decode");

  assert_eq!(decoded.len(), 1);
  assert_eq!(decoded[0], value);
}

#[test]
fn test_json_null_value_codec_roundtrip() {
  use db_engine::key_encoding::DefaultEncoding;

  let values = vec![
    EngineValue::Json(r#"{"x": null}"#.to_string()),
    EngineValue::Null,
  ];

  let encoded = DefaultEncoding::encode_values(&values);
  let decoded = DefaultEncoding::decode_values(&encoded).expect("failed to decode");

  assert_eq!(decoded, values);
}

#[test]
fn test_json_mixed_types_codec_roundtrip() {
  use db_engine::key_encoding::DefaultEncoding;

  let values = vec![
    EngineValue::Integer(42),
    EngineValue::Text("hello".to_string()),
    EngineValue::Json(r#"{"key": "value"}"#.to_string()),
    EngineValue::Null,
    EngineValue::Float(3.5),
  ];

  let encoded = DefaultEncoding::encode_values(&values);
  let decoded = DefaultEncoding::decode_values(&encoded).expect("failed to decode");

  assert_eq!(decoded, values);
}

#[test]
fn test_json_roundtrip_preserves_structure() {
  use db_engine::key_encoding::DefaultEncoding;

  let complex_json = r#"{
    "users": [
      {"id": 1, "name": "Alice", "tags": ["admin", "user"]},
      {"id": 2, "name": "Bob", "tags": ["user"]}
    ],
    "metadata": {
      "created": "2024-05-18",
      "count": 2
    }
  }"#;

  // Normalize (parse and re-serialize)
  let parsed: serde_json::Value = serde_json::from_str(complex_json).expect("parse failed");
  let normalized = parsed.to_string();

  // Create engine value
  let value = EngineValue::Json(normalized.clone());

  // Roundtrip
  let encoded = DefaultEncoding::encode_values(std::slice::from_ref(&value));
  let decoded = DefaultEncoding::decode_values(&encoded).expect("decode failed");

  assert_eq!(decoded[0], value);

  // Verify structure is preserved
  if let EngineValue::Json(s) = &decoded[0] {
    let reparsed: serde_json::Value = serde_json::from_str(&s).expect("parse failed");
    assert_eq!(reparsed["users"].as_array().unwrap().len(), 2);
    assert_eq!(reparsed["users"][0]["name"], "Alice");
  }
}
