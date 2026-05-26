//! Integration tests for JSON type codec and operations roundtrip.

use db_engine::key_encoding::KeyEncoding;
use db_engine::{EngineValue, json_extract, json_merge, json_valid};

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
fn test_json_operations_extract_and_merge() {
  let user1 = r#"{"name": "Alice", "email": "alice@example.com"}"#;
  let user2 = r#"{"email": "alice.updated@example.com", "status": "active"}"#;

  // Extract name from user1
  let name_result = json_extract(user1, "$.name").expect("extract failed");
  if let EngineValue::Json(s) = name_result {
    assert_eq!(s, r#""Alice""#);
  } else {
    panic!("expected Json value");
  }

  // Merge user1 and user2
  let merged = json_merge(user1, user2).expect("merge failed");
  if let EngineValue::Json(s) = merged {
    let parsed: serde_json::Value = serde_json::from_str(&s).expect("invalid JSON");
    assert_eq!(parsed["name"], "Alice");
    assert_eq!(parsed["email"], "alice.updated@example.com");
    assert_eq!(parsed["status"], "active");
  } else {
    panic!("expected Json value");
  }
}

#[test]
fn test_json_validation() {
  let valid_json = r#"{"key": "value"}"#;
  let invalid_json = r#"{ bad json }"#;

  assert!(json_valid(valid_json));
  assert!(!json_valid(invalid_json));
}

#[test]
fn test_json_extract_nested_paths() {
  let json = r#"{"user": {"profile": {"name": "Alice"}}}"#;

  let result = json_extract(json, "$.user.profile.name").expect("extract failed");
  if let EngineValue::Json(s) = result {
    assert_eq!(s, r#""Alice""#);
  } else {
    panic!("expected Json value");
  }
}

#[test]
fn test_json_extract_array_elements() {
  let json = r#"{"items": [{"id": 1}, {"id": 2}, {"id": 3}]}"#;

  let result = json_extract(json, "$.items[1]").expect("extract failed");
  if let EngineValue::Json(s) = result {
    let parsed: serde_json::Value = serde_json::from_str(&s).expect("invalid JSON");
    assert_eq!(parsed["id"], 2);
  } else {
    panic!("expected Json value");
  }
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
    let reparsed: serde_json::Value = serde_json::from_str(s).expect("parse failed");
    assert_eq!(reparsed["users"].as_array().unwrap().len(), 2);
    assert_eq!(reparsed["users"][0]["name"], "Alice");
  }
}
