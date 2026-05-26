//! JSON value operations: extraction, merging, and validation.

#[cfg(not(feature = "std"))]
use alloc::string::{String, ToString};
#[cfg(feature = "std")]
use std::string::String;

use crate::EngineValue;

/// Validates that a string is valid JSON.
pub fn json_valid(json_str: &str) -> bool {
  serde_json::from_str::<serde_json::Value>(json_str).is_ok()
}

/// Extracts a value from JSON using a simple path notation.
///
/// Supports paths like:
/// - `$.field` or `.field` for object field access
/// - `$.array[0]` for array indexing
///
/// Returns `EngineValue::Json` of the extracted value, or `Null` if not found.
pub fn json_extract(json_str: &str, path: &str) -> Result<EngineValue, String> {
  let value: serde_json::Value =
    serde_json::from_str(json_str).map_err(|e| format!("invalid JSON: {}", e))?;

  let extracted = extract_path(&value, path)
    .map(|v| v.to_string())
    .unwrap_or_else(|| "null".to_string());

  Ok(EngineValue::Json(extracted))
}

/// Merges two JSON objects.
///
/// If both values are objects, performs a shallow merge (right overwrites left).
/// If either is not an object, returns an error.
pub fn json_merge(left_str: &str, right_str: &str) -> Result<EngineValue, String> {
  let left: serde_json::Value =
    serde_json::from_str(left_str).map_err(|e| format!("invalid JSON (left): {}", e))?;
  let right: serde_json::Value =
    serde_json::from_str(right_str).map_err(|e| format!("invalid JSON (right): {}", e))?;

  let merged = merge_objects(&left, &right)
    .ok_or_else(|| "both JSON values must be objects to merge".to_string())?;

  Ok(EngineValue::Json(merged.to_string()))
}

/// Extracts a path from a JSON value.
///
/// Supports simple paths like `$.field`, `.field`, or `$.array[0]`.
fn extract_path(value: &serde_json::Value, path: &str) -> Option<serde_json::Value> {
  let trimmed = path.trim_start_matches('$');
  let trimmed = trimmed.trim_start_matches('.');

  if trimmed.is_empty() {
    return Some(value.clone());
  }

  // Handle array indexing: `field[0]`
  if let Some(bracket_pos) = trimmed.find('[') {
    let field = &trimmed[..bracket_pos];
    let rest = &trimmed[bracket_pos..];

    let current = if field.is_empty() {
      value.clone()
    } else {
      value.get(field)?.clone()
    };

    // Parse all bracketed indices
    return extract_indices(&current, rest);
  }

  // Simple field access
  if let Some(dot_pos) = trimmed.find('.') {
    let field = &trimmed[..dot_pos];
    let rest = &trimmed[dot_pos + 1..];
    let next = value.get(field)?;
    extract_path(next, &format!("$.{}", rest))
  } else {
    value.get(trimmed).cloned()
  }
}

/// Extracts values using array indices like `[0][1]`.
fn extract_indices(value: &serde_json::Value, indices: &str) -> Option<serde_json::Value> {
  let mut current = value.clone();

  let mut chars = indices.chars().peekable();
  while chars.peek() == Some(&'[') {
    chars.next(); // consume '['

    let mut num_str = String::new();
    while let Some(&ch) = chars.peek() {
      if ch == ']' {
        break;
      }
      num_str.push(ch);
      chars.next();
    }

    if chars.next() != Some(']') {
      return None; // malformed
    }

    let index: usize = num_str.parse().ok()?;
    current = current.get(index)?.clone();
  }

  Some(current)
}

/// Merges two JSON objects (shallow merge).
fn merge_objects(left: &serde_json::Value, right: &serde_json::Value) -> Option<serde_json::Value> {
  let left_obj = left.as_object()?;
  let right_obj = right.as_object()?;

  let mut merged = left_obj.clone();
  for (k, v) in right_obj {
    merged.insert(k.clone(), v.clone());
  }

  Some(serde_json::json!(merged))
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_json_valid() {
    assert!(json_valid("{}"));
    assert!(json_valid("[]"));
    assert!(json_valid(r#"{"key": "value"}"#));
    assert!(!json_valid("{invalid}"));
    assert!(!json_valid(""));
  }

  #[test]
  fn test_json_extract_simple_field() {
    let json = r#"{"name": "Alice", "age": 30}"#;
    let result = json_extract(json, "$.name").unwrap();

    if let EngineValue::Json(s) = result {
      assert_eq!(s, r#""Alice""#);
    } else {
      panic!("expected Json variant");
    }
  }

  #[test]
  fn test_json_extract_missing_field() {
    let json = r#"{"name": "Alice"}"#;
    let result = json_extract(json, "$.missing").unwrap();

    if let EngineValue::Json(s) = result {
      assert_eq!(s, "null");
    } else {
      panic!("expected Json variant");
    }
  }

  #[test]
  fn test_json_extract_array_index() {
    let json = r#"{"items": [10, 20, 30]}"#;
    let result = json_extract(json, "$.items[1]").unwrap();

    if let EngineValue::Json(s) = result {
      assert_eq!(s, "20");
    } else {
      panic!("expected Json variant");
    }
  }

  #[test]
  fn test_json_merge_objects() {
    let left = r#"{"a": 1, "b": 2}"#;
    let right = r#"{"b": 3, "c": 4}"#;
    let result = json_merge(left, right).unwrap();

    if let EngineValue::Json(s) = result {
      let parsed: serde_json::Value = serde_json::from_str(&s).unwrap();
      assert_eq!(parsed["a"], 1);
      assert_eq!(parsed["b"], 3); // right overwrites left
      assert_eq!(parsed["c"], 4);
    } else {
      panic!("expected Json variant");
    }
  }

  #[test]
  fn test_json_merge_non_object_fails() {
    let left = r#"[1, 2, 3]"#;
    let right = r#"[4, 5, 6]"#;
    assert!(json_merge(left, right).is_err());
  }
}
