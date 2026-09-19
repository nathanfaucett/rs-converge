use std::{
    collections::{BTreeMap, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
};

use uuid::Uuid;

use crate::{JsonNumber, JsonValue, Value, ValueType};

fn hash<T: Hash>(value: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

#[test]
fn json_number_compares_all_representations() {
    let values = [
        JsonNumber::I64(-1),
        JsonNumber::U64(1),
        JsonNumber::F64(1.0),
    ];

    for left in &values {
        for right in &values {
            assert_eq!(left.eq(right), right.eq(left));
            assert_eq!(left.cmp(right), right.cmp(left).reverse());
        }
    }

    assert_ne!(JsonNumber::I64(-1), JsonNumber::U64(u64::MAX));
    assert_eq!(JsonNumber::I64(1), JsonNumber::U64(1));
    assert_eq!(
        JsonNumber::I64(1).cmp(&JsonNumber::U64(1)),
        core::cmp::Ordering::Equal
    );
}

#[cfg(feature = "serde_json")]
#[test]
fn json_number_converts_serde_numbers() {
    assert_eq!(
        JsonNumber::from(serde_json::Number::from(-1)),
        JsonNumber::I64(-1)
    );
    assert_eq!(
        JsonNumber::from(serde_json::Number::from(1_u64)),
        JsonNumber::U64(1)
    );
    assert_eq!(
        JsonNumber::from(serde_json::Number::from_f64(1.5).unwrap()),
        JsonNumber::F64(1.5)
    );
}

#[cfg(feature = "serde_json")]
#[test]
fn json_value_converts_and_formats_all_variants() {
    let value = serde_json::json!({
        "array": [null, true, 1, "text"],
        "object": { "key": false }
    });
    let json = JsonValue::from(value);

    assert_eq!(
        json.to_string(),
        "{\"array\": [null, true, 1, \"text\"], \"object\": {\"key\": false}}"
    );
}

#[test]
fn value_type_ranks_follow_variant_order() {
    let types = [
        ValueType::Null,
        ValueType::Type,
        ValueType::Uuid,
        ValueType::Bool,
        ValueType::Integer,
        ValueType::Float,
        ValueType::Text,
        ValueType::Json,
        ValueType::Blob,
    ];

    for (rank, value_type) in types.iter().enumerate() {
        assert_eq!(value_type.rank(), rank as u8);
    }
}

#[test]
fn values_type_hash_and_compare_all_variants() {
    let mut object = BTreeMap::new();
    object.insert("key".into(), JsonValue::Null);
    let values = [
        Value::Null,
        Value::Type(ValueType::Bool),
        Value::Uuid(Uuid::nil()),
        Value::Bool(true),
        Value::Integer(1),
        Value::Float(1.0),
        Value::Text("text".into()),
        Value::Json(JsonValue::Object(object)),
        Value::Blob(vec![1]),
    ];
    let types = [
        ValueType::Null,
        ValueType::Type,
        ValueType::Uuid,
        ValueType::Bool,
        ValueType::Integer,
        ValueType::Float,
        ValueType::Text,
        ValueType::Json,
        ValueType::Blob,
    ];

    for (value, value_type) in values.iter().zip(types) {
        assert_eq!(value.r#type(), value_type);
        assert_eq!(hash(value), hash(value));
    }

    for left in &values {
        for right in &values {
            assert_eq!(left.eq(right), right.eq(left));
            assert_eq!(left.cmp(right), right.cmp(left).reverse());
        }
    }
}

#[test]
fn value_integer_conversion_accepts_only_finite_in_range_floats() {
    assert_eq!(Value::Integer(1).as_integer(), Some(1));
    assert_eq!(Value::Float(1.5).as_integer(), Some(1));
    assert_eq!(Value::Float(f64::NAN).as_integer(), None);
    assert_eq!(Value::Float(f64::INFINITY).as_integer(), None);
    assert_eq!(Value::Float((i64::MAX as f64) * 2.0).as_integer(), None);
    assert_eq!(Value::Text("1".into()).as_integer(), None);
}
