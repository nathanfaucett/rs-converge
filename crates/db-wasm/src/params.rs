use db_engine::EngineValue;
use db_facade::SqlParams;
use js_sys::{Array, Object, Reflect, Uint8Array};
use std::collections::BTreeMap;
use wasm_bindgen::prelude::*;

pub fn to_js_error(message: impl core::fmt::Display) -> JsValue {
  js_sys::Error::new(&message.to_string()).into()
}

fn is_date(value: &JsValue) -> bool {
  // Check if the object has both toISOString and getTime methods, which are unique to Date
  value.is_object()
    && Reflect::has(value, &"toISOString".into()).unwrap_or(false)
    && Reflect::has(value, &"getTime".into()).unwrap_or(false)
}

fn to_iso_string(value: &JsValue) -> Result<String, JsValue> {
  let iso_string = Reflect::apply(
    &Reflect::get(value, &"toISOString".into())?.into(),
    value,
    &Array::new(),
  )?;

  iso_string
    .as_string()
    .ok_or_else(|| to_js_error("Failed to extract ISO string from Date"))
}

fn number_to_engine_value(value: f64) -> EngineValue {
  if value.is_finite()
    && value.fract() == 0.0
    && value >= i64::MIN as f64
    && value <= i64::MAX as f64
  {
    EngineValue::Integer(value as i64)
  } else {
    EngineValue::Float(value)
  }
}

fn bytes_to_engine_value(bytes: Vec<u8>) -> EngineValue {
  if bytes.len() == 16 {
    let mut uuid = [0_u8; 16];
    uuid.copy_from_slice(&bytes);
    EngineValue::Uuid(uuid)
  } else {
    EngineValue::Blob(bytes)
  }
}

fn parse_json_value(value: &JsValue, path: &str) -> Result<EngineValue, JsValue> {
  let json = js_sys::JSON::stringify(value).map_err(|_| {
    to_js_error(format!(
      "invalid param at {path}: value is not JSON-serializable"
    ))
  })?;

  let Some(json) = json.as_string() else {
    return Err(to_js_error(format!(
      "invalid param at {path}: value is not JSON-serializable"
    )));
  };

  Ok(EngineValue::Json(json))
}

fn parse_byte_array(array: &Array) -> Option<Vec<u8>> {
  let mut bytes = Vec::with_capacity(array.length() as usize);
  for entry in array.iter() {
    let Some(number) = entry.as_f64() else {
      return None;
    };
    if !number.is_finite() || number.fract() != 0.0 || !(0.0..=255.0).contains(&number) {
      return None;
    }
    bytes.push(number as u8);
  }
  Some(bytes)
}

type ParseEngineValueFn = fn(&JsValue, &str) -> Option<Result<EngineValue, JsValue>>;

const ENGINE_VALUE_PARSERS: &[ParseEngineValueFn] = &[
  try_parse_null,
  try_parse_number,
  try_parse_string,
  try_parse_uint8array,
  try_parse_array,
  try_parse_date,
  try_parse_bool_or_object,
];

fn parse_engine_value(value: JsValue, path: &str) -> Result<EngineValue, JsValue> {
  parse_engine_value_impl(&value, path)
}

fn parse_engine_value_impl(value: &JsValue, path: &str) -> Result<EngineValue, JsValue> {
  ENGINE_VALUE_PARSERS
    .iter()
    .find_map(|parser| parser(value, path))
    .unwrap_or_else(|| {
      Err(to_js_error(format!(
        "invalid param at {path}: unsupported parameter type"
      )))
    })
}

fn try_parse_null(value: &JsValue, _path: &str) -> Option<Result<EngineValue, JsValue>> {
  if value.is_null() || value.is_undefined() {
    Some(Ok(EngineValue::Null))
  } else {
    None
  }
}

fn try_parse_number(value: &JsValue, _path: &str) -> Option<Result<EngineValue, JsValue>> {
  value
    .as_f64()
    .map(|number| Ok(number_to_engine_value(number)))
}

fn try_parse_string(value: &JsValue, _path: &str) -> Option<Result<EngineValue, JsValue>> {
  value.as_string().map(|text| Ok(EngineValue::Text(text)))
}

fn try_parse_uint8array(value: &JsValue, _path: &str) -> Option<Result<EngineValue, JsValue>> {
  if Uint8Array::instanceof(value) {
    Some(parse_uint8array(value))
  } else {
    None
  }
}

fn try_parse_array(value: &JsValue, path: &str) -> Option<Result<EngineValue, JsValue>> {
  if Array::is_array(value) {
    Some(parse_js_array(value, path))
  } else {
    None
  }
}

fn try_parse_date(value: &JsValue, path: &str) -> Option<Result<EngineValue, JsValue>> {
  if is_date(value) {
    Some(parse_date_value(value, path))
  } else {
    None
  }
}

fn try_parse_bool_or_object(value: &JsValue, path: &str) -> Option<Result<EngineValue, JsValue>> {
  if value.as_bool().is_some() || value.is_object() {
    Some(parse_json_value(value, path))
  } else {
    None
  }
}

fn parse_uint8array(value: &JsValue) -> Result<EngineValue, JsValue> {
  let bytes = Uint8Array::new(value).to_vec();
  Ok(bytes_to_engine_value(bytes))
}

fn parse_js_array(value: &JsValue, path: &str) -> Result<EngineValue, JsValue> {
  let array = Array::from(value);
  if let Some(bytes) = parse_byte_array(&array) {
    return Ok(bytes_to_engine_value(bytes));
  }
  parse_json_value(value, path)
}

fn parse_date_value(value: &JsValue, path: &str) -> Result<EngineValue, JsValue> {
  match to_iso_string(value) {
    Ok(iso_string) => Ok(EngineValue::Text(iso_string)),
    Err(_) => Err(to_js_error(format!(
      "invalid param at {path}: cannot extract ISO string from Date"
    ))),
  }
}

fn parse_named_params(params: JsValue) -> Result<BTreeMap<String, EngineValue>, JsValue> {
  if !params.is_object() || Array::is_array(&params) {
    return Err(to_js_error(
      "invalid named params: expected an object with key/value pairs",
    ));
  }

  let object = Object::from(params.clone());
  let keys = Object::keys(&object);
  let mut named = BTreeMap::new();

  for key in keys.iter() {
    let Some(name) = key.as_string() else {
      return Err(to_js_error(
        "invalid named params: found non-string object key",
      ));
    };
    let raw_value = Reflect::get(&object, &key)
      .map_err(|_| to_js_error(format!("invalid named params: cannot read key '{name}'")))?;
    let parsed = parse_engine_value(raw_value, &format!("named param '{name}'"))?;
    named.insert(name, parsed);
  }

  Ok(named)
}

pub fn parse_sql_params(params: JsValue) -> Result<SqlParams, JsValue> {
  if Array::is_array(&params) {
    let array = Array::from(&params);
    let mut positional = Vec::with_capacity(array.length() as usize);
    for (index, value) in array.iter().enumerate() {
      positional.push(parse_engine_value(
        value,
        &format!("positional param ${}", index + 1),
      )?);
    }
    return Ok(SqlParams::from(positional));
  }

  let named = parse_named_params(params)?;
  Ok(SqlParams::named(named))
}
