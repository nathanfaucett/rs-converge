use alloc::{string::ToString, vec::Vec};

use async_stream::stream;
use futures::{Stream, StreamExt, pin_mut};

use value::{Row, Value};

use crate::{EngineError, EngineResult, KernelTransaction};

pub trait RowTable: KernelTransaction {
    fn get_entry(
        &self,
        table: &str,
        key: &Row,
    ) -> impl Future<Output = EngineResult<Option<Row>>> + Send {
        async move {
            self.get_bytes(table, &encode_key(key))
                .await?
                .map(decode_value)
                .transpose()
        }
    }

    fn scan_entries(&self, table: &str) -> impl Stream<Item = EngineResult<(Row, Row)>> + Send {
        self.scan_bytes(table).map(|entry| {
            let (key, value) = entry?;
            Ok((decode_key(&key)?, decode_value(value)?))
        })
    }

    fn scan_entries_owned(
        &self,
        table: &str,
    ) -> impl Stream<Item = EngineResult<(Row, Row)>> + Send {
        stream! {
            let entries = self.scan_bytes(table);
            pin_mut!(entries);
            while let Some(entry) = entries.next().await {
                let (key, value) = entry?;
                yield (decode_key(&key).and_then(|key| decode_value(value).map(|value| (key, value))));
            }
        }
    }

    fn put_entry(
        &mut self,
        table: &str,
        key: Row,
        value: Row,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        self.put_bytes(table, encode_key(&key), encode_value(&value))
    }

    fn remove_entry(
        &mut self,
        table: &str,
        key: &Row,
    ) -> impl Future<Output = EngineResult<Option<Row>>> + Send {
        async move {
            self.remove_bytes(table, &encode_key(key))
                .await?
                .map(decode_value)
                .transpose()
        }
    }
}

impl<T: KernelTransaction + ?Sized> RowTable for T {}

fn encode_value(row: &Row) -> Vec<u8> {
    postcard::to_allocvec(row).expect("Row serialization failed")
}

fn decode_value(bytes: Vec<u8>) -> EngineResult<Row> {
    postcard::from_bytes(&bytes).map_err(EngineError::custom)
}

fn encode_key(row: &Row) -> Vec<u8> {
    let mut bytes = Vec::new();
    for value in &row.values {
        encode_value_order(value, &mut bytes);
    }
    bytes.extend_from_slice(&[0, 0]);
    bytes.extend(encode_value(row));
    bytes
}

fn encode_value_order(value: &Value, bytes: &mut Vec<u8>) {
    match value {
        Value::Null => bytes.push(1),
        Value::Type(value) => {
            bytes.extend_from_slice(&[2, value.rank() + 1]);
        }
        Value::Uuid(value) => {
            bytes.push(3);
            bytes.extend_from_slice(value.as_bytes());
        }
        Value::Bool(value) => bytes.extend_from_slice(&[4, u8::from(*value) + 1]),
        Value::Integer(value) => {
            bytes.push(5);
            bytes.extend_from_slice(&((*value as u64 ^ (1 << 63)).to_be_bytes()));
        }
        Value::Float(value) => {
            bytes.push(6);
            bytes.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        Value::Text(value) => {
            bytes.push(7);
            encode_escaped(value.as_bytes(), bytes);
        }
        Value::Json(value) => {
            bytes.push(8);
            encode_escaped(value.to_string().as_bytes(), bytes);
        }
        Value::Blob(value) => {
            bytes.push(9);
            encode_escaped(value, bytes);
        }
    }
}

fn encode_escaped(value: &[u8], bytes: &mut Vec<u8>) {
    for byte in value {
        if *byte == 0 {
            bytes.extend_from_slice(&[0, 255]);
        } else {
            bytes.push(*byte);
        }
    }
    bytes.extend_from_slice(&[0, 0]);
}

fn decode_key(bytes: &[u8]) -> EngineResult<Row> {
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            0 if bytes.get(index + 1) == Some(&0) => {
                return postcard::from_bytes(&bytes[index + 2..]).map_err(EngineError::custom);
            }
            1 => index += 1,
            2 => index += 2,
            3 => index += 17,
            4 => index += 2,
            5 | 6 => index += 9,
            7..=9 => {
                index += 1;
                while index < bytes.len() {
                    match bytes[index] {
                        0 if bytes.get(index + 1) == Some(&0) => {
                            index += 2;
                            break;
                        }
                        0 if bytes.get(index + 1) == Some(&255) => index += 2,
                        _ => index += 1,
                    }
                }
            }
            _ => return Err(EngineError::custom("Invalid row key")),
        }
    }
    Err(EngineError::custom("Invalid row key"))
}

#[cfg(test)]
mod tests {
    use alloc::{string::String, vec};

    use value::{Row, Value};

    use super::{decode_key, decode_value, encode_key, encode_value};

    #[test]
    fn key_order_and_round_trip() {
        let rows = [
            Row::default(),
            Row::new(vec![Value::Null]),
            Row::new(vec![Value::Integer(-1)]),
            Row::new(vec![Value::Integer(0)]),
            Row::new(vec![Value::Text(String::from("a"))]),
            Row::new(vec![Value::Blob(vec![0, 1])]),
        ];
        for pair in rows.windows(2) {
            assert_eq!(
                pair[0].cmp(&pair[1]),
                encode_key(&pair[0]).cmp(&encode_key(&pair[1]))
            );
            assert_eq!(decode_key(&encode_key(&pair[0])).unwrap(), pair[0]);
        }
    }

    #[test]
    fn value_round_trip_and_malformed_bytes() {
        let row = Row::new(vec![Value::Text(String::from("value"))]);
        assert_eq!(decode_value(encode_value(&row)).unwrap(), row);
        assert!(decode_value(vec![255]).is_err());
        assert!(decode_key(&[0]).is_err());
    }
}
