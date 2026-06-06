use core::{
  cmp::Ordering,
  fmt,
  hash::{Hash, Hasher},
};

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
  feature = "automerge",
  derive(autosurgeon::Hydrate, autosurgeon::Reconcile)
)]
#[cfg_attr(
  feature = "wasm",
  derive(tsify::Tsify),
  tsify(into_wasm_abi, from_wasm_abi)
)]
pub enum JsonNumber {
  I64(i64),
  U64(u64),
  F64(f64),
}

impl Eq for JsonNumber {}

impl PartialEq for JsonNumber {
  fn eq(&self, other: &Self) -> bool {
    match (self, other) {
      (JsonNumber::I64(i1), JsonNumber::I64(i2)) => i1 == i2,
      (JsonNumber::U64(u1), JsonNumber::U64(u2)) => u1 == u2,
      (JsonNumber::F64(f1), JsonNumber::F64(f2)) => f1 == f2,
      (JsonNumber::I64(i), JsonNumber::U64(u)) => {
        if *i < 0 {
          false
        } else {
          (*i as u64) == *u
        }
      }
      (JsonNumber::U64(u), JsonNumber::I64(i)) => {
        if *i < 0 {
          false
        } else {
          *u == (*i as u64)
        }
      }
      (JsonNumber::I64(i), JsonNumber::F64(f)) => (*i as f64) == *f,
      (JsonNumber::F64(f), JsonNumber::I64(i)) => *f == (*i as f64),
      (JsonNumber::U64(u), JsonNumber::F64(f)) => (*u as f64) == *f,
      (JsonNumber::F64(f), JsonNumber::U64(u)) => *f == (*u as f64),
    }
  }
}

impl Ord for JsonNumber {
  fn cmp(&self, other: &Self) -> Ordering {
    match (self, other) {
      (JsonNumber::I64(i1), JsonNumber::I64(i2)) => i1.cmp(i2),
      (JsonNumber::U64(u1), JsonNumber::U64(u2)) => u1.cmp(u2),
      (JsonNumber::F64(f1), JsonNumber::F64(f2)) => f1.to_bits().cmp(&f2.to_bits()),
      (JsonNumber::I64(i), JsonNumber::U64(u)) => {
        if *i < 0 {
          Ordering::Less
        } else {
          (*i as u64).cmp(u)
        }
      }
      (JsonNumber::U64(u), JsonNumber::I64(i)) => {
        if *i < 0 {
          Ordering::Greater
        } else {
          u.cmp(&(*i as u64))
        }
      }
      (JsonNumber::I64(i), JsonNumber::F64(f)) => (*i as f64).to_bits().cmp(&f.to_bits()),
      (JsonNumber::F64(f), JsonNumber::I64(i)) => f.to_bits().cmp(&(*i as f64).to_bits()),
      (JsonNumber::U64(u), JsonNumber::F64(f)) => (*u as f64).to_bits().cmp(&f.to_bits()),
      (JsonNumber::F64(f), JsonNumber::U64(u)) => f.to_bits().cmp(&(*u as f64).to_bits()),
    }
  }
}

impl PartialOrd for JsonNumber {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Hash for JsonNumber {
  fn hash<H: Hasher>(&self, state: &mut H) {
    match self {
      JsonNumber::I64(i) => i.hash(state),
      JsonNumber::U64(u) => u.hash(state),
      JsonNumber::F64(f) => f.to_bits().hash(state),
    }
  }
}

macro_rules! impl_json_number_for_integer {
    ($($t:ty),*) => {
        $(
        impl From<$t> for JsonNumber {
            fn from(value: $t) -> Self {
                JsonNumber::I64(value as i64)
            }
        }
        )*
    };
}

impl_json_number_for_integer!(i8, i16, i32, i64, isize, i128);

macro_rules! impl_json_number_for_unsigned_integer {
    ($($t:ty),*) => {
        $(
        impl From<$t> for JsonNumber {
            fn from(value: $t) -> Self {
                JsonNumber::U64(value as u64)
            }
        }
        )*
    };
}

impl_json_number_for_unsigned_integer!(u8, u16, u32, u64, usize, u128);

macro_rules! impl_json_number_for_float {
    ($($t:ty),*) => {
        $(
        impl From<$t> for JsonNumber {
            fn from(value: $t) -> Self {
                JsonNumber::F64(value as f64)
            }
        }
        )*
    };
}

impl_json_number_for_float!(f32, f64);

#[cfg(feature = "serde_json")]
impl From<serde_json::Number> for JsonNumber {
  fn from(num: serde_json::Number) -> Self {
    if num.is_f64() {
      match num.as_f64() {
        Some(f) => JsonNumber::F64(f),
        None => JsonNumber::F64(f64::NAN),
      }
    } else if num.is_i64() {
      match num.as_i64() {
        Some(i) => JsonNumber::I64(i),
        None => JsonNumber::F64(f64::NAN),
      }
    } else if num.is_u64() {
      match num.as_u64() {
        Some(u) => JsonNumber::U64(u),
        None => JsonNumber::F64(f64::NAN),
      }
    } else {
      JsonNumber::F64(f64::NAN)
    }
  }
}

impl fmt::Display for JsonNumber {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      JsonNumber::I64(i) => write!(f, "{}", i),
      JsonNumber::U64(u) => write!(f, "{}", u),
      JsonNumber::F64(fl) => write!(f, "{}", fl),
    }
  }
}
