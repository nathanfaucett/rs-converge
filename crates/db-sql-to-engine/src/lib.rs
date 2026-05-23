#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(not(feature = "std"))]
extern crate alloc;

pub mod core;
pub mod engine_adapter;
pub mod ir;
#[allow(clippy::module_inception)]
pub mod translate;

pub use core::Translator;
pub use ir::{CanonicalQuery, CanonicalStatement, DdlOp};
pub use translate::{
  DefaultValueMapper, SchemaResolver, SqlParams, TranslateError, ValueMapper, parse_and_translate,
  parse_and_translate_statement, parse_and_translate_statement_to_ir,
  parse_and_translate_statement_with_params, parse_and_translate_to_ir,
  parse_and_translate_to_ir_with_mapper, parse_and_translate_to_ir_with_params,
  parse_and_translate_with_mapper, parse_and_translate_with_params, translate_statement,
  translate_statement_to_canonical, translate_statement_to_ir,
  translate_statement_to_ir_with_mapper,
};
