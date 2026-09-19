#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

mod bytes_table;
mod catalog;
mod change;
mod codec;
mod engine;
mod envelope;
mod executor;
mod id;
#[cfg(feature = "in-memory")]
mod in_memory;
mod index;
mod kernel;
mod row_table;
mod schema;

pub use bytes_table::{BytesTable, BytesTableTransaction};
pub use catalog::ENGINE_TABLE_FIELDS_FIELD_COLUMN_ID;
pub use change::{Change, ChangeKey};
pub use codec::{DirectRowCodec, RowCodec};
pub use engine::{Engine, EngineError, EngineResult, UuidProvider};
pub use envelope::{
    Checkpoint, CheckpointRow, EnvelopeHeader, EnvelopeId, EnvelopeOutcome, Frontier, RowTombstone,
    TransactionEnvelope,
};
pub use id::{ColumnGenerationId, IndexGenerationId, TableGenerationId};
#[cfg(feature = "in-memory")]
pub use in_memory::{InMemoryKernel, InMemoryKernelTransaction};
pub use kernel::{Kernel, KernelTransaction};
pub use row_table::RowTable;
pub use schema::SchemaChange;
