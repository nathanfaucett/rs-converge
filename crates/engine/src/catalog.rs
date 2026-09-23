pub const ENGINE_TABLE_FIELDS_FIELD_COLUMN_ID: &str = "column_id";

pub const ENGINE_TABLES_STORAGE: uuid::Uuid =
    uuid::Uuid::from_u128(0x5441_424c_4553_0000_0000_0000_0000_0001);
pub const ENGINE_TABLE_FIELDS_STORAGE: uuid::Uuid =
    uuid::Uuid::from_u128(0x5441_424c_4c45_4649_454c_4453_0000_0001);

pub const INTERNAL_INDICES: &str = "__internal_indices";
pub const INTERNAL_ENVELOPES: &str = "__internal_envelopes";
pub const INTERNAL_ENVELOPE_FRONTIER: &str = "__internal_envelope_frontier";
pub const INTERNAL_ENVELOPE_LOG: &str = "__internal_envelope_log";
pub const INTERNAL_ENVELOPE_SEQUENCE: &str = "__internal_envelope_sequence";
pub const INTERNAL_ENVELOPE_STATUS: &str = "__internal_envelope_status";
pub const INTERNAL_ENVELOPE_HEADERS: &str = "__internal_envelope_headers";
pub const INTERNAL_QUARANTINED_ENVELOPES: &str = "__internal_quarantined_envelopes";

#[derive(Clone, Copy)]
pub struct InternalColumn {
    pub name: &'static str,
    pub value_type: value::ValueType,
}

const INDEX_COLUMNS: [InternalColumn; 6] = [
    InternalColumn {
        name: "id",
        value_type: value::ValueType::Uuid,
    },
    InternalColumn {
        name: "label",
        value_type: value::ValueType::Text,
    },
    InternalColumn {
        name: "table",
        value_type: value::ValueType::Uuid,
    },
    InternalColumn {
        name: "unique",
        value_type: value::ValueType::Bool,
    },
    InternalColumn {
        name: "columns",
        value_type: value::ValueType::Blob,
    },
    InternalColumn {
        name: "deleted",
        value_type: value::ValueType::Bool,
    },
];
const ENVELOPE_COLUMNS: [InternalColumn; 2] = [
    InternalColumn {
        name: "id",
        value_type: value::ValueType::Uuid,
    },
    InternalColumn {
        name: "envelope",
        value_type: value::ValueType::Blob,
    },
];
const FRONTIER_COLUMNS: [InternalColumn; 2] = [
    InternalColumn {
        name: "id",
        value_type: value::ValueType::Uuid,
    },
    InternalColumn {
        name: "head",
        value_type: value::ValueType::Blob,
    },
];
const LOG_COLUMNS: [InternalColumn; 3] = [
    InternalColumn {
        name: "id",
        value_type: value::ValueType::Uuid,
    },
    InternalColumn {
        name: "sequence",
        value_type: value::ValueType::Integer,
    },
    InternalColumn {
        name: "envelope",
        value_type: value::ValueType::Blob,
    },
];
const SEQUENCE_COLUMNS: [InternalColumn; 2] = [
    InternalColumn {
        name: "id",
        value_type: value::ValueType::Uuid,
    },
    InternalColumn {
        name: "value",
        value_type: value::ValueType::Integer,
    },
];
const STATUS_COLUMNS: [InternalColumn; 2] = [
    InternalColumn {
        name: "id",
        value_type: value::ValueType::Uuid,
    },
    InternalColumn {
        name: "status",
        value_type: value::ValueType::Blob,
    },
];
const HEADER_COLUMNS: [InternalColumn; 2] = [
    InternalColumn {
        name: "id",
        value_type: value::ValueType::Uuid,
    },
    InternalColumn {
        name: "parents",
        value_type: value::ValueType::Blob,
    },
];
const QUARANTINE_COLUMNS: [InternalColumn; 3] = [
    InternalColumn {
        name: "id",
        value_type: value::ValueType::Uuid,
    },
    InternalColumn {
        name: "bytes",
        value_type: value::ValueType::Blob,
    },
    InternalColumn {
        name: "reason",
        value_type: value::ValueType::Text,
    },
];

pub const INTERNAL_TABLE_NAMES: [&str; 8] = [
    INTERNAL_INDICES,
    INTERNAL_ENVELOPES,
    INTERNAL_ENVELOPE_FRONTIER,
    INTERNAL_ENVELOPE_LOG,
    INTERNAL_ENVELOPE_SEQUENCE,
    INTERNAL_ENVELOPE_STATUS,
    INTERNAL_ENVELOPE_HEADERS,
    INTERNAL_QUARANTINED_ENVELOPES,
];

pub fn is_internal_table_name(name: &str) -> bool {
    INTERNAL_TABLE_NAMES.contains(&name)
}

pub fn internal_table_columns(name: &str) -> &'static [InternalColumn] {
    match name {
        INTERNAL_INDICES => &INDEX_COLUMNS,
        INTERNAL_ENVELOPES => &ENVELOPE_COLUMNS,
        INTERNAL_ENVELOPE_FRONTIER => &FRONTIER_COLUMNS,
        INTERNAL_ENVELOPE_LOG => &LOG_COLUMNS,
        INTERNAL_ENVELOPE_SEQUENCE => &SEQUENCE_COLUMNS,
        INTERNAL_ENVELOPE_STATUS => &STATUS_COLUMNS,
        INTERNAL_ENVELOPE_HEADERS => &HEADER_COLUMNS,
        INTERNAL_QUARANTINED_ENVELOPES => &QUARANTINE_COLUMNS,
        _ => &[],
    }
}

pub fn internal_table_generation(name: &str) -> Option<crate::TableGenerationId> {
    INTERNAL_TABLE_NAMES
        .iter()
        .position(|candidate| *candidate == name)
        .map(|index| {
            let base = 0x494e_5445_524e_414c_0000_0000_0000_0000u128 + (index as u128) * 64;
            crate::TableGenerationId(uuid::Uuid::from_u128(base))
        })
}

pub fn internal_table_storage_uuid(name: &str) -> Option<uuid::Uuid> {
    internal_table_generation(name).map(|generation| generation.0)
}

pub fn is_internal_table_generation(table: crate::TableGenerationId) -> bool {
    INTERNAL_TABLE_NAMES
        .iter()
        .filter_map(|name| internal_table_generation(name))
        .any(|generation| generation == table)
}
