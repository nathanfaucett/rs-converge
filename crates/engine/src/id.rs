use serde::{Deserialize, Serialize};
use uuid::Uuid;

macro_rules! generation_id {
    ($name:ident) => {
        #[derive(
            Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
        )]
        pub struct $name(pub Uuid);
    };
}

generation_id!(TableGenerationId);
generation_id!(ColumnGenerationId);
generation_id!(IndexGenerationId);
