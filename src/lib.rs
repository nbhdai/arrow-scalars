mod scalar;
use arrow::error::ArrowError;
pub use scalar::*;
mod time;
pub use time::*;
mod list;
pub use list::*;
mod arrow_scalars;
pub use crate::arrow_scalars::*;
mod record_batch;
pub use crate::record_batch::*;
mod proto_types;
pub use crate::proto_types::*;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ArrowScalarError {
    #[error("Method `{0}` is not available for type `{1}`")]
    Unimplemented(String, String),
    #[error("Invalid Protobuf")]
    InvalidProtobuf,
    #[error("Out of Bounds Access Error")]
    AccessError,
    #[error("Arrow Error: `{0}`")]
    ArrowError(ArrowError),
}
