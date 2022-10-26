mod scalar;
pub use scalar::*;
mod time;
pub use time::*;
mod list;
pub use list::*;
mod arrow_scalars;
pub use crate::arrow_scalars::*;
mod record_batch;
pub use crate::record_batch::*;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum ArrowScalarError {
    #[error("Method `{0}` is not available for type `{1}`")]
    Unimplemented(String, String),
}
