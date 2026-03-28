pub mod data;
pub mod index;
pub mod meta;

mod error;
pub use error::Error;

pub type Result<T> = std::result::Result<T, Error>;
