pub mod data;
pub mod index;
pub mod meta;
pub mod reader;
pub mod spill;
pub mod writer;

mod error;
pub use error::Error;

pub type Result<T> = std::result::Result<T, Error>;
