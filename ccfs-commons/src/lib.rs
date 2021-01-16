pub mod data;
pub mod errors;
pub mod http_utils;
pub mod path;
pub mod result;

mod types;
pub use types::*;

pub const CHUNK_SIZE: u64 = 64 * 1024 * 1024;
