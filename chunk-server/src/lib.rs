mod errors;
pub mod jobs;
pub mod routes;
pub mod server_config;

use std::path::PathBuf;
use uuid::Uuid;

pub type MetadataUrl = String;
pub type ServerID = Uuid;
pub type UploadsDir = PathBuf;
