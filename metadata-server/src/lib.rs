pub mod errors;
pub mod jobs;
pub mod routes;
pub mod server_config;
pub mod ws;

use ccfs_commons::{Chunk, ChunkServer, FileMetadata};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

pub type ServersMap = Arc<RwLock<HashMap<Uuid, ChunkServer>>>;
pub type ChunksMap = Arc<RwLock<HashMap<Uuid, HashSet<Chunk>>>>;
pub type FilesMap = Arc<RwLock<HashMap<Uuid, (String, FileMetadata)>>>;
pub type FileMetadataTree = Arc<RwLock<FileMetadata>>;
