#[macro_use]
extern crate rocket;
#[macro_use]
extern crate rocket_contrib;
extern crate ccfs_commons;

mod routes;

use ccfs_commons::{Chunk, ChunkServer, File, FileMetadata};
use rocket_contrib::uuid::uuid_crate::Uuid;
use routes::{
    chunk_server_ping, create_file, get_chunks, get_file, get_server, get_servers,
    signal_chuck_upload_completed,
};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub type ChunkServersMap = Arc<RwLock<HashMap<Uuid, ChunkServer>>>;
pub type ChunksMap = Arc<RwLock<HashMap<Uuid, Chunk>>>;
pub type FilesMap = Arc<RwLock<HashMap<Uuid, File>>>;
pub type FileMetadataTree = Arc<RwLock<FileMetadata>>;

#[launch]
fn rocket() -> rocket::Rocket {
    let chunk_servers: ChunkServersMap = Arc::new(RwLock::new(HashMap::new()));
    let chunks: ChunksMap = Arc::new(RwLock::new(HashMap::new()));
    let files: FilesMap = Arc::new(RwLock::new(HashMap::new()));
    let file_metadata_tree: FileMetadataTree = Arc::new(RwLock::new(FileMetadata::create_root()));
    rocket::ignite()
        .mount(
            "/api",
            routes![
                get_servers,
                get_server,
                chunk_server_ping,
                create_file,
                signal_chuck_upload_completed,
                get_file,
                get_chunks
            ],
        )
        .manage(chunk_servers)
        .manage(chunks)
        .manage(files)
        .manage(file_metadata_tree)
}
