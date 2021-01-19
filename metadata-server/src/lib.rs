pub mod errors;
pub mod jobs;
pub mod routes;

use actix_service::ServiceFactory;
use actix_web::{dev, web, App};
use ccfs_commons::{Chunk, ChunkServer, FileMetadata};
use routes::{
    chunk_server_ping, create_file, get_chunks, get_file, get_server, get_servers,
    signal_chuck_upload_completed,
};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use uuid::Uuid;

pub type ChunkServersMap = Arc<RwLock<HashMap<Uuid, ChunkServer>>>;
pub type ChunksMap = Arc<RwLock<HashMap<Uuid, HashSet<Chunk>>>>;
pub type FilesMap = Arc<RwLock<HashMap<Uuid, (String, FileMetadata)>>>;
pub type FileMetadataTree = Arc<RwLock<FileMetadata>>;

pub fn create_app(
    chunk_servers: ChunkServersMap,
    chunks: ChunksMap,
    files: FilesMap,
    file_metadata_tree: FileMetadataTree,
) -> App<
    impl ServiceFactory<
        Config = (),
        Request = dev::ServiceRequest,
        Response = dev::ServiceResponse<actix_http::body::Body>,
        Error = actix_http::Error,
        InitError = (),
    >,
    dev::Body,
> {
    App::new()
        .data(chunk_servers)
        .data(chunks)
        .data(files)
        .data(file_metadata_tree)
        .service(
            web::scope("/api")
                .service(get_servers)
                .service(get_server)
                .service(chunk_server_ping)
                .service(create_file)
                .service(signal_chuck_upload_completed)
                .service(get_file)
                .service(get_chunks),
        )
}
