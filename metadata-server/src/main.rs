mod errors;
mod jobs;
mod routes;

use actix_web::web::scope;
use actix_web::{App, HttpServer};
use ccfs_commons::data::Data;
use ccfs_commons::{errors::Error as BaseError, result::CCFSResult};
use ccfs_commons::{Chunk, ChunkServer, File, FileMetadata};
use errors::*;
use jobs::{replication, snapshot};
use routes::{
    chunk_server_ping, create_file, get_chunks, get_file, get_server, get_servers,
    signal_chuck_upload_completed,
};
use snafu::ResultExt;
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs::File as FileFS;
use std::path::Path;
use std::sync::{Arc, RwLock};
use tokio::task;
use uuid::Uuid;

pub type ChunkServersMap = Arc<RwLock<HashMap<Uuid, ChunkServer>>>;
pub type ChunksMap = Arc<RwLock<HashMap<Uuid, Chunk>>>;
pub type FilesMap = Arc<RwLock<HashMap<Uuid, File>>>;
pub type FileMetadataTree = Arc<RwLock<FileMetadata>>;

const HOST: &str = "HOST";
const PORT: &str = "PORT";

async fn init_metadata_tree(path: &Path) -> CCFSResult<FileMetadataTree> {
    let tree = match path.exists() {
        true => {
            let file = FileFS::open(path).map_err(|source| BaseError::Read {
                path: path.into(),
                source,
            })?;
            bincode::deserialize_from(&file).context(Deserialize)?
        }
        false => FileMetadata::create_root(),
    };
    Ok(Arc::new(RwLock::new(tree)))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let host = env::var(HOST).unwrap_or_else(|_| "127.0.0.1".into());
    let port = env::var(PORT).unwrap_or_else(|_| "8000".into());
    let addr = format!("{}:{}", host, port);

    let upload_path = dirs::home_dir()
        .expect("Couldn't determine home dir")
        .join("ccfs-snapshots");
    let snapshot_path = upload_path.join("snapshot");

    let chunk_servers: Data<ChunkServersMap> = Data::new(Arc::new(RwLock::new(HashMap::new())));
    let chunks: Data<ChunksMap> = Data::new(Arc::new(RwLock::new(HashMap::new())));
    let files: Data<FilesMap> = Data::new(Arc::new(RwLock::new(HashMap::new())));
    let file_metadata_tree: Data<FileMetadataTree> = Data::new(
        init_metadata_tree(&snapshot_path)
            .await
            .unwrap_or_else(|err| panic!("Couldn't init metadata tree: {:?}", err)),
    );

    task::spawn_local(snapshot::start_snapshot_job(
        upload_path,
        snapshot_path,
        file_metadata_tree.inner.clone(),
    ));
    task::spawn_local(replication::start_replication_job(
        files.inner.clone(),
        chunks.inner.clone(),
        chunk_servers.inner.clone(),
    ));

    HttpServer::new(move || {
        App::new()
            .data(chunk_servers.clone())
            .data(chunks.clone())
            .data(files.clone())
            .data(file_metadata_tree.clone())
            .service(
                scope("/api")
                    .service(get_servers)
                    .service(get_server)
                    .service(chunk_server_ping)
                    .service(create_file)
                    .service(signal_chuck_upload_completed)
                    .service(get_file)
                    .service(get_chunks),
            )
    })
    .bind(&addr)?
    .run()
    .await
}
