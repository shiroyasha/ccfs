mod errors;
mod routes;
mod snapshot;

use actix_web::web::scope;
use actix_web::{App, HttpServer};
use ccfs_commons::data::Data;
use ccfs_commons::result::CCFSResult;
use ccfs_commons::{Chunk, ChunkServer, File, FileMetadata};
use errors::*;
use routes::{
    chunk_server_ping, create_file, get_chunks, get_file, get_server, get_servers,
    signal_chuck_upload_completed,
};
use snafu::ResultExt;
use std::collections::HashMap;
use std::fs::File as FileFS;
use std::path::Path;
use std::sync::{Arc, RwLock};
use tokio::task;
use uuid::Uuid;

pub type ChunkServersMap = Arc<RwLock<HashMap<Uuid, ChunkServer>>>;
pub type ChunksMap = Arc<RwLock<HashMap<Uuid, Chunk>>>;
pub type FilesMap = Arc<RwLock<HashMap<Uuid, File>>>;
pub type FileMetadataTree = Arc<RwLock<FileMetadata>>;

async fn init_metadata_tree(path: &Path) -> CCFSResult<FileMetadataTree> {
    let tree = match path.exists() {
        true => {
            let file = FileFS::open(path).context(IORead { path })?;
            bincode::deserialize_from(&file).context(Deserialize)?
        }
        false => FileMetadata::create_root(),
    };
    Ok(Arc::new(RwLock::new(tree)))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let host = "127.0.0.1";
    let port = "8080";
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
