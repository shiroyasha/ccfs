use actix_web::HttpServer;
use ccfs_commons::FileMetadata;
use ccfs_commons::{errors::Error as BaseError, result::CCFSResult};
use metadata_server::errors::*;
use metadata_server::jobs::{replication, snapshot};
use metadata_server::FileMetadataTree;
use snafu::ResultExt;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, RwLock};
use tokio::task;

const HOST: &str = "HOST";
const PORT: &str = "PORT";

async fn init_metadata_tree(path: &Path) -> CCFSResult<FileMetadataTree> {
    let tree = match path.exists() {
        true => {
            let file = File::open(path).map_err(|source| BaseError::Read {
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

    let chunk_servers = Arc::new(RwLock::new(HashMap::new()));
    let chunks = Arc::new(RwLock::new(HashMap::new()));
    let files = Arc::new(RwLock::new(HashMap::new()));
    let file_metadata_tree = init_metadata_tree(&snapshot_path)
        .await
        .unwrap_or_else(|err| panic!("Couldn't init metadata tree: {:?}", err));

    task::spawn_local(snapshot::start_snapshot_job(
        upload_path,
        snapshot_path,
        file_metadata_tree.clone(),
    ));
    task::spawn_local(replication::start_replication_job(
        file_metadata_tree.clone(),
        chunks.clone(),
        chunk_servers.clone(),
    ));

    HttpServer::new(move || {
        metadata_server::create_app(
            chunk_servers.clone(),
            chunks.clone(),
            files.clone(),
            file_metadata_tree.clone(),
        )
    })
    .bind(&addr)?
    .run()
    .await
}
