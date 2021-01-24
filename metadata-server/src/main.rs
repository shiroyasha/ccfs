use actix_web::HttpServer;
use ccfs_commons::FileMetadata;
use ccfs_commons::{errors::Error as BaseError, result::CCFSResult};
use metadata_server::jobs::{replication, snapshot};
use metadata_server::FileMetadataTree;
use metadata_server::{errors::*, server_config::ServerConfig};
use snafu::ResultExt;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, RwLock};
use tokio::task;

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
    let config_file_path = env::var("CONFIG_PATH").unwrap_or_else(|_| "./ms_config.yml".into());
    let config = Arc::new(ServerConfig::load_config(&config_file_path)?);

    let chunk_servers = Arc::new(RwLock::new(HashMap::new()));
    let chunks = Arc::new(RwLock::new(HashMap::new()));
    let files = Arc::new(RwLock::new(HashMap::new()));
    let tree = init_metadata_tree(&config.snapshot_path())
        .await
        .unwrap_or_else(|err| panic!("Couldn't init metadata tree: {:?}", err));

    task::spawn_local(snapshot::start_snapshot_job(config.clone(), tree.clone()));
    task::spawn_local(replication::start_replication_job(
        config.replication_interval,
        tree.clone(),
        chunks.clone(),
        chunk_servers.clone(),
    ));

    HttpServer::new(move || {
        metadata_server::create_app(
            chunk_servers.clone(),
            chunks.clone(),
            files.clone(),
            tree.clone(),
        )
    })
    .bind(&config.address())?
    .run()
    .await
}
