use actix_web::{web, App, HttpServer};
use ccfs_commons::FileMetadata;
use ccfs_commons::{errors::Error as BaseError, result::CCFSResult};
use metadata_server::jobs::{replication, snapshot};
use metadata_server::routes::api::{
    chunk_server_ping, create_file, get_chunks, get_file, get_server, get_servers, join_cluster,
    signal_chuck_upload_completed,
};
use metadata_server::{errors::*, server_config::ServerConfig};
use metadata_server::{ChunksMap, FileMetadataTree, FilesMap, ServersMap};
use snafu::ResultExt;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
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

    let chunk_servers: ServersMap = Arc::new(RwLock::new(HashMap::new()));
    let chunks: ChunksMap = Arc::new(RwLock::new(HashMap::new()));
    let files: FilesMap = Arc::new(RwLock::new(HashMap::new()));
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

    let address = config.address();
    HttpServer::new(move || {
        App::new()
            .data(chunk_servers.clone())
            .data(chunks.clone())
            .data(files.clone())
            .data(tree.clone())
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
            .service(
                web::scope("/raft")
                    .service(web::resource("/ws/").route(web::get().to(join_cluster))),
            )
    })
    .bind(&address)?
    .run()
    .await
}
