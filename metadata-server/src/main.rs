use actix::*;
use actix_web::{web, App, HttpServer};
use async_raft::Config;
use ccfs_commons::http_utils::get_ip;
use ccfs_commons::FileMetadata;
use ccfs_commons::{errors::Error as BaseError, result::CCFSResult};
use metadata_server::jobs::{replication, snapshot};
use metadata_server::raft::network::CCFSNetwork;
use metadata_server::raft::storage::CCFSStorage;
use metadata_server::raft::CCFSRaft;
use metadata_server::routes::api::{
    chunk_server_ping, create_file, get_chunks, get_file, get_server, get_servers,
    signal_chuck_upload_completed,
};
use metadata_server::routes::raft::{bootstrap, join_cluster, vote};
use metadata_server::ws::{cluster::Cluster, SetRaftNode};
use metadata_server::{bootstrap_cluster, ChunksMap, FileMetadataTree, FilesMap, ServersMap};
use metadata_server::{errors::*, server_config::ServerConfig};
use snafu::ResultExt;
use std::collections::{HashMap, HashSet};
use std::env::{self, VarError};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
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
    let bootstrap_size = env::var("BOOTSTRAP_SIZE")
        .and_then(|v| v.parse::<usize>().map_err(|_err| VarError::NotPresent))
        .unwrap_or(0);
    let config = Arc::new(ServerConfig::load_config(&config_file_path)?);
    let server_ip = get_ip().unwrap_or_else(|| "127.0.0.1".into());
    let server_address = format!("http://{}:{}", server_ip, &config.port);

    let chunk_servers: ServersMap = Arc::new(RwLock::new(HashMap::new()));
    let chunks: ChunksMap = Arc::new(RwLock::new(HashMap::new()));
    let files: FilesMap = Arc::new(RwLock::new(HashMap::new()));
    let tree = init_metadata_tree(&config.snapshot_path())
        .await
        .unwrap_or_else(|err| panic!("Couldn't init metadata tree: {:?}", err));

    let raft_config = Config::build("ccfs_metadata_server_cluster".into())
        .election_timeout_max(1500)
        .election_timeout_min(1000)
        .heartbeat_interval(200)
        .replication_lag_threshold(2000)
        .validate()
        .unwrap_or_else(|err| panic!("Couldn't create raft config: {:?}", err));
    let cluster = Cluster::new(config.id, &server_address).start();
    let network = Arc::new(CCFSNetwork::new(cluster.clone()));
    let storage = Arc::new(CCFSStorage::new(config.id));
    let raft_node = Arc::new(CCFSRaft::new(
        config.id,
        Arc::new(raft_config),
        network,
        storage,
    ));

    let bootstrap_cluster_nodes = Arc::new(Mutex::new(HashSet::<u64>::new()));
    cluster.do_send(SetRaftNode {
        node: Arc::clone(&raft_node),
    });

    task::spawn_local(bootstrap_cluster(
        config.id,
        raft_node.clone(),
        server_address,
        cluster.clone(),
        bootstrap_size,
    ));
    // task::spawn_local(snapshot::start_snapshot_job(config.clone(), tree.clone()));
    // task::spawn_local(replication::start_replication_job(
    //     config.replication_interval,
    //     tree.clone(),
    //     chunks.clone(),
    //     chunk_servers.clone(),
    // ));

    let address = config.address();
    HttpServer::new(move || {
        App::new()
            .data(chunk_servers.clone())
            .data(chunks.clone())
            .data(files.clone())
            .data(tree.clone())
            .data(raft_node.clone())
            .data(cluster.clone())
            .data(config.clone())
            .data(bootstrap_cluster_nodes.clone())
            .data(bootstrap_size)
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
                    .service(web::resource("/ws").route(web::get().to(join_cluster)))
                    .service(vote)
                    .service(bootstrap),
            )
    })
    .bind(&address)?
    .run()
    .await
}
