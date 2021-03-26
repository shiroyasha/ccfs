use actix::*;
use actix_web::{web, App, HttpServer};
use async_raft::Config;
use metadata_server::middleware::RedirectToLeader;
// use metadata_server::jobs::replication;
use metadata_server::raft::network::CCFSNetwork;
use metadata_server::raft::storage::CCFSStorage;
use metadata_server::raft::CCFSRaft;
use metadata_server::routes::api::{
    chunk_server_ping, create_file, get_chunks, get_file, get_server, get_servers,
    signal_chuck_upload_completed,
};
use metadata_server::routes::raft::{bootstrap, get_leader_address, join_cluster, vote};
use metadata_server::server_config::ServerConfig;
use metadata_server::ws::{cluster::Cluster, SetRaftNode};
use metadata_server::{connect_to_cluster, ServersMap};
use std::collections::{HashMap, HashSet};
use std::env::{self, VarError};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio::task;

// async fn init_metadata_tree(path: &Path) -> CCFSResult<FileMetadata> {
//     let tree = match path.exists() {
//         true => {
//             let file = File::open(path).context(errors::Read { path })?;
//             bincode::deserialize_from(&file).context(Deserialize)?
//         }
//         false => FileMetadata::create_root(),
//     };
//     Ok(tree)
// }

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let config_file_path = env::var("CONFIG_PATH").unwrap_or_else(|_| "./ms_config.yml".into());
    let bootstrap_url = env::var("BOOTSTRAP_URL").ok();
    let bootstrap_size = env::var("BOOTSTRAP_SIZE")
        .and_then(|v| v.parse::<usize>().map_err(|_err| VarError::NotPresent))
        .unwrap_or(0);
    let config = Arc::new(ServerConfig::load_config(&config_file_path)?);

    let chunk_servers: ServersMap = Arc::new(RwLock::new(HashMap::new()));
    let raft_config = Config::build("ccfs_metadata_server_cluster".into())
        .election_timeout_max(1500)
        .election_timeout_min(1000)
        .heartbeat_interval(200)
        .replication_lag_threshold(2000)
        .validate()
        .unwrap_or_else(|err| panic!("Couldn't create raft config: {:?}", err));
    let server_address = config.full_address();
    let cluster = Cluster::new(config.id, &server_address).start();
    let network = Arc::new(CCFSNetwork::new(cluster.clone()));
    let storage = Arc::new(CCFSStorage::new(config.id));
    let raft_node = Arc::new(CCFSRaft::new(
        config.id,
        Arc::new(raft_config),
        network,
        storage.clone(),
    ));

    let bootstrap_cluster_nodes = Arc::new(Mutex::new(HashSet::<u64>::new()));
    cluster.do_send(SetRaftNode {
        node: Arc::clone(&raft_node),
    });

    task::spawn_local(connect_to_cluster(
        config.id,
        raft_node.clone(),
        server_address,
        cluster.clone(),
        bootstrap_url,
        bootstrap_size,
    ));
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
            .data(raft_node.clone())
            .data(storage.clone())
            .data(cluster.clone())
            .data(config.clone())
            .data(bootstrap_cluster_nodes.clone())
            .data(bootstrap_size)
            .service(
                web::scope("/api")
                    .wrap(RedirectToLeader)
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
                    .service(bootstrap)
                    .service(get_leader_address),
            )
    })
    .bind(&address)?
    .run()
    .await
}
