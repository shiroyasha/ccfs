pub mod errors;
pub mod jobs;
pub mod middleware;
pub mod raft;
pub mod routes;
pub mod server_config;
pub mod ws;

use actix::Addr;
use awc::Client as WsClient;
use ccfs_commons::http_utils::get_header;
use ccfs_commons::{Chunk, ChunkServer};
use raft::CCFSRaft;
use reqwest::Client;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use uuid::Uuid;
use ws::client::CCFSWsClient;
use ws::cluster::Cluster;

pub type ServersMap = Arc<RwLock<HashMap<Uuid, ChunkServer>>>;
pub type ChunksMap = HashMap<Uuid, HashSet<Chunk>>;
pub type FilesMap = HashMap<Uuid, String>;

pub async fn update_ds(c: &Client, address: &str) -> Result<String, anyhow::Error> {
    let resp = c
        .get(format!(
            "http://eds:5000/edsservice/register?endpoint={}",
            address.strip_prefix("http://").unwrap()
        ))
        .send()
        .await
        .map_err(|err| anyhow::anyhow!("request failed {}", err))?;
    Ok(resp
        .text()
        .await
        .map_err(|err| anyhow::anyhow!("failed to read response {}", err))?)
}

pub async fn connect_to_cluster(
    id: u64,
    node: Arc<CCFSRaft>,
    address: String,
    cluster: Addr<Cluster>,
    bootstrap_url: Option<String>,
    bootstrap_size: usize,
) {
    let c = Client::new();
    let wsc = Arc::new(WsClient::new());
    let mut connection: Option<Addr<CCFSWsClient>> = None;
    match (bootstrap_url, bootstrap_size) {
        (Some(target_url), target_size) if target_size > 0 => {
            loop {
                match c
                    .post(&format!("{}/raft/bootstrap", target_url))
                    .header("x-ccfs-node-id", id.to_string())
                    .send()
                    .await
                {
                    Ok(resp) if resp.status().is_success() => {
                        let members: HashSet<u64> = resp.json().await.unwrap();
                        let _ = node.initialize(members).await;
                        break;
                    }
                    _ => tokio::time::sleep(Duration::from_secs(1)).await,
                }
            }
            loop {
                connection = connect(
                    wsc.clone(),
                    format!("{}/raft/ws?bootstrap=true", target_url),
                    id,
                    Arc::clone(&node),
                    address.clone(),
                    cluster.clone(),
                    connection,
                )
                .await;
                if connection.is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
        _ => {}
    }
    loop {
        match c.get("http://envoy:10000/raft/leader").send().await {
            Ok(resp) if resp.status().is_success() => {
                connection = connect(
                    wsc.clone(),
                    format!("{}/raft/ws", resp.text().await.unwrap()),
                    id,
                    Arc::clone(&node),
                    address.clone(),
                    cluster.clone(),
                    connection,
                )
                .await;
                if connection.is_some() {
                    let _ = update_ds(&c, &address).await;
                }
            }
            _ => {}
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

pub async fn connect(
    c: Arc<WsClient>,
    url: String,
    id: u64,
    node: Arc<CCFSRaft>,
    address: String,
    cluster: Addr<Cluster>,
    connection: Option<Addr<CCFSWsClient>>,
) -> Option<Addr<CCFSWsClient>> {
    match connection {
        Some(conn) if conn.connected() => Some(conn),
        _ => match c.ws(url).header("x-ccfs-node-id", id).connect().await {
            Ok((resp, framed)) => match get_header(resp.headers(), "x-ccfs-node-id") {
                Ok(node_id) => {
                    let peer_id = node_id.parse().unwrap();
                    Some(CCFSWsClient::new(
                        id, peer_id, &address, framed, node, cluster,
                    ))
                }
                _ => None,
            },
            Err(err) => {
                println!("couldn't connect to cluster: {:?}", err);
                None
            }
        },
    }
}
