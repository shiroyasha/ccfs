pub mod errors;
pub mod jobs;
pub mod raft;
pub mod routes;
pub mod server_config;
pub mod ws;

use actix::io::SinkWrite;
use actix::{Actor, Addr, StreamHandler};
use awc::Client;
use ccfs_commons::http_utils::read_body;
use ccfs_commons::{Chunk, ChunkServer};
use futures::StreamExt;
use raft::CCFSRaft;
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

pub async fn update_ds(address: &str) -> Result<String, anyhow::Error> {
    let c = Client::new();
    let resp = c
        .get(format!(
            "http://eds:5000/edsservice/register?endpoint={}",
            address.strip_prefix("http://").unwrap()
        ))
        .send()
        .await
        .map_err(|err| anyhow::anyhow!("request failed {}", err))?;
    Ok(read_body(resp)
        .await
        .map_err(|err| anyhow::anyhow!("failed to read response {}", err))?)
}

/// try to join cluster, if no response (either leader is down or the
/// cluster is not initialized) -> update the DS to redirect to self
pub async fn bootstrap_cluster(
    id: u64,
    node: Arc<CCFSRaft>,
    address: String,
    cluster: Addr<Cluster>,
    bootstrap_url: Option<String>,
    bootstrap_size: usize,
) {
    let c = Arc::new(Client::new());
    match (bootstrap_url, bootstrap_size) {
        (Some(url), target_size) if target_size > 0 => loop {
            println!(
                "calling bootstrap endpoint, addr {}",
                format!("{}/raft/bootstrap", url)
            );
            match c
                .post(&format!("{}/raft/bootstrap", url))
                .insert_header(("node_id", id.to_string()))
                .send()
                .await
            {
                Ok(mut resp) if resp.status().is_success() => {
                    println!("resp status {}", resp.status());
                    let members: HashSet<u64> = resp.json().await.unwrap();
                    let _ = node.initialize(members).await;
                    break;
                }
                Ok(resp) => {
                    println!("couldn't bootstrap cluster yet {:?}", read_body(resp).await);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Err(err) => {
                    println!("couldn't bootstrap cluster yet {}", err);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        },
        _ => {}
    }
    let mut connection: Option<Addr<CCFSWsClient>> = None;
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        println!("{:?}", node.metrics());
        if let Some(conn) = &connection {
            if !conn.connected() {
                connection = connect_to_leader(&c, id, &node, &address, cluster.clone()).await;
            }
        } else {
            connection = connect_to_leader(&c, id, &node, &address, cluster.clone()).await;
        }
    }
}

pub async fn connect_to_leader(
    c: &Arc<Client>,
    id: u64,
    node: &Arc<CCFSRaft>,
    address: &str,
    cluster: Addr<Cluster>,
) -> Option<Addr<CCFSWsClient>> {
    match c.get("http://envoy:10000/api/servers").send().await {
        Ok(resp) => println!("get servers resp: {:?}", read_body(resp).await),
        Err(err) => println!("get servers err: {:?}", err),
    };
    match c
        .ws("http://envoy:10000/raft/ws")
        .header("node_id", id)
        .connect()
        .await
    {
        Ok((_resp, framed)) => {
            let (sink, stream) = framed.split();
            let addr = CCFSWsClient::create(|ctx| {
                CCFSWsClient::add_stream(stream, ctx);
                CCFSWsClient::new(
                    id,
                    address,
                    SinkWrite::new(sink, ctx),
                    Arc::clone(node),
                    cluster,
                )
            });
            return Some(addr);
        }
        Err(err) => {
            println!("couldn't connect to cluster: {}", err);
            // update ds
            None
        }
    }
}
