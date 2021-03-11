use std::collections::HashSet;
use std::sync::Arc;

use crate::raft::CCFSRaft;
use crate::ws::cluster::Cluster;
use crate::ws::server::CCFSWebSocket;
use actix::Addr;
use actix_web::web::{Data, Json, Payload};
use actix_web::{post, Error, HttpRequest, HttpResponse};
use actix_web_actors::ws;
use async_raft::raft::VoteRequest;
use tokio::sync::Mutex;

/// Registers the node to the cluster by openning a ws connection to the leader
pub async fn join_cluster(
    request: HttpRequest,
    srv: Data<Addr<Cluster>>,
    stream: Payload,
) -> Result<HttpResponse, Error> {
    let id = request
        .headers()
        .get("node_id")
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    ws::start(
        CCFSWebSocket::new(id, srv.get_ref().clone()),
        &request,
        stream,
    )
}

/// Registers a new active chunk server or updates the latest_ping_time
#[post("/vote")]
pub async fn vote(rpc: Json<VoteRequest>, raft_node: Data<Arc<CCFSRaft>>) -> HttpResponse {
    match raft_node.vote(rpc.into_inner()).await {
        Ok(resp) => HttpResponse::Ok().json(&resp),
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}

/// Registers a new active chunk server or updates the latest_ping_time
#[post("/bootstrap")]
pub async fn bootstrap(
    request: HttpRequest,
    raft_node: Data<Arc<CCFSRaft>>,
    bootstrap_cluster_nodes: Data<Arc<Mutex<HashSet<u64>>>>,
    bootstrap_size: Data<usize>,
) -> HttpResponse {
    let id = request
        .headers()
        .get("node_id")
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    let mut nodes = bootstrap_cluster_nodes.lock().await;
    nodes.insert(id);
    if nodes.len() < **bootstrap_size {
        HttpResponse::InternalServerError().body("waiting for all nodes to connect".to_string())
    } else {
        HttpResponse::Ok().json(&*nodes)
    }
}
