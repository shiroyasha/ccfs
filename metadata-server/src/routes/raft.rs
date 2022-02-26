use crate::errors::*;
use crate::raft::CCFSRaft;
use crate::server_config::ServerConfig;
use crate::ws::{cluster::Cluster, server::CCFSWebSocket, GetAddress};
use actix::Addr;
use actix_web::web::{Data, Json, Payload};
use actix_web::{get, post, HttpRequest, HttpResponse, Responder};
use actix_web_actors::ws;
use async_raft::raft::VoteRequest;
use ccfs_commons::errors;
use ccfs_commons::http_utils::get_header;
use ccfs_commons::result::CCFSResult;
use snafu::ResultExt;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Registers the node to the cluster by openning a ws connection to the leader
pub async fn join_cluster(
    request: HttpRequest,
    stream: Payload,
    srv: Data<Addr<Cluster>>,
    raft_node: Data<Arc<CCFSRaft>>,
    config: Data<Arc<ServerConfig>>,
) -> CCFSResult<HttpResponse> {
    let text = get_header(request.headers(), "x-ccfs-node-id")?;
    let id = text.parse::<u64>().context(errors::ParseInt { text })?;
    let resp = ws::start(
        CCFSWebSocket::new(id, srv.get_ref().clone(), raft_node.get_ref().clone()),
        &request,
        stream,
    )
    .context(WsConnect)?;
    Ok(resp
        .with_header(("x-ccfs-node-id", config.id))
        .respond_to(&request))
}

/// Runs the `vote` procedure on the raft.
#[post("/vote")]
pub async fn vote(rpc: Json<VoteRequest>, raft_node: Data<Arc<CCFSRaft>>) -> HttpResponse {
    match raft_node.vote(rpc.into_inner()).await {
        Ok(resp) => HttpResponse::Ok().json(&resp),
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}

/// Bootstraping a new cluster. Returns the list of nodes when all nodes have joined.
#[post("/bootstrap")]
pub async fn bootstrap(
    request: HttpRequest,
    bootstrap_cluster_nodes: Data<Arc<Mutex<HashSet<u64>>>>,
    bootstrap_size: Data<usize>,
) -> CCFSResult<HttpResponse> {
    let text = get_header(request.headers(), "x-ccfs-node-id")?;
    let id = text.parse::<u64>().context(errors::ParseInt { text })?;
    let mut nodes = bootstrap_cluster_nodes.lock().await;
    nodes.insert(id);
    Ok(if nodes.len() < **bootstrap_size {
        HttpResponse::InternalServerError().body("waiting for all nodes to connect".to_string())
    } else {
        HttpResponse::Ok().json(&*nodes)
    })
}

/// Returns leaders address
#[get("/leader")]
pub async fn get_leader_address(
    raft_node: Data<Arc<CCFSRaft>>,
    cluster: Data<Addr<Cluster>>,
    config: Data<Arc<ServerConfig>>,
) -> HttpResponse {
    match raft_node.current_leader().await {
        Some(leader_id) if leader_id != config.id => {
            match cluster.send(GetAddress { id: leader_id }).await {
                Ok(Some(leader_address)) => HttpResponse::Ok().body(leader_address),
                _ => {
                    HttpResponse::InternalServerError().body("No leader at the moment".to_string())
                }
            }
        }
        Some(_) => HttpResponse::Ok().body(config.full_address()),
        None => HttpResponse::InternalServerError().body("No leader at the moment".to_string()),
    }
}
