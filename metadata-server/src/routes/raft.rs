use crate::ws::cluster::Cluster;
use crate::ws::server::CCFSWebSocket;
use actix::Addr;
use actix_web::web::Data;
use actix_web::{web, Error, HttpRequest, HttpResponse};
use actix_web_actors::ws;
use web::Payload;

/// Registers the node to the cluster by openning a ws connection to the leader
pub async fn join_cluster(
    request: HttpRequest,
    srv: Data<Addr<Cluster>>,
    stream: Payload,
) -> Result<HttpResponse, Error> {
    let headers = request.headers();
    let id = headers
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
