use super::cluster::Cluster;
use super::{
    AppendRequest, AppendResponse, Connect, Disconnect, Message, RegisterAddress, RequestMessage,
    ResponseMessage, SnapshotRequest, SnapshotResponse, UpdateAddresses,
};
use crate::raft::CCFSRaft;
use actix::prelude::*;
use actix::Addr;
use actix_web_actors::ws;
use std::sync::Arc;

pub struct CCFSWebSocket {
    node_id: u64,
    node: Arc<CCFSRaft>,
    cluster: Addr<Cluster>,
}

impl CCFSWebSocket {
    pub fn new(node_id: u64, cluster: Addr<Cluster>, node: Arc<CCFSRaft>) -> Self {
        Self {
            node_id,
            cluster,
            node,
        }
    }

    fn handle_request(
        &mut self,
        req: RequestMessage,
        node_ref: Arc<CCFSRaft>,
        ctx: &mut ws::WebsocketContext<Self>,
    ) {
        match req {
            RequestMessage::Append(r) => {
                let AppendRequest {
                    id,
                    req_id,
                    request,
                    ..
                } = r;
                println!("received append request on server {:?}", request);
                async move { node_ref.append_entries(request).await }
                    .into_actor(self)
                    .then(move |res, _act, ctx| {
                        println!("append request exec result server {:?}", res);
                        match res {
                            Ok(response) => {
                                ctx.binary(
                                    bincode::serialize(&ResponseMessage::Append(AppendResponse {
                                        id,
                                        req_id,
                                        response,
                                    }))
                                    .expect("cannot ser AppendResponse"),
                                );
                            }
                            _ => {
                                // something is wrong with the server
                                println!("handle append_request failed");
                                ctx.stop();
                            }
                        }
                        fut::ready(())
                    })
                    .wait(ctx);
            }
            RequestMessage::Snapshot(r) => {
                let SnapshotRequest {
                    id,
                    req_id,
                    request,
                    ..
                } = r;
                async move { node_ref.install_snapshot(request).await }
                    .into_actor(self)
                    .then(move |res, _act, ctx| {
                        match res {
                            Ok(response) => {
                                ctx.binary(
                                    bincode::serialize(&ResponseMessage::Snapshot(
                                        SnapshotResponse {
                                            id,
                                            req_id,
                                            response,
                                        },
                                    ))
                                    .expect("cannot ser SnapshotResponse"),
                                );
                            }
                            _ => {
                                // something is wrong with the server
                                println!("handle snapshot_request failed");
                                ctx.stop();
                            }
                        }
                        fut::ready(())
                    })
                    .wait(ctx);
            }
            _ => {}
        };
    }
}

impl Actor for CCFSWebSocket {
    type Context = ws::WebsocketContext<Self>;

    /// Method is called on actor start.
    /// We register ws session with Cluster
    fn started(&mut self, ctx: &mut Self::Context) {
        println!("server: new connection {}", self.node_id);
        let conn = ctx.address();
        self.cluster
            .send(Connect {
                id: self.node_id,
                addr: conn.recipient(),
            })
            .into_actor(self)
            .then(|res, _act, ctx| {
                if res.is_err() {
                    // something is wrong with cluster server
                    ctx.stop();
                }
                fut::ready(())
            })
            .wait(ctx);
    }

    fn stopping(&mut self, _: &mut Self::Context) -> Running {
        // notify cluster server
        self.cluster.do_send(Disconnect { id: self.node_id });
        Running::Stop
    }
}

/// Handle messages from cluster server, we simply send it to peer websocket
impl Handler<Message> for CCFSWebSocket {
    type Result = ();

    fn handle(&mut self, msg: Message, ctx: &mut Self::Context) {
        let data = match msg {
            Message::Request(req) => bincode::serialize(&req).expect("failed ws serialize"),
            Message::Response(res) => bincode::serialize(&res).expect("failed ws serialize"),
            Message::Register(res) => bincode::serialize(&res).expect("failed ws serialize"),
            Message::UpdateAddrs(res) => bincode::serialize(&res).expect("failed ws serialize"),
        };
        ctx.binary(data);
    }
}

/// WebSocket message handler
impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for CCFSWebSocket {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        let msg = match msg {
            Err(_) => {
                ctx.stop();
                return;
            }
            Ok(msg) => msg,
        };

        match msg {
            ws::Message::Ping(msg) => {
                ctx.pong(&msg);
            }
            ws::Message::Binary(data) => {
                if let Ok(resp) = bincode::deserialize::<ResponseMessage>(&data) {
                    self.cluster.do_send(resp);
                } else if let Ok(req) = bincode::deserialize::<RequestMessage>(&data) {
                    self.handle_request(req, Arc::clone(&self.node), ctx);
                } else if let Ok(addr) = bincode::deserialize::<RegisterAddress>(&data) {
                    self.cluster.do_send(addr);
                } else if let Ok(addresses) = bincode::deserialize::<UpdateAddresses>(&data) {
                    self.cluster.do_send(addresses);
                } else {
                    println!("didn't match any above server")
                }
            }
            ws::Message::Close(reason) => {
                ctx.close(reason);
                ctx.stop();
            }
            ws::Message::Continuation(_) => {
                ctx.stop();
            }
            _ => {}
        }
    }
}
