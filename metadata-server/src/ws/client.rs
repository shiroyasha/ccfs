use super::cluster::Cluster;
use super::{
    AppendRequest, AppendResponse, Connect, Disconnect, Message as CCFSMessage, RegisterAddress,
    RequestMessage, ResponseMessage, SnapshotRequest, SnapshotResponse, UpdateAddresses,
};
use crate::raft::CCFSRaft;
use actix::io::SinkWrite;
use actix::{prelude::*, StreamHandler};
use actix_codec::Framed;
use awc::{
    error::WsProtocolError,
    ws::{Codec, Frame, Message},
    BoxedSocket,
};
use futures::stream::SplitSink;
use futures::StreamExt;
use std::sync::Arc;

pub struct CCFSWsClient {
    pub id: u64,
    pub peer_id: u64,
    pub address: String,
    pub conn: SinkWrite<Message, SplitSink<Framed<BoxedSocket, Codec>, Message>>,
    pub node: Arc<CCFSRaft>,
    pub cluster: Addr<Cluster>,
}

impl Actor for CCFSWsClient {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.cluster.do_send(Connect {
            id: self.peer_id,
            addr: ctx.address().recipient(),
        });
        self.conn.write(Message::Binary(
            bincode::serialize(&RegisterAddress {
                id: self.id,
                address: self.address.clone(),
            })
            .expect("failed ws serialize")
            .into(),
        ));
    }

    fn stopping(&mut self, _: &mut Self::Context) -> Running {
        self.cluster.do_send(Disconnect { id: self.peer_id });
        Running::Stop
    }
}

impl CCFSWsClient {
    pub fn new(
        id: u64,
        peer_id: u64,
        address: &str,
        framed: Framed<BoxedSocket, Codec>,
        node: Arc<CCFSRaft>,
        cluster: Addr<Cluster>,
    ) -> Addr<Self> {
        let (sink, stream) = framed.split();
        Self::create(|ctx| {
            Self::add_stream(stream, ctx);
            let conn = SinkWrite::new(sink, ctx);
            Self {
                id,
                peer_id,
                address: address.into(),
                conn,
                node,
                cluster,
            }
        })
    }

    fn handle_request(
        &mut self,
        req: RequestMessage,
        node_ref: Arc<CCFSRaft>,
        ctx: &mut Context<Self>,
    ) {
        match req {
            RequestMessage::Append(r) => {
                let AppendRequest {
                    id,
                    req_id,
                    request,
                    ..
                } = r;
                println!("received append request on client {:?}", request);
                async move { node_ref.append_entries(request).await }
                    .into_actor(self)
                    .then(move |res, act, _| {
                        println!("append request exec result client {:?}", res);
                        match res {
                            Ok(response) => {
                                act.conn.write(Message::Binary(
                                    bincode::serialize(&ResponseMessage::Append(AppendResponse {
                                        id,
                                        req_id,
                                        response,
                                    }))
                                    .expect("cannot ser AppendResponse")
                                    .into(),
                                ));
                            }
                            _ => println!("handle append_request failed"),
                        }
                        fut::ready(())
                    })
                    .wait(ctx);
            }
            RequestMessage::Snapshot(r) => {
                let SnapshotRequest {
                    id,
                    request,
                    req_id,
                    ..
                } = r;
                async move { node_ref.install_snapshot(request).await }
                    .into_actor(self)
                    .then(move |res, act, _| {
                        match res {
                            Ok(response) => {
                                act.conn.write(Message::Binary(
                                    bincode::serialize(&ResponseMessage::Snapshot(
                                        SnapshotResponse {
                                            id,
                                            req_id,
                                            response,
                                        },
                                    ))
                                    .expect("cannot ser AppendResponse")
                                    .into(),
                                ));
                            }
                            _ => println!("handle snapshot_request failed"),
                        }
                        fut::ready(())
                    })
                    .wait(ctx);
            }
            _ => {}
        };
    }
}

/// Handle messages from cluster, we simply send it to peer websocket
impl Handler<CCFSMessage> for CCFSWsClient {
    type Result = ();

    fn handle(&mut self, msg: CCFSMessage, _ctx: &mut Context<Self>) {
        let data = match msg {
            CCFSMessage::Request(req) => bincode::serialize(&req).expect("failed ws serialize"),
            CCFSMessage::Response(res) => bincode::serialize(&res).expect("failed ws serialize"),
            CCFSMessage::Register(res) => bincode::serialize(&res).expect("failed ws serialize"),
            CCFSMessage::UpdateAddrs(res) => bincode::serialize(&res).expect("failed ws serialize"),
        };
        self.conn.write(Message::Binary(data.into()));
    }
}

/// Handle server websocket messages
impl StreamHandler<Result<Frame, WsProtocolError>> for CCFSWsClient {
    fn handle(&mut self, msg: Result<Frame, WsProtocolError>, ctx: &mut Context<Self>) {
        if let Ok(Frame::Binary(data)) = msg {
            if let Ok(resp) = bincode::deserialize::<ResponseMessage>(&data) {
                self.cluster.do_send(resp);
            } else if let Ok(req) = bincode::deserialize::<RequestMessage>(&data) {
                self.handle_request(req, Arc::clone(&self.node), ctx);
            } else if let Ok(addr) = bincode::deserialize::<RegisterAddress>(&data) {
                self.cluster.do_send(addr);
            } else if let Ok(addresses) = bincode::deserialize::<UpdateAddresses>(&data) {
                self.cluster.do_send(addresses);
            } else {
                println!("didn't match any above client")
            }
        }
    }

    fn started(&mut self, _ctx: &mut Context<Self>) {
        // println!("Connected");
    }

    fn finished(&mut self, ctx: &mut Context<Self>) {
        // println!("Server disconnected");
        ctx.stop()
    }
}

impl actix::io::WriteHandler<WsProtocolError> for CCFSWsClient {}
