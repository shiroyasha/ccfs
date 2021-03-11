use super::cluster::Cluster;
use super::{
    AppendResponse, RequestMessage, ResponseMessage, SnapshotResponse, TextMessage, VotingResponse,
};
use crate::raft::CCFSRaft;
use actix::io::SinkWrite;
use actix::{prelude::*, Context, StreamHandler};
use actix_codec::Framed;
use awc::{
    error::WsProtocolError,
    ws::{Codec, Frame, Message},
    BoxedSocket,
};
use bytes::Bytes;
use futures::stream::SplitSink;
use std::sync::Arc;

pub struct CCFSWsClient {
    pub id: u64,
    pub address: String,
    pub conn: SinkWrite<Message, SplitSink<Framed<BoxedSocket, Codec>, Message>>,
    pub node: Arc<CCFSRaft>,
    /// Cluster server
    pub cluster: Addr<Cluster>,
}

impl Actor for CCFSWsClient {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Context<Self>) {
        println!("Connected");
        self.conn.write(Message::Text(
            format!("{}|{}", self.id, self.address).into(),
        ));
    }

    fn stopped(&mut self, _: &mut Context<Self>) {
        println!("Disconnected");
    }
}

impl CCFSWsClient {
    pub fn new(
        id: u64,
        address: &str,
        conn: SinkWrite<Message, SplitSink<Framed<BoxedSocket, Codec>, Message>>,
        node: Arc<CCFSRaft>,
        cluster: Addr<Cluster>,
    ) -> Self {
        Self {
            id,
            address: address.into(),
            conn,
            node,
            cluster,
        }
    }

    fn handle_request(
        &mut self,
        req: RequestMessage,
        node_ref: Arc<CCFSRaft>,
        ctx: &mut Context<Self>,
    ) {
        match req {
            RequestMessage::Append(r) => {
                let id = r.id;
                async move { node_ref.append_entries(r.request).await }
                    .into_actor(self)
                    .then(move |res, act, ctx| {
                        match res {
                            Ok(resp) => {
                                act.conn.write(Message::Binary(Bytes::from(
                                    bincode::serialize(&ResponseMessage::Append(AppendResponse {
                                        id,
                                        response: resp,
                                    }))
                                    .expect("cannot ser AppendResponse"),
                                )));
                            }
                            // something is wrong with the server
                            _ => {
                                println!("handle append_request failed");
                                ctx.stop();
                            }
                        }
                        fut::ready(())
                    })
                    .wait(ctx);
            }
            RequestMessage::Snapshot(r) => {
                let id = r.id;
                async move { node_ref.install_snapshot(r.request).await }
                    .into_actor(self)
                    .then(move |res, act, ctx| {
                        match res {
                            Ok(resp) => {
                                act.conn.write(Message::Binary(Bytes::from(
                                    bincode::serialize(&ResponseMessage::Snapshot(
                                        SnapshotResponse { id, response: resp },
                                    ))
                                    .expect("cannot ser AppendResponse"),
                                )));
                            }
                            // something is wrong with the server
                            _ => {
                                println!("handle snapshot_request failed");
                                ctx.stop();
                            }
                        }
                        fut::ready(())
                    })
                    .wait(ctx);
            }
            RequestMessage::Vote(r) => {
                let id = r.id;
                async move { node_ref.vote(r.request).await }
                    .into_actor(self)
                    .then(move |res, act, ctx| {
                        match res {
                            Ok(resp) => {
                                act.conn.write(Message::Binary(Bytes::from(
                                    bincode::serialize(&ResponseMessage::Vote(VotingResponse {
                                        id,
                                        response: resp,
                                    }))
                                    .expect("cannot ser AppendResponse"),
                                )));
                            }
                            // something is wrong with the server
                            _ => {
                                println!("handle vote_request failed");
                                ctx.stop();
                            }
                        }
                        fut::ready(())
                    })
                    .wait(ctx);
            }
        };
    }
}

/// Handle server websocket messages
impl StreamHandler<Result<Frame, WsProtocolError>> for CCFSWsClient {
    fn handle(&mut self, msg: Result<Frame, WsProtocolError>, ctx: &mut Context<Self>) {
        match msg {
            Ok(Frame::Binary(data)) => match bincode::deserialize::<RequestMessage>(&data) {
                Ok(req) => {
                    self.handle_request(req, Arc::clone(&self.node), ctx);
                }
                Err(err) => println!("couldn't deser into AppendRequest: {}", err),
            },
            Ok(Frame::Text(bytes)) => {
                self.cluster
                    .do_send(TextMessage(String::from_utf8(bytes.to_vec()).unwrap()));
            }
            _ => {}
        }
    }

    fn started(&mut self, _ctx: &mut Context<Self>) {
        println!("Connected");
    }

    fn finished(&mut self, ctx: &mut Context<Self>) {
        println!("Server disconnected");
        ctx.stop()
    }
}

impl actix::io::WriteHandler<WsProtocolError> for CCFSWsClient {}
