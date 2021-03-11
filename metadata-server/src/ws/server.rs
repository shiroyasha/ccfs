use super::cluster::Cluster;
use super::{Connect, Disconnect, Message, NodeAddress, ResponseMessage};
use actix::prelude::*;
use actix::Addr;
use actix_web_actors::ws;

pub struct CCFSWebSocket {
    /// Node id
    node_id: u64,
    /// Cluster server
    addr: Addr<Cluster>,
}

impl Actor for CCFSWebSocket {
    type Context = ws::WebsocketContext<Self>;

    /// Method is called on actor start.
    /// We register ws session with Cluster
    fn started(&mut self, ctx: &mut Self::Context) {
        println!("new connection {}", self.node_id);
        let addr = ctx.address();
        self.addr
            .send(Connect {
                id: self.node_id,
                addr: addr.recipient(),
            })
            .into_actor(self)
            .then(|res, _act, ctx| {
                match res {
                    Ok(_res) => {}
                    // something is wrong with cluster server
                    _ => ctx.stop(),
                }
                fut::ready(())
            })
            .wait(ctx);
    }

    fn stopping(&mut self, _: &mut Self::Context) -> Running {
        // notify cluster server
        self.addr.do_send(Disconnect { id: self.node_id });
        Running::Stop
    }
}

/// Handle messages from cluster server, we simply send it to peer websocket
impl Handler<Message> for CCFSWebSocket {
    type Result = ();

    fn handle(&mut self, msg: Message, ctx: &mut Self::Context) {
        match msg {
            Message::Request(req) => {
                ctx.binary(bincode::serialize(&req).expect("failed to serialize ws msg"));
            }
            Message::Text(text) => {
                ctx.text(text.0);
            }
        }
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
            ws::Message::Pong(_) => {}
            ws::Message::Text(text) => {
                let content = text.to_string();
                let parts = content.split_terminator('|').collect::<Vec<_>>();
                self.addr.do_send(NodeAddress {
                    id: parts[0].parse().unwrap(),
                    address: parts[1].to_string(),
                })
            }
            ws::Message::Binary(data) => match bincode::deserialize::<ResponseMessage>(&data) {
                Ok(resp) => {
                    self.addr.do_send(resp);
                }
                Err(err) => println!("couldn't deser response message: {}", err),
            },
            ws::Message::Close(reason) => {
                ctx.close(reason);
                ctx.stop();
            }
            ws::Message::Continuation(_) => {
                ctx.stop();
            }
            ws::Message::Nop => (),
        }
    }
}

impl CCFSWebSocket {
    pub fn new(node_id: u64, addr: Addr<Cluster>) -> Self {
        Self { node_id, addr }
    }
}
