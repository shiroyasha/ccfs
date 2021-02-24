use super::Message as CCFSMessage;
use actix::io::SinkWrite;
use actix::{prelude::*, Context, StreamHandler};
use actix_codec::Framed;
use awc::{
    error::WsProtocolError,
    ws::{Codec, Frame, Message},
    BoxedSocket,
};
use futures::stream::SplitSink;

pub struct CCFSWsClient {
    pub conn: SinkWrite<Message, SplitSink<Framed<BoxedSocket, Codec>, Message>>,
}

impl Actor for CCFSWsClient {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Context<Self>) {
        println!("Connected");
    }

    fn stopped(&mut self, _: &mut Context<Self>) {
        println!("Disconnected");
    }
}

/// Handle server websocket messages
impl StreamHandler<Result<Frame, WsProtocolError>> for CCFSWsClient {
    fn handle(&mut self, msg: Result<Frame, WsProtocolError>, _ctx: &mut Context<Self>) {
        if let Ok(Frame::Binary(data)) = msg {
            match bincode::deserialize::<CCFSMessage>(&data) {
                Ok(req) => println!("Recieved {:?}", req),
                Err(err) => println!("couldn't deser into AppendRequest: {}", err),
            }
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
