//! `Cluster` is an actor. It maintains the connection sessions.
//! The leader sends messages to other nodes through `Cluster`.

use super::{Connect, Disconnect, Message};
use actix::prelude::*;
use std::collections::HashMap;

/// `Cluster` manages communication between leader and nodes
pub struct Cluster {
    pub sessions: HashMap<u64, Recipient<Message>>,
}

impl Default for Cluster {
    fn default() -> Self {
        Self {
            sessions: HashMap::new(),
        }
    }
}

impl Actor for Cluster {
    type Context = Context<Self>;
}

/// Handler for Connect message.
///
/// Register new session in cluster
impl Handler<Connect> for Cluster {
    type Result = ();

    fn handle(&mut self, msg: Connect, _ctx: &mut Context<Self>) {
        println!("Someone joined");
        self.sessions.insert(msg.id, msg.addr);
    }
}

/// Handler for Disconnect message.
impl Handler<Disconnect> for Cluster {
    type Result = ();

    fn handle(&mut self, msg: Disconnect, _: &mut Context<Self>) {
        println!("Someone disconnected");
        // remove address
        self.sessions.remove(&msg.id);
    }
}
