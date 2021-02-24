pub mod client;
pub mod cluster;
pub mod server;

use actix::prelude::*;
use serde::{Deserialize, Serialize};

/// Cluster server sends this message to session
#[derive(Debug, Message, Serialize, Deserialize)]
#[rtype(result = "()")]
pub struct Message(pub String);

/// New session is created
#[derive(Message)]
#[rtype(result = "()")]
pub struct Connect {
    pub id: u64,
    pub addr: Recipient<Message>,
}

/// Session is disconnected
#[derive(Message, Serialize, Deserialize)]
#[rtype(result = "()")]
pub struct Disconnect {
    pub id: u64,
}
