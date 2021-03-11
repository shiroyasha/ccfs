pub mod client;
pub mod cluster;
pub mod server;

use crate::raft::data::ClientRequest;
use crate::raft::CCFSRaft;
use actix::prelude::*;
use async_raft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use futures::channel::oneshot::Sender;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};
use std::sync::Arc;

/// Leader sends this message to followers
#[derive(Message, Serialize, Deserialize)]
#[rtype(result = "()")]
pub enum Message {
    Text(TextMessage),
    Request(RequestMessage),
}

/// Leader sends this message to followers
#[derive(Message, Serialize, Deserialize)]
#[rtype(result = "()")]
pub struct TextMessage(pub String);

/// Leader sends this message to followers
#[derive(Message, Serialize, Deserialize)]
#[rtype(result = "()")]
pub enum RequestMessage {
    Append(AppendRequest),
    Snapshot(SnapshotRequest),
    Vote(VotingRequest),
}

/// Leader receives this message from followers
#[derive(Message, Serialize, Deserialize)]
#[rtype(result = "()")]
pub enum ResponseMessage {
    Append(AppendResponse),
    Snapshot(SnapshotResponse),
    Vote(VotingResponse),
}

/// Leader receives this message from followers
#[derive(Message, Serialize, Deserialize)]
#[rtype(result = "()")]
pub struct NodeAddress {
    pub id: u64,
    pub address: String,
}
impl Display for NodeAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}|{}", self.id, self.address)
    }
}

/// Update raft node for cluster
#[derive(Message)]
#[rtype(result = "()")]
pub struct SetRaftNode {
    pub node: Arc<CCFSRaft>,
}

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

/// The append entries message
#[derive(Message, Serialize, Deserialize)]
#[rtype(result = "()")]
pub struct AppendRequest {
    /// Id of the target node
    pub id: u64,
    /// Peer message
    pub request: AppendEntriesRequest<ClientRequest>,
    #[serde(skip)]
    pub tx: Option<Sender<AppendEntriesResponse>>,
}

/// Send message
#[derive(Message, Serialize, Deserialize)]
#[rtype(result = "()")]
pub struct AppendResponse {
    /// Id of the target node
    pub id: u64,
    /// Peer message
    pub response: AppendEntriesResponse,
}

/// Send message
#[derive(Message, Serialize, Deserialize)]
#[rtype(result = "()")]
pub struct SnapshotRequest {
    /// Id of the target node
    pub id: u64,
    /// Peer message
    pub request: InstallSnapshotRequest,
    #[serde(skip)]
    pub tx: Option<Sender<InstallSnapshotResponse>>,
}

/// Send message
#[derive(Message, Serialize, Deserialize)]
#[rtype(result = "()")]
pub struct SnapshotResponse {
    /// Id of the target node
    pub id: u64,
    /// Peer message
    pub response: InstallSnapshotResponse,
}

/// Send message
#[derive(Message, Serialize, Deserialize)]
#[rtype(result = "()")]
pub struct VotingRequest {
    /// Id of the target node
    pub id: u64,
    /// Peer message
    pub request: VoteRequest,
    #[serde(skip)]
    pub tx: Option<Sender<VoteResponse>>,
}

/// Send message
#[derive(Message, Serialize, Deserialize)]
#[rtype(result = "()")]
pub struct VotingResponse {
    /// Id of the target node
    pub id: u64,
    /// Peer message
    pub response: VoteResponse,
}
