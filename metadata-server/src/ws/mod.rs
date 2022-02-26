pub mod client;
pub mod cluster;
pub mod server;

use crate::raft::data::ClientRequest;
use crate::raft::CCFSRaft;
use actix::prelude::*;
use anyhow::Error;
use async_raft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use futures::channel::oneshot::Sender;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Leader sends this message to followers
#[derive(Message, Serialize, Deserialize)]
#[rtype(result = "()")]
pub enum Message {
    Request(RequestMessage),
    Response(ResponseMessage),
    Register(RegisterAddress),
    UpdateAddrs(UpdateAddresses),
}

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
pub struct UpdateAddresses {
    pub req_id: uuid::Uuid,
    pub addresses: HashMap<u64, String>,
}

/// Leader receives this message from followers
#[derive(Message, Serialize, Deserialize, Debug)]
#[rtype(result = "()")]
pub struct RegisterAddress {
    pub id: u64,
    pub address: String,
}

/// Message to request the nodes address from the cluster
#[derive(Message, Serialize, Deserialize)]
#[rtype(result = "Option<String>")]
pub struct GetAddress {
    pub id: u64,
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
    /// Request Id
    pub req_id: uuid::Uuid,
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
    /// Request Id
    pub req_id: uuid::Uuid,
    /// Peer message
    pub response: AppendEntriesResponse,
}

/// Send message
#[derive(Message, Serialize, Deserialize)]
#[rtype(result = "()")]
pub struct SnapshotRequest {
    /// Id of the target node
    pub id: u64,
    /// Request Id
    pub req_id: uuid::Uuid,
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
    /// Request Id
    pub req_id: uuid::Uuid,
    /// Peer message
    pub response: InstallSnapshotResponse,
}

/// Send message
#[derive(Message, Serialize, Deserialize)]
#[rtype(result = "Result<VoteResponse, Error>")]
pub struct VotingRequest {
    /// Id of the target node
    pub id: u64,
    /// Peer message
    pub request: VoteRequest,
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
