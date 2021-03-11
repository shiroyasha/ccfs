use async_raft::raft::MembershipConfig;
use async_raft::{AppData, AppDataResponse, NodeId};
use ccfs_commons::FileMetadata;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum CCFSAction {
    Add {
        item: FileMetadata,
        target_path: String,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CCFSSnapshot {
    /// The last index covered by this snapshot.
    pub index: u64,
    /// The term of the last index covered by this snapshot.
    pub term: u64,
    /// The last memberhsip config included in this snapshot.
    pub membership: MembershipConfig,
    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct CCFSStateMachine {
    pub last_applied_log: u64,
    pub client_serial_responses: HashMap<Uuid, (Uuid, ClientResponse)>,
    pub tree: FileMetadata,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientRequest {
    /// The ID of the client which has sent the request.
    pub client: Uuid,
    /// The serial number of this request.
    pub serial: Uuid,
    pub action: CCFSAction,
}

impl AppData for ClientRequest {}

/// The application data response type which the `CCFSStorage` works with.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientResponse {
    pub tree: FileMetadata,
}

impl AppDataResponse for ClientResponse {}

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeData {
    id: NodeId,
    server_id: Uuid,
    address: String,
}
