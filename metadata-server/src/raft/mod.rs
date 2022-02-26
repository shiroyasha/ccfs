use async_raft::Raft;
use data::{ClientRequest, ClientResponse};
use network::CCFSNetwork;
use storage::CCFSStorage;

pub mod data;
pub mod network;
pub mod storage;

pub type CCFSRaft = Raft<ClientRequest, ClientResponse, CCFSNetwork, CCFSStorage>;
