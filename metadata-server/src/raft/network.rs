use super::data::ClientRequest;
use crate::ws::{cluster::Cluster, AppendRequest, SnapshotRequest, VotingRequest};
use actix::Addr;
use anyhow::Result;
use async_raft::async_trait::async_trait;
use async_raft::network::RaftNetwork;
use async_raft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use async_raft::NodeId;
use futures::channel::oneshot;

pub struct CCFSNetwork {
    // store the active websockets when nodes establish a connection
    cluster: Addr<Cluster>,
}

impl CCFSNetwork {
    pub fn new(cluster: Addr<Cluster>) -> Self {
        Self { cluster }
    }
}

#[async_trait]
impl RaftNetwork<ClientRequest> for CCFSNetwork {
    async fn append_entries(
        &self,
        target: NodeId,
        rpc: AppendEntriesRequest<ClientRequest>,
    ) -> Result<AppendEntriesResponse> {
        println!("appending entries {:?}", rpc);
        let (tx, rx) = oneshot::channel();
        let _resp = self
            .cluster
            .send(AppendRequest {
                id: target,
                request: rpc,
                tx: Some(tx),
            })
            .await;
        let res = rx
            .await
            .map_err(|err| anyhow::anyhow!("append_entries Ws Request Cancelled: {}", err));
        println!("append_entries res {:?}", res);
        res
    }

    async fn install_snapshot(
        &self,
        target: NodeId,
        rpc: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        println!("install snapshot {:?}", rpc);
        let (tx, rx) = oneshot::channel();
        let _resp = self
            .cluster
            .send(SnapshotRequest {
                id: target,
                request: rpc,
                tx: Some(tx),
            })
            .await;
        let res = rx
            .await
            .map_err(|err| anyhow::anyhow!("install_snapshot Ws Request Cancelled: {}", err));
        println!("install_snapshot res {:?}", res);
        res
    }

    async fn vote(&self, target: NodeId, rpc: VoteRequest) -> Result<VoteResponse> {
        println!("vote {:?}", rpc);
        let (tx, rx) = oneshot::channel();
        let resp = self
            .cluster
            .send(VotingRequest {
                id: target,
                request: rpc,
                tx: Some(tx),
            })
            .await;
        println!("{:?}", resp);
        let res = rx
            .await
            .map_err(|err| anyhow::anyhow!("vote Ws Request Cancelled: {}", err));
        println!("vote res {:?}", res);
        res
    }
}
