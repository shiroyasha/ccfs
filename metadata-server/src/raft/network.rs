use super::data::ClientRequest;
use crate::ws::{cluster::Cluster, AppendRequest, SnapshotRequest, VotingRequest};
use actix::Addr;
use anyhow::{anyhow, Result};
use async_raft::async_trait::async_trait;
use async_raft::network::RaftNetwork;
use async_raft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use async_raft::NodeId;
use futures::channel::oneshot;
use uuid::Uuid;

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
        println!("appending entries to node {} : {:?}", target, rpc);
        let (tx, rx) = oneshot::channel();
        let _resp = self
            .cluster
            .send(AppendRequest {
                id: target,
                req_id: Uuid::new_v4(),
                request: rpc,
                tx: Some(tx),
            })
            .await;
        let res = rx
            .await
            .map_err(|err| anyhow!("append_entries Ws Request Cancelled: {}", err));
        println!("append_entries res to node {} : {:?}", target, res);
        res
    }

    async fn install_snapshot(
        &self,
        target: NodeId,
        rpc: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        println!("install snapshot to node {} :  {:?}", target, rpc);
        let (tx, rx) = oneshot::channel();
        let _resp = self
            .cluster
            .send(SnapshotRequest {
                id: target,
                req_id: Uuid::new_v4(),
                request: rpc,
                tx: Some(tx),
            })
            .await;
        let res = rx
            .await
            .map_err(|err| anyhow!("install_snapshot Ws Request Cancelled: {}", err));
        println!("install_snapshot res node {} :  {:?}", target, res);
        res
    }

    async fn vote(&self, target: NodeId, rpc: VoteRequest) -> Result<VoteResponse> {
        println!("vote for node {} :  {:?}", target, rpc);
        let res = self
            .cluster
            .send(VotingRequest {
                id: target,
                request: rpc,
            })
            .await
            .map_err(|err| anyhow!("vote raft error: {}", err))?;
        println!("vote res node {} : {:?}", target, res);
        res
    }
}
