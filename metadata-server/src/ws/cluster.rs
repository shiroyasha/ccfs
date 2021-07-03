//! `Cluster` is an actor. It maintains the connection sessions.
//! The leader sends messages to other nodes through `Cluster`.

use super::client::CCFSWsClient;
use super::{
    AppendRequest, Connect, Disconnect, GetAddress, Message, RegisterAddress, RequestMessage,
    ResponseMessage, SetRaftNode, SnapshotRequest, UpdateAddresses, VotingRequest,
};
use crate::raft::CCFSRaft;
use crate::{connect, update_ds};
use actix::prelude::*;
use anyhow::{anyhow, Error};
use async_raft::raft::{AppendEntriesResponse, InstallSnapshotResponse, VoteResponse};
use awc::Client as WsClient;
use futures::channel::oneshot::Sender;
use reqwest::Client;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use uuid::Uuid;

/// `Cluster` manages communication between leader and nodes
pub struct Cluster {
    pub id: u64,
    pub sessions: HashMap<u64, Recipient<Message>>,
    pub addresses: HashMap<u64, String>,
    pub active_append_requests: HashMap<Uuid, Sender<AppendEntriesResponse>>,
    pub active_snapshot_requests: HashMap<Uuid, Sender<InstallSnapshotResponse>>,
    pub node: Option<Arc<CCFSRaft>>,
    pub http_client: Client,
    pub ws_client: Arc<WsClient>,
}

impl Cluster {
    pub fn new(id: u64, address: &str) -> Self {
        let mut addresses = HashMap::new();
        addresses.insert(id, address.into());
        Self {
            id,
            sessions: HashMap::new(),
            addresses,
            active_append_requests: HashMap::new(),
            active_snapshot_requests: HashMap::new(),
            node: None,
            http_client: Client::new(),
            ws_client: Arc::new(WsClient::new()),
        }
    }
}

impl Cluster {
    fn update_ds(&self, address: String, ctx: &mut Context<Self>) {
        let c = self.http_client.clone();
        async move { update_ds(&c, &address).await }
            .into_actor(self)
            .then(|res: Result<String, Error>, _act, _ctx| {
                if let Err(err) = res {
                    println!("Failed to register address: {}", err);
                }
                fut::ready(())
            })
            .wait(ctx);
    }

    fn send_to_remote_node(&mut self, msg: RequestMessage, ctx: &mut Context<Self>) {
        let id = match &msg {
            RequestMessage::Append(req) => (req.id),
            RequestMessage::Snapshot(req) => (req.id),
            _ => panic!("unexpected request message"),
        };
        if !matches!(self.sessions.get(&id), Some(session) if session.connected()) {
            if let (Some(url), Some(node_ref), Some(address)) = (
                self.addresses.get(&id),
                &self.node,
                self.addresses.get(&self.id),
            ) {
                let cluster = ctx.address();
                let c = self.ws_client.clone();
                let cluster_id = self.id;
                let url = format!("{}/raft/ws", url);
                let node_ref = Arc::clone(node_ref);
                let address = address.clone();
                async move {
                    (
                        id,
                        connect(c, url, cluster_id, node_ref, address, cluster, None).await,
                    )
                }
                .into_actor(self)
                .then(|(id, conn): (u64, Option<Addr<CCFSWsClient>>), act, _ctx| {
                    match conn {
                        Some(addr) => {
                            act.sessions.insert(id, addr.recipient());
                            println!("created conn between 2 non leaders");
                            send_request(act, msg, _ctx);
                        }
                        // something is wrong with the server
                        _ => println!("Failed to open ws connection"),
                    };
                    fut::ready(())
                })
                .wait(ctx);
                return;
            }
        }
        send_request(self, msg, ctx);
    }
}

pub fn send_request(cluster: &mut Cluster, msg: RequestMessage, ctx: &mut Context<Cluster>) {
    match msg {
        RequestMessage::Append(req) => send_append_request(cluster, req, ctx),
        RequestMessage::Snapshot(req) => send_snapshot_request(cluster, req, ctx),
        _ => panic!("unexpected request message"),
    }
}

/// Send append request message to a node
fn send_append_request(
    cluster: &mut Cluster,
    mut message: AppendRequest,
    _: &mut Context<Cluster>,
) {
    cluster.active_append_requests.insert(
        message.req_id,
        message.tx.take().expect("no sender provided"),
    );
    if let Some(conn) = cluster.sessions.get(&message.id) {
        println!("sending append to node {:?}", message.id);
        let res = conn.do_send(Message::Request(RequestMessage::Append(message)));
        if let Err(err) = res {
            println!("send_append_request failed: {}", err);
        }
    } else {
        println!("no connection to send append to node {:?}", message.id);
    }
}

/// Send snapshot message to a node
fn send_snapshot_request(
    cluster: &mut Cluster,
    mut message: SnapshotRequest,
    _: &mut Context<Cluster>,
) {
    cluster.active_snapshot_requests.insert(
        message.req_id,
        message.tx.take().expect("no sender provided"),
    );
    if let Some(conn) = cluster.sessions.get(&message.id) {
        let res = conn.do_send(Message::Request(RequestMessage::Snapshot(message)));
        if let Err(err) = res {
            println!("send_snapshot_request failed: {}", err);
        }
    }
}

impl Actor for Cluster {
    type Context = Context<Self>;
}
impl Supervised for Cluster {}

/// Handler for SetRaftNode message.
///
/// Set the provided raft node reference to the cluster
impl Handler<SetRaftNode> for Cluster {
    type Result = ();

    fn handle(&mut self, msg: SetRaftNode, _: &mut Context<Self>) {
        self.node = Some(msg.node);
    }
}

/// Handler for RegisterAddress message.
///
/// Set the provided raft node reference to the cluster
impl Handler<RegisterAddress> for Cluster {
    type Result = ();

    fn handle(&mut self, msg: RegisterAddress, _: &mut Context<Self>) {
        self.addresses.insert(msg.id, msg.address);
        for (id, conn) in &self.sessions {
            if id != &self.id && conn.connected() {
                let _ = conn.do_send(Message::UpdateAddrs(UpdateAddresses {
                    req_id: Uuid::new_v4(),
                    addresses: self.addresses.clone(),
                }));
            }
        }
    }
}

/// Handler for UpdateAddresses message.
///
/// Set the provided raft node reference to the cluster
impl Handler<UpdateAddresses> for Cluster {
    type Result = ();

    fn handle(&mut self, msg: UpdateAddresses, ctx: &mut Context<Self>) {
        let UpdateAddresses { addresses, .. } = msg;
        for (id, address) in addresses {
            self.addresses.insert(id, address.clone());
            self.update_ds(address, ctx);
        }
    }
}

/// Handler for Address message.
///
/// Return the address for the node with id
impl Handler<GetAddress> for Cluster {
    type Result = MessageResult<GetAddress>;

    fn handle(&mut self, msg: GetAddress, _: &mut Context<Self>) -> Self::Result {
        MessageResult(self.addresses.get(&msg.id).cloned())
    }
}

/// Handler for Connect message.
///
/// Register new session
impl Handler<Connect> for Cluster {
    type Result = ();

    fn handle(&mut self, msg: Connect, ctx: &mut Context<Self>) {
        let Connect { id, addr } = msg;
        self.sessions.insert(id, addr);

        let new_membership = self.sessions.keys().cloned().collect::<HashSet<_>>();
        if let Some(node) = &self.node {
            let node = node.clone();
            let fut = actix::fut::wrap_future(async move {
                if let Err(err) = node.add_non_voter(id).await {
                    println!("couldn't add voter: {}", err);
                }
                if let Err(err) = node.change_membership(new_membership).await {
                    println!("couldn't change membership: {}", err);
                }
            });
            ctx.wait(fut);
        }
    }
}

/// Handler for Disconnect message.
impl Handler<Disconnect> for Cluster {
    type Result = ();

    fn handle(&mut self, msg: Disconnect, _: &mut Context<Self>) {
        self.sessions.remove(&msg.id);
    }
}

/// Handler for AppendRequest message.
impl Handler<AppendRequest> for Cluster {
    type Result = ();

    fn handle(&mut self, req: AppendRequest, ctx: &mut Context<Self>) {
        println!("raft log append {:?}", self.node.clone().unwrap().metrics());
        println!("addresses {:?}", self.addresses);
        println!(
            "sessions {:?}",
            self.sessions
                .iter()
                .map(|(id, s)| (id, s.connected()))
                .collect::<HashMap<_, _>>()
        );
        self.send_to_remote_node(RequestMessage::Append(req), ctx);
    }
}

/// Handler for SnapshotRequest message.
impl Handler<SnapshotRequest> for Cluster {
    type Result = ();

    fn handle(&mut self, req: SnapshotRequest, ctx: &mut Context<Self>) {
        self.send_to_remote_node(RequestMessage::Snapshot(req), ctx);
    }
}

/// Handler for VotingRequest message.
impl Handler<VotingRequest> for Cluster {
    type Result = ResponseFuture<Result<VoteResponse, Error>>;

    fn handle(&mut self, req: VotingRequest, _: &mut Context<Self>) -> Self::Result {
        let VotingRequest { id, request } = req;
        let c = self.http_client.clone();
        let addr = self.addresses.get(&id).cloned();
        Box::pin(async move {
            let addr = addr.ok_or_else(|| anyhow!("no address for this node"))?;
            let url = format!("{}/raft/vote", addr);
            let resp = c
                .post(&url)
                .json(&request)
                .send()
                .await
                .map_err(|err| anyhow!("request failed {}", err))?
                .json()
                .await
                .map_err(|err| anyhow!("parse to json failed {}", err))?;
            Ok(resp)
        })
    }
}

/// Handler for ResponseMessage message.
impl Handler<ResponseMessage> for Cluster {
    type Result = ();

    fn handle(&mut self, response: ResponseMessage, _ctx: &mut Context<Self>) {
        println!("raft log {:?}", self.node.clone().unwrap().metrics());
        match response {
            ResponseMessage::Append(msg) => {
                println!("Received append response {:?}", msg.response);
                if let Some(tx) = self.active_append_requests.remove(&msg.req_id) {
                    match tx.send(msg.response) {
                        Ok(_) => {}
                        Err(data) => println!("couldn't send to rx channel: {:?}", data),
                    }
                }
            }
            ResponseMessage::Snapshot(msg) => {
                if let Some(tx) = self.active_snapshot_requests.remove(&msg.req_id) {
                    match tx.send(msg.response) {
                        Ok(_) => {}
                        Err(data) => println!("couldn't send to rx channel: {:?}", data),
                    }
                }
            }
            _ => {}
        }
    }
}
