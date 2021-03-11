//! `Cluster` is an actor. It maintains the connection sessions.
//! The leader sends messages to other nodes through `Cluster`.

use super::{
    AppendRequest, Connect, Disconnect, Message, NodeAddress, RequestMessage, ResponseMessage,
    SetRaftNode, SnapshotRequest, TextMessage, VotingRequest,
};
use crate::raft::CCFSRaft;
use crate::update_ds;
use actix::prelude::*;
use async_raft::raft::{AppendEntriesResponse, InstallSnapshotResponse, VoteResponse};
use awc::Client;
use futures::channel::oneshot::Sender;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// `Cluster` manages communication between leader and nodes
pub struct Cluster {
    pub sessions: HashMap<u64, Recipient<Message>>,
    pub addresses: HashMap<u64, String>,
    pub active_append_requests: HashMap<u64, Sender<AppendEntriesResponse>>,
    pub active_snapshot_requests: HashMap<u64, Sender<InstallSnapshotResponse>>,
    pub node: Option<Arc<CCFSRaft>>,
}

impl Cluster {
    pub fn new(id: u64, address: &str) -> Self {
        let mut addresses = HashMap::new();
        addresses.insert(id, address.into());
        Self {
            sessions: HashMap::new(),
            addresses,
            active_append_requests: HashMap::new(),
            active_snapshot_requests: HashMap::new(),
            node: None,
        }
    }
}

impl Cluster {
    /// Send append request message to a node
    fn send_append_request(&mut self, mut message: AppendRequest, ctx: &mut Context<Self>) {
        self.active_append_requests
            .insert(message.id, message.tx.take().expect("no sender provided"));
        if let Some(conn) = self.sessions.get(&message.id) {
            conn.send(Message::Request(RequestMessage::Append(message)))
                .into_actor(self)
                .then(|res, _act, ctx| {
                    match res {
                        Ok(_response) => {}
                        // something is wrong with the server
                        _ => {
                            println!("send_append_request failed");
                            ctx.stop();
                        }
                    }
                    fut::ready(())
                })
                .wait(ctx);
        }
    }

    /// Send snapshot message to a node
    fn send_snapshot_request(&mut self, mut message: SnapshotRequest, ctx: &mut Context<Self>) {
        self.active_snapshot_requests
            .insert(message.id, message.tx.take().expect("no sender provided"));
        if let Some(conn) = self.sessions.get(&message.id) {
            conn.send(Message::Request(RequestMessage::Snapshot(message)))
                .into_actor(self)
                .then(|res, _act, ctx| {
                    match res {
                        Ok(_response) => {}
                        // something is wrong with the server
                        _ => {
                            println!("send_snapshot_request failed");
                            ctx.stop();
                        }
                    }
                    fut::ready(())
                })
                .wait(ctx);
        }
    }

    /// Send vote message to a node
    fn send_vote_request(&mut self, mut message: VotingRequest, ctx: &mut Context<Self>) {
        println!("addresses: {:?}", self.addresses);
        let tx = message.tx.take().expect("no sender provided");
        if let Some(addr) = self.addresses.get(&message.id) {
            let url = format!("{}/raft/vote", addr);
            async move {
                let c = Client::new();
                let resp = c
                    .post(&url)
                    .send_json(&message.request)
                    .await
                    .map_err(|err| anyhow::anyhow!("request failed {}", err))?
                    .json()
                    .await
                    .map_err(|err| anyhow::anyhow!("parse to json failed {}", err))?;
                println!("got response from {}: {:?}", url, resp);
                Ok(resp)
            }
            .into_actor(self)
            .then(|res: Result<VoteResponse, anyhow::Error>, _act, _ctx| {
                println!("send_vote_request res {:?}", res);
                match res {
                    Ok(response) => {
                        let _ = tx.send(response);
                    }
                    // something is wrong with the server
                    Err(err) => println!("vote request failed: {}", err),
                }
                fut::ready(())
            })
            .wait(ctx);
        }
    }

    fn update_ds(&self, address: String, ctx: &mut Context<Self>) {
        async move { update_ds(&address).await }
            .into_actor(self)
            .then(|res: Result<String, anyhow::Error>, _act, _ctx| {
                println!("update_ds res {:?}", res);
                match res {
                    Ok(_response) => {}
                    // something is wrong with the server
                    Err(err) => println!("Failed to register address: {}", err),
                }
                fut::ready(())
            })
            .wait(ctx);
    }
}

impl Actor for Cluster {
    type Context = Context<Self>;
}

/// Handler for SetRaftNode message.
///
/// Set the provided raft node reference to the cluster
impl Handler<SetRaftNode> for Cluster {
    type Result = ();

    fn handle(&mut self, msg: SetRaftNode, _: &mut Context<Self>) {
        self.node = Some(msg.node);
    }
}

/// Handler for NodeAddress message.
///
/// Set the provided raft node reference to the cluster
impl Handler<NodeAddress> for Cluster {
    type Result = ();

    fn handle(&mut self, msg: NodeAddress, _: &mut Context<Self>) {
        self.addresses.insert(msg.id, msg.address.clone());
        for (id, conn) in &self.sessions {
            if id != &msg.id {
                let _ = conn.do_send(Message::Text(TextMessage(msg.to_string())));
            } else {
                let strings = self
                    .addresses
                    .iter()
                    .map(|(id, addr)| format!("{}|{}", id, addr))
                    .collect::<Vec<_>>();
                let _ = conn.do_send(Message::Text(TextMessage(strings.join(";"))));
            }
        }
    }
}

/// Handler for Connect message.
///
/// Register new session
impl Handler<Connect> for Cluster {
    type Result = ();

    fn handle(&mut self, msg: Connect, ctx: &mut Context<Self>) {
        println!("Someone joined");
        let Connect { id, addr } = msg;
        self.sessions.insert(id, addr);

        let new_membership = self.sessions.keys().cloned().collect::<HashSet<_>>();
        if let Some(node) = &self.node {
            let node = node.clone();
            let fut = actix::fut::wrap_future(async move {
                match node.add_non_voter(id).await {
                    Ok(_) => {}
                    Err(err) => println!("couldn't add voter: {}", err),
                }
                match node.change_membership(new_membership).await {
                    Ok(_) => {}
                    Err(err) => println!("couldn't change membership: {}", err),
                }
            });
            ctx.spawn(fut);
        }
    }
}

/// Handler for Disconnect message.
impl Handler<Disconnect> for Cluster {
    type Result = ();

    fn handle(&mut self, msg: Disconnect, _: &mut Context<Self>) {
        println!("Someone disconnected");
        self.sessions.remove(&msg.id);
    }
}

/// Handler for AppendRequest message.
impl Handler<AppendRequest> for Cluster {
    type Result = ();

    fn handle(&mut self, req: AppendRequest, ctx: &mut Context<Self>) {
        println!("{:?}", self.addresses);
        self.send_append_request(req, ctx)
    }
}

/// Handler for SnapshotRequest message.
impl Handler<SnapshotRequest> for Cluster {
    type Result = ();

    fn handle(&mut self, req: SnapshotRequest, ctx: &mut Context<Self>) {
        self.send_snapshot_request(req, ctx)
    }
}

/// Handler for VotingRequest message.
impl Handler<VotingRequest> for Cluster {
    type Result = ();

    fn handle(&mut self, req: VotingRequest, ctx: &mut Context<Self>) {
        println!("handling voting request");
        self.send_vote_request(req, ctx)
    }
}

/// Handler for TextMessage message.
impl Handler<TextMessage> for Cluster {
    type Result = ();

    fn handle(&mut self, msg: TextMessage, _ctx: &mut Context<Self>) {
        for (id, address) in msg.0.split_terminator(';').filter_map(|s| {
            let parts = s.split_terminator('|').collect::<Vec<_>>();
            Some((parts[0].parse::<u64>().ok()?, parts[1].to_string()))
        }) {
            self.addresses.insert(id, address);
            // self.update_ds(address, ctx);
        }
    }
}

/// Handler for ResponseMessage message.
impl Handler<ResponseMessage> for Cluster {
    type Result = ();

    fn handle(&mut self, response: ResponseMessage, _ctx: &mut Context<Self>) {
        match response {
            ResponseMessage::Append(msg) => {
                if let Some(tx) = self.active_append_requests.remove(&msg.id) {
                    match tx.send(msg.response) {
                        Ok(_) => {}
                        Err(data) => println!("couldn't send to rx channel: {:?}", data),
                    }
                }
            }
            ResponseMessage::Snapshot(msg) => {
                if let Some(tx) = self.active_snapshot_requests.remove(&msg.id) {
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
