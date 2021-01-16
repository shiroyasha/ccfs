use crate::{errors::*, FileMetadataTree};
use crate::{ChunkServersMap, ChunksMap};
use actix_web::client::Client;
use ccfs_commons::result::CCFSResult;
use ccfs_commons::{Chunk, ChunkServer, FileInfo, FileMetadata};
use futures::future::{join_all, FutureExt, LocalBoxFuture};
use std::collections::{HashMap, HashSet};
use tokio::time::{delay_for, Duration};
use uuid::Uuid;

pub async fn start_replication_job(
    tree: FileMetadataTree,
    chunks: ChunksMap,
    servers: ChunkServersMap,
) {
    loop {
        delay_for(Duration::from_secs(20)).await;
        if let Err(err) = replicate_files(tree.clone(), chunks.clone(), servers.clone(), 3).await {
            // TODO: replace with logger
            println!("Error while creating replicas: {:?}", err);
        } else {
            println!("Successfully created replicas");
        }
    }
}

fn replicate_files(
    tree: FileMetadataTree,
    chunks_map: ChunksMap,
    servers_map: ChunkServersMap,
    required_replicas: usize,
) -> LocalBoxFuture<'static, CCFSResult<()>> {
    let c = Client::new();
    async move {
        let files_tree = tree.read().map_err(|_| ReadLock.build())?.clone();
        let chunks = chunks_map.read().map_err(|_| ReadLock.build())?.clone();
        let servers = servers_map.read().map_err(|_| ReadLock.build())?.clone();

        let active_servers = servers
            .iter()
            .filter_map(|(id, s)| match s.is_active() {
                true => Some(id),
                false => None,
            })
            .collect::<HashSet<_>>();
        let files = files_tree
            .dfs_iter()
            .filter(|f| matches!(f.file_info, FileInfo::File { .. }));
        let futures = files
            .map(|f| replicate_file(&c, f, &chunks, &active_servers, &servers, required_replicas));
        join_all(futures).await;
        Ok(())
    }
    .boxed_local()
}

async fn replicate_file(
    c: &Client,
    file: &FileMetadata,
    chunks: &HashMap<Uuid, HashSet<Chunk>>,
    active_servers: &HashSet<&Uuid>,
    servers: &HashMap<Uuid, ChunkServer>,
    required_replicas: usize,
) -> CCFSResult<()> {
    if let FileInfo::File {
        chunks: ref file_chunks,
        id,
        ..
    } = &file.file_info
    {
        for chunk in file_chunks.iter() {
            if let Some(replicas) = chunks.get(chunk) {
                let replica_servers = replicas
                    .iter()
                    .filter(|c| active_servers.contains(&c.server_id))
                    .map(|c| &c.server_id)
                    .collect::<HashSet<_>>();
                if !replica_servers.is_empty() && replica_servers.len() < required_replicas {
                    let target_server_candidates = active_servers - &replica_servers;
                    if !target_server_candidates.is_empty() {
                        send_replication_requests(
                            c,
                            servers,
                            &replica_servers,
                            &target_server_candidates,
                            &id,
                            chunk,
                            required_replicas,
                        )
                        .await?;
                    }
                }
            }
        }
    }
    Ok(())
}

async fn send_replication_requests(
    c: &Client,
    servers_map: &HashMap<Uuid, ChunkServer>,
    replica_servers: &HashSet<&Uuid>,
    target_servers: &HashSet<&Uuid>,
    file_id: &Uuid,
    chunk_id: &Uuid,
    required_replicas: usize,
) -> CCFSResult<()> {
    let mut remaining = required_replicas - replica_servers.len();
    let mut active_iter = replica_servers.iter().cycle();
    let mut target_iter = target_servers.iter().peekable();
    while remaining > 0 && target_iter.peek().is_some() {
        let requests = (0..remaining).filter_map(|_| {
            let s_id = target_iter.next()?;
            let from_server = &servers_map.get(active_iter.next()?)?.address;
            let target_server = &servers_map.get(s_id)?.address;
            Some(
                c.post(format!("{}/api/replicate", &from_server))
                    .header("x-ccfs-chunk-id", chunk_id.to_string())
                    .header("x-ccfs-file-id", file_id.to_string())
                    .header("x-ccfs-server-url", target_server.clone())
                    .send(),
            )
        });
        let responses = join_all(requests).await;
        let success_responses = responses.into_iter().filter_map(|resp| match resp {
            Ok(r) if r.status().is_success() => Some(r),
            _ => None,
        });
        remaining -= success_responses.count();
    }
    Ok(())
}
