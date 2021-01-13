use crate::errors::*;
use crate::{ChunkServersMap, ChunksMap, FilesMap};
use actix_web::client::Client;
use ccfs_commons::result::CCFSResult;
use ccfs_commons::FileStatus;
use futures::future::{join_all, FutureExt, LocalBoxFuture};
use std::collections::HashSet;
use tokio::time::{delay_for, Duration};

pub async fn start_replication_job(files: FilesMap, chunks: ChunksMap, servers: ChunkServersMap) {
    loop {
        delay_for(Duration::from_secs(20)).await;
        if let Err(err) = replicate_chunks(files.clone(), chunks.clone(), servers.clone(), 3).await
        {
            // TODO: replace with logger
            println!("Error while creating replicas: {:?}", err);
        } else {
            println!("Successfully created replicas");
        }
    }
}

fn replicate_chunks(
    files_map: FilesMap,
    chunks_map: ChunksMap,
    servers_map: ChunkServersMap,
    required_replicas: usize,
) -> LocalBoxFuture<'static, CCFSResult<()>> {
    let c = Client::new();
    async move {
        let files = files_map.read().map_err(|_| ReadLock.build())?.clone();
        let chunks = chunks_map.read().map_err(|_| ReadLock.build())?.clone();
        let servers = servers_map.read().map_err(|_| ReadLock.build())?.clone();
        println!("files {:?}", files);
        println!("chunks {:?}", chunks);
        println!("servers {:?}", servers);

        let active_files = files.values().filter(|f| f.status == FileStatus::Completed);
        let active_servers = servers
            .iter()
            .filter_map(|(id, s)| match s.is_active() {
                true => Some(id),
                false => None,
            })
            .collect::<HashSet<_>>();
        for f in active_files {
            for chunk in f.chunks.iter() {
                if let Some(replicas) = chunks.get(chunk) {
                    let active_replicas = replicas
                        .iter()
                        .filter(|c| active_servers.contains(&c.server_id))
                        .map(|c| &c.server_id)
                        .collect::<HashSet<_>>();
                    if active_replicas.len() < required_replicas {
                        let mut remaining = required_replicas - active_replicas.len();
                        let server_candidates = &active_servers - &active_replicas;
                        if !server_candidates.is_empty() {
                            let mut iter = server_candidates.iter().peekable();
                            while remaining > 0 && iter.peek().is_some() {
                                let requests = (0..remaining).filter_map(|_| {
                                    let s_id = iter.next()?;
                                    let server = servers.get(s_id)?;
                                    Some(
                                        c.post(format!("{}/replicate", &server.address))
                                            .header("x-ccfs-chunk-id", "")
                                            .header("x-ccfs-chunk-file-id", "")
                                            .header("x-ccfs-chunk-server-url", "")
                                            .send(),
                                    )
                                });
                                let success_responses = join_all(requests)
                                    .await
                                    .into_iter()
                                    .filter_map(|resp| match resp {
                                        Ok(r) if r.status().is_success() => Some(r),
                                        _ => None,
                                    });
                                remaining -= success_responses.count();
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
    .boxed_local()
}
