use crate::ws::cluster::Cluster;
use crate::ws::server::CCFSWebSocket;
use crate::{errors::*, ChunksMap, FileMetadataTree, FilesMap, ServersMap};
use actix::Addr;
use actix_web::web::{Data, Path, Payload};
use actix_web::{get, post, web, HttpRequest, HttpResponse};
use actix_web_actors::ws;
use ccfs_commons::path::evaluate_path;
use ccfs_commons::result::CCFSResult;
use ccfs_commons::{Chunk, ChunkServer, FileInfo, FileMetadata, FileStatus, ROOT_DIR};
use chrono::{DateTime, Utc};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;
use web::{Json, Query};

/// Returns a list of available chunk servers where the file chunks can be uploaded
#[get("/servers")]
pub async fn get_servers(servers: Data<ServersMap>) -> CCFSResult<HttpResponse> {
    let servers_map = servers.read().await;
    Ok(HttpResponse::Ok().json(
        servers_map
            .values()
            .filter(|s| s.is_active())
            .cloned()
            .collect::<Vec<ChunkServer>>(),
    ))
}

/// Returns chunk servers data for the server with ID <id>
#[get("/servers/{id}")]
pub async fn get_server(id: Path<Uuid>, servers: Data<ServersMap>) -> CCFSResult<HttpResponse> {
    let servers_map = servers.read().await;
    let server = servers_map.get(&id).ok_or_else(|| NotFound.build())?;
    Ok(HttpResponse::Ok().json(server))
}

/// Registers a new active chunk server or updates the latest_ping_time
#[post("/ping")]
pub async fn chunk_server_ping(
    payload: ChunkServer,
    servers: Data<ServersMap>,
) -> CCFSResult<HttpResponse> {
    let mut servers_map = servers.write().await;
    let server = servers_map.entry(payload.id).or_insert_with(|| payload);
    server.latest_ping_time = DateTime::from_utc(Utc::now().naive_utc(), Utc);
    Ok(HttpResponse::Ok().finish())
}

/// Creates a file entity with basic file info
#[post("/files/upload")]
pub async fn create_file(
    file_info: Json<FileMetadata>,
    params: Query<HashMap<String, String>>,
    files: Data<FilesMap>,
    file_metadata_tree: Data<FileMetadataTree>,
) -> CCFSResult<HttpResponse> {
    let file = file_info.into_inner();
    let mut tree = file_metadata_tree.write().await;
    let target_path = match params.get("path") {
        Some(path) if !path.is_empty() => evaluate_path(ROOT_DIR, &tree, path)?,
        _ => String::new(),
    };
    let target = tree.traverse_mut(&target_path)?;
    match &file.file_info {
        FileInfo::Directory { .. } => {
            target
                .children_mut()?
                .insert(file.name.clone(), file.clone());
        }
        FileInfo::File { id, .. } => {
            let mut files_map = files.write().await;
            files_map.insert(*id, (target_path, file.clone()));
        }
    }
    Ok(HttpResponse::Ok().json(&file))
}

/// Returns the file info
#[get("/files")]
pub async fn get_file(
    params: Query<HashMap<String, String>>,
    file_metadata_tree: Data<FileMetadataTree>,
) -> CCFSResult<HttpResponse> {
    let files_tree = file_metadata_tree.read().await;
    let path = match params.get("path") {
        Some(path) if !path.is_empty() => evaluate_path(ROOT_DIR, &files_tree, path)?,
        _ => String::new(),
    };
    let files = files_tree.traverse(&path)?;
    Ok(HttpResponse::Ok().json(files))
}

/// Notifies the metadata server to mark the chunk as completed
#[post("/chunk/completed")]
pub async fn signal_chuck_upload_completed(
    chunk: Json<Chunk>,
    file_metadata_tree: Data<FileMetadataTree>,
    files: Data<FilesMap>,
    chunks: Data<ChunksMap>,
) -> CCFSResult<HttpResponse> {
    let mut chunks = chunks.write().await;
    let mut files = files.write().await;
    let (path, file) = files
        .get_mut(&chunk.file_id)
        .ok_or_else(|| NotFound.build())?;
    let chunk_set = chunks.entry(chunk.id).or_insert_with(HashSet::new);
    if chunk_set.is_empty() {
        if let FileInfo::File {
            num_of_completed_chunks,
            chunks: file_chunks,
            status,
            ..
        } = &mut file.file_info
        {
            *num_of_completed_chunks += 1;
            if *num_of_completed_chunks == file_chunks.len() {
                *status = FileStatus::Completed;
                let mut tree = file_metadata_tree.write().await;
                let target_dir = tree.traverse_mut(path).map_err(|_| NotFound.build())?;
                target_dir
                    .children_mut()?
                    .insert(file.name.clone(), file.clone());
            }
        }
    }
    chunk_set.insert(*chunk);
    Ok(HttpResponse::Ok().finish())
}

/// Returns the list of servers which contains the
/// uploaded chunks for a file
#[get("/chunks/file/{file_id}")]
pub async fn get_chunks(
    file_id: Path<Uuid>,
    chunks: Data<ChunksMap>,
    files: Data<FilesMap>,
) -> CCFSResult<HttpResponse> {
    let chunks_map = chunks.read().await;
    let files_map = files.read().await;
    let (_, file) = files_map.get(&file_id).ok_or_else(|| NotFound.build())?;
    Ok(HttpResponse::Ok().json(
        file.chunks()?
            .iter()
            .filter_map(|chunk_id| chunks_map.get(chunk_id))
            .map(|set| set.iter().cloned().collect())
            .collect::<Vec<Vec<Chunk>>>(),
    ))
}

/// Registers the node to the cluster by openning a ws connection to the leader
pub async fn join_cluster(
    request: HttpRequest,
    srv: Data<Addr<Cluster>>,
    stream: Payload,
) -> Result<HttpResponse, actix_web::Error> {
    let headers = request.headers();
    let id = headers
        .get("node_id")
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    ws::start(
        CCFSWebSocket::new(id, srv.get_ref().clone()),
        &request,
        stream,
    )
}
