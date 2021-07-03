use crate::raft::data::{CCFSAction, ClientRequest, ClientResponse};
use crate::raft::storage::CCFSStorage;
use crate::raft::CCFSRaft;
use crate::{errors::*, ServersMap};
use actix_web::web::{Data, Path};
use actix_web::{get, post, web, HttpRequest, HttpResponse};
use async_raft::raft::ClientWriteRequest;
use ccfs_commons::http_utils::get_header;
use ccfs_commons::path::evaluate_path;
use ccfs_commons::{errors, result::CCFSResult};
use ccfs_commons::{Chunk, ChunkServer, FileMetadata, ROOT_DIR};
use chrono::{DateTime, Utc};
use snafu::ResultExt;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
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
    request: HttpRequest,
    file_info: Json<FileMetadata>,
    params: Query<HashMap<String, String>>,
    raft_node: Data<Arc<CCFSRaft>>,
) -> CCFSResult<HttpResponse> {
    let client_id_str = get_header(request.headers(), "x-ccfs-client-id")?;
    let client_id = Uuid::from_str(client_id_str).context(errors::ParseUuid {
        text: client_id_str,
    })?;
    let file = file_info.into_inner();
    let target_path = params.get("path").unwrap_or(&String::new()).to_owned();
    let response = raft_node
        .client_write(ClientWriteRequest::new(ClientRequest::new(
            client_id,
            CCFSAction::Add {
                item: file,
                target_path,
            },
        )))
        .await
        .context(ClientWrite)?;
    match response.data {
        ClientResponse::Success { tree } => Ok(HttpResponse::Ok().json(&tree)),
        ClientResponse::Error { msg } => Ok(HttpResponse::InternalServerError().body(&msg)),
    }
}

/// Returns the file info
#[get("/files")]
pub async fn get_file(
    params: Query<HashMap<String, String>>,
    storage: Data<Arc<CCFSStorage>>,
    raft_node: Data<Arc<CCFSRaft>>,
) -> CCFSResult<HttpResponse> {
    raft_node.client_read().await.context(ClientRead)?;
    let lock = storage.state_machine_read().await;
    let path = match params.get("path") {
        Some(path) if !path.is_empty() => evaluate_path(ROOT_DIR, &lock.state.tree, path)?,
        _ => String::new(),
    };
    let files = lock.state.tree.traverse(&path)?;
    Ok(HttpResponse::Ok().json(files))
}

/// Notifies the metadata server to mark the chunk as completed
#[post("/chunk/completed")]
pub async fn signal_chuck_upload_completed(
    request: HttpRequest,
    chunk: Json<Chunk>,
    raft_node: Data<Arc<CCFSRaft>>,
) -> CCFSResult<HttpResponse> {
    let client_id_str = get_header(request.headers(), "x-ccfs-client-id")?;
    let client_id = Uuid::from_str(client_id_str).context(errors::ParseUuid {
        text: client_id_str,
    })?;
    raft_node
        .client_write(ClientWriteRequest::new(ClientRequest::new(
            client_id,
            CCFSAction::UploadCompleted { chunk: *chunk },
        )))
        .await
        .context(ClientWrite)?;
    Ok(HttpResponse::Ok().finish())
}

/// Returns the list of servers which contains the
/// uploaded chunks for a file
#[get("/chunks/file/{file_id}")]
pub async fn get_chunks(
    file_id: Path<Uuid>,
    storage: Data<Arc<CCFSStorage>>,
) -> CCFSResult<HttpResponse> {
    let lock = storage.state_machine_read().await;
    let path = lock
        .state
        .files
        .get(&file_id)
        .ok_or_else(|| NotFound.build())?;
    Ok(HttpResponse::Ok().json(
        lock.state
            .tree
            .traverse(path)?
            .chunks()?
            .iter()
            .filter_map(|chunk_id| lock.state.chunks.get(chunk_id))
            .map(|set| set.iter().cloned().collect())
            .collect::<Vec<Vec<Chunk>>>(),
    ))
}
