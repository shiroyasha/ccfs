use std::collections::{HashMap, HashSet};

use crate::errors::*;
use crate::{ChunkServersMap, ChunksMap, FileMetadataTree, FilesMap};
use actix_web::{get, post, web, HttpResponse};
use ccfs_commons::data::Data;
use ccfs_commons::path::evaluate_path;
use ccfs_commons::result::CCFSResult;
use ccfs_commons::{Chunk, ChunkServer, FileInfo, FileMetadata, FileStatus, ROOT_DIR};
use chrono::{DateTime, Utc};
use uuid::Uuid;

/// Returns a list of available chunk servers where the file chunks can be uploaded
#[get("/servers")]
pub async fn get_servers(servers: web::Data<Data<ChunkServersMap>>) -> CCFSResult<HttpResponse> {
    let servers_map = servers.read().map_err(|_| ReadLock.build())?;
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
pub async fn get_server(
    id: web::Path<Uuid>,
    servers: web::Data<Data<ChunkServersMap>>,
) -> CCFSResult<HttpResponse> {
    let servers_map = servers.read().map_err(|_| ReadLock.build())?;
    let server = servers_map.get(&id).ok_or_else(|| NotFound.build())?;
    Ok(HttpResponse::Ok().json(server))
}

/// Registers a new active chunk server or updates the latest_ping_time
#[post("/ping")]
pub async fn chunk_server_ping(
    payload: ChunkServer,
    servers: web::Data<Data<ChunkServersMap>>,
) -> CCFSResult<HttpResponse> {
    let mut servers_map = servers.write().map_err(|_| WriteLock.build())?;
    let server = servers_map.entry(payload.id).or_insert_with(|| payload);
    server.latest_ping_time = DateTime::from_utc(Utc::now().naive_utc(), Utc);
    Ok(HttpResponse::Ok().json(server))
}

/// Creates a file entity with basic file info
#[post("/files/upload")]
pub async fn create_file(
    file_info: web::Json<FileMetadata>,
    web::Query(params): web::Query<HashMap<String, String>>,
    files: web::Data<Data<FilesMap>>,
    file_metadata_tree: web::Data<Data<FileMetadataTree>>,
) -> CCFSResult<HttpResponse> {
    let file = file_info.into_inner();
    let mut tree = file_metadata_tree.write().map_err(|_| WriteLock.build())?;
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
            let mut files_map = files.write().map_err(|_| WriteLock.build())?;
            files_map.insert(*id, (target_path, file.clone()));
        }
    }
    Ok(HttpResponse::Ok().json(file))
}

/// Returns the file info
#[get("/files")]
pub async fn get_file(
    web::Query(params): web::Query<HashMap<String, String>>,
    file_metadata_tree: web::Data<Data<FileMetadataTree>>,
) -> CCFSResult<HttpResponse> {
    let files_tree = file_metadata_tree.read().map_err(|_| ReadLock.build())?;
    let path = match params.get("path") {
        Some(path) if !path.is_empty() => evaluate_path(ROOT_DIR, &files_tree, path)?,
        _ => String::new(),
    };
    let files = files_tree.traverse(&path).map_err(|_| NotFound.build())?;
    Ok(HttpResponse::Ok().json(files))
}

/// Notifies the metadata server to mark the chunk as completed
#[post("/chunk/completed")]
pub async fn signal_chuck_upload_completed(
    chunk: web::Json<Chunk>,
    file_metadata_tree: web::Data<Data<FileMetadataTree>>,
    files: web::Data<Data<FilesMap>>,
    chunks: web::Data<Data<ChunksMap>>,
) -> CCFSResult<HttpResponse> {
    let mut chunks = chunks.write().map_err(|_| WriteLock.build())?;
    let mut files = files.write().map_err(|_| WriteLock.build())?;
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
                let mut tree = file_metadata_tree.write().map_err(|_| WriteLock.build())?;
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

/// Returns the list of servers which contain the
/// uploaded chunks for a file
#[get("/chunks/file/{file_id}")]
pub async fn get_chunks(
    file_id: web::Path<Uuid>,
    chunks: web::Data<Data<ChunksMap>>,
) -> CCFSResult<HttpResponse> {
    let chunks_map = chunks.read().map_err(|_| ReadLock.build())?;
    Ok(HttpResponse::Ok().json(
        chunks_map
            .values()
            .filter_map(|chs| match chs.iter().next() {
                Some(c) if c.file_id == *file_id => Some(chs.iter().cloned().collect()),
                _ => None,
            })
            .collect::<Vec<Vec<Chunk>>>(),
    ))
}
