use std::collections::HashMap;

use crate::{ChunkServersMap, ChunksMap, FileMetadataTree, FilesMap};
use actix_web::web::{self, Data, Json, Query};
use actix_web::HttpResponse;
use ccfs_commons::{Chunk, ChunkServer, FileInfo, FileMetadata, FileStatus};
use chrono::{DateTime, Duration, Utc};
use uuid::Uuid;

/// Returns a list of available chunk servers where the file chunks can be uploaded
pub fn get_servers(chunk_servers: Data<ChunkServersMap>) -> HttpResponse {
    match chunk_servers.read() {
        Ok(servers_map) => HttpResponse::Ok().json(
            servers_map
                .values()
                .filter(|s| {
                    s.latest_ping_time.signed_duration_since(Utc::now()) <= Duration::seconds(6)
                })
                .cloned()
                .collect::<Vec<ChunkServer>>(),
        ),
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}

/// Returns chunk servers data for the server with ID <id>
pub fn get_server(id: web::Path<Uuid>, chunk_servers: Data<ChunkServersMap>) -> HttpResponse {
    match chunk_servers.read() {
        Ok(servers_map) => match servers_map.get(&id) {
            Some(server) => HttpResponse::Ok().json(server),
            None => HttpResponse::NotFound().finish(),
        },
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}

/// Registers a new active chunk server or updates the latest_ping_time
pub fn chunk_server_ping(
    header_info: ChunkServer,
    chunk_servers: Data<ChunkServersMap>,
) -> HttpResponse {
    let server_id = header_info.id;
    let server_addr = header_info.address;

    match chunk_servers.write() {
        Ok(mut servers_map) => {
            let server = servers_map
                .entry(server_id)
                .or_insert_with(|| ChunkServer::new(server_id, server_addr));
            server.latest_ping_time = DateTime::from_utc(Utc::now().naive_utc(), Utc);
            HttpResponse::Ok().json(server)
        }
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}

/// Creates a file entity with basic file info
pub fn create_file(
    file_info: Json<FileMetadata>,
    Query(params): Query<HashMap<String, String>>,
    files: Data<FilesMap>,
    file_metadata_tree: Data<FileMetadataTree>,
) -> HttpResponse {
    let file = file_info.into_inner();
    match params.get("path") {
        Some(path) => {
            let mut files_map = files.write().unwrap();
            let mut file_names_tree = file_metadata_tree.write().unwrap();
            let (dir_path, _) = path.split_at(path.rfind('/').unwrap_or(0));
            let target = file_names_tree
                .traverse_mut(&dir_path)
                .unwrap_or_else(|err| panic!("{:?}", err));
            match &file.file_info {
                FileInfo::Directory(name) => {
                    target.children.insert(name.clone(), file.clone());
                }
                FileInfo::File(f) => {
                    let id = f.id;
                    target.children.insert(f.name.clone(), file.clone());
                    files_map.insert(id, f.clone());
                }
            }
            HttpResponse::Ok().json(file)
        }
        None => HttpResponse::BadRequest().finish(),
    }
}

/// Returns the file info
pub fn get_file(
    Query(params): Query<HashMap<String, String>>,
    file_metadata_tree: Data<FileMetadataTree>,
) -> HttpResponse {
    let path = match params.get("path") {
        Some(path) => path.to_owned(),
        None => String::new(),
    };
    let file_names_map = file_metadata_tree.read().unwrap();
    let files = file_names_map.traverse(&path).unwrap();
    HttpResponse::Ok().json(files)
}

/// Notifies the metadata server to mark the chunk as completed
pub fn signal_chuck_upload_completed(
    chunk_info: Json<Chunk>,
    files: Data<FilesMap>,
    chunks: Data<ChunksMap>,
) -> HttpResponse {
    match (chunks.write(), files.write()) {
        (Ok(mut chunks_map), Ok(mut files_map)) => {
            let chunk = chunk_info.into_inner();
            match files_map.get_mut(&chunk.file_id) {
                Some(file) => {
                    chunks_map.insert(chunk.id, chunk);

                    file.num_of_completed_chunks += 1;
                    if file.num_of_completed_chunks == file.chunks.len() {
                        file.status = FileStatus::Completed;
                    }
                    HttpResponse::Ok().json(file)
                }
                None => HttpResponse::NotFound().finish(),
            }
        }
        (Err(_), _) | (_, Err(_)) => HttpResponse::InternalServerError().finish(),
    }
}

/// Returns the list of servers which contain the
/// uploaded chunks for a file
pub fn get_chunks(file_id: web::Path<Uuid>, chunks: Data<ChunksMap>) -> HttpResponse {
    match chunks.read() {
        Ok(chunks_map) => HttpResponse::Ok().json(
            chunks_map
                .values()
                .filter(|c| c.file_id == *file_id)
                .copied()
                .collect::<Vec<Chunk>>(),
        ),
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}
