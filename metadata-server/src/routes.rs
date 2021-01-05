use ccfs_commons::{Chunk, ChunkServer, FileInfo, FileMetadata, FileStatus};
use chrono::{DateTime, Duration, Utc};
use rocket::State;
use rocket_contrib::json::{Json, JsonValue};
use rocket_contrib::uuid::Uuid;

use crate::{ChunkServersMap, ChunksMap, FileMetadataTree, FilesMap};

/// Returns a list of available chunk servers where the file chunks can be uploaded
#[get("/servers")]
pub fn get_servers(chunk_servers: State<'_, ChunkServersMap>) -> JsonValue {
    match chunk_servers.read() {
        Ok(servers_map) => {
            json!(servers_map
                .values()
                .filter(|s| s.latest_ping_time.signed_duration_since(Utc::now())
                    <= Duration::seconds(6))
                .cloned()
                .collect::<Vec<ChunkServer>>())
        }
        Err(_) => json!({ "status": "error", "reason": "Unexpected error, try again" }),
    }
}

/// Returns chunk servers data for the server with ID <id>
#[get("/servers/<id>")]
pub fn get_server(id: Uuid, chunk_servers: State<'_, ChunkServersMap>) -> JsonValue {
    match chunk_servers.read() {
        Ok(servers_map) => match servers_map.get(&id) {
            Some(server) => json!(server),
            None => {
                let reason = format!("Could not find server with ID {}", id);
                json!({ "status": "error", "reason": reason })
            }
        },
        Err(_) => json!({ "status": "error", "reason": "Unexpected error, try again" }),
    }
}

/// Registers a new active chunk server or updates the latest_ping_time
#[post("/ping")]
pub fn chunk_server_ping(
    header_info: ChunkServer,
    chunk_servers: State<'_, ChunkServersMap>,
) -> JsonValue {
    let server_id = header_info.id;
    let server_addr = header_info.address;

    match chunk_servers.write() {
        Ok(mut servers_map) => {
            let server = servers_map
                .entry(server_id)
                .or_insert_with(|| ChunkServer::new(server_id, server_addr));
            server.latest_ping_time = DateTime::from_utc(Utc::now().naive_utc(), Utc);
            json!(*server)
        }
        Err(_) => json!({ "status": "error", "reason": "Unexpected error, try again" }),
    }
}

/// Creates a file entity with basic file info
#[post("/files/upload?<path>", format = "json", data = "<file_info>")]
pub fn create_file(
    file_info: Json<FileMetadata>,
    path: String,
    files: State<'_, FilesMap>,
    file_metadata_tree: State<'_, FileMetadataTree>,
) -> JsonValue {
    let file = file_info.into_inner();
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

    json!(file)
}

/// Returns the file info
#[get("/files?<path>")]
pub fn get_file(
    path: Option<String>,
    file_metadata_tree: State<'_, FileMetadataTree>,
) -> JsonValue {
    let file_names_map = file_metadata_tree.read().unwrap();
    let files = file_names_map
        .traverse(&path.unwrap_or_else(String::new))
        .unwrap();
    json!(files)
}

/// Notifies the metadata server to mark the chunk as completed
#[post("/chunk/completed", format = "json", data = "<chunk_info>")]
pub fn signal_chuck_upload_completed(
    chunk_info: Json<Chunk>,
    files: State<'_, FilesMap>,
    chunks: State<'_, ChunksMap>,
) -> JsonValue {
    match (chunks.write(), files.write()) {
        (Ok(mut chunks_map), Ok(mut files_map)) => {
            let chunk = chunk_info.into_inner();
            match files_map.get_mut(&chunk.file_id) {
                Some(file) => {
                    chunks_map.insert(chunk.id, chunk);

                    file.num_of_completed_chunks += 1;
                    if file.num_of_completed_chunks == file.num_of_chunks {
                        file.status = FileStatus::Completed;
                    }
                    json!(file)
                }
                None => {
                    let reason = format!("Could not find file with ID {}", chunk.file_id);
                    json!({ "status": "error", "reason": reason })
                }
            }
        }
        (Err(_), _) | (_, Err(_)) => {
            json!({ "status": "error", "reason": "Unexpected error, try again" })
        }
    }
}

/// Returns the list of servers which contain the
/// uploaded chunks for a file
#[get("/chunks/file/<file_id>")]
pub fn get_chunks(file_id: Uuid, chunks: State<'_, ChunksMap>) -> JsonValue {
    match chunks.read() {
        Ok(chunks_map) => json!(chunks_map
            .values()
            .filter(|c| c.file_id == file_id.into_inner())
            .copied()
            .collect::<Vec<Chunk>>()),
        Err(_) => json!({ "status": "error", "reason": "Unexpected error, try again" }),
    }
}
