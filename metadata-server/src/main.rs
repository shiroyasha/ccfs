#[macro_use]
extern crate rocket;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate rocket_contrib;
extern crate ccfs_commons;

use ccfs_commons::{Chunk, ChunkServer, File, FileInfo, FileMetadata, FileStatus};
use chrono::{DateTime, Duration, Utc};
use rocket_contrib::json::{Json, JsonValue};
use rocket_contrib::uuid::uuid_crate as uuid;
use rocket_contrib::uuid::Uuid;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

lazy_static! {
    static ref CHUNK_SERVERS: Arc<RwLock<HashMap<uuid::Uuid, ChunkServer>>> =
        Arc::new(RwLock::new(HashMap::new()));
    static ref FILES: Arc<RwLock<HashMap<uuid::Uuid, File>>> =
        Arc::new(RwLock::new(HashMap::new()));
    static ref FILE_NAMES: Arc<RwLock<FileMetadata>> = {
        // read from file or create new
        Arc::new(RwLock::new(FileMetadata::create_root()))
    };
    static ref CHUNKS: Arc<RwLock<HashMap<uuid::Uuid, Chunk>>> =
        Arc::new(RwLock::new(HashMap::new()));
}

/// Returns a list of available chunk servers where the file chunks can be uploaded
#[get("/servers")]
fn get_servers() -> JsonValue {
    match CHUNK_SERVERS.read() {
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
fn get_server(id: Uuid) -> JsonValue {
    match CHUNK_SERVERS.read() {
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
fn chunk_server_ping(header_info: ChunkServer) -> JsonValue {
    let server_id = header_info.id;
    let server_addr = header_info.address;

    match CHUNK_SERVERS.write() {
        Ok(mut servers_map) => {
            let server = servers_map
                .entry(server_id.into_inner())
                .or_insert_with(|| ChunkServer::new(server_id, server_addr));
            server.latest_ping_time = DateTime::from_utc(Utc::now().naive_utc(), Utc);
            json!(*server)
        }
        Err(_) => json!({ "status": "error", "reason": "Unexpected error, try again" }),
    }
}

/// Creates a file entity with basic file info
#[post("/files/upload?<path>", format = "json", data = "<file_info>")]
fn create_file(file_info: Json<FileMetadata>, path: String) -> JsonValue {
    let file = file_info.into_inner();
    let mut files_map = FILES.write().unwrap();
    let mut file_names_tree = FILE_NAMES.write().unwrap();
    let (dir_path, _) = path.split_at(path.rfind('/').unwrap_or(0));
    let target = file_names_tree
        .traverse_mut(&dir_path)
        .unwrap_or_else(|err| panic!("{:?}", err));
    match &file.file_info {
        FileInfo::Directory(name) => {
            target.children.insert(name.clone(), file.clone());
        }
        FileInfo::File(f) => {
            let id = f.id.into_inner();
            target.children.insert(f.name.clone(), file.clone());
            files_map.insert(id, f.clone());
        }
    }

    json!(file)
}

/// Returns the file info
#[get("/files?<path>")]
fn get_file(path: String) -> JsonValue {
    let file_names_map = FILE_NAMES.read().unwrap();
    let files = file_names_map.traverse(&path).unwrap();
    json!(files)
}

/// Notifies the metadata server to mark the chunk as completed
#[post("/chunk/completed", format = "json", data = "<chunk_info>")]
fn signal_chuck_upload_completed(chunk_info: Json<Chunk>) -> JsonValue {
    match (CHUNKS.write(), FILES.write()) {
        (Ok(mut chunks_map), Ok(mut files_map)) => {
            let chunk = chunk_info.into_inner();
            match files_map.get_mut(&chunk.file_id.into_inner()) {
                Some(file) => {
                    chunks_map.insert(chunk.id.into_inner(), chunk);

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
fn get_chunks(file_id: Uuid) -> JsonValue {
    match CHUNKS.read() {
        Ok(chunks_map) => json!(chunks_map
            .values()
            .filter(|c| c.file_id == file_id)
            .copied()
            .collect::<Vec<Chunk>>()),
        Err(_) => json!({ "status": "error", "reason": "Unexpected error, try again" }),
    }
}

#[catch(404)]
fn not_found() -> JsonValue {
    json!({ "status": "error", "reason": "Resource was not found." })
}

#[launch]
fn rocket() -> rocket::Rocket {
    rocket::ignite()
        .mount(
            "/api",
            routes![
                get_servers,
                get_server,
                chunk_server_ping,
                create_file,
                signal_chuck_upload_completed,
                get_file,
                get_chunks
            ],
        )
        .register(catchers![not_found])
}
