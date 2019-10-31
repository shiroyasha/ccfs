#![feature(proc_macro_hygiene, decl_macro)]

#[macro_use] extern crate rocket;
#[macro_use] extern crate lazy_static;
#[macro_use] extern crate rocket_contrib;
extern crate mut_static;

use std::collections::HashMap;
use std::time::{Instant};
use serde::{Deserialize, Serialize};
use mut_static::MutStatic;

use uuid::Uuid;
use rocket_contrib::uuid::Uuid as UuidRC;
use rocket_contrib::json::{Json, JsonValue};
use rocket::request::{self, Request, FromRequest};
use rocket::Outcome;
use rocket::http::Status;

const CHUNK_SIZE: u64 = 64000000;

mod custom_uuid {
    use serde::{de::Error, Deserialize, Deserializer, Serialize, Serializer};
    use uuid::Uuid;
    use std::str::FromStr;

    pub fn serialize<'a, S>(val: &'a Uuid, serializer: S)
         -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        val.to_string().serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Uuid, D::Error>
    where
        D: Deserializer<'de>,
    {
        let val: &str = Deserialize::deserialize(deserializer)?;
        Uuid::from_str(val).map_err(D::Error::custom)
    }
}

// mod custom_string {
//     use serde::{Deserialize, Deserializer, Serialize, Serializer};

//     pub fn serialize<'a, S>(val: &'a str, serializer: S)
//          -> Result<S::Ok, S::Error>
//     where
//         S: Serializer,
//     {
//         val.to_string().serialize(serializer)
//     }

//     pub fn deserialize<'de, D>(deserializer: D) -> Result<String, D::Error>
//     where
//         D: Deserializer<'de>,
//     {
//         let val: &str = Deserialize::deserialize(deserializer)?;
//         Ok(val.to_string())
//     }
// }

mod custom_instant {
    use serde::{Deserializer, Serialize, Serializer};
    use std::time::{Instant};

    pub fn serialize<S>(_val: &Instant, serializer: S)
        -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        "".to_string().serialize(serializer)
    }

    pub fn deserialize<'de, D>(_deserializer: D) -> Result<Instant, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Instant::now())
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, Copy)]
struct ChunkServer {
    #[serde(with = "custom_uuid")]
    #[serde(default = "Uuid::nil")]
    id: Uuid,
    // #[serde(with = "custom_string")]
    // address: String,
    is_active: bool,
    #[serde(with = "custom_instant")]
    latest_ping_time: Instant,
}
impl ChunkServer {
    fn new(id: Uuid/* , address: String */) -> ChunkServer {
        ChunkServer {
            id,
            // address,
            is_active: true,
            latest_ping_time: Instant::now()
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
enum FileStatus {
    Started,
    Completed,
    Canceled
}
impl FileStatus {
    fn init() -> Self { FileStatus::Started }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
struct File {
    #[serde(with = "custom_uuid")]
    #[serde(default = "Uuid::nil")]
    id: Uuid,
    // #[serde(with = "custom_string")]
    // name: String,
    size: u64,
    num_of_chunks: u16,
    #[serde(default)]
    num_of_completed_chunks: u16,
    #[serde(default = "FileStatus::init")]
    status: FileStatus
}
impl File {
    fn new(/* name: String,  */size: u64) -> File {
        File {
            id: Uuid::new_v4(),
            // name,
            size,
            num_of_chunks: (size / CHUNK_SIZE + 1) as u16,
            num_of_completed_chunks: 0,
            status: FileStatus::Started
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct Chunk {
    #[serde(with = "custom_uuid")]
    #[serde(default = "Uuid::nil")]
    id: Uuid,
    #[serde(with = "custom_uuid")]
    file_id: Uuid,
    #[serde(with = "custom_uuid")]
    server_id: Uuid,

    file_part_num: u16,
}
impl Chunk {
    fn new(file_id: Uuid, server_id: Uuid, file_part_num: u16) -> Chunk {
        Chunk {
            id: Uuid::new_v4(),
            file_id,
            server_id,
            file_part_num: file_part_num,
        }
    }
}

lazy_static! {
    // should be replaced with DB
    static ref CHUNK_SERVERS:
        MutStatic<HashMap<Uuid, ChunkServer>> = MutStatic::new();
    static ref FILES:
        MutStatic<HashMap<Uuid, File>> = MutStatic::new();
    static ref CHUNKS:
        MutStatic<HashMap<Uuid, Chunk>> = MutStatic::new();
}

#[get("/servers")]
fn get_servers() -> JsonValue {
    // Returns a list of available chunk servers where the file chunks
    // can be uploaded
    let servers_map = CHUNK_SERVERS.read().unwrap();
    let server_refs: Vec<ChunkServer> =
        servers_map.iter().map(|(_, server)| server.clone()).collect();
    json!({
        "available_servers": server_refs
    })
}

#[derive(Debug)]
enum HeaderError {
    Missing,
    Invalid,
}

impl<'a, 'r> FromRequest<'a, 'r> for ChunkServer {
    type Error = HeaderError;

    fn from_request(request: &'a Request<'r>)
        -> request::Outcome<Self, Self::Error> {

        let id_header: Vec<_> = request.headers()
                                        .get("x-chunk-server-id")
                                        .collect();
        let address_header: Vec<_> = request.headers()
                                            .get("x-chunk-server-address")
                                            .collect();
        if id_header.len() == 0 || address_header.len() == 0 {
            return Outcome::Failure((Status::BadRequest, HeaderError::Missing))
        }
        let parsed_id = Uuid::parse_str(&id_header.concat());
        match parsed_id {
            Ok(id) =>
                Outcome::Success(ChunkServer::new(
                    id
                    /* , address_header.concat() */
                )),
            _ => Outcome::Failure((Status::BadRequest, HeaderError::Invalid))
        }
    }
}

#[post("/ping")]
fn chunk_server_ping(header_info: ChunkServer) -> Json<ChunkServer> {
    // Registers a chunk server as active, or updates the latest_ping_time
    // if the map already contains it
    let server_id = header_info.id;
    // let server_addr = header_info.address;

    let mut servers_map = CHUNK_SERVERS.write().unwrap();
    let chunk_server;
    if let Some(server) = servers_map.get_mut(&server_id) {
        server.latest_ping_time = Instant::now();
        chunk_server = *server;
    } else {
        chunk_server = ChunkServer::new(
            server_id,
            // String::from(server_addr.to_string()),
        );
        servers_map.insert(
            server_id,
            chunk_server
        );
    }
    Json(chunk_server)
}

#[post("/files/upload", format = "json", data = "<file_info>")]
fn create_file(file_info: Json<File>) -> Json<File> {
    // Creates a file entity with basic file info
    // let name = file_info.0.name;
    let size = file_info.0.size;
    let file = File::new(
        // name,
        size
    );
    let mut files_map = FILES.write().unwrap();
    files_map.insert(file.id, file);

    Json(file)
}

#[get("/files/<id>")]
fn get_file(id: UuidRC) -> Json<File> {
    // Returns the file info
    let files_map = FILES.read().unwrap();
    let file = files_map.get(&id);

    Json(*file.unwrap())
}

#[post("/chunk/completed", format = "json", data = "<chunk_info>")]
fn signal_chuck_upload_completed(chunk_info: Json<Chunk>)-> JsonValue {
    // Because Uuid implements the Deref trait, we use Deref coercion to convert
    // rocket_contrib::uuid::Uuid to uuid::Uuid.
    // Notifies the metadata server to mark the chunk as completed
    let mut chunks_map = CHUNKS.write().unwrap();

    let mut files_map = FILES.write().unwrap();
    if let Some(file) = files_map.get_mut(&chunk_info.0.file_id) {
        // The creation of the chunk entity should be actually in the chunk
        // server api, but for it is also here for dev purpose
        let chunk = Chunk::new(
            chunk_info.0.file_id,
            chunk_info.0.server_id,
            chunk_info.0.file_part_num
        );
        chunks_map.insert(chunk.id, chunk);

        file.num_of_completed_chunks = file.num_of_completed_chunks + 1;
        if file.num_of_completed_chunks == file.num_of_chunks {
            file.status = FileStatus::Completed;
        }
        json!(file)
    } else {
        json!({
            "status": "error",
            "reason": format!(
                "Could not find file with ID {}", chunk_info.0.file_id
            )
        })
    }
}

#[get("/chunks/file/<file_id>")]
fn get_chunks(file_id: UuidRC) -> JsonValue {
    // Because Uuid implements the Deref trait, we use Deref coercion to convert
    // rocket_contrib::uuid::Uuid to uuid::Uuid.
    // Returns the list of servers which contain the
    // uploaded chunks for a file

    let chunks_map = CHUNKS.read().unwrap();
    let mut chunk_refs: Vec<Chunk> =
        chunks_map.iter().map(|(_, chunk)| chunk.clone()).collect();
    chunk_refs
        .retain(|chunk| {
            chunk.file_id == *file_id
        });
    json!(chunk_refs)
}

#[catch(404)]
fn not_found() -> JsonValue {
    json!({
        "status": "error",
        "reason": "Resource was not found."
    })
}

fn rocket() -> rocket::Rocket {
    rocket::ignite()
        .mount("/api", routes![
            get_servers,
            chunk_server_ping,
            create_file,
            signal_chuck_upload_completed,
            get_file,
            get_chunks
        ])
        .register(catchers![not_found])
}

fn main() {
    CHUNK_SERVERS.set(HashMap::new()).unwrap();
    FILES.set(HashMap::new()).unwrap();
    CHUNKS.set(HashMap::new()).unwrap();

    rocket().launch();
}
