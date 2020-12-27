use rocket::http::Status;
use rocket::outcome::Outcome::*;
use rocket::request::{self, FromRequest, Request};
use rocket_contrib::uuid::{uuid_crate, Uuid};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::time::Instant;

pub const CHUNK_SIZE: u64 = 64000000;

pub fn init_value() -> Uuid {
    Uuid::from_str("").unwrap()
}

pub mod custom_uuid {
    use rocket_contrib::uuid::Uuid;
    use serde::{de::Error, Deserialize, Deserializer, Serialize, Serializer};
    use std::str::FromStr;

    pub fn serialize<S: Serializer>(val: &'_ Uuid, serializer: S) -> Result<S::Ok, S::Error> {
        val.to_string().serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Uuid, D::Error> {
        let val: &str = Deserialize::deserialize(deserializer)?;
        Uuid::from_str(val).map_err(D::Error::custom)
    }
}

pub mod custom_instant {
    use serde::{Deserializer, Serialize, Serializer};
    use std::time::Instant;

    pub fn serialize<S: Serializer>(_val: &Instant, serializer: S) -> Result<S::Ok, S::Error> {
        "".to_string().serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(_deserializer: D) -> Result<Instant, D::Error> {
        Ok(Instant::now())
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ChunkServer {
    #[serde(with = "custom_uuid")]
    #[serde(default = "init_value")]
    pub id: Uuid,
    pub address: String,
    pub is_active: bool,
    #[serde(with = "custom_instant")]
    #[serde(skip_deserializing)]
    #[serde(default = "Instant::now")]
    pub latest_ping_time: Instant,
}
impl ChunkServer {
    pub fn new(id: Uuid, address: String) -> ChunkServer {
        ChunkServer {
            id,
            address,
            is_active: true,
            latest_ping_time: Instant::now(),
        }
    }
}

#[derive(Debug)]
pub enum HeaderError {
    Missing,
    Invalid,
}

#[rocket::async_trait]
impl<'a, 'r> FromRequest<'a, 'r> for ChunkServer {
    type Error = HeaderError;

    async fn from_request(request: &'a Request<'r>) -> request::Outcome<Self, Self::Error> {
        let id_header: Vec<_> = request.headers().get("x-chunk-server-id").collect();
        let address_header: Vec<_> = request.headers().get("x-chunk-server-address").collect();
        match (id_header.len(), address_header.len()) {
            (a, b) if a == 0 || b == 0 => Failure((Status::BadRequest, HeaderError::Missing)),
            _ => {
                let parsed_id = Uuid::from_str(&id_header.concat());
                match parsed_id {
                    Ok(id) => Success(ChunkServer::new(id, address_header.concat())),
                    _ => Failure((Status::BadRequest, HeaderError::Invalid)),
                }
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum FileStatus {
    Started,
    Completed,
    Canceled,
}
impl FileStatus {
    pub fn init() -> Self {
        FileStatus::Started
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct File {
    #[serde(with = "custom_uuid")]
    #[serde(default = "init_value")]
    pub id: Uuid,
    pub name: String,
    pub size: u64,
    pub num_of_chunks: u16,
    #[serde(default)]
    pub num_of_completed_chunks: u16,
    #[serde(default = "FileStatus::init")]
    pub status: FileStatus,
}
impl File {
    pub fn new(name: String, size: u64) -> File {
        File {
            id: Uuid::from_str(uuid_crate::Uuid::new_v4().to_string().as_str()).unwrap(),
            name,
            size,
            num_of_chunks: (size / CHUNK_SIZE + 1) as u16,
            num_of_completed_chunks: 0,
            status: FileStatus::Started,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Copy)]
pub struct Chunk {
    #[serde(with = "custom_uuid")]
    #[serde(default = "init_value")]
    pub id: Uuid,
    #[serde(with = "custom_uuid")]
    pub file_id: Uuid,
    #[serde(with = "custom_uuid")]
    pub server_id: Uuid,
    pub file_part_num: u16,
}
impl Chunk {
    pub fn new(file_id: Uuid, server_id: Uuid, file_part_num: u16) -> Chunk {
        Chunk {
            id: Uuid::from_str(uuid_crate::Uuid::new_v4().to_string().as_str()).unwrap(),
            file_id,
            server_id,
            file_part_num,
        }
    }
}
