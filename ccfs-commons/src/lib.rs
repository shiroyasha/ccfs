pub const CHUNK_SIZE: u64 = 64000000;

pub mod custom_uuid {
  use serde::{de::Error, Deserialize, Deserializer, Serialize, Serializer};
  use std::str::FromStr;
  use uuid::Uuid;

  pub fn serialize<'a, S>(
    val: &'a Uuid,
    serializer: S,
  ) -> Result<S::Ok, S::Error>
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

pub mod custom_instant {
  use serde::{Deserializer, Serialize, Serializer};
  use std::time::Instant;

  pub fn serialize<S>(_val: &Instant, serializer: S) -> Result<S::Ok, S::Error>
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

use rocket::http::Status;
use rocket::request::{self, FromRequest, Request};
use rocket::Outcome;
use serde::{Deserialize, Serialize};
use std::time::Instant;
use uuid::Uuid;

#[derive(Clone, Serialize, Deserialize, Debug, Copy)]
pub struct ChunkServer {
  #[serde(with = "custom_uuid")]
  #[serde(default = "Uuid::nil")]
  pub id: Uuid,
  // #[serde(with = "custom_string")]
  // address: String,
  pub is_active: bool,
  #[serde(with = "custom_instant")]
  pub latest_ping_time: Instant,
}
impl ChunkServer {
  pub fn new(id: Uuid /* , address: String */) -> ChunkServer {
    ChunkServer {
      id,
      // address,
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

impl<'a, 'r> FromRequest<'a, 'r> for ChunkServer {
  type Error = HeaderError;

  fn from_request(
    request: &'a Request<'r>,
  ) -> request::Outcome<Self, Self::Error> {
    let id_header: Vec<_> =
      request.headers().get("x-chunk-server-id").collect();
    let address_header: Vec<_> =
      request.headers().get("x-chunk-server-address").collect();
    if id_header.len() == 0 || address_header.len() == 0 {
      return Outcome::Failure((Status::BadRequest, HeaderError::Missing));
    }
    let parsed_id = Uuid::parse_str(&id_header.concat());
    match parsed_id {
      Ok(id) => Outcome::Success(ChunkServer::new(
        id, /* , address_header.concat() */
      )),
      _ => Outcome::Failure((Status::BadRequest, HeaderError::Invalid)),
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

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct File {
  #[serde(with = "custom_uuid")]
  #[serde(default = "Uuid::nil")]
  pub id: Uuid,
  // #[serde(with = "custom_string")]
  // name: String,
  pub size: u64,
  pub num_of_chunks: u16,
  #[serde(default)]
  pub num_of_completed_chunks: u16,
  #[serde(default = "FileStatus::init")]
  pub status: FileStatus,
}
impl File {
  pub fn new(/* name: String,  */ size: u64) -> File {
    File {
      id: Uuid::new_v4(),
      // name,
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
  #[serde(default = "Uuid::nil")]
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
      id: Uuid::new_v4(),
      file_id,
      server_id,
      file_part_num,
    }
  }
}
