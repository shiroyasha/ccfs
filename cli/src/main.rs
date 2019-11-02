#![feature(proc_macro_hygiene, decl_macro)]

extern crate clap;
extern crate reqwest;

use clap::{App, Arg, SubCommand};
use reqwest::multipart::Part;
use serde::{Deserialize, Serialize};
use std::fs::File as FileFS;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::str;
use std::time::Instant;
use uuid::Uuid;

const CHUNK_SIZE: u64 = 64000000;

mod custom_uuid {
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

mod custom_instant {
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

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
enum FileStatus {
  Started,
  Completed,
  Canceled,
}
impl FileStatus {
  fn init() -> Self {
    FileStatus::Started
  }
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
  status: FileStatus,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
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

fn main() {
  let matches = App::new("Chop-Chop File System")
    .version("1.0")
    .author("Zoran L. <lazarevic.zoki91@gmail.com>")
    .about("A distrubuted highly available file system")
    .subcommand(
      SubCommand::with_name("upload")
        .about("upload a file to the CCFS")
        .arg(
          Arg::with_name("file_path")
            .help("absolute or relative path to the file")
            .index(1)
            .required(true),
        ),
    )
    .subcommand(
      SubCommand::with_name("download")
        .about("move a local file to the CCFS")
        .arg(
          Arg::with_name("file_id")
            .help("the name of the file on the CCFS")
            .index(1)
            .required(true),
        )
        .arg(
          Arg::with_name("file_path")
            .help("absolute or relative path where the file will be saved"),
        ),
    )
    .subcommand(
      SubCommand::with_name("remove")
        .about("remove a file from the CCFS")
        .arg(
          Arg::with_name("file_path")
            .help("absolute or relative path to the file")
            .index(1)
            .required(true),
        ),
    )
    .get_matches();
  let client = reqwest::Client::new();

  if let Some(ref matches) = matches.subcommand_matches("upload") {
    println!("matches {:?}", matches.value_of("file_path"));
    upload(client, matches.value_of("file_path").unwrap())
  } else if let Some(ref matches) = matches.subcommand_matches("download") {
    let file_id = matches.value_of("file_id").unwrap();
    let mut path_buf = PathBuf::new();
    let path;
    if let Some(path_str) = matches.value_of("file_path") {
      path = Path::new(path_str);
      if !path.exists() {
        println!("path doesn't exist")
      }
      if !path.is_dir() {
        println!("target {} is not a directory", path_str)
      }
    } else {
      path_buf.push(".");
      path_buf.push(file_id);
      path = path_buf.as_path();
    }
    download(client, file_id, path)
  } else if let Some(ref matches) = matches.subcommand_matches("remove") {
    println!("Not implemented yet :(")
  } else {
    println!("Some other subcommand was used")
  }
}

fn upload(client: reqwest::Client, path: &str) {
  let file_path = Path::new(path);
  println!("upload");
  if file_path.exists() {
    let size = file_path.metadata().unwrap().len();
    let file_data = File {
      id: Uuid::nil(),
      size,
      num_of_chunks: 0,
      num_of_completed_chunks: 0,
      status: FileStatus::Started,
    };
    let file_resp: Result<File, _> = client
      .post("http://localhost:8080/api/files/upload")
      .json(&file_data)
      .send()
      .unwrap()
      .json();
    // let mut servers = client.get("http://localhost:8080/api/servers");

    match file_resp {
      Ok(file) => {
        println!("file id: {}", file.id);
        let mut file_part = 1;
        let mut f = FileFS::open(file_path).unwrap();

        loop {
          let mut chunk = Vec::with_capacity(CHUNK_SIZE as usize);
          let n = std::io::Read::by_ref(&mut f)
            .take(CHUNK_SIZE)
            .read_to_end(&mut chunk)
            .unwrap();
          if n == 0 {
            break;
          }
          // select random server and upload chunk
          let form = reqwest::multipart::Form::new()
            .text("file_id", file.id.to_string())
            .text("file_part_num", file_part.to_string())
            .part("file", Part::bytes(chunk));
          client
            .post("http://localhost:8000/api/upload")
            .multipart(form)
            .send()
            .unwrap();

          file_part = file_part + 1;

          if n < CHUNK_SIZE as usize {
            break;
          }
        }
        println!("Completed file upload");
      }
      _ => println!("Error while uploading file"),
    }
  } else {
    println!("The file {} doesn't exists", path);
  }
}

fn download(client: reqwest::Client, file_id: &str, path: &Path) {
  // get chunks and merge them into a file
  let file_resp: Result<File, _> = client
    .get(
      reqwest::Url::parse(
        format!("http://localhost:8080/api/files/{}", &file_id).as_str(),
      )
      .unwrap(),
    )
    .send()
    .unwrap()
    .json();
  if let Ok(file) = file_resp {
    println!("{}", &file.id);
    let chunks_resp: Result<Vec<Chunk>, _> = client
      .get(
        reqwest::Url::parse(
          format!("http://localhost:8080/api/chunks/file/{}", &file.id)
            .as_str(),
        )
        .unwrap(),
      )
      .send()
      .unwrap()
      .json();
    if let Ok(mut chunks) = chunks_resp {
      println!("{}", chunks.len());
      chunks.sort_by(|a, b| a.file_part_num.cmp(&b.file_part_num));
      let mut file = FileFS::create(path).unwrap();
      for chunk in chunks.iter() {
        println!("{:?}", chunk);
        let mut resp = client
          .get(
            reqwest::Url::parse(
              format!("http://localhost:8000/api/download/{}", &chunk.id)
                .as_str(),
            )
            .unwrap(),
          )
          .send()
          .unwrap();
        println!("{:?}", resp);
        let mut buf: Vec<u8> = vec![];
        resp.copy_to(&mut buf).unwrap();
        file.write(&buf).unwrap();
      }
    }
  }
}
