#![feature(proc_macro_hygiene, decl_macro)]

extern crate ccfs_commons;
extern crate clap;
extern crate reqwest;

use ccfs_commons::{Chunk, ChunkServer, File, FileStatus, CHUNK_SIZE};
use clap::{App, Arg, SubCommand};
use reqwest::multipart::Part;
use std::collections::HashMap;
use std::fs::File as FileFS;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::str;
use uuid::Uuid;

fn main() {
  let matches = App::new("Chop-Chop File System")
    .version("1.0")
    .author("Zoran L. <lazarevic.zoki91@gmail.com>")
    .about("A distrubuted highly available file system")
    .arg(
      Arg::with_name("config")
        .short("c")
        .long("config")
        .value_name("FILE")
        .help("Sets a custom config file")
        .default_value("./cli_config.yaml")
        .takes_value(true),
    )
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

  let config_file_path = matches.value_of("config").unwrap_or_default();
  let path = Path::new(config_file_path);
  if !path.exists() {
    println!("{} file doesn't exists", config_file_path)
  }
  if path.is_dir() {
    println!("{} is a directory", config_file_path)
  }

  let config_file = std::fs::File::open(path).unwrap();
  let config_map: HashMap<String, String> =
    serde_yaml::from_reader(config_file).unwrap();
  let meta_server_url = config_map.get("metadata-server-url").unwrap();
  let client = reqwest::Client::new();

  if let Some(ref matches) = matches.subcommand_matches("upload") {
    upload(
      &meta_server_url,
      client,
      matches.value_of("file_path").unwrap(),
    )
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
    download(&meta_server_url, client, file_id, path)
  } else if let Some(ref _matches) = matches.subcommand_matches("remove") {
    println!("Not implemented yet :(")
  } else {
    println!("Some other subcommand was used")
  }
}

fn upload(meta_server_url: &str, client: reqwest::Client, path: &str) {
  let file_path = Path::new(path);
  if file_path.exists() {
    let size = file_path.metadata().unwrap().len();
    let file_data = File {
      id: Uuid::nil(),
      name: file_path.to_str().unwrap().to_string(),
      size,
      num_of_chunks: 0,
      num_of_completed_chunks: 0,
      status: FileStatus::Started,
    };
    let file_resp: Result<File, _> = client
      .post(format!("{}/api/files/upload", meta_server_url).as_str())
      .json(&file_data)
      .send()
      .unwrap()
      .json();
    let servers_resp: Result<Vec<ChunkServer>, _> = client
      .get(format!("{}/api/servers", meta_server_url).as_str())
      .send()
      .unwrap()
      .json();
    if !servers_resp.is_ok() {
      return println!("There are no available servers at the moment");
    } else {
      match file_resp {
        Ok(file) => {
          println!("file id: {}", file.id);
          let servers = servers_resp.unwrap();
          let mut i = 0;
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
            let form = reqwest::multipart::Form::new()
              .text("file_id", file.id.to_string())
              .text("file_part_num", file_part.to_string())
              .part("file", Part::bytes(chunk));
            let server = &servers[i];
            i = i + 1;
            client
              .post(format!("{}/api/upload", server.address).as_str())
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
    }
  } else {
    println!("The file {} doesn't exists", path);
  }
}

fn download(
  meta_server_url: &str,
  client: reqwest::Client,
  file_id: &str,
  path: &Path,
) {
  // get chunks and merge them into a file
  let file_resp: Result<File, _> = client
    .get(
      reqwest::Url::parse(
        format!("{}/api/files/{}", meta_server_url, &file_id).as_str(),
      )
      .unwrap(),
    )
    .send()
    .unwrap()
    .json();
  if let Ok(file) = file_resp {
    let chunks_resp: Result<Vec<Chunk>, _> = client
      .get(
        reqwest::Url::parse(
          format!("{}/api/chunks/file/{}", meta_server_url, &file.id).as_str(),
        )
        .unwrap(),
      )
      .send()
      .unwrap()
      .json();
    if let Ok(mut chunks) = chunks_resp {
      chunks.sort_by(|a, b| a.file_part_num.cmp(&b.file_part_num));
      let mut file = FileFS::create(path).unwrap();
      for chunk in chunks.iter() {
        let server_resp: Result<ChunkServer, _> = client
          .get(
            reqwest::Url::parse(
              format!("{}/api/servers/{}", meta_server_url, &chunk.server_id)
                .as_str(),
            )
            .unwrap(),
          )
          .send()
          .unwrap()
          .json();
        if !server_resp.is_ok() {
          println!("Cannot find server {}", &chunk.server_id);
        } else {
          let server = server_resp.unwrap();
          let mut resp = client
            .get(
              reqwest::Url::parse(
                format!("{}/api/download/{}", server.address, &chunk.id)
                  .as_str(),
              )
              .unwrap(),
            )
            .send()
            .unwrap();
          if resp.status().is_success() {
            let mut buf: Vec<u8> = vec![];
            resp.copy_to(&mut buf).unwrap();
            file.write(&buf).unwrap();
          }
        }
      }
    }
  }
}
