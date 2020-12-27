extern crate ccfs_commons;
extern crate clap;
extern crate reqwest;

use ccfs_commons::{Chunk, ChunkServer, File, CHUNK_SIZE};
use clap::{App, Arg, SubCommand};
use futures_util::StreamExt;
use reqwest::multipart::Part;
use std::collections::HashMap;
use std::error::Error;
use std::path::{Path, PathBuf};
use std::str;
use tokio::fs::File as FileFS;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
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
    let config_map: HashMap<String, String> = serde_yaml::from_reader(config_file).unwrap();
    let meta_server_url = config_map.get("metadata-server-url").unwrap();
    let client = reqwest::Client::new();

    if let Some(ref matches) = matches.subcommand_matches("upload") {
        upload(
            &meta_server_url,
            client,
            matches.value_of("file_path").unwrap(),
        )
        .await
        .unwrap();
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
        download(&meta_server_url, client, file_id, path).await?;
    } else if let Some(ref _matches) = matches.subcommand_matches("remove") {
        unimplemented!("Not implemented yet :(")
    } else {
        println!("Some other subcommand was used");
    }
    Ok(())
}

async fn upload(
    meta_server_url: &str,
    client: reqwest::Client,
    path: &str,
) -> Result<(), reqwest::Error> {
    let file_path = Path::new(path);
    if file_path.exists() {
        let size = file_path.metadata().unwrap().len();
        let file_data = File::new(file_path.to_str().unwrap().to_string(), size);
        let file_resp: Result<File, _> = client
            .post(&format!("{}/api/files/upload", meta_server_url))
            .json(&file_data)
            .send()
            .await?
            .json()
            .await;
        let servers: Vec<ChunkServer> = client
            .get(&format!("{}/api/servers", meta_server_url))
            .send()
            .await?
            .json()
            .await?;
        match file_resp {
            Ok(file) => {
                println!("file id: {}", file.id);
                let mut i = 0usize;
                let mut file_part = 1usize;
                let mut f = FileFS::open(file_path).await.unwrap();

                loop {
                    let mut chunk = Vec::with_capacity(CHUNK_SIZE as usize);
                    let n = f.read_buf(&mut chunk).await.unwrap();
                    if n == 0 && chunk.is_empty() {
                        break;
                    }
                    let form = reqwest::multipart::Form::new()
                        .text("file_id", file.id.to_string())
                        .text("file_part_num", file_part.to_string())
                        .part("file", Part::bytes(chunk));
                    let server = &servers[i];
                    i += 1;
                    client
                        .post(format!("{}/api/upload", server.address).as_str())
                        .multipart(form)
                        .send()
                        .await?;

                    file_part += 1;

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
    Ok(())
}

async fn download(
    meta_server_url: &str,
    client: reqwest::Client,
    file_id: &str,
    path: &Path,
) -> Result<(), reqwest::Error> {
    // get chunks and merge them into a file
    let file_resp: Result<File, _> = client
        .get(
            reqwest::Url::parse(format!("{}/api/files/{}", meta_server_url, &file_id).as_str())
                .unwrap(),
        )
        .send()
        .await?
        .json()
        .await;
    if let Ok(file) = file_resp {
        let chunks_resp: Result<Vec<Chunk>, _> = client
            .get(
                reqwest::Url::parse(
                    format!("{}/api/chunks/file/{}", meta_server_url, &file.id).as_str(),
                )
                .unwrap(),
            )
            .send()
            .await?
            .json()
            .await;
        if let Ok(mut chunks) = chunks_resp {
            chunks.sort_by(|a, b| a.file_part_num.cmp(&b.file_part_num));
            let mut file = FileFS::create(path).await.unwrap();
            for chunk in chunks.iter() {
                let server: ChunkServer = client
                    .get(
                        reqwest::Url::parse(
                            format!("{}/api/servers/{}", meta_server_url, &chunk.server_id)
                                .as_str(),
                        )
                        .unwrap(),
                    )
                    .send()
                    .await?
                    .json()
                    .await?;
                let mut stream = reqwest::get(
                    reqwest::Url::parse(
                        format!("{}/api/download/{}", server.address, &chunk.id).as_str(),
                    )
                    .unwrap(),
                )
                .await?
                .bytes_stream();
                while let Some(content) = stream.next().await {
                    file.write(&content.unwrap()).await.unwrap();
                }
            }
        }
    }
    Ok(())
}
