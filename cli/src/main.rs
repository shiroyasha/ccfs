extern crate ccfs_commons;

use ccfs_commons::{Chunk, ChunkServer, File, CHUNK_SIZE};
use futures::future::join_all;
use rand::{seq::SliceRandom, thread_rng};
use reqwest::multipart::{Form, Part};
use reqwest::{Client, Response};
use serde::{de::DeserializeOwned, Serialize};
use slice_group_by::GroupBy;
use snafu::{ResultExt, Snafu};
use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use structopt::StructOpt;
use tokio::fs::File as FileFS;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::stream::StreamExt;
use uuid::Uuid;

const BUF_SIZE: usize = 16384;

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Unable to read file metadata {}: {}", path.display(), source))]
    ReadMetadata {
        source: tokio::io::Error,
        path: PathBuf,
    },

    #[snafu(display("Request to {} failed: {}", url, source))]
    FailedRequest { source: reqwest::Error, url: String },

    #[snafu(display("Unable to parse to json: {}", source))]
    ParseJson { source: reqwest::Error },

    #[snafu(display("Unable to {} file {}: {}", action, path.display(), source))]
    FileIO {
        source: tokio::io::Error,
        path: PathBuf,
        action: FileAction,
    },

    #[snafu(display("Chunk {} is currently not available", chunk_id))]
    ChunkNotAvailable { chunk_id: Uuid },

    #[snafu(display("Failed to download some chunks"))]
    SomeChunksNotAvailable,

    #[snafu(display("Failed to upload some chunks"))]
    UploadChunks,

    #[snafu(display("File doesn't exist: {}", path.display()))]
    FileNotExist { path: PathBuf },
}

#[derive(Debug)]
enum FileAction {
    Read,
    Write,
    Create,
    Open,
}
impl fmt::Display for FileAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FileAction::Read => write!(f, "read"),
            FileAction::Write => write!(f, "write"),
            FileAction::Create => write!(f, "create"),
            FileAction::Open => write!(f, "open"),
        }
    }
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, StructOpt)]
/// Chop-Chop File System
///
/// A distrubuted highly available file system
struct CliOpts {
    /// Sets a custom config file
    #[structopt(short, long, default_value = "./cli_config.yaml")]
    config: String,

    #[structopt(subcommand)]
    cmd: Command,
}

#[derive(Debug, StructOpt)]
enum Command {
    /// Upload files to the CCFS
    Upload {
        /// The local absolute or relative path to the file to be uploaded to CCFS
        file_path: String,
    },
    /// Download file from the CCFS
    Download {
        /// The path of the file on CCFS
        file_path: String,
    },
    /// Remove a file from the CCFS
    Remove {
        /// The path of the file on CCFS
        file_path: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let opts = CliOpts::from_args();
    let path = Path::new(&opts.config);
    if !path.exists() {
        panic!("{} file doesn't exists", &opts.config)
    }
    if path.is_dir() {
        panic!("{} is a directory", &opts.config)
    }

    let mut config_file = FileFS::open(path).await.context(FileIO {
        path,
        action: FileAction::Open,
    })?;
    let mut content = String::new();
    FileFS::read_to_string(&mut config_file, &mut content)
        .await
        .context(FileIO {
            path,
            action: FileAction::Read,
        })?;
    let config_map: HashMap<String, String> =
        serde_yaml::from_str(&content).expect("Couldn't deserialize config yaml");
    let meta_server_url = config_map
        .get("metadata-server-url")
        .expect("Couldn't find metadata-server-url config");
    let client = Client::new();

    match opts.cmd {
        Command::Upload { file_path } => {
            upload(&meta_server_url, &client, &file_path).await?;
        }
        Command::Download { file_path } => {
            download(&meta_server_url, &client, Path::new(&file_path), None).await?;
        }
        Command::Remove { file_path: _path } => {
            unimplemented!("Not implemented yet :(")
        }
    }
    Ok(())
}

async fn upload<T: AsRef<Path>>(meta_url: &str, client: &Client, file_path: T) -> Result<()> {
    let path = file_path.as_ref();
    if path.exists() && !path.is_dir() {
        let file_meta = path.metadata().context(ReadMetadata { path })?;
        let file_data = File::new(
            path.file_name().unwrap().to_str().unwrap().to_string(),
            file_meta.len(),
        );
        let upload_url = format!("{}/api/files/upload", meta_url);
        let file: File = post_request(&client, upload_url, &file_data)
            .await?
            .json()
            .await
            .context(ParseJson)?;
        let servers_url = format!("{}/api/servers", meta_url);
        let servers: Vec<ChunkServer> = get_request_json(&client, servers_url).await?;
        let mut f = FileFS::open(path).await.context(FileIO {
            path,
            action: FileAction::Open,
        })?;

        let parts_count = (file_meta.len() / CHUNK_SIZE) + 1;
        let mut file_parts = Vec::with_capacity(parts_count as usize);
        for file_part in 0..parts_count {
            let mut chunk = Vec::with_capacity(CHUNK_SIZE as usize);
            loop {
                let mut buff_size = BUF_SIZE;
                let remaining = CHUNK_SIZE as usize - chunk.len();
                if remaining < BUF_SIZE {
                    buff_size = remaining;
                }
                let mut temp = Vec::with_capacity(buff_size);
                let n = f.read_buf(&mut temp).await.context(FileIO {
                    path,
                    action: FileAction::Read,
                })?;
                chunk.append(&mut temp);
                if n < BUF_SIZE || CHUNK_SIZE == chunk.len() as u64 {
                    break;
                }
            }
            file_parts.push((file.id.to_string(), file_part.to_string(), chunk));
        }
        let requests = file_parts
            .into_iter()
            .map(|part| upload_chunk(client, &servers, part))
            .collect::<Vec<_>>();
        let responses = join_all(requests).await;
        if responses.iter().filter(|resp| resp.is_err()).size_hint().0 > 0 {
            return Err(Error::UploadChunks);
        }
        println!("Completed file upload");
        return Ok(());
    } else {
        return Err(Error::FileNotExist {
            path: path.to_path_buf(),
        });
    }
}

async fn upload_chunk(
    client: &Client,
    servers: &[ChunkServer],
    form_data: (String, String, Vec<u8>),
) -> Result<()> {
    let (file_id, file_part, raw_data) = form_data;
    let mut slice = servers.to_vec();
    slice.shuffle(&mut thread_rng());
    for server in servers {
        let upload_url = format!("{}/api/upload", server.address);
        let resp = client
            .post(&upload_url)
            .multipart(
                Form::new()
                    .text("file_id", file_id.clone())
                    .text("file_part_num", file_part.clone())
                    .part("file", Part::bytes(raw_data.clone())),
            )
            .send()
            .await
            .context(FailedRequest { url: upload_url })?;
        if resp.status().is_success() {
            return Ok(());
        }
    }
    Err(Error::ChunkNotAvailable {
        chunk_id: servers[0].id.into_inner(),
    })
}

async fn download(
    meta_url: &str,
    client: &Client,
    path: &Path,
    target_path: Option<&Path>,
) -> Result<()> {
    // get chunks and merge them into a file
    let file_url = format!("{}/api/files?path={}", meta_url, path.display());
    let file: File = get_request_json(&client, file_url).await?;
    let chunks_url = format!("{}/api/chunks/file/{}", meta_url, &file.id);
    let chunks: Vec<Chunk> = get_request_json(&client, chunks_url).await?;
    let groups = chunks.linear_group_by(|a, b| a.file_part_num == b.file_part_num);
    let default_path = format!("./{}", file.name);
    let path = target_path.unwrap_or_else(|| Path::new(&default_path));
    let mut file = FileFS::create(path).await.context(FileIO {
        path,
        action: FileAction::Create,
    })?;
    let mut requests = Vec::with_capacity(groups.size_hint().0);
    for group in groups {
        requests.push(download_chunk(client, group, meta_url));
    }
    let responses = join_all(requests).await;
    let errors = responses.iter().filter(|resp| resp.is_err());
    if errors.size_hint().0 > 0 {
        return Err(Error::SomeChunksNotAvailable);
    }
    let mut file_parts = responses
        .into_iter()
        .map(|resp| {
            let (order, resp) = resp.unwrap();
            (order, resp.bytes_stream())
        })
        .collect::<Vec<_>>();
    file_parts.sort_by(|a, b| a.0.cmp(&b.0));
    for (_part, mut stream) in file_parts {
        while let Some(content) = stream.next().await {
            file.write(&content.unwrap()).await.context(FileIO {
                path,
                action: FileAction::Write,
            })?;
        }
    }
    Ok(())
}

async fn download_chunk(
    client: &Client,
    servers: &[Chunk],
    meta_url: &str,
) -> Result<(u16, Response)> {
    for chunk in servers {
        let chunk_url = format!("{}/api/servers/{}", meta_url, &chunk.server_id);
        let resp: Response = get_request(&client, chunk_url).await?;
        if resp.status().is_success() {
            let server: ChunkServer = resp.json().await.context(ParseJson)?;
            let download_url = format!("{}/api/download/{}", server.address, &chunk.id);
            let download_resp = get_request(&client, download_url).await?;
            if download_resp.status().is_success() {
                return Ok((chunk.file_part_num, download_resp));
            }
        }
    }
    Err(Error::ChunkNotAvailable {
        chunk_id: servers[0].id.into_inner(),
    })
}

async fn get_request(client: &Client, url: String) -> Result<Response> {
    client.get(&url).send().await.context(FailedRequest { url })
}
async fn get_request_json<T: DeserializeOwned>(client: &Client, url: String) -> Result<T> {
    get_request(client, url)
        .await?
        .json()
        .await
        .context(ParseJson)
}
async fn post_request<T: Serialize>(client: &Client, url: String, data: &T) -> Result<Response> {
    client
        .post(&url)
        .json(data)
        .send()
        .await
        .context(FailedRequest { url })
}
