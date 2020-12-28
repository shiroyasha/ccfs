extern crate ccfs_commons;
extern crate reqwest;

use ccfs_commons::{Chunk, ChunkServer, File, CHUNK_SIZE};
use futures_util::StreamExt;
use reqwest::multipart::Part;
use snafu::{ResultExt, Snafu};
use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::str;
use structopt::StructOpt;
use tokio::fs::File as FileFS;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
    let client = reqwest::Client::new();

    match opts.cmd {
        Command::Upload { file_path } => {
            upload(&meta_server_url, client, &file_path).await?;
        }
        Command::Download { file_path } => {
            download(&meta_server_url, client, Path::new(&file_path), None).await?;
        }
        Command::Remove { file_path: _path } => {
            unimplemented!("Not implemented yet :(")
        }
    }
    Ok(())
}

async fn upload<T: AsRef<Path>>(
    meta_server_url: &str,
    client: reqwest::Client,
    file_path: T,
) -> Result<()> {
    let path = file_path.as_ref();
    if path.exists() && !path.is_dir() {
        let file_meta = path.metadata().context(ReadMetadata { path })?;
        let file_data = File::new(
            path.file_name().unwrap().to_str().unwrap().to_string(),
            file_meta.len(),
        );
        let upload_url = format!("{}/api/files/upload", meta_server_url);
        let file: File = client
            .post(&upload_url)
            .json(&file_data)
            .send()
            .await
            .context(FailedRequest { url: upload_url })?
            .json()
            .await
            .context(ParseJson)?;
        let servers_url = format!("{}/api/servers", meta_server_url);
        let servers: Vec<ChunkServer> = client
            .get(&servers_url)
            .send()
            .await
            .context(FailedRequest { url: servers_url })?
            .json()
            .await
            .context(ParseJson)?;
        let mut i = 0;
        let mut file_part = 0usize;
        let mut f = FileFS::open(path).await.context(FileIO {
            path,
            action: FileAction::Open,
        })?;

        loop {
            let mut chunk = Vec::with_capacity(CHUNK_SIZE as usize);
            let n = f.read_buf(&mut chunk).await.context(FileIO {
                path,
                action: FileAction::Read,
            })?;
            if n == 0 && chunk.is_empty() {
                break;
            }
            let form = reqwest::multipart::Form::new()
                .text("file_id", file.id.to_string())
                .text("file_part_num", file_part.to_string())
                .part("file", Part::bytes(chunk));
            let server = &servers[i];
            i += 1;
            let upload_url = format!("{}/api/upload", server.address);
            client
                .post(&upload_url)
                .multipart(form)
                .send()
                .await
                .context(FailedRequest { url: upload_url })?;

            file_part += 1;

            if n < CHUNK_SIZE as usize {
                break;
            }
        }
        println!("Completed file upload");
    } else {
        panic!("The file {} doesn't exists", path.display());
    }
    Ok(())
}

async fn download(
    meta_server_url: &str,
    client: reqwest::Client,
    file_path: &Path,
    target_path: Option<&Path>,
) -> Result<()> {
    // get chunks and merge them into a file
    let file_url = format!(
        "{}/api/files/{}",
        meta_server_url,
        &file_path.file_name().unwrap().to_str().unwrap().to_string()
    );
    let file: File = client
        .get(&file_url)
        .send()
        .await
        .context(FailedRequest { url: file_url })?
        .json()
        .await
        .context(ParseJson)?;
    let chunks_url = format!("{}/api/chunks/file/{}", meta_server_url, &file.id);
    let mut chunks: Vec<Chunk> = client
        .get(&chunks_url)
        .send()
        .await
        .context(FailedRequest { url: chunks_url })?
        .json()
        .await
        .context(ParseJson)?;
    chunks.sort_by(|a, b| a.file_part_num.cmp(&b.file_part_num));
    let default_path = format!("./{}", file.name);
    let path = target_path.unwrap_or_else(|| Path::new(&default_path));
    let mut file = FileFS::create(path).await.context(FileIO {
        path,
        action: FileAction::Create,
    })?;
    for chunk in chunks.iter() {
        let chunk_url = format!("{}/api/servers/{}", meta_server_url, &chunk.server_id);
        let server: ChunkServer = client
            .get(&chunk_url)
            .send()
            .await
            .context(FailedRequest { url: chunk_url })?
            .json()
            .await
            .context(ParseJson)?;
        let download_url = format!("{}/api/download/{}", server.address, &chunk.id);
        let mut stream = client
            .get(&download_url)
            .send()
            .await
            .context(FailedRequest { url: download_url })?
            .bytes_stream();
        while let Some(content) = stream.next().await {
            file.write(&content.unwrap()).await.context(FileIO {
                path,
                action: FileAction::Write,
            })?;
        }
    }
    Ok(())
}
