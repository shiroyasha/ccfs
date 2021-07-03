mod errors;
mod file_ops;

use ccfs_commons::errors::{self as base, CCFSResponseError};
use errors::*;
use file_ops::{download, list, tree, upload};
use snafu::ResultExt;
use std::collections::HashMap;
use std::path::PathBuf;
use structopt::StructOpt;
use tokio::fs::read_to_string;
use uuid::Uuid;
use reqwest::Client;

#[derive(Debug, StructOpt)]
/// Chop-Chop File System
///
/// A distrubuted highly available file system
struct CliOpts {
    /// Sets a custom config file
    #[structopt(short, long, default_value = "./cli_config.yml")]
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
    /// List directory content
    List,
    /// Print directory tree structure
    Tree,
}

#[actix_web::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts = CliOpts::from_args();
    let path = PathBuf::from(&opts.config);
    if !path.exists() {
        return Err(FileNotExist { path }.build().into());
    }
    if path.is_dir() {
        return Err(base::NotAFile { path }.build().into());
    }

    let content = read_to_string(&path).await.context(base::Read { path })?;
    let config_map: HashMap<String, String> = serde_yaml::from_str(&content).context(ParseYaml)?;
    let key = "metadata-server-url";
    let meta_url = config_map
        .get(key)
        .ok_or_else(|| CCFSResponseError::from(MissingConfigVal { key }.build()))?;

    let client = Client::new();
    let client_id = Uuid::new_v4();
    match opts.cmd {
        Command::Upload { file_path } => upload(&client, &client_id, &meta_url, &file_path).await?,
        Command::Download { file_path } => {
            download(&client, &client_id, &meta_url, &file_path, None, false).await?
        }
        Command::Remove { file_path: _path } => unimplemented!(),
        Command::List => list(&client, &client_id, &meta_url).await?,
        Command::Tree => tree(&client, &client_id, &meta_url).await?,
    };
    Ok(())
}
