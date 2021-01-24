mod errors;
mod file_ops;

use actix_web::client::Client;
use ccfs_commons::errors::{CCFSResponseError, Error as BaseError};
use errors::*;
use file_ops::{download, list, tree, upload};
use snafu::ResultExt;
use std::collections::HashMap;
use std::path::PathBuf;
use structopt::StructOpt;
use tokio::fs::read_to_string;

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
        return Err(BaseError::NotAFile { path }.into());
    }

    let content = read_to_string(&path)
        .await
        .map_err(|source| BaseError::Read { path, source })?;
    let config_map: HashMap<String, String> = serde_yaml::from_str(&content).context(ParseYaml)?;
    let key = "metadata-server-url";
    let meta_url = config_map
        .get(key)
        .ok_or_else(|| CCFSResponseError::from(MissingConfigVal { key }.build()))?;

    let client = Client::new();
    match opts.cmd {
        Command::Upload { file_path } => upload(&client, &meta_url, &file_path).await?,
        Command::Download { file_path } => {
            download(&client, &meta_url, &file_path, None, false).await?
        }
        Command::Remove { file_path: _path } => unimplemented!(),
        Command::List => list(&client, &meta_url).await?,
        Command::Tree => tree(&client, &meta_url).await?,
    };
    Ok(())
}
