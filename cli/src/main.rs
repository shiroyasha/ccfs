mod errors;
mod file_ops;

use errors::{FileAction, Result};
use file_ops::{download, list, tree, upload};
use reqwest::Client;
use snafu::ResultExt;
use std::collections::HashMap;
use std::path::Path;
use structopt::StructOpt;
use tokio::fs::File as FileFS;
use tokio::io::AsyncReadExt;

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
    /// List directory content
    List,
    /// Print directory tree structure
    Tree,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opts = CliOpts::from_args();
    let path = Path::new(&opts.config);
    if !path.exists() {
        return Err(errors::FileNotExist { path }.build());
    }
    if path.is_dir() {
        return Err(errors::NotAFile { path }.build());
    }

    let mut config_file = FileFS::open(path).await.context(errors::FileIO {
        path,
        action: FileAction::Open,
    })?;
    let mut content = String::new();
    FileFS::read_to_string(&mut config_file, &mut content)
        .await
        .context(errors::FileIO {
            path,
            action: FileAction::Read,
        })?;
    let config_map: HashMap<String, String> =
        serde_yaml::from_str(&content).context(errors::ParseYaml)?;
    let key = "metadata-server-url";
    let meta_url = config_map
        .get(key)
        .ok_or_else(|| errors::MissingConfigVal { key }.build())?;

    let client = Client::new();
    match opts.cmd {
        Command::Upload { file_path } => upload(&client, &meta_url, &file_path).await?,
        Command::Download { file_path } => download(&client, &meta_url, &file_path, None).await?,
        Command::Remove { file_path: _path } => unimplemented!(),
        Command::List => list(&client, &meta_url).await?,
        Command::Tree => tree(&client, &meta_url).await?,
    }
    Ok(())
}
