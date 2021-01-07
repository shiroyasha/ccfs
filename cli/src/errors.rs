use snafu::Snafu;
use std::fmt;
use std::path::PathBuf;
use uuid::Uuid;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum Error {
    #[snafu(display("Unable to read file metadata {}: {}", path.display(), source))]
    ReadMetadata {
        source: tokio::io::Error,
        path: PathBuf,
    },

    #[snafu(display("Request to {} failed: {}", url, source))]
    FailedRequest {
        source: actix_web::client::SendRequestError,
        url: String,
    },

    #[snafu(display("Unable to parse to json: {}", source))]
    ParseJson {
        source: actix_web::client::JsonPayloadError,
    },

    #[snafu(display("Unable to read payload: {}", source))]
    ParseBytes {
        source: actix_web::client::PayloadError,
    },

    #[snafu(display("Unable to parse yaml: {}", source))]
    ParseYaml { source: serde_yaml::Error },

    #[snafu(display("Unable to {} file {}: {}", action, path.display(), source))]
    FileIO {
        source: tokio::io::Error,
        path: PathBuf,
        action: FileAction,
    },

    #[snafu(display("Chunk {} is currently not available", chunk_name))]
    ChunkNotAvailable { chunk_name: String },

    #[snafu(display("Failed to download some chunks"))]
    SomeChunksNotAvailable,

    #[snafu(display("Failed to upload some chunks"))]
    UploadChunks,

    #[snafu(display("Failed to upload chunk {} for file {}", part, chunk_id))]
    UploadSingleChunk { part: usize, chunk_id: Uuid },

    #[snafu(display("File doesn't exist: {}", path.display()))]
    FileNotExist { path: PathBuf },

    #[snafu(display("{} is a directory", path.display()))]
    NotAFile { path: PathBuf },

    #[snafu(display("Missing config value {}", key))]
    MissingConfigVal { key: String },
}

#[derive(Debug, Clone, Copy)]
pub enum FileAction {
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
