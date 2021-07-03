use actix_web::error::{ErrorBadRequest, ErrorInternalServerError};
use actix_web::{HttpResponse, ResponseError};
use ccfs_commons::errors::{CCFSResponseError, Error as BaseError};
use snafu::Snafu;
use std::path::PathBuf;
use uuid::Uuid;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum Error {
    #[snafu(display("{}", source))]
    Base { source: BaseError },

    #[snafu(display("Unable to parse to json: {}", source))]
    ParseJson { source: reqwest::Error },

    #[snafu(display("Unable to parse yaml: {}", source))]
    ParseYaml { source: serde_yaml::Error },

    #[snafu(display("Chunk {} is currently not available", chunk_name))]
    ChunkNotAvailable { chunk_name: String },

    #[snafu(display("Failed to download some chunks"))]
    SomeChunksNotAvailable,

    #[snafu(display("Failed to upload some chunks"))]
    UploadChunks,

    #[snafu(display("Failed to upload chunk {} for file {}", part, chunk_id))]
    UploadSingleChunk { part: usize, chunk_id: Uuid },

    #[snafu(display("File doesn't exist: '{}'", path.display()))]
    FileNotExist { path: PathBuf },

    #[snafu(display("'{}' already exist", path.display()))]
    AlreadyExists { path: PathBuf },

    #[snafu(display("Missing config value '{}'", key))]
    MissingConfigVal { key: String },

    #[snafu(display("There are no available servers, try again later"))]
    NoAvailableServers,

    #[snafu(display("Cannot create temp dir"))]
    TempDir { source: std::io::Error },
}

impl<'a> ResponseError for Error {
    fn error_response(&self) -> HttpResponse {
        use Error::*;
        let display = format!("{}", self);
        match self {
            Base { source } => source.error_response(),
            ParseJson { .. } | ParseYaml { .. } => ErrorBadRequest(display).into(),
            ChunkNotAvailable { .. }
            | SomeChunksNotAvailable { .. }
            | UploadChunks { .. }
            | UploadSingleChunk { .. }
            | FileNotExist { .. }
            | TempDir { .. }
            | AlreadyExists { .. }
            | NoAvailableServers { .. }
            | MissingConfigVal { .. } => ErrorInternalServerError(display).into(),
        }
    }
}

impl From<Error> for CCFSResponseError {
    fn from(error: Error) -> Self {
        Self {
            inner: Box::new(error),
        }
    }
}

impl From<BaseError> for Error {
    fn from(error: BaseError) -> Self {
        Self::Base { source: error }
    }
}
