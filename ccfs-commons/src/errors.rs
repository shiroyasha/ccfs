use actix_web::error::{ErrorBadRequest, ErrorInternalServerError};
use actix_web::{HttpResponse, ResponseError};
use snafu::Snafu;
use snafu_cli_debug::SnafuCliDebug;
use std::path::PathBuf;

#[derive(Snafu, SnafuCliDebug)]
#[snafu(display("{}", inner))]
pub struct CCFSResponseError {
    pub inner: Box<dyn ResponseError>,
}

impl ResponseError for CCFSResponseError {
    fn error_response(&self) -> HttpResponse {
        self.inner.error_response()
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum Error {
    #[snafu(display("Unable to create '{}': {}", path.display(), source))]
    Create {
        source: tokio::io::Error,
        path: PathBuf,
    },

    #[snafu(display("Unable to open '{}': {}", path.display(), source))]
    Open {
        source: tokio::io::Error,
        path: PathBuf,
    },

    #[snafu(display("Unable to read '{}': {}", path.display(), source))]
    Read {
        source: tokio::io::Error,
        path: PathBuf,
    },

    #[snafu(display("Unable to write to '{}': {}", path.display(), source))]
    Write {
        source: tokio::io::Error,
        path: PathBuf,
    },

    #[snafu(display("Unable to rename from '{}' to '{}': {}", from.display(), to.display(), source))]
    Rename {
        source: tokio::io::Error,
        from: PathBuf,
        to: PathBuf,
    },

    #[snafu(display("Unable to remove '{}': {}", path.display(), source))]
    Remove {
        source: tokio::io::Error,
        path: PathBuf,
    },

    #[snafu(display("Unable to parse to String: '{}'", source))]
    ParseString { source: std::string::FromUtf8Error },

    #[snafu(display("Unable to parse uuid '{}': {}", text, source))]
    ParseUuid { source: uuid::Error, text: String },

    #[snafu(display("Request failed: {}", response))]
    Unsuccessful { response: String },

    #[snafu(display("Request to {} failed: {}", url, source))]
    FailedRequest {
        source: actix_web::client::SendRequestError,
        url: String,
    },

    #[snafu(display("'{}' is not a directory", path.display()))]
    NotADir { path: PathBuf },

    #[snafu(display("'{}' is a directory", path.display()))]
    NotAFile { path: PathBuf },

    #[snafu(display("Path '{}' doesn't exist", path.display()))]
    NotExist { path: PathBuf },

    #[snafu(display("Invalid path: {}", msg))]
    InvalidPath { msg: String },
}

impl ResponseError for Error {
    fn error_response(&self) -> HttpResponse {
        use Error::*;
        let display = format!("{}", self);
        match self {
            Create { .. }
            | Open { .. }
            | Read { .. }
            | Write { .. }
            | Rename { .. }
            | Remove { .. }
            | NotADir { .. }
            | NotAFile { .. }
            | NotExist { .. }
            | FailedRequest { .. }
            | Unsuccessful { .. } => ErrorInternalServerError(display).into(),
            InvalidPath { .. } | ParseString { .. } | ParseUuid { .. } => {
                ErrorBadRequest(display).into()
            }
        }
    }
}

impl From<Error> for CCFSResponseError {
    fn from(error: Error) -> CCFSResponseError {
        CCFSResponseError {
            inner: Box::new(error),
        }
    }
}
