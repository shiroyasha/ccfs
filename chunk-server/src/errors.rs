use actix_web::error::{ErrorBadRequest, ErrorUnprocessableEntity};
use actix_web::{HttpResponse, ResponseError};
use ccfs_commons::errors::CCFSResponseError;
use snafu::Snafu;
use std::path::PathBuf;

impl From<Error> for CCFSResponseError {
    fn from(error: Error) -> CCFSResponseError {
        CCFSResponseError {
            inner: Box::new(error),
        }
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum Error {
    #[snafu(display("{}", inner))]
    Base { inner: ccfs_commons::errors::Error },

    #[snafu(display("Unable to create {}: {}", path.display(), source))]
    Create {
        source: tokio::io::Error,
        path: PathBuf,
    },

    #[snafu(display("Unable to read {}: {}", path.display(), source))]
    Read {
        source: tokio::io::Error,
        path: PathBuf,
    },

    #[snafu(display("Unable to write to {}: {}", path.display(), source))]
    Write {
        source: tokio::io::Error,
        path: PathBuf,
    },

    #[snafu(display("Unable to rename from {} to {}: {}", from.display(), to.display(), source))]
    Rename {
        source: tokio::io::Error,
        from: PathBuf,
        to: PathBuf,
    },

    #[snafu(display("Unable to parse to String: {}", source))]
    ParseString { source: std::string::FromUtf8Error },

    #[snafu(display("Unable to parse uuid {}: {}", text, source))]
    ParseUuid { source: uuid::Error, text: String },

    #[snafu(display("Unable to parse number {}: {}", text, source))]
    ParseNumber {
        source: std::num::ParseIntError,
        text: String,
    },

    #[snafu(display("Communication error with metadata server: {}", reason))]
    MetaServerCommunication { reason: String },

    #[snafu(display("Missing some form parts"))]
    MissingPart,
}

impl ResponseError for Error {
    fn error_response(&self) -> HttpResponse {
        let display = format!("{}", self);
        match self {
            Error::Base { inner } => inner.error_response(),

            Error::Write { .. }
            | Error::Read { .. }
            | Error::Create { .. }
            | Error::Rename { .. }
            | Error::MetaServerCommunication { .. }
            | Error::ParseString { .. }
            | Error::ParseUuid { .. }
            | Error::ParseNumber { .. } => ErrorUnprocessableEntity(display).into(),

            Error::MissingPart => ErrorBadRequest(display).into(),
        }
    }
}
