use std::path::PathBuf;

use actix_web::error::ErrorUnprocessableEntity;
use actix_web::{HttpResponse, ResponseError};
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub struct CCFSResponseError {
    pub inner: Box<dyn ResponseError>,
}

impl From<Error> for CCFSResponseError {
    fn from(error: Error) -> CCFSResponseError {
        CCFSResponseError {
            inner: Box::new(error),
        }
    }
}

impl ResponseError for CCFSResponseError {
    fn error_response(&self) -> HttpResponse {
        self.inner.error_response()
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum Error {
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

    #[snafu(display("Unable to parse to String: {}", source))]
    ParseString { source: std::string::FromUtf8Error },
}

impl ResponseError for Error {
    fn error_response(&self) -> HttpResponse {
        use Error::*;
        let display = format!("{}", self);
        match self {
            Create { .. } | Read { .. } | Write { .. } | ParseString { .. } => {
                ErrorUnprocessableEntity(display).into()
            }
        }
    }
}
