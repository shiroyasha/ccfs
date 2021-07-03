use actix_web::error::{ErrorBadRequest, ErrorInternalServerError};
use actix_web::{HttpResponse, ResponseError};
use ccfs_commons::errors::{CCFSResponseError, Error as BaseError};
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum Error {
    #[snafu(display("{}", source))]
    Base { source: BaseError },

    #[snafu(display("Communication error with metadata server: {}", reason))]
    MetaServerCommunication { reason: String },

    #[snafu(display("Missing some form parts"))]
    MissingPart,

    #[snafu(display("Cannot create temp dir"))]
    TempDir { source: std::io::Error },
}

impl ResponseError for Error {
    fn error_response(&self) -> HttpResponse {
        use Error::*;
        let display = format!("{}", self);
        match self {
            Base { source } => source.error_response(),
            MetaServerCommunication { .. } | TempDir { .. } => {
                ErrorInternalServerError(display).into()
            }
            MissingPart => ErrorBadRequest(display).into(),
        }
    }
}

impl From<BaseError> for Error {
    fn from(error: BaseError) -> Self {
        Self::Base { source: error }
    }
}

impl From<Error> for CCFSResponseError {
    fn from(error: Error) -> Self {
        Self {
            inner: Box::new(error),
        }
    }
}
