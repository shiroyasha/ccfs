use actix_web::error::{ErrorBadRequest, ErrorInternalServerError};
use actix_web::{HttpResponse, ResponseError};
use ccfs_commons::errors::CCFSResponseError;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum Error {
    #[snafu(display("{}", source))]
    Base { source: ccfs_commons::errors::Error },

    #[snafu(display("Communication error with metadata server: {}", reason))]
    MetaServerCommunication { reason: String },

    #[snafu(display("Missing some form parts"))]
    MissingPart,

    #[snafu(display("Missing some headers"))]
    MissingHeader,
}

impl ResponseError for Error {
    fn error_response(&self) -> HttpResponse {
        use Error::*;
        let display = format!("{}", self);
        match self {
            Base { source } => source.error_response(),
            MetaServerCommunication { .. } => ErrorInternalServerError(display).into(),
            MissingPart | MissingHeader => ErrorBadRequest(display).into(),
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
