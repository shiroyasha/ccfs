use actix_web::error::{ErrorBadRequest, ErrorInternalServerError};
use actix_web::{HttpResponse, ResponseError};
use ccfs_commons::errors::CCFSResponseError;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum Error {
    #[snafu(display("{}", source))]
    Base { source: ccfs_commons::errors::Error },

    #[snafu(display("Unable to deserialize snapshot: {}", source))]
    Deserialize {
        source: std::boxed::Box<bincode::ErrorKind>,
    },

    #[snafu(display("Not found"))]
    NotFound,

    #[snafu(display("Missing required query param"))]
    MissingParam,
}

impl<'a> ResponseError for Error {
    fn error_response(&self) -> HttpResponse {
        use Error::*;
        let display = format!("{}", self);
        match self {
            Base { source } => source.error_response(),
            Deserialize { .. } | MissingParam { .. } => ErrorBadRequest(display).into(),
            NotFound { .. } => ErrorInternalServerError(display).into(),
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
