use crate::raft::data::ClientRequest;
use actix_web::error::{ErrorBadRequest, ErrorInternalServerError};
use actix_web::{HttpResponse, ResponseError};
use async_raft::error::ClientReadError;
use async_raft::ClientWriteError;
use ccfs_commons::errors::{CCFSResponseError, Error as BaseError};
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

    #[snafu(display("Couldn't process read request"))]
    ClientRead { source: ClientReadError },

    #[snafu(display("Couldn't process write request"))]
    ClientWrite {
        source: ClientWriteError<ClientRequest>,
    },
}

impl<'a> ResponseError for Error {
    fn error_response(&self) -> HttpResponse {
        use Error::*;
        let display = format!("{}", self);
        match self {
            Base { source } => source.error_response(),
            Deserialize { .. } | MissingParam { .. } => ErrorBadRequest(display).into(),
            NotFound { .. } | ClientRead { .. } | ClientWrite { .. } => {
                ErrorInternalServerError(display).into()
            }
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
