use actix_web::error::ErrorUnprocessableEntity;
use actix_web::{HttpResponse, ResponseError};
use snafu::Snafu;
use std::path::PathBuf;

pub type CCFSResult<T, E = Error> = Result<T, E>;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum Error {
    #[snafu(display("Unable to create {}: {}", path.display(), source))]
    IOCreate {
        source: tokio::io::Error,
        path: PathBuf,
    },

    #[snafu(display("Unable to read {}: {}", path.display(), source))]
    IORead {
        source: tokio::io::Error,
        path: PathBuf,
    },

    #[snafu(display("Unable to write to {}: {}", path.display(), source))]
    IOWrite {
        source: tokio::io::Error,
        path: PathBuf,
    },

    #[snafu(display("Unable to rename from {} to {}: {}", from.display(), to.display(), source))]
    Rename {
        source: tokio::io::Error,
        from: PathBuf,
        to: PathBuf,
    },

    #[snafu(display("Unable to deserialize snapshot: {}", source))]
    Deserialize {
        source: std::boxed::Box<bincode::ErrorKind>,
    },

    #[snafu(display("Unable to read file content: {}", source))]
    Read { source: std::io::Error },

    #[snafu(display("Not found"))]
    NotFound,

    #[snafu(display("Missing required query param"))]
    MissingParam,

    #[snafu(display("ReadLock poison error"))]
    ReadLock,

    #[snafu(display("WriteLock poison error"))]
    WriteLock,
}

impl<'a> ResponseError for Error {
    fn error_response(&self) -> HttpResponse {
        use Error::*;
        let display = format!("{}", self);
        match self {
            IOCreate { .. }
            | IOWrite { .. }
            | Rename { .. }
            | Deserialize { .. }
            | Read { .. }
            | NotFound { .. }
            | MissingParam { .. }
            | ReadLock { .. }
            | WriteLock { .. }
            | IORead { .. } => ErrorUnprocessableEntity(display).into(),
        }
    }
}
