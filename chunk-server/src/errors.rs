use rocket::http::Status;
use rocket::request::Request;
use rocket::response::Responder;
use rocket::Response;
use snafu::Snafu;
use std::path::PathBuf;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum Error {
    #[snafu(display("Unable to create {}: {}", path.display(), source))]
    IOCreate {
        source: tokio::io::Error,
        path: PathBuf,
    },

    #[snafu(display("Unable to write to {}: {}", path.display(), source))]
    IOWrite {
        source: tokio::io::Error,
        path: PathBuf,
    },

    #[snafu(display("Unable to parse multipart form data: {}", source))]
    ParseData {
        source: rocket_multipart_form_data::MultipartFormDataError,
    },

    #[snafu(display("Unable to parse uuid {}: {}", text, source))]
    ParseUuid {
        source: rocket_contrib::uuid::uuid_crate::Error,
        text: String,
    },

    #[snafu(display("Unable to parse number {}: {}", text, source))]
    ParseNumber {
        source: std::num::ParseIntError,
        text: String,
    },

    #[snafu(display("Communication error with metadata server: {}", source))]
    MetaServerCommunication { source: reqwest::Error },

    #[snafu(display("Missing form part {}", key))]
    MissingPart { key: String },
}

impl<'r> Responder<'r, 'static> for Error {
    fn respond_to(self, _request: &'r Request<'_>) -> rocket::response::Result<'static> {
        Response::build().status(Status::InternalServerError).ok()
    }
}
