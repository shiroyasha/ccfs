mod errors;
pub mod jobs;
mod routes;

use crate::routes::{download, replicate, upload};
use actix_service::ServiceFactory;
use actix_web::{dev, web, App};
use std::path::PathBuf;
use uuid::Uuid;

pub type MetadataUrl = String;
pub type ServerID = Uuid;
pub type UploadsDir = PathBuf;

pub fn create_app(
    metadata_url: MetadataUrl,
    server_id: ServerID,
    upload_path: UploadsDir,
) -> App<
    impl ServiceFactory<
        Config = (),
        Request = dev::ServiceRequest,
        Response = dev::ServiceResponse<actix_http::body::Body>,
        Error = actix_http::Error,
        InitError = (),
    >,
    dev::Body,
> {
    App::new()
        .data(metadata_url)
        .data(server_id)
        .data(upload_path)
        .service(
            web::scope("/api")
                .service(upload)
                .service(download)
                .service(replicate),
        )
}
