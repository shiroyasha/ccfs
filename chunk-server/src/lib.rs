mod errors;
pub mod jobs;
mod routes;
pub mod server_config;

use crate::routes::{download, replicate, upload};
use actix_service::ServiceFactory;
use actix_web::{dev, web, App};
use server_config::ServerConfig;
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;

pub type MetadataUrl = String;
pub type ServerID = Uuid;
pub type UploadsDir = PathBuf;

pub fn create_app(
    config: Arc<ServerConfig>,
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
        .data(config.metadata_url.clone())
        .data(config.server_id)
        .data(config.upload_path.clone())
        .service(
            web::scope("/api")
                .service(upload)
                .service(download)
                .service(replicate),
        )
}
