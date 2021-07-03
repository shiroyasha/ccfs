use actix_web::{web, App, HttpServer};
use ccfs_commons::http_utils::get_ip;
use chunk_server::jobs;
use chunk_server::routes::{download, replicate, upload};
use chunk_server::server_config::ServerConfig;
use std::env;
use std::sync::Arc;
use tokio::fs::create_dir_all;
use tokio::task;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let config_file_path = env::var("CONFIG_PATH").unwrap_or_else(|_| "./cs_config.yml".into());
    let config = Arc::new(ServerConfig::load_config(&config_file_path)?);

    let server_ip = get_ip().unwrap_or_else(|| "127.0.0.1".into());
    let server_addr = format!("http://{}:{}", server_ip, &config.port);
    let upload_path = dirs::home_dir()
        .expect("Couldn't determine home dir")
        .join("ccfs-uploads");
    create_dir_all(&upload_path).await?;

    task::spawn_local(jobs::start_ping_job(server_addr, config.clone()));

    let address = config.address();
    HttpServer::new(move || {
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
    })
    .bind(&address)?
    .run()
    .await
}
