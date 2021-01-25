use actix_web::HttpServer;
use chunk_server::server_config::ServerConfig;
use chunk_server::{create_app, jobs};
use std::env;
use std::sync::Arc;
use tokio::fs::create_dir_all;
use tokio::task;

#[cfg(target_os = "linux")]
fn get_ip() -> Option<String> {
    get_private_ip("eth0")
}

#[cfg(target_os = "macos")]
fn get_ip() -> Option<String> {
    get_private_ip("en0")
}

fn get_private_ip(target_name: &str) -> Option<String> {
    let interfaces = pnet::datalink::interfaces();
    interfaces.iter().find(|i| i.name == target_name).map(|i| {
        i.ips
            .iter()
            .find(|ip| ip.is_ipv4())
            .map(|ip| ip.ip().to_string())
    })?
}

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
    HttpServer::new(move || create_app(config.clone()))
        .bind(&address)?
        .run()
        .await
}
