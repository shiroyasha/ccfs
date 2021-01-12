mod errors;
mod routes;

use actix_web::{client::Client, web, App, HttpServer};
use ccfs_commons::data::Data;
use ccfs_commons::http_utils::read_body;
use routes::{download, upload};
use std::env;
use std::path::PathBuf;
use std::str::FromStr;
use tokio::task;
use tokio::time::{delay_for, Duration};
use uuid::Uuid;

const HOST: &str = "HOST";
const PORT: &str = "PORT";
const METADATA_URL_KEY: &str = "METADATA_URL";
const SERVER_ID_KEY: &str = "SERVER_ID";

pub type MetadataUrl = String;
pub type ServerID = Uuid;
pub type UploadsDir = PathBuf;

async fn start_ping_job(address: String, metadata_url: String, server_id: String) {
    let client = Client::new();
    loop {
        let res = client
            .post(&format!("{}/api/ping", metadata_url))
            .header("x-chunk-server-id", server_id.clone())
            .header("x-chunk-server-address", address.clone())
            .send()
            .await;
        match res {
            Ok(s) => match s.status().is_success() {
                true => println!("successfully pinged meta server"),
                false => println!("ping failed: {:?}", read_body(s).await),
            },
            Err(err) => {
                println!("ping failed: {}", err)
            }
        }
        delay_for(Duration::from_secs(5)).await;
    }
}

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
            .map(|ip| ip.to_string())
    })?
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let host = env::var(HOST).unwrap_or_else(|_| "127.0.0.1".into());
    let port = env::var(PORT).unwrap_or_else(|_| "8000".into());

    let server_ip = get_ip().unwrap_or_else(|| "127.0.0.1".into());
    let server_addr = format!("http://{}:{}", server_ip, port);
    let addr = format!("{}:{}", host, port);
    let metadata_url: MetadataUrl = env::var(METADATA_URL_KEY)
        .unwrap_or_else(|_| panic!("missing {} env variable", METADATA_URL_KEY));
    let server_id = env::var(SERVER_ID_KEY)
        .unwrap_or_else(|_| panic!("missing {} env variable", SERVER_ID_KEY));
    let id: ServerID = Uuid::from_str(&server_id).expect("Server ID is not valid");
    let upload_path: UploadsDir = dirs::home_dir()
        .expect("Couldn't determine home dir")
        .join("ccfs-uploads");

    let meta_url_state = Data::new(metadata_url.clone());
    let id_state = Data::new(id);
    let upload_path_state = Data::new(upload_path.clone());
    task::spawn_local(start_ping_job(server_addr, metadata_url, server_id));
    HttpServer::new(move || {
        App::new()
            .data(meta_url_state.clone())
            .data(id_state.clone())
            .data(upload_path_state.clone())
            .service(web::scope("/api").service(upload).service(download))
    })
    .bind(&addr)?
    .run()
    .await
}
