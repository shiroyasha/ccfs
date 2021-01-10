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

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let host = "127.0.0.1";
    let port = "8000";
    let addr = format!("{}:{}", host, port);
    let server_addr = format!("http://{}", addr);
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
