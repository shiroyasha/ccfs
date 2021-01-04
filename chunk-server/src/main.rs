#[macro_use]
extern crate rocket;
#[macro_use]
extern crate rocket_contrib;
extern crate ccfs_commons;
extern crate dirs;
extern crate rocket_multipart_form_data;

mod errors;
mod routes;

use rocket_contrib::uuid::Uuid;
use routes::{download, multipart_upload};
use std::env;
use std::path::PathBuf;
use std::str::FromStr;
use tokio::task;
use tokio::time::{delay_for, Duration};

const METADATA_URL_KEY: &str = "METADATA_URL";
const SERVER_ID_KEY: &str = "SERVER_ID";

pub struct MetadataUrl(String);
pub struct ServerID(Uuid);
pub struct UploadsDir(PathBuf);

async fn start_ping_job(address: String, metadata_url: String, server_id: String) {
    loop {
        let _resp = reqwest::Client::new()
            .post(&format!("{}/api/ping", &metadata_url))
            .header("x-chunk-server-id", &server_id)
            .header("x-chunk-server-address", &address)
            .send()
            .await;
        delay_for(Duration::from_secs(5)).await;
    }
}

#[launch]
fn rocket() -> rocket::Rocket {
    let metadata_url = env::var(METADATA_URL_KEY)
        .unwrap_or_else(|_| panic!("missing {} env variable", METADATA_URL_KEY));
    let server_id = env::var(SERVER_ID_KEY)
        .unwrap_or_else(|_| panic!("missing {} env variable", SERVER_ID_KEY));
    let upload_path = dirs::home_dir()
        .expect("Couldn't determine home dir")
        .join("ccfs-uploads");

    let inst = rocket::ignite()
        .mount("/api", routes![multipart_upload, download])
        .manage(MetadataUrl(metadata_url.clone()))
        .manage(ServerID(
            Uuid::from_str(&server_id).expect("Server ID is not valid"),
        ))
        .manage(UploadsDir(upload_path));
    let server_addr = format!("http://{}:{}", inst.config().address, inst.config().port);
    task::spawn(start_ping_job(server_addr, metadata_url, server_id));

    inst
}
