use crate::server_config::ServerConfig;
use actix_web::client::Client;
use ccfs_commons::http_utils::read_body;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

pub async fn start_ping_job(address: String, config: Arc<ServerConfig>) {
    loop {
        // TODO: investigate why using a client initialize outside the loop occasionally gives `connector has been disconnected` error
        let client = Client::new();
        let res = client
            .post(&format!("{}/api/ping", config.metadata_url))
            .insert_header(("x-ccfs-chunk-server-id", config.server_id.to_string()))
            .insert_header(("x-ccfs-chunk-server-address", address.clone()))
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
        sleep(Duration::from_secs(config.ping_interval)).await;
    }
}
