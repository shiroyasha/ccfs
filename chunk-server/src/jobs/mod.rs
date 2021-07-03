use crate::server_config::ServerConfig;
use reqwest::Client;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

pub async fn start_ping_job(address: String, config: Arc<ServerConfig>) {
    let meta_url = format!("{}/api/ping", config.metadata_url);
    let server_id = config.server_id.to_string();
    let client = Client::new();
    loop {
        // TODO: investigate why using a client initialize outside the loop occasionally gives `connector has been disconnected` error
        let res = client
            .post(&meta_url)
            .header("x-ccfs-chunk-server-id", &server_id)
            .header("x-ccfs-chunk-server-address", &address)
            .send()
            .await;
        match res {
            Ok(s) => match s.status().is_success() {
                true => println!("successfully pinged meta server"),
                false => println!("ping failed: {:?}", s.text().await),
            },
            Err(err) => {
                println!("ping failed: {}", err)
            }
        }
        sleep(Duration::from_secs(config.ping_interval)).await;
    }
}
