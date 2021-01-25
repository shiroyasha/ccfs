use crate::server_config::ServerConfig;
use actix_web::client::Client;
use ccfs_commons::http_utils::read_body;
use std::sync::Arc;
use tokio::time::{delay_for, Duration};

pub async fn start_ping_job(address: String, config: Arc<ServerConfig>) {
    loop {
        // TODO: investigate why using a client initialize outside the loop occasionally gives `connector has been disconnected` error
        let client = Client::new();
        let res = client
            .post(&format!("{}/api/ping", config.metadata_url))
            .header("x-ccfs-chunk-server-id", config.server_id.to_string())
            .header("x-ccfs-chunk-server-address", address.clone())
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
        delay_for(Duration::from_secs(config.ping_interval)).await;
    }
}
