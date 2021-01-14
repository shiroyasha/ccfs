use actix_web::client::Client;
use ccfs_commons::http_utils::read_body;
use tokio::time::{delay_for, Duration};

pub async fn start_ping_job(address: String, metadata_url: String, server_id: String) {
    let client = Client::new();
    loop {
        let res = client
            .post(&format!("{}/api/ping", metadata_url))
            .header("x-ccfs-chunk-server-id", server_id.clone())
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
        delay_for(Duration::from_secs(5)).await;
    }
}
