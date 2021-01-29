use actix_http::http::StatusCode;
use actix_web::{test, web, App};
use ccfs_commons::ChunkServer;
use chrono::{Duration, Utc};
use metadata_server::routes::chunk_server_ping;
use metadata_server::ServersMap;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use test::{call_service, init_service, TestRequest};
use tokio::sync::RwLock;
use uuid::Uuid;

#[actix_rt::test]
async fn test_ping_server_does_not_exist() -> std::io::Result<()> {
    let servers: ServersMap = Arc::new(RwLock::new(HashMap::new()));
    let mut server = init_service(
        App::new()
            .data(servers.clone())
            .service(web::scope("/api").service(chunk_server_ping)),
    )
    .await;

    let req = TestRequest::post()
        .uri("/api/ping")
        .header(
            "x-ccfs-chunk-server-id",
            "1a6e7006-12a7-4935-b8c0-58fa7ea84b09",
        )
        .header("x-ccfs-chunk-server-address", "http://localhost:7654")
        .to_request();
    let resp = call_service(&mut server, req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    let servers_map = servers.read().await;
    assert_eq!(servers_map.len(), 1);

    let s = servers_map.values().next().unwrap();
    assert_eq!(s.id.to_string(), "1a6e7006-12a7-4935-b8c0-58fa7ea84b09");
    assert_eq!(s.address, "http://localhost:7654");
    assert!(Utc::now().signed_duration_since(s.latest_ping_time) < Duration::seconds(1));
    Ok(())
}

#[actix_rt::test]
async fn test_ping_server_missing_header() -> std::io::Result<()> {
    let servers: ServersMap = Arc::new(RwLock::new(HashMap::new()));
    let mut server = init_service(
        App::new()
            .data(servers)
            .service(web::scope("/api").service(chunk_server_ping)),
    )
    .await;

    let req = TestRequest::post()
        .uri("/api/ping")
        .header("x-ccfs-chunk-server-address", "http://localhost:7654")
        .to_request();
    let resp = call_service(&mut server, req).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

#[actix_rt::test]
async fn test_ping_server_when_already_exists() -> std::io::Result<()> {
    let mut map = HashMap::new();
    let s = ChunkServer::new(
        Uuid::from_str("1a6e7006-12a7-4935-b8c0-58fa7ea84b09").unwrap(),
        "http://localhost:7654".into(),
    );
    let old_time = s.latest_ping_time;
    map.insert(s.id, s);
    let servers: ServersMap = Arc::new(RwLock::new(map));
    let mut server = init_service(
        App::new()
            .data(servers.clone())
            .service(web::scope("/api").service(chunk_server_ping)),
    )
    .await;

    let req = TestRequest::post()
        .uri("/api/ping")
        .header(
            "x-ccfs-chunk-server-id",
            "1a6e7006-12a7-4935-b8c0-58fa7ea84b09",
        )
        .header("x-ccfs-chunk-server-address", "http://localhost:7654")
        .to_request();
    let resp = call_service(&mut server, req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    let servers_map = servers.read().await;
    assert_eq!(servers_map.len(), 1);

    let s = servers_map.values().next().unwrap();
    assert_eq!(s.id.to_string(), "1a6e7006-12a7-4935-b8c0-58fa7ea84b09");
    assert_eq!(s.address, "http://localhost:7654");
    assert!(s.latest_ping_time.signed_duration_since(old_time) > Duration::seconds(0));
    Ok(())
}
