use actix_http::http::StatusCode;
use actix_web::test;
use ccfs_commons::{ChunkServer, FileMetadata};
use chrono::{Duration, Utc};
use metadata_server::create_app;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use test::{call_service, init_service, TestRequest};
use uuid::Uuid;

#[actix_rt::test]
async fn test_ping_server_does_not_exist() -> std::io::Result<()> {
    let servers = Arc::new(RwLock::new(HashMap::new()));
    let chunks = Arc::new(RwLock::new(HashMap::new()));
    let files = Arc::new(RwLock::new(HashMap::new()));
    let metadata_tree = Arc::new(RwLock::new(FileMetadata::create_root()));
    let mut server = init_service(create_app(servers.clone(), chunks, files, metadata_tree)).await;

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

    let servers_map = servers.read().unwrap();
    assert_eq!(servers_map.len(), 1);

    let s = servers_map.values().next().unwrap();
    assert_eq!(s.id.to_string(), "1a6e7006-12a7-4935-b8c0-58fa7ea84b09");
    assert_eq!(s.address, "http://localhost:7654");
    assert!(Utc::now().signed_duration_since(s.latest_ping_time) < Duration::seconds(1));
    Ok(())
}

#[actix_rt::test]
async fn test_ping_server_missing_header() -> std::io::Result<()> {
    let servers = Arc::new(RwLock::new(HashMap::new()));
    let chunks = Arc::new(RwLock::new(HashMap::new()));
    let files = Arc::new(RwLock::new(HashMap::new()));
    let metadata_tree = Arc::new(RwLock::new(FileMetadata::create_root()));
    let mut server = init_service(create_app(servers.clone(), chunks, files, metadata_tree)).await;

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
    let servers = Arc::new(RwLock::new(map));
    let chunks = Arc::new(RwLock::new(HashMap::new()));
    let files = Arc::new(RwLock::new(HashMap::new()));
    let metadata_tree = Arc::new(RwLock::new(FileMetadata::create_root()));
    let mut server = init_service(create_app(servers.clone(), chunks, files, metadata_tree)).await;

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

    let servers_map = servers.read().unwrap();
    assert_eq!(servers_map.len(), 1);

    let s = servers_map.values().next().unwrap();
    assert_eq!(s.id.to_string(), "1a6e7006-12a7-4935-b8c0-58fa7ea84b09");
    assert_eq!(s.address, "http://localhost:7654");
    assert!(s.latest_ping_time.signed_duration_since(old_time) > Duration::seconds(0));
    Ok(())
}
