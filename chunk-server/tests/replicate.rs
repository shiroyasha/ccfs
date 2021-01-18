mod utils;

use actix_http::http::StatusCode;
use actix_web::test::{call_service, init_service, TestRequest};
use ccfs_commons::chunk_name;
use chunk_server::create_app;
use httpmock::{Method, MockServer};
use tempfile::tempdir;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

#[actix_rt::test]
async fn test_successful_replication() -> std::io::Result<()> {
    let chunk_id = "1a6e7006-12a7-4935-b8c0-58fa7ea84b09".to_string();
    let file_id = "6d53a85f-505b-4a1a-ae6d-f7c18761d04a".to_string();
    let chunk_file_name = chunk_name(&file_id, &chunk_id);

    let temp = tempdir()?;
    let mut f = File::create(temp.path().join(&chunk_file_name)).await?;
    f.write_all(b"Test file content").await?;

    // setup chunk server mock
    let mut server =
        init_service(create_app("url".into(), Uuid::new_v4(), temp.path().into())).await;

    // setup other chunk server mock
    let chunk_server2 = MockServer::start();
    let upload_mock = chunk_server2.mock(|when, then| {
        when.method(Method::POST)
            .path("/api/upload")
            .body_contains(&chunk_id)
            .body_contains(&file_id)
            .body_contains("Test file content");
        then.status(204);
    });

    let req = TestRequest::post()
        .uri("/api/replicate")
        .header("x-ccfs-chunk-id", chunk_id)
        .header("x-ccfs-file-id", file_id)
        .header("x-ccfs-server-url", chunk_server2.base_url())
        .to_request();
    let resp = call_service(&mut server, req).await;
    upload_mock.assert();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    Ok(())
}

#[actix_rt::test]
async fn test_chunk2_failed() -> std::io::Result<()> {
    let chunk_id = "1a6e7006-12a7-4935-b8c0-58fa7ea84b09".to_string();
    let file_id = "6d53a85f-505b-4a1a-ae6d-f7c18761d04a".to_string();
    let chunk_file_name = chunk_name(&file_id, &chunk_id);

    let temp = tempdir()?;
    let mut f = File::create(temp.path().join(&chunk_file_name)).await?;
    f.write_all(b"Test file content").await?;

    // setup chunk server mock
    let mut server =
        init_service(create_app("url".into(), Uuid::new_v4(), temp.path().into())).await;

    // setup other chunk server mock
    let chunk_server2 = MockServer::start();
    let upload_mock = chunk_server2.mock(|when, then| {
        when.method(Method::POST)
            .path("/api/upload")
            .body_contains(&chunk_id)
            .body_contains(&file_id)
            .body_contains("Test file content");
        then.status(500);
    });

    let req = TestRequest::post()
        .uri("/api/replicate")
        .header("x-ccfs-chunk-id", chunk_id)
        .header("x-ccfs-file-id", file_id)
        .header("x-ccfs-server-url", chunk_server2.base_url())
        .to_request();
    let resp = call_service(&mut server, req).await;
    upload_mock.assert();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    Ok(())
}

#[actix_rt::test]
async fn test_missing_form_data() -> std::io::Result<()> {
    let chunk_id = "1a6e7006-12a7-4935-b8c0-58fa7ea84b09".to_string();
    let file_id = "6d53a85f-505b-4a1a-ae6d-f7c18761d04a".to_string();
    let chunk_file_name = chunk_name(&file_id, &chunk_id);

    let temp = tempdir()?;
    let mut f = File::create(temp.path().join(&chunk_file_name)).await?;
    f.write_all(b"Test file content").await?;

    // setup chunk server mock
    let mut server =
        init_service(create_app("url".into(), Uuid::new_v4(), temp.path().into())).await;

    // setup other chunk server mock
    let chunk_server2 = MockServer::start();
    let upload_mock = chunk_server2.mock(|when, then| {
        when.method(Method::POST);
        then.status(500);
    });

    let req = TestRequest::post()
        .uri("/api/replicate")
        .header("x-ccfs-chunk-id", chunk_id)
        .header("x-ccfs-server-url", chunk_server2.base_url())
        .to_request();
    let resp = call_service(&mut server, req).await;
    upload_mock.assert_hits(0);
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

#[actix_rt::test]
async fn test_missing_file() -> std::io::Result<()> {
    let chunk_id = "1a6e7006-12a7-4935-b8c0-58fa7ea84b09".to_string();
    let file_id = "6d53a85f-505b-4a1a-ae6d-f7c18761d04a".to_string();
    let temp = tempdir()?;

    // setup chunk server mock
    let mut server =
        init_service(create_app("url".into(), Uuid::new_v4(), temp.path().into())).await;

    // setup other chunk server mock
    let chunk_server2 = MockServer::start();
    let upload_mock = chunk_server2.mock(|when, then| {
        when.method(Method::POST);
        then.status(500);
    });

    let req = TestRequest::post()
        .uri("/api/replicate")
        .header("x-ccfs-chunk-id", chunk_id)
        .header("x-ccfs-file-id", file_id)
        .header("x-ccfs-server-url", chunk_server2.base_url())
        .to_request();
    let resp = call_service(&mut server, req).await;
    upload_mock.assert_hits(0);
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    Ok(())
}
