mod utils;

use actix_http::http::StatusCode;
use actix_web::test::{call_service, init_service};
use ccfs_commons::chunk_name;
use chunk_server::create_app;
use httpmock::{Method, MockServer};
use std::str::FromStr;
use tempfile::tempdir;
use utils::{create_multipart_request, is_empty};
use uuid::Uuid;

#[actix_rt::test]
async fn test_successful_upload() -> std::io::Result<()> {
    let chunk_id = "1a6e7006-12a7-4935-b8c0-58fa7ea84b09".to_string();
    let file_id = "6d53a85f-505b-4a1a-ae6d-f7c18761d04a".to_string();
    let server_id = Uuid::from_str("6d53a85f-505b-4a1a-ae6d-f7c18761d04b").unwrap();
    let chunk_file_name = chunk_name(&file_id, &chunk_id);

    // setup metadata server mock
    let meta = MockServer::start();
    let upload_mock = meta.mock(|when, then| {
        when.method(Method::POST).path("/api/chunk/completed");
        then.status(204);
    });

    let temp = tempdir()?;
    assert!(is_empty(temp.path()).await?);

    // setup chunk server mock
    let mut server = init_service(create_app(meta.base_url(), server_id, temp.path().into())).await;

    let req = create_multipart_request("/api/upload", chunk_id.into(), file_id.into()).await;
    let resp = call_service(&mut server, req).await;
    upload_mock.assert();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    assert!(temp.path().join(chunk_file_name).exists());
    Ok(())
}

#[actix_rt::test]
async fn test_meta_fail() -> std::io::Result<()> {
    let chunk_id = "1a6e7006-12a7-4935-b8c0-58fa7ea84b09".to_string();
    let file_id = "6d53a85f-505b-4a1a-ae6d-f7c18761d04a".to_string();
    let server_id = Uuid::from_str("6d53a85f-505b-4a1a-ae6d-f7c18761d04b").unwrap();
    let chunk_file_name = chunk_name(&file_id, &chunk_id);

    // setup metadata server mock
    let meta = MockServer::start();
    let upload_mock = meta.mock(|when, then| {
        when.method(Method::POST).path("/api/chunk/completed");
        then.status(500)
            .header("Content-Type", "text/html")
            .body("Metadata communication error");
    });

    let temp = tempdir()?;
    assert!(is_empty(temp.path()).await?);

    // setup chunk server mock
    let mut server = init_service(create_app(meta.base_url(), server_id, temp.path().into())).await;

    let req = create_multipart_request("/api/upload", chunk_id.into(), file_id.into()).await;
    let resp = call_service(&mut server, req).await;
    upload_mock.assert();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);

    assert!(temp.path().join(chunk_file_name).exists());
    Ok(())
}

#[actix_rt::test]
async fn test_missing_form_data() -> std::io::Result<()> {
    let chunk_id = "1a6e7006-12a7-4935-b8c0-58fa7ea84b09".to_string();
    let server_id = Uuid::from_str("6d53a85f-505b-4a1a-ae6d-f7c18761d04b").unwrap();

    // setup metadata server mock
    let meta = MockServer::start();
    let upload_mock = meta.mock(|when, then| {
        when.method(Method::POST).path("/api/chunk/completed");
        then.status(200);
    });

    let temp = tempdir()?;
    assert!(is_empty(temp.path()).await?);

    // setup chunk server mock
    let mut server = init_service(create_app(meta.base_url(), server_id, temp.path().into())).await;

    let req = create_multipart_request("/api/upload", chunk_id.into(), None).await;
    let resp = call_service(&mut server, req).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    upload_mock.assert_hits(0);

    assert_eq!(is_empty(temp.path()).await?, true);
    Ok(())
}
