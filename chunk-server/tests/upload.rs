mod utils;

use actix_http::http::StatusCode;
use actix_web::test::{call_service, init_service};
use actix_web::web::Data;
use actix_web::{web, App};
use ccfs_commons::chunk_name;
use chunk_server::routes::upload;
use httpmock::{Method, MockServer};
use std::sync::Arc;
use tempfile::tempdir;
use utils::{create_multipart_request, is_empty, test_config};

#[actix_rt::test]
async fn test_successful_upload() -> std::io::Result<()> {
    let chunk_id = "1a6e7006-12a7-4935-b8c0-58fa7ea84b09".to_string();
    let file_id = "6d53a85f-505b-4a1a-ae6d-f7c18761d04a".to_string();
    let chunk_file_name = chunk_name(&file_id, &chunk_id);

    // setup metadata server mock
    let meta = MockServer::start();
    let upload_mock = meta.mock(|when, then| {
        when.method(Method::POST).path("/api/chunk/completed");
        then.status(204);
    });

    let temp = tempdir()?;
    assert!(is_empty(temp.path()).await?);

    let server_config = Arc::new(test_config(meta.base_url(), temp.path()));
    // setup chunk server mock
    let server = init_service(
        App::new()
            .app_data(Data::new(server_config.metadata_url.clone()))
            .app_data(Data::new(server_config.server_id))
            .app_data(Data::new(server_config.upload_path.clone()))
            .service(web::scope("/api").service(upload)),
    )
    .await;

    let req = create_multipart_request("/api/upload", chunk_id.into(), file_id.into()).await;
    let resp = call_service(&server, req).await;
    upload_mock.assert();
    assert_eq!(resp.status(), StatusCode::OK);

    assert!(temp.path().join(chunk_file_name).exists());
    Ok(())
}

#[actix_rt::test]
async fn test_meta_fail() -> std::io::Result<()> {
    let chunk_id = "1a6e7006-12a7-4935-b8c0-58fa7ea84b09".to_string();
    let file_id = "6d53a85f-505b-4a1a-ae6d-f7c18761d04a".to_string();
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

    let server_config = Arc::new(test_config(meta.base_url(), temp.path()));
    // setup chunk server mock
    let server = init_service(
        App::new()
            .app_data(Data::new(server_config.metadata_url.clone()))
            .app_data(Data::new(server_config.server_id))
            .app_data(Data::new(server_config.upload_path.clone()))
            .service(web::scope("/api").service(upload)),
    )
    .await;

    let req = create_multipart_request("/api/upload", chunk_id.into(), file_id.into()).await;
    let resp = call_service(&server, req).await;
    upload_mock.assert();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);

    assert!(temp.path().join(chunk_file_name).exists());
    Ok(())
}

#[actix_rt::test]
async fn test_missing_form_data() -> std::io::Result<()> {
    let chunk_id = "1a6e7006-12a7-4935-b8c0-58fa7ea84b09".to_string();

    // setup metadata server mock
    let meta = MockServer::start();
    let upload_mock = meta.mock(|when, then| {
        when.method(Method::POST).path("/api/chunk/completed");
        then.status(200);
    });

    let temp = tempdir()?;
    assert!(is_empty(temp.path()).await?);

    let server_config = Arc::new(test_config(meta.base_url(), temp.path()));
    // setup chunk server mock
    let server = init_service(
        App::new()
            .app_data(Data::new(server_config.metadata_url.clone()))
            .app_data(Data::new(server_config.server_id))
            .app_data(Data::new(server_config.upload_path.clone()))
            .service(web::scope("/api").service(upload)),
    )
    .await;

    let req = create_multipart_request("/api/upload", chunk_id.into(), None).await;
    let resp = call_service(&server, req).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    upload_mock.assert_hits(0);

    assert!(is_empty(temp.path()).await?);
    Ok(())
}
