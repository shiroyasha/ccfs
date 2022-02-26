mod utils;

use actix_web::web::Data;
use actix_web::{test, web, App};
use ccfs_commons::chunk_name;
use chunk_server::routes::download;
use std::sync::Arc;
use tempfile::tempdir;
use test::{call_service, init_service, TestRequest};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use utils::test_config;

#[actix_rt::test]
async fn test_download() -> std::io::Result<()> {
    let chunk_id = "1a6e7006-12a7-4935-b8c0-58fa7ea84b09".to_string();
    let file_id = "6d53a85f-505b-4a1a-ae6d-f7c18761d04a".to_string();
    let chunk_file_name = chunk_name(&file_id, &chunk_id);

    let temp = tempdir()?;
    let mut f = File::create(temp.path().join(&chunk_file_name)).await?;
    f.write_all(b"Test file content").await?;

    let server_config = Arc::new(test_config("url".into(), temp.path()));
    // setup chunk server mock
    let server = init_service(
        App::new()
            .app_data(Data::new(server_config.upload_path.clone()))
            .service(web::scope("/api").service(download)),
    )
    .await;

    let req = TestRequest::get()
        .uri(&format!("/api/download/{}", chunk_file_name))
        .to_request();
    let resp = call_service(&server, req).await;
    assert!(resp.status().is_success());

    let bytes = test::load_body(resp.into_body()).await;
    assert_eq!(
        bytes.unwrap(),
        web::Bytes::from_static(b"Test file content")
    );
    Ok(())
}
