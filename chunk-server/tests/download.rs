mod utils;

use actix_web::{test, web};
use ccfs_commons::chunk_name;
use chunk_server::create_app;
use futures_util::stream::TryStreamExt;
use tempfile::tempdir;
use test::{call_service, init_service, TestRequest};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

#[actix_rt::test]
async fn test_download() -> std::io::Result<()> {
    let chunk_id = "1a6e7006-12a7-4935-b8c0-58fa7ea84b09".to_string();
    let file_id = "6d53a85f-505b-4a1a-ae6d-f7c18761d04a".to_string();
    let chunk_file_name = chunk_name(&file_id, &chunk_id);

    let temp = tempdir()?;
    let mut f = File::create(temp.path().join(&chunk_file_name)).await?;
    f.write_all(b"Test file content").await?;

    // setup chunk server mock
    let mut server =
        init_service(create_app("url".into(), Uuid::new_v4(), temp.path().into())).await;

    let req = TestRequest::get()
        .uri(&format!("/api/download/{}", chunk_file_name))
        .to_request();
    let mut resp = call_service(&mut server, req).await;
    assert!(resp.status().is_success());

    let bytes = test::load_stream(resp.take_body().into_stream()).await;
    assert_eq!(
        bytes.unwrap(),
        web::Bytes::from_static(b"Test file content")
    );
    Ok(())
}
