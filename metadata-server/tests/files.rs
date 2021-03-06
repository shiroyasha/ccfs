use actix_http::http::StatusCode;
use actix_web::{test, web, App};
use ccfs_commons::{FileInfo, FileMetadata};
use metadata_server::routes::api::{create_file, get_file};
use metadata_server::FilesMap;
use std::collections::HashMap;
use std::sync::Arc;
use test::{call_service, init_service, read_response_json, TestRequest};
use tokio::sync::RwLock;
use uuid::Uuid;

#[actix_rt::test]
async fn test_get_files() -> std::io::Result<()> {
    let metadata_tree = Arc::new(RwLock::new(FileMetadata::create_root()));
    let server = init_service(
        App::new()
            .data(metadata_tree)
            .service(web::scope("/api").service(get_file)),
    )
    .await;

    let req = TestRequest::get().uri("/api/files").to_request();
    let data: FileMetadata = read_response_json(&server, req).await;
    assert_eq!(data.name, "/");
    assert!(matches!(
        data.file_info,
        FileInfo::Directory {
            children
        } if children.is_empty()
    ));

    let req = TestRequest::get().uri("/api/files?path=").to_request();
    let data: FileMetadata = read_response_json(&server, req).await;
    assert_eq!(data.name, "/");
    assert!(matches!(
        data.file_info,
        FileInfo::Directory {
            children
        } if children.is_empty()
    ));
    Ok(())
}

#[actix_rt::test]
async fn test_get_non_existing_file() -> std::io::Result<()> {
    let metadata_tree = Arc::new(RwLock::new(FileMetadata::create_root()));
    let server = init_service(
        App::new()
            .data(metadata_tree)
            .service(web::scope("/api").service(get_file)),
    )
    .await;

    let req = TestRequest::get()
        .uri("/api/files?path=./test.txt")
        .to_request();
    let resp = call_service(&server, req).await;
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    Ok(())
}

#[actix_rt::test]
async fn test_get_file_invalid_path() -> std::io::Result<()> {
    let metadata_tree = Arc::new(RwLock::new(FileMetadata::create_root()));
    let server = init_service(
        App::new()
            .data(metadata_tree)
            .service(web::scope("/api").service(get_file)),
    )
    .await;

    let req = TestRequest::get()
        .uri("/api/files?path=./test.txt/...../some_dir")
        .to_request();
    let resp = call_service(&server, req).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

#[actix_rt::test]
async fn test_get_file_at_path() -> std::io::Result<()> {
    let mut tree = FileMetadata::create_root();
    tree.insert_dir("projects").unwrap();
    let projects = tree.traverse_mut("projects").unwrap();
    let chunk_id = Uuid::new_v4();
    projects
        .insert_file("test.txt", 10, vec![chunk_id])
        .unwrap();
    let metadata_tree = Arc::new(RwLock::new(tree));
    let server = init_service(
        App::new()
            .data(metadata_tree)
            .service(web::scope("/api").service(get_file)),
    )
    .await;

    let req = TestRequest::get()
        .uri("/api/files?path=/projects/test.txt")
        .to_request();
    let data: FileMetadata = read_response_json(&server, req).await;
    assert_eq!(data.name, "test.txt");
    assert!(matches!(
        data.file_info,
        FileInfo::File {
            size,chunks,..
        } if size == 10 && chunks == vec![chunk_id]
    ));
    Ok(())
}

#[actix_rt::test]
async fn test_upload_file() -> std::io::Result<()> {
    let files: FilesMap = Arc::new(RwLock::new(HashMap::new()));
    let metadata_tree = Arc::new(RwLock::new(FileMetadata::create_root()));
    let server = init_service(
        App::new()
            .data(files.clone())
            .data(metadata_tree)
            .service(web::scope("/api").service(create_file)),
    )
    .await;

    let chunk_id = Uuid::new_v4();
    let new_file = FileMetadata::create_file("test.txt".into(), 10, vec![chunk_id]);
    let file_id = match &new_file.file_info {
        FileInfo::File { id, .. } => id,
        _ => unreachable!(),
    };
    let req = TestRequest::post()
        .uri("/api/files/upload")
        .set_json(&new_file)
        .to_request();
    let data: FileMetadata = read_response_json(&server, req).await;
    assert_eq!(data, new_file);
    {
        let files_map = files.read().await;
        assert_eq!(files_map.len(), 1);
        assert_eq!(files_map.get(file_id), Some(&("".into(), new_file.clone())));
    }

    let req = TestRequest::post()
        .uri("/api/files/upload?path=")
        .set_json(&new_file)
        .to_request();
    let data: FileMetadata = read_response_json(&server, req).await;
    assert_eq!(data, new_file);
    {
        let files_map = files.read().await;
        assert_eq!(files_map.len(), 1);
        assert_eq!(files_map.get(file_id), Some(&("".into(), new_file.clone())));
    }
    Ok(())
}

#[actix_rt::test]
async fn test_upload_empty_dir() -> std::io::Result<()> {
    let files: FilesMap = Arc::new(RwLock::new(HashMap::new()));
    let metadata_tree = Arc::new(RwLock::new(FileMetadata::create_root()));
    let server = init_service(
        App::new()
            .data(files.clone())
            .data(metadata_tree.clone())
            .service(web::scope("/api").service(create_file)),
    )
    .await;

    let new_dir = FileMetadata::create_dir("test".into());
    let req = TestRequest::post()
        .uri("/api/files/upload")
        .set_json(&new_dir)
        .to_request();
    let data: FileMetadata = read_response_json(&server, req).await;
    assert_eq!(data, new_dir);
    let files_map = files.read().await;
    assert_eq!(files_map.len(), 0);
    let tree = metadata_tree.read().await;
    let dir = tree.traverse("test").unwrap();
    assert_eq!(dir, &new_dir);
    Ok(())
}

#[actix_rt::test]
async fn test_upload_to_non_existing_path() -> std::io::Result<()> {
    let files: FilesMap = Arc::new(RwLock::new(HashMap::new()));
    let metadata_tree = Arc::new(RwLock::new(FileMetadata::create_root()));
    let server = init_service(
        App::new()
            .data(files)
            .data(metadata_tree)
            .service(web::scope("/api").service(create_file)),
    )
    .await;

    let new_dir = FileMetadata::create_dir("test".into());
    let req = TestRequest::post()
        .uri("/api/files/upload?path=./some_dir")
        .set_json(&new_dir)
        .to_request();
    let resp = call_service(&server, req).await;
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    Ok(())
}

#[actix_rt::test]
async fn test_upload_invalid_path() -> std::io::Result<()> {
    let files: FilesMap = Arc::new(RwLock::new(HashMap::new()));
    let metadata_tree = Arc::new(RwLock::new(FileMetadata::create_root()));
    let server = init_service(
        App::new()
            .data(files)
            .data(metadata_tree)
            .service(web::scope("/api").service(create_file)),
    )
    .await;

    let new_dir = FileMetadata::create_dir("test".into());
    let req = TestRequest::post()
        .uri("/api/files/upload?path=./test.txt/...../some_dir")
        .set_json(&new_dir)
        .to_request();
    let resp = call_service(&server, req).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

#[actix_rt::test]
async fn test_upload_at_path() -> std::io::Result<()> {
    let files: FilesMap = Arc::new(RwLock::new(HashMap::new()));
    let mut tree = FileMetadata::create_root();
    tree.insert_dir("projects").unwrap();
    let metadata_tree = Arc::new(RwLock::new(tree));
    let server = init_service(
        App::new()
            .data(files)
            .data(metadata_tree)
            .service(web::scope("/api").service(create_file)),
    )
    .await;

    let new_dir = FileMetadata::create_dir("test".into());
    let req = TestRequest::post()
        .uri("/api/files/upload?path=/projects")
        .set_json(&new_dir)
        .to_request();
    let data: FileMetadata = read_response_json(&server, req).await;
    assert_eq!(data, new_dir);
    Ok(())
}
