mod utils;

use assert_cmd::prelude::*;
use ccfs_commons::{Chunk, ChunkServer, FileMetadata};
use httpmock::{Method, MockServer};
use predicates::prelude::*;
use std::path::Path;
use std::process::Command;
use tempfile::tempdir_in;
use tokio::fs::{read_to_string, File};
use utils::{create_config_file, Cleanup};
use uuid::Uuid;

#[actix_rt::test]
async fn test_download_dir() -> Result<(), Box<dyn std::error::Error>> {
    const TEST_DIR: &str = "test";
    let downloaded = Path::new(TEST_DIR);
    assert!(!downloaded.exists());
    let _cleanup = Cleanup::new(vec![downloaded.to_path_buf()]);
    let dir_resp = FileMetadata::create_dir(TEST_DIR.into());

    let meta_server = MockServer::start();
    meta_server.mock(|when, then| {
        when.method(Method::GET).path("/api/files");
        then.status(200)
            .header("content-type", "application/json")
            .json_body_obj(&dir_resp);
    });

    let temp_dir = tempdir_in("./")?;
    let config_file_path = create_config_file(&meta_server.base_url(), temp_dir.path()).await?;
    Command::cargo_bin("cli")?
        .arg("-c")
        .arg(&config_file_path)
        .arg("download")
        .arg("./test")
        .assert()
        .success()
        .stdout(predicate::str::contains("Finished downloading `test`"));

    assert!(downloaded.exists());
    assert!(downloaded.is_dir());
    Ok(())
}

#[actix_rt::test]
async fn test_download_file() -> Result<(), Box<dyn std::error::Error>> {
    const TEST_FILE: &str = "test.txt";
    let downloaded = Path::new(TEST_FILE);
    assert!(!downloaded.exists());
    let _cleanup = Cleanup::new(vec![downloaded.to_path_buf()]);
    let chunk_id = Uuid::new_v4();
    let server_id = Uuid::new_v4();
    let file_resp = FileMetadata::create_file(TEST_FILE.into(), 10, vec![chunk_id]);
    let file_id = match &file_resp.file_info {
        ccfs_commons::FileInfo::File { id, .. } => *id,
        _ => unreachable!(),
    };
    let chunk = Chunk::new(chunk_id, file_id, server_id);
    let temp_dir = tempdir_in("./")?;
    File::create(temp_dir.path().join(chunk.chunk_name())).await?;

    let chunk_server = MockServer::start();
    let chunk_server_val = ChunkServer::new(server_id, chunk_server.base_url());

    chunk_server.mock(|when, then| {
        when.method(Method::GET)
            .path(format!("/api/download/{}", chunk.chunk_name()));
        then.status(200).body("Test file content");
    });
    let meta_server = MockServer::start();
    meta_server.mock(|when, then| {
        when.method(Method::GET)
            .path("/api/files")
            .query_param("path", "./test.txt");
        then.status(200)
            .header("content-type", "application/json")
            .json_body_obj(&file_resp);
    });
    meta_server.mock(|when, then| {
        when.method(Method::GET)
            .path(format!("/api/chunks/file/{}", file_id));
        then.status(200)
            .header("content-type", "application/json")
            .json_body_obj(&vec![vec![chunk]]);
    });
    meta_server.mock(|when, then| {
        when.method(Method::GET)
            .path(format!("/api/servers/{}", server_id));
        then.status(200)
            .header("Content-Type", "application/json")
            .json_body_obj(&chunk_server_val);
    });

    let config_file_path = create_config_file(&meta_server.base_url(), temp_dir.path()).await?;
    Command::cargo_bin("cli")?
        .arg("-c")
        .arg(&config_file_path)
        .arg("download")
        .arg("./test.txt")
        .assert()
        .success()
        .stdout(predicate::str::contains("Finished downloading `test.txt`"));

    assert!(downloaded.exists());
    assert!(downloaded.is_file());
    assert_eq!(read_to_string(downloaded).await?, "Test file content");
    Ok(())
}

#[actix_rt::test]
async fn test_download_not_existing() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir_in("./")?;

    let meta_server = MockServer::start();
    meta_server.mock(|when, then| {
        when.method(Method::GET).path("/api/files");
        then.status(500).body("File doesn't exist");
    });

    let config_file_path = create_config_file(&meta_server.base_url(), temp_dir.path()).await?;
    Command::cargo_bin("cli")?
        .arg("-c")
        .arg(&config_file_path)
        .arg("download")
        .arg("./test.txt")
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "Error: Request failed: File doesn't exist",
        ));
    Ok(())
}

#[actix_rt::test]
async fn test_download_file_no_chunks_for_file() -> Result<(), Box<dyn std::error::Error>> {
    const TEST_FILE: &str = "test.txt";
    let downloaded = Path::new(TEST_FILE);
    assert!(!downloaded.exists());
    let _cleanup = Cleanup::new(vec![downloaded.to_path_buf()]);
    let chunk_id = Uuid::new_v4();
    let server_id = Uuid::new_v4();
    let file_resp = FileMetadata::create_file(TEST_FILE.into(), 10, vec![chunk_id]);
    let file_id = match &file_resp.file_info {
        ccfs_commons::FileInfo::File { id, .. } => *id,
        _ => unreachable!(),
    };
    let chunk = Chunk::new(chunk_id, file_id, server_id);
    let temp_dir = tempdir_in("./")?;
    File::create(temp_dir.path().join(chunk.chunk_name())).await?;

    let meta_server = MockServer::start();
    meta_server.mock(|when, then| {
        when.method(Method::GET)
            .path("/api/files")
            .query_param("path", "./test.txt");
        then.status(200)
            .header("content-type", "application/json")
            .json_body_obj(&file_resp);
    });
    let chunks_resp: Vec<Vec<Chunk>> = vec![vec![]];
    meta_server.mock(|when, then| {
        when.method(Method::GET)
            .path(format!("/api/chunks/file/{}", file_id));
        then.status(200)
            .header("content-type", "application/json")
            .json_body_obj(&chunks_resp);
    });

    let config_file_path = create_config_file(&meta_server.base_url(), temp_dir.path()).await?;
    Command::cargo_bin("cli")?
        .arg("-c")
        .arg(&config_file_path)
        .arg("download")
        .arg("./test.txt")
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "Error: Failed to download some chunks",
        ));
    Ok(())
}

#[actix_rt::test]
async fn test_download_file_chunk_download_failure() -> Result<(), Box<dyn std::error::Error>> {
    const TEST_FILE: &str = "test.txt";
    let downloaded = Path::new(TEST_FILE);
    assert!(!downloaded.exists());
    let _cleanup = Cleanup::new(vec![downloaded.to_path_buf()]);
    let chunk_id = Uuid::new_v4();
    let server_id = Uuid::new_v4();
    let file_resp = FileMetadata::create_file(TEST_FILE.into(), 10, vec![chunk_id]);
    let file_id = match &file_resp.file_info {
        ccfs_commons::FileInfo::File { id, .. } => *id,
        _ => unreachable!(),
    };
    let chunk = Chunk::new(chunk_id, file_id, server_id);
    let temp_dir = tempdir_in("./")?;
    File::create(temp_dir.path().join(chunk.chunk_name())).await?;

    let chunk_server = MockServer::start();
    let chunk_server_val = ChunkServer::new(server_id, chunk_server.base_url());

    chunk_server.mock(|when, then| {
        when.method(Method::GET)
            .path(format!("/api/download/{}", chunk.chunk_name()));
        then.status(500).body("Failed to download");
    });
    let meta_server = MockServer::start();
    meta_server.mock(|when, then| {
        when.method(Method::GET)
            .path("/api/files")
            .query_param("path", "./test.txt");
        then.status(200)
            .header("content-type", "application/json")
            .json_body_obj(&file_resp);
    });
    meta_server.mock(|when, then| {
        when.method(Method::GET)
            .path(format!("/api/chunks/file/{}", file_id));
        then.status(200)
            .header("content-type", "application/json")
            .json_body_obj(&vec![vec![chunk]]);
    });
    meta_server.mock(|when, then| {
        when.method(Method::GET)
            .path(format!("/api/servers/{}", server_id));
        then.status(200)
            .header("Content-Type", "application/json")
            .json_body_obj(&chunk_server_val);
    });

    let config_file_path = create_config_file(&meta_server.base_url(), temp_dir.path()).await?;
    Command::cargo_bin("cli")?
        .arg("-c")
        .arg(&config_file_path)
        .arg("download")
        .arg("./test.txt")
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "Error: Failed to download some chunks",
        ));
    Ok(())
}

#[actix_rt::test]
async fn test_download_file_get_server_failure() -> Result<(), Box<dyn std::error::Error>> {
    const TEST_FILE: &str = "test.txt";
    let downloaded = Path::new(TEST_FILE);
    assert!(!downloaded.exists());
    let _cleanup = Cleanup::new(vec![downloaded.to_path_buf()]);
    let chunk_id = Uuid::new_v4();
    let server_id = Uuid::new_v4();
    let file_resp = FileMetadata::create_file(TEST_FILE.into(), 10, vec![chunk_id]);
    let file_id = match &file_resp.file_info {
        ccfs_commons::FileInfo::File { id, .. } => *id,
        _ => unreachable!(),
    };
    let chunk = Chunk::new(chunk_id, file_id, server_id);
    let temp_dir = tempdir_in("./")?;
    File::create(temp_dir.path().join(chunk.chunk_name())).await?;

    let meta_server = MockServer::start();
    meta_server.mock(|when, then| {
        when.method(Method::GET)
            .path("/api/files")
            .query_param("path", "./test.txt");
        then.status(200)
            .header("content-type", "application/json")
            .json_body_obj(&file_resp);
    });
    meta_server.mock(|when, then| {
        when.method(Method::GET)
            .path(format!("/api/chunks/file/{}", file_id));
        then.status(200)
            .header("content-type", "application/json")
            .json_body_obj(&vec![vec![chunk]]);
    });
    meta_server.mock(|when, then| {
        when.method(Method::GET)
            .path(format!("/api/servers/{}", server_id));
        then.status(500);
    });

    let config_file_path = create_config_file(&meta_server.base_url(), temp_dir.path()).await?;
    Command::cargo_bin("cli")?
        .arg("-c")
        .arg(&config_file_path)
        .arg("download")
        .arg("./test.txt")
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "Error: Failed to download some chunks",
        ));
    Ok(())
}
