use assert_cmd::prelude::*;
use ccfs_commons::ChunkServer;
use httpmock::{Method, MockServer};
use predicates::prelude::*;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::tempdir_in;
use tokio::fs::{create_dir, File};
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

async fn create_test_config_file(
    meta_url: &str,
    temp_dir: &Path,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let config_file_path = temp_dir.join("config.yml");
    let mut config_file = File::create(&config_file_path).await?;
    config_file
        .write_all(format!("metadata-server-url: {}", meta_url).as_bytes())
        .await?;
    Ok(config_file_path)
}

#[actix_rt::test]
async fn test_upload_not_existing_file() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("cli")?;
    cmd.arg("upload").arg("./not-existing-file.txt");
    cmd.assert().failure().stderr(predicate::str::contains(
        "Error: File doesn't exist: './not-existing-file.txt",
    ));
    Ok(())
}

#[actix_rt::test]
async fn test_upload_file_meta_servers_error() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("cli")?;
    let temp_dir = tempdir_in("./")?;
    let file_path = temp_dir.path().join("test.txt");
    let mut file = File::create(&file_path).await?;
    file.write_all(b"Test file content").await?;

    let meta_server = MockServer::start();
    meta_server.mock(|when, then| {
        when.method(Method::POST).path("/api/files/upload");
        then.status(500).body("Metaserver connection error");
    });
    let config_file_path =
        create_test_config_file(&meta_server.base_url(), temp_dir.path()).await?;

    cmd.arg("-c")
        .arg(&config_file_path)
        .arg("upload")
        .arg(&file_path);
    cmd.assert().failure().stderr(predicate::str::contains(
        "Error: Request failed: Metaserver connection error",
    ));
    Ok(())
}

#[actix_rt::test]
async fn test_upload_file_fetch_servers_error() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("cli")?;
    let temp_dir = tempdir_in("./")?;
    let file_path = temp_dir.path().join("test.txt");
    let mut file = File::create(&file_path).await?;
    file.write_all(b"Test file content").await?;

    let meta_server = MockServer::start();
    meta_server.mock(|when, then| {
        when.method(Method::POST).path("/api/files/upload");
        then.status(200)
            .header("content-type", "application/json")
            .body_from_file("tests/file_resp.json");
    });
    meta_server.mock(|when, then| {
        when.method(Method::GET).path("/api/servers");
        then.status(500).body("Couldn't fetch servers");
    });
    let config_file_path =
        create_test_config_file(&meta_server.base_url(), temp_dir.path()).await?;

    cmd.arg("-c")
        .arg(&config_file_path)
        .arg("upload")
        .arg(&file_path);
    cmd.assert().failure().stderr(predicate::str::contains(
        "Error: Request failed: Couldn't fetch servers",
    ));
    Ok(())
}

#[actix_rt::test]
async fn test_upload_file_no_available_servers() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("cli")?;
    let temp_dir = tempdir_in("./")?;
    let file_path = temp_dir.path().join("test.txt");
    let mut file = File::create(&file_path).await?;
    file.write_all(b"Test file content").await?;

    let meta_server = MockServer::start();
    meta_server.mock(|when, then| {
        when.method(Method::POST).path("/api/files/upload");
        then.status(200)
            .header("content-type", "application/json")
            .body_from_file("tests/file_resp.json");
    });
    let data: Vec<ChunkServer> = Vec::new();
    meta_server.mock(|when, then| {
        when.method(Method::GET).path("/api/servers");
        then.status(200)
            .header("Content-Type", "application/json")
            .json_body_obj(&data);
    });
    let config_file_path =
        create_test_config_file(&meta_server.base_url(), temp_dir.path()).await?;

    cmd.arg("-c")
        .arg(&config_file_path)
        .arg("upload")
        .arg(&file_path);
    cmd.assert().failure().stderr(predicate::str::contains(
        "Error: There are no available servers, try again later",
    ));
    Ok(())
}

#[actix_rt::test]
async fn test_upload_file() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("cli")?;
    let temp_dir = tempdir_in("./")?;
    let file_path = temp_dir.path().join("test.txt");
    let mut file = File::create(&file_path).await?;
    file.write_all(b"Test file content").await?;

    let chunk_server = MockServer::start();
    let meta_server = MockServer::start();
    meta_server.mock(|when, then| {
        when.method(Method::POST).path("/api/files/upload");
        then.status(200)
            .header("content-type", "application/json")
            .body_from_file("tests/file_resp.json");
    });
    let chunk_server_val = ChunkServer::new(Uuid::new_v4(), chunk_server.base_url());
    meta_server.mock(|when, then| {
        when.method(Method::GET).path("/api/servers");
        then.status(200)
            .header("Content-Type", "application/json")
            .json_body_obj(&vec![chunk_server_val]);
    });
    chunk_server.mock(|when, then| {
        when.method(Method::POST).path("/api/upload");
        then.status(200);
    });
    let config_file_path =
        create_test_config_file(&meta_server.base_url(), temp_dir.path()).await?;

    cmd.arg("-c")
        .arg(&config_file_path)
        .arg("upload")
        .arg(&file_path);
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Completed file upload"));
    Ok(())
}

#[actix_rt::test]
async fn test_upload_dir() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("cli")?;
    let temp_dir = tempdir_in("./")?;
    let dir_path = temp_dir.path().join("test");
    create_dir(&dir_path).await?;

    let meta_server = MockServer::start();
    meta_server.mock(|when, then| {
        when.method(Method::POST).path("/api/files/upload");
        then.status(200)
            .header("content-type", "application/json")
            .body_from_file("tests/dir_resp.json");
    });
    let config_file_path =
        create_test_config_file(&meta_server.base_url(), temp_dir.path()).await?;

    cmd.arg("-c")
        .arg(&config_file_path)
        .arg("upload")
        .arg(&dir_path);
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Completed directory upload"));
    Ok(())
}
