mod utils;

use assert_cmd::prelude::*;
use ccfs_commons::{Chunk, ChunkServer, FileInfo, FileMetadata};
use httpmock::{Method, MockServer};
use predicates::prelude::*;
use std::path::Path;
use std::process::Command;
use tempfile::tempdir_in;
use tokio::fs::read_to_string;
use utils::{create_config_file, Cleanup};
use uuid::Uuid;

#[actix_rt::test]
async fn test_download_empty_dir() -> Result<(), Box<dyn std::error::Error>> {
    const TEST_DIR: &str = "test";
    let downloaded = Path::new(TEST_DIR);
    assert!(!downloaded.exists());
    let _cleanup = Cleanup::new(vec![downloaded.into()]);
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
    let _cleanup = Cleanup::new(vec![downloaded.into()]);
    let chunk_id = Uuid::new_v4();
    let server_id = Uuid::new_v4();
    let file_resp = FileMetadata::create_file(TEST_FILE.into(), 10, vec![chunk_id]);
    let file_id = match &file_resp.file_info {
        FileInfo::File { id, .. } => *id,
        _ => unreachable!(),
    };
    let chunk = Chunk::new(chunk_id, file_id, server_id);
    let temp_dir = tempdir_in("./")?;

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
            .query_param("path", TEST_FILE);
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
        .arg(TEST_FILE)
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
    const TEST_FILE: &str = "non-existing.txt";
    let downloaded = Path::new(TEST_FILE);
    assert!(!downloaded.exists());
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
        .arg(TEST_FILE)
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "Error: Request failed: File doesn't exist",
        ));
    assert!(!downloaded.exists());
    Ok(())
}

#[actix_rt::test]
async fn test_download_file_no_chunks_for_file() -> Result<(), Box<dyn std::error::Error>> {
    const TEST_FILE: &str = "test1.txt";
    let downloaded = Path::new(TEST_FILE);
    assert!(!downloaded.exists());
    let chunk_id = Uuid::new_v4();
    let file_resp = FileMetadata::create_file(TEST_FILE.into(), 10, vec![chunk_id]);
    let file_id = match &file_resp.file_info {
        FileInfo::File { id, .. } => *id,
        _ => unreachable!(),
    };
    let temp_dir = tempdir_in("./")?;

    let meta_server = MockServer::start();
    meta_server.mock(|when, then| {
        when.method(Method::GET)
            .path("/api/files")
            .query_param("path", TEST_FILE);
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
        .arg(TEST_FILE)
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "Error: Failed to download some chunks",
        ));
    Ok(())
}

#[actix_rt::test]
async fn test_download_file_chunk_download_failure() -> Result<(), Box<dyn std::error::Error>> {
    const TEST_FILE: &str = "test2.txt";
    let downloaded = Path::new(TEST_FILE);
    assert!(!downloaded.exists());
    let chunk_id = Uuid::new_v4();
    let server_id = Uuid::new_v4();
    let file_resp = FileMetadata::create_file(TEST_FILE.into(), 10, vec![chunk_id]);
    let file_id = match &file_resp.file_info {
        FileInfo::File { id, .. } => *id,
        _ => unreachable!(),
    };
    let chunk = Chunk::new(chunk_id, file_id, server_id);
    let temp_dir = tempdir_in("./")?;

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
            .query_param("path", TEST_FILE);
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
        .arg(TEST_FILE)
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "Error: Failed to download some chunks",
        ));
    Ok(())
}

#[actix_rt::test]
async fn test_download_file_get_server_failure() -> Result<(), Box<dyn std::error::Error>> {
    const TEST_FILE: &str = "test3.txt";
    let downloaded = Path::new(TEST_FILE);
    assert!(!downloaded.exists());
    let chunk_id = Uuid::new_v4();
    let server_id = Uuid::new_v4();
    let file_resp = FileMetadata::create_file(TEST_FILE.into(), 10, vec![chunk_id]);
    let file_id = match &file_resp.file_info {
        FileInfo::File { id, .. } => *id,
        _ => unreachable!(),
    };
    let chunk = Chunk::new(chunk_id, file_id, server_id);
    let temp_dir = tempdir_in("./")?;

    let meta_server = MockServer::start();
    meta_server.mock(|when, then| {
        when.method(Method::GET)
            .path("/api/files")
            .query_param("path", TEST_FILE);
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
        .arg(TEST_FILE)
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "Error: Failed to download some chunks",
        ));
    assert!(!downloaded.exists());
    Ok(())
}

#[actix_rt::test]
async fn test_download_dir_with_items() -> Result<(), Box<dyn std::error::Error>> {
    const TEST_DIR: &str = "test_with_items";
    const TEST_FILE: &str = "test4.txt";
    let downloaded = Path::new(TEST_DIR);
    let downloaded_file = downloaded.join(TEST_FILE);
    assert!(!downloaded.exists());
    let _cleanup = Cleanup::new(vec![downloaded.into()]);
    let mut dir_resp = FileMetadata::create_dir(TEST_DIR.into());

    let chunk_id = Uuid::new_v4();
    let server_id = Uuid::new_v4();
    let file_node = FileMetadata::create_file(TEST_FILE.into(), 10, vec![chunk_id]);
    let file_id = match &file_node.file_info {
        FileInfo::File { id, .. } => *id,
        _ => unreachable!(),
    };
    match dir_resp.file_info {
        FileInfo::Directory { ref mut children } => {
            children.insert(file_node.name.clone(), file_node);
        }
        _ => unreachable!(),
    }
    let chunk = Chunk::new(chunk_id, file_id, server_id);
    let temp_dir = tempdir_in("./")?;

    let chunk_server = MockServer::start();
    let chunk_server_val = ChunkServer::new(server_id, chunk_server.base_url());

    chunk_server.mock(|when, then| {
        when.method(Method::GET)
            .path(format!("/api/download/{}", chunk.chunk_name()));
        then.status(200).body("Test file content");
    });
    let meta_server = MockServer::start();
    meta_server.mock(|when, then| {
        when.method(Method::GET).path("/api/files");
        then.status(200)
            .header("content-type", "application/json")
            .json_body_obj(&dir_resp);
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
        .arg(TEST_DIR)
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Finished downloading `test_with_items`",
        ));

    assert!(downloaded.exists());
    assert!(downloaded.is_dir());
    assert!(downloaded_file.exists());
    assert!(downloaded_file.is_file());
    assert_eq!(read_to_string(downloaded_file).await?, "Test file content");
    Ok(())
}

#[actix_rt::test]
async fn test_download_file_with_multiple_parts() -> Result<(), Box<dyn std::error::Error>> {
    const TEST_FILE: &str = "test-multipart.txt";
    let downloaded = Path::new(TEST_FILE);
    assert!(!downloaded.exists());
    let _cleanup = Cleanup::new(vec![downloaded.into()]);
    let chunk1_id = Uuid::new_v4();
    let chunk2_id = Uuid::new_v4();
    let server1_id = Uuid::new_v4();
    let server2_id = Uuid::new_v4();
    let file_resp = FileMetadata::create_file(TEST_FILE.into(), 10, vec![chunk1_id, chunk2_id]);
    let file_id = match &file_resp.file_info {
        FileInfo::File { id, .. } => *id,
        _ => unreachable!(),
    };
    let chunk1 = Chunk::new(chunk1_id, file_id, server1_id);
    let chunk2 = Chunk::new(chunk2_id, file_id, server2_id);
    let temp_dir = tempdir_in("./")?;

    let chunk_server1 = MockServer::start();
    let chunk_server1_val = ChunkServer::new(server1_id, chunk_server1.base_url());
    let chunk_server2 = MockServer::start();
    let chunk_server2_val = ChunkServer::new(server2_id, chunk_server2.base_url());

    chunk_server1.mock(|when, then| {
        when.method(Method::GET)
            .path(format!("/api/download/{}", chunk1.chunk_name()));
        then.status(200).body("Test file content part1");
    });
    chunk_server2.mock(|when, then| {
        when.method(Method::GET)
            .path(format!("/api/download/{}", chunk2.chunk_name()));
        then.status(200).body("Test file content part2");
    });
    let meta_server = MockServer::start();
    meta_server.mock(|when, then| {
        when.method(Method::GET)
            .path("/api/files")
            .query_param("path", TEST_FILE);
        then.status(200)
            .header("content-type", "application/json")
            .json_body_obj(&file_resp);
    });
    meta_server.mock(|when, then| {
        when.method(Method::GET)
            .path(format!("/api/chunks/file/{}", file_id));
        then.status(200)
            .header("content-type", "application/json")
            .json_body_obj(&vec![vec![chunk1], vec![chunk2]]);
    });
    meta_server.mock(|when, then| {
        when.method(Method::GET)
            .path(format!("/api/servers/{}", server1_id));
        then.status(200)
            .header("Content-Type", "application/json")
            .json_body_obj(&chunk_server1_val);
    });
    meta_server.mock(|when, then| {
        when.method(Method::GET)
            .path(format!("/api/servers/{}", server2_id));
        then.status(200)
            .header("Content-Type", "application/json")
            .json_body_obj(&chunk_server2_val);
    });

    let config_file_path = create_config_file(&meta_server.base_url(), temp_dir.path()).await?;
    Command::cargo_bin("cli")?
        .arg("-c")
        .arg(&config_file_path)
        .arg("download")
        .arg(TEST_FILE)
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Finished downloading `test-multipart.txt`",
        ));

    assert!(downloaded.exists());
    assert!(downloaded.is_file());
    assert_eq!(
        read_to_string(downloaded).await?,
        "Test file content part1Test file content part2"
    );
    Ok(())
}

#[actix_rt::test]
async fn test_download_file_with_multiple_parts_one_chunk_failure(
) -> Result<(), Box<dyn std::error::Error>> {
    const TEST_FILE: &str = "test-multipart-failed.txt";
    let downloaded = Path::new(TEST_FILE);
    assert!(!downloaded.exists());
    let chunk1_id = Uuid::new_v4();
    let chunk2_id = Uuid::new_v4();
    let server1_id = Uuid::new_v4();
    let server2_id = Uuid::new_v4();
    let file_resp = FileMetadata::create_file(TEST_FILE.into(), 10, vec![chunk1_id, chunk2_id]);
    let file_id = match &file_resp.file_info {
        FileInfo::File { id, .. } => *id,
        _ => unreachable!(),
    };
    let chunk1 = Chunk::new(chunk1_id, file_id, server1_id);
    let chunk2 = Chunk::new(chunk2_id, file_id, server2_id);
    let temp_dir = tempdir_in("./")?;

    let chunk_server1 = MockServer::start();
    let chunk_server1_val = ChunkServer::new(server1_id, chunk_server1.base_url());
    let chunk_server2 = MockServer::start();

    chunk_server1.mock(|when, then| {
        when.method(Method::GET)
            .path(format!("/api/download/{}", chunk1.chunk_name()));
        then.status(200).body("Test file content part1");
    });
    chunk_server2.mock(|when, then| {
        when.method(Method::GET)
            .path(format!("/api/download/{}", chunk2.chunk_name()));
        then.status(200).body("Test file content part2");
    });
    let meta_server = MockServer::start();
    meta_server.mock(|when, then| {
        when.method(Method::GET)
            .path("/api/files")
            .query_param("path", TEST_FILE);
        then.status(200)
            .header("content-type", "application/json")
            .json_body_obj(&file_resp);
    });
    meta_server.mock(|when, then| {
        when.method(Method::GET)
            .path(format!("/api/chunks/file/{}", file_id));
        then.status(200)
            .header("content-type", "application/json")
            .json_body_obj(&vec![vec![chunk1], vec![chunk2]]);
    });
    meta_server.mock(|when, then| {
        when.method(Method::GET)
            .path(format!("/api/servers/{}", server1_id));
        then.status(200)
            .header("Content-Type", "application/json")
            .json_body_obj(&chunk_server1_val);
    });
    meta_server.mock(|when, then| {
        when.method(Method::GET)
            .path(format!("/api/servers/{}", server2_id));
        then.status(500);
    });

    let config_file_path = create_config_file(&meta_server.base_url(), temp_dir.path()).await?;
    Command::cargo_bin("cli")?
        .arg("-c")
        .arg(&config_file_path)
        .arg("download")
        .arg(TEST_FILE)
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "Error: Failed to download some chunks",
        ));
    assert!(!downloaded.exists());
    Ok(())
}
