mod utils;

use assert_cmd::prelude::*;
use ccfs_commons::test_utils::build_tree;
use ccfs_commons::FileMetadata;
use httpmock::{Method, MockServer};
use predicates::prelude::*;
use std::process::Command;
use tempfile::tempdir_in;
use tokio::fs::read_to_string;
use utils::create_config_file;

#[actix_rt::test]
async fn test_print_tree_empty() -> Result<(), Box<dyn std::error::Error>> {
    let meta_server = MockServer::start();
    meta_server.mock(|when, then| {
        when.method(Method::GET).path("/api/files");
        then.status(200)
            .header("content-type", "application/json")
            .json_body_obj(&FileMetadata::create_root());
    });

    let temp_dir = tempdir_in("./")?;
    let config_file_path = create_config_file(&meta_server.base_url(), temp_dir.path()).await?;
    Command::cargo_bin("cli")?
        .arg("-c")
        .arg(&config_file_path)
        .arg("tree")
        .assert()
        .success()
        .stdout(predicate::str::contains("/"));
    Ok(())
}

#[actix_rt::test]
async fn test_print_tree_root() -> Result<(), Box<dyn std::error::Error>> {
    let tree = build_tree()?;
    let meta_server = MockServer::start();
    meta_server.mock(|when, then| {
        when.method(Method::GET).path("/api/files");
        then.status(200)
            .header("content-type", "application/json")
            .json_body_obj(&tree);
    });

    let temp_dir = tempdir_in("./")?;
    let config_file_path = create_config_file(&meta_server.base_url(), temp_dir.path()).await?;
    Command::cargo_bin("cli")?
        .arg("-c")
        .arg(&config_file_path)
        .arg("tree")
        .assert()
        .success()
        .stdout(predicate::str::contains(
            read_to_string("tests/expected-tree.txt").await?,
        ));
    Ok(())
}

#[actix_rt::test]
async fn test_print_tree_from_some_child() -> Result<(), Box<dyn std::error::Error>> {
    let tree = build_tree()?;
    let subtree = tree.traverse("dir2")?;
    let meta_server = MockServer::start();
    meta_server.mock(|when, then| {
        when.method(Method::GET).path("/api/files");
        then.status(200)
            .header("content-type", "application/json")
            .json_body_obj(subtree);
    });

    let temp_dir = tempdir_in("./")?;
    let config_file_path = create_config_file(&meta_server.base_url(), temp_dir.path()).await?;
    Command::cargo_bin("cli")?
        .arg("-c")
        .arg(&config_file_path)
        .arg("tree")
        .assert()
        .success()
        .stdout(predicate::str::contains(
            read_to_string("tests/expected-subtree.txt").await?,
        ));
    Ok(())
}
