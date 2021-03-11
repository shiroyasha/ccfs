use crate::server_config::ServerConfig;
use crate::FileMetadataTree;
use ccfs_commons::{errors, result::CCFSResult};
use futures::future::{FutureExt, LocalBoxFuture};
use snafu::ResultExt;
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir_in;
use tokio::fs::{rename, write};
use tokio::time::{sleep, Duration};

pub async fn start_snapshot_job(config: Arc<ServerConfig>, metadata_tree: FileMetadataTree) {
    let temp_dir = tempdir_in(&config.snapshot_dir_path).expect("Couldn't create temp dir");
    let temp_path = temp_dir.path().join("tmp_snapshot");
    let snapshot_path = config.snapshot_path();
    loop {
        sleep(Duration::from_secs(config.snapshot_interval)).await;
        match create_snapshot(&snapshot_path, &temp_path, metadata_tree.clone()).await {
            Ok(_) => println!("Successfully created snapshot"),
            Err(err) => println!("Error while creating snapshot: {:?}", err), // TODO: replace with logger
        }
    }
}

fn create_snapshot(
    snapshot_path: &Path,
    temp_path: &Path,
    tree: FileMetadataTree,
) -> LocalBoxFuture<'static, CCFSResult<()>> {
    let temp_path = temp_path.to_path_buf();
    let snapshot_path = snapshot_path.to_path_buf();
    async move {
        write(
            &temp_path,
            &bincode::serialize(&*tree.read().await).unwrap(),
        )
        .await
        .context(errors::Write { path: &temp_path })?;
        rename(&temp_path, &snapshot_path)
            .await
            .context(errors::Rename {
                from: temp_path,
                to: snapshot_path,
            })?;
        Ok(())
    }
    .boxed_local()
}
