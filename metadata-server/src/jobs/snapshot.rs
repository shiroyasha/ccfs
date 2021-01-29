use crate::server_config::ServerConfig;
use crate::FileMetadataTree;
use ccfs_commons::{errors::Error as BaseError, result::CCFSResult};
use futures::future::{FutureExt, LocalBoxFuture};
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::tempdir_in;
use tokio::fs::{rename, write};
use tokio::time::{sleep, Duration};

pub async fn start_snapshot_job(config: Arc<ServerConfig>, metadata_tree: FileMetadataTree) {
    let temp_dir = tempdir_in(&config.snapshot_dir_path).expect("Couldn't create temp dir");
    let temp_path = Arc::new(temp_dir.path().join("tmp_snapshot"));
    let snapshot_path = Arc::new(config.snapshot_path());
    loop {
        sleep(Duration::from_secs(config.snapshot_interval)).await;
        match create_snapshot(
            snapshot_path.clone(),
            temp_path.clone(),
            metadata_tree.clone(),
        )
        .await
        {
            Ok(_) => println!("Successfully created snapshot"),
            Err(err) => println!("Error while creating snapshot: {:?}", err), // TODO: replace with logger
        }
    }
}

fn create_snapshot(
    snapshot_path: Arc<PathBuf>,
    temp_path: Arc<PathBuf>,
    tree: FileMetadataTree,
) -> LocalBoxFuture<'static, CCFSResult<()>> {
    async move {
        write(
            &*temp_path,
            &bincode::serialize(&*tree.read().await).unwrap(),
        )
        .await
        .map_err(|source| BaseError::Write {
            path: temp_path.to_path_buf(),
            source,
        })?;
        rename(&*temp_path, &*snapshot_path)
            .await
            .map_err(|source| BaseError::Rename {
                from: temp_path.to_path_buf(),
                to: snapshot_path.to_path_buf(),
                source,
            })?;
        Ok(())
    }
    .boxed_local()
}
