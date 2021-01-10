use crate::FileMetadataTree;
use ccfs_commons::{errors::Error as BaseError, result::CCFSResult};
use futures::future::{FutureExt, LocalBoxFuture};
use std::path::PathBuf;
use tokio::fs::{create_dir_all, rename, File as FileFS};
use tokio::io::AsyncWriteExt;
use tokio::time::{delay_for, Duration};

pub async fn start_snapshot_job(
    upload_path: PathBuf,
    snapshot_path: PathBuf,
    metadata_tree: FileMetadataTree,
) {
    let temp_path = &upload_path.join("temp_snapshot");
    loop {
        delay_for(Duration::from_secs(10)).await;
        if let Err(err) = create_snapshot(
            upload_path.to_path_buf(),
            snapshot_path.to_path_buf(),
            temp_path.to_path_buf(),
            metadata_tree.clone(),
        )
        .await
        {
            // TODO: replace with logger
            println!("Error while creating snapshot: {:?}", err);
        } else {
            println!("Successfully created snapshot");
        }
    }
}

fn create_snapshot(
    upload_path: PathBuf,
    snapshot_path: PathBuf,
    temp_path: PathBuf,
    metadata_tree: FileMetadataTree,
) -> LocalBoxFuture<'static, CCFSResult<()>> {
    async move {
        if !upload_path.exists() {
            create_dir_all(&upload_path)
                .await
                .map_err(|source| BaseError::Create {
                    path: upload_path,
                    source,
                })?;
        }
        let mut temp_file =
            FileFS::create(&temp_path)
                .await
                .map_err(|source| BaseError::Create {
                    path: temp_path.clone(),
                    source,
                })?;
        temp_file
            .write_all(&bincode::serialize(&*metadata_tree).unwrap())
            .await
            .map_err(|source| BaseError::Write {
                path: temp_path.clone(),
                source,
            })?;
        rename(&temp_path, &snapshot_path)
            .await
            .map_err(|source| BaseError::Rename {
                from: temp_path,
                to: snapshot_path,
                source,
            })?;
        Ok(())
    }
    .boxed_local()
}
