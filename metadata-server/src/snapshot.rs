use crate::errors::*;
use crate::FileMetadataTree;
use futures::future::{FutureExt, LocalBoxFuture};
use snafu::ResultExt;
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
                .context(IOCreate { path: &upload_path })?;
        }
        let mut temp_file = FileFS::create(&temp_path)
            .await
            .context(IOCreate { path: &temp_path })?;
        temp_file
            .write_all(&bincode::serialize(&*metadata_tree).unwrap())
            .await
            .context(IOWrite { path: &temp_path })?;
        rename(&temp_path, &snapshot_path).await.context(Rename {
            from: temp_path,
            to: snapshot_path,
        })?;
        Ok(())
    }
    .boxed_local()
}
