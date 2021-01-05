#[macro_use]
extern crate rocket;
#[macro_use]
extern crate rocket_contrib;

mod errors;
mod routes;

use ccfs_commons::{Chunk, ChunkServer, File, FileMetadata};
use errors::Result;
use futures::future::{BoxFuture, FutureExt};
use rocket_contrib::uuid::uuid_crate::Uuid;
use routes::{
    chunk_server_ping, create_file, get_chunks, get_file, get_server, get_servers,
    signal_chuck_upload_completed,
};
use snafu::ResultExt;
use std::collections::HashMap;
use std::fs::File as FileSync;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tokio::fs::{create_dir_all, rename, File as FileFS};
use tokio::io::AsyncWriteExt;
use tokio::task;
use tokio::time::{delay_for, Duration};

pub type ChunkServersMap = Arc<RwLock<HashMap<Uuid, ChunkServer>>>;
pub type ChunksMap = Arc<RwLock<HashMap<Uuid, Chunk>>>;
pub type FilesMap = Arc<RwLock<HashMap<Uuid, File>>>;
pub type FileMetadataTree = Arc<RwLock<FileMetadata>>;

async fn start_snapshot_job(
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
) -> BoxFuture<'static, Result<()>> {
    async move {
        if !upload_path.exists() {
            create_dir_all(&upload_path)
                .await
                .context(errors::IOCreate { path: &upload_path })?;
        }
        let mut temp_file = FileFS::create(&temp_path)
            .await
            .context(errors::IOCreate { path: &temp_path })?;
        temp_file
            .write_all(&bincode::serialize(&*metadata_tree).unwrap())
            .await
            .context(errors::IOWrite { path: &temp_path })?;
        rename(&temp_path, &snapshot_path)
            .await
            .context(errors::Rename {
                from: temp_path,
                to: snapshot_path,
            })?;
        Ok(())
    }
    .boxed()
}

fn init_metadata_tree(snapshot_path: &Path) -> Result<FileMetadataTree> {
    let tree: FileMetadataTree;
    if snapshot_path.exists() {
        let mut file = FileSync::open(snapshot_path).context(errors::IORead {
            path: snapshot_path,
        })?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).context(errors::ReadContent)?;
        tree = Arc::new(RwLock::new(
            bincode::deserialize(&buf).context(errors::Deserialize)?,
        ));
    } else {
        tree = Arc::new(RwLock::new(FileMetadata::create_root()));
    }
    Ok(tree)
}

#[launch]
fn rocket() -> rocket::Rocket {
    let upload_path = dirs::home_dir()
        .expect("Couldn't determine home dir")
        .join("ccfs-snapshots");
    let snapshot_path = upload_path.join("snapshot");

    let chunk_servers: ChunkServersMap = Arc::new(RwLock::new(HashMap::new()));
    let chunks: ChunksMap = Arc::new(RwLock::new(HashMap::new()));
    let files: FilesMap = Arc::new(RwLock::new(HashMap::new()));
    let file_metadata_tree: FileMetadataTree = init_metadata_tree(&snapshot_path)
        .unwrap_or_else(|err| panic!("Couldn't init metadata tree: {:?}", err));
    let inst = rocket::ignite()
        .mount(
            "/api",
            routes![
                get_servers,
                get_server,
                chunk_server_ping,
                create_file,
                signal_chuck_upload_completed,
                get_file,
                get_chunks
            ],
        )
        .manage(chunk_servers)
        .manage(chunks)
        .manage(files)
        .manage(file_metadata_tree.clone());

    task::spawn(start_snapshot_job(
        upload_path,
        snapshot_path,
        file_metadata_tree,
    ));
    inst
}
