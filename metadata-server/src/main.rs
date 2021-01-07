mod errors;
mod routes;

use actix_web::web::{get, post, resource, scope, Data};
use actix_web::{App, HttpServer};
use ccfs_commons::{Chunk, ChunkServer, File, FileMetadata};
use errors::Result;
use futures::future::{BoxFuture, FutureExt};
use routes::{
    chunk_server_ping, create_file, get_chunks, get_file, get_server, get_servers,
    signal_chuck_upload_completed,
};
use snafu::ResultExt;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tokio::fs::{create_dir_all, rename, File as FileFS};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task;
use tokio::time::{delay_for, Duration};
use uuid::Uuid;

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

async fn init_metadata_tree(path: &Path) -> Result<FileMetadataTree> {
    let tree = match path.exists() {
        true => {
            let mut file = FileFS::open(path).await.context(errors::IORead { path })?;
            let mut buf = Vec::new();
            file.read_to_end(&mut buf).await.context(errors::Read)?;
            bincode::deserialize(&buf).context(errors::Deserialize)?
        }
        false => FileMetadata::create_root(),
    };
    Ok(Arc::new(RwLock::new(tree)))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let host = "127.0.0.1";
    let port = "8080";
    let addr = format!("{}:{}", host, port);

    let upload_path = dirs::home_dir()
        .expect("Couldn't determine home dir")
        .join("ccfs-snapshots");
    let snapshot_path = upload_path.join("snapshot");

    let chunk_servers: ChunkServersMap = Arc::new(RwLock::new(HashMap::new()));
    let chunks: ChunksMap = Arc::new(RwLock::new(HashMap::new()));
    let files: FilesMap = Arc::new(RwLock::new(HashMap::new()));
    let file_metadata_tree: FileMetadataTree = init_metadata_tree(&snapshot_path)
        .await
        .unwrap_or_else(|err| panic!("Couldn't init metadata tree: {:?}", err));

    task::spawn(start_snapshot_job(
        upload_path,
        snapshot_path,
        file_metadata_tree.clone(),
    ));
    HttpServer::new(move || {
        App::new()
            .app_data(Data::new(chunk_servers.clone()))
            .app_data(Data::new(chunks.clone()))
            .app_data(Data::new(files.clone()))
            .app_data(Data::new(file_metadata_tree.clone()))
            .service(
                scope("/api")
                    .service(resource("/servers").route(get().to(get_servers)))
                    .service(resource("/servers/{id}").route(get().to(get_server)))
                    .service(resource("/ping").route(post().to(chunk_server_ping)))
                    .service(resource("/files/upload").route(post().to(create_file)))
                    .service(
                        resource("/chunk/completed")
                            .route(post().to(signal_chuck_upload_completed)),
                    )
                    .service(resource("/files").route(get().to(get_file)))
                    .service(resource("/chunks/file/{file_id}").route(get().to(get_chunks))),
            )
    })
    .bind(&addr)?
    .run()
    .await
}
