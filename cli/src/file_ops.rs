use crate::errors::*;
use ccfs_commons::http_utils::create_ccfs_multipart;
use ccfs_commons::{errors, result::CCFSResult};
use ccfs_commons::{Chunk, ChunkServer, FileInfo, FileMetadata, CHUNK_SIZE, CURR_DIR};
use futures::future::join_all;
use rand::{seq::SliceRandom, thread_rng};
use reqwest::{Body, Client, Response};
use serde::{de::DeserializeOwned, Serialize};
use snafu::ResultExt;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::Path;
use std::time::Instant;
use tempfile::tempdir_in;
use tokio::fs::{create_dir, remove_dir_all, rename, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio_stream::StreamExt;
use tokio_util::codec::{BytesCodec, FramedRead};
use uuid::Uuid;

pub async fn list(c: &Client, client_id: &Uuid, meta_url: &str) -> CCFSResult<()> {
    let file: FileMetadata =
        get_request_json(c, &format!("{}/api/files", meta_url), client_id).await?;
    println!("{}", file.print_current_dir()?);
    Ok(())
}

pub async fn tree(c: &Client, client_id: &Uuid, meta_url: &str) -> CCFSResult<()> {
    let file: FileMetadata =
        get_request_json(c, &format!("{}/api/files", meta_url), client_id).await?;
    println!("{}", file.print_subtree());
    Ok(())
}

pub async fn upload<T: AsRef<Path>>(
    c: &Client,
    client_id: &Uuid,
    meta_url: &str,
    file_path: T,
) -> CCFSResult<()> {
    let path = file_path.as_ref().to_path_buf();
    if !path.exists() {
        return Err(FileNotExist { path }.build().into());
    }
    let is_dir = path.is_dir();
    let path_prefix = path.parent().map(|p| p.to_path_buf()).unwrap_or_default();
    let mut paths = vec![path];
    while let Some(curr) = paths.pop() {
        upload_item(c, meta_url, curr.as_path(), &path_prefix, client_id).await?;
        if curr.is_dir() {
            paths.extend(
                curr.read_dir()
                    .context(errors::Open { path: curr })?
                    .filter_map(|item| item.ok())
                    .map(|item| item.path()),
            );
        }
    }
    match is_dir {
        true => println!("Completed directory upload"),
        false => println!("Completed file upload"),
    }
    Ok(())
}

pub async fn upload_item(
    c: &Client,
    meta_url: &str,
    path: &Path,
    prefix: &Path,
    client_id: &Uuid,
) -> CCFSResult<()> {
    let mut chunks = Vec::new();
    let file_meta = path.metadata().context(errors::Read { path })?;
    let file_name = path.file_name().unwrap().to_str().unwrap().to_string();
    let file_data = match file_meta.is_dir() {
        true => FileMetadata::create_dir(file_name),
        false => {
            chunks = generate_chunk_ids(file_meta.len());
            FileMetadata::create_file(file_name, file_meta.len(), chunks.clone())
        }
    };
    let relative_path = path.strip_prefix(prefix).unwrap();
    let target_dir = relative_path.parent().unwrap().display();
    let upload_url = format!("{}/api/files/upload?path={}", meta_url, target_dir);
    let resp = post_request(c, &upload_url, file_data, client_id).await?;
    let file: FileMetadata = resp.json().await.context(ParseJson)?;
    if let FileInfo::File { id, .. } = &file.file_info {
        upload_file(c, meta_url, id, chunks, path, client_id).await?;
    }
    return Ok(());
}

fn generate_chunk_ids(size: u64) -> Vec<Uuid> {
    (0..size / CHUNK_SIZE + 1).map(|_| Uuid::new_v4()).collect()
}

pub async fn upload_file(
    c: &Client,
    meta_url: &str,
    file_id: &Uuid,
    chunks: Vec<Uuid>,
    path: &Path,
    client_id: &Uuid,
) -> CCFSResult<()> {
    let servers: Vec<ChunkServer> =
        get_request_json(c, &format!("{}/api/servers", meta_url), client_id).await?;
    if servers.is_empty() {
        return Err(NoAvailableServers.build().into());
    }

    let requests = chunks
        .into_iter()
        .enumerate()
        .map(|(i, chunk)| upload_chunk(c, &servers, path, (file_id, chunk, i)));
    let responses = join_all(requests).await;
    if responses.iter().any(|resp| resp.is_err()) {
        return Err(UploadChunks.build().into());
    }
    Ok(())
}

pub async fn upload_chunk(
    c: &Client,
    servers: &[ChunkServer],
    path: &Path,
    data: (&Uuid, Uuid, usize),
) -> CCFSResult<()> {
    let (file_id, chunk_id, part) = data;
    let file_id_str = file_id.to_string();
    let chunk_id_str = chunk_id.to_string();
    let mut rng = thread_rng();
    for _ in 0..servers.len() {
        let server = servers.choose(&mut rng).expect("servers is empty");
        let mut f = File::open(path).await.context(errors::Open { path })?;
        f.seek(SeekFrom::Start(part as u64 * CHUNK_SIZE))
            .await
            .context(errors::Open { path })?;
        let stream = FramedRead::new(f.take(CHUNK_SIZE), BytesCodec::new());
        let mpart = create_ccfs_multipart(&chunk_id_str, &file_id_str, Body::wrap_stream(stream));
        let url = format!("{}/api/upload", server.address);
        let resp = c
            .post(&url)
            .multipart(mpart)
            .send()
            .await
            .context(errors::FailedRequest { url })?;
        if resp.status().is_success() {
            return Ok(());
        }
        println!(
            "failed to upload chunk {} to server {:?}\nreason: {:?}",
            chunk_id,
            server,
            resp.text().await
        );
    }
    Err(UploadSingleChunk { part, chunk_id }.build().into())
}

pub async fn download<T: AsRef<Path>>(
    c: &Client,
    client_id: &Uuid,
    meta_url: &str,
    path: T,
    target_path: Option<&Path>,
    force: bool,
) -> CCFSResult<()> {
    let instant = Instant::now();
    // get chunks and merge them into a file
    let file_url = format!("{}/api/files?path={}", meta_url, path.as_ref().display());
    let file: FileMetadata = get_request_json(c, &file_url, client_id).await?;
    let target_path = target_path
        .unwrap_or_else(|| Path::new(CURR_DIR))
        .to_path_buf();
    let tmp = tempdir_in(".").context(TempDir)?;
    let from = tmp.path().join(&file.name);
    let to = target_path.join(&file.name);
    if to.exists() {
        if !force {
            return Err(AlreadyExists { path: to.clone() }.build().into());
        } else if to.is_dir() {
            remove_dir_all(&to)
                .await
                .context(errors::Remove { path: &to })?;
        }
    }

    for (curr_f, parent_dir) in file.bfs_iter().zip(file.bfs_paths_iter()) {
        let curr_dir = tmp.path().join(parent_dir);
        if let FileInfo::Directory { .. } = &curr_f.file_info {
            let curr_path = curr_dir.join(&curr_f.name);
            create_dir(&curr_path)
                .await
                .context(errors::Create { path: curr_path })?;
        } else {
            download_file(c, meta_url, &curr_f, &curr_dir, client_id).await?;
        }
    }

    rename(&from, &to)
        .await
        .context(errors::Rename { from, to })?;
    println!(
        "Finished downloading `{}` in {} ms",
        file.name,
        instant.elapsed().as_millis()
    );
    Ok(())
}

pub async fn download_file(
    c: &Client,
    meta_url: &str,
    file: &FileMetadata,
    target_dir: &Path,
    client_id: &Uuid,
) -> CCFSResult<()> {
    if let FileInfo::File { id, chunks, .. } = &file.file_info {
        let chunks_url = format!("{}/api/chunks/file/{}", meta_url, id);
        let target_path = target_dir.join(&file.name);
        let path = target_path.as_path();
        let chunk_groups: Vec<Vec<Chunk>> = get_request_json(c, &chunks_url, client_id).await?;
        println!("download file path: {}", path.display());
        let groups: Vec<Vec<Chunk>> = chunk_groups
            .iter()
            .filter(|chunks| !chunks.is_empty())
            .cloned()
            .collect();
        if groups.len() < chunks.len() {
            return Err(SomeChunksNotAvailable.build().into());
        }
        let mut file = File::create(path).await.context(errors::Create { path })?;
        let requests = groups
            .iter()
            .map(|group| download_chunk(c, group, meta_url, client_id));
        let mut responses: HashMap<Uuid, Response> = join_all(requests)
            .await
            .into_iter()
            .filter_map(|resp| resp.ok())
            .filter(|(_, resp)| resp.status().is_success())
            .collect();
        if responses.len() < chunks.len() {
            return Err(SomeChunksNotAvailable.build().into());
        }
        for curr_chunk_id in chunks {
            if let Some(payload) = responses.remove(curr_chunk_id) {
                let mut stream = payload.bytes_stream();
                while let Some(Ok(mut bytes)) = stream.next().await {
                    file.write_buf(&mut bytes)
                        .await
                        .context(errors::Write { path })?;
                }
            }
        }
    }
    Ok(())
}

pub async fn download_chunk(
    c: &Client,
    chunks: &[Chunk],
    meta_url: &str,
    client_id: &Uuid,
) -> CCFSResult<(Uuid, Response)> {
    let chunk_name = chunks[0].chunk_name();
    for chunk in chunks {
        let chunk_servers_url = format!("{}/api/servers/{}", meta_url, &chunk.server_id);
        let server: ChunkServer = get_request_json(c, &chunk_servers_url, client_id).await?;
        let download_url = format!("{}/api/download/{}", server.address, chunk.chunk_name());
        let download_resp = get_request(c, &download_url, client_id).await?;
        if download_resp.status().is_success() {
            return Ok((chunk.id, download_resp));
        }
    }
    Err(ChunkNotAvailable { chunk_name }.build().into())
}

async fn get_request(c: &Client, url: &str, client_id: &Uuid) -> CCFSResult<Response> {
    let resp = c
        .get(url)
        .header("x-ccfs-client-id", client_id.to_string())
        .send()
        .await
        .context(errors::FailedRequest { url })?;
    match resp.status().is_success() {
        true => Ok(resp),
        false => {
            let response = resp.text().await.context(errors::ReadString)?;
            Err(errors::Unsuccessful { url, response }.build().into())
        }
    }
}

async fn get_request_json<T: DeserializeOwned>(
    c: &Client,
    url: &str,
    client_id: &Uuid,
) -> CCFSResult<T> {
    let resp = get_request(c, url, client_id).await?;
    Ok(resp.json().await.context(ParseJson)?)
}

async fn post_request<T: Serialize>(
    c: &Client,
    url: &str,
    data: T,
    client_id: &Uuid,
) -> CCFSResult<Response> {
    let resp = c
        .post(url)
        .header("x-ccfs-client-id", client_id.to_string())
        .json(&data)
        .send()
        .await
        .context(errors::FailedRequest { url })?;
    match resp.status().is_success() {
        true => Ok(resp),
        false => {
            let response = resp.text().await.context(errors::ReadString)?;
            Err(errors::Unsuccessful { url, response }.build().into())
        }
    }
}
