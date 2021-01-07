use super::Result;
use crate::errors::{self, FileAction};
use actix_web::body::BodyStream;
use actix_web::client::{Client, ClientResponse};
use actix_web::dev::{Decompress, Payload};
use actix_web::http::header::CONTENT_TYPE;
use ccfs_commons::{Chunk, ChunkServer, File, FileInfo, FileMetadata, CHUNK_SIZE};
use futures::future::join_all;
use mpart_async::client::MultipartRequest;
use rand::{seq::SliceRandom, thread_rng};
use serde::{de::DeserializeOwned, Serialize};
use slice_group_by::GroupBy;
use snafu::ResultExt;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::Path;
use tokio::fs::{create_dir, File as FileFS};
use tokio::io::{reader_stream, AsyncReadExt, AsyncWriteExt};
use tokio::stream::StreamExt;
use uuid::Uuid;

type Response = ClientResponse<Decompress<Payload>>;

pub async fn list(c: &Client, meta_url: &str) -> Result<()> {
    let file_url = format!("{}/api/files", meta_url);
    let file: FileMetadata = get_request_json(c, &file_url).await?;
    println!("{}", file.print_current_dir());
    Ok(())
}

pub async fn tree(c: &Client, meta_url: &str) -> Result<()> {
    let file_url = format!("{}/api/files", meta_url);
    let file: FileMetadata = get_request_json(c, &file_url).await?;
    println!("{}", file.print_subtree());
    Ok(())
}

pub async fn upload<T: AsRef<Path>>(c: &Client, meta_url: &str, file_path: T) -> Result<()> {
    let path = file_path.as_ref().to_path_buf();
    if path.exists() {
        let path_prefix = path.ancestors().nth(1).unwrap().to_path_buf();
        let mut paths = vec![path];
        while !paths.is_empty() {
            let curr = paths.pop().unwrap();
            upload_item(c, meta_url, curr.as_path(), &path_prefix).await?;
            if curr.is_dir() {
                paths.extend(
                    curr.read_dir()
                        .context(errors::FileIO {
                            path: curr.clone(),
                            action: FileAction::Open,
                        })?
                        .filter_map(|item| item.ok())
                        .map(|item| item.path()),
                );
            }
        }
        Ok(())
    } else {
        Err(errors::FileNotExist { path }.build())
    }
}

pub async fn upload_item(
    c: &Client,
    meta_url: &str,
    path: &Path,
    path_prefix: &Path,
) -> Result<()> {
    let mut chunks = Vec::new();
    let file_meta = path.metadata().context(errors::ReadMetadata { path })?;
    let file_name = path.file_name().unwrap().to_str().unwrap().to_string();
    let file_data = match file_meta.is_dir() {
        true => FileMetadata::create_dir(file_name),
        false => {
            chunks = generate_chunk_ids(file_meta.len());
            FileMetadata::create_file(file_name, file_meta.len(), chunks.clone())
        }
    };
    let upload_url = format!(
        "{}/api/files/upload?path={}",
        meta_url,
        path.strip_prefix(path_prefix).unwrap().display()
    );
    let file: FileMetadata = post_request(c, &upload_url, file_data)
        .await?
        .json()
        .await
        .context(errors::ParseJson)?;
    if let FileInfo::File(file_info) = &file.file_info {
        upload_file(c, meta_url, &file_info.id, chunks, path).await?;
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
) -> Result<()> {
    let servers_url = format!("{}/api/servers", meta_url);
    let servers: Vec<ChunkServer> = get_request_json(c, &servers_url).await?;

    let requests = chunks
        .into_iter()
        .enumerate()
        .map(|(i, chunk)| upload_chunk(c, &servers, path, (file_id, chunk, i)))
        .collect::<Vec<_>>();
    let responses = join_all(requests).await;
    if responses.iter().filter(|resp| resp.is_err()).size_hint().0 > 0 {
        return Err(errors::UploadChunks.build());
    }
    println!("Completed file upload");
    Ok(())
}

pub async fn upload_chunk(
    c: &Client,
    servers: &[ChunkServer],
    path: &Path,
    data: (&Uuid, Uuid, usize),
) -> Result<()> {
    let (file_id, chunk_id, part) = data;
    let mut slice = servers.to_vec();
    slice.shuffle(&mut thread_rng());
    let chunk_file_name = format!("{}-{}", &chunk_id, &file_id);
    let content_type = "application/octet-stream";
    let action = FileAction::Open;
    for server in servers {
        let mut f = FileFS::open(path)
            .await
            .context(errors::FileIO { path, action })?;
        f.seek(SeekFrom::Start(part as u64 * CHUNK_SIZE))
            .await
            .context(errors::FileIO { path, action })?;
        let stream = reader_stream(f.take(CHUNK_SIZE));
        let mut mpart = MultipartRequest::default();
        mpart.add_field("chunk_id", &chunk_id.to_string());
        mpart.add_field("file_id", &file_id.to_string());
        mpart.add_stream("file", &chunk_file_name, content_type, stream);
        let url = format!("{}/api/upload", server.address);
        let resp = c
            .post(&url)
            .header(
                CONTENT_TYPE,
                format!("multipart/form-data; boundary={}", &mpart.get_boundary()),
            )
            .send_body(BodyStream::new(Box::new(mpart)))
            .await
            .context(errors::FailedRequest { url })?;
        if resp.status().is_success() {
            return Ok(());
        }
    }
    Err(errors::UploadSingleChunk { part, chunk_id }.build())
}

pub async fn download<T: AsRef<Path>>(
    c: &Client,
    meta_url: &str,
    path: T,
    target_path: Option<&Path>,
) -> Result<()> {
    // get chunks and merge them into a file
    let file_url = format!("{}/api/files?path={}", meta_url, path.as_ref().display());
    let file: FileMetadata = get_request_json(c, &file_url).await?;
    let path = target_path.unwrap_or_else(|| Path::new(".")).to_path_buf();
    let mut items = vec![(file, path)];
    while !items.is_empty() {
        let (curr_f, curr_path) = items.pop().unwrap();
        match curr_f.file_info {
            FileInfo::Directory(name) => {
                let new_path = curr_path.join(name);
                create_dir(&new_path).await.context(errors::FileIO {
                    path: new_path.clone(),
                    action: FileAction::Create,
                })?;
                items.extend(
                    &mut curr_f
                        .children
                        .into_iter()
                        .map(|(_, f)| (f, new_path.clone())),
                );
            }
            FileInfo::File(f) => {
                download_file(c, meta_url, f, &curr_path).await?;
            }
        }
    }
    Ok(())
}

pub async fn download_file(
    c: &Client,
    meta_url: &str,
    file_info: File,
    target_dir: &Path,
) -> Result<()> {
    let chunks_url = format!("{}/api/chunks/file/{}", meta_url, &file_info.id);
    let target_path = target_dir.join(&file_info.name);
    let path = target_path.as_path();
    let chunks: Vec<Chunk> = get_request_json(c, &chunks_url).await?;
    let groups = chunks.linear_group_by_key(|a| a.id);
    let mut file = FileFS::create(path).await.context(errors::FileIO {
        path,
        action: FileAction::Create,
    })?;
    let requests = groups
        .map(|group| download_chunk(c, group, meta_url))
        .collect::<Vec<_>>();
    let expected_responses_count = requests.len();
    let mut responses: HashMap<Uuid, Response> = join_all(requests)
        .await
        .into_iter()
        .filter_map(|resp| resp.ok())
        .collect();
    if responses.len() < expected_responses_count {
        return Err(errors::SomeChunksNotAvailable.build());
    }
    for curr_chunk_id in &file_info.chunks {
        let mut payload = responses
            .remove(curr_chunk_id)
            .ok_or_else(|| unreachable!())?;
        while let Some(Ok(mut bytes)) = payload.next().await {
            file.write_buf(&mut bytes).await.context(errors::FileIO {
                path,
                action: FileAction::Write,
            })?;
        }
    }
    Ok(())
}

pub async fn download_chunk(
    c: &Client,
    chunks: &[Chunk],
    meta_url: &str,
) -> Result<(Uuid, Response)> {
    let chunk_name = chunks[0].chunk_name();
    for chunk in chunks {
        let chunk_servers_url = format!("{}/api/servers/{}", meta_url, &chunk.server_id);
        let mut resp = get_request(c, &chunk_servers_url).await?;
        if resp.status().is_success() {
            let server: ChunkServer = resp.json().await.context(errors::ParseJson)?;
            let download_url = format!("{}/api/download/{}", server.address, chunk.chunk_name());
            let download_resp = get_request(c, &download_url).await?;
            if download_resp.status().is_success() {
                return Ok((chunk.id, download_resp));
            }
        }
    }
    Err(errors::ChunkNotAvailable { chunk_name }.build())
}

async fn get_request(c: &Client, url: &str) -> Result<Response> {
    c.get(url)
        .send()
        .await
        .context(errors::FailedRequest { url })
}
async fn get_request_json<T: DeserializeOwned>(c: &Client, url: &str) -> Result<T> {
    let mut resp = get_request(c, url).await?;
    resp.json().await.context(errors::ParseJson)
}
async fn post_request<T: Serialize>(c: &Client, url: &str, data: T) -> Result<Response> {
    c.post(url)
        .send_json(&data)
        .await
        .context(errors::FailedRequest { url })
}
