use crate::errors::*;
use actix_web::body::BodyStream;
use actix_web::client::{Client, ClientResponse};
use actix_web::dev::{Decompress, Payload};
use actix_web::http::header::CONTENT_TYPE;
use ccfs_commons::http_utils::read_body;
use ccfs_commons::{chunk_name, Chunk, ChunkServer, FileInfo, FileMetadata, CHUNK_SIZE};
use ccfs_commons::{errors::Error as BaseError, result::CCFSResult};
use futures::future::join_all;
use mpart_async::client::MultipartRequest;
use rand::{seq::SliceRandom, thread_rng};
use serde::{de::DeserializeOwned, Serialize};
use snafu::ResultExt;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::Path;
use tokio::fs::{create_dir, File as FileFS};
use tokio::io::{reader_stream, AsyncReadExt, AsyncWriteExt};
use tokio::stream::StreamExt;
use uuid::Uuid;

type Response = ClientResponse<Decompress<Payload>>;

pub async fn list(c: &Client, meta_url: &str) -> CCFSResult<()> {
    let file: FileMetadata = get_request_json(c, &format!("{}/api/files", meta_url)).await?;
    println!("{}", file.print_current_dir()?);
    Ok(())
}

pub async fn tree(c: &Client, meta_url: &str) -> CCFSResult<()> {
    let file: FileMetadata = get_request_json(c, &format!("{}/api/files", meta_url)).await?;
    println!("{}", file.print_subtree());
    Ok(())
}

pub async fn upload<T: AsRef<Path>>(c: &Client, meta_url: &str, file_path: T) -> CCFSResult<()> {
    let path = file_path.as_ref().to_path_buf();
    if !path.exists() {
        return Err(FileNotExist { path }.build().into());
    }
    let path_prefix = path
        .ancestors()
        .nth(1)
        .map(|p| p.to_path_buf())
        .unwrap_or_default();
    let mut paths = vec![path];
    while let Some(curr) = paths.pop() {
        upload_item(c, meta_url, curr.as_path(), &path_prefix).await?;
        if curr.is_dir() {
            paths.extend(
                curr.read_dir()
                    .map_err(|source| BaseError::Open { path: curr, source })?
                    .filter_map(|item| item.ok())
                    .map(|item| item.path()),
            );
        }
    }
    Ok(())
}

pub async fn upload_item(c: &Client, meta_url: &str, path: &Path, prefix: &Path) -> CCFSResult<()> {
    let mut chunks = Vec::new();
    let file_meta = path.metadata().map_err(|source| BaseError::Read {
        path: path.into(),
        source,
    })?;
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
        path.strip_prefix(prefix).unwrap().display()
    );
    let file: FileMetadata = post_request(c, &upload_url, file_data)
        .await?
        .json()
        .await
        .context(ParseJson)?;
    if let FileInfo::File { id, .. } = &file.file_info {
        upload_file(c, meta_url, id, chunks, path).await?;
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
) -> CCFSResult<()> {
    let servers: Vec<ChunkServer> =
        get_request_json(c, &format!("{}/api/servers", meta_url)).await?;
    if servers.is_empty() {
        return Err(NoAvailableServers.build().into());
    }

    let requests = chunks
        .into_iter()
        .enumerate()
        .map(|(i, chunk)| upload_chunk(c, &servers, path, (file_id, chunk, i)));
    let responses = join_all(requests).await;
    if responses.iter().filter(|resp| resp.is_err()).size_hint().0 > 0 {
        return Err(UploadChunks.build().into());
    }
    println!("Completed file upload");
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
    let chunk_file_name = chunk_name(&file_id_str, &chunk_id_str);
    let content_type = "application/octet-stream";
    let mut rng = thread_rng();
    for _ in 0..servers.len() {
        let server = servers.choose(&mut rng).expect("servers is empty");
        let mut f = FileFS::open(path).await.map_err(|source| BaseError::Open {
            path: path.into(),
            source,
        })?;
        f.seek(SeekFrom::Start(part as u64 * CHUNK_SIZE))
            .await
            .map_err(|source| BaseError::Open {
                path: path.into(),
                source,
            })?;
        let stream = reader_stream(f.take(CHUNK_SIZE));
        let mut mpart = MultipartRequest::default();
        mpart.add_field("chunk_id", &chunk_id_str);
        mpart.add_field("file_id", &file_id_str);
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
            .map_err(|source| BaseError::FailedRequest { url, source })?;
        if resp.status().is_success() {
            return Ok(());
        }
    }
    Err(UploadSingleChunk { part, chunk_id }.build().into())
}

pub async fn download<T: AsRef<Path>>(
    c: &Client,
    meta_url: &str,
    path: T,
    target_path: Option<&Path>,
) -> CCFSResult<()> {
    // get chunks and merge them into a file
    let file_url = format!("{}/api/files?path={}", meta_url, path.as_ref().display());
    let file: FileMetadata = get_request_json(c, &file_url).await?;
    let mut curr_path = target_path.unwrap_or_else(|| Path::new(".")).to_path_buf();
    for curr_f in file.bfs_iter() {
        curr_path = curr_path.join(&curr_f.name);
        if let FileInfo::Directory { .. } = &curr_f.file_info {
            create_dir(&curr_path)
                .await
                .map_err(|source| BaseError::Create {
                    path: curr_path.clone(),
                    source,
                })?;
        } else {
            download_file(c, meta_url, &curr_f, &curr_path).await?;
        }
    }
    Ok(())
}

pub async fn download_file(
    c: &Client,
    meta_url: &str,
    file: &FileMetadata,
    target_dir: &Path,
) -> CCFSResult<()> {
    if let FileInfo::File { id, chunks, .. } = &file.file_info {
        let chunks_url = format!("{}/api/chunks/file/{}", meta_url, id);
        let target_path = target_dir.join(&file.name);
        let path = target_path.as_path();
        let groups: Vec<Vec<Chunk>> = get_request_json(c, &chunks_url).await?;
        if groups.len() < chunks.len() {
            return Err(SomeChunksNotAvailable.build().into());
        }
        let mut file = FileFS::create(path)
            .await
            .map_err(|source| BaseError::Create {
                path: path.into(),
                source,
            })?;
        let requests = groups
            .iter()
            .map(|group| download_chunk(c, group, meta_url));
        let mut responses: HashMap<Uuid, Response> = join_all(requests)
            .await
            .into_iter()
            .filter_map(|resp| match resp {
                Ok(pair) if pair.1.status().is_success() => Some(pair),
                _ => None,
            })
            .collect();
        if responses.len() < chunks.len() {
            return Err(SomeChunksNotAvailable.build().into());
        }
        for curr_chunk_id in chunks {
            if let Some(mut payload) = responses.remove(curr_chunk_id) {
                while let Some(Ok(mut bytes)) = payload.next().await {
                    file.write_buf(&mut bytes)
                        .await
                        .map_err(|source| BaseError::Write {
                            path: path.into(),
                            source,
                        })?;
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
) -> CCFSResult<(Uuid, Response)> {
    let chunk_name = chunks[0].chunk_name();
    for chunk in chunks {
        let chunk_servers_url = format!("{}/api/servers/{}", meta_url, &chunk.server_id);
        let server: ChunkServer = get_request_json(c, &chunk_servers_url).await?;
        let download_url = format!("{}/api/download/{}", server.address, chunk.chunk_name());
        let download_resp = get_request(c, &download_url).await?;
        if download_resp.status().is_success() {
            return Ok((chunk.id, download_resp));
        }
    }
    Err(ChunkNotAvailable { chunk_name }.build().into())
}

async fn get_request(c: &Client, url: &str) -> CCFSResult<Response> {
    let resp = c
        .get(url)
        .send()
        .await
        .map_err(|source| BaseError::FailedRequest {
            url: url.into(),
            source,
        })?;
    match resp.status().is_success() {
        true => Ok(resp),
        false => Err(BaseError::Unsuccessful {
            response: read_body(resp).await?,
        }
        .into()),
    }
}

async fn get_request_json<T: DeserializeOwned>(c: &Client, url: &str) -> CCFSResult<T> {
    let mut resp = get_request(c, url).await?;
    Ok(resp.json().await.context(ParseJson)?)
}

async fn post_request<T: Serialize>(c: &Client, url: &str, data: T) -> CCFSResult<Response> {
    Ok(c.post(url)
        .send_json(&data)
        .await
        .map_err(|source| BaseError::FailedRequest {
            url: url.into(),
            source,
        })?)
}
