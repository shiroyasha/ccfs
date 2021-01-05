use super::Result;
use crate::errors::{self, Error, FileAction};
use ccfs_commons::{Chunk, ChunkServer, File, FileInfo, FileMetadata, CHUNK_SIZE};
use futures::future::join_all;
use rand::{seq::SliceRandom, thread_rng};
use reqwest::multipart::{Form, Part};
use reqwest::{Client, Response};
use serde::{de::DeserializeOwned, Serialize};
use slice_group_by::GroupBy;
use snafu::ResultExt;
use std::path::Path;
use tokio::fs::{create_dir, File as FileFS};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::stream::StreamExt;

const BUF_SIZE: usize = 16384;

pub async fn list(client: &Client, meta_url: &str) -> Result<()> {
    let file_url = format!("{}/api/files", meta_url);
    let file: FileMetadata = get_request_json(&client, &file_url).await?;
    println!("{}", file.print_current_dir());
    Ok(())
}

pub async fn tree(client: &Client, meta_url: &str) -> Result<()> {
    let file_url = format!("{}/api/files", meta_url);
    let file: FileMetadata = get_request_json(&client, &file_url).await?;
    println!("{}", file.print_subtree());
    Ok(())
}

pub async fn upload<T: AsRef<Path>>(client: &Client, meta_url: &str, file_path: T) -> Result<()> {
    let path = file_path.as_ref().to_path_buf();
    if path.exists() {
        let path_prefix = path.ancestors().nth(1).unwrap().to_path_buf();
        let mut paths = vec![path];
        while !paths.is_empty() {
            let curr = paths.pop().unwrap();
            upload_item(client, meta_url, curr.as_path(), &path_prefix).await?;
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
        Err(Error::FileNotExist { path })
    }
}

pub async fn upload_item(
    client: &Client,
    meta_url: &str,
    path: &Path,
    path_prefix: &Path,
) -> Result<()> {
    let file_meta = path.metadata().context(errors::ReadMetadata { path })?;
    let file_name = path.file_name().unwrap().to_str().unwrap().to_string();
    let file_data = match file_meta.is_dir() {
        true => FileMetadata::create_dir(file_name),
        false => FileMetadata::create_file(file_name, file_meta.len()),
    };
    let upload_url = format!(
        "{}/api/files/upload?path={}",
        meta_url,
        path.strip_prefix(path_prefix).unwrap().display()
    );
    let file: FileMetadata = post_request(&client, &upload_url, &file_data)
        .await?
        .json()
        .await
        .context(errors::ParseJson)?;
    if let FileInfo::File(file_info) = file.file_info {
        upload_file(client, meta_url, path, &file_info).await?;
    }
    return Ok(());
}

pub async fn upload_file(
    client: &Client,
    meta_url: &str,
    path: &Path,
    file_info: &File,
) -> Result<()> {
    let servers_url = format!("{}/api/servers", meta_url);
    let servers: Vec<ChunkServer> = get_request_json(&client, &servers_url).await?;
    let mut f = FileFS::open(path).await.context(errors::FileIO {
        path,
        action: FileAction::Open,
    })?;

    let mut file_parts = Vec::with_capacity(file_info.num_of_chunks as usize);
    for file_part in 0..file_info.num_of_chunks {
        let mut chunk = Vec::with_capacity(CHUNK_SIZE as usize);
        loop {
            let mut buff_size = BUF_SIZE;
            let remaining = CHUNK_SIZE as usize - chunk.len();
            if remaining < BUF_SIZE {
                buff_size = remaining;
            }
            let mut temp = Vec::with_capacity(buff_size);
            let n = f.read_buf(&mut temp).await.context(errors::FileIO {
                path,
                action: FileAction::Read,
            })?;
            chunk.append(&mut temp);
            if n < BUF_SIZE || CHUNK_SIZE == chunk.len() as u64 {
                break;
            }
        }
        file_parts.push((file_info.id.to_string(), file_part.to_string(), chunk));
    }
    let requests = file_parts
        .into_iter()
        .map(|part| upload_chunk(client, &servers, part))
        .collect::<Vec<_>>();
    let responses = join_all(requests).await;
    if responses.iter().filter(|resp| resp.is_err()).size_hint().0 > 0 {
        return Err(Error::UploadChunks);
    }
    println!("Completed file upload");
    Ok(())
}

pub async fn upload_chunk(
    client: &Client,
    servers: &[ChunkServer],
    form_data: (String, String, Vec<u8>),
) -> Result<()> {
    let (file_id, file_part, raw_data) = form_data;
    let mut slice = servers.to_vec();
    slice.shuffle(&mut thread_rng());
    for server in servers {
        let upload_url = format!("{}/api/upload", server.address);
        let resp = client
            .post(&upload_url)
            .multipart(
                Form::new()
                    .text("file_id", file_id.clone())
                    .text("file_part_num", file_part.clone())
                    .part("file", Part::bytes(raw_data.clone())),
            )
            .send()
            .await
            .context(errors::FailedRequest { url: upload_url })?;
        if resp.status().is_success() {
            return Ok(());
        }
    }
    Err(Error::ChunkNotAvailable {
        chunk_id: servers[0].id,
    })
}

pub async fn download<T: AsRef<Path>>(
    client: &Client,
    meta_url: &str,
    path: T,
    target_path: Option<&Path>,
) -> Result<()> {
    // get chunks and merge them into a file
    let file_url = format!("{}/api/files?path={}", meta_url, path.as_ref().display());
    let file: FileMetadata = get_request_json(&client, &file_url).await?;
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
                download_file(client, meta_url, f, &curr_path).await?;
            }
        }
    }
    Ok(())
}

pub async fn download_file(
    client: &Client,
    meta_url: &str,
    file: File,
    target_dir: &Path,
) -> Result<()> {
    let chunks_url = format!("{}/api/chunks/file/{}", meta_url, &file.id);
    let target_path = target_dir.join(file.name);
    let path = target_path.as_path();
    let mut chunks: Vec<Chunk> = get_request_json(&client, &chunks_url).await?;
    chunks.sort_by_key(|a| a.file_part_num);
    let groups = chunks.linear_group_by(|a, b| a.file_part_num == b.file_part_num);
    let mut file = FileFS::create(path).await.context(errors::FileIO {
        path,
        action: FileAction::Create,
    })?;
    let mut requests = Vec::with_capacity(groups.size_hint().0);
    for group in groups {
        requests.push(download_chunk(client, group, meta_url));
    }
    let responses = join_all(requests).await;
    let errors = responses.iter().filter(|resp| resp.is_err());
    if errors.size_hint().0 > 0 {
        return Err(Error::SomeChunksNotAvailable);
    }
    for mut stream in responses
        .into_iter()
        .map(|resp| resp.unwrap().bytes_stream())
    {
        while let Some(content) = stream.next().await {
            file.write(&content.unwrap())
                .await
                .context(errors::FileIO {
                    path,
                    action: FileAction::Write,
                })?;
        }
    }
    Ok(())
}

pub async fn download_chunk(
    client: &Client,
    servers: &[Chunk],
    meta_url: &str,
) -> Result<Response> {
    for chunk in servers {
        let chunk_url = format!("{}/api/servers/{}", meta_url, &chunk.server_id);
        let resp: Response = get_request(&client, &chunk_url).await?;
        if resp.status().is_success() {
            let server: ChunkServer = resp.json().await.context(errors::ParseJson)?;
            let download_url = format!("{}/api/download/{}", server.address, &chunk.id);
            let download_resp = get_request(&client, &download_url).await?;
            if download_resp.status().is_success() {
                return Ok(download_resp);
            }
        }
    }
    Err(Error::ChunkNotAvailable {
        chunk_id: servers[0].id,
    })
}

async fn get_request(client: &Client, url: &str) -> Result<Response> {
    client
        .get(url)
        .send()
        .await
        .context(errors::FailedRequest { url })
}
async fn get_request_json<T: DeserializeOwned>(client: &Client, url: &str) -> Result<T> {
    get_request(client, url)
        .await?
        .json()
        .await
        .context(errors::ParseJson)
}
async fn post_request<T: Serialize>(client: &Client, url: &str, data: &T) -> Result<Response> {
    client
        .post(url)
        .json(data)
        .send()
        .await
        .context(errors::FailedRequest { url })
}
