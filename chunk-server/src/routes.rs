use crate::errors::*;
use crate::{MetadataUrl, ServerID, UploadsDir};
use actix_multipart::Multipart;
use actix_web::{get, post, HttpResponse};
use actix_web::{web::Data, web::Path, HttpRequest};
use ccfs_commons::http_utils::{create_ccfs_multipart, get_header, handle_file, handle_string};
use ccfs_commons::{chunk_name, errors, result::CCFSResult, Chunk};
use futures::TryStreamExt;
use reqwest::{Body, Client};
use snafu::ResultExt;
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use tempfile::tempdir;
use tokio::fs::{rename, File};
use tokio_util::codec::{BytesCodec, FramedRead};
use tokio_util::io::ReaderStream;
use uuid::Uuid;

#[post("/upload")]
pub async fn upload(
    mut data: Multipart,
    meta_url: Data<MetadataUrl>,
    server: Data<ServerID>,
    target_dir: Data<UploadsDir>,
) -> CCFSResult<HttpResponse> {
    let temp = tempdir().context(TempDir)?;
    let mut parts: HashMap<String, String> = HashMap::new();
    while let Ok(Some(field)) = data.try_next().await {
        if let Some(content_disposition) = field.content_disposition() {
            if let Some(name) = content_disposition.get_name() {
                match name {
                    "chunk_id" | "file_id" => {
                        parts.insert(name.into(), handle_string(field).await?);
                    }
                    "file" => {
                        let file_path = temp.path().join(Uuid::new_v4().to_string());
                        handle_file(field, &file_path).await?;
                        parts.insert(name.into(), file_path.display().to_string());
                    }
                    _ => {}
                }
            }
        }
    }
    if parts.len() != 3 {
        return Err(MissingPart.build().into());
    }

    let id_str = parts.remove("chunk_id").unwrap_or_else(|| unreachable!());
    let file_id_str = parts.remove("file_id").unwrap_or_else(|| unreachable!());
    let file_path_str = parts.remove("file").unwrap_or_else(|| unreachable!());

    let id = Uuid::from_str(&id_str).context(errors::ParseUuid { text: id_str })?;
    let file_id = Uuid::from_str(&file_id_str).context(errors::ParseUuid { text: file_id_str })?;

    let chunk = Chunk::new(id, file_id, **server);
    let from = PathBuf::from(file_path_str);
    let to = target_dir.join(chunk.chunk_name());
    rename(&from, &to)
        .await
        .context(errors::Rename { from, to })?;

    let resp = Client::new()
        .post(&format!("{}/api/chunk/completed", **meta_url))
        .header("x-ccfs-client-id", &*server.to_string())
        .json(&chunk)
        .send()
        .await
        .map_err(|err| {
            let reason = format!("{}", err);
            MetaServerCommunication { reason }.build()
        })?;
    match resp.status().is_success() {
        true => Ok(HttpResponse::Ok().finish()),
        false => {
            let reason = resp.text().await.context(errors::ReadString)?;
            Err(MetaServerCommunication { reason }.build().into())
        }
    }
}

#[get("/download/{chunk_name}")]
pub async fn download(info: Path<String>, dir: Data<UploadsDir>) -> CCFSResult<HttpResponse> {
    let path = dir.join(&info.into_inner());
    let file = File::open(&path).await.context(errors::Read { path })?;
    Ok(HttpResponse::Ok().streaming(ReaderStream::new(file)))
}

#[post("/replicate")]
pub async fn replicate(request: HttpRequest, dir: Data<UploadsDir>) -> CCFSResult<HttpResponse> {
    let headers = request.headers();
    let chunk_id = get_header(headers, "x-ccfs-chunk-id")?;
    let file_id = get_header(headers, "x-ccfs-file-id")?;
    let server_url = get_header(headers, "x-ccfs-server-url")?;
    let chunk_file_name = chunk_name(file_id, chunk_id);
    let path = dir.join(&chunk_file_name);

    let f = File::open(&path).await.context(errors::Open { path })?;
    let stream = FramedRead::new(f, BytesCodec::new());
    let mpart = create_ccfs_multipart(chunk_id, file_id, Body::wrap_stream(stream));

    let url = format!("{}/api/upload", server_url);
    let resp = Client::new()
        .post(&url)
        .multipart(mpart)
        .send()
        .await
        .context(errors::FailedRequest { url: &url })?;
    if !resp.status().is_success() {
        let response = resp.text().await.context(errors::ReadString)?;
        return Err(errors::Unsuccessful { url, response }.build().into());
    }
    Ok(HttpResponse::Ok().finish())
}
