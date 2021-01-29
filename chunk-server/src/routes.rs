use crate::errors::*;
use crate::{MetadataUrl, ServerID, UploadsDir};
use actix_multipart::Multipart;
use actix_web::http::header::CONTENT_TYPE;
use actix_web::{body::BodyStream, client::Client, get, post, HttpResponse};
use actix_web::{web::Data, web::Path, HttpRequest};
use ccfs_commons::http_utils::{
    create_ccfs_multipart, get_header, handle_file, handle_string, read_body,
};
use ccfs_commons::{chunk_name, errors::Error as BaseError, result::CCFSResult, Chunk};
use futures::TryStreamExt;
use snafu::ResultExt;
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use tempfile::tempdir;
use tokio::fs::{rename, File};
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

    let id = Uuid::from_str(&id_str).map_err(|source| BaseError::ParseUuid {
        text: id_str,
        source,
    })?;
    let file_id = Uuid::from_str(&file_id_str).map_err(|source| BaseError::ParseUuid {
        text: file_id_str,
        source,
    })?;

    let chunk = Chunk::new(id, file_id, **server);
    let from = PathBuf::from(file_path_str);
    let to = target_dir.join(chunk.chunk_name());
    rename(&from, &to)
        .await
        .map_err(|source| BaseError::Rename { from, to, source })?;

    let resp = Client::new()
        .post(&format!("{}/api/chunk/completed", **meta_url))
        .send_json(&chunk)
        .await
        .map_err(|err| {
            let reason = format!("{}", err);
            MetaServerCommunication { reason }.build()
        })?;
    match resp.status().is_success() {
        true => Ok(HttpResponse::Ok().finish()),
        false => {
            let reason = read_body(resp).await?;
            Err(MetaServerCommunication { reason }.build().into())
        }
    }
}

#[get("/download/{chunk_name}")]
pub async fn download(info: Path<String>, dir: Data<UploadsDir>) -> CCFSResult<HttpResponse> {
    let path = dir.join(&info.into_inner());
    let file = File::open(&path)
        .await
        .map_err(|source| BaseError::Read { path, source })?;
    Ok(HttpResponse::Ok().streaming(ReaderStream::new(file)))
}

#[post("/replicate")]
pub async fn replicate(request: HttpRequest, dir: Data<UploadsDir>) -> CCFSResult<HttpResponse> {
    let headers = request.headers();
    let chunk_id = get_header(headers, "x-ccfs-chunk-id").ok_or_else(|| MissingHeader.build())?;
    let file_id = get_header(headers, "x-ccfs-file-id").ok_or_else(|| MissingHeader.build())?;
    let server_url =
        get_header(headers, "x-ccfs-server-url").ok_or_else(|| MissingHeader.build())?;
    let chunk_file_name = chunk_name(file_id, chunk_id);
    let path = dir.join(&chunk_file_name);

    let f = File::open(&path)
        .await
        .map_err(|source| BaseError::Open { path, source })?;
    let stream = ReaderStream::new(f);
    let mpart = create_ccfs_multipart(chunk_id, file_id, stream);

    let url = format!("{}/api/upload", server_url);
    let resp = Client::new()
        .post(&url)
        .header(
            CONTENT_TYPE,
            format!("multipart/form-data; boundary={}", &mpart.get_boundary()),
        )
        .send_body(BodyStream::new(Box::new(mpart)))
        .await
        .map_err(|source| BaseError::FailedRequest { url, source })?;
    if !resp.status().is_success() {
        let response = read_body(resp).await?;
        return Err(BaseError::Unsuccessful { response }.into());
    }
    Ok(HttpResponse::Ok().finish())
}
