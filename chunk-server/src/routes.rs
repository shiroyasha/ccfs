use crate::errors::*;
use crate::{MetadataUrl, ServerID, UploadsDir};
use actix_multipart::Multipart;
use actix_web::{client::Client, get, post, web, HttpResponse};
use ccfs_commons::http_utils::{handle_file, handle_string, read_body};
use ccfs_commons::{data::Data, Chunk};
use ccfs_commons::{errors::Error as BaseError, result::CCFSResult};
use fs::{create_dir_all, rename};
use futures::TryStreamExt;
use std::collections::HashMap;
use std::str::FromStr;
use tokio::fs::{self, File};
use tokio::io::reader_stream;
use uuid::Uuid;

#[post("/upload")]
pub async fn upload(
    mut data: Multipart,
    meta_url: web::Data<Data<MetadataUrl>>,
    server: web::Data<Data<ServerID>>,
    dir: web::Data<Data<UploadsDir>>,
) -> CCFSResult<HttpResponse> {
    let path = &dir.join(".tmp");
    if !path.exists() {
        create_dir_all(&path)
            .await
            .map_err(|source| BaseError::Create {
                path: path.into(),
                source,
            })?;
    }
    let mut parts: HashMap<String, String> = HashMap::new();
    while let Ok(Some(field)) = data.try_next().await {
        if let Some(content_disposition) = field.content_disposition() {
            if let Some(name) = content_disposition.get_name() {
                match name {
                    "chunk_id" | "file_id" => {
                        parts.insert(name.into(), handle_string(field).await?);
                    }
                    "file" => {
                        let file_path = path.join(Uuid::new_v4().to_string()).display().to_string();
                        handle_file(field, &file_path).await?;
                        parts.insert(name.into(), file_path);
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

    let chunk = Chunk::new(id, file_id, server.inner);
    let from = file_path_str;
    let to = &dir.join(chunk.chunk_name());
    rename(&from, to)
        .await
        .map_err(|source| BaseError::Rename {
            from: from.into(),
            to: to.into(),
            source,
        })?;

    let resp = Client::new()
        .post(&format!("{}/api/chunk/completed", meta_url.inner))
        .send_json(&chunk)
        .await
        .map_err(|err| {
            let reason = format!("{}", err);
            MetaServerCommunication { reason }.build()
        })?;
    match resp.status().is_success() {
        true => Ok(HttpResponse::Ok().json(chunk)),
        false => {
            let reason = read_body(resp).await?;
            Err(MetaServerCommunication { reason }.build().into())
        }
    }
}

#[get("/download/{chunk_name}")]
pub async fn download(
    info: web::Path<String>,
    dir: web::Data<Data<UploadsDir>>,
) -> CCFSResult<HttpResponse> {
    let path = dir.join(&info.into_inner());
    let file = File::open(&path)
        .await
        .map_err(|source| BaseError::Read { path, source })?;
    Ok(HttpResponse::Ok().streaming(reader_stream(file)))
}
