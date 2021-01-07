use crate::errors::{self, Result};
use crate::{MetadataUrl, ServerID, UploadsDir};
use actix_multipart::{Field, Multipart};
use actix_web::web::{self, Data};
use actix_web::{client::Client, HttpResponse};
use ccfs_commons::Chunk;
use fs::{create_dir_all, rename};
use futures::{StreamExt, TryStreamExt};
use snafu::ResultExt;
use std::collections::HashMap;
use std::str::FromStr;
use tokio::fs::{self, File};
use tokio::io::{reader_stream, AsyncWriteExt};
use uuid::Uuid;

async fn handle_string(mut data: Field) -> Result<String> {
    let mut content = Vec::new();
    while let Some(Ok(bytes)) = data.next().await {
        content.extend(bytes.as_ref());
    }
    String::from_utf8(content).context(errors::ParseString)
}

async fn handle_file(mut data: Field, path: &str) -> Result<()> {
    let mut f = File::create(path).await.context(errors::Create { path })?;
    while let Some(Ok(bytes)) = data.next().await {
        f.write_all(&bytes).await.context(errors::Write { path })?;
    }
    Ok(())
}

pub async fn upload(
    mut data: Multipart,
    meta_url: Data<MetadataUrl>,
    server: Data<ServerID>,
    dir: Data<UploadsDir>,
) -> Result<HttpResponse> {
    let path = &dir.join(".tmp");
    if !path.exists() {
        create_dir_all(&path)
            .await
            .context(errors::Create { path })?;
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
        return Err(errors::MissingPart.build());
    }

    let id_str = parts.remove("chunk_id").unwrap_or_else(|| unreachable!());
    let file_id_str = parts.remove("file_id").unwrap_or_else(|| unreachable!());
    let file_path_str = parts.remove("file").unwrap_or_else(|| unreachable!());

    let id = Uuid::from_str(&id_str).context(errors::ParseUuid { text: id_str })?;
    let file_id = Uuid::from_str(&file_id_str).context(errors::ParseUuid { text: file_id_str })?;

    let chunk = Chunk::new(id, file_id, *server.into_inner());
    let from = file_path_str;
    let to = &dir.join(chunk.chunk_name());
    rename(&from, to)
        .await
        .context(errors::Rename { from, to })?;

    let _resp = Client::new()
        .post(&format!("{}/api/chunk/completed", meta_url.into_inner()))
        .send_json(&chunk)
        .await
        .context(errors::MetaServerCommunication);

    Ok(HttpResponse::Ok().json(chunk))
}

pub async fn download(info: web::Path<String>, dir: Data<UploadsDir>) -> Result<HttpResponse> {
    let path = dir.join(&info.into_inner());
    let file = File::open(&path).await.context(errors::Read { path })?;
    Ok(HttpResponse::Ok().streaming(reader_stream(file)))
}
