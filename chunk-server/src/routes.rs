use crate::errors::{self, Result};
use crate::{MetadataUrl, ServerID, UploadsDir};
use ccfs_commons::{Chunk, CHUNK_SIZE};
use rocket::data::ToByteUnit;
use rocket::http::ContentType;
use rocket::response::Stream;
use rocket::{Data, State};
use rocket_contrib::json::JsonValue;
use rocket_contrib::uuid::uuid_crate::Uuid;
use rocket_multipart_form_data::{
    MultipartFormData, MultipartFormDataField, MultipartFormDataOptions,
};
use snafu::ResultExt;
use std::collections::HashMap;
use std::str::FromStr;
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;

#[post("/upload", data = "<data>")]
pub async fn multipart_upload(
    metadata_url: State<'_, MetadataUrl>,
    server_id: State<'_, ServerID>,
    uploads_dir: State<'_, UploadsDir>,
    content_type: &ContentType,
    data: Data,
) -> Result<JsonValue> {
    let path = uploads_dir.inner();
    if !path.exists() {
        fs::create_dir(path)
            .await
            .context(errors::IOCreate { path })?;
    }
    let options = MultipartFormDataOptions {
        temporary_dir: path.to_path_buf(),
        allowed_fields: vec![
            MultipartFormDataField::raw("file").size_limit(CHUNK_SIZE),
            MultipartFormDataField::text("chunk_id"),
            MultipartFormDataField::text("file_id"),
        ],
    };

    let limit = 64.megabytes();
    let form_data = MultipartFormData::parse(content_type, data.open(limit), options)
        .await
        .context(errors::ParseData)?;

    let chunk_str = &get_multipart_field_data(&form_data.texts, "chunk_id")?[0].text;
    let file_str = &get_multipart_field_data(&form_data.texts, "file_id")?[0].text;
    let file = &get_multipart_field_data(&form_data.raw, "file")?[0];

    let chunk_id = Uuid::from_str(&chunk_str).context(errors::ParseUuid { text: chunk_str })?;
    let file_id = Uuid::from_str(&file_str).context(errors::ParseUuid { text: file_str })?;

    let chunk = Chunk::new(chunk_id, file_id, *server_id.inner());
    let new_path = path.join(chunk.chunk_name());

    let mut f = File::create(&new_path)
        .await
        .context(errors::IOCreate { path: &new_path })?;
    f.write_all(&file.raw)
        .await
        .context(errors::IOWrite { path: new_path })?;

    let _resp = reqwest::Client::new()
        .post(&format!("{}/api/chunk/completed", metadata_url.inner()))
        .json(&chunk)
        .send()
        .await
        .context(errors::MetaServerCommunication)?;
    Ok(json!(chunk))
}

#[get("/download/<chunk_name>")]
pub async fn download(
    chunk_name: String,
    uploads_dir: State<'_, UploadsDir>,
) -> Option<Stream<File>> {
    let file_path = uploads_dir.join(chunk_name);
    File::open(file_path).await.map(Stream::from).ok()
}

fn get_multipart_field_data<'a, T>(map: &'a HashMap<String, T>, key: &str) -> Result<&'a T> {
    map.get(key)
        .ok_or_else(|| errors::MissingPart { key }.build())
}
