use crate::result::CCFSResult;
use crate::{chunk_name, errors::*};
use actix_multipart::Field;
use actix_web::http::HeaderMap;
use futures_util::StreamExt;
use reqwest::multipart::{Form, Part};
use reqwest::Body;
use snafu::ResultExt;
use std::path::Path;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

#[cfg(target_os = "linux")]
pub fn get_ip() -> Option<String> {
    get_private_ip("eth0")
}

#[cfg(target_os = "macos")]
pub fn get_ip() -> Option<String> {
    get_private_ip("en0")
}

pub fn get_private_ip(target_name: &str) -> Option<String> {
    let interfaces = pnet::datalink::interfaces();
    interfaces.iter().find(|i| i.name == target_name).map(|i| {
        i.ips
            .iter()
            .find(|ip| ip.is_ipv4())
            .map(|ip| ip.ip().to_string())
    })?
}

pub async fn handle_string(mut data: Field) -> CCFSResult<String> {
    let mut content = Vec::new();
    while let Some(Ok(bytes)) = data.next().await {
        content.extend(bytes.as_ref());
    }
    Ok(String::from_utf8(content).context(ParseString)?)
}

pub async fn handle_file(mut data: Field, path: &Path) -> CCFSResult<()> {
    let mut f = File::create(path).await.context(Create { path })?;
    while let Some(Ok(bytes)) = data.next().await {
        f.write_all(&bytes).await.context(Write { path })?;
    }
    Ok(())
}

pub fn get_header<'a>(headers: &'a HeaderMap, key: &'a str) -> Result<&'a str, Error> {
    headers
        .get(key)
        .map(|v| v.to_str().map_err(|_| Error::MissingHeader))
        .ok_or(Error::MissingHeader)?
}

pub fn create_ccfs_multipart(chunk_id: &str, file_id: &str, stream: Body) -> Form {
    let file_part = Part::stream(stream)
        .file_name(chunk_name(&file_id, &chunk_id))
        .mime_str("application/octet-stream")
        .unwrap();
    Form::new()
        .text("chunk_id", chunk_id.to_owned())
        .text("file_id", file_id.to_owned())
        .part("file", file_part)
}
