use crate::errors::*;
use crate::result::CCFSResult;
use actix_multipart::Field;
use actix_web::client::ClientResponse;
use actix_web::dev::{Decompress, Payload};
use actix_web::http::HeaderMap;
use futures_util::StreamExt;
use snafu::ResultExt;
use std::path::Path;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

pub type Response = ClientResponse<Decompress<Payload>>;

pub async fn read_body(mut resp: Response) -> CCFSResult<String> {
    let mut content = Vec::new();
    if let Ok(bytes) = resp.body().await {
        content.extend(bytes.as_ref());
    }
    Ok(String::from_utf8(content).context(ParseString)?)
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

pub fn get_header<'a>(headers: &'a HeaderMap, key: &'a str) -> Option<&'a str> {
    headers.get(key)?.to_str().ok()
}
