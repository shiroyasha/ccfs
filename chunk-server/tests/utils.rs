use actix_http::Request;
use actix_web::body::BodyStream;
use actix_web::dev::Body;
use actix_web::test::TestRequest;
use actix_web::web::{Bytes, BytesMut};
use futures_util::future::poll_fn;
use mpart_async::client::MultipartRequest;
use std::path::Path;
use std::pin::Pin;
use tokio::fs::read_dir;
use tokio::io::reader_stream;

pub async fn is_empty(path: &Path) -> std::io::Result<bool> {
    Ok(matches!(read_dir(path).await?.next_entry().await?, None))
}

pub async fn create_multipart_request(
    url: &'static str,
    chunk_id: Option<String>,
    file_id: Option<String>,
) -> Request {
    let stream = reader_stream("content".as_bytes());
    let mut mpart = MultipartRequest::default();
    if let Some(chunk) = chunk_id {
        mpart.add_field("chunk_id", &chunk);
    }
    if let Some(file_id) = file_id {
        mpart.add_field("file_id", &file_id);
    }
    mpart.add_stream("file", "file_name", "application/octet-stream", stream);
    let boundary = mpart.get_boundary().to_string();
    let body = Body::from(BodyStream::new(Box::new(mpart)));
    let bytes = match body {
        Body::Message(mut b) => {
            let mut res = BytesMut::new();
            while let Some(Ok(bytes)) = poll_fn(|cx| Pin::new(b.as_mut()).poll_next(cx)).await {
                res.extend(bytes);
            }
            Bytes::from(res)
        }
        _ => unreachable!(),
    };
    let content_type = format!("multipart/form-data; boundary={}", boundary);
    TestRequest::post()
        .uri(url)
        .header("content-type", content_type)
        .set_payload(bytes)
        .to_request()
}
