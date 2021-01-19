use actix_web::dev::Payload;
use actix_web::error::ErrorBadRequest;
use actix_web::{Error as ReqError, FromRequest, HttpRequest};
use chrono::serde::ts_milliseconds;
use chrono::{DateTime, Duration, Utc};
use futures_util::future::{err, ok, Ready};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use uuid::Uuid;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct ChunkServer {
    pub id: Uuid,
    pub address: String,
    #[serde(with = "ts_milliseconds")]
    pub latest_ping_time: DateTime<Utc>,
}
impl ChunkServer {
    pub fn new(id: Uuid, address: String) -> Self {
        Self {
            id,
            address,
            latest_ping_time: Utc::now(),
        }
    }

    pub fn is_active(&self) -> bool {
        Utc::now().signed_duration_since(self.latest_ping_time) <= Duration::seconds(6)
    }
}

impl FromRequest for ChunkServer {
    type Error = ReqError;
    type Future = Ready<Result<ChunkServer, Self::Error>>;
    type Config = ();

    fn from_request(request: &HttpRequest, _payload: &mut Payload) -> Self::Future {
        let headers = request.headers();
        match (
            headers.get("x-ccfs-chunk-server-id"),
            headers.get("x-ccfs-chunk-server-address"),
        ) {
            (Some(id_header), Some(address_header)) => {
                match (id_header.to_str(), address_header.to_str()) {
                    (Ok(id_str), Ok(url)) => match Uuid::from_str(id_str) {
                        Ok(id) => ok(ChunkServer::new(id, url.to_string())),
                        Err(_) => err(ErrorBadRequest("Not a valid uuid")),
                    },
                    _ => err(ErrorBadRequest("Cannot read header value")),
                }
            }
            _ => err(ErrorBadRequest("Missing header values")),
        }
    }
}
