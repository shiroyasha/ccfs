use actix_web::dev::Payload;
use actix_web::error::ErrorBadRequest;
use actix_web::{Error as ReqError, FromRequest, HttpRequest};
use chrono::serde::ts_milliseconds;
use chrono::{DateTime, Duration, Utc};
use futures_util::future::{err, ok, Ready};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::error::Error;
use std::str::FromStr;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Chunk {
    pub id: Uuid,
    pub file_id: Uuid,
    pub server_id: Uuid,
}
impl Chunk {
    pub fn new(id: Uuid, file_id: Uuid, server_id: Uuid) -> Self {
        Self {
            id,
            file_id,
            server_id,
        }
    }

    pub fn chunk_name(&self) -> String {
        chunk_name(&self.file_id.to_string(), &self.id.to_string())
    }
}

pub fn chunk_name(file_id: &str, chunk_id: &str) -> String {
    format!("{}_{}", file_id, chunk_id)
}
