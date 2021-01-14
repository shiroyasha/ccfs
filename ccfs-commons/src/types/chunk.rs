use serde::{Deserialize, Serialize};
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
