pub mod data;
pub mod errors;
pub mod http_utils;
pub mod result;

use actix_web::dev::Payload;
use actix_web::error::ErrorBadRequest;
use actix_web::{Error as ReqError, FromRequest, HttpRequest};
use chrono::serde::ts_milliseconds;
use chrono::{DateTime, Utc};
use futures_util::future::{err, ok, Ready};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::error::Error;
use std::str::FromStr;
use uuid::Uuid;

pub const CHUNK_SIZE: u64 = 64 * 1024 * 1024;

#[derive(Clone, Serialize, Deserialize, Debug)]
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
}

#[derive(Debug)]
pub enum HeaderError {
    Missing,
    Invalid,
}

impl FromRequest for ChunkServer {
    type Error = ReqError;
    type Future = Ready<Result<ChunkServer, Self::Error>>;
    type Config = ();

    fn from_request(request: &HttpRequest, _payload: &mut Payload) -> Self::Future {
        let headers = request.headers();
        match (
            headers.get("x-chunk-server-id"),
            headers.get("x-chunk-server-address"),
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

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum FileStatus {
    Started,
    Completed,
    Canceled,
}
impl Default for FileStatus {
    fn default() -> Self {
        Self::Started
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileMetadata {
    pub file_info: FileInfo,
    pub children: BTreeMap<String, FileMetadata>,
    pub version: usize,
    #[serde(with = "ts_milliseconds")]
    pub created_at: DateTime<Utc>,
    #[serde(with = "ts_milliseconds")]
    pub modified_at: DateTime<Utc>,
}

impl FileMetadata {
    pub fn create_root() -> Self {
        Self::create_dir("root".into())
    }

    pub fn create_dir(name: String) -> Self {
        let now = Utc::now();
        Self {
            file_info: FileInfo::Directory(name),
            children: BTreeMap::new(),
            version: 1,
            created_at: now,
            modified_at: now,
        }
    }

    pub fn create_file(name: String, size: u64, chunks: Vec<Uuid>) -> Self {
        let now = Utc::now();
        Self {
            file_info: FileInfo::File(File::new(name, size, chunks)),
            children: BTreeMap::new(),
            version: 1,
            created_at: now,
            modified_at: now,
        }
    }

    pub fn traverse(&self, path: &str) -> Result<&Self, Box<dyn Error>> {
        let mut curr = self;
        if !path.is_empty() {
            for segment in path.split_terminator('/') {
                match curr.children.get(segment) {
                    Some(next) => curr = next,
                    None => return Err(format!("path {} doesn't exist", path).into()),
                }
            }
        }
        Ok(curr)
    }

    pub fn traverse_mut(&mut self, path: &str) -> Result<&mut Self, Box<dyn Error>> {
        let mut curr = self;
        if !path.is_empty() {
            for segment in path.split_terminator('/') {
                match curr.children.get_mut(segment) {
                    Some(next) => curr = next,
                    None => return Err(format!("path {} doesn't exist", path).into()),
                }
            }
        }
        Ok(curr)
    }

    pub fn insert_dir(&mut self, name: &str) {
        self.children
            .insert(name.into(), Self::create_dir(name.into()));
    }

    pub fn insert_file(&mut self, name: &str, size: u64, chunks: Vec<Uuid>) {
        self.children
            .insert(name.into(), Self::create_file(name.into(), size, chunks));
    }

    pub fn print_subtree(&self) -> String {
        match &self.file_info {
            FileInfo::File(file) => file.name.to_string(),
            FileInfo::Directory(name) => {
                let mut iter = self.children.values().peekable();
                let mut s = name.to_string();
                while let Some(child) = iter.next() {
                    let prefix = if iter.peek().is_some() { "├" } else { "└" };
                    let subdir_prefix = if iter.peek().is_some() { "│" } else { " " };
                    let subtree = child.print_subtree();
                    let mut lines_iter = subtree.lines();
                    s.push_str(&format!("\n{:─<2} {}", prefix, lines_iter.next().unwrap()));
                    for l in lines_iter {
                        s.push_str(&format!("\n{:<2} {}", subdir_prefix, l));
                    }
                }
                s
            }
        }
    }

    pub fn print_current_dir(&self) -> String {
        match &self.file_info {
            FileInfo::Directory(_name) => {
                let mut iter = self.children.values().peekable();
                let mut s = String::new();
                while let Some(child) = iter.next() {
                    let has_next = iter.peek().is_some();
                    let child_name = match &child.file_info {
                        FileInfo::Directory(name) => &name,
                        FileInfo::File(file) => &file.name,
                    };
                    s.push_str(&child_name);
                    if has_next {
                        s.push('\n');
                    }
                }
                s
            }
            _ => unreachable!(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum FileInfo {
    Directory(String),
    File(File),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct File {
    pub id: Uuid,
    pub name: String,
    pub size: u64,
    pub chunks: Vec<Uuid>,
    #[serde(default)]
    pub num_of_completed_chunks: usize,
    #[serde(default = "FileStatus::default")]
    pub status: FileStatus,
}
impl File {
    pub fn new(name: String, size: u64, chunks: Vec<Uuid>) -> Self {
        Self {
            id: Uuid::new_v4(),
            name,
            size,
            chunks,
            num_of_completed_chunks: 0,
            status: FileStatus::Started,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
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
        format!("{}_{}", self.file_id, self.id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trie_insert_test() -> Result<(), Box<dyn Error>> {
        let mut trie = FileMetadata::create_root();
        trie.insert_dir("dir1");
        assert_eq!(trie.children.len(), 1);
        assert_eq!(
            trie.children.get("dir1").ok_or("missing dir1")?.file_info,
            FileInfo::Directory("dir1".into())
        );
        trie.insert_dir("dir2");
        assert_eq!(trie.children.len(), 2);
        assert_eq!(
            trie.children.get("dir2").ok_or("missing dir2")?.file_info,
            FileInfo::Directory("dir2".into())
        );
        trie.insert_file(
            "some.zip",
            20,
            vec![Uuid::from_str("ec73d743-050b-4f52-992a-d1102340d739")?],
        );
        assert_eq!(trie.children.len(), 3);
        let file = &trie.children.get("some.zip").ok_or("some.zip not found")?;
        match &file.file_info {
            FileInfo::File(f) => {
                assert_eq!(f.size, 20);
            }
            _ => return Err("some.zip is dir".into()),
        }
        Ok(())
    }

    #[test]
    fn trie_traverse_test() -> Result<(), Box<dyn Error>> {
        let mut trie = FileMetadata::create_root();
        trie.insert_dir("dir1");
        trie.insert_dir("dir2");
        trie.insert_file(
            "some.zip",
            20,
            vec![Uuid::from_str("ec73d743-050b-4f52-992a-d1102340d739")?],
        );
        let dir1 = trie.traverse("dir1")?;
        match &dir1.file_info {
            FileInfo::Directory(name) => assert_eq!(name, "dir1"),
            _ => return Err("not a dir".into()),
        }
        match dir1.traverse("subdir") {
            Err(err) => {
                assert_eq!(err.to_string(), "path subdir doesn\'t exist");
            }
            _ => return Err("should be an error".into()),
        }
        match trie.traverse("dir1/subdir") {
            Err(err) => {
                assert_eq!(err.to_string(), "path dir1/subdir doesn\'t exist");
            }
            _ => return Err("should be an error".into()),
        }
        let dir2 = trie.traverse("dir2")?;
        match &dir2.file_info {
            FileInfo::Directory(name) => assert_eq!(name, "dir2"),
            _ => return Err("not a dir".into()),
        }
        let file = trie.traverse("some.zip")?;
        match &file.file_info {
            FileInfo::File(file) => assert_eq!(file.name, "some.zip"),
            _ => return Err("not a file".into()),
        }

        Ok(())
    }

    fn add_dir2(trie: &mut FileMetadata) -> Result<(), Box<dyn Error>> {
        trie.insert_dir("dir2");
        let dir2 = trie.traverse_mut("dir2")?;
        dir2.insert_file(
            "test.txt",
            10,
            vec![Uuid::from_str("1a6e7006-12a7-4935-b8c0-58fa7ea84b09")?],
        );
        dir2.insert_dir("subdir");
        let subdir = dir2.traverse_mut("subdir")?;
        subdir.insert_dir("tmp");
        subdir.insert_file(
            "file",
            100,
            vec![Uuid::from_str("6d53a85f-505b-4a1a-ae6d-f7c18761d04a")?],
        );
        Ok(())
    }

    fn build() -> Result<FileMetadata, Box<dyn Error>> {
        let mut trie = FileMetadata::create_root();
        trie.insert_dir("dir1");
        add_dir2(&mut trie)?;
        trie.insert_file(
            "some.zip",
            0,
            vec![Uuid::from_str("ec73d743-050b-4f52-992a-d1102340d739")?],
        );

        Ok(trie)
    }

    #[test]
    fn trie_print_subtree_test() -> Result<(), Box<dyn Error>> {
        let trie = build()?;
        let expected = std::fs::read_to_string("expected-tree.txt")?;
        assert_eq!(trie.print_subtree(), expected);
        Ok(())
    }

    #[test]
    fn trie_print_single_dir_subtree_test() -> Result<(), Box<dyn Error>> {
        let mut trie = FileMetadata::create_root();
        add_dir2(&mut trie)?;
        let expected = std::fs::read_to_string("expected-single-dir-tree.txt")?;
        assert_eq!(trie.print_subtree(), expected);
        Ok(())
    }

    #[test]
    fn trie_print_current_dir_test() -> Result<(), Box<dyn Error>> {
        let trie = build()?;
        assert_eq!(trie.print_current_dir(), "dir1\ndir2\nsome.zip");
        assert_eq!(trie.traverse("dir1")?.print_current_dir(), "");
        assert_eq!(
            trie.traverse("dir2")?.print_current_dir(),
            "subdir\ntest.txt"
        );
        assert_eq!(
            trie.traverse("dir2/subdir")?.print_current_dir(),
            "file\ntmp"
        );
        Ok(())
    }
}
