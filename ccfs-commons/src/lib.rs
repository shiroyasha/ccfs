use chrono::{DateTime, Utc};
use rocket::http::Status;
use rocket::outcome::Outcome::*;
use rocket::request::{self, FromRequest, Request};
use rocket_contrib::uuid::Uuid;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::error::Error;
use std::str::FromStr;

pub const CHUNK_SIZE: u64 = 64000000;

pub mod custom_uuid {
    use rocket_contrib::uuid::Uuid;
    use serde::{de::Error, Deserialize, Deserializer, Serialize, Serializer};
    use std::str::FromStr;

    pub fn serialize<S: Serializer>(val: &'_ Uuid, serializer: S) -> Result<S::Ok, S::Error> {
        val.to_string().serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Uuid, D::Error> {
        let val: &str = Deserialize::deserialize(deserializer)?;
        Uuid::from_str(val).map_err(D::Error::custom)
    }
}

pub mod custom_time {
    use chrono::{DateTime, NaiveDateTime, Utc};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(val: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error> {
        val.timestamp().to_string().serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<DateTime<Utc>, D::Error> {
        let s = String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)?;
        Ok(DateTime::from_utc(NaiveDateTime::from_timestamp(s, 0), Utc))
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ChunkServer {
    #[serde(with = "custom_uuid")]
    pub id: Uuid,
    pub address: String,
    pub is_active: bool,
    #[serde(with = "custom_time")]
    pub latest_ping_time: DateTime<Utc>,
}
impl ChunkServer {
    pub fn new(id: Uuid, address: String) -> ChunkServer {
        ChunkServer {
            id,
            address,
            is_active: true,
            latest_ping_time: DateTime::from_utc(Utc::now().naive_utc(), Utc),
        }
    }
}

#[derive(Debug)]
pub enum HeaderError {
    Missing,
    Invalid,
}

#[rocket::async_trait]
impl<'a, 'r> FromRequest<'a, 'r> for ChunkServer {
    type Error = HeaderError;

    async fn from_request(request: &'a Request<'r>) -> request::Outcome<Self, Self::Error> {
        let id_header: Vec<_> = request.headers().get("x-chunk-server-id").collect();
        let address_header: Vec<_> = request.headers().get("x-chunk-server-address").collect();
        match (id_header.len(), address_header.len()) {
            (a, b) if a == 0 || b == 0 => Failure((Status::BadRequest, HeaderError::Missing)),
            _ => {
                let parsed_id = Uuid::from_str(&id_header.concat());
                match parsed_id {
                    Ok(id) => Success(ChunkServer::new(id, address_header.concat())),
                    _ => Failure((Status::BadRequest, HeaderError::Invalid)),
                }
            }
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
        FileStatus::Started
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileMetadata {
    file_info: FileInfo,
    children: BTreeMap<String, FileMetadata>,
    version: usize,
    #[serde(with = "custom_time")]
    created_at: DateTime<Utc>,
    #[serde(with = "custom_time")]
    modified_at: DateTime<Utc>,
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

    pub fn create_file(name: String, size: u64) -> Self {
        let now = Utc::now();
        Self {
            file_info: FileInfo::File(File::new(name, size)),
            children: BTreeMap::new(),
            version: 1,
            created_at: now,
            modified_at: now,
        }
    }

    pub fn traverse(&mut self, path: &str) -> Result<&mut Self, Box<dyn Error>> {
        let mut curr = self;
        let path_items = path.split_terminator('/').collect::<Vec<_>>();
        let mut remaining_path = path_items.get(..).unwrap();
        while !remaining_path.is_empty() {
            match curr.children.get_mut(remaining_path[0]) {
                Some(next) => curr = next,
                None => return Err(format!("path {} doesn't exist", path).into()),
            }
            remaining_path = remaining_path.get(1..).unwrap();
        }
        Ok(curr)
    }

    pub fn insert_dir(&mut self, name: &str) {
        self.children
            .insert(name.into(), FileMetadata::create_dir(name.into()));
    }

    pub fn insert_file(&mut self, name: &str, size: u64) {
        self.children
            .insert(name.into(), FileMetadata::create_file(name.into(), size));
    }

    pub fn print_subtree(&self) -> String {
        match &self.file_info {
            FileInfo::File(file) => file.name.clone(),
            FileInfo::Directory(name) => {
                let mut iter = self.children.values().peekable();
                let mut s = name.clone();
                while let Some(child) = iter.next() {
                    let prefix = if iter.peek().is_some() { "├" } else { "└" };
                    let subtree = child.print_subtree();
                    let mut lines_iter = subtree.lines();
                    s.push_str(&format!("\n{:─<2} {}", prefix, lines_iter.next().unwrap()));
                    for l in lines_iter {
                        s.push_str(&format!("\n{:<2} {}", "│", l));
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
    #[serde(with = "custom_uuid")]
    pub id: Uuid,
    pub name: String,
    pub size: u64,
    pub num_of_chunks: u16,
    #[serde(default)]
    pub num_of_completed_chunks: u16,
    #[serde(default = "FileStatus::default")]
    pub status: FileStatus,
}
impl File {
    pub fn new(name: String, size: u64) -> File {
        File {
            id: Uuid::from_str(&uuid::Uuid::new_v4().to_string()).unwrap(),
            name,
            size,
            num_of_chunks: (size / CHUNK_SIZE + 1) as u16,
            num_of_completed_chunks: 0,
            status: FileStatus::Started,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Copy)]
pub struct Chunk {
    #[serde(with = "custom_uuid")]
    pub id: Uuid,
    #[serde(with = "custom_uuid")]
    pub file_id: Uuid,
    #[serde(with = "custom_uuid")]
    pub server_id: Uuid,
    pub file_part_num: u16,
}
impl Chunk {
    pub fn new(file_id: Uuid, server_id: Uuid, file_part_num: u16) -> Chunk {
        Chunk {
            id: Uuid::from_str(&uuid::Uuid::new_v4().to_string()).unwrap(),
            file_id,
            server_id,
            file_part_num,
        }
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
            trie.children.get("dir1").unwrap().file_info,
            FileInfo::Directory("dir1".into())
        );
        trie.insert_dir("dir2");
        assert_eq!(trie.children.len(), 2);
        assert_eq!(
            trie.children.get("dir1").unwrap().file_info,
            FileInfo::Directory("dir1".into())
        );
        trie.insert_file("some.zip", 20);
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
        trie.insert_file("some.zip", 20);
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

    fn build() -> Result<FileMetadata, Box<dyn Error>> {
        let mut trie = FileMetadata::create_root();
        trie.insert_dir("dir1");
        trie.insert_dir("dir2");
        trie.insert_file("some.zip", 0);
        let dir2 = trie.traverse("dir2")?;
        dir2.insert_file("test.txt", 10);
        dir2.insert_dir("subdir");
        let subdir = dir2.traverse("subdir")?;
        subdir.insert_dir("tmp");
        subdir.insert_file("file", 100);

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
    fn trie_print_current_dir_test() -> Result<(), Box<dyn Error>> {
        let mut trie = build()?;

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
