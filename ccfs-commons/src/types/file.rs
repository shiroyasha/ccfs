use crate::{errors::Error::*, result::CCFSResult};
use crate::{BFSPathsIter, BFSTreeIter, DFSTreeIter, TreeNavigator, TreeZipper, ROOT_DIR};
use chrono::serde::ts_nanoseconds;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;
use uuid::Uuid;

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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FileMetadata {
    pub name: String,
    pub file_info: FileInfo,
    pub version: usize,
    #[serde(with = "ts_nanoseconds")]
    pub created_at: DateTime<Utc>,
    #[serde(with = "ts_nanoseconds")]
    pub modified_at: DateTime<Utc>,
}

impl Default for FileMetadata {
    fn default() -> Self {
        Self::create_root()
    }
}

impl FileMetadata {
    pub fn create_root() -> Self {
        Self::create_dir(ROOT_DIR.into())
    }

    pub fn create_dir(name: String) -> Self {
        let now = Utc::now();
        Self {
            name,
            file_info: FileInfo::Directory {
                children: Default::default(),
            },
            version: 1,
            created_at: now,
            modified_at: now,
        }
    }

    pub fn create_file(name: String, size: u64, chunks: Vec<Uuid>) -> Self {
        let now = Utc::now();
        Self {
            name,
            file_info: FileInfo::new_file(size, chunks),
            version: 1,
            created_at: now,
            modified_at: now,
        }
    }

    pub fn children(&self) -> CCFSResult<&BTreeMap<String, FileMetadata>> {
        if let FileInfo::Directory { ref children } = self.file_info {
            Ok(children)
        } else {
            let path = PathBuf::from(&self.name);
            Err(NotADir { path }.into())
        }
    }

    pub fn children_mut(&mut self) -> CCFSResult<&mut BTreeMap<String, FileMetadata>> {
        if let FileInfo::Directory { ref mut children } = self.file_info {
            Ok(children)
        } else {
            let path = PathBuf::from(&self.name);
            Err(NotADir { path }.into())
        }
    }

    pub fn chunks(&self) -> CCFSResult<&Vec<Uuid>> {
        if let FileInfo::File { ref chunks, .. } = self.file_info {
            Ok(chunks)
        } else {
            let path = PathBuf::from(&self.name);
            Err(NotAFile { path }.into())
        }
    }

    pub fn traverse<'a>(&'a self, target: &'a str) -> CCFSResult<&Self> {
        let mut curr = self.navigate();
        if !target.is_empty() {
            let skip = if target.starts_with(ROOT_DIR) { 1 } else { 0 };
            for segment in target.split_terminator('/').skip(skip) {
                curr = curr.child(segment)?;
            }
        }
        Ok(curr.node)
    }

    pub fn traverse_mut(&mut self, target: &str) -> CCFSResult<&mut Self> {
        let mut curr = self;
        if !target.is_empty() {
            let path = PathBuf::from(target);
            let skip = if target.starts_with(ROOT_DIR) { 1 } else { 0 };
            for segment in target.split_terminator('/').skip(skip) {
                match curr.file_info {
                    FileInfo::File { .. } => return Err(NotExist { path: path.clone() }.into()),
                    _ => {
                        curr = curr
                            .children_mut()?
                            .get_mut(segment)
                            .ok_or_else(|| NotExist { path: path.clone() })?
                    }
                }
            }
        }
        Ok(curr)
    }

    pub fn insert_dir(&mut self, name: &str) -> CCFSResult<()> {
        self.children_mut()?
            .insert(name.into(), Self::create_dir(name.into()));
        Ok(())
    }

    pub fn insert_file(&mut self, name: &str, size: u64, chunks: Vec<Uuid>) -> CCFSResult<()> {
        self.children_mut()?
            .insert(name.into(), Self::create_file(name.into(), size, chunks));
        Ok(())
    }

    pub fn print_subtree(&self) -> String {
        let mut s = self.name.to_string();
        if let FileInfo::Directory { children } = &self.file_info {
            let mut iter = children.values().peekable();
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
        }
        s
    }

    pub fn print_current_dir(&self) -> CCFSResult<String> {
        let mut s = String::new();
        let children = &self.children()?;
        let mut iter = children.values().peekable();
        while let Some(child) = iter.next() {
            let has_next = iter.peek().is_some();
            s.push_str(&child.name);
            if has_next {
                s.push('\n');
            }
        }
        Ok(s)
    }

    pub fn dfs_iter(&self) -> DFSTreeIter {
        DFSTreeIter::new(self)
    }

    pub fn bfs_iter(&self) -> BFSTreeIter {
        BFSTreeIter::new(self)
    }

    pub fn bfs_paths_iter(&self) -> BFSPathsIter {
        BFSPathsIter::new(self)
    }

    pub fn navigate(&'_ self) -> TreeNavigator {
        TreeNavigator {
            node: self,
            parent: None,
        }
    }

    pub fn zipper(self) -> TreeZipper {
        TreeZipper {
            node: self,
            parent: None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum FileInfo {
    Directory {
        children: BTreeMap<String, FileMetadata>,
    },
    File {
        id: Uuid,
        size: u64,
        chunks: Vec<Uuid>,
        #[serde(default)]
        num_of_completed_chunks: usize,
        #[serde(default = "FileStatus::default")]
        status: FileStatus,
    },
}
impl FileInfo {
    pub fn new_file(size: u64, chunks: Vec<Uuid>) -> Self {
        Self::File {
            id: Uuid::new_v4(),
            size,
            chunks,
            num_of_completed_chunks: 0,
            status: FileStatus::Started,
        }
    }
}

#[cfg(test)]
pub mod tests {
    use crate::test_utils::{add_dir2, build_tree};

    use super::*;
    use std::str::FromStr;

    #[test]
    fn tree_insert_test() -> CCFSResult<()> {
        let mut tree = FileMetadata::create_root();
        tree.insert_dir("dir1")?;
        assert_eq!(tree.children()?.len(), 1);
        assert_eq!(tree.children()?.get("dir1").unwrap().name, "dir1");
        tree.insert_dir("dir2")?;

        assert_eq!(tree.children()?.len(), 2);
        assert_eq!(tree.children()?.get("dir2").unwrap().name, "dir2");
        tree.insert_file(
            "some.zip",
            20,
            vec![Uuid::from_str("ec73d743-050b-4f52-992a-d1102340d739").unwrap()],
        )?;
        assert_eq!(tree.children()?.len(), 3);
        let file = &tree.children()?.get("some.zip").unwrap();
        assert!(matches!(file.file_info, FileInfo::File { size: 20, .. }));
        Ok(())
    }

    #[test]
    fn tree_traverse_test() -> CCFSResult<()> {
        let mut tree = FileMetadata::create_root();
        tree.insert_dir("dir1")?;
        tree.insert_dir("dir2")?;
        tree.insert_file(
            "some.zip",
            20,
            vec![Uuid::from_str("ec73d743-050b-4f52-992a-d1102340d739").unwrap()],
        )?;
        let dir1 = tree.traverse("dir1")?;
        assert!(matches!(dir1.file_info, FileInfo::Directory { .. }));
        assert_eq!(dir1.name, "dir1");
        assert_eq!(
            dir1.traverse("subdir").unwrap_err().to_string(),
            "Path 'subdir' doesn't exist"
        );
        assert_eq!(
            dir1.traverse("dir1/subdir").unwrap_err().to_string(),
            "Path 'dir1' doesn't exist"
        );
        let dir2 = tree.traverse("dir2")?;
        assert!(matches!(dir2.file_info, FileInfo::Directory { .. }));
        assert_eq!(dir2.name, "dir2");
        let file = tree.traverse("some.zip")?;
        assert!(matches!(file.file_info, FileInfo::File { .. }));
        assert_eq!(file.name, "some.zip");

        Ok(())
    }

    #[test]
    fn tree_print_subtree_test() -> CCFSResult<()> {
        let tree = build_tree()?;
        let expected = std::fs::read_to_string("expected-tree.txt").unwrap();
        assert_eq!(tree.print_subtree(), expected);
        Ok(())
    }

    #[test]
    fn tree_print_single_dir_subtree_test() -> CCFSResult<()> {
        let mut tree = FileMetadata::create_root();
        add_dir2(&mut tree)?;
        let expected = std::fs::read_to_string("expected-single-dir-tree.txt").unwrap();
        assert_eq!(tree.print_subtree(), expected);
        Ok(())
    }

    #[test]
    fn tree_print_current_dir_test() -> CCFSResult<()> {
        let tree = build_tree()?;
        assert_eq!(tree.print_current_dir()?, "dir1\ndir2\nsome.zip");
        assert_eq!(tree.traverse("dir1")?.print_current_dir()?, "");
        assert_eq!(
            tree.traverse("dir2")?.print_current_dir()?,
            "subdir\ntest.txt"
        );
        assert_eq!(
            tree.traverse("dir2/subdir")?.print_current_dir()?,
            "file\ntmp"
        );
        Ok(())
    }
}
