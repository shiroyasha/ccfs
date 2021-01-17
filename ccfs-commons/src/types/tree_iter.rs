use std::collections::VecDeque;
use std::path::PathBuf;

use crate::{FileInfo, FileMetadata};

pub struct DFSTreeIter<'a> {
    items: Vec<&'a FileMetadata>,
}

impl<'a> DFSTreeIter<'a> {
    pub fn new(item: &'a FileMetadata) -> Self {
        Self { items: vec![item] }
    }
}

impl<'a> Iterator for DFSTreeIter<'a> {
    type Item = &'a FileMetadata;

    fn next(&mut self) -> Option<Self::Item> {
        match self.items.pop() {
            Some(item) => {
                if let FileInfo::Directory { ref children } = item.file_info {
                    self.items.extend(children.values());
                }
                Some(&item)
            }
            None => None,
        }
    }
}

pub struct BFSTreeIter<'a> {
    items: VecDeque<&'a FileMetadata>,
}

impl<'a> BFSTreeIter<'a> {
    pub fn new(item: &'a FileMetadata) -> Self {
        Self {
            items: vec![item].into(),
        }
    }
}

impl<'a> Iterator for BFSTreeIter<'a> {
    type Item = &'a FileMetadata;

    fn next(&mut self) -> Option<Self::Item> {
        match self.items.pop_front() {
            Some(item) => {
                if let FileInfo::Directory { ref children } = item.file_info {
                    self.items.extend(children.values());
                }
                Some(&item)
            }
            None => None,
        }
    }
}

/// An iterator that returns the currently visited items parent directory path
pub struct BFSPathsIter<'a> {
    items: VecDeque<&'a FileMetadata>,
    paths: VecDeque<PathBuf>,
}

impl<'a> BFSPathsIter<'a> {
    pub fn new(item: &'a FileMetadata) -> Self {
        Self {
            items: vec![item].into(),
            paths: vec![PathBuf::new()].into(),
        }
    }
}

impl<'a> Iterator for BFSPathsIter<'a> {
    type Item = PathBuf;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.items.pop_front(), self.paths.pop_front()) {
            (Some(item), Some(path)) => {
                if let FileInfo::Directory { ref children } = item.file_info {
                    self.items.extend(children.values());
                    self.paths
                        .extend(children.values().map(|_| path.join(&item.name)));
                }
                Some(path)
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::result::CCFSResult;
    use crate::types::file::tests::build;
    use crate::ROOT_DIR;

    #[test]
    fn dfs_iter_test() -> CCFSResult<()> {
        let tree = build()?;
        let mut iter = tree.dfs_iter();
        assert_eq!(iter.next().unwrap().name, ROOT_DIR);
        assert_eq!(iter.next().unwrap().name, "some.zip");
        assert_eq!(iter.next().unwrap().name, "dir2");
        assert_eq!(iter.next().unwrap().name, "test.txt");
        assert_eq!(iter.next().unwrap().name, "subdir");
        assert_eq!(iter.next().unwrap().name, "tmp");
        assert_eq!(iter.next().unwrap().name, "file");
        assert_eq!(iter.next().unwrap().name, "dir1");
        Ok(())
    }

    #[test]
    fn bfs_iter_test() -> CCFSResult<()> {
        let tree = build()?;
        let mut iter = tree.bfs_iter();
        assert_eq!(iter.next().unwrap().name, ROOT_DIR);
        assert_eq!(iter.next().unwrap().name, "dir1");
        assert_eq!(iter.next().unwrap().name, "dir2");
        assert_eq!(iter.next().unwrap().name, "some.zip");
        assert_eq!(iter.next().unwrap().name, "subdir");
        assert_eq!(iter.next().unwrap().name, "test.txt");
        assert_eq!(iter.next().unwrap().name, "file");
        assert_eq!(iter.next().unwrap().name, "tmp");
        Ok(())
    }

    #[test]
    fn bfs_paths_iter_test() -> CCFSResult<()> {
        let tree = build()?;
        let mut iter = tree.bfs_paths_iter();
        assert_eq!(iter.next().unwrap(), PathBuf::from(""));
        assert_eq!(iter.next().unwrap(), PathBuf::from("/"));
        assert_eq!(iter.next().unwrap(), PathBuf::from("/"));
        assert_eq!(iter.next().unwrap(), PathBuf::from("/"));
        assert_eq!(iter.next().unwrap(), PathBuf::from("/dir2"));
        assert_eq!(iter.next().unwrap(), PathBuf::from("/dir2"));
        assert_eq!(iter.next().unwrap(), PathBuf::from("/dir2/subdir"));
        assert_eq!(iter.next().unwrap(), PathBuf::from("/dir2/subdir"));
        Ok(())
    }
}
