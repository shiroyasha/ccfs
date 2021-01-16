use std::collections::VecDeque;

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
            items: VecDeque::from(vec![item]),
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
