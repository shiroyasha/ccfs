use crate::{FileInfo, FileMetadata};

pub struct TreeIter<'a> {
    items: Vec<&'a FileMetadata>,
}

impl<'a> TreeIter<'a> {
    pub fn new(item: &'a FileMetadata) -> Self {
        Self { items: vec![item] }
    }
}

impl<'a> Iterator for TreeIter<'a> {
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
