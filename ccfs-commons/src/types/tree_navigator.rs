use crate::{errors::Error::*, result::CCFSResult};
use crate::{FileMetadata, CURR_DIR, PREV_DIR, ROOT_DIR};

#[derive(Debug, Clone)]
pub struct TreeNavigator<'a> {
    pub node: &'a FileMetadata,
    pub parent: Option<Box<TreeNavigator<'a>>>,
}

impl<'a> TreeNavigator<'a> {
    pub fn child(self, name: &'a str) -> CCFSResult<Self> {
        match self.node.children()?.get(name) {
            Some(child) => Ok(Self {
                node: child,
                parent: Some(Box::new(self)),
            }),
            None => Err(NotExist { path: name.into() }.into()),
        }
    }

    pub fn parent(self) -> Self {
        if let Some(parent_node) = self.parent {
            *parent_node
        } else {
            // stay in same dir when attempting to .. from root
            self
        }
    }

    pub fn get_path(&self) -> String {
        let mut curr = self.clone();
        let mut path = Vec::new();
        while curr.parent.is_some() {
            path.push(&curr.node.name);
            curr = curr.parent().clone();
        }
        path.iter()
            .rev()
            .fold(curr.node.name.clone(), |mut acc, p| {
                if acc != ROOT_DIR {
                    acc.push('/');
                }
                acc.push_str(p);
                acc
            })
    }

    pub fn move_to(self, next_dir: &'a str) -> CCFSResult<Self> {
        let next = match next_dir {
            s if s == CURR_DIR => self,
            s if s == PREV_DIR => self.parent(),
            _ => self.child(next_dir)?,
        };
        Ok(next)
    }
}

#[derive(Debug, Clone)]
pub struct TreeZipper {
    pub node: FileMetadata,
    pub parent: Option<Box<TreeZipper>>,
}

/// TreeZipper is a mutable tree navigator
///
/// Note: In order to be mutable, it takes ownership of the item (moves it out
/// of the children vec), therefore the `finish` fn must be called to reconstruct
/// the tree
impl TreeZipper {
    pub fn child(mut self, name: &str) -> CCFSResult<Self> {
        match self.node.children_mut() {
            Ok(children) => match children.remove(name) {
                Some(child) => Ok(Self {
                    node: child,
                    parent: Some(Box::new(self)),
                }),
                None => {
                    self.finish()?;
                    Err(NotExist { path: name.into() }.into())
                }
            },
            Err(err) => {
                self.finish()?;
                Err(err)
            }
        }
    }

    pub fn parent(self) -> CCFSResult<Self> {
        if let Some(parent_node) = self.parent {
            let Self {
                node: mut parent_node,
                parent: parent_parent,
            } = *parent_node;
            parent_node
                .children_mut()?
                .insert(self.node.name.clone(), self.node);

            Ok(Self {
                node: parent_node,
                parent: parent_parent,
            })
        } else {
            // stay in same dir when attempting to .. from root
            Ok(self)
        }
    }

    pub fn finish(mut self) -> CCFSResult<FileMetadata> {
        while self.parent.is_some() {
            self = self.parent()?;
        }

        Ok(self.node)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::build_tree;

    #[test]
    fn navigator_tests() -> CCFSResult<()> {
        let tree = build_tree()?;
        let mut navigator = tree.navigate();
        assert_eq!(navigator.get_path(), ROOT_DIR);
        assert!(navigator.parent.is_none());

        navigator = navigator.child("dir2")?;
        assert_eq!(navigator.get_path(), "/dir2");
        let parent = navigator.parent.clone();
        assert_eq!(parent.unwrap().node.name, ROOT_DIR);

        navigator = navigator.child("subdir")?;
        assert_eq!(navigator.get_path(), "/dir2/subdir");
        let parent = navigator.parent.clone();
        assert_eq!(parent.unwrap().node.name, "dir2");

        navigator = navigator.parent().parent();
        assert_eq!(navigator.get_path(), ROOT_DIR);

        navigator = navigator.child("dir1")?;
        assert_eq!(navigator.get_path(), "/dir1");
        let parent = navigator.parent.clone();
        assert_eq!(parent.unwrap().node.name, ROOT_DIR);

        let res = navigator.child("file.txt");
        assert_eq!(
            res.unwrap_err().to_string(),
            "Path 'file.txt' doesn't exist"
        );
        Ok(())
    }

    #[test]
    fn zipper_tests() -> CCFSResult<()> {
        let tree = build_tree()?;
        let mut zipper = tree.zipper();
        assert_eq!(zipper.node.name, ROOT_DIR);
        assert!(zipper.parent.is_none());

        assert_eq!(zipper.node.print_current_dir()?, "dir1\ndir2\nsome.zip");

        zipper = zipper.child("dir2")?;
        assert_eq!(zipper.node.name, "dir2");
        let parent = zipper.parent.clone();
        assert_eq!(parent.unwrap().node.name, ROOT_DIR);

        zipper.node.name = "dir3".into();
        zipper = zipper.parent()?;
        assert_eq!(zipper.node.print_current_dir()?, "dir1\ndir3\nsome.zip");
        let res = zipper.child("dir2");
        assert_eq!(res.unwrap_err().to_string(), "Path 'dir2' doesn't exist");

        Ok(())
    }
}
