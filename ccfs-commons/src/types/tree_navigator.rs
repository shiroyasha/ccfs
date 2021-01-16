use crate::FileMetadata;
use crate::{errors::Error::*, result::CCFSResult};

#[derive(Debug, Clone)]
pub struct TreeNavigator<'a> {
    pub node: &'a FileMetadata,
    pub parent: Option<Box<TreeNavigator<'a>>>,
}

impl<'a> TreeNavigator<'a> {
    pub fn child(self, name: &'a str) -> CCFSResult<TreeNavigator> {
        match self.node.children()?.get(name) {
            Some(child) => Ok(TreeNavigator {
                node: child,
                parent: Some(Box::new(self)),
            }),
            None => Err(NotExist { path: name.into() }.into()),
        }
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
    pub fn child(mut self, name: &str) -> CCFSResult<TreeZipper> {
        match self.node.children_mut() {
            Ok(children) => match children.remove(name) {
                Some(child) => Ok(TreeZipper {
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

    pub fn parent(self) -> CCFSResult<TreeZipper> {
        if let Some(parent_node) = self.parent {
            let TreeZipper {
                node: mut parent_node,
                parent: parent_parent,
            } = *parent_node;
            parent_node
                .children_mut()?
                .insert(self.node.name.clone(), self.node);

            Ok(TreeZipper {
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
    use crate::types::file::tests::build;

    #[test]
    fn navigator_tests() -> CCFSResult<()> {
        let tree = build()?;
        let mut navigator = tree.navigate();
        assert_eq!(navigator.node.name, "/");
        assert!(navigator.parent.is_none());

        navigator = navigator.child("dir2")?;
        assert_eq!(navigator.node.name, "dir2");
        let parent = navigator.parent.clone();
        assert_eq!(parent.unwrap().node.name, "/");

        navigator = navigator.child("subdir")?;
        assert_eq!(navigator.node.name, "subdir");
        let parent = navigator.parent.clone();
        assert_eq!(parent.unwrap().node.name, "dir2");

        let dir1_navigator = navigator.parent.unwrap();
        navigator = *dir1_navigator.parent.unwrap();

        navigator = navigator.child("dir1")?;
        assert_eq!(navigator.node.name, "dir1");
        let parent = navigator.parent.clone();
        assert_eq!(parent.unwrap().node.name, "/");

        let res = navigator.child("file.txt");
        assert_eq!(
            format!("{:?}", res.unwrap_err()),
            "NotExist { path: \"file.txt\" }"
        );
        Ok(())
    }

    #[test]
    fn zipper_tests() -> CCFSResult<()> {
        let tree = build()?;
        let mut zipper = tree.zipper();
        assert_eq!(zipper.node.name, "/");
        assert!(zipper.parent.is_none());

        assert_eq!(zipper.node.print_current_dir()?, "dir1\ndir2\nsome.zip");

        zipper = zipper.child("dir2")?;
        assert_eq!(zipper.node.name, "dir2");
        let parent = zipper.parent.clone();
        assert_eq!(parent.unwrap().node.name, "/");

        zipper.node.name = "dir3".into();
        zipper = zipper.parent()?;
        assert_eq!(zipper.node.print_current_dir()?, "dir1\ndir3\nsome.zip");
        let res = zipper.child("dir2");
        assert_eq!(
            format!("{:?}", res.unwrap_err()),
            "NotExist { path: \"dir2\" }"
        );

        Ok(())
    }
}
