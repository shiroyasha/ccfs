use crate::FileMetadata;
use crate::{errors::Error::*, result::CCFSResult};
use std::rc::Rc;

#[derive(Debug)]
pub struct TreeNavigator<'a> {
    pub node: &'a FileMetadata,
    pub parent: Option<Rc<TreeNavigator<'a>>>,
}

impl<'a> TreeNavigator<'a> {
    pub fn child(self, name: &'a str) -> CCFSResult<TreeNavigator> {
        match self.node.children()?.get(name) {
            Some(child) => Ok(TreeNavigator {
                node: child,
                parent: Some(Rc::new(self)),
            }),
            None => Err(NotExist { path: name.into() }.into()),
        }
    }
}

#[derive(Debug)]
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
