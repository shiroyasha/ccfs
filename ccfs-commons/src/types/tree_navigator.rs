use crate::FileMetadata;
use crate::{errors::Error::*, result::CCFSResult};
use std::rc::Rc;

#[derive(Debug)]
pub struct NavigableNode<'a> {
    pub node: &'a FileMetadata,
    pub parent: Option<Rc<NavigableNode<'a>>>,
}

impl<'a> NavigableNode<'a> {
    pub fn child(self, name: &'a str) -> CCFSResult<NavigableNode> {
        match self.node.children()?.get(name) {
            Some(child) => Ok(NavigableNode {
                node: child,
                parent: Some(Rc::new(self)),
            }),
            None => Err(NotExist { path: name.into() }.into()),
        }
    }
}
