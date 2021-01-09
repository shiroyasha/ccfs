use std::ops::Deref;

#[derive(Clone)]
pub struct Data<T> {
    pub inner: T,
}
impl<T> Data<T> {
    pub fn new(state: T) -> Self {
        Self { inner: state }
    }
}
impl<T> Deref for Data<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
