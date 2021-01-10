use crate::errors::CCFSResponseError;

pub type CCFSResult<T, E = CCFSResponseError> = Result<T, E>;
