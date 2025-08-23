use std::fmt;

use surrealdb::error::Api;

#[derive(Debug)]
pub enum Error {
    Anyhow(anyhow::Error),
    InvalidFilter,
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Anyhow(e) => write!(f, "{e}"),
            Self::InvalidFilter => write!(f, "Invalid filter"),
        }
    }
}

impl From<Error> for surrealdb::Error {
    fn from(e: Error) -> Self {
        surrealdb::Error::Api(Api::InvalidRequest(e.to_string()))
    }
}
