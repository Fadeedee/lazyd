use std::fmt::{Display, Formatter};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    BadRequest(String),
    Conflict(String),
    NotFound(String),
    Io(std::io::Error),
    Json(serde_json::Error),
    Http(reqwest::Error),
    Remote(String),
    Fanotify(String),
}

impl Error {
    pub fn status_code(&self) -> u16 {
        match self {
            Self::BadRequest(_) => 400,
            Self::NotFound(_) => 404,
            Self::Conflict(_) => 409,
            Self::Io(_) | Self::Json(_) | Self::Http(_) | Self::Remote(_) | Self::Fanotify(_) => {
                500
            }
        }
    }

    pub fn message(&self) -> String {
        match self {
            Self::BadRequest(msg)
            | Self::Conflict(msg)
            | Self::NotFound(msg)
            | Self::Remote(msg)
            | Self::Fanotify(msg) => msg.clone(),
            Self::Io(err) => err.to_string(),
            Self::Json(err) => err.to_string(),
            Self::Http(err) => err.to_string(),
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message())
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

impl From<reqwest::Error> for Error {
    fn from(value: reqwest::Error) -> Self {
        Self::Http(value)
    }
}
