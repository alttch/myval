#![ doc = include_str!( concat!( env!( "CARGO_MANIFEST_DIR" ), "/", "README.md" ) ) ]
use chrono::Local;
use std::fmt;

mod df;
pub use df::{Chunk, DataFrame, DataType, Metadata, Schema, Series, TimeUnit};

pub mod db;

#[derive(Debug)]
pub enum Error {
    OutOfBounds,
    RowsNotMatch,
    ColsNotMatch,
    TypeMismatch,
    Arrow(arrow2::error::Error),
    NotFound(String),
    Unimplemented(String),
    Other(String),
    #[cfg(feature = "sqlx")]
    Database(sqlx::Error),
    #[cfg(feature = "serde_json")]
    Json(serde_json::Error),
}

impl From<arrow2::error::Error> for Error {
    #[inline]
    fn from(err: arrow2::error::Error) -> Self {
        Error::Arrow(err)
    }
}

impl Error {
    #[inline]
    pub fn other(err: impl fmt::Display) -> Self {
        Self::Other(err.to_string())
    }
}

impl From<fmt::Error> for Error {
    #[inline]
    fn from(err: fmt::Error) -> Self {
        Error::other(err)
    }
}

#[cfg(feature = "sqlx")]
impl From<sqlx::Error> for Error {
    fn from(err: sqlx::Error) -> Self {
        Error::Database(err)
    }
}

#[cfg(feature = "serde_json")]
impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Json(err)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::OutOfBounds => write!(f, "index out of bounds"),
            Error::RowsNotMatch => write!(f, "row count does not match"),
            Error::ColsNotMatch => write!(f, "column count does not match"),
            Error::TypeMismatch => write!(f, "type does not match"),
            Error::Arrow(e) => write!(f, "{}", e),
            Error::NotFound(s) => write!(f, "not found: {}", s),
            Error::Unimplemented(s) => write!(f, "feature/type not implemented: {}", s),
            Error::Other(e) => write!(f, "{}", e),
            #[cfg(feature = "sqlx")]
            Error::Database(e) => write!(f, "database error: {}", e),
            #[cfg(feature = "serde_json")]
            Error::Json(e) => write!(f, "de/serialize error: {}", e),
        }
    }
}

impl std::error::Error for Error {}

#[derive(Clone)]
pub enum TimeZone {
    Local,
    Custom(String),
    No,
}

impl From<TimeZone> for Option<String> {
    fn from(tz: TimeZone) -> Self {
        match tz {
            TimeZone::Local => Some(Local::now().format("%Z").to_string()),
            TimeZone::Custom(s) => Some(s),
            TimeZone::No => None,
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
struct Time {
    sec: u64,
    nsec: u64,
}

impl Time {
    #[allow(clippy::cast_sign_loss)]
    #[allow(clippy::cast_possible_truncation)]
    #[inline]
    fn from_timestamp(timestamp: f64) -> Self {
        Self {
            sec: timestamp.trunc() as u64,
            nsec: (timestamp.fract() * 1_000_000_000_f64) as u64,
        }
    }
    #[inline]
    fn timestamp_ns(&self) -> u64 {
        self.sec * 1_000_000_000 + self.nsec
    }
    #[inline]
    fn timestamp_us(&self) -> u64 {
        self.sec * 1_000_000 + self.nsec / 1_000
    }
    #[inline]
    fn timestamp_ms(&self) -> u64 {
        self.sec * 1_000 + self.nsec / 1_000_000
    }
}

// concat multiple data frames
pub fn concat(data_frames: &[&DataFrame]) -> Result<DataFrame, Error> {
    if data_frames.is_empty() {
        Ok(DataFrame::new0())
    } else {
        let cols = data_frames[0].data().len();
        let fields = data_frames[0].fields().to_owned();
        let meta = data_frames[0].metadata().to_owned();
        let mut data = Vec::with_capacity(cols);
        for c in 0..cols {
            let mut serie = Vec::with_capacity(data_frames.len());
            for df in data_frames {
                serie.push(df.data().get(c).ok_or(Error::ColsNotMatch)?.as_ref());
            }
            let c_data = arrow2::compute::concatenate::concatenate(&serie)?;
            data.push(c_data);
        }
        DataFrame::from_parts(fields, data, Some(meta))
    }
}
