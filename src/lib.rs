use chrono::Local;
use std::fmt;

pub mod df;

#[derive(Debug)]
pub enum Error {
    OutOfBounds,
    RowsNotMatch,
    Unimplemented(String),
    #[cfg(feature = "sqlx")]
    Database(sqlx::Error),
}

#[cfg(feature = "sqlx")]
impl From<sqlx::Error> for Error {
    fn from(err: sqlx::Error) -> Self {
        Error::Database(err)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::OutOfBounds => write!(f, "index out of bounds"),
            Error::RowsNotMatch => write!(f, "row count does not match"),
            Error::Unimplemented(s) => write!(f, "feature/type not implemented: {}", s),
            #[cfg(feature = "sqlx")]
            Error::Database(e) => write!(f, "database error: {}", e),
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
