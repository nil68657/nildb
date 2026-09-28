use std::fmt;
use std::io;

/// Error returned by every engine call. It is `Clone` so an iterator can keep
/// the first error it hit and report it from `status` more than once.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    /// An operating-system call failed; `context` names the file or step.
    Io {
        context: String,
        kind: io::ErrorKind,
        message: String,
    },
    /// Stored bytes failed a checksum or a structural check.
    Corruption(String),
    /// The caller passed something the engine cannot accept.
    InvalidArgument(String),
    /// The database or column family does not exist.
    NotFound(String),
    /// Another process holds the database lock.
    Busy(String),
    /// The engine was closed (or crashed by a test) before the call.
    Closed,
    /// The engine does not implement the operation.
    Unsupported(String),
}

impl Error {
    pub fn corruption(msg: impl Into<String>) -> Self {
        Error::Corruption(msg.into())
    }

    pub fn invalid(msg: impl Into<String>) -> Self {
        Error::InvalidArgument(msg.into())
    }

    pub fn io(context: impl Into<String>, err: &io::Error) -> Self {
        Error::Io {
            context: context.into(),
            kind: err.kind(),
            message: err.to_string(),
        }
    }

    /// Short machine-readable class, used as the prefix of C error strings.
    pub fn code(&self) -> &'static str {
        match self {
            Error::Io { .. } => "io",
            Error::Corruption(_) => "corruption",
            Error::InvalidArgument(_) => "invalid-argument",
            Error::NotFound(_) => "not-found",
            Error::Busy(_) => "busy",
            Error::Closed => "closed",
            Error::Unsupported(_) => "unsupported",
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io {
                context, message, ..
            } => write!(f, "io: {context}: {message}"),
            Error::Corruption(m) => write!(f, "corruption: {m}"),
            Error::InvalidArgument(m) => write!(f, "invalid-argument: {m}"),
            Error::NotFound(m) => write!(f, "not-found: {m}"),
            Error::Busy(m) => write!(f, "busy: {m}"),
            Error::Closed => write!(f, "closed: the engine is closed"),
            Error::Unsupported(m) => write!(f, "unsupported: {m}"),
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Attaches a context string to an `io::Result`.
pub trait IoContext<T> {
    fn ctx<S: Into<String>>(self, context: impl FnOnce() -> S) -> Result<T>;
}

impl<T> IoContext<T> for io::Result<T> {
    fn ctx<S: Into<String>>(self, context: impl FnOnce() -> S) -> Result<T> {
        self.map_err(|e| Error::io(context(), &e))
    }
}
