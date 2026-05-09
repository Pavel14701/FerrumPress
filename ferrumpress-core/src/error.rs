use thiserror::Error;

#[derive(Error, Debug)]
pub enum DbError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("duplicate entry: {0}")]
    Duplicate(String),
    #[error("database error: {0}")]
    Internal(String),
}

#[derive(Error, Debug)]
pub enum SessionError {
    #[error("session not found")]
    NotFound,
    #[error("session expired")]
    Expired,
    #[error("storage error: {0}")]
    Storage(String),
}

#[derive(Error, Debug)]
pub enum AuthError {
    #[error("invalid credentials")]
    InvalidCredentials,
    #[error("token expired")]
    TokenExpired,
    #[error("invalid token")]
    InvalidToken,
    #[error("user not found")]
    UserNotFound,
    #[error("internal error: {0}")]
    Internal(String),
}

#[derive(Error, Debug)]
pub enum QueueError {
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("connection error: {0}")]
    Connection(String),
    #[error("timeout")]
    Timeout,
    #[error("unknown error: {0}")]
    Unknown(String),
}

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("object not found: {0}")]
    NotFound(String),
    #[error("upload failed: {0}")]
    UploadFailed(String),
    #[error("download failed: {0}")]
    DownloadFailed(String),
    #[error("configuration error: {0}")]
    Config(String),
}

#[derive(Error, Debug)]
pub enum SearchError {
    #[error("indexing failed: {0}")]
    IndexingFailed(String),
    #[error("search query failed: {0}")]
    QueryFailed(String),
    #[error("connection error: {0}")]
    Connection(String),
}

// A unified application-level error (optional)
#[derive(Error, Debug)]
pub enum AppError {
    #[error("database error: {0}")]
    Db(#[from] DbError),
    #[error("auth error: {0}")]
    Auth(#[from] AuthError),
    #[error("session error: {0}")]
    Session(#[from] SessionError),
    #[error("queue error: {0}")]
    Queue(#[from] QueueError),
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("search error: {0}")]
    Search(#[from] SearchError),
    #[error("configuration error: {0}")]
    Config(String),
    #[error("internal error: {0}")]
    Internal(String),
}

#[derive(Error, Debug)]
pub enum QueryError {
    #[error("only SELECT queries are allowed")]
    NotSelect,
    #[error("table '{0}' is not allowed")]
    TableNotAllowed(String),
    #[error("sql parsing error: {0}")]
    Parse(String),
    #[error("execution error: {0}")]
    Execution(String),
    #[error("not allowed: {0}")]
    NotAllowed(String),
}

#[derive(Error, Debug)]
pub enum CacheError {
    #[error("cache backend error: {0}")]
    Backend(String),
    #[error("serialization error: {0}")]
    Serialization(String),
}


#[derive(Error, Debug)]
pub enum SchemaError {
    #[error("table already exists: {0}")]
    TableExists(String),
    #[error("database error: {0}")]
    Database(String),
}