use async_trait::async_trait;
use serde::Serialize;
use crate::error::QueryError;

#[derive(Debug, Serialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub affected_rows: Option<u64>,
}

#[async_trait]
pub trait RawQueryExecutor: Send + Sync {
    async fn execute_raw_query(&self, query: &str) -> Result<QueryResult, QueryError>;
}