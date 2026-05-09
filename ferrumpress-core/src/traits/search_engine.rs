use async_trait::async_trait;
use crate::error::SearchError;

#[async_trait]
pub trait SearchEngine: Send + Sync {
    async fn index_post(&self, post_id: &str, title: &str, content: &str) -> Result<(), SearchError>;
    async fn search_posts(&self, query: &str, limit: usize) -> Result<Vec<String>, SearchError>; // возвращает ID постов
    async fn delete_post_index(&self, post_id: &str) -> Result<(), SearchError>;
}