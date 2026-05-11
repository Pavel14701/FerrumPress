use async_trait::async_trait;
use crate::traits::ProcessedVariant;

#[async_trait]
pub trait ImageProcessor: Send + Sync {
    async fn process_image(&self, data: Vec<u8>, source_mime: &str) -> Result<Vec<ProcessedVariant>, String>;
}
