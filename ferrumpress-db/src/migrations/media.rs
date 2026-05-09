use async_trait::async_trait;
use sqlx::AnyPool;
use super::{Migration, MigrationVersion};

pub struct CreateMediaTable;

#[async_trait]
impl Migration for CreateMediaTable {
    fn version(&self) -> MigrationVersion {
        MigrationVersion::new("002")  // или следующая по порядку
    }
    fn name(&self) -> &str {
        "create_media_table"
    }

    async fn up(&self, pool: &AnyPool) -> Result<(), sqlx::Error> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS media (
                id UUID PRIMARY KEY,
                original_name TEXT NOT NULL,
                storage_strategy TEXT NOT NULL,
                storage_key TEXT NOT NULL,
                mime_type TEXT NOT NULL,
                size BIGINT NOT NULL,
                width INTEGER,
                height INTEGER,
                status TEXT NOT NULL DEFAULT 'pending',
                variants TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )"
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn down(&self, pool: &AnyPool) -> Result<(), sqlx::Error> {
        sqlx::query("DROP TABLE IF EXISTS media")
            .execute(pool)
            .await?;
        Ok(())
    }
}