use async_trait::async_trait;
use sqlx::AnyPool;
use super::{Migration, MigrationVersion};

pub struct CreateTaskLocksTable;

#[async_trait]
impl Migration for CreateTaskLocksTable {
    fn version(&self) -> MigrationVersion {
        MigrationVersion::new("003")
    }
    fn name(&self) -> &str {
        "create_task_locks_table"
    }

    async fn up(&self, pool: &AnyPool) -> Result<(), sqlx::Error> {
        // Cross-database compatible approach
        // Uses IF NOT EXISTS for SQLite/MySQL and standard INSERT for PostgreSQL
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS task_locks (
                task_id TEXT PRIMARY KEY,
                expires_at TEXT NOT NULL
            )"
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn down(&self, pool: &AnyPool) -> Result<(), sqlx::Error> {
        sqlx::query("DROP TABLE IF EXISTS task_locks")
            .execute(pool)
            .await?;
        Ok(())
    }
}
