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
        // SQLite and MySQL use IF NOT EXISTS / INSERT OR IGNORE
        // PostgreSQL uses ON CONFLICT DO NOTHING
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

/// Cleanup expired task locks
/// Should be called periodically (e.g., via a scheduled task)
pub async fn cleanup_expired_task_locks(pool: &AnyPool) -> Result<u64, sqlx::Error> {
    // Cross-database compatible: SQLite and MySQL support DELETE with subquery
    let result = sqlx::query::<sqlx::Any>(
        "DELETE FROM task_locks WHERE expires_at < datetime('now')"
    )
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}
