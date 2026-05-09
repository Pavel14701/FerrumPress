use async_trait::async_trait;
use chrono::Utc;
use sqlx::AnyPool;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct MigrationVersion(String);

impl MigrationVersion {
    pub fn new(version: &str) -> Self {
        Self(version.to_string())
    }
}

#[async_trait]
pub trait Migration: Send + Sync {
    fn version(&self) -> MigrationVersion;
    fn name(&self) -> &str;
    async fn up(&self, pool: &AnyPool) -> Result<(), sqlx::Error>;
    async fn down(&self, pool: &AnyPool) -> Result<(), sqlx::Error>;
}

pub struct Migrator {
    pool: AnyPool,
    migrations: BTreeMap<MigrationVersion, Arc<dyn Migration>>,
}

impl Migrator {
    pub fn new(pool: AnyPool) -> Self {
        Self {
            pool,
            migrations: BTreeMap::new(),
        }
    }

    pub fn add_migration(&mut self, migration: Arc<dyn Migration>) {
        self.migrations.insert(migration.version(), migration);
    }

    /// Создаёт таблицу `_migrations`, если её нет
    pub async fn init(&self) -> Result<(), sqlx::Error> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS _migrations (
                version TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                applied_at TEXT NOT NULL
            )"
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Применяет все ещё не применённые миграции
    pub async fn up(&self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        self.init().await?;
        let applied_versions = self.get_applied_versions().await?;
        let mut applied = Vec::new();

        for (version, migration) in &self.migrations {
            if !applied_versions.contains(version) {
                println!("Applying migration {} - {}", version.0, migration.name());
                migration.up(&self.pool).await?;
                self.record_migration(version, migration.name()).await?;
                applied.push(version.0.clone());
            }
        }
        Ok(applied)
    }

    /// Откатывает последнюю применённую миграцию
    pub async fn down_last(&self) -> Result<Option<String>, Box<dyn std::error::Error>> {
        let applied_versions = self.get_applied_versions().await?;
        if let Some(last_version) = applied_versions.last() {
            if let Some(migration) = self.migrations.get(last_version) {
                println!("Rolling back migration {} - {}", last_version.0, migration.name());
                migration.down(&self.pool).await?;
                self.remove_migration_record(last_version).await?;
                return Ok(Some(last_version.0.clone()));
            }
        }
        Ok(None)
    }

    async fn get_applied_versions(&self) -> Result<Vec<MigrationVersion>, sqlx::Error> {
        let rows = sqlx::query_as::<_, (String,)>(
            "SELECT version FROM _migrations ORDER BY version ASC"
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(v,)| MigrationVersion(v)).collect())
    }

    async fn record_migration(&self, version: &MigrationVersion, name: &str) -> Result<(), sqlx::Error> {
        let now = Utc::now().to_rfc3339();
        sqlx::query("INSERT INTO _migrations (version, name, applied_at) VALUES ($1, $2, $3)")
            .bind(&version.0)
            .bind(name)
            .bind(now)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn remove_migration_record(&self, version: &MigrationVersion) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM _migrations WHERE version = $1")
            .bind(&version.0)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}