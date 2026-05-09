// ferrumpress-db/src/schema.rs
use async_trait::async_trait;
use barrel::{Migration, executor::Executor};
use sqlx::AnyPool;
use std::sync::Arc;
use tokio::sync::Mutex;
use ferrumpress_core::schema::{Schema, TableBuilder};
use ferrumpress_core::error::SchemaError;
use crate::schema_builder::TableBuilderImpl;
use crate::registry::MODEL_REGISTRY;

pub struct SchemaImpl {
    pool: AnyPool,
}

impl SchemaImpl {
    pub fn new(pool: AnyPool) -> Self { Self { pool } }

    async fn execute_barrel_migration(&self, migration: &Migration) -> Result<(), SchemaError> {
        let sql = migration.make_sql(); // возвращает Vec<String>
        for stmt in sql {
            sqlx::query(&stmt)
                .execute(&self.pool)
                .await
                .map_err(|e| SchemaError::Database(e.to_string()))?;
        }
        Ok(())
    }
}

#[async_trait]
impl Schema for SchemaImpl {
    async fn create_table<F>(&self, table_name: &str, build: F) -> Result<(), SchemaError>
    where F: FnOnce(&mut dyn TableBuilder) + Send + Sync
    {
        let mut builder = TableBuilderImpl::new(table_name);
        build(&mut builder);
        let table = builder.into_table();

        let mut migration = Migration::new();
        migration.create_table_if_not_exists(table);
        self.execute_barrel_migration(&migration).await?;

        // Регистрируем модель (упрощённо)
        let model = Arc::new(GeneratedModel {
            table_name: table_name.to_string(),
            columns: vec![], // можно заполнить, но для краткости опустим
            primary_keys: vec![],
        });
        MODEL_REGISTRY.lock().await.insert(table_name.to_string(), model);

        Ok(())
    }

    async fn drop_table(&self, table_name: &str) -> Result<(), SchemaError> {
        let mut migration = Migration::new();
        migration.drop_table_if_exists(table_name);
        self.execute_barrel_migration(&migration).await?;
        MODEL_REGISTRY.lock().await.remove(table_name);
        Ok(())
    }
}