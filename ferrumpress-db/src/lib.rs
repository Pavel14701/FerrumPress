#[cfg(feature = "sqlite")]
pub mod sqlite;
#[cfg(feature = "sqlite")]
pub use sqlite::SqliteDb;

#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "postgres")]
pub use postgres::PostgresDb;

#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(feature = "mysql")]
pub use mysql::MysqlDb;

// Пользовательские запросы
pub mod query_executor;
pub use query_executor::DbQueryExecutor;

// Миграции, реестры, сервисы
#[cfg(feature = "migrations")]
pub mod registry;
#[cfg(feature = "migrations")]
pub mod schema;
#[cfg(feature = "migrations")]
pub mod schema_builder;
#[cfg(feature = "migrations")]
pub mod type_mapping;
#[cfg(feature = "migrations")]
pub mod generic_service;
#[cfg(feature = "discover")]
pub mod schema_discovery;