#[cfg(feature = "discover")]
use sea_schema::discovery::{SchemaDiscovery, DiscoveryResult};
#[cfg(feature = "discover")]
use sqlx::AnyPool;
#[cfg(feature = "discover")]
use ferrumpress_core::model::{ColumnKind, ColumnInfo};
#[cfg(feature = "discover")]
use crate::schema_builder::TableDefinition;

#[cfg(feature = "discover")]
pub async fn discover_tables(pool: &AnyPool) -> Result<Vec<TableDefinition>, Box<dyn std::error::Error>> {
    // Определяем тип базы данных по URL пула (можно передать явно)
    let url = pool.connect_options().get_url().to_string();
    let dialect = detect_dialect(&url)?;

    let mut discovery = SchemaDiscovery::new(dialect, pool.acquire().await?);
    let result: DiscoveryResult = discovery
        .discover()
        .await?;

    let mut tables = Vec::new();
    for table in result.tables {
        let mut columns = Vec::new();
        let mut primary_keys = Vec::new();

        for col in table.columns {
            let kind = map_column_type(&col.col_type)?;
            columns.push(ColumnInfo {
                name: col.name.clone(),
                kind,
                nullable: col.nullable,
                unique: col.unique.unwrap_or(false),
                default: col.default.map(|d| d.to_string()),
            });
            if col.primary_key {
                primary_keys.push(col.name.clone());
            }
        }

        tables.push(TableDefinition {
            name: table.name,
            columns,
            primary_key: if primary_keys.is_empty() { None } else { Some(primary_keys) },
        });
    }

    Ok(tables)
}

#[cfg(feature = "discover")]
fn detect_dialect(url: &str) -> Result<sea_schema::dialect::DatabaseDialect, Box<dyn std::error::Error>> {
    if url.starts_with("postgresql://") || url.starts_with("postgres://") {
        Ok(sea_schema::dialect::DatabaseDialect::Postgres)
    } else if url.starts_with("mysql://") || url.starts_with("mariadb://") {
        Ok(sea_schema::dialect::DatabaseDialect::Mysql)
    } else if url.starts_with("sqlite:") || url.ends_with(".db") || url.ends_with(".sqlite") {
        Ok(sea_schema::dialect::DatabaseDialect::Sqlite)
    } else {
        Err(format!("Unsupported database URL: {}", url).into())
    }
}

#[cfg(feature = "discover")]
fn map_column_type(sea_type: &sea_schema::ColumnType) -> Result<ColumnKind, Box<dyn std::error::Error>> {
    use sea_schema::ColumnType as S;
    Ok(match sea_type {
        S::TinyInt(_) => ColumnKind::SmallInt,
        S::SmallInt(_) => ColumnKind::SmallInt,
        S::Int(_) => ColumnKind::Integer,
        S::BigInt(_) => ColumnKind::BigInt,
        S::Float(_) => ColumnKind::Float,
        S::Double(_) => ColumnKind::Double,
        S::Decimal(prec, scale) => ColumnKind::Decimal(*prec as usize, *scale as usize),
        S::Boolean => ColumnKind::Boolean,
        S::Char(len) => ColumnKind::Char(*len as usize),
        S::VarChar(len) => ColumnKind::Varchar(*len as usize),
        S::Text(_) => ColumnKind::Text,
        S::Date => ColumnKind::Date,
        S::Time(_) => ColumnKind::Time,
        S::DateTime(_) => ColumnKind::DateTime,
        S::Timestamp(_) => ColumnKind::Timestamp,
        S::Binary(_) => ColumnKind::Blob,
        S::VarBinary(len) => ColumnKind::VarBinary(*len as usize),
        S::Uuid => ColumnKind::Uuid,
        S::Json => ColumnKind::Json,
        S::Enum(variants) => ColumnKind::Enum(variants.clone()),
        _ => ColumnKind::Custom(format!("{:?}", sea_type)),
    })
}