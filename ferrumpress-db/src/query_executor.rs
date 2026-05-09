use async_trait::async_trait;
use sqlparser::ast::*;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use ferrumpress_core::traits::raw_query::{RawQueryExecutor, QueryResult};
use ferrumpress_core::error::QueryError;
use sqlx::{AnyPool, Row, Column};

pub struct DbQueryExecutor {
    pool: AnyPool,
    allowed_prefixes: Vec<String>,
    allowed_specific_tables: Vec<String>,
    protected_tables: Vec<String>,
}

impl DbQueryExecutor {
    pub fn new(
        pool: AnyPool,
        allowed_prefixes: Vec<String>,
        allowed_specific_tables: Vec<String>,
        protected_tables: Vec<String>,
    ) -> Self {
        Self { pool, allowed_prefixes, allowed_specific_tables, protected_tables }
    }

    fn is_modifiable_table(&self, name: &str) -> bool {
        self.allowed_specific_tables.contains(&name.to_string()) ||
        self.allowed_prefixes.iter().any(|prefix| name.starts_with(prefix))
    }

    fn is_protected(&self, name: &str) -> bool {
        self.protected_tables.contains(&name.to_string())
    }

    fn validate_statement(&self, statement: &Statement) -> Result<(), QueryError> {
        match statement {
            Statement::Query(_) => Ok(()),

            Statement::Insert(insert) => {
                let name = match &insert.table {
                    TableObject::TableName(obj_name) => object_name_to_string(obj_name),
                    _ => return Err(QueryError::Parse("unsupported table object in INSERT".into())),
                };
                if self.is_protected(&name) {
                    return Err(QueryError::TableNotAllowed(name));
                }
                if !self.is_modifiable_table(&name) {
                    return Err(QueryError::TableNotAllowed(name));
                }
                Ok(())
            }

            Statement::Update(update) => {
                let name = table_name_from_update(&update.table)?;
                if self.is_protected(&name) {
                    return Err(QueryError::TableNotAllowed(name));
                }
                if !self.is_modifiable_table(&name) {
                    return Err(QueryError::TableNotAllowed(name));
                }
                Ok(())
            }

            Statement::Delete(delete) => {
                let name = table_name_from_delete(&delete.from)?;
                if self.is_protected(&name) {
                    return Err(QueryError::TableNotAllowed(name));
                }
                if !self.is_modifiable_table(&name) {
                    return Err(QueryError::TableNotAllowed(name));
                }
                Ok(())
            }

            Statement::CreateTable(create_table) => {
                let name_str = object_name_to_string(&create_table.name);
                if self.is_protected(&name_str) {
                    return Err(QueryError::TableNotAllowed(name_str));
                }
                if !self.is_modifiable_table(&name_str) {
                    return Err(QueryError::TableNotAllowed(name_str));
                }
                Ok(())
            }

            Statement::AlterTable(alter_table) => {
                let name_str = object_name_to_string(&alter_table.name);
                if self.is_protected(&name_str) {
                    return Err(QueryError::TableNotAllowed(name_str));
                }
                if !self.is_modifiable_table(&name_str) {
                    return Err(QueryError::TableNotAllowed(name_str));
                }
                Ok(())
            }

            Statement::Drop { object_type, names, .. } => {
                if *object_type == ObjectType::Table {
                    for name in names {
                        let name_str = object_name_to_string(name);
                        if self.is_protected(&name_str) {
                            return Err(QueryError::TableNotAllowed(name_str));
                        }
                        if !self.is_modifiable_table(&name_str) {
                            return Err(QueryError::TableNotAllowed(name_str));
                        }
                    }
                }
                Ok(())
            }

            _ => Err(QueryError::NotAllowed("statement type not permitted".into())),
        }
    }

    pub fn validate_query(&self, sql: &str) -> Result<(), QueryError> {
        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql)
            .map_err(|e| QueryError::Parse(e.to_string()))?;
        for statement in &ast {
            self.validate_statement(statement)?;
        }
        Ok(())
    }
}

#[async_trait]
impl RawQueryExecutor for DbQueryExecutor {
    async fn execute_raw_query(&self, query: &str) -> Result<QueryResult, QueryError> {
        self.validate_query(query)?;

        // fetch_all возвращает Vec<AnyRow>
        let rows_data = sqlx::query(query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| QueryError::Execution(e.to_string()))?;

        let mut columns = Vec::new();
        let mut rows = Vec::new();

        if !rows_data.is_empty() {
            // Получаем имена колонок из первой строки (трейт Column даёт name())
            columns = rows_data[0]
                .columns()
                .iter()
                .map(|c| c.name().to_string())
                .collect();

            for row in &rows_data {
                let mut row_vec = Vec::new();
                for (i, _) in row.columns().iter().enumerate() {
                    // try_get доступен из трейта Row
                    let val: serde_json::Value = if let Ok(s) = row.try_get::<String, _>(i) {
                        serde_json::Value::String(s)
                    } else if let Ok(n) = row.try_get::<i64, _>(i) {
                        serde_json::Value::Number(n.into())
                    } else if let Ok(f) = row.try_get::<f64, _>(i) {
                        serde_json::Number::from_f64(f)
                            .map(serde_json::Value::Number)
                            .unwrap_or(serde_json::Value::Null)
                    } else {
                        serde_json::Value::Null
                    };
                    row_vec.push(val);
                }
                rows.push(row_vec);
            }
        }

        Ok(QueryResult {
            columns,
            rows,
            affected_rows: None, // количество изменённых строк недоступно при fetch_all
        })
    }
}

/// Извлекает имя таблицы из TableWithJoins (для UPDATE)
fn table_name_from_update(table: &TableWithJoins) -> Result<String, QueryError> {
    match &table.relation {
        TableFactor::Table { name, .. } => Ok(object_name_to_string(name)),
        _ => Err(QueryError::Parse("unsupported table in UPDATE".into())),
    }
}

/// Извлекает имя таблицы из FromTable (для DELETE)
fn table_name_from_delete(from: &FromTable) -> Result<String, QueryError> {
    match from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => {
            if let Some(first) = tables.first() {
                match &first.relation {
                    TableFactor::Table { name, .. } => Ok(object_name_to_string(name)),
                    _ => Err(QueryError::Parse("unsupported table in DELETE".into())),
                }
            } else {
                Err(QueryError::Parse("no table in DELETE".into()))
            }
        }
    }
}

fn object_name_to_string(name: &ObjectName) -> String {
    name.0.iter()
        .map(|part| match part {
            ObjectNamePart::Identifier(ident) => ident.value.clone(),
            ObjectNamePart::Function(func) => func.to_string(),
        })
        .collect::<Vec<_>>()
        .join(".")
}