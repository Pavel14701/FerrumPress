use async_trait::async_trait;
use sqlx::{AnyPool, Row, Column};
use serde_json::{Value, Map};
use std::sync::Arc;
use ferrumpress_core::models::{Model, ModelService, ColumnInfo, ColumnKind};

pub struct GenericModelService {
    model: Arc<dyn Model>,
    pool: AnyPool,
}

impl GenericModelService {
    pub fn new(model: Arc<dyn Model>, pool: AnyPool) -> Self {
        Self { model, pool }
    }

    /// Имена столбцов первичного ключа
    fn pk_columns(&self) -> Vec<String> {
        self.model.primary_keys()
    }

    /// Преобразовать строку из БД в JSON-объект
    fn row_to_json(row: &sqlx::any::AnyRow) -> Result<Value, String> {
        let mut map = Map::new();
        for col in row.columns() {
            let name = col.name().to_string();
            let val = if let Ok(s) = row.try_get::<String, _>(&name) {
                Value::String(s)
            } else if let Ok(n) = row.try_get::<i64, _>(&name) {
                Value::Number(n.into())
            } else if let Ok(f) = row.try_get::<f64, _>(&name) {
                serde_json::Number::from_f64(f).map(Value::Number).unwrap_or(Value::Null)
            } else if let Ok(b) = row.try_get::<bool, _>(&name) {
                Value::Bool(b)
            } else {
                Value::Null
            };
            map.insert(name, val);
        }
        Ok(Value::Object(map))
    }

    /// Строит INSERT-запрос без RETURNING.
    /// Возвращает SQL и список колонок, которые нужно забиндить.
    fn build_insert_sql(&self, data: &Value) -> Result<(String, Vec<String>), String> {
        let cols = self.model.columns();
        let obj = data.as_object().ok_or("Data must be a JSON object")?;

        let mut insert_cols = Vec::new();
        let mut placeholders = Vec::new();
        let mut bind_order = Vec::new();

        for (i, col) in cols.iter().enumerate() {
            // Пропускаем SERIAL – но в универсальной версии SERIAL не используется.
            if matches!(col.kind, ColumnKind::Serial) && !obj.contains_key(&col.name) {
                continue;
            }
            insert_cols.push(col.name.clone());
            placeholders.push(format!("${}", i + 1));
            bind_order.push(col.name.clone());
        }

        let query = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            self.model.table_name(),
            insert_cols.join(", "),
            placeholders.join(", ")
        );
        Ok((query, bind_order))
    }

    /// Строит UPDATE-запрос без RETURNING.
    /// Возвращает SQL, порядок колонок для биндинга, имена ключевых колонок.
    fn build_update_sql(&self, data: &Value) -> Result<(String, Vec<String>, Vec<String>), String> {
        let pk = self.pk_columns();
        if pk.is_empty() {
            return Err("No primary key for update".into());
        }

        let cols = self.model.columns();
        let obj = data.as_object().ok_or("Data must be a JSON object")?;

        let mut set_clauses = Vec::new();
        let mut bind_order = Vec::new();
        let mut param_idx = 1;

        for col in &cols {
            if pk.contains(&col.name) {
                continue;
            }
            if !obj.contains_key(&col.name) {
                continue;
            }
            set_clauses.push(format!("{} = ${}", col.name, param_idx));
            bind_order.push(col.name.clone());
            param_idx += 1;
        }

        if set_clauses.is_empty() {
            return Err("No fields to update".into());
        }

        let mut where_clauses = Vec::new();
        for pk_col in &pk {
            where_clauses.push(format!("{} = ${}", pk_col, param_idx));
            bind_order.push(pk_col.clone());
            param_idx += 1;
        }

        let query = format!(
            "UPDATE {} SET {} WHERE {}",
            self.model.table_name(),
            set_clauses.join(", "),
            where_clauses.join(" AND ")
        );
        Ok((query, bind_order, pk.clone()))
    }

    /// Привязывает значения из JSON к запросу в указанном порядке.
    fn bind_values<'q>(
        mut query: sqlx::query::Query<'q, sqlx::Any, sqlx::any::AnyArguments<'q>>,
        data: &Value,
        bind_order: &[String],
    ) -> Result<sqlx::query::Query<'q, sqlx::Any, sqlx::any::AnyArguments<'q>>, String> {
        for name in bind_order {
            let val = data.get(name).ok_or_else(|| format!("Missing field {}", name))?;
            query = match val {
                Value::Null => query.bind(None::<String>),
                Value::String(s) => query.bind(s.clone()),
                Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        query.bind(i)
                    } else if let Some(f) = n.as_f64() {
                        query.bind(f)
                    } else {
                        return Err("Unsupported number type".into());
                    }
                }
                Value::Bool(b) => query.bind(*b),
                _ => return Err("Unsupported value type".into()),
            };
        }
        Ok(query)
    }

    /// Выполняет SELECT по первичному ключу (поддерживает составные ключи).
    async fn select_by_pk(&self, id: &Value) -> Result<Option<sqlx::any::AnyRow>, String> {
        let pk = self.pk_columns();
        let pk_obj = if pk.len() == 1 {
            // Одиночный ключ может быть передан как скаляр или объект с одним полем
            match id {
                Value::Object(_) => id.clone(),
                _ => {
                    let mut map = Map::new();
                    map.insert(pk[0].clone(), id.clone());
                    Value::Object(map)
                }
            }
        } else {
            id.clone()
        };

        let pk_obj = pk_obj.as_object().ok_or("Primary key must be a JSON object")?;
        let mut conditions = Vec::new();
        let mut bind_values = Vec::new();
        for (i, pk_col) in pk.iter().enumerate() {
            conditions.push(format!("{} = ${}", pk_col, i + 1));
            bind_values.push(
                pk_obj.get(pk_col)
                    .ok_or_else(|| format!("Missing primary key field {}", pk_col))?
                    .clone()
            );
        }

        let query = format!(
            "SELECT * FROM {} WHERE {}",
            self.model.table_name(),
            conditions.join(" AND ")
        );
        let mut q = sqlx::query(&query);
        for val in bind_values {
            q = Self::bind_value(q, &val)?;
        }

        q.fetch_optional(&self.pool)
            .await
            .map_err(|e| e.to_string())
    }

    /// Привязывает одиночное значение к запросу.
    fn bind_value<'q>(
        q: sqlx::query::Query<'q, sqlx::Any, sqlx::any::AnyArguments<'q>>,
        val: &Value,
    ) -> Result<sqlx::query::Query<'q, sqlx::Any, sqlx::any::AnyArguments<'q>>, String> {
        Ok(match val {
            Value::Null => q.bind(None::<String>),
            Value::String(s) => q.bind(s.clone()),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() { q.bind(i) }
                else if let Some(f) = n.as_f64() { q.bind(f) }
                else { return Err("Invalid number".into()); }
            }
            Value::Bool(b) => q.bind(*b),
            _ => return Err("Unsupported bind value".into()),
        })
    }
}

#[async_trait]
impl ModelService for GenericModelService {
    async fn find_by_id(&self, id: &Value) -> Result<Option<Value>, String> {
        let row = self.select_by_pk(id).await?;
        row.map(|r| Self::row_to_json(&r)).transpose()
    }

    async fn find_all(&self, limit: Option<u32>, offset: Option<u32>) -> Result<Vec<Value>, String> {
        let mut query = format!("SELECT * FROM {}", self.model.table_name());
        let mut param_idx = 1;
        if let Some(lim) = limit {
            query.push_str(&format!(" LIMIT ${}", param_idx));
            param_idx += 1;
        }
        if let Some(off) = offset {
            query.push_str(&format!(" OFFSET ${}", param_idx));
            param_idx += 1;
        }

        let mut q = sqlx::query(&query);
        if let Some(lim) = limit { q = q.bind(lim as i64); }
        if let Some(off) = offset { q = q.bind(off as i64); }

        let rows = q.fetch_all(&self.pool).await.map_err(|e| e.to_string())?;
        rows.iter().map(Self::row_to_json).collect()
    }

    async fn insert(&self, data: Value) -> Result<Value, String> {
        let (sql, bind_order) = self.build_insert_sql(&data)?;
        let q = sqlx::query(&sql);
        let q = Self::bind_values(q, &data, &bind_order)?;
        q.execute(&self.pool).await.map_err(|e| e.to_string())?;

        // После вставки извлекаем запись по первичному ключу (он должен быть в data)
        let pk = self.pk_columns();
        if pk.is_empty() {
            return Err("Cannot identify inserted row without primary key".into());
        }
        self.select_by_pk(&data)?.ok_or_else(|| "Inserted row not found".into()).map(|r| Self::row_to_json(&r))?
    }

    async fn update(&self, id: &Value, data: Value) -> Result<Value, String> {
        let (sql, bind_order, pk_cols) = self.build_update_sql(&data)?;
        let q = sqlx::query(&sql);
        let q = Self::bind_values(q, &data, &bind_order)?;
        // Привязываем значения ключа из параметра id
        let q = Self::bind_values(q, id, &pk_cols)?;
        let affected = q.execute(&self.pool).await.map_err(|e| e.to_string())?.rows_affected();
        if affected == 0 {
            return Err("No rows updated".into());
        }

        // Читаем обновлённую запись
        self.select_by_pk(id)?.ok_or_else(|| "Updated row not found".into()).map(|r| Self::row_to_json(&r))?
    }

    async fn delete(&self, id: &Value) -> Result<u64, String> {
        let pk = self.pk_columns();
        let pk_obj = if pk.len() == 1 {
            match id {
                Value::Object(_) => id.clone(),
                _ => {
                    let mut map = Map::new();
                    map.insert(pk[0].clone(), id.clone());
                    Value::Object(map)
                }
            }
        } else {
            id.clone()
        };

        let pk_obj = pk_obj.as_object().ok_or("Primary key must be a JSON object")?;
        let mut conditions = Vec::new();
        let mut bind_values = Vec::new();
        for (i, pk_col) in pk.iter().enumerate() {
            conditions.push(format!("{} = ${}", pk_col, i + 1));
            bind_values.push(
                pk_obj.get(pk_col)
                    .ok_or_else(|| format!("Missing primary key field {}", pk_col))?
                    .clone()
            );
        }

        let query = format!(
            "DELETE FROM {} WHERE {}",
            self.model.table_name(),
            conditions.join(" AND ")
        );
        let mut q = sqlx::query(&query);
        for val in bind_values {
            q = Self::bind_value(q, &val)?;
        }

        let res = q.execute(&self.pool).await.map_err(|e| e.to_string())?;
        Ok(res.rows_affected())
    }
}