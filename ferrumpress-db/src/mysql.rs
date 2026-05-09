use async_trait::async_trait;
use sqlx::MySqlPool;
use uuid::Uuid;
use ferrumpress_core::models::user::{User, Role};
use ferrumpress_core::traits::RelationalDb;
use ferrumpress_core::error::DbError;

pub struct MysqlDb {
    pool: MySqlPool,
}

impl MysqlDb {
    pub async fn new(database_url: &str) -> Result<Self, sqlx::Error> {
        let pool = MySqlPool::connect(database_url).await?;
        Self::migrate(&pool).await?;
        Ok(Self { pool })
    }

    async fn migrate(pool: &MySqlPool) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS users (
                id CHAR(36) PRIMARY KEY,
                login VARCHAR(255) NOT NULL UNIQUE,
                email VARCHAR(255) NOT NULL,
                password_hash TEXT,
                dilithium_public_key TEXT,
                role INT NOT NULL DEFAULT 0,
                is_superuser BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            "#
        )
        .execute(pool)
        .await?;
        Ok(())
    }
}

#[async_trait]
impl RelationalDb for MysqlDb {
    async fn get_user_by_login(&self, login: &str) -> Result<Option<User>, DbError> {
        let row = sqlx::query_as!(
            UserRow,
            r#"SELECT id as "id: String", login, email, password_hash, dilithium_public_key, role as "role: i32", is_superuser as "is_superuser: bool", created_at as "created_at: chrono::NaiveDateTime" FROM users WHERE login = ?"#,
            login
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| DbError::Internal(e.to_string()))?;

        Ok(row.map(|r| {
            let id = Uuid::parse_str(&r.id).unwrap_or_default();
            let created_at = r.created_at.and_utc(); // предполагаем UTC
            User {
                id,
                login: r.login,
                email: r.email,
                password_hash: r.password_hash,
                dilithium_public_key: r.dilithium_public_key,
                role: Role::from_repr(r.role as u8).unwrap_or(Role::Subscriber),
                is_superuser: r.is_superuser,
                created_at,
            }
        }))
    }

    async fn get_user_by_id(&self, id: Uuid) -> Result<Option<User>, DbError> {
        let id_str = id.to_string();
        let row = sqlx::query_as!(
            UserRow,
            r#"SELECT id as "id: String", login, email, password_hash, dilithium_public_key, role as "role: i32", is_superuser as "is_superuser: bool", created_at as "created_at: chrono::NaiveDateTime" FROM users WHERE id = ?"#,
            id_str
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| DbError::Internal(e.to_string()))?;

        Ok(row.map(|r| {
            let created_at = r.created_at.and_utc();
            User {
                id,
                login: r.login,
                email: r.email,
                password_hash: r.password_hash,
                dilithium_public_key: r.dilithium_public_key,
                role: Role::from_repr(r.role as u8).unwrap_or(Role::Subscriber),
                is_superuser: r.is_superuser,
                created_at,
            }
        }))
    }

    async fn create_user(&self, user: &User) -> Result<User, DbError> {
        let id_str = user.id.to_string();
        sqlx::query!(
            r#"INSERT INTO users (id, login, email, password_hash, dilithium_public_key, role, is_superuser, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)"#,
            id_str,
            user.login,
            user.email,
            user.password_hash,
            user.dilithium_public_key,
            user.role as i32,
            user.is_superuser,
            user.created_at.naive_utc()
        )
        .execute(&self.pool)
        .await
        .map_err(|e| {
            if e.as_database_error().map(|d| d.is_unique_violation()).unwrap_or(false) {
                DbError::Duplicate("User already exists".into())
            } else {
                DbError::Internal(e.to_string())
            }
        })?;
        Ok(user.clone())
    }

    async fn update_user(&self, user: &User) -> Result<User, DbError> {
        let id_str = user.id.to_string();
        sqlx::query!(
            r#"UPDATE users SET login=?, email=?, password_hash=?, dilithium_public_key=?, role=?, is_superuser=? WHERE id=?"#,
            user.login,
            user.email,
            user.password_hash,
            user.dilithium_public_key,
            user.role as i32,
            user.is_superuser,
            id_str
        )
        .execute(&self.pool)
        .await
        .map_err(|e| DbError::Internal(e.to_string()))?;
        Ok(user.clone())
    }

    async fn delete_user(&self, id: Uuid) -> Result<(), DbError> {
        let id_str = id.to_string();
        sqlx::query!("DELETE FROM users WHERE id = ?", id_str)
            .execute(&self.pool)
            .await
            .map_err(|e| DbError::Internal(e.to_string()))?;
        Ok(())
    }
}

// Вспомогательная структура для MySQL
#[derive(sqlx::FromRow)]
struct UserRow {
    id: String,
    login: String,
    email: String,
    password_hash: Option<String>,
    dilithium_public_key: Option<String>,
    role: i32,
    is_superuser: bool,
    created_at: chrono::NaiveDateTime,
}