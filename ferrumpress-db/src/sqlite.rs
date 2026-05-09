use async_trait::async_trait;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};
use sqlx::types::chrono;
use uuid::Uuid;
use ferrumpress_core::models::user::User;
use ferrumpress_core::models::role::Role;
use ferrumpress_core::traits::RelationalDb;
use ferrumpress_core::error::DbError;

pub struct SqliteDb {
    pool: SqlitePool,
}

impl SqliteDb {
    pub async fn new(path: &str) -> Result<Self, sqlx::Error> {
        let pool = SqlitePoolOptions::new()
            .connect(path)
            .await?;
        Self::migrate(&pool).await?;
        Ok(Self { pool })
    }

    async fn migrate(pool: &SqlitePool) -> Result<(), sqlx::Error> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS users (
                id BLOB PRIMARY KEY,
                login TEXT NOT NULL UNIQUE,
                email TEXT NOT NULL,
                password_hash TEXT,
                dilithium_public_key TEXT,
                role INTEGER NOT NULL DEFAULT 0,
                is_superuser INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            )"
        )
        .execute(pool)
        .await?;
        Ok(())
    }
}

#[async_trait]
impl RelationalDb for SqliteDb {
    async fn get_user_by_login(&self, login: &str) -> Result<Option<User>, DbError> {
        let row = sqlx::query_as::<_, (Vec<u8>, String, String, Option<String>, Option<String>, i32, bool, String)>(
            "SELECT id, login, email, password_hash, dilithium_public_key, role, is_superuser, created_at FROM users WHERE login = ?"
        )
        .bind(login)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| DbError::Internal(e.to_string()))?;

        Ok(row.map(|(id, login, email, password_hash, dilithium_public_key, role, is_superuser, created_at)| {
            User {
                id: Uuid::from_slice(&id).unwrap(),
                login,
                email,
                password_hash,
                dilithium_public_key,
                role: Role::from_repr(role as u8).unwrap_or(Role::Subscriber),
                is_superuser,
                created_at: chrono::DateTime::parse_from_rfc3339(&created_at).unwrap().with_timezone(&chrono::Utc),
            }
        }))
    }

    async fn get_user_by_id(&self, id: Uuid) -> Result<Option<User>, DbError> {
        let row = sqlx::query_as::<_, (String, String, Option<String>, Option<String>, i32, bool, String)>(
            "SELECT login, email, password_hash, dilithium_public_key, role, is_superuser, created_at FROM users WHERE id = ?"
        )
        .bind(id.as_bytes().as_slice())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| DbError::Internal(e.to_string()))?;

        Ok(row.map(|(login, email, password_hash, dilithium_public_key, role, is_superuser, created_at)| {
            User {
                id,
                login,
                email,
                password_hash,
                dilithium_public_key,
                role: Role::from_repr(role as u8).unwrap_or(Role::Subscriber),
                is_superuser,
                created_at: chrono::DateTime::parse_from_rfc3339(&created_at).unwrap().with_timezone(&chrono::Utc),
            }
        }))
    }

    async fn create_user(&self, user: &User) -> Result<User, DbError> {
        sqlx::query(
            "INSERT INTO users (id, login, email, password_hash, dilithium_public_key, role, is_superuser, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        )
        .bind(user.id.as_bytes().as_slice())
        .bind(&user.login)
        .bind(&user.email)
        .bind(&user.password_hash)                         // Option<String>
        .bind(&user.dilithium_public_key)                  // Option<String>
        .bind(user.role as i32)                            // Role -> i32
        .bind(user.is_superuser)
        .bind(user.created_at.to_rfc3339())
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
        sqlx::query(
            "UPDATE users SET login=?, email=?, password_hash=?, dilithium_public_key=?, role=?, is_superuser=? WHERE id=?"
        )
        .bind(&user.login)
        .bind(&user.email)
        .bind(&user.password_hash)
        .bind(&user.dilithium_public_key)
        .bind(user.role as i32)
        .bind(user.is_superuser)
        .bind(user.id.as_bytes().as_slice())
        .execute(&self.pool)
        .await
        .map_err(|e| DbError::Internal(e.to_string()))?;
        Ok(user.clone())
    }

    async fn delete_user(&self, id: Uuid) -> Result<(), DbError> {
        sqlx::query("DELETE FROM users WHERE id = ?")
            .bind(id.as_bytes().as_slice())
            .execute(&self.pool)
            .await
            .map_err(|e| DbError::Internal(e.to_string()))?;
        Ok(())
    }
}