use async_trait::async_trait;
use sqlx::PgPool;
use uuid::Uuid;
use ferrumpress_core::models::user::{User, Role};
use ferrumpress_core::traits::RelationalDb;
use ferrumpress_core::error::DbError;

pub struct PostgresDb {
    pool: PgPool,
}

impl PostgresDb {
    pub async fn new(database_url: &str) -> Result<Self, sqlx::Error> {
        let pool = PgPool::connect(database_url).await?;
        Self::migrate(&pool).await?;
        Ok(Self { pool })
    }

    async fn migrate(pool: &PgPool) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS users (
                id UUID PRIMARY KEY,
                login TEXT NOT NULL UNIQUE,
                email TEXT NOT NULL,
                password_hash TEXT,
                dilithium_public_key TEXT,
                role INTEGER NOT NULL DEFAULT 0,
                is_superuser BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMPTZ NOT NULL
            )
            "#
        )
        .execute(pool)
        .await?;
        Ok(())
    }
}

#[async_trait]
impl RelationalDb for PostgresDb {
    async fn get_user_by_login(&self, login: &str) -> Result<Option<User>, DbError> {
        let row = sqlx::query_as!(
            UserRow,
            r#"SELECT id as "id: Uuid", login, email, password_hash, dilithium_public_key, role as "role: i32", is_superuser, created_at as "created_at: chrono::DateTime<chrono::Utc>" FROM users WHERE login = $1"#,
            login
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| DbError::Internal(e.to_string()))?;

        Ok(row.map(|r| User {
            id: r.id,
            login: r.login,
            email: r.email,
            password_hash: r.password_hash,
            dilithium_public_key: r.dilithium_public_key,
            role: Role::from_repr(r.role as u8).unwrap_or(Role::Subscriber),
            is_superuser: r.is_superuser,
            created_at: r.created_at,
        }))
    }

    async fn get_user_by_id(&self, id: Uuid) -> Result<Option<User>, DbError> {
        let row = sqlx::query_as!(
            UserRow,
            r#"SELECT id as "id: Uuid", login, email, password_hash, dilithium_public_key, role as "role: i32", is_superuser, created_at as "created_at: chrono::DateTime<chrono::Utc>" FROM users WHERE id = $1"#,
            id
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| DbError::Internal(e.to_string()))?;

        Ok(row.map(|r| User {
            id: r.id,
            login: r.login,
            email: r.email,
            password_hash: r.password_hash,
            dilithium_public_key: r.dilithium_public_key,
            role: Role::from_repr(r.role as u8).unwrap_or(Role::Subscriber),
            is_superuser: r.is_superuser,
            created_at: r.created_at,
        }))
    }

    async fn create_user(&self, user: &User) -> Result<User, DbError> {
        sqlx::query!(
            r#"INSERT INTO users (id, login, email, password_hash, dilithium_public_key, role, is_superuser, created_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"#,
            user.id,
            user.login,
            user.email,
            user.password_hash,
            user.dilithium_public_key,
            user.role as i32,
            user.is_superuser,
            user.created_at
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
        sqlx::query!(
            r#"UPDATE users SET login=$1, email=$2, password_hash=$3, dilithium_public_key=$4, role=$5, is_superuser=$6 WHERE id=$7"#,
            user.login,
            user.email,
            user.password_hash,
            user.dilithium_public_key,
            user.role as i32,
            user.is_superuser,
            user.id
        )
        .execute(&self.pool)
        .await
        .map_err(|e| DbError::Internal(e.to_string()))?;
        Ok(user.clone())
    }

    async fn delete_user(&self, id: Uuid) -> Result<(), DbError> {
        sqlx::query!("DELETE FROM users WHERE id = $1", id)
            .execute(&self.pool)
            .await
            .map_err(|e| DbError::Internal(e.to_string()))?;
        Ok(())
    }
}

// Вспомогательная структура для query_as!
#[derive(sqlx::FromRow)]
struct UserRow {
    id: Uuid,
    login: String,
    email: String,
    password_hash: Option<String>,
    dilithium_public_key: Option<String>,
    role: i32,
    is_superuser: bool,
    created_at: chrono::DateTime<chrono::Utc>,
}