use async_trait::async_trait;
use sqlx::AnyPool;
use uuid::Uuid;
use chrono::Utc;
use ferrumpress_core::models::token_pair::RefreshTokenInfo;
use ferrumpress_core::traits::SessionStore;
use ferrumpress_core::error::SessionError;

pub struct DatabaseSessionStore {
    pool: AnyPool,
}

impl DatabaseSessionStore {
    pub fn new(pool: AnyPool) -> Self {
        Self { pool }
    }

    pub async fn migrate(&self) -> Result<(), sqlx::Error> {
        // SQL универсален для всех поддерживаемых БД
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS refresh_tokens (
                jti TEXT PRIMARY KEY,
                user_id BLOB NOT NULL,
                expires_at TEXT NOT NULL,
                user_agent TEXT,
                ip_address TEXT
            )"
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

#[async_trait]
impl SessionStore for DatabaseSessionStore {
    async fn save_refresh_token(&self, info: &RefreshTokenInfo) -> Result<(), SessionError> {
        sqlx::query(
            "INSERT INTO refresh_tokens (jti, user_id, expires_at, user_agent, ip_address)
             VALUES ($1, $2, $3, $4, $5)"
        )
        .bind(&info.jti)
        .bind(info.user_id.as_bytes().to_vec())   // BLOB
        .bind(info.expires_at.to_rfc3339())
        .bind(&info.user_agent)
        .bind(&info.ip_address)
        .execute(&self.pool)
        .await
        .map_err(|e| SessionError::Storage(e.to_string()))?;
        Ok(())
    }

    async fn get_refresh_token(&self, jti: &str) -> Result<Option<RefreshTokenInfo>, SessionError> {
        let now = Utc::now().to_rfc3339();
        let row = sqlx::query_as::<_, (String, Vec<u8>, String, Option<String>, Option<String>)>(
            "SELECT jti, user_id, expires_at, user_agent, ip_address
             FROM refresh_tokens
             WHERE jti = $1 AND expires_at > $2"
        )
        .bind(jti)
        .bind(now)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| SessionError::Storage(e.to_string()))?;

        Ok(row.map(|(jti, user_id, expires_at, user_agent, ip_address)| {
            let parsed_user_id = Uuid::from_slice(&user_id).unwrap_or_else(|_| Uuid::nil());
            let parsed_expires_at = chrono::DateTime::parse_from_rfc3339(&expires_at)
                .unwrap_or_else(|_| chrono::DateTime::parse_from_rfc3339("1970-01-01T00:00:00Z").unwrap())
                .with_timezone(&chrono::Utc);
            RefreshTokenInfo {
                jti,
                user_id: parsed_user_id,
                expires_at: parsed_expires_at,
                user_agent,
                ip_address,
            }
        }))
    }

    async fn revoke_refresh_token(&self, jti: &str) -> Result<(), SessionError> {
        sqlx::query("DELETE FROM refresh_tokens WHERE jti = $1")
            .bind(jti)
            .execute(&self.pool)
            .await
            .map_err(|e| SessionError::Storage(e.to_string()))?;
        Ok(())
    }

    async fn revoke_all_for_user(&self, user_id: Uuid) -> Result<(), SessionError> {
        sqlx::query("DELETE FROM refresh_tokens WHERE user_id = $1")
            .bind(user_id.as_bytes().to_vec())
            .execute(&self.pool)
            .await
            .map_err(|e| SessionError::Storage(e.to_string()))?;
        Ok(())
    }

    async fn cleanup_expired(&self) -> Result<u64, SessionError> {
        let now = Utc::now().to_rfc3339();
        let result = sqlx::query("DELETE FROM refresh_tokens WHERE expires_at < $1")
            .bind(now)
            .execute(&self.pool)
            .await
            .map_err(|e| SessionError::Storage(e.to_string()))?;
        Ok(result.rows_affected())
    }
}
