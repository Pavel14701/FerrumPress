use async_trait::async_trait;
use redis::aio::ConnectionManager;
use uuid::Uuid;
use chrono::Utc;
use ferrumpress_core::models::token_pair::RefreshTokenInfo;
use ferrumpress_core::traits::SessionStore;
use ferrumpress_core::error::SessionError;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct RedisSessionStore {
    conn: Arc<Mutex<ConnectionManager>>,
}

impl RedisSessionStore {
    pub fn new(client: redis::Client) -> Result<Self, redis::RedisError> {
        let conn = client.get_tokio_connection_manager()?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }
}

#[async_trait]
impl SessionStore for RedisSessionStore {
    async fn save_refresh_token(&self, info: &RefreshTokenInfo) -> Result<(), SessionError> {
        let mut conn = self.conn.lock().await;
        let key = format!("refresh_token:{}", info.jti);
        let value = serde_json::to_string(info)
            .map_err(|e| SessionError::Storage(e.to_string()))?;
        let ttl = (info.expires_at - Utc::now()).num_seconds().max(1);
        redis::cmd("SETEX")
            .arg(key)
            .arg(ttl)
            .arg(value)
            .query_async(&mut *conn)
            .await
            .map_err(|e| SessionError::Storage(e.to_string()))?;
        Ok(())
    }

    async fn get_refresh_token(&self, jti: &str) -> Result<Option<RefreshTokenInfo>, SessionError> {
        let mut conn = self.conn.lock().await;
        let key = format!("refresh_token:{}", jti);
        let result: Option<String> = redis::cmd("GET")
            .arg(key)
            .query_async(&mut *conn)
            .await
            .map_err(|e| SessionError::Storage(e.to_string()))?;
        match result {
            Some(json) => {
                let info: RefreshTokenInfo = serde_json::from_str(&json)
                    .map_err(|e| SessionError::Storage(e.to_string()))?;
                Ok(Some(info))
            }
            None => Ok(None),
        }
    }

    async fn revoke_refresh_token(&self, jti: &str) -> Result<(), SessionError> {
        let mut conn = self.conn.lock().await;
        let key = format!("refresh_token:{}", jti);
        redis::cmd("DEL")
            .arg(key)
            .query_async(&mut *conn)
            .await
            .map_err(|e| SessionError::Storage(e.to_string()))?;
        Ok(())
    }

    async fn revoke_all_for_user(&self, user_id: Uuid) -> Result<(), SessionError> {
        let mut conn = self.conn.lock().await;
        let mut cursor: u64 = 0;
        loop {
            let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg("refresh_token:*")
                .query_async(&mut *conn)
                .await
                .map_err(|e| SessionError::Storage(e.to_string()))?;
            for key in keys {
                let val: Option<String> = redis::cmd("GET")
                    .arg(&key)
                    .query_async(&mut *conn)
                    .await
                    .map_err(|e| SessionError::Storage(e.to_string()))?;
                if let Some(json) = val {
                    if let Ok(info) = serde_json::from_str::<RefreshTokenInfo>(&json) {
                        if info.user_id == user_id {
                            let _: () = redis::cmd("DEL")
                                .arg(&key)
                                .query_async(&mut *conn)
                                .await
                                .map_err(|e| SessionError::Storage(e.to_string()))?;
                        }
                    }
                }
            }
            if new_cursor == 0 {
                break;
            }
            cursor = new_cursor;
        }
        Ok(())
    }

    async fn cleanup_expired(&self) -> Result<u64, SessionError> {
        // Redis удаляет ключи автоматически по TTL
        Ok(0)
    }
}