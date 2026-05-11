use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;
use tokio::time::sleep;
use ferrumpress_core::models::token_pair::RefreshTokenInfo;
use ferrumpress_core::traits::{SessionStore, CacheProvider, CacheOptions};
use ferrumpress_core::error::SessionError;

pub struct SessionManager {
    store: Arc<dyn SessionStore>,
    cache: Option<Arc<dyn CacheProvider>>,
    cleanup_handle: Option<tokio::task::JoinHandle<()>>,
}

impl SessionManager {
    pub fn new(
        store: Arc<dyn SessionStore>,
        cache: Option<Arc<dyn CacheProvider>>,
        cleanup_interval: Option<Duration>,
    ) -> Self {
        let cleanup_handle = cleanup_interval.map(|interval| {
            let store_clone = store.clone();
            tokio::spawn(Self::run_cleanup_loop(store_clone, interval))
        });

        Self { store, cache, cleanup_handle }
    }

    async fn run_cleanup_loop(store: Arc<dyn SessionStore>, interval: Duration) {
        loop {
            tokio::select! {
                _ = sleep(interval) => {
                    if let Err(e) = store.cleanup_expired().await {
                        tracing::error!("Session cleanup failed: {}", e);
                    }
                }
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("Session cleanup shutting down gracefully");
                    break;
                }
            }
        }
    }

    // ------------------------------------------------------------
    //  Refresh-токены
    // ------------------------------------------------------------
    pub async fn save_refresh_token(&self, info: &RefreshTokenInfo) -> Result<(), SessionError> {
        self.store.save_refresh_token(info).await
    }

    pub async fn get_refresh_token(&self, jti: &str) -> Result<Option<RefreshTokenInfo>, SessionError> {
        self.store.get_refresh_token(jti).await
    }

    pub async fn revoke_refresh_token(&self, jti: &str) -> Result<(), SessionError> {
        if let Some(cache) = &self.cache {
            let _ = cache.delete(&session_data_key(jti)).await;
        }
        self.store.revoke_refresh_token(jti).await
    }

    pub async fn revoke_all_for_user(&self, user_id: Uuid) -> Result<(), SessionError> {
        self.store.revoke_all_for_user(user_id).await
    }

    pub async fn cleanup_expired(&self) -> Result<u64, SessionError> {
        self.store.cleanup_expired().await
    }

    // ------------------------------------------------------------
    //  Произвольные данные сессии
    // ------------------------------------------------------------
    pub async fn set_data<T: serde::Serialize>(
        &self,
        jti: &str,
        data: &T,
        ttl: Option<Duration>,
    ) -> Result<(), SessionError> {
        let cache = self.cache.as_ref().ok_or(SessionError::Storage("cache not configured".into()))?;
        let json = serde_json::to_vec(data).map_err(|e| SessionError::Storage(e.to_string()))?;
        let effective_ttl = self.compute_effective_ttl(jti, ttl).await?;

        let opts = CacheOptions {
            ttl: Some(effective_ttl),
            tags: vec![session_tag(jti)],
        };
        cache.set(&session_data_key(jti), json, opts).await.map_err(|e| SessionError::Storage(e.to_string()))
    }

    pub async fn get_data<T: serde::de::DeserializeOwned>(&self, jti: &str) -> Result<Option<T>, SessionError> {
        let cache = self.cache.as_ref().ok_or(SessionError::Storage("cache not configured".into()))?;
        match cache.get(&session_data_key(jti)).await.map_err(|e| SessionError::Storage(e.to_string()))? {
            Some(bytes) => {
                let data = serde_json::from_slice(&bytes).map_err(|e| SessionError::Storage(e.to_string()))?;
                Ok(Some(data))
            }
            None => Ok(None),
        }
    }

    pub async fn remove_data(&self, jti: &str) -> Result<(), SessionError> {
        let cache = self.cache.as_ref().ok_or(SessionError::Storage("cache not configured".into()))?;
        cache.delete(&session_data_key(jti)).await.map_err(|e| SessionError::Storage(e.to_string()))?;
        Ok(())
    }

    // Вычисление эффективного TTL
    async fn compute_effective_ttl(&self, jti: &str, ttl: Option<Duration>) -> Result<Duration, SessionError> {
        if let Some(t) = ttl {
            return Ok(t);
        }
        let token_info = self.store.get_refresh_token(jti).await?.ok_or(SessionError::NotFound)?;
        let now = chrono::Utc::now();
        let remaining = token_info.expires_at - now;
        if remaining.num_seconds() <= 0 {
            return Err(SessionError::Expired);
        }
        Ok(Duration::from_secs(remaining.num_seconds() as u64))
    }
}

impl Drop for SessionManager {
    fn drop(&mut self) {
        if let Some(handle) = self.cleanup_handle.take() {
            handle.abort();
        }
    }
}

fn session_data_key(jti: &str) -> String {
    format!("session_data:{}", jti)
}

fn session_tag(jti: &str) -> String {
    format!("session:{}", jti)
}
