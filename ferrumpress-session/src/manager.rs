use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;
use ferrumpress_core::models::token_pair::RefreshTokenInfo;
use ferrumpress_core::traits::{SessionStore, CacheProvider, CacheOptions};
use ferrumpress_core::error::SessionError;

pub struct SessionManager {
    store: Arc<dyn SessionStore>,
    cache: Option<Arc<dyn CacheProvider>>,   // для произвольных данных
    cleanup_handle: Option<tokio::task::JoinHandle<()>>,
}

impl SessionManager {
    /// Создаёт менеджера сессий.
    /// - `store` – хранилище refresh‑токенов.
    /// - `cache` – опциональный кэш для хранения дополнительных данных сессии.
    /// - `cleanup_interval` – интервал автоматической очистки истёкших токенов.
    pub fn new(
        store: Arc<dyn SessionStore>,
        cache: Option<Arc<dyn CacheProvider>>,
        cleanup_interval: Option<Duration>,
    ) -> Self {
        let cleanup_handle = if let Some(interval) = cleanup_interval {
            let store_clone = store.clone();
            Some(tokio::spawn(async move {
                loop {
                    tokio::time::sleep(interval).await;
                    if let Err(e) = store_clone.cleanup_expired().await {
                        tracing::error!("Session cleanup failed: {}", e);
                    }
                }
            }))
        } else {
            None
        };
        Self { store, cache, cleanup_handle }
    }

    // ------------------------------------------------------------
    //  Работа с refresh‑токенами (основные методы SessionStore)
    // ------------------------------------------------------------

    pub async fn save_refresh_token(&self, info: &RefreshTokenInfo) -> Result<(), SessionError> {
        self.store.save_refresh_token(info).await
    }

    pub async fn get_refresh_token(&self, jti: &str) -> Result<Option<RefreshTokenInfo>, SessionError> {
        self.store.get_refresh_token(jti).await
    }

    pub async fn revoke_refresh_token(&self, jti: &str) -> Result<(), SessionError> {
        // При отзыве токена удаляем и связанные данные сессии
        if let Some(cache) = &self.cache {
            let _ = cache.delete(&session_data_key(jti)).await;
        }
        self.store.revoke_refresh_token(jti).await
    }

    pub async fn revoke_all_for_user(&self, user_id: Uuid) -> Result<(), SessionError> {
        // При принудительном выходе всех сессий пользователя удаляем все данные всех сессий (не зная jti)
        // Можно реализовать сканирование ключей, но проще инвалидировать по тегу, если используется Redis.
        // Пока оставим базовую реализацию через store, а данные будут теряться по TTL.
        self.store.revoke_all_for_user(user_id).await
    }

    pub async fn cleanup_expired(&self) -> Result<u64, SessionError> {
        self.store.cleanup_expired().await
    }

    // ------------------------------------------------------------
    //  Произвольные данные сессии (связаны с jti)
    // ------------------------------------------------------------

    /// Сохранить произвольные данные сессии.
    ///
    /// - `jti` – идентификатор refresh‑токена (идентификатор сессии).
    /// - `data` – данные для сохранения (любой сериализуемый объект).
    /// - `ttl` – время жизни данных; если `None`, используется оставшееся время refresh‑токена.
    pub async fn set_data<T: serde::Serialize>(
        &self,
        jti: &str,
        data: &T,
        ttl: Option<Duration>,
    ) -> Result<(), SessionError> {
        let cache = self.cache.as_ref().ok_or(SessionError::Storage("cache not configured".into()))?;
        let json = serde_json::to_vec(data).map_err(|e| SessionError::Storage(e.to_string()))?;

        let effective_ttl = match ttl {
            Some(t) => t,
            None => {
                // Пытаемся получить refresh‑токен, чтобы узнать оставшееся время
                if let Some(token_info) = self.store.get_refresh_token(jti).await? {
                    let now = chrono::Utc::now();
                    let remaining = token_info.expires_at - now;
                    if remaining.num_seconds() > 0 {
                        Duration::from_secs(remaining.num_seconds() as u64)
                    } else {
                        return Err(SessionError::Expired);
                    }
                } else {
                    return Err(SessionError::NotFound);
                }
            }
        };

        let opts = CacheOptions {
            ttl: Some(effective_ttl),
            tags: vec![format!("session:{}", jti)],
        };
        cache.set(&session_data_key(jti), json, opts).await.map_err(|e| SessionError::Storage(e.to_string()))
    }

    /// Получить произвольные данные сессии.
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

    /// Удалить произвольные данные сессии.
    pub async fn remove_data(&self, jti: &str) -> Result<(), SessionError> {
        let cache = self.cache.as_ref().ok_or(SessionError::Storage("cache not configured".into()))?;
        cache.delete(&session_data_key(jti)).await.map_err(|e| SessionError::Storage(e.to_string()))?;
        Ok(())
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