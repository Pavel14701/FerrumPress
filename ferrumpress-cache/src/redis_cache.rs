use async_trait::async_trait;
use ferrumpress_core::traits::CacheProvider;
use ferrumpress_core::traits::cache::CacheOptions;
use ferrumpress_core::error::CacheError;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;

const CACHE_PREFIX: &str = "cache:";

pub struct RedisCache {
    conn: ConnectionManager,
}

impl RedisCache {
    pub fn new(url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let client = redis::Client::open(url)?;
        let conn = client.get_tokio_connection_manager()?;
        Ok(Self { conn })
    }

    fn prefixed_key(key: &str) -> String {
        format!("{}{}", CACHE_PREFIX, key)
    }
}

#[async_trait]
impl CacheProvider for RedisCache {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        let mut conn = self.conn.clone();
        let result: Option<Vec<u8>> = conn
            .get(Self::prefixed_key(key))
            .await
            .map_err(|e| CacheError::Backend(e.to_string()))?;
        Ok(result)
    }

    async fn set(&self, key: &str, value: Vec<u8>, options: CacheOptions) -> Result<(), CacheError> {
        let mut conn = self.conn.clone();
        let prefixed = Self::prefixed_key(key);

        if let Some(ttl) = options.ttl {
            let _: () = conn
                .set_ex(&prefixed, value, ttl.as_secs() as usize)
                .await
                .map_err(|e| CacheError::Backend(e.to_string()))?;
        } else {
            let _: () = conn
                .set(&prefixed, value)
                .await
                .map_err(|e| CacheError::Backend(e.to_string()))?;
        }

        for tag in &options.tags {
            let _: () = conn
                .sadd(format!("tag:{}", tag), &prefixed)
                .await
                .map_err(|e| CacheError::Backend(e.to_string()))?;
        }
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), CacheError> {
        let mut conn = self.conn.clone();
        // При удалении ключа мы не знаем его тегов, поэтому мёртвые ссылки в Set'ах могут остаться.
        // Они будут удалены при инвалидации по тегу или при следующем обращении (ключ уже не существует).
        // Для полноценной очистки можно хранить обратную связь (например, в хеше meta:key), но для кэша это избыточно.
        let _: () = conn
            .del(Self::prefixed_key(key))
            .await
            .map_err(|e| CacheError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn invalidate_by_tag(&self, tag: &str) -> Result<u64, CacheError> {
        let mut conn = self.conn.clone();
        let tag_key = format!("tag:{}", tag);
        let keys: Vec<String> = conn
            .smembers(&tag_key)
            .await
            .map_err(|e| CacheError::Backend(e.to_string()))?;
        let count = keys.len() as u64;
        if !keys.is_empty() {
            let _: () = conn
                .del(&keys)
                .await
                .map_err(|e| CacheError::Backend(e.to_string()))?;
        }
        // Удаляем сам Set, чтобы не копился мусор
        let _: () = conn
            .del(&tag_key)
            .await
            .map_err(|e| CacheError::Backend(e.to_string()))?;
        Ok(count)
    }

    async fn clear(&self) -> Result<(), CacheError> {
        let mut conn = self.conn.clone();
        let mut cursor: u64 = 0;
        loop {
            let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(format!("{}*", CACHE_PREFIX))
                .arg("COUNT")
                .arg(1000)
                .query_async(&mut conn)
                .await
                .map_err(|e| CacheError::Backend(e.to_string()))?;
            if !keys.is_empty() {
                let _: () = conn
                    .del(&keys)
                    .await
                    .map_err(|e| CacheError::Backend(e.to_string()))?;
            }
            if new_cursor == 0 {
                break;
            }
            cursor = new_cursor;
        }
        Ok(())
    }
}