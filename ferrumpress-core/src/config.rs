use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub database: DatabaseBackend,
    pub session: SessionBackend,
    pub auth: AuthBackend,
    pub storage: StorageBackend,
    pub search: SearchBackend,
    pub queue: QueueBackend,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum DatabaseBackend {
    Sqlite { path: String },
    Postgres { url: String },
    Mysql { url: String },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SessionBackend {
    Database,
    Redis { url: String },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum AuthBackend {
    Password {
        jwt_ed25519_secret: String,
        kyber_public_pem: String,
        kyber_secret_pem: String,
        access_ttl_secs: u64,
        refresh_ttl_secs: u64,
    },
    Pqc {
        jwt_ed25519_secret: String,
        kyber_public_pem: String,
        kyber_secret_pem: String,
        access_ttl_secs: u64,
        refresh_ttl_secs: u64,
    },
    Both {
        jwt_ed25519_secret: String,
        kyber_public_pem: String,
        kyber_secret_pem: String,
        access_ttl_secs: u64,
        refresh_ttl_secs: u64,
    },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum StorageBackend {
    Local { path: String },
    S3 { bucket: String, region: String, endpoint: Option<String> },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SearchBackend {
    Noop,
    Elasticsearch { url: String },
    Meilisearch { url: String, api_key: Option<String> },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum QueueBackend {
    InMemory,
    Redis { url: String },
    RabbitMq { url: String },
    Kafka { brokers: String, group_id: String },
}

#[derive(Debug, Clone, Deserialize)]
pub struct CacheConfig {
    pub backend: CacheBackend,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum CacheBackend {
    Noop,
    Memory,
    Redis { url: String },
}