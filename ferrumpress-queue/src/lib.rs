// Трейты из ядра
pub use ferrumpress_core::traits::task_queue::{
    IdempotencyStore, TaskHandler, TaskQueue,
};
pub use ferrumpress_core::models::{DeliverySemantics, Task};

// Бэкенды очередей
#[cfg(feature = "redis_queue")]
pub mod redis_queue;
#[cfg(feature = "redis_queue")]
pub use redis_queue::RedisQueue;

#[cfg(feature = "rabbitmq")]
pub mod rabbitmq;
#[cfg(feature = "rabbitmq")]
pub use rabbitmq::RabbitMqQueue;

#[cfg(feature = "kafka")]
pub mod kafka;
#[cfg(feature = "kafka")]
pub use kafka::KafkaQueue;

// Хранилища идемпотентности
#[cfg(feature = "idempotency-redis")]
pub mod idempotency_redis;
#[cfg(feature = "idempotency-redis")]
pub use idempotency_redis::RedisIdempotencyStore;

#[cfg(feature = "idempotency-db")]
pub mod idempotency_db;
#[cfg(feature = "idempotency-db")]
pub use idempotency_db::DatabaseIdempotencyStore;