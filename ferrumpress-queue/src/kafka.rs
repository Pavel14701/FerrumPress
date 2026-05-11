use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{timeout, Duration};
use ferrumpress_core::error::QueueError;
use crate::{DeliverySemantics, Task, TaskQueue};

pub struct KafkaQueue {
    consumer: StreamConsumer,
    producer: FutureProducer,
    topic: String,
    pending: Arc<Mutex<HashMap<String, (i32, i64)>>>,
    semantics: DeliverySemantics,
}

impl KafkaQueue {
    pub async fn new(
        brokers: &str,
        group_id: &str,
        topic: &str,
        semantics: DeliverySemantics,
    ) -> Result<Self, QueueError> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .create()
            .map_err(|e| QueueError::Unknown(format!("kafka producer: {}", e)))?;

        let mut consumer_config = ClientConfig::new();
        consumer_config
            .set("group.id", group_id)
            .set("bootstrap.servers", brokers)
            .set("auto.offset.reset", "earliest");

        if semantics == DeliverySemantics::AtMostOnce {
            consumer_config.set("enable.auto.commit", "true");
        } else {
            consumer_config.set("enable.auto.commit", "false");
        }

        let consumer: StreamConsumer = consumer_config
            .create()
            .map_err(|e| QueueError::Unknown(format!("kafka consumer: {}", e)))?;

        consumer.subscribe(&[topic])
            .map_err(|e| QueueError::Unknown(format!("subscribe: {}", e)))?;

        Ok(Self {
            consumer,
            producer,
            topic: topic.to_string(),
            pending: Arc::new(Mutex::new(HashMap::new())),
            semantics,
        })
    }
}

#[async_trait]
impl TaskQueue for KafkaQueue {
    async fn push(&self, task: Task) -> Result<(), QueueError> {
        let payload = serde_json::to_vec(&task)
            .map_err(|e| QueueError::Serialization(e.to_string()))?;
        self.producer
            .send(
                FutureRecord::to(&self.topic).key(&task.id).payload(&payload),
                Duration::from_secs(5),
            )
            .await
            .map(|_| ())
            .map_err(|(e, _)| QueueError::Unknown(format!("kafka send: {}", e)))?;
        Ok(())
    }

    async fn pop(&self, timeout_secs: u64) -> Result<Option<Task>, QueueError> {
        let future = self.consumer.recv();
        let msg = match timeout(Duration::from_secs(timeout_secs), future).await {
            Ok(Ok(msg)) => msg,
            Ok(Err(e)) => return Err(QueueError::Unknown(format!("recv: {}", e))),
            Err(_) => return Ok(None),
        };

        let payload = msg.payload().ok_or(QueueError::Unknown("empty payload".into()))?;
        let task: Task = serde_json::from_slice(payload)
            .map_err(|e| QueueError::Serialization(e.to_string()))?;

        if self.semantics != DeliverySemantics::AtMostOnce {
            let mut pending = self.pending.lock().await;
            pending.insert(task.id.clone(), (msg.partition(), msg.offset()));
        }

        Ok(Some(task))
    }

    async fn ack(&self, task_id: &str) -> Result<(), QueueError> {
        if self.semantics == DeliverySemantics::AtMostOnce { return Ok(()); }
        let mut pending = self.pending.lock().await;
        if let Some((partition, offset)) = pending.remove(task_id) {
            // Use commit_offsets which is the correct API
            self.consumer
                .commit_offsets(&[(partition, offset + 1)], rdkafka::consumer::CommitMode::Async)
                .map_err(|e| QueueError::Unknown(format!("commit: {}", e)))?;
        }
        Ok(())
    }

    async fn nack(&self, task_id: &str) -> Result<(), QueueError> {
        if self.semantics == DeliverySemantics::AtMostOnce { return Ok(()); }
        let mut pending = self.pending.lock().await;
        pending.remove(task_id);
        Ok(())
    }
}
