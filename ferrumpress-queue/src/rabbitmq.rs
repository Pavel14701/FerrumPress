use async_trait::async_trait;
use lapin::{options::*, types::FieldTable, BasicProperties, Channel, Connection, ConnectionProperties, Consumer};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{timeout, Duration};
use ferrumpress_core::error::QueueError;
use crate::{DeliverySemantics, Task, TaskQueue};

pub struct RabbitMqQueue {
    channel: Channel,
    consumer: Arc<Mutex<Consumer>>,
    tag_to_id: Arc<Mutex<HashMap<u64, String>>>,
    semantics: DeliverySemantics,
}

impl RabbitMqQueue {
    pub async fn new(
        amqp_url: &str,
        queue_name: &str,
        semantics: DeliverySemantics,
    ) -> Result<Self, QueueError> {
        let conn = Connection::connect(amqp_url, ConnectionProperties::default())
            .await
            .map_err(|e| QueueError::Internal(format!("RabbitMQ connection: {}", e)))?;
        let channel = conn.create_channel()
            .await
            .map_err(|e| QueueError::Internal(format!("create channel: {}", e)))?;

        let consumer = match semantics {
            DeliverySemantics::AtMostOnce => {
                channel
                    .basic_consume(queue_name, "ferrumpress_media", BasicConsumeOptions { no_ack: true, ..Default::default() }, FieldTable::default())
                    .await
            }
            _ => {
                channel
                    .basic_consume(queue_name, "ferrumpress_media", BasicConsumeOptions::default(), FieldTable::default())
                    .await
            }
        }
        .map_err(|e| QueueError::Internal(format!("consumer: {}", e)))?;

        Ok(Self {
            channel,
            consumer: Arc::new(Mutex::new(consumer)),
            tag_to_id: Arc::new(Mutex::new(HashMap::new())),
            semantics,
        })
    }
}

#[async_trait]
impl TaskQueue for RabbitMqQueue {
    async fn push(&self, task: Task) -> Result<(), QueueError> {
        let payload = serde_json::to_vec(&task)
            .map_err(|e| QueueError::Serialization(e.to_string()))?;
        self.channel
            .basic_publish(
                "",    // exchange (по умолчанию)
                "",    // routing key (по умолчанию)
                BasicPublishOptions::default(),
                &payload,
                BasicProperties::default(),
            )
            .await
            .map_err(|e| QueueError::Internal(format!("publish: {}", e)))?;
        Ok(())
    }

    async fn pop(&self, timeout_secs: u64) -> Result<Option<Task>, QueueError> {
        let consumer = self.consumer.clone();
        let mut guard = consumer.lock().await;
        let recv = guard.next();
        let delivery = match timeout(Duration::from_secs(timeout_secs), recv).await {
            Ok(Some(Ok(delivery))) => delivery,
            Ok(Some(Err(e))) => return Err(QueueError::Internal(format!("delivery error: {}", e))),
            Ok(None) | Err(_) => return Ok(None),
        };

        let task: Task = serde_json::from_slice(&delivery.data)
            .map_err(|e| QueueError::Serialization(e.to_string()))?;

        if self.semantics != DeliverySemantics::AtMostOnce {
            let mut map = self.tag_to_id.lock().await;
            map.insert(delivery.delivery_tag, task.id.clone());
        }

        Ok(Some(task))
    }

    async fn ack(&self, task_id: &str) -> Result<(), QueueError> {
        if self.semantics == DeliverySemantics::AtMostOnce { return Ok(()); }
        let mut map = self.tag_to_id.lock().await;
        if let Some(tag) = map.remove(task_id) {
            self.channel
                .basic_ack(tag, BasicAckOptions::default())
                .await
                .map_err(|e| QueueError::Internal(format!("ack: {}", e)))?;
        }
        Ok(())
    }

    async fn nack(&self, task_id: &str) -> Result<(), QueueError> {
        if self.semantics == DeliverySemantics::AtMostOnce { return Ok(()); }
        let mut map = self.tag_to_id.lock().await;
        if let Some(tag) = map.remove(task_id) {
            self.channel
                .basic_nack(tag, false, true, BasicNackOptions::default())
                .await
                .map_err(|e| QueueError::Internal(format!("nack: {}", e)))?;
        }
        Ok(())
    }
}