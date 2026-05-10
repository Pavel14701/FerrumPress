use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: String,
    pub kind: String,       // e.g., "resize_image", "index_post"
    pub payload: Vec<u8>,   // serialized data (JSON, MessagePack)
    pub priority: u8,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum DeliverySemantics {
    AtMostOnce,
    AtLeastOnce,
    ExactlyOnce,  // требует идемпотентности + поддержки брокера (транзакции)
}
