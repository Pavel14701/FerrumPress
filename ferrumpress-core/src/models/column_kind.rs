use serde::{Deserialize, Serialize};
use async_trait::async_trait;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnKind {
    Text,
    Varchar(usize),
    Char(usize),
    SmallInt,
    Integer,
    BigInt,
    Serial,
    Float,
    Double,
    Boolean,
    Timestamp,
    Date,
    Time,
    DateTime,
    Blob,
    VarBinary(usize),
    Uuid,
    Json,
    Xml,
    Decimal(usize, usize),
    Money,
    Array(Box<ColumnKind>),
    Enum(Vec<String>),
    Custom(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub kind: ColumnKind,
    pub nullable: bool,
    pub unique: bool,
    pub default: Option<String>,
}

pub trait Model: Send + Sync {
    fn table_name(&self) -> &str;
    fn columns(&self) -> Vec<ColumnInfo>;
    fn primary_keys(&self) -> Vec<String>;
}

#[async_trait]
pub trait ModelService: Send + Sync {
    async fn find_by_id(&self, id: &Value) -> Result<Option<Value>, String>;
    async fn find_all(&self, limit: Option<u32>, offset: Option<u32>) -> Result<Vec<Value>, String>;
    async fn insert(&self, data: Value) -> Result<Value, String>;
    async fn update(&self, id: &Value, data: Value) -> Result<Value, String>;
    async fn delete(&self, id: &Value) -> Result<u64, String>;
}