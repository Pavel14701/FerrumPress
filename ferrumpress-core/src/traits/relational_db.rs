use async_trait::async_trait;
use uuid::Uuid;
use crate::models::user::User;
use crate::error::DbError;

#[async_trait]
pub trait RelationalDb: Send + Sync {
    async fn get_user_by_login(&self, login: &str) -> Result<Option<User>, DbError>;
    async fn get_user_by_id(&self, id: Uuid) -> Result<Option<User>, DbError>;
    async fn create_user(&self, user: &User) -> Result<User, DbError>;
    async fn update_user(&self, user: &User) -> Result<User, DbError>;
    async fn delete_user(&self, id: Uuid) -> Result<(), DbError>;
}