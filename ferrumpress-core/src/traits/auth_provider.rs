use async_trait::async_trait;
use uuid::Uuid;
use crate::models::user::User;
use crate::models::token_pair::TokenPair;
use crate::error::AuthError;

#[async_trait]
pub trait AuthProvider: Send + Sync {
    /// Login with credentials, returns user and token pair
    async fn authenticate(&self, login: &str, password: &str) -> Result<(User, TokenPair), AuthError>;
    /// Validate access token and return user
    async fn validate_access_token(&self, token: &str) -> Result<User, AuthError>;
    /// Exchange refresh token for new token pair
    async fn refresh_tokens(&self, refresh_token: &str) -> Result<TokenPair, AuthError>;
    /// Revoke a specific refresh token session
    async fn revoke_session(&self, refresh_token: &str) -> Result<(), AuthError>;
    /// Logout all sessions for a user
    async fn logout_all(&self, user_id: Uuid) -> Result<(), AuthError>;
}