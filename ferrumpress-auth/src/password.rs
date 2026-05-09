use std::sync::Arc;
use async_trait::async_trait;
use uuid::Uuid;
use chrono::{Utc, Duration};
use serde::{Serialize, Deserialize};
use jsonwebtoken::{encode, decode, Header, Algorithm, EncodingKey, DecodingKey, Validation};
use argon2::{
    password_hash::PasswordHash, PasswordVerifier, Argon2,
};
use ed25519_dalek::SigningKey;
use pqcrypto_kyber::kyber1024;
use pqcrypto_traits::kem::{SharedSecret, Ciphertext};
use aes_gcm::{Aes256Gcm, KeyInit, aead::Aead};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};

use ferrumpress_core::models::user::User;
use ferrumpress_core::models::token_pair::{TokenPair, RefreshTokenInfo};
use ferrumpress_core::traits::{AuthProvider, RelationalDb, SessionStore};
use ferrumpress_core::error::AuthError;

#[derive(Debug, Serialize, Deserialize)]
struct AccessClaims {
    sub: String,
    jti: String,
    role: String,
    exp: usize,
    iat: usize,
}

#[derive(Debug, Serialize, Deserialize)]
struct RefreshClaims {
    sub: String,
    jti: String,
    exp: usize,
    iat: usize,
}

pub struct PasswordAuthProvider {
    db: Arc<dyn RelationalDb>,
    session_store: Arc<dyn SessionStore>,
    ed25519_signing_key: SigningKey,
    ed25519_verifying_key: ed25519_dalek::VerifyingKey,
    kyber_public: kyber1024::PublicKey,
    kyber_secret: kyber1024::SecretKey,
    access_ttl: i64,
    refresh_ttl: i64,
}

impl PasswordAuthProvider {
    pub fn new(
        db: Arc<dyn RelationalDb>,
        session_store: Arc<dyn SessionStore>,
        ed25519_key: SigningKey,
        kyber_public: kyber1024::PublicKey,
        kyber_secret: kyber1024::SecretKey,
        access_ttl_secs: i64,
        refresh_ttl_secs: i64,
    ) -> Self {
        let verifying_key = ed25519_key.verifying_key();
        Self {
            db,
            session_store,
            ed25519_signing_key: ed25519_key,
            ed25519_verifying_key: verifying_key,
            kyber_public,
            kyber_secret,
            access_ttl: access_ttl_secs,
            refresh_ttl: refresh_ttl_secs,
        }
    }
}

#[async_trait]
impl AuthProvider for PasswordAuthProvider {
    async fn authenticate(&self, login: &str, password: &str) -> Result<(User, TokenPair), AuthError> {
        let user = self.db.get_user_by_login(login)
            .await
            .map_err(|e| AuthError::Internal(e.to_string()))?
            .ok_or(AuthError::InvalidCredentials)?;

        let hash = user.password_hash.as_ref()
            .ok_or(AuthError::InvalidCredentials)?;

        let parsed_hash = PasswordHash::new(hash)
            .map_err(|_| AuthError::Internal("invalid password hash".into()))?;
        Argon2::default()
            .verify_password(password.as_bytes(), &parsed_hash)
            .map_err(|_| AuthError::InvalidCredentials)?;

        let now = Utc::now();
        let access_jti = Uuid::new_v4().to_string();
        let refresh_jti = Uuid::new_v4().to_string();

        let refresh_claims = RefreshClaims {
            sub: user.id.to_string(),
            jti: refresh_jti.clone(),
            exp: (now + Duration::seconds(self.refresh_ttl)).timestamp() as usize,
            iat: now.timestamp() as usize,
        };
        let encoding_key = EncodingKey::from_ed_der(&self.ed25519_signing_key.to_keypair_bytes());
        let refresh_jwt = encode(&Header::new(Algorithm::EdDSA), &refresh_claims, &encoding_key)
            .map_err(|e| AuthError::Internal(e.to_string()))?;

        // Гибридное шифрование refresh JWT
        let (shared_secret, ciphertext_kem) = kyber1024::encapsulate(&self.kyber_public);
        let cipher = Aes256Gcm::new_from_slice(shared_secret.as_bytes())
            .map_err(|_| AuthError::Internal("AES key creation failed".into()))?;
        let nonce = aes_gcm::Nonce::from_slice(b"ferrumpress-nonce"); // в production — случайный
        let encrypted_refresh = cipher
            .encrypt(nonce, refresh_jwt.as_bytes())
            .map_err(|_| AuthError::Internal("encryption failed".into()))?;

        let mut refresh_token_bytes = Vec::new();
        refresh_token_bytes.extend_from_slice(ciphertext_kem.as_bytes());
        refresh_token_bytes.extend_from_slice(&encrypted_refresh);
        let refresh_token = BASE64.encode(&refresh_token_bytes);

        let info = RefreshTokenInfo {
            jti: refresh_jti,
            user_id: user.id,
            expires_at: now + Duration::seconds(self.refresh_ttl),
            user_agent: None,
            ip_address: None,
        };
        self.session_store.save_refresh_token(&info)
            .await
            .map_err(|e| AuthError::Internal(e.to_string()))?;

        let access_claims = AccessClaims {
            sub: user.id.to_string(),
            jti: access_jti,
            role: format!("{:?}", user.role),
            exp: (now + Duration::seconds(self.access_ttl)).timestamp() as usize,
            iat: now.timestamp() as usize,
        };
        let access_token = encode(&Header::new(Algorithm::EdDSA), &access_claims, &encoding_key)
            .map_err(|e| AuthError::Internal(e.to_string()))?;

        Ok((user, TokenPair {
            access_token,
            refresh_token,
            expires_in: self.access_ttl as u64,
        }))
    }

    async fn validate_access_token(&self, token: &str) -> Result<User, AuthError> {
        let validation = Validation::new(Algorithm::EdDSA);
        let decoding_key = DecodingKey::from_ed_der(&self.ed25519_verifying_key.to_bytes());
        let token_data = decode::<AccessClaims>(token, &decoding_key, &validation)
            .map_err(|e| match e.kind() {
                jsonwebtoken::errors::ErrorKind::ExpiredSignature => AuthError::TokenExpired,
                _ => AuthError::InvalidToken,
            })?;
        let user_id = Uuid::parse_str(&token_data.claims.sub).map_err(|_| AuthError::InvalidToken)?;
        self.db.get_user_by_id(user_id)
            .await
            .map_err(|e| AuthError::Internal(e.to_string()))?
            .ok_or(AuthError::UserNotFound)
    }

    async fn refresh_tokens(&self, refresh_token: &str) -> Result<TokenPair, AuthError> {
        let combined = BASE64.decode(refresh_token).map_err(|_| AuthError::InvalidToken)?;
        if combined.len() < 1568 {
            return Err(AuthError::InvalidToken);
        }
        let (ciphertext_kem_bytes, encrypted_refresh) = combined.split_at(1568);
        let ciphertext_kem = kyber1024::Ciphertext::from_bytes(ciphertext_kem_bytes)
            .map_err(|_| AuthError::InvalidToken)?;
        let shared_secret = kyber1024::decapsulate(&ciphertext_kem, &self.kyber_secret);

        let cipher = Aes256Gcm::new_from_slice(shared_secret.as_bytes())
            .map_err(|_| AuthError::Internal("AES key creation failed".into()))?;
        let nonce = aes_gcm::Nonce::from_slice(b"ferrumpress-nonce");
        let refresh_jwt = cipher
            .decrypt(nonce, encrypted_refresh)
            .map_err(|_| AuthError::InvalidToken)?;
        let refresh_jwt = String::from_utf8(refresh_jwt).map_err(|_| AuthError::InvalidToken)?;

        let validation = Validation::new(Algorithm::EdDSA);
        let decoding_key = DecodingKey::from_ed_der(&self.ed25519_verifying_key.to_bytes());
        let token_data = decode::<RefreshClaims>(&refresh_jwt, &decoding_key, &validation)
            .map_err(|_| AuthError::InvalidToken)?;
        let claims = token_data.claims;

        let stored = self.session_store.get_refresh_token(&claims.jti)
            .await
            .map_err(|e| AuthError::Internal(e.to_string()))?
            .ok_or(AuthError::InvalidToken)?;

        self.session_store.revoke_refresh_token(&claims.jti)
            .await
            .map_err(|e| AuthError::Internal(e.to_string()))?;

        let user = self.db.get_user_by_id(stored.user_id)
            .await
            .map_err(|e| AuthError::Internal(e.to_string()))?
            .ok_or(AuthError::UserNotFound)?;

        let now = Utc::now();
        let new_refresh_jti = Uuid::new_v4().to_string();
        let new_access_jti = Uuid::new_v4().to_string();

        let new_refresh_claims = RefreshClaims {
            sub: user.id.to_string(),
            jti: new_refresh_jti.clone(),
            exp: (now + Duration::seconds(self.refresh_ttl)).timestamp() as usize,
            iat: now.timestamp() as usize,
        };
        let encoding_key = EncodingKey::from_ed_der(&self.ed25519_signing_key.to_keypair_bytes());
        let new_refresh_jwt = encode(&Header::new(Algorithm::EdDSA), &new_refresh_claims, &encoding_key)
            .map_err(|e| AuthError::Internal(e.to_string()))?;

        let (shared_secret, ciphertext_kem) = kyber1024::encapsulate(&self.kyber_public);
        let cipher = Aes256Gcm::new_from_slice(shared_secret.as_bytes())
            .map_err(|_| AuthError::Internal("AES key creation failed".into()))?;
        let encrypted_refresh = cipher
            .encrypt(nonce, new_refresh_jwt.as_bytes())
            .map_err(|_| AuthError::Internal("encryption failed".into()))?;

        let mut refresh_token_bytes = Vec::new();
        refresh_token_bytes.extend_from_slice(ciphertext_kem.as_bytes());
        refresh_token_bytes.extend_from_slice(&encrypted_refresh);
        let new_refresh_token = BASE64.encode(&refresh_token_bytes);

        let new_access_claims = AccessClaims {
            sub: user.id.to_string(),
            jti: new_access_jti,
            role: format!("{:?}", user.role),
            exp: (now + Duration::seconds(self.access_ttl)).timestamp() as usize,
            iat: now.timestamp() as usize,
        };
        let new_access_token = encode(&Header::new(Algorithm::EdDSA), &new_access_claims, &encoding_key)
            .map_err(|e| AuthError::Internal(e.to_string()))?;

        let info = RefreshTokenInfo {
            jti: new_refresh_jti,
            user_id: user.id,
            expires_at: now + Duration::seconds(self.refresh_ttl),
            user_agent: None,
            ip_address: None,
        };
        self.session_store.save_refresh_token(&info).await
            .map_err(|e| AuthError::Internal(e.to_string()))?;

        Ok(TokenPair {
            access_token: new_access_token,
            refresh_token: new_refresh_token,
            expires_in: self.access_ttl as u64,
        })
    }

    async fn revoke_session(&self, refresh_token: &str) -> Result<(), AuthError> {
        let combined = BASE64.decode(refresh_token).map_err(|_| AuthError::InvalidToken)?;
        if combined.len() < 1568 { return Err(AuthError::InvalidToken); }
        let (ciphertext_kem_bytes, encrypted_refresh) = combined.split_at(1568);
        let ciphertext_kem = kyber1024::Ciphertext::from_bytes(ciphertext_kem_bytes)
            .map_err(|_| AuthError::InvalidToken)?;
        let shared_secret = kyber1024::decapsulate(&ciphertext_kem, &self.kyber_secret);

        let cipher = Aes256Gcm::new_from_slice(shared_secret.as_bytes())
            .map_err(|_| AuthError::Internal("AES key creation failed".into()))?;
        let nonce = aes_gcm::Nonce::from_slice(b"ferrumpress-nonce");
        let refresh_jwt = cipher
            .decrypt(nonce, encrypted_refresh)
            .map_err(|_| AuthError::InvalidToken)?;
        let refresh_jwt = String::from_utf8(refresh_jwt).map_err(|_| AuthError::InvalidToken)?;

        let validation = Validation::new(Algorithm::EdDSA);
        let decoding_key = DecodingKey::from_ed_der(&self.ed25519_verifying_key.to_bytes());
        let token_data = decode::<RefreshClaims>(&refresh_jwt, &decoding_key, &validation)
            .map_err(|_| AuthError::InvalidToken)?;
        self.session_store.revoke_refresh_token(&token_data.claims.jti)
            .await
            .map_err(|e| AuthError::Internal(e.to_string()))?;
        Ok(())
    }

    async fn logout_all(&self, user_id: Uuid) -> Result<(), AuthError> {
        self.session_store.revoke_all_for_user(user_id)
            .await
            .map_err(|e| AuthError::Internal(e.to_string()))?;
        Ok(())
    }
}