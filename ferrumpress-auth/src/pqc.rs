#[cfg(feature = "pqc")]
use std::sync::Arc;
#[cfg(feature = "pqc")]
use async_trait::async_trait;
#[cfg(feature = "pqc")]
use uuid::Uuid;
#[cfg(feature = "pqc")]
use chrono::{Utc, Duration};
#[cfg(feature = "pqc")]
use serde::{Serialize, Deserialize};
#[cfg(feature = "pqc")]
use jsonwebtoken::{encode, decode, Header, Algorithm, EncodingKey, DecodingKey, Validation};
#[cfg(feature = "pqc")]
use ed25519_dalek::SigningKey;
#[cfg(feature = "pqc")]
use pqcrypto_kyber::kyber1024;
#[cfg(feature = "pqc")]
use pqcrypto_dilithium::dilithium3;
#[cfg(feature = "pqc")]
use aes_gcm::{Aes256Gcm, KeyInit, aead::Aead};
#[cfg(feature = "pqc")]
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
#[cfg(feature = "pqc")]
use rand::Rng;
#[cfg(feature = "pqc")]
use tokio::sync::Mutex;
#[cfg(feature = "pqc")]
use std::collections::HashMap;

#[cfg(feature = "pqc")]
use ferrumpress_core::models::user::User;
#[cfg(feature = "pqc")]
use ferrumpress_core::models::token_pair::{TokenPair, RefreshTokenInfo};
#[cfg(feature = "pqc")]
use ferrumpress_core::traits::{AuthProvider, RelationalDb, SessionStore};
#[cfg(feature = "pqc")]
use ferrumpress_core::error::AuthError;

#[cfg(feature = "pqc")]
#[derive(Debug, Serialize, Deserialize)]
struct AccessClaims {
    sub: String,
    jti: String,
    role: String,
    exp: usize,
    iat: usize,
}

#[cfg(feature = "pqc")]
#[derive(Debug, Serialize, Deserialize)]
struct RefreshClaims {
    sub: String,
    jti: String,
    exp: usize,
    iat: usize,
}

#[cfg(feature = "pqc")]
pub struct PqcAuthProvider {
    db: Arc<dyn RelationalDb>,
    session_store: Arc<dyn SessionStore>,
    ed25519_signing_key: SigningKey,
    ed25519_verifying_key: ed25519_dalek::VerifyingKey,
    kyber_public: kyber1024::PublicKey,
    kyber_secret: kyber1024::SecretKey,
    access_ttl: i64,
    refresh_ttl: i64,
    nonces: Mutex<HashMap<String, (String, chrono::DateTime<Utc>)>>,
}

#[cfg(feature = "pqc")]
#[derive(Deserialize)]
pub struct PqcLoginRequest {
    pub user_id: Uuid,
    pub nonce: String,
    pub signature: String,
}

#[cfg(feature = "pqc")]
impl PqcAuthProvider {
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
            nonces: Mutex::new(HashMap::new()),
        }
    }

    pub async fn generate_challenge(&self, user_id: Uuid) -> String {
        let nonce: String = rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(32)
            .map(char::from)
            .collect();
        let mut map = self.nonces.lock().await;
        map.insert(nonce.clone(), (user_id.to_string(), Utc::now() + Duration::seconds(60)));
        nonce
    }

    pub async fn verify_pqc_login(&self, req: PqcLoginRequest) -> Result<(User, TokenPair), AuthError> {
        let mut map = self.nonces.lock().await;
        let (uid_str, expires) = map.remove(&req.nonce).ok_or(AuthError::InvalidToken)?;
        if Utc::now() > expires {
            return Err(AuthError::TokenExpired);
        }
        if uid_str != req.user_id.to_string() {
            return Err(AuthError::InvalidToken);
        }

        let user = self.db.get_user_by_id(req.user_id).await
            .map_err(|e| AuthError::Internal(e.to_string()))?
            .ok_or(AuthError::UserNotFound)?;
        let pubkey_b64 = user.dilithium_public_key.as_ref()
            .ok_or(AuthError::InvalidCredentials)?;
        let pubkey_bytes = BASE64.decode(pubkey_b64).map_err(|_| AuthError::InvalidToken)?;
        let pubkey = dilithium3::PublicKey::from_bytes(&pubkey_bytes)
            .map_err(|_| AuthError::InvalidCredentials)?;

        let mut msg = Vec::new();
        msg.extend_from_slice(req.nonce.as_bytes());
        msg.extend_from_slice(req.user_id.to_string().as_bytes());

        let sig_bytes = BASE64.decode(&req.signature).map_err(|_| AuthError::InvalidToken)?;
        let sig = dilithium3::SignedMessage::from_bytes(&sig_bytes)
            .map_err(|_| AuthError::InvalidToken)?;
        pqcrypto_dilithium::dilithium3::verify(&msg, &sig, &pubkey)
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

        let refresh_token = self.encrypt_refresh_token(&refresh_jwt).await?;

        let info = RefreshTokenInfo {
            jti: refresh_jti,
            user_id: user.id,
            expires_at: now + Duration::seconds(self.refresh_ttl),
            user_agent: None,
            ip_address: None,
        };
        self.session_store.save_refresh_token(&info).await
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

    /// Шифрует refresh JWT с помощью Kyber + AES-GCM
    async fn encrypt_refresh_token(&self, jwt: &str) -> Result<String, AuthError> {
        let (shared_secret, ciphertext_kem) = kyber1024::encapsulate(&self.kyber_public);
        let cipher = Aes256Gcm::new_from_slice(shared_secret.as_bytes())
            .map_err(|_| AuthError::Internal("AES key creation failed".into()))?;
        let nonce = aes_gcm::Nonce::from_slice(b"ferrumpress-nonce");
        let encrypted = cipher
            .encrypt(nonce, jwt.as_bytes())
            .map_err(|_| AuthError::Internal("encryption failed".into()))?;
        let mut combined = Vec::new();
        combined.extend_from_slice(ciphertext_kem.as_bytes());
        combined.extend_from_slice(&encrypted);
        Ok(BASE64.encode(&combined))
    }

    /// Расшифровывает refresh-токен, возвращает исходный JWT
    async fn decrypt_refresh_token(&self, token: &str) -> Result<String, AuthError> {
        let combined = BASE64.decode(token).map_err(|_| AuthError::InvalidToken)?;
        if combined.len() < kyber1024::CIPHERTEXTBYTES {
            return Err(AuthError::InvalidToken);
        }
        let (kem_bytes, encrypted) = combined.split_at(kyber1024::CIPHERTEXTBYTES);
        let ciphertext = kyber1024::Ciphertext::from_bytes(kem_bytes)
            .map_err(|_| AuthError::InvalidToken)?;
        let shared_secret = kyber1024::decapsulate(&ciphertext, &self.kyber_secret);
        let cipher = Aes256Gcm::new_from_slice(shared_secret.as_bytes())
            .map_err(|_| AuthError::Internal("AES key creation failed".into()))?;
        let nonce = aes_gcm::Nonce::from_slice(b"ferrumpress-nonce");
        let decrypted = cipher
            .decrypt(nonce, encrypted)
            .map_err(|_| AuthError::InvalidToken)?;
        String::from_utf8(decrypted).map_err(|_| AuthError::InvalidToken)
    }
}

#[cfg(feature = "pqc")]
#[async_trait]
impl AuthProvider for PqcAuthProvider {
    async fn authenticate(&self, _login: &str, _password: &str) -> Result<(User, TokenPair), AuthError> {
        // Парольный вход не поддерживается для этого провайдера
        Err(AuthError::InvalidCredentials)
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
        let refresh_jwt = self.decrypt_refresh_token(refresh_token).await?;

        let validation = Validation::new(Algorithm::EdDSA);
        let decoding_key = DecodingKey::from_ed_der(&self.ed25519_verifying_key.to_bytes());
        let token_data = decode::<RefreshClaims>(&refresh_jwt, &decoding_key, &validation)
            .map_err(|_| AuthError::InvalidToken)?;
        let claims = token_data.claims;

        let stored = self.session_store.get_refresh_token(&claims.jti)
            .await
            .map_err(|e| AuthError::Internal(e.to_string()))?
            .ok_or(AuthError::InvalidToken)?;

        // Ротация: удаляем старый токен
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

        let new_refresh_token = self.encrypt_refresh_token(&new_refresh_jwt).await?;

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
        let refresh_jwt = self.decrypt_refresh_token(refresh_token).await?;
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