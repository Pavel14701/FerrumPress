use ed25519_dalek::SigningKey;
use pqcrypto_kyber::kyber1024;
use pqcrypto_traits::kem::{PublicKey, SecretKey};
use std::fs;

pub fn load_ed25519_keypair(path: &str) -> Result<SigningKey, Box<dyn std::error::Error>> {
    let pem_str = fs::read_to_string(path)?;
    let pem = pem::parse(&pem_str)?;
    let key = SigningKey::from_bytes(&pem.contents().try_into().unwrap());
    Ok(key)
}

pub fn generate_ed25519_keypair(path: &str) -> Result<SigningKey, Box<dyn std::error::Error>> {
    use rand::Rng;
    let mut seed = [0u8; 32];
    rand::rng().fill_bytes(&mut seed);
    let signing_key = SigningKey::from_bytes(&seed);
    let pem_data = pem::Pem::new("PRIVATE KEY", signing_key.to_bytes().to_vec());
    fs::write(path, pem::encode(&pem_data))?;
    Ok(signing_key)
}

pub fn load_kyber_keypair(
    public_path: &str, secret_path: &str
) -> Result<(kyber1024::PublicKey, kyber1024::SecretKey), Box<dyn std::error::Error>> {
    let pub_bytes = fs::read(public_path)?;
    let sec_bytes = fs::read(secret_path)?;
    let public = kyber1024::PublicKey::from_bytes(&pub_bytes).map_err(|_| "invalid public key")?;
    let secret = kyber1024::SecretKey::from_bytes(&sec_bytes).map_err(|_| "invalid secret key")?;
    Ok((public, secret))
}

pub fn generate_kyber_keypair(
    public_path: &str, secret_path: &str
) -> Result<(), Box<dyn std::error::Error>> {
    let (pk, sk) = kyber1024::keypair();
    fs::write(public_path, pk.as_bytes())?;
    fs::write(secret_path, sk.as_bytes())?;
    Ok(())
}