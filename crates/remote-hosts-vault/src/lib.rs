//! Internal encrypted credential vault.

use argon2::{Algorithm, Argon2, Params, Version};
use base64::{Engine as _, engine::general_purpose::STANDARD_NO_PAD};
use chacha20poly1305::{
    XChaCha20Poly1305, XNonce,
    aead::{Aead, Generate, KeyInit},
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

/// Secret material stored inside the encrypted credential blob.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Zeroize, ZeroizeOnDrop)]
pub struct CredentialSecret {
    /// SSH password.
    pub password: Option<String>,
    /// PEM or OpenSSH private key.
    pub private_key_pem: Option<String>,
    /// Private key passphrase.
    pub private_key_passphrase: Option<String>,
    /// sudo password.
    pub sudo_password: Option<String>,
    /// Authenticate through the connector process's SSH agent.
    #[serde(default)]
    pub use_ssh_agent: bool,
}

/// KDF parameters persisted with the encrypted blob.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct KdfParams {
    /// KDF name.
    pub algorithm: String,
    /// Argon2 memory cost in KiB.
    pub memory_cost_kib: u32,
    /// Argon2 time cost.
    pub time_cost: u32,
    /// Argon2 parallelism.
    pub parallelism: u32,
    /// Output length in bytes.
    pub output_len: usize,
    /// Base64-encoded random salt.
    pub salt_b64: String,
}

/// Encrypted credential blob stored in the database.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct EncryptedCredentialBlob {
    /// Blob version.
    pub version: u16,
    /// AEAD algorithm.
    pub aead: String,
    /// Base64-encoded nonce.
    pub nonce_b64: String,
    /// Base64-encoded ciphertext.
    pub ciphertext_b64: String,
    /// KDF parameters.
    pub kdf: KdfParams,
}

/// Vault errors.
#[derive(Debug, Error)]
pub enum VaultError {
    /// KDF failed.
    #[error("key derivation failed")]
    Kdf,
    /// Encryption failed.
    #[error("encryption failed")]
    Encrypt,
    /// Decryption failed.
    #[error("decryption failed")]
    Decrypt,
    /// Base64 decoding failed.
    #[error("base64 decode failed: {0}")]
    Base64(#[from] base64::DecodeError),
    /// JSON encoding or decoding failed.
    #[error("json failed: {0}")]
    Json(#[from] serde_json::Error),
    /// Unsupported blob algorithm or version.
    #[error("unsupported vault blob: {0}")]
    Unsupported(String),
}

#[derive(Zeroize, ZeroizeOnDrop)]
struct DerivedKey {
    material: [u8; 32],
}

/// Stateless credential vault helper.
pub struct CredentialVault;

impl CredentialVault {
    /// Encrypts a credential secret with a master password.
    ///
    /// # Errors
    ///
    /// Returns an error if key derivation, JSON serialization, or AEAD encryption fails.
    pub fn encrypt(
        master_password: &SecretString,
        secret: &CredentialSecret,
    ) -> Result<EncryptedCredentialBlob, VaultError> {
        let salt = *Uuid::new_v4().as_bytes();
        let kdf = KdfParams {
            algorithm: "argon2id".to_owned(),
            memory_cost_kib: 64 * 1024,
            time_cost: 3,
            parallelism: 1,
            output_len: 32,
            salt_b64: STANDARD_NO_PAD.encode(salt),
        };

        let key = derive_key(master_password, &kdf)?;
        let cipher =
            XChaCha20Poly1305::new_from_slice(&key.material).map_err(|_| VaultError::Encrypt)?;
        let nonce = XNonce::generate();
        let plaintext = Zeroizing::new(serde_json::to_vec(secret)?);
        let ciphertext = cipher
            .encrypt(&nonce, plaintext.as_slice())
            .map_err(|_| VaultError::Encrypt)?;

        Ok(EncryptedCredentialBlob {
            version: 1,
            aead: "xchacha20poly1305".to_owned(),
            nonce_b64: STANDARD_NO_PAD.encode(nonce.as_slice()),
            ciphertext_b64: STANDARD_NO_PAD.encode(ciphertext),
            kdf,
        })
    }

    /// Decrypts a credential secret with a master password.
    ///
    /// # Errors
    ///
    /// Returns an error if the blob is unsupported, malformed, cannot be authenticated, or cannot
    /// be decoded.
    pub fn decrypt(
        master_password: &SecretString,
        blob: &EncryptedCredentialBlob,
    ) -> Result<CredentialSecret, VaultError> {
        if blob.version != 1 {
            return Err(VaultError::Unsupported(format!("version {}", blob.version)));
        }
        if blob.aead != "xchacha20poly1305" {
            return Err(VaultError::Unsupported(blob.aead.clone()));
        }

        let key = derive_key(master_password, &blob.kdf)?;
        let cipher =
            XChaCha20Poly1305::new_from_slice(&key.material).map_err(|_| VaultError::Decrypt)?;
        let nonce_bytes: [u8; 24] = STANDARD_NO_PAD
            .decode(&blob.nonce_b64)?
            .try_into()
            .map_err(|_| VaultError::Decrypt)?;
        let nonce = XNonce::from(nonce_bytes);
        let ciphertext = STANDARD_NO_PAD.decode(&blob.ciphertext_b64)?;
        let plaintext = Zeroizing::new(
            cipher
                .decrypt(&nonce, ciphertext.as_slice())
                .map_err(|_| VaultError::Decrypt)?,
        );

        serde_json::from_slice(&plaintext).map_err(VaultError::from)
    }
}

fn derive_key(master_password: &SecretString, kdf: &KdfParams) -> Result<DerivedKey, VaultError> {
    if kdf.algorithm != "argon2id" || kdf.output_len != 32 {
        return Err(VaultError::Unsupported(kdf.algorithm.clone()));
    }

    let salt = STANDARD_NO_PAD.decode(&kdf.salt_b64)?;
    let params = Params::new(
        kdf.memory_cost_kib,
        kdf.time_cost,
        kdf.parallelism,
        Some(kdf.output_len),
    )
    .map_err(|_| VaultError::Kdf)?;
    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let mut material = [0_u8; 32];
    argon2
        .hash_password_into(
            master_password.expose_secret().as_bytes(),
            &salt,
            &mut material,
        )
        .map_err(|_| VaultError::Kdf)?;
    Ok(DerivedKey { material })
}

#[cfg(test)]
mod tests {
    use secrecy::SecretString;

    use super::{CredentialSecret, CredentialVault, VaultError};

    fn sample_secret() -> CredentialSecret {
        CredentialSecret {
            password: Some("ssh-password".to_owned()),
            private_key_pem: None,
            private_key_passphrase: None,
            sudo_password: Some("sudo-password".to_owned()),
            use_ssh_agent: false,
        }
    }

    #[test]
    fn credential_blob_round_trips() -> Result<(), VaultError> {
        let master = SecretString::from("correct horse battery staple".to_owned());
        let secret = sample_secret();

        let blob = CredentialVault::encrypt(&master, &secret)?;
        let decrypted = CredentialVault::decrypt(&master, &blob)?;

        assert!(decrypted == secret);
        assert!(!blob.ciphertext_b64.contains("ssh-password"));
        Ok(())
    }

    #[test]
    fn wrong_master_password_cannot_decrypt() -> Result<(), VaultError> {
        let master = SecretString::from("correct horse battery staple".to_owned());
        let wrong = SecretString::from("wrong password".to_owned());
        let blob = CredentialVault::encrypt(&master, &sample_secret())?;

        let result = CredentialVault::decrypt(&wrong, &blob);

        assert!(matches!(result, Err(VaultError::Decrypt)));
        Ok(())
    }
}
