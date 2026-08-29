//! Direct, durable metadata synchronization between approved Remote Hosts instances.
//!
//! The crate intentionally synchronizes only durable, non-secret records. SSH transports,
//! credentials, workspaces, PTYs, operation queues, and their audit streams stay local.

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use base64::{Engine as _, engine::general_purpose::STANDARD_NO_PAD};
use chacha20poly1305::{
    XChaCha20Poly1305, XNonce,
    aead::{Aead, Generate, KeyInit},
};
use remote_hosts_db::{DbError, Repositories};
use remote_hosts_domain::{
    AccessPath, AccessPathId, ConnectionMode, CredentialId, CredentialKind, CredentialMetadata,
    Host, HostId, InstanceIdentity, InstancePeer, InstancePeerId, InstancePeerState,
    InstanceSyncCollection, InstanceSyncConflict, InstanceSyncEnvelope, InstanceSyncRecord,
    InstanceSyncRecordDisposition, InstanceSyncResult, KnowledgeItem, KnowledgeItemId, Protocol,
    RouteType, now_utc,
};
use remote_hosts_vault::{CredentialSecret, CredentialVault, EncryptedCredentialBlob};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;
use zeroize::Zeroizing;

/// Instance-sync protocol version implemented by this crate.
pub const PROTOCOL_VERSION: u16 = 1;
/// Header carrying an approved peer's bearer token.
pub const PEER_TOKEN_HEADER: &str = "x-remote-hosts-sync-token";
const MAX_RECORDS_PER_ENVELOPE: usize = 1_000;
const PEER_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const CREDENTIAL_SEAL_AEAD: &str = "xchacha20poly1305";

/// Result of requesting an export from a peer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InstanceSyncExportRequest {
    /// Collections requested by the peer.
    pub collections: Vec<InstanceSyncCollection>,
    /// Receiver identity when already known.
    pub recipient_instance_id: Option<Uuid>,
}

/// Error raised by the instance-sync service.
#[derive(Debug, Error)]
pub enum InstanceSyncError {
    /// Durable storage failed.
    #[error("instance sync database error: {0}")]
    Database(#[from] DbError),
    /// A peer is not allowed to exchange data.
    #[error("peer is not active: {0}")]
    PeerInactive(String),
    /// The configured peer is missing its outbound token.
    #[error("peer credential is unavailable: {0}")]
    Credential(String),
    /// A peer request failed.
    #[error("peer request failed: {0}")]
    Request(#[from] reqwest::Error),
    /// A received protocol payload is invalid.
    #[error("invalid instance-sync payload: {0}")]
    InvalidPayload(String),
}

/// One compact, agent-visible result for an outgoing synchronization call.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InstanceSyncPushReport {
    /// Peer label.
    pub peer: String,
    /// Peer identity learned from the response.
    pub peer_instance_id: Option<Uuid>,
    /// Number of records sent.
    pub sent: u32,
    /// Receiver application result.
    pub result: InstanceSyncResult,
}

/// Receives and sends approved instance-sync envelopes.
#[derive(Clone)]
pub struct InstanceSyncService {
    repositories: Arc<Repositories>,
    vault_master_password: Option<Arc<SecretString>>,
    instance_display_name: Arc<String>,
    http_client: reqwest::Client,
}

impl InstanceSyncService {
    /// Creates a service without outbound vault access. It can serve inbound peer requests.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTPS client cannot be initialized.
    pub fn new(repositories: Repositories) -> Result<Self, InstanceSyncError> {
        Self::with_vault_master_password(repositories, None)
    }

    /// Creates a service with optional vault access for direct outgoing peer requests.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTPS client cannot be initialized.
    pub fn with_vault_master_password(
        repositories: Repositories,
        vault_master_password: Option<SecretString>,
    ) -> Result<Self, InstanceSyncError> {
        let instance_display_name = std::env::var("REMOTE_HOSTS_INSTANCE_NAME")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| std::env::var("HOSTNAME").ok())
            .unwrap_or_else(|| "remote-hosts".to_owned());
        let http_client = reqwest::Client::builder()
            .use_rustls_tls()
            .http2_adaptive_window(true)
            .timeout(PEER_REQUEST_TIMEOUT)
            .build()?;
        Ok(Self {
            repositories: Arc::new(repositories),
            vault_master_password: vault_master_password.map(Arc::new),
            instance_display_name: Arc::new(instance_display_name),
            http_client,
        })
    }

    /// Returns the initialized local instance identity.
    ///
    /// # Errors
    ///
    /// Returns an error if the identity cannot be read or initialized.
    pub async fn identity(&self) -> Result<InstanceIdentity, InstanceSyncError> {
        Ok(self
            .repositories
            .instance_sync
            .get_or_create_identity(&self.instance_display_name, now_utc())
            .await?)
    }

    /// Lists configured peers without revealing their outbound tokens.
    ///
    /// # Errors
    ///
    /// Returns an error if peer state cannot be read.
    pub async fn list_peers(&self) -> Result<Vec<InstancePeer>, InstanceSyncError> {
        Ok(self.repositories.instance_sync.list_peers().await?)
    }

    /// Creates or replaces one explicit local peer configuration without returning its token.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid endpoint or collection, an unavailable vault, or failed
    /// credential and peer persistence.
    pub async fn configure_peer(
        &self,
        display_name: String,
        endpoint: String,
        token: SecretString,
        allowed_collections: Vec<InstanceSyncCollection>,
    ) -> Result<InstancePeer, InstanceSyncError> {
        let display_name = normalize_peer_label(&display_name)?;
        let endpoint = normalize_peer_endpoint(&endpoint)?;
        let allowed_collections = normalize_collections(&allowed_collections)?
            .into_iter()
            .collect::<Vec<_>>();
        let master = self.vault_master_password.as_deref().ok_or_else(|| {
            InstanceSyncError::Credential(
                "configure a local vault master password for peer sync".to_owned(),
            )
        })?;
        let existing = self
            .repositories
            .instance_sync
            .get_peer_by_display_name(&display_name)
            .await?;
        let credential_name = format!("instance-sync:{display_name}");
        let existing_credential = if let Some(peer) = &existing {
            self.repositories
                .credentials
                .get(peer.outbound_credential_id)
                .await?
        } else {
            self.repositories
                .credentials
                .get_by_name(&credential_name)
                .await?
        };
        let now = now_utc();
        let metadata = CredentialMetadata {
            id: existing_credential
                .as_ref()
                .map_or_else(CredentialId::new, |credential| credential.metadata.id),
            name: credential_name,
            kind: CredentialKind::GenericSecret,
            username_hint: None,
            created_at: existing_credential
                .as_ref()
                .map_or(now, |credential| credential.metadata.created_at),
            updated_at: now,
            last_used_at: existing_credential
                .as_ref()
                .and_then(|credential| credential.metadata.last_used_at),
        };
        let token_value = token.expose_secret().to_owned();
        let encrypted_blob = tokio::task::spawn_blocking({
            let master = master.clone();
            move || {
                CredentialVault::encrypt(
                    &master,
                    &CredentialSecret {
                        password: None,
                        private_key_pem: None,
                        private_key_passphrase: None,
                        sudo_password: None,
                        token: Some(token_value),
                        secret_text: None,
                        use_ssh_agent: false,
                    },
                )
            }
        })
        .await
        .map_err(|_| {
            InstanceSyncError::Credential("peer credential encryption task failed".to_owned())
        })?
        .map_err(|_| {
            InstanceSyncError::Credential("peer credential cannot be encrypted".to_owned())
        })?;
        self.repositories
            .credentials
            .upsert(&remote_hosts_domain::StoredCredential {
                metadata: metadata.clone(),
                encrypted_blob_json: serde_json::to_value(encrypted_blob)
                    .map_err(|error| InstanceSyncError::InvalidPayload(error.to_string()))?,
            })
            .await?;
        let peer = InstancePeer {
            id: existing
                .as_ref()
                .map_or_else(InstancePeerId::new, |peer| peer.id),
            peer_instance_id: existing.as_ref().and_then(|peer| peer.peer_instance_id),
            display_name,
            endpoint,
            outbound_credential_id: metadata.id,
            inbound_token_sha256: token_sha256(token.expose_secret()),
            allowed_collections,
            state: InstancePeerState::Active,
            last_pushed_at: existing.as_ref().and_then(|peer| peer.last_pushed_at),
            last_pulled_at: existing.as_ref().and_then(|peer| peer.last_pulled_at),
            last_error: None,
            created_at: existing.as_ref().map_or(now, |peer| peer.created_at),
            updated_at: now,
        };
        self.repositories.instance_sync.upsert_peer(&peer).await?;
        Ok(peer)
    }

    /// Builds one deterministic bounded envelope for the requested durable collections.
    ///
    /// # Errors
    ///
    /// Returns an error for unsupported collections, invalid payload serialization, or failed
    /// inventory and knowledge reads.
    pub async fn export(
        &self,
        collections: &[InstanceSyncCollection],
        recipient_instance_id: Option<Uuid>,
    ) -> Result<InstanceSyncEnvelope, InstanceSyncError> {
        self.export_with_peer_token(collections, recipient_instance_id, None)
            .await
    }

    /// Builds an envelope for an authenticated approved peer, sealing credentials when selected.
    ///
    /// # Errors
    ///
    /// Returns an error if the peer is inactive, its pairing token cannot be read, or export fails.
    pub async fn export_for_peer(
        &self,
        peer: &InstancePeer,
        collections: &[InstanceSyncCollection],
        recipient_instance_id: Option<Uuid>,
    ) -> Result<InstanceSyncEnvelope, InstanceSyncError> {
        if peer.state != InstancePeerState::Active {
            return Err(InstanceSyncError::PeerInactive(peer.display_name.clone()));
        }
        let peer_token = self.outbound_token(peer).await?;
        let selected = normalize_collections(collections)?;
        if !selected
            .iter()
            .all(|collection| peer.allowed_collections.contains(collection))
        {
            return Err(InstanceSyncError::InvalidPayload(
                "requested collections are not approved for this peer".to_owned(),
            ));
        }
        self.export_with_peer_token(
            &selected.into_iter().collect::<Vec<_>>(),
            recipient_instance_id,
            Some(&peer_token),
        )
        .await
    }

    /// Builds one deterministic bounded envelope for an approved peer.
    ///
    /// Credentials require the peer pairing token so their payload can be sealed before it enters
    /// the HTTP envelope. Other collections may be exported without that token.
    async fn export_with_peer_token(
        &self,
        collections: &[InstanceSyncCollection],
        recipient_instance_id: Option<Uuid>,
        peer_token: Option<&SecretString>,
    ) -> Result<InstanceSyncEnvelope, InstanceSyncError> {
        let sender = self.identity().await?;
        let selected = normalize_collections(collections)?;
        let mut records = Vec::new();
        if selected.contains(&InstanceSyncCollection::Inventory)
            || selected.contains(&InstanceSyncCollection::Credentials)
        {
            for host in self.repositories.hosts.list().await? {
                records.push(record_for_host(&sender, host)?);
            }
        }
        for collection in selected {
            match collection {
                InstanceSyncCollection::Inventory => {}
                InstanceSyncCollection::Knowledge => {
                    for item in self.repositories.knowledge.list().await? {
                        records.push(record_for_knowledge(&sender, item)?);
                    }
                }
                InstanceSyncCollection::Credentials => {
                    let peer_token = peer_token.ok_or_else(|| {
                        InstanceSyncError::Credential(
                            "credentials require an approved peer pairing token".to_owned(),
                        )
                    })?;
                    records.extend(self.export_access_credentials(&sender, peer_token).await?);
                }
                unsupported => {
                    return Err(InstanceSyncError::InvalidPayload(format!(
                        "collection is not implemented by protocol v{PROTOCOL_VERSION}: {unsupported:?}"
                    )));
                }
            }
        }
        records.sort_by(|left, right| {
            (&left.collection, &left.entity_type, &left.entity_key).cmp(&(
                &right.collection,
                &right.entity_type,
                &right.entity_key,
            ))
        });
        if records.len() > MAX_RECORDS_PER_ENVELOPE {
            return Err(InstanceSyncError::InvalidPayload(format!(
                "selected records exceed the bounded envelope limit of {MAX_RECORDS_PER_ENVELOPE}"
            )));
        }
        Ok(InstanceSyncEnvelope {
            sender,
            recipient_instance_id,
            dry_run: false,
            records,
        })
    }

    async fn export_access_credentials(
        &self,
        sender: &InstanceIdentity,
        peer_token: &SecretString,
    ) -> Result<Vec<InstanceSyncRecord>, InstanceSyncError> {
        let mut records = Vec::new();
        for host in self.repositories.hosts.list().await? {
            for access_path in self
                .repositories
                .access_paths
                .list_for_host(host.id)
                .await?
            {
                let credential = self
                    .repositories
                    .credentials
                    .get(access_path.credential_id)
                    .await?
                    .ok_or_else(|| {
                        InstanceSyncError::Credential(format!(
                            "access path {} references a missing credential",
                            access_path.id
                        ))
                    })?;
                let secret = self.decrypt_credential(access_path.credential_id).await?;
                if !has_syncable_access_secret(&secret) {
                    continue;
                }
                records.push(record_for_access_credential(
                    sender,
                    access_path,
                    &credential.metadata,
                    seal_credential_secret(peer_token, &secret)?,
                )?);
            }
        }
        Ok(records)
    }

    /// Applies a peer envelope after the API has authenticated its sender peer.
    ///
    /// # Errors
    ///
    /// Returns an error for peer, protocol, identity, validation, or durable-write failures.
    pub async fn receive(
        &self,
        peer: &InstancePeer,
        envelope: InstanceSyncEnvelope,
    ) -> Result<InstanceSyncResult, InstanceSyncError> {
        if peer.state != InstancePeerState::Active {
            return Err(InstanceSyncError::PeerInactive(peer.display_name.clone()));
        }
        if envelope.sender.protocol_version != PROTOCOL_VERSION {
            return Err(InstanceSyncError::InvalidPayload(format!(
                "unsupported sender protocol version {}",
                envelope.sender.protocol_version
            )));
        }
        let local = self.identity().await?;
        if envelope
            .recipient_instance_id
            .is_some_and(|recipient| recipient != local.instance_id)
        {
            return Err(InstanceSyncError::InvalidPayload(
                "envelope was addressed to a different instance".to_owned(),
            ));
        }
        if envelope.records.len() > MAX_RECORDS_PER_ENVELOPE {
            return Err(InstanceSyncError::InvalidPayload(format!(
                "envelope exceeds the {MAX_RECORDS_PER_ENVELOPE} record limit"
            )));
        }
        if peer
            .peer_instance_id
            .is_some_and(|expected| expected != envelope.sender.instance_id)
        {
            return Err(InstanceSyncError::InvalidPayload(
                "authenticated peer identity does not match the sender identity".to_owned(),
            ));
        }

        let allowed = peer
            .allowed_collections
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let credential_peer_token = if envelope
            .records
            .iter()
            .any(|record| record.collection == InstanceSyncCollection::Credentials)
        {
            Some(self.outbound_token(peer).await?)
        } else {
            None
        };
        let mut result = InstanceSyncResult {
            sender_instance_id: envelope.sender.instance_id,
            receiver_instance_id: local.instance_id,
            applied: 0,
            duplicates: 0,
            conflicts: 0,
            rejected: 0,
            details: Vec::new(),
        };
        for record in envelope.records {
            let disposition = match self
                .receive_record(
                    &allowed,
                    credential_peer_token.as_ref(),
                    envelope.dry_run,
                    &record,
                    &mut result,
                )
                .await
            {
                Ok(disposition) => disposition,
                Err(InstanceSyncError::InvalidPayload(error)) => {
                    push_detail(
                        &mut result,
                        &format!("rejected {}: {error}", record_label(&record)),
                    );
                    InstanceSyncRecordDisposition::Rejected
                }
                Err(error) => return Err(error),
            };
            match disposition {
                InstanceSyncRecordDisposition::Applied => result.applied += 1,
                InstanceSyncRecordDisposition::Duplicate => result.duplicates += 1,
                InstanceSyncRecordDisposition::Conflict => result.conflicts += 1,
                InstanceSyncRecordDisposition::Rejected => result.rejected += 1,
            }
        }
        if !envelope.dry_run {
            let mut updated_peer = peer.clone();
            updated_peer.peer_instance_id = Some(envelope.sender.instance_id);
            updated_peer.last_pulled_at = Some(now_utc());
            updated_peer.last_error = None;
            updated_peer.updated_at = now_utc();
            self.repositories
                .instance_sync
                .upsert_peer(&updated_peer)
                .await?;
        }
        Ok(result)
    }

    /// Pushes a current bounded metadata envelope directly to one configured peer.
    ///
    /// # Errors
    ///
    /// Returns an error for unavailable peer credentials, invalid peer configuration, failed
    /// export, or failed HTTP exchange.
    pub async fn push(
        &self,
        peer_id: remote_hosts_domain::InstancePeerId,
    ) -> Result<InstanceSyncPushReport, InstanceSyncError> {
        let peer = self
            .repositories
            .instance_sync
            .get_peer(peer_id)
            .await?
            .ok_or_else(|| {
                InstanceSyncError::InvalidPayload(format!("peer not found: {peer_id}"))
            })?;
        if peer.state != InstancePeerState::Active {
            return Err(InstanceSyncError::PeerInactive(peer.display_name));
        }
        let exchange = async {
            let token = self.outbound_token(&peer).await?;
            let envelope = self
                .export_with_peer_token(
                    &peer.allowed_collections,
                    peer.peer_instance_id,
                    Some(&token),
                )
                .await?;
            let sent = u32::try_from(envelope.records.len())?;
            let endpoint = format!(
                "{}/v1/instance-sync/receive",
                peer.endpoint.trim_end_matches('/')
            );
            let response = self
                .http_client
                .post(endpoint)
                .header(PEER_TOKEN_HEADER, token.expose_secret())
                .json(&envelope)
                .send()
                .await?
                .error_for_status()?;
            let result = response.json::<InstanceSyncResult>().await?;
            Ok::<_, InstanceSyncError>((sent, result))
        }
        .await;
        let (sent, result) = match exchange {
            Ok(exchange) => exchange,
            Err(error) => {
                self.record_peer_error(&peer, &error).await?;
                return Err(error);
            }
        };
        let mut updated_peer = peer.clone();
        updated_peer.peer_instance_id = Some(result.receiver_instance_id);
        updated_peer.last_pushed_at = Some(now_utc());
        updated_peer.last_error = None;
        updated_peer.updated_at = now_utc();
        self.repositories
            .instance_sync
            .upsert_peer(&updated_peer)
            .await?;
        Ok(InstanceSyncPushReport {
            peer: updated_peer.display_name,
            peer_instance_id: updated_peer.peer_instance_id,
            sent,
            result,
        })
    }

    async fn record_peer_error(
        &self,
        peer: &InstancePeer,
        error: &InstanceSyncError,
    ) -> Result<(), InstanceSyncError> {
        let mut updated_peer = peer.clone();
        updated_peer.last_error = Some(limit_detail(&error.to_string()));
        updated_peer.updated_at = now_utc();
        self.repositories
            .instance_sync
            .upsert_peer(&updated_peer)
            .await?;
        Ok(())
    }

    async fn receive_record(
        &self,
        allowed: &BTreeSet<InstanceSyncCollection>,
        credential_peer_token: Option<&SecretString>,
        dry_run: bool,
        record: &InstanceSyncRecord,
        result: &mut InstanceSyncResult,
    ) -> Result<InstanceSyncRecordDisposition, InstanceSyncError> {
        validate_record(record)?;
        if !allowed.contains(&record.collection) {
            push_detail(
                result,
                &format!(
                    "rejected {}: collection is not approved",
                    record_label(record)
                ),
            );
            return Ok(InstanceSyncRecordDisposition::Rejected);
        }
        if self
            .repositories
            .instance_sync
            .has_receipt(
                record.origin_instance_id,
                record.collection,
                &record.entity_type,
                &record.entity_key,
                &record.payload_sha256,
            )
            .await?
        {
            return Ok(InstanceSyncRecordDisposition::Duplicate);
        }
        let disposition = match record.collection {
            InstanceSyncCollection::Inventory if record.entity_type == "host" => {
                self.apply_host(record, dry_run).await?
            }
            InstanceSyncCollection::Knowledge if record.entity_type == "knowledge_item" => {
                self.apply_knowledge(record, dry_run).await?
            }
            InstanceSyncCollection::Credentials if record.entity_type == "access_credential" => {
                self.apply_access_credential(
                    record,
                    credential_peer_token.ok_or_else(|| {
                        InstanceSyncError::Credential(
                            "credential records require an approved peer pairing token".to_owned(),
                        )
                    })?,
                    dry_run,
                )
                .await?
            }
            _ => {
                push_detail(
                    result,
                    &format!("rejected {}: unsupported entity", record_label(record)),
                );
                InstanceSyncRecordDisposition::Rejected
            }
        };
        if !dry_run && disposition != InstanceSyncRecordDisposition::Rejected {
            self.repositories
                .instance_sync
                .insert_receipt(
                    record.origin_instance_id,
                    record.collection,
                    &record.entity_type,
                    &record.entity_key,
                    &record.payload_sha256,
                    now_utc(),
                )
                .await?;
        }
        Ok(disposition)
    }

    async fn apply_host(
        &self,
        record: &InstanceSyncRecord,
        dry_run: bool,
    ) -> Result<InstanceSyncRecordDisposition, InstanceSyncError> {
        let source: Host = serde_json::from_value(record.payload.clone())
            .map_err(|error| InstanceSyncError::InvalidPayload(format!("host payload: {error}")))?;
        if source.id.to_string() != record.entity_key {
            return Err(InstanceSyncError::InvalidPayload(
                "host key does not match payload id".to_owned(),
            ));
        }
        let existing = match self.repositories.hosts.get(source.id).await? {
            some @ Some(_) => some,
            None => self.repositories.hosts.get_by_name(&source.name).await?,
        };
        if let Some(local) = existing {
            if local.updated_at > source.updated_at {
                if !dry_run {
                    self.record_conflict(record, local.updated_at, &local)
                        .await?;
                    self.repositories
                        .instance_sync
                        .upsert_entity_mapping(
                            record.origin_instance_id,
                            "host",
                            &record.entity_key,
                            &local.id.to_string(),
                            now_utc(),
                        )
                        .await?;
                }
                return Ok(InstanceSyncRecordDisposition::Conflict);
            }
            let mut merged = source;
            merged.id = local.id;
            if !dry_run {
                self.repositories.hosts.upsert(&merged).await?;
                self.repositories
                    .instance_sync
                    .upsert_entity_mapping(
                        record.origin_instance_id,
                        "host",
                        &record.entity_key,
                        &merged.id.to_string(),
                        now_utc(),
                    )
                    .await?;
            }
            return Ok(InstanceSyncRecordDisposition::Applied);
        }
        if !dry_run {
            self.repositories.hosts.upsert(&source).await?;
            self.repositories
                .instance_sync
                .upsert_entity_mapping(
                    record.origin_instance_id,
                    "host",
                    &record.entity_key,
                    &source.id.to_string(),
                    now_utc(),
                )
                .await?;
        }
        Ok(InstanceSyncRecordDisposition::Applied)
    }

    async fn apply_knowledge(
        &self,
        record: &InstanceSyncRecord,
        dry_run: bool,
    ) -> Result<InstanceSyncRecordDisposition, InstanceSyncError> {
        let source: SyncKnowledgePayload =
            serde_json::from_value(record.payload.clone()).map_err(|error| {
                InstanceSyncError::InvalidPayload(format!("knowledge payload: {error}"))
            })?;
        if source.id.to_string() != record.entity_key {
            return Err(InstanceSyncError::InvalidPayload(
                "knowledge key does not match payload id".to_owned(),
            ));
        }
        let mut linked_host_ids = Vec::with_capacity(source.linked_host_ids.len());
        for remote_id in &source.linked_host_ids {
            let local_id = self
                .repositories
                .instance_sync
                .get_entity_mapping(record.origin_instance_id, "host", &remote_id.to_string())
                .await?
                .ok_or_else(|| {
                    InstanceSyncError::InvalidPayload(format!(
                        "knowledge references a host not received from this peer: {remote_id}"
                    ))
                })?;
            linked_host_ids.push(local_id.parse::<HostId>().map_err(|_| {
                InstanceSyncError::InvalidPayload("mapped host id is invalid".to_owned())
            })?);
        }
        let item = KnowledgeItem {
            id: source.id,
            title: source.title,
            body: source.body,
            source: source.source,
            linked_host_ids,
            linked_access_path_ids: Vec::new(),
            linked_software_ids: Vec::new(),
            linked_operation_ids: Vec::new(),
            tags: source.tags,
            created_at: source.created_at,
            updated_at: source.updated_at,
        };
        if let Some(local) = self.repositories.knowledge.get(item.id).await?
            && local.updated_at > item.updated_at
        {
            if !dry_run {
                self.record_conflict(record, local.updated_at, &local)
                    .await?;
            }
            return Ok(InstanceSyncRecordDisposition::Conflict);
        }
        if !dry_run {
            self.repositories.knowledge.upsert(&item).await?;
        }
        Ok(InstanceSyncRecordDisposition::Applied)
    }

    #[allow(clippy::too_many_lines)]
    async fn apply_access_credential(
        &self,
        record: &InstanceSyncRecord,
        peer_token: &SecretString,
        dry_run: bool,
    ) -> Result<InstanceSyncRecordDisposition, InstanceSyncError> {
        let source: SyncAccessCredentialPayload = serde_json::from_value(record.payload.clone())
            .map_err(|error| {
                InstanceSyncError::InvalidPayload(format!("access credential payload: {error}"))
            })?;
        if source.access_path_id.to_string() != record.entity_key {
            return Err(InstanceSyncError::InvalidPayload(
                "access credential key does not match payload access path id".to_owned(),
            ));
        }
        let local_host_id = self
            .repositories
            .instance_sync
            .get_entity_mapping(
                record.origin_instance_id,
                "host",
                &source.host_id.to_string(),
            )
            .await?
            .ok_or_else(|| {
                InstanceSyncError::InvalidPayload(
                    "access credential references a host not received from this peer".to_owned(),
                )
            })?
            .parse::<HostId>()
            .map_err(|_| {
                InstanceSyncError::InvalidPayload("mapped host id is invalid".to_owned())
            })?;
        let secret = open_credential_secret(peer_token, &source.sealed)?;
        let mapped_credential_id = self
            .repositories
            .instance_sync
            .get_entity_mapping(
                record.origin_instance_id,
                "credential",
                &source.credential.id.to_string(),
            )
            .await?
            .map(|id| {
                id.parse::<CredentialId>().map_err(|_| {
                    InstanceSyncError::InvalidPayload("mapped credential id is invalid".to_owned())
                })
            })
            .transpose()?;
        let existing_paths = self
            .repositories
            .access_paths
            .list_for_host(local_host_id)
            .await?;
        let matched_path = existing_paths
            .into_iter()
            .find(|path| access_path_matches_sync_payload(path, &source));
        let local_credential_id = mapped_credential_id
            .or_else(|| matched_path.as_ref().map(|path| path.credential_id))
            .unwrap_or_else(CredentialId::new);
        let existing_credential = self
            .repositories
            .credentials
            .get(local_credential_id)
            .await?;
        if let Some(local) = &existing_credential
            && local.metadata.updated_at > source.credential.updated_at
        {
            if !dry_run {
                self.record_conflict(record, local.metadata.updated_at, local)
                    .await?;
            }
            return Ok(InstanceSyncRecordDisposition::Conflict);
        }
        let encrypted_blob = self.encrypt_local_credential(secret).await?;
        let now = now_utc();
        let credential = remote_hosts_domain::StoredCredential {
            metadata: CredentialMetadata {
                id: local_credential_id,
                name: existing_credential.as_ref().map_or_else(
                    || synced_credential_name(record.origin_instance_id, source.credential.id),
                    |local| local.metadata.name.clone(),
                ),
                kind: source.credential.kind,
                username_hint: source.credential.username_hint,
                created_at: existing_credential
                    .as_ref()
                    .map_or(source.credential.created_at, |local| {
                        local.metadata.created_at
                    }),
                updated_at: source.credential.updated_at,
                last_used_at: source.credential.last_used_at,
            },
            encrypted_blob_json: serde_json::to_value(encrypted_blob)
                .map_err(|error| InstanceSyncError::InvalidPayload(error.to_string()))?,
        };
        let mut local_path = if let Some(path) = matched_path {
            path
        } else {
            let (environment_id, connector_id) = self.resolve_import_target().await?;
            AccessPath {
                id: AccessPathId::new(),
                host_id: local_host_id,
                environment_id,
                connector_id,
                protocol: source.protocol,
                address: source.address,
                port: source.port,
                username: source.username,
                credential_id: local_credential_id,
                route_type: source.route_type,
                proxy_chain: source.proxy_chain,
                priority: source.priority,
                enabled: source.enabled,
                connection_mode: source.connection_mode,
                idle_ttl_seconds: source.idle_ttl_seconds,
                keepalive_seconds: source.keepalive_seconds,
                max_concurrent_channels: source.max_concurrent_channels,
                max_new_connections_per_minute: source.max_new_connections_per_minute,
                requires_tty: source.requires_tty,
                notes: source.notes,
            }
        };
        local_path.credential_id = local_credential_id;
        if !dry_run {
            self.repositories.credentials.upsert(&credential).await?;
            self.repositories.access_paths.upsert(&local_path).await?;
            self.repositories
                .instance_sync
                .upsert_entity_mapping(
                    record.origin_instance_id,
                    "credential",
                    &source.credential.id.to_string(),
                    &local_credential_id.to_string(),
                    now,
                )
                .await?;
            self.repositories
                .instance_sync
                .upsert_entity_mapping(
                    record.origin_instance_id,
                    "access_path",
                    &source.access_path_id.to_string(),
                    &local_path.id.to_string(),
                    now,
                )
                .await?;
        }
        Ok(InstanceSyncRecordDisposition::Applied)
    }

    async fn resolve_import_target(
        &self,
    ) -> Result<
        (
            remote_hosts_domain::EnvironmentId,
            Option<remote_hosts_domain::ConnectorId>,
        ),
        InstanceSyncError,
    > {
        if let Some(connector) = self
            .repositories
            .connectors
            .list()
            .await?
            .into_iter()
            .max_by_key(|connector| connector.last_seen_at)
        {
            return Ok((connector.environment_id, Some(connector.id)));
        }
        let environments = self.repositories.environments.list().await?;
        if let [environment] = environments.as_slice() {
            return Ok((environment.id, None));
        }
        Err(InstanceSyncError::InvalidPayload(
            "credential import needs one local connector or exactly one local environment"
                .to_owned(),
        ))
    }

    async fn encrypt_local_credential(
        &self,
        secret: CredentialSecret,
    ) -> Result<EncryptedCredentialBlob, InstanceSyncError> {
        let master = self.vault_master_password.as_deref().ok_or_else(|| {
            InstanceSyncError::Credential(
                "credential synchronization requires an unlocked local vault".to_owned(),
            )
        })?;
        tokio::task::spawn_blocking({
            let master = master.clone();
            move || CredentialVault::encrypt(&master, &secret)
        })
        .await
        .map_err(|_| InstanceSyncError::Credential("credential encryption task failed".to_owned()))?
        .map_err(|_| {
            InstanceSyncError::Credential(
                "credential cannot be encrypted in local vault".to_owned(),
            )
        })
    }

    async fn record_conflict<T: Serialize>(
        &self,
        record: &InstanceSyncRecord,
        local_updated_at: OffsetDateTime,
        local: &T,
    ) -> Result<(), InstanceSyncError> {
        let local_payload = serde_json::to_value(local).map_err(|error| {
            InstanceSyncError::InvalidPayload(format!("local conflict payload: {error}"))
        })?;
        let conflict = InstanceSyncConflict {
            id: Uuid::now_v7(),
            origin_instance_id: record.origin_instance_id,
            collection: record.collection,
            entity_type: record.entity_type.clone(),
            entity_key: record.entity_key.clone(),
            local_updated_at,
            remote_updated_at: record.updated_at,
            local_payload_sha256: payload_sha256(&local_payload)?,
            remote_payload_sha256: record.payload_sha256.clone(),
            created_at: now_utc(),
        };
        self.repositories
            .instance_sync
            .insert_conflict(&conflict)
            .await?;
        Ok(())
    }

    async fn outbound_token(&self, peer: &InstancePeer) -> Result<SecretString, InstanceSyncError> {
        let mut secret = self.decrypt_credential(peer.outbound_credential_id).await?;
        secret
            .token
            .take()
            .or_else(|| secret.secret_text.take())
            .filter(|token| !token.is_empty())
            .map(SecretString::from)
            .ok_or_else(|| InstanceSyncError::Credential("peer credential has no token".to_owned()))
    }

    async fn decrypt_credential(
        &self,
        credential_id: CredentialId,
    ) -> Result<CredentialSecret, InstanceSyncError> {
        let master = self.vault_master_password.as_deref().ok_or_else(|| {
            InstanceSyncError::Credential(
                "credential synchronization requires an unlocked local vault".to_owned(),
            )
        })?;
        let credential = self
            .repositories
            .credentials
            .get(credential_id)
            .await?
            .ok_or_else(|| {
                InstanceSyncError::Credential(format!("credential {credential_id} is missing"))
            })?;
        let blob: EncryptedCredentialBlob = serde_json::from_value(credential.encrypted_blob_json)
            .map_err(|_| {
                InstanceSyncError::Credential(
                    "credential is not an encrypted local vault entry".to_owned(),
                )
            })?;
        tokio::task::spawn_blocking({
            let master = master.clone();
            move || CredentialVault::decrypt(&master, &blob)
        })
        .await
        .map_err(|_| InstanceSyncError::Credential("credential decryption task failed".to_owned()))?
        .map_err(|_| InstanceSyncError::Credential("credential cannot be decrypted".to_owned()))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SyncKnowledgePayload {
    id: KnowledgeItemId,
    title: String,
    body: String,
    source: remote_hosts_domain::FactSource,
    linked_host_ids: Vec<HostId>,
    tags: Vec<String>,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
}

/// Ciphertext carried in a credential-sync record. It is decrypted only inside an approved
/// receiving instance and immediately re-encrypted for that instance's local vault.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SealedCredentialSecret {
    aead: String,
    nonce_b64: String,
    ciphertext_b64: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SyncAccessCredentialPayload {
    access_path_id: AccessPathId,
    host_id: HostId,
    credential: CredentialMetadata,
    protocol: Protocol,
    address: String,
    port: u16,
    username: String,
    route_type: RouteType,
    proxy_chain: Vec<String>,
    priority: i32,
    enabled: bool,
    connection_mode: ConnectionMode,
    idle_ttl_seconds: u64,
    keepalive_seconds: u64,
    max_concurrent_channels: u16,
    max_new_connections_per_minute: u16,
    requires_tty: bool,
    notes: Option<String>,
    sealed: SealedCredentialSecret,
}

fn record_for_host(
    sender: &InstanceIdentity,
    host: Host,
) -> Result<InstanceSyncRecord, InstanceSyncError> {
    record_from_payload(
        sender,
        InstanceSyncCollection::Inventory,
        "host",
        host.id.to_string(),
        host.updated_at,
        serde_json::to_value(host),
    )
}

fn record_for_knowledge(
    sender: &InstanceIdentity,
    item: KnowledgeItem,
) -> Result<InstanceSyncRecord, InstanceSyncError> {
    let payload = SyncKnowledgePayload {
        id: item.id,
        title: item.title,
        body: item.body,
        source: item.source,
        linked_host_ids: item.linked_host_ids,
        tags: item.tags,
        created_at: item.created_at,
        updated_at: item.updated_at,
    };
    record_from_payload(
        sender,
        InstanceSyncCollection::Knowledge,
        "knowledge_item",
        payload.id.to_string(),
        payload.updated_at,
        serde_json::to_value(payload),
    )
}

fn record_for_access_credential(
    sender: &InstanceIdentity,
    access_path: AccessPath,
    credential: &CredentialMetadata,
    sealed: SealedCredentialSecret,
) -> Result<InstanceSyncRecord, InstanceSyncError> {
    let payload = SyncAccessCredentialPayload {
        access_path_id: access_path.id,
        host_id: access_path.host_id,
        credential: credential.clone(),
        protocol: access_path.protocol,
        address: access_path.address,
        port: access_path.port,
        username: access_path.username,
        route_type: access_path.route_type,
        proxy_chain: access_path.proxy_chain,
        priority: access_path.priority,
        enabled: access_path.enabled,
        connection_mode: access_path.connection_mode,
        idle_ttl_seconds: access_path.idle_ttl_seconds,
        keepalive_seconds: access_path.keepalive_seconds,
        max_concurrent_channels: access_path.max_concurrent_channels,
        max_new_connections_per_minute: access_path.max_new_connections_per_minute,
        requires_tty: access_path.requires_tty,
        notes: access_path.notes,
        sealed,
    };
    record_from_payload(
        sender,
        InstanceSyncCollection::Credentials,
        "access_credential",
        payload.access_path_id.to_string(),
        credential.updated_at,
        serde_json::to_value(payload),
    )
}

fn has_syncable_access_secret(secret: &CredentialSecret) -> bool {
    secret.password.is_some()
        || secret.private_key_pem.is_some()
        || secret.private_key_passphrase.is_some()
        || secret.sudo_password.is_some()
        || secret.token.is_some()
        || secret.secret_text.is_some()
}

fn seal_credential_secret(
    peer_token: &SecretString,
    secret: &CredentialSecret,
) -> Result<SealedCredentialSecret, InstanceSyncError> {
    let key = credential_seal_key(peer_token);
    let cipher = XChaCha20Poly1305::new_from_slice(key.as_slice()).map_err(|_| {
        InstanceSyncError::Credential("credential seal initialization failed".to_owned())
    })?;
    let nonce = XNonce::generate();
    let plaintext = Zeroizing::new(
        serde_json::to_vec(secret)
            .map_err(|error| InstanceSyncError::InvalidPayload(error.to_string()))?,
    );
    let ciphertext = cipher.encrypt(&nonce, plaintext.as_slice()).map_err(|_| {
        InstanceSyncError::Credential("credential seal encryption failed".to_owned())
    })?;
    Ok(SealedCredentialSecret {
        aead: CREDENTIAL_SEAL_AEAD.to_owned(),
        nonce_b64: STANDARD_NO_PAD.encode(nonce.as_slice()),
        ciphertext_b64: STANDARD_NO_PAD.encode(ciphertext),
    })
}

fn open_credential_secret(
    peer_token: &SecretString,
    sealed: &SealedCredentialSecret,
) -> Result<CredentialSecret, InstanceSyncError> {
    if sealed.aead != CREDENTIAL_SEAL_AEAD {
        return Err(InstanceSyncError::InvalidPayload(
            "unsupported credential seal algorithm".to_owned(),
        ));
    }
    let nonce_bytes: [u8; 24] = STANDARD_NO_PAD
        .decode(&sealed.nonce_b64)
        .map_err(|_| {
            InstanceSyncError::InvalidPayload("credential seal nonce is invalid".to_owned())
        })?
        .try_into()
        .map_err(|_| {
            InstanceSyncError::InvalidPayload("credential seal nonce has invalid length".to_owned())
        })?;
    let ciphertext = STANDARD_NO_PAD
        .decode(&sealed.ciphertext_b64)
        .map_err(|_| {
            InstanceSyncError::InvalidPayload("credential seal ciphertext is invalid".to_owned())
        })?;
    let key = credential_seal_key(peer_token);
    let cipher = XChaCha20Poly1305::new_from_slice(key.as_slice()).map_err(|_| {
        InstanceSyncError::Credential("credential seal initialization failed".to_owned())
    })?;
    let plaintext = Zeroizing::new(
        cipher
            .decrypt(&XNonce::from(nonce_bytes), ciphertext.as_slice())
            .map_err(|_| {
                InstanceSyncError::InvalidPayload("credential seal cannot be opened".to_owned())
            })?,
    );
    serde_json::from_slice(&plaintext).map_err(|_| {
        InstanceSyncError::InvalidPayload("credential seal payload is invalid".to_owned())
    })
}

fn credential_seal_key(peer_token: &SecretString) -> Zeroizing<Vec<u8>> {
    let mut hasher = Sha256::new();
    hasher.update(b"remote-hosts-instance-sync-credential-v1");
    hasher.update(peer_token.expose_secret().as_bytes());
    Zeroizing::new(hasher.finalize().to_vec())
}

fn access_path_matches_sync_payload(
    path: &AccessPath,
    source: &SyncAccessCredentialPayload,
) -> bool {
    path.protocol == source.protocol
        && path.address == source.address
        && path.port == source.port
        && path.username == source.username
        && path.route_type == source.route_type
        && path.proxy_chain == source.proxy_chain
}

fn synced_credential_name(origin_instance_id: Uuid, source_credential_id: CredentialId) -> String {
    format!("instance-sync:{origin_instance_id}:{source_credential_id}")
}

fn record_from_payload(
    sender: &InstanceIdentity,
    collection: InstanceSyncCollection,
    entity_type: &str,
    entity_key: String,
    updated_at: OffsetDateTime,
    payload: Result<Value, serde_json::Error>,
) -> Result<InstanceSyncRecord, InstanceSyncError> {
    let payload = payload.map_err(|error| InstanceSyncError::InvalidPayload(error.to_string()))?;
    let payload_sha256 = payload_sha256(&payload)?;
    let event_id = event_id_for(
        sender.instance_id,
        collection,
        entity_type,
        &entity_key,
        &payload_sha256,
    );
    Ok(InstanceSyncRecord {
        event_id,
        origin_instance_id: sender.instance_id,
        collection,
        entity_type: entity_type.to_owned(),
        entity_key,
        updated_at,
        payload,
        payload_sha256,
    })
}

fn normalize_collections(
    collections: &[InstanceSyncCollection],
) -> Result<BTreeSet<InstanceSyncCollection>, InstanceSyncError> {
    let mut selected = collections.iter().copied().collect::<BTreeSet<_>>();
    if selected.is_empty() {
        return Err(InstanceSyncError::InvalidPayload(
            "at least one collection is required".to_owned(),
        ));
    }
    if selected.contains(&InstanceSyncCollection::Credentials) {
        selected.insert(InstanceSyncCollection::Inventory);
    }
    Ok(selected)
}

fn validate_record(record: &InstanceSyncRecord) -> Result<(), InstanceSyncError> {
    if record.entity_type.trim().is_empty()
        || record.entity_key.trim().is_empty()
        || record.entity_type.len() > 64
        || record.entity_key.len() > 256
    {
        return Err(InstanceSyncError::InvalidPayload(
            "record entity type and key must be non-empty bounded identifiers".to_owned(),
        ));
    }
    let expected = payload_sha256(&record.payload)?;
    if expected != record.payload_sha256 {
        return Err(InstanceSyncError::InvalidPayload(format!(
            "payload hash does not match {}",
            record_label(record)
        )));
    }
    let expected_event_id = event_id_for(
        record.origin_instance_id,
        record.collection,
        &record.entity_type,
        &record.entity_key,
        &record.payload_sha256,
    );
    if record.event_id != expected_event_id {
        return Err(InstanceSyncError::InvalidPayload(format!(
            "event id does not match {}",
            record_label(record)
        )));
    }
    if record.collection == InstanceSyncCollection::Credentials {
        validate_sealed_credential_payload(&record.payload)?;
    } else {
        ensure_no_secret_keys(&record.payload)?;
    }
    Ok(())
}

fn validate_sealed_credential_payload(value: &Value) -> Result<(), InstanceSyncError> {
    let payload: SyncAccessCredentialPayload =
        serde_json::from_value(value.clone()).map_err(|_| {
            InstanceSyncError::InvalidPayload(
                "credential sync payload must contain only the supported sealed fields".to_owned(),
            )
        })?;
    let object = value.as_object().ok_or_else(|| {
        InstanceSyncError::InvalidPayload("credential sync payload must be an object".to_owned())
    })?;
    ensure_exact_keys(
        object,
        &[
            "access_path_id",
            "host_id",
            "credential",
            "protocol",
            "address",
            "port",
            "username",
            "route_type",
            "proxy_chain",
            "priority",
            "enabled",
            "connection_mode",
            "idle_ttl_seconds",
            "keepalive_seconds",
            "max_concurrent_channels",
            "max_new_connections_per_minute",
            "requires_tty",
            "notes",
            "sealed",
        ],
        "credential sync payload",
    )?;
    let credential = object
        .get("credential")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            InstanceSyncError::InvalidPayload("credential metadata must be an object".to_owned())
        })?;
    ensure_exact_keys(
        credential,
        &[
            "id",
            "name",
            "kind",
            "username_hint",
            "created_at",
            "updated_at",
            "last_used_at",
        ],
        "credential metadata",
    )?;
    let sealed = object
        .get("sealed")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            InstanceSyncError::InvalidPayload("credential seal must be an object".to_owned())
        })?;
    ensure_exact_keys(
        sealed,
        &["aead", "nonce_b64", "ciphertext_b64"],
        "credential seal",
    )?;
    if payload.sealed.aead != CREDENTIAL_SEAL_AEAD {
        return Err(InstanceSyncError::InvalidPayload(
            "unsupported credential seal algorithm".to_owned(),
        ));
    }
    Ok(())
}

fn ensure_exact_keys(
    object: &serde_json::Map<String, Value>,
    allowed: &[&str],
    label: &str,
) -> Result<(), InstanceSyncError> {
    if object.len() != allowed.len() || object.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err(InstanceSyncError::InvalidPayload(format!(
            "{label} contains an unsupported field"
        )));
    }
    Ok(())
}

fn ensure_no_secret_keys(value: &Value) -> Result<(), InstanceSyncError> {
    const FORBIDDEN: &[&str] = &[
        "password",
        "passphrase",
        "private_key",
        "token",
        "secret",
        "credential",
        "encrypted_blob",
    ];
    match value {
        Value::Object(object) => {
            for (key, child) in object {
                let normalized = key.to_ascii_lowercase();
                if FORBIDDEN
                    .iter()
                    .any(|forbidden| normalized.contains(forbidden))
                {
                    return Err(InstanceSyncError::InvalidPayload(format!(
                        "secret-like field is forbidden in sync payload: {key}"
                    )));
                }
                ensure_no_secret_keys(child)?;
            }
        }
        Value::Array(values) => {
            for child in values {
                ensure_no_secret_keys(child)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn payload_sha256(payload: &Value) -> Result<String, InstanceSyncError> {
    let bytes = serde_json::to_vec(payload).map_err(|error| {
        InstanceSyncError::InvalidPayload(format!("payload serialization: {error}"))
    })?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn event_id_for(
    origin_instance_id: Uuid,
    collection: InstanceSyncCollection,
    entity_type: &str,
    entity_key: &str,
    payload_sha256: &str,
) -> Uuid {
    Uuid::new_v5(
        &origin_instance_id,
        format!("{collection:?}:{entity_type}:{entity_key}:{payload_sha256}").as_bytes(),
    )
}

/// Returns the non-reversible token digest stored for inbound peer authentication.
#[must_use]
pub fn token_sha256(token: &str) -> String {
    format!("{:x}", Sha256::digest(token.as_bytes()))
}

fn normalize_peer_label(value: &str) -> Result<String, InstanceSyncError> {
    let value = value.trim();
    if value.is_empty() || value.len() > 128 {
        return Err(InstanceSyncError::InvalidPayload(
            "peer display name must contain 1..=128 characters".to_owned(),
        ));
    }
    Ok(value.to_owned())
}

fn normalize_peer_endpoint(value: &str) -> Result<String, InstanceSyncError> {
    let endpoint = value.trim().trim_end_matches('/');
    let parsed = reqwest::Url::parse(endpoint).map_err(|_| {
        InstanceSyncError::InvalidPayload("peer endpoint must be an absolute URL".to_owned())
    })?;
    if !matches!(parsed.scheme(), "https" | "http") || parsed.host_str().is_none() {
        return Err(InstanceSyncError::InvalidPayload(
            "peer endpoint must use http or https and include a host".to_owned(),
        ));
    }
    Ok(endpoint.to_owned())
}

fn push_detail(result: &mut InstanceSyncResult, detail: &str) {
    if result.details.len() < 10 {
        result.details.push(limit_detail(detail));
    }
}

fn record_label(record: &InstanceSyncRecord) -> String {
    format!(
        "{}:{}",
        limit_detail(&record.entity_type),
        limit_detail(&record.entity_key)
    )
}

fn limit_detail(detail: &str) -> String {
    const MAX_DETAIL_CHARS: usize = 512;
    let mut chars = detail.chars();
    let truncated = chars.by_ref().take(MAX_DETAIL_CHARS).collect::<String>();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

impl From<std::num::TryFromIntError> for InstanceSyncError {
    fn from(error: std::num::TryFromIntError) -> Self {
        Self::InvalidPayload(format!("record count overflow: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use remote_hosts_db::{Repositories, connect_sqlite, migrate};
    use remote_hosts_domain::{
        AccessPath, ConnectionMode, Connector, ConnectorId, CredentialKind, CredentialMetadata,
        EntityState, Environment, EnvironmentId, EnvironmentKind, FactSource, HostKind,
        InstancePeer, Protocol, RiskLevel, RouteType, StoredCredential, TrustLevel,
    };
    use tempfile::TempDir;

    use super::*;

    async fn repositories(
        tempdir: &TempDir,
        name: &str,
    ) -> Result<Repositories, Box<dyn std::error::Error>> {
        let database_url = format!(
            "sqlite://{}",
            tempdir.path().join(format!("{name}.sqlite")).display()
        );
        let pool = connect_sqlite(&database_url).await?;
        migrate(&pool).await?;
        Ok(Repositories::new(pool))
    }

    async fn peer_for(
        repositories: &Repositories,
        label: &str,
    ) -> Result<InstancePeer, Box<dyn std::error::Error>> {
        let now = now_utc();
        let credential = remote_hosts_domain::CredentialId::new();
        repositories
            .credentials
            .insert(&StoredCredential {
                metadata: CredentialMetadata {
                    id: credential,
                    name: format!("test-peer-{label}"),
                    kind: CredentialKind::GenericSecret,
                    username_hint: None,
                    created_at: now,
                    updated_at: now,
                    last_used_at: None,
                },
                encrypted_blob_json: serde_json::json!({"test": true}),
            })
            .await?;
        let peer = InstancePeer {
            id: InstancePeerId::new(),
            peer_instance_id: None,
            display_name: label.to_owned(),
            endpoint: "http://127.0.0.1:8787".to_owned(),
            outbound_credential_id: credential,
            inbound_token_sha256: token_sha256("test-token"),
            allowed_collections: vec![
                InstanceSyncCollection::Inventory,
                InstanceSyncCollection::Knowledge,
            ],
            state: InstancePeerState::Active,
            last_pushed_at: None,
            last_pulled_at: None,
            last_error: None,
            created_at: now,
            updated_at: now,
        };
        repositories.instance_sync.upsert_peer(&peer).await?;
        Ok(peer)
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn export_receive_is_idempotent_and_keeps_newer_local_host()
    -> Result<(), Box<dyn std::error::Error>> {
        let tempdir = TempDir::new()?;
        let sender_repositories = repositories(&tempdir, "sender").await?;
        let receiver_repositories = repositories(&tempdir, "receiver").await?;
        let sender = InstanceSyncService::new(sender_repositories.clone())?;
        let receiver = InstanceSyncService::new(receiver_repositories.clone())?;
        let source_now = now_utc();
        let source_host = Host {
            id: HostId::new(),
            name: "macstudio".to_owned(),
            display_name: "Mac Studio".to_owned(),
            kind: HostKind::Macos,
            owner: Some("jinliang".to_owned()),
            tags: vec!["home".to_owned()],
            description: Some("source inventory".to_owned()),
            risk_level: RiskLevel::Personal,
            created_at: source_now,
            updated_at: source_now,
        };
        sender_repositories.hosts.insert(&source_host).await?;
        let knowledge = KnowledgeItem {
            id: KnowledgeItemId::new(),
            title: "Mac Studio runtime".to_owned(),
            body: "Uses a local connector and a pooled SSH transport.".to_owned(),
            source: FactSource::Manual,
            linked_host_ids: vec![source_host.id],
            linked_access_path_ids: Vec::new(),
            linked_software_ids: Vec::new(),
            linked_operation_ids: Vec::new(),
            tags: vec!["connector".to_owned()],
            created_at: source_now,
            updated_at: source_now,
        };
        sender_repositories.knowledge.insert(&knowledge).await?;
        let receiver_peer = peer_for(&receiver_repositories, "sender").await?;

        let envelope = sender
            .export(
                &[
                    InstanceSyncCollection::Inventory,
                    InstanceSyncCollection::Knowledge,
                ],
                None,
            )
            .await?;
        let first = receiver.receive(&receiver_peer, envelope.clone()).await?;
        assert_eq!(first.applied, 2);
        assert_eq!(first.duplicates, 0);
        assert_eq!(
            receiver_repositories
                .hosts
                .get_by_name("macstudio")
                .await?
                .as_ref()
                .map(|host| host.id),
            Some(source_host.id)
        );
        assert_eq!(
            receiver_repositories
                .knowledge
                .get(knowledge.id)
                .await?
                .as_ref()
                .map(|item| item.linked_host_ids.clone()),
            Some(vec![source_host.id])
        );

        let replay = receiver.receive(&receiver_peer, envelope.clone()).await?;
        assert_eq!(replay.applied, 0);
        assert_eq!(replay.duplicates, 2);

        let mut newer_local = receiver_repositories
            .hosts
            .get(source_host.id)
            .await?
            .ok_or("synced host missing")?;
        newer_local.description = Some("local correction".to_owned());
        newer_local.updated_at = source_now + time::Duration::seconds(1);
        receiver_repositories.hosts.upsert(&newer_local).await?;
        let mut changed_envelope = envelope;
        let host_record = changed_envelope
            .records
            .iter_mut()
            .find(|record| record.entity_type == "host")
            .ok_or("host record missing")?;
        let mut stale_host: Host = serde_json::from_value(host_record.payload.clone())?;
        stale_host.description = Some("stale peer update".to_owned());
        host_record.payload = serde_json::to_value(stale_host)?;
        host_record.payload_sha256 = payload_sha256(&host_record.payload)?;
        host_record.event_id = event_id_for(
            host_record.origin_instance_id,
            host_record.collection,
            &host_record.entity_type,
            &host_record.entity_key,
            &host_record.payload_sha256,
        );
        let conflict = receiver.receive(&receiver_peer, changed_envelope).await?;
        assert_eq!(conflict.conflicts, 1);
        assert_eq!(
            receiver_repositories
                .instance_sync
                .list_conflicts(10)
                .await?
                .len(),
            1
        );
        assert_eq!(
            receiver_repositories
                .hosts
                .get(source_host.id)
                .await?
                .and_then(|host| host.description),
            Some("local correction".to_owned())
        );
        Ok(())
    }

    #[tokio::test]
    async fn invalid_record_does_not_block_following_valid_records()
    -> Result<(), Box<dyn std::error::Error>> {
        let tempdir = TempDir::new()?;
        let sender_repositories = repositories(&tempdir, "sender").await?;
        let receiver_repositories = repositories(&tempdir, "receiver").await?;
        let sender = InstanceSyncService::new(sender_repositories.clone())?;
        let receiver = InstanceSyncService::new(receiver_repositories.clone())?;
        let now = now_utc();
        let host = Host {
            id: HostId::new(),
            name: "valid-after-invalid".to_owned(),
            display_name: "Valid After Invalid".to_owned(),
            kind: HostKind::Linux,
            owner: None,
            tags: Vec::new(),
            description: None,
            risk_level: RiskLevel::Development,
            created_at: now,
            updated_at: now,
        };
        sender_repositories.hosts.insert(&host).await?;
        let mut envelope = sender
            .export(&[InstanceSyncCollection::Inventory], None)
            .await?;
        let valid = envelope.records[0].clone();
        let mut invalid = valid.clone();
        invalid.entity_key = HostId::new().to_string();
        invalid.payload = serde_json::json!({"password": "must-not-cross-instances"});
        invalid.payload_sha256 = payload_sha256(&invalid.payload)?;
        invalid.event_id = event_id_for(
            invalid.origin_instance_id,
            invalid.collection,
            &invalid.entity_type,
            &invalid.entity_key,
            &invalid.payload_sha256,
        );
        envelope.records = vec![invalid, valid];

        let result = receiver
            .receive(&peer_for(&receiver_repositories, "sender").await?, envelope)
            .await?;

        assert_eq!(result.rejected, 1);
        assert_eq!(result.applied, 1);
        assert!(
            receiver_repositories
                .hosts
                .get_by_name(&host.name)
                .await?
                .is_some()
        );
        Ok(())
    }

    #[tokio::test]
    async fn failed_push_keeps_a_bounded_peer_error_for_diagnosis()
    -> Result<(), Box<dyn std::error::Error>> {
        let tempdir = TempDir::new()?;
        let repositories = repositories(&tempdir, "sender").await?;
        let service = InstanceSyncService::with_vault_master_password(
            repositories.clone(),
            Some(SecretString::from("failed-push-test-master".to_owned())),
        )?;
        let peer = service
            .configure_peer(
                "offline-peer".to_owned(),
                "http://127.0.0.1:9".to_owned(),
                SecretString::from("failed-push-test-token".to_owned()),
                vec![InstanceSyncCollection::Inventory],
            )
            .await?;

        assert!(service.push(peer.id).await.is_err());
        let updated = repositories
            .instance_sync
            .get_peer(peer.id)
            .await?
            .ok_or("peer disappeared after failed push")?;
        assert!(updated.last_error.is_some());
        assert!(
            updated
                .last_error
                .as_deref()
                .is_some_and(|error| error.len() <= 515)
        );
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn credential_sync_reencrypts_access_passwords_and_creates_a_local_path()
    -> Result<(), Box<dyn std::error::Error>> {
        let tempdir = TempDir::new()?;
        let sender_repositories = repositories(&tempdir, "sender").await?;
        let receiver_repositories = repositories(&tempdir, "receiver").await?;
        let sender_master = SecretString::from("sender-credential-sync-master".to_owned());
        let receiver_master = SecretString::from("receiver-credential-sync-master".to_owned());
        let sender = InstanceSyncService::with_vault_master_password(
            sender_repositories.clone(),
            Some(sender_master.clone()),
        )?;
        let receiver = InstanceSyncService::with_vault_master_password(
            receiver_repositories.clone(),
            Some(receiver_master.clone()),
        )?;
        let now = now_utc();
        let source_host = Host {
            id: HostId::new(),
            name: "credential-sync-target".to_owned(),
            display_name: "Credential Sync Target".to_owned(),
            kind: HostKind::Linux,
            owner: Some("ops".to_owned()),
            tags: vec!["shared".to_owned()],
            description: None,
            risk_level: RiskLevel::Development,
            created_at: now,
            updated_at: now,
        };
        sender_repositories.hosts.insert(&source_host).await?;
        let source_environment = Environment {
            id: EnvironmentId::new(),
            name: "sender-lan".to_owned(),
            kind: EnvironmentKind::HomeLan,
            description: None,
            trust_level: TrustLevel::Owned,
            notes: None,
        };
        sender_repositories
            .environments
            .insert(&source_environment)
            .await?;
        let source_secret = CredentialSecret {
            password: Some("sync-password".to_owned()),
            private_key_pem: Some("PRIVATE-KEY-BYTES".to_owned()),
            private_key_passphrase: Some("key-passphrase".to_owned()),
            sudo_password: Some("sudo-password".to_owned()),
            token: None,
            secret_text: None,
            use_ssh_agent: false,
        };
        let source_credential = StoredCredential {
            metadata: CredentialMetadata {
                id: CredentialId::new(),
                name: "source ssh credential".to_owned(),
                kind: CredentialKind::SshPrivateKeyWithPassphrase,
                username_hint: Some("ops".to_owned()),
                created_at: now,
                updated_at: now,
                last_used_at: None,
            },
            encrypted_blob_json: serde_json::to_value(CredentialVault::encrypt(
                &sender_master,
                &source_secret,
            )?)?,
        };
        sender_repositories
            .credentials
            .insert(&source_credential)
            .await?;
        sender_repositories
            .access_paths
            .insert(&AccessPath {
                id: AccessPathId::new(),
                host_id: source_host.id,
                environment_id: source_environment.id,
                connector_id: None,
                protocol: Protocol::Ssh,
                address: "192.168.50.20".to_owned(),
                port: 2222,
                username: "ops".to_owned(),
                credential_id: source_credential.metadata.id,
                route_type: RouteType::Lan,
                proxy_chain: Vec::new(),
                priority: 10,
                enabled: true,
                connection_mode: ConnectionMode::Pooled,
                idle_ttl_seconds: 600,
                keepalive_seconds: 30,
                max_concurrent_channels: 8,
                max_new_connections_per_minute: 4,
                requires_tty: false,
                notes: Some("sender route".to_owned()),
            })
            .await?;
        let receiver_environment = Environment {
            id: EnvironmentId::new(),
            name: "receiver-lan".to_owned(),
            kind: EnvironmentKind::HomeLan,
            description: None,
            trust_level: TrustLevel::Owned,
            notes: None,
        };
        receiver_repositories
            .environments
            .insert(&receiver_environment)
            .await?;
        let receiver_connector = Connector {
            id: ConnectorId::new(),
            name: "receiver-connector".to_owned(),
            environment_id: receiver_environment.id,
            host_id: None,
            version: "test".to_owned(),
            state: EntityState::Healthy,
            last_seen_at: Some(now),
            current_network: Some("receiver".to_owned()),
        };
        receiver_repositories
            .connectors
            .upsert(&receiver_connector)
            .await?;
        let pairing_token = "credential-sync-pairing-token";
        let sender_peer = sender
            .configure_peer(
                "receiver".to_owned(),
                "http://127.0.0.1:8788".to_owned(),
                SecretString::from(pairing_token.to_owned()),
                vec![InstanceSyncCollection::Credentials],
            )
            .await?;
        let receiver_peer = receiver
            .configure_peer(
                "sender".to_owned(),
                "http://127.0.0.1:8788".to_owned(),
                SecretString::from(pairing_token.to_owned()),
                vec![InstanceSyncCollection::Credentials],
            )
            .await?;

        let envelope = sender
            .export_for_peer(&sender_peer, &sender_peer.allowed_collections, None)
            .await?;
        assert_eq!(envelope.records.len(), 2);
        assert!(!serde_json::to_string(&envelope)?.contains("sync-password"));
        let credential_record = envelope
            .records
            .iter()
            .find(|record| record.collection == InstanceSyncCollection::Credentials)
            .ok_or("credential record missing")?;
        let mut plaintext_injection = credential_record.clone();
        plaintext_injection
            .payload
            .as_object_mut()
            .ok_or("credential payload is not an object")?
            .insert(
                "password".to_owned(),
                Value::String("injected-secret".to_owned()),
            );
        plaintext_injection.payload_sha256 = payload_sha256(&plaintext_injection.payload)?;
        plaintext_injection.event_id = event_id_for(
            plaintext_injection.origin_instance_id,
            plaintext_injection.collection,
            &plaintext_injection.entity_type,
            &plaintext_injection.entity_key,
            &plaintext_injection.payload_sha256,
        );
        assert!(matches!(
            validate_record(&plaintext_injection),
            Err(InstanceSyncError::InvalidPayload(_))
        ));
        let result = receiver.receive(&receiver_peer, envelope).await?;
        assert_eq!(result.applied, 2);
        let local_host = receiver_repositories
            .hosts
            .get_by_name(&source_host.name)
            .await?
            .ok_or("synchronized host missing")?;
        let local_paths = receiver_repositories
            .access_paths
            .list_for_host(local_host.id)
            .await?;
        assert_eq!(local_paths.len(), 1);
        assert_eq!(local_paths[0].environment_id, receiver_environment.id);
        assert_eq!(local_paths[0].connector_id, Some(receiver_connector.id));
        let local_credential = receiver_repositories
            .credentials
            .get(local_paths[0].credential_id)
            .await?
            .ok_or("synchronized credential missing")?;
        assert_ne!(
            local_credential.encrypted_blob_json,
            source_credential.encrypted_blob_json
        );
        let local_blob: EncryptedCredentialBlob =
            serde_json::from_value(local_credential.encrypted_blob_json)?;
        let restored = CredentialVault::decrypt(&receiver_master, &local_blob)?;
        assert!(restored == source_secret);
        Ok(())
    }
}
