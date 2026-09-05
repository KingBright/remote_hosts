//! Private S3-compatible relay for explicitly registered interactive bastion targets.
//! Long-lived credentials never leave the connector vault. Targets receive only scoped URLs.

use base64::Engine as _;
use md5::Digest as _;
use remote_hosts_core::SftpRequest;
use remote_hosts_core::transport::TransportError;
use remote_hosts_db::Repositories;
use remote_hosts_domain::{AccessPath, AccessPathId, CredentialId, HostId, RouteType};
use remote_hosts_vault::{CredentialVault, EncryptedCredentialBlob};
use reqwest::{Client, Method, StatusCode, Url};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{path::Path, sync::Arc, time::Duration};
use time::OffsetDateTime;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zeroize::Zeroizing;

mod pty;

const PART_BYTES: usize = 16 * 1024 * 1024;
const URL_TTL: u64 = 7200;

fn failure(message: &str) -> TransportError {
    TransportError::FileTransfer(format!("minio_relay: {message}"))
}

/// Non-secret connector-local relay policy. An empty list preserves direct transfer behavior.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MinioRelayConfig {
    /// Explicit target and access-path bindings.
    pub profiles: Vec<MinioRelayProfile>,
}

/// One approved endpoint and target; this never discovers credentials from historical aliases.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MinioRelayProfile {
    /// Stable profile name used in private object prefixes.
    pub name: String,
    /// Exact canonical destination host.
    pub host_id: HostId,
    /// Exact interactive access path.
    pub access_path_id: AccessPathId,
    /// Hostname that must be observed inside the selected terminal before any transfer.
    pub expected_hostname: String,
    /// S3 API endpoint, not the Console URL.
    pub endpoint: String,
    /// Private relay bucket, separate from business and release objects.
    pub bucket: String,
    /// Existing encrypted service credential in the Remote Hosts vault.
    pub credential_id: CredentialId,
    /// Minimum file size for automatic relay selection.
    pub threshold_bytes: u64,
    /// Explicit approval for an HTTP endpoint on the owner's internal network.
    #[serde(default)]
    pub allow_http: bool,
}

impl MinioRelayProfile {
    /// Whether this exact route is eligible; ordinary LAN/FRP/VPN SSH remains direct.
    pub fn matches_route(&self, path: &AccessPath) -> bool {
        path.enabled
            && path.host_id == self.host_id
            && path.id == self.access_path_id
            && path.route_type == RouteType::Bastion
            && path.requires_tty
            && path.proxy_chain.is_empty()
    }

    /// Validates policy before loading any credential.
    ///
    /// # Errors
    /// Rejects invalid target identifiers, bucket names, and unapproved endpoints.
    pub fn validate(&self) -> Result<(), TransportError> {
        let identifier = |s: &str| {
            !s.is_empty()
                && s.len() <= 128
                && s.as_bytes()[0].is_ascii_alphanumeric()
                && s.as_bytes()[s.len() - 1].is_ascii_alphanumeric()
                && s.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.')
        };
        if !identifier(&self.name)
            || !identifier(&self.expected_hostname)
            || !self.bucket.starts_with("remote-hosts-")
            || !identifier(&self.bucket)
            || self.bucket.len() > 63
            || self.bucket.ends_with('-')
            || !self
                .bucket
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            || self.threshold_bytes == 0
        {
            return Err(failure("invalid target, bucket, name, or size threshold"));
        }
        let endpoint = Url::parse(&self.endpoint).map_err(|_| failure("invalid S3 endpoint"))?;
        if endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || endpoint.path() != "/"
            || !(endpoint.scheme() == "https" || endpoint.scheme() == "http" && self.allow_http)
        {
            return Err(failure(
                "endpoint must be an approved HTTP(S) S3 origin without credentials, path, query, or fragment",
            ));
        }
        Ok(())
    }
}

/// Vault-backed relay configuration owned by one connector process.
pub struct MinioRelayStore {
    pub(crate) config: MinioRelayConfig,
    repositories: Repositories,
    master: Arc<SecretString>,
}

impl MinioRelayStore {
    /// Loads non-secret policy. A missing file intentionally leaves ordinary transfers unchanged.
    ///
    /// # Errors
    /// Rejects unreadable, malformed, or ambiguous configuration.
    pub fn load(
        path: &Path,
        repositories: Repositories,
        master: SecretString,
    ) -> Result<Option<Self>, TransportError> {
        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(failure("cannot read relay configuration")),
        };
        let config: MinioRelayConfig = serde_json::from_slice(&bytes)
            .map_err(|_| failure("invalid relay configuration JSON"))?;
        let mut targets = std::collections::BTreeSet::new();
        for profile in &config.profiles {
            profile.validate()?;
            if !targets.insert((profile.host_id, profile.access_path_id)) {
                return Err(failure("ambiguous duplicate target relay profiles"));
            }
        }
        Ok(Some(Self {
            config,
            repositories,
            master: Arc::new(master),
        }))
    }

    pub(crate) async fn client(
        &self,
        profile: &MinioRelayProfile,
    ) -> Result<RelayClient, TransportError> {
        profile.validate()?;
        let stored = self
            .repositories
            .credentials
            .get(profile.credential_id)
            .await
            .map_err(|_| failure("credential metadata unavailable"))?
            .ok_or_else(|| failure("encrypted service credential not found"))?;
        let access_key = stored
            .metadata
            .username_hint
            .filter(|v| !v.is_empty())
            .ok_or_else(|| failure("service credential requires an access-key username hint"))?;
        let blob: EncryptedCredentialBlob = serde_json::from_value(stored.encrypted_blob_json)
            .map_err(|_| failure("invalid encrypted service credential"))?;
        let master = Arc::clone(&self.master);
        let mut secret =
            tokio::task::spawn_blocking(move || CredentialVault::decrypt(master.as_ref(), &blob))
                .await
                .map_err(|_| failure("credential decryption unavailable"))?
                .map_err(|_| failure("credential vault is locked"))?;
        let secret_key = secret
            .password
            .take()
            .or_else(|| secret.secret_text.take())
            .filter(|v| !v.is_empty())
            .ok_or_else(|| failure("service credential has no secret key"))?;
        RelayClient::new(profile.clone(), access_key, SecretString::from(secret_key))
    }

    /// Checks authenticated private object CRUD and exact byte integrity without revealing secrets.
    /// With `provision`, creates only the configured dedicated relay bucket and its retention rule.
    ///
    /// # Errors
    /// Reports sanitized credential, storage, integrity, or cleanup failures.
    pub async fn check(&self, provision: bool) -> Result<Vec<serde_json::Value>, TransportError> {
        let mut receipts = Vec::new();
        for profile in &self.config.profiles {
            let client = self.client(profile).await?;
            if provision {
                client.ensure_bucket().await?;
            }
            let stale_multipart_aborted = client.abort_stale_uploads().await?;
            let key = format!(
                "remote-hosts/{}/check-{}/payload",
                profile.name,
                uuid::Uuid::new_v4()
            );
            let payload = b"remote-hosts-private-relay-check\n";
            let put = client
                .request(Method::PUT, Some(&key), &[])?
                .body(payload.to_vec())
                .send()
                .await
                .map_err(|_| failure("check upload network failure"))?;
            require_success(put.status(), "check upload")?;
            let result = async {
                let anonymous = client
                    .http
                    .get(client.object_url(Some(&key)))
                    .send()
                    .await
                    .map_err(|_| failure("private-object verification failed"))?;
                if anonymous.status() != StatusCode::FORBIDDEN {
                    return Err(failure(
                        "anonymous object access was not denied with HTTP 403; private storage is unverified",
                    ));
                }
                let mut response = client
                    .request(Method::GET, Some(&key), &[])?
                    .send()
                    .await
                    .map_err(|_| failure("check download network failure"))?;
                require_success(response.status(), "check download")?;
                let mut bytes = Vec::new();
                while let Some(chunk) = response
                    .chunk()
                    .await
                    .map_err(|_| failure("check download failed"))?
                {
                    bytes.extend_from_slice(&chunk);
                    if bytes.len() > payload.len() {
                        return Err(failure("check size mismatch"));
                    }
                }
                if bytes != payload {
                    return Err(failure("check SHA-256 mismatch"));
                }
                Ok(())
            }
            .await;
            client.delete(&key).await?;
            result?;
            receipts.push(serde_json::json!({"profile":profile.name,"endpoint":profile.endpoint,
                "credential_id":profile.credential_id,"private":true,"sha256_verified":true,"cleanup_verified":true,"stale_multipart_aborted":stale_multipart_aborted}));
        }
        Ok(receipts)
    }
}

pub(crate) struct RelayClient {
    pub(crate) profile: MinioRelayProfile,
    http: Client,
    access_key: String,
    secret_key: SecretString,
}

impl RelayClient {
    fn new(
        profile: MinioRelayProfile,
        access_key: String,
        secret_key: SecretString,
    ) -> Result<Self, TransportError> {
        profile.validate()?;
        let http = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .read_timeout(Duration::from_secs(60))
            .timeout(Duration::from_secs(7200))
            .build()
            .map_err(|_| failure("HTTP client initialization failed"))?;
        Ok(Self {
            profile,
            http,
            access_key,
            secret_key,
        })
    }

    fn object_url(&self, key: Option<&str>) -> String {
        let base = self.profile.endpoint.trim_end_matches('/');
        let suffix = key.map_or_else(String::new, |key| format!("/{}", uri_encode(key, true)));
        format!("{base}/{}{suffix}", self.profile.bucket)
    }

    pub(crate) fn signed_url(&self, method: &str, key: &str) -> Result<String, TransportError> {
        self.sign(method, Some(key), &[], OffsetDateTime::now_utc())
    }

    fn sign(
        &self,
        method: &str,
        key: Option<&str>,
        extra: &[(&str, String)],
        now: OffsetDateTime,
    ) -> Result<String, TransportError> {
        let url = self.object_url(key);
        let parsed = Url::parse(&url).map_err(|_| failure("invalid object URL"))?;
        let hostname = parsed
            .host_str()
            .ok_or_else(|| failure("missing endpoint host"))?;
        let host = parsed
            .port()
            .map_or_else(|| hostname.to_owned(), |port| format!("{hostname}:{port}"));
        let date = format!(
            "{:04}{:02}{:02}",
            now.year(),
            u8::from(now.month()),
            now.day()
        );
        let timestamp = format!(
            "{date}T{:02}{:02}{:02}Z",
            now.hour(),
            now.minute(),
            now.second()
        );
        let scope = format!("{date}/us-east-1/s3/aws4_request");
        let mut fields = vec![
            ("X-Amz-Algorithm", "AWS4-HMAC-SHA256".to_owned()),
            ("X-Amz-Credential", format!("{}/{scope}", self.access_key)),
            ("X-Amz-Date", timestamp.clone()),
            ("X-Amz-Expires", URL_TTL.to_string()),
            ("X-Amz-SignedHeaders", "host".to_owned()),
        ];
        fields.extend(extra.iter().cloned());
        let mut encoded = fields
            .into_iter()
            .map(|(k, v)| (uri_encode(k, false), uri_encode(&v, false)))
            .collect::<Vec<_>>();
        encoded.sort();
        let query = encoded
            .into_iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        let canonical = format!(
            "{method}\n{}\n{query}\nhost:{host}\n\nhost\nUNSIGNED-PAYLOAD",
            parsed.path()
        );
        let to_sign = format!(
            "AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{:x}",
            Sha256::digest(canonical.as_bytes())
        );
        let initial = Zeroizing::new(format!("AWS4{}", self.secret_key.expose_secret()));
        let date_key = Zeroizing::new(hmac_sha256(initial.as_bytes(), date.as_bytes()));
        let region_key = Zeroizing::new(hmac_sha256(&*date_key, b"us-east-1"));
        let service_key = Zeroizing::new(hmac_sha256(&*region_key, b"s3"));
        let signing_key = Zeroizing::new(hmac_sha256(&*service_key, b"aws4_request"));
        let signature = hmac_sha256(&*signing_key, to_sign.as_bytes());
        let signature = hex_bytes(&signature);
        Ok(format!("{url}?{query}&X-Amz-Signature={signature}"))
    }

    fn request(
        &self,
        method: Method,
        key: Option<&str>,
        extra: &[(&str, String)],
    ) -> Result<reqwest::RequestBuilder, TransportError> {
        let url = self.sign(method.as_str(), key, extra, OffsetDateTime::now_utc())?;
        Ok(self.http.request(method, url))
    }

    async fn ensure_bucket(&self) -> Result<(), TransportError> {
        let head = self
            .request(Method::HEAD, None, &[])?
            .send()
            .await
            .map_err(|_| failure("bucket preflight network failure"))?;
        if head.status() == StatusCode::NOT_FOUND {
            let response = self
                .request(Method::PUT, None, &[])?
                .body(Vec::new())
                .send()
                .await
                .map_err(|_| failure("bucket creation network failure"))?;
            require_success(response.status(), "create relay bucket")?;
        } else {
            require_success(head.status(), "relay bucket preflight")?;
        }
        // Provision may be resumed after bucket creation. Never replace an existing lifecycle policy.
        let response = self
            .request(Method::GET, None, &[("lifecycle", String::new())])?
            .send()
            .await
            .map_err(|_| failure("bucket retention read failed"))?;
        if response.status().is_success() {
            let xml = bounded_xml(response).await?;
            if !xml.split("<Rule>").any(|rule| {
                let rule = rule.split("</Rule>").next().unwrap_or("");
                rule.contains("<ID>remote-hosts-orphans</ID>")
                    && rule.contains("<Status>Enabled</Status>")
                    && rule.contains("<Prefix>remote-hosts/</Prefix>")
                    && rule.contains("<Expiration><Days>1</Days></Expiration>")
            }) {
                return Err(failure(
                    "existing bucket lifecycle is not the relay policy; reconcile it explicitly",
                ));
            }
            return Ok(());
        }
        if response.status() != StatusCode::NOT_FOUND {
            return require_success(response.status(), "read relay retention");
        }
        let rule = "<LifecycleConfiguration><Rule><ID>remote-hosts-orphans</ID><Filter><Prefix>remote-hosts/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration><AbortIncompleteMultipartUpload><DaysAfterInitiation>1</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>";
        let checksum =
            base64::engine::general_purpose::STANDARD.encode(md5::Md5::digest(rule.as_bytes()));
        let response = self
            .request(Method::PUT, None, &[("lifecycle", String::new())])?
            .header("content-md5", checksum)
            .header("content-type", "application/xml")
            .body(rule)
            .send()
            .await
            .map_err(|_| failure("bucket retention setup failed"))?;
        require_success(response.status(), "configure relay retention")?;
        Ok(())
    }

    /// Older `MinIO` servers may accept but drop `AbortIncompleteMultipartUpload` lifecycle fields.
    /// Reap at most 20 uploads per invocation, scoped to this dedicated profile and older than
    /// one day (far beyond the two-hour operation deadline). Never abort current operations.
    async fn abort_stale_uploads(&self) -> Result<u32, TransportError> {
        let prefix = format!("remote-hosts/{}/", self.profile.name);
        let response = self
            .request(
                Method::GET,
                None,
                &[
                    ("uploads", String::new()),
                    ("prefix", prefix.clone()),
                    ("max-uploads", "20".into()),
                ],
            )?
            .send()
            .await
            .map_err(|_| failure("stale multipart listing failed"))?;
        require_success(response.status(), "list owned multipart uploads")?;
        let xml = bounded_xml(response).await?;
        let mut aborted = 0;
        for upload in xml.split("<Upload>").skip(1).take(20) {
            let upload = upload.split("</Upload>").next().unwrap_or("");
            let key = xml_field(upload, "Key")?;
            let upload_id = xml_field(upload, "UploadId")?;
            let initiated = xml_field(upload, "Initiated")?;
            if !stale_owned_upload(&prefix, &key, &initiated, OffsetDateTime::now_utc()) {
                continue;
            }
            let response = self
                .request(Method::DELETE, Some(&key), &[("uploadId", upload_id)])?
                .send()
                .await
                .map_err(|_| failure("stale multipart abort receipt unavailable"))?;
            if response.status() != StatusCode::NOT_FOUND {
                require_success(response.status(), "abort owned stale multipart")?;
            }
            aborted += 1;
        }
        Ok(aborted)
    }

    #[allow(clippy::too_many_lines)] // One ordered multipart transaction with abort on failure.
    pub(crate) async fn put_file(
        &self,
        key: &str,
        request: &SftpRequest,
        size: u64,
        expected_sha: &str,
    ) -> Result<(), TransportError> {
        // Housekeeping is best effort; failure does not reverse this operation's placement.
        if self.abort_stale_uploads().await.is_err() {
            tracing::warn!(profile=%self.profile.name,"owned stale multipart housekeeping pending");
        }
        // Bounded multipart upload; a failed part may retry the same upload ID/part number.
        let response = self
            .request(Method::POST, Some(key), &[("uploads", String::new())])?
            .send()
            .await
            .map_err(|_| failure("start multipart network failure"))?;
        require_success(response.status(), "start multipart")?;
        let xml = bounded_xml(response).await?;
        let upload_id = xml_field(&xml, "UploadId")?;
        let result = async {
            let mut source = tokio::fs::File::open(&request.spec.local_path)
                .await
                .map_err(|_| failure("cannot open upload source"))?;
            let mut total = 0u64;
            let mut hasher = Sha256::new();
            let mut parts = Vec::new();
            let mut number = 1;
            while total < size {
                let count = usize::try_from((size - total).min(PART_BYTES as u64))
                    .map_err(|_| failure("invalid part size"))?;
                let mut bytes = vec![0u8; count];
                source
                    .read_exact(&mut bytes)
                    .await
                    .map_err(|_| failure("upload source changed or became unreadable"))?;
                hasher.update(&bytes);
                let mut etag = None;
                for retry in 0..3 {
                    let sent = self
                        .request(
                            Method::PUT,
                            Some(key),
                            &[
                                ("partNumber", number.to_string()),
                                ("uploadId", upload_id.clone()),
                            ],
                        )?
                        .body(bytes.clone())
                        .send()
                        .await;
                    match sent {
                        Ok(response) if response.status().is_success() => {
                            etag = response
                                .headers()
                                .get("etag")
                                .and_then(|v| v.to_str().ok())
                                .map(str::to_owned);
                            break;
                        }
                        Ok(response) if response.status().is_client_error() => {
                            return Err(failure(
                                "multipart part rejected; check storage authorization",
                            ));
                        }
                        _ if retry < 2 => {
                            tokio::time::sleep(Duration::from_millis(250 * (retry + 1))).await;
                        }
                        _ => return Err(failure("multipart part failed after bounded retries")),
                    }
                }
                let etag = etag
                    .filter(|s| {
                        s.len() < 128
                            && s.bytes()
                                .all(|b| b.is_ascii_hexdigit() || b == b'"' || b == b'-')
                    })
                    .ok_or_else(|| failure("invalid multipart ETag"))?;
                parts.push(format!(
                    "<Part><PartNumber>{number}</PartNumber><ETag>{}</ETag></Part>",
                    etag.replace('"', "&quot;")
                ));
                total += count as u64;
                number += 1;
                super::emit_sftp_progress(request, "minio_uploading", total, Some(size), 0, 0);
            }
            let mut extra = [0u8; 1];
            if source
                .read(&mut extra)
                .await
                .map_err(|_| failure("source verification failed"))?
                != 0
                || format!("{:x}", hasher.finalize()) != expected_sha
            {
                return Err(failure("local file changed during relay upload"));
            }
            let response = self
                .request(Method::POST, Some(key), &[("uploadId", upload_id.clone())])?
                .body(format!(
                    "<CompleteMultipartUpload>{}</CompleteMultipartUpload>",
                    parts.join("")
                ))
                .send()
                .await
                .map_err(|_| failure("multipart completion receipt unavailable"))?;
            require_success(response.status(), "complete multipart")?;
            let xml = bounded_xml(response).await?;
            if !xml.contains("<CompleteMultipartUploadResult") || xml.contains("<Error>") {
                return Err(failure("multipart completion failed"));
            }
            Ok(())
        }
        .await;
        if result.is_err() {
            let _ = self
                .request(Method::DELETE, Some(key), &[("uploadId", upload_id)])?
                .send()
                .await;
        }
        result
    }

    pub(crate) async fn download(
        &self,
        key: &str,
        path: &Path,
        request: &SftpRequest,
        size: u64,
        expected_sha: &str,
    ) -> Result<(), TransportError> {
        let mut response = self
            .request(Method::GET, Some(key), &[])?
            .send()
            .await
            .map_err(|_| failure("relay download network failure"))?;
        require_success(response.status(), "relay download")?;
        if response.content_length().is_some_and(|n| n != size) {
            return Err(failure("relay object size mismatch"));
        }
        let mut file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
            .await
            .map_err(|_| failure("cannot create local relay temporary file"))?;
        super::set_local_mode(path, 0o600).await?;
        let mut total = 0u64;
        let mut last_reported = 0u64;
        let mut hasher = Sha256::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| failure("relay download interrupted"))?
        {
            total += chunk.len() as u64;
            if total > size || total > request.spec.max_size_bytes {
                return Err(failure("relay download exceeded expected size"));
            }
            file.write_all(&chunk)
                .await
                .map_err(|_| failure("local relay write failed"))?;
            hasher.update(&chunk);
            if total.saturating_sub(last_reported) >= 1024 * 1024 || total == size {
                super::emit_sftp_progress(request, "minio_downloading", total, Some(size), 0, 0);
                last_reported = total;
            }
        }
        file.sync_all()
            .await
            .map_err(|_| failure("local relay flush failed"))?;
        if total != size || format!("{:x}", hasher.finalize()) != expected_sha {
            return Err(failure("relay download SHA-256 mismatch"));
        }
        Ok(())
    }

    pub(crate) async fn delete(&self, key: &str) -> Result<(), TransportError> {
        let response = self
            .request(Method::DELETE, Some(key), &[])?
            .send()
            .await
            .map_err(|_| failure("relay cleanup network failure"))?;
        require_success(response.status(), "delete owned relay object")?;
        let response = self
            .request(Method::HEAD, Some(key), &[])?
            .send()
            .await
            .map_err(|_| failure("cleanup verification unavailable"))?;
        if response.status() != StatusCode::NOT_FOUND {
            return Err(failure("relay cleanup not confirmed"));
        }
        Ok(())
    }
}

fn stale_owned_upload(prefix: &str, key: &str, initiated: &str, now: OffsetDateTime) -> bool {
    key.starts_with(prefix)
        && OffsetDateTime::parse(initiated, &time::format_description::well_known::Rfc3339)
            .is_ok_and(|created| now - created > time::Duration::days(1))
}

fn require_success(status: StatusCode, stage: &str) -> Result<(), TransportError> {
    if status.is_success() {
        Ok(())
    } else {
        Err(failure(&format!("{stage}: HTTP {}", status.as_u16())))
    }
}

async fn bounded_xml(mut response: reqwest::Response) -> Result<String, TransportError> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| failure("storage response interrupted"))?
    {
        if bytes.len() + chunk.len() > 65536 {
            return Err(failure("storage response exceeded metadata limit"));
        }
        bytes.extend_from_slice(&chunk);
    }
    String::from_utf8(bytes).map_err(|_| failure("invalid storage XML encoding"))
}

fn xml_field(xml: &str, tag: &str) -> Result<String, TransportError> {
    let value = xml
        .split_once(&format!("<{tag}>"))
        .and_then(|(_, s)| s.split_once(&format!("</{tag}>")))
        .map(|(s, _)| s)
        .filter(|s| !s.is_empty() && s.len() < 2048)
        .ok_or_else(|| failure("missing storage response field"))?;
    Ok(value
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&"))
}

fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    bytes
        .iter()
        .flat_map(|b| {
            [
                char::from(HEX[usize::from(b >> 4)]),
                char::from(HEX[usize::from(b & 15)]),
            ]
        })
        .collect()
}

fn uri_encode(input: &str, keep_slashes: bool) -> String {
    input
        .bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) || keep_slashes && b == b'/' {
                char::from(b).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

// RFC 2104 over the existing SHA-256 implementation; verified with the RFC 4231 vector below.
fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut padded = Zeroizing::new([0u8; 64]);
    if key.len() > 64 {
        padded[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        padded[..key.len()].copy_from_slice(key);
    }
    let inner = Zeroizing::new(padded.map(|b| b ^ 0x36));
    let outer = Zeroizing::new(padded.map(|b| b ^ 0x5c));
    let mut hash = Sha256::new();
    hash.update(*inner);
    hash.update(data);
    let digest = hash.finalize();
    let mut hash = Sha256::new();
    hash.update(*outer);
    hash.update(digest);
    hash.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use remote_hosts_domain::{ConnectionMode, EnvironmentId, Protocol};

    fn fixture() -> (MinioRelayProfile, AccessPath) {
        let host_id = HostId::new();
        let access_path_id = AccessPathId::new();
        let credential_id = CredentialId::new();
        let profile = MinioRelayProfile {
            name: "approved-relay".into(),
            host_id,
            access_path_id,
            expected_hostname: "management-01".into(),
            endpoint: "https://storage.example.test".into(),
            bucket: "remote-hosts-transfer".into(),
            credential_id,
            threshold_bytes: 16 * 1024 * 1024,
            allow_http: false,
        };
        let path = AccessPath {
            id: access_path_id,
            host_id,
            environment_id: EnvironmentId::new(),
            connector_id: None,
            protocol: Protocol::Ssh,
            address: "gateway.example.test".into(),
            port: 22,
            username: "operator".into(),
            credential_id,
            route_type: RouteType::Bastion,
            proxy_chain: vec![],
            priority: 1,
            enabled: true,
            connection_mode: ConnectionMode::Pooled,
            idle_ttl_seconds: 600,
            keepalive_seconds: 30,
            max_concurrent_channels: 8,
            max_new_connections_per_minute: 1,
            requires_tty: true,
            notes: None,
        };
        (profile, path)
    }

    #[test]
    fn only_the_explicit_interactive_bastion_route_is_eligible() {
        let (profile, mut path) = fixture();
        assert!(profile.matches_route(&path));
        path.route_type = RouteType::Frp;
        assert!(!profile.matches_route(&path));
        path.route_type = RouteType::Bastion;
        path.requires_tty = false;
        assert!(!profile.matches_route(&path));
        path.requires_tty = true;
        path.host_id = HostId::new();
        assert!(!profile.matches_route(&path));
    }

    #[test]
    fn hmac_matches_rfc4231_and_encoding_preserves_object_boundaries() {
        let actual = hex_bytes(&hmac_sha256(&[0x0b; 20], b"Hi There"));
        assert_eq!(
            actual,
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
        assert_eq!(uri_encode("a/b c+%", true), "a/b%20c%2B%25");
        assert_eq!(uri_encode("a/b c+%", false), "a%2Fb%20c%2B%25");
    }

    #[test]
    fn policy_rejects_unapproved_http_and_business_buckets()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut p, _) = fixture();
        assert!(p.validate().is_ok());
        p.endpoint = "http://storage.test".into();
        assert!(p.validate().is_err());
        p.allow_http = true;
        assert!(p.validate().is_ok());
        p.bucket = "business-data".into();
        assert!(p.validate().is_err());
        p.bucket = "remote-hosts-transfer".into();
        p.endpoint = "https://user:secret@storage.test/path?token=private".into();
        let error = p
            .validate()
            .err()
            .ok_or("expected invalid policy")?
            .to_string();
        assert!(!error.contains("private"));
        assert!(!error.contains("user:secret"));
        Ok(())
    }

    #[test]
    fn presigned_urls_scope_method_host_object_and_expiry() -> Result<(), Box<dyn std::error::Error>>
    {
        let (p, _) = fixture();
        let client = RelayClient::new(
            p,
            "fixture-access".into(),
            SecretString::from("fixture-secret".to_owned()),
        )?;
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000)?;
        let get = client.sign("GET", Some("prefix/object +"), &[], now)?;
        let put = client.sign("PUT", Some("prefix/object +"), &[], now)?;
        assert!(get.starts_with(
            "https://storage.example.test/remote-hosts-transfer/prefix/object%20%2B?"
        ));
        assert!(get.contains("X-Amz-Expires=7200"));
        assert!(get.contains("X-Amz-SignedHeaders=host"));
        assert!(!get.contains("fixture-secret"));
        assert_ne!(get, put);
        Ok(())
    }
    #[test]
    fn stale_multipart_cleanup_never_touches_live_foreign_or_unknown_uploads()
    -> Result<(), Box<dyn std::error::Error>> {
        let now = OffsetDateTime::parse(
            "2026-09-05T00:00:00Z",
            &time::format_description::well_known::Rfc3339,
        )?;
        let prefix = "remote-hosts/approved/";
        assert!(stale_owned_upload(
            prefix,
            "remote-hosts/approved/old/payload",
            "2026-09-03T00:00:00Z",
            now
        ));
        assert!(!stale_owned_upload(
            prefix,
            "remote-hosts/other/old/payload",
            "2026-09-03T00:00:00Z",
            now
        ));
        assert!(!stale_owned_upload(
            prefix,
            "remote-hosts/approved/current/payload",
            "2026-09-04T23:00:00Z",
            now
        ));
        assert!(!stale_owned_upload(
            prefix,
            "remote-hosts/approved/unknown/payload",
            "unknown",
            now
        ));
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires authorized relay Vault configuration paths"]
    async fn live_retention_receipt_has_expected_fields() -> Result<(), Box<dyn std::error::Error>>
    {
        let config = std::env::var("REMOTE_HOSTS_RELAY_TEST_CONFIG")?;
        let database = std::env::var("REMOTE_HOSTS_RELAY_TEST_DATABASE")?;
        let master_path = std::env::var("REMOTE_HOSTS_RELAY_TEST_MASTER_FILE")?;
        let master = SecretString::from(std::fs::read_to_string(master_path)?.trim().to_owned());
        let pool = remote_hosts_db::connect_sqlite(&database).await?;
        let store = MinioRelayStore::load(Path::new(&config), Repositories::new(pool), master)?
            .ok_or("config")?;
        let client = store
            .client(store.config.profiles.first().ok_or("profile")?)
            .await?;
        client.ensure_bucket().await?;
        client.abort_stale_uploads().await?;
        Ok(())
    }
}
