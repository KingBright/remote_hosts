//! Runs bounded relay control scripts through the existing captured PTY transport.

use super::{MinioRelayProfile, MinioRelayStore, RelayClient, failure};
use crate::{
    ConnectorPtyManager, InteractivePtyTransferHandle, ManagedPtyBackend, PtyBackendState,
    SftpDirection, SftpRequest, SftpResult, TransportError, WorkspaceId, emit_sftp_progress,
    ensure_expected_sha256, hash_local_source, interactive_pty_download_metadata_command,
    require_exec_transfer_success, resumable_remote_temporary_path,
    russh_exec_upload_finalize_command, shell_quote,
};
use std::path::Path;

impl<B: ManagedPtyBackend + 'static> ConnectorPtyManager<B> {
    #[allow(clippy::too_many_lines)] // Keep PTY ownership and restoration in one scope.
    pub(crate) async fn transfer_through_minio(
        &self,
        store: &MinioRelayStore,
        profile: &MinioRelayProfile,
        request: &SftpRequest,
        workspace_id: WorkspaceId,
    ) -> Result<Option<SftpResult>, TransportError> {
        let deadline = tokio::time::Instant::now()
            + std::time::Duration::from_secs(request.spec.timeout_seconds);
        // Use actual local size, not max_size_bytes (which is only a safety limit).
        let upload_meta = if request.spec.direction == SftpDirection::Upload {
            let metadata = tokio::fs::metadata(&request.spec.local_path)
                .await
                .map_err(|_| failure("local source is unavailable"))?;
            if metadata.len() < profile.threshold_bytes {
                return Ok(None);
            }
            let (size, sha) = hash_local_source(
                Path::new(&request.spec.local_path),
                request.spec.max_size_bytes,
            )
            .await?;
            ensure_expected_sha256(&request.spec, &sha)?;
            Some((size, sha))
        } else {
            None
        };
        let sessions = self
            .repositories
            .pty_sessions
            .list_for_workspace(workspace_id)
            .await
            .map_err(|_| failure("target PTY metadata unavailable"))?;
        let live = sessions
            .iter()
            .filter(|s| s.backend_state == PtyBackendState::Active && s.input_allowed)
            .collect::<Vec<_>>();
        if live.len() != 1 || live[0].interaction.is_some() || live[0].foreground_process.is_some()
        {
            return Err(failure(
                "requires exactly one idle, input-ready target PTY; finish foreground work or resolve the active interaction",
            ));
        }
        let pty_id = live[0].pty_session_id;
        let handle = {
            let active = self.active.lock().await;
            let selected = active
                .get(&pty_id)
                .ok_or_else(|| failure("selected PTY is no longer active"))?;
            InteractivePtyTransferHandle {
                input_tx: selected.input_tx.clone(),
                transfer_lock: std::sync::Arc::clone(&selected.transfer_lock),
                transfer_capture: std::sync::Arc::clone(&selected.transfer_capture),
            }
        };
        let _guard = handle
            .transfer_lock
            .try_lock()
            .map_err(|_| failure("selected PTY is busy; wait for its current input or transfer"))?;
        let current = self
            .repositories
            .pty_sessions
            .get(pty_id)
            .await
            .map_err(|_| failure("PTY state recheck failed"))?
            .ok_or_else(|| failure("selected PTY disappeared"))?;
        if current.backend_state != PtyBackendState::Active
            || !current.input_allowed
            || current.interaction.is_some()
            || current.foreground_process.is_some()
        {
            return Err(failure("selected PTY is no longer idle and input-ready"));
        }
        self.enter_pty_transfer_mode(&handle, request.operation_id)
            .await?;
        let result = self
            .run_minio_transfer(&handle, store, profile, request, upload_meta, deadline)
            .await;
        let restore = if deadline.saturating_duration_since(tokio::time::Instant::now())
            > std::time::Duration::from_secs(4)
        {
            tokio::time::timeout(
                std::time::Duration::from_secs(2),
                self.leave_pty_transfer_mode(&handle, request.operation_id),
            )
            .await
            .unwrap_or_else(|_| Err(failure("terminal restoration receipt pending")))
        } else {
            Err(failure(
                "terminal restoration deferred at operation deadline",
            ))
        };
        match (result, restore) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(error), _) | (Ok(None), Err(error)) => Err(error),
            (Ok(Some(mut value)), Err(_)) => {
                value.warnings.push("pty_restore_pending".into());
                // Placement was verified. Terminal-restoration trouble must not replay the file mutation.
                tracing::warn!(operation_id=%request.operation_id,"MinIO placement verified but PTY restoration failed; inspect this PTY before more input");
                Ok(Some(value))
            }
        }
    }

    async fn run_minio_transfer(
        &self,
        handle: &InteractivePtyTransferHandle,
        store: &MinioRelayStore,
        profile: &MinioRelayProfile,
        request: &SftpRequest,
        upload_meta: Option<(u64, String)>,
        deadline: tokio::time::Instant,
    ) -> Result<Option<SftpResult>, TransportError> {
        let identity = identity_script(&profile.expected_hostname);
        let outcome = self
            .execute_pty_transfer_stage(handle, request.operation_id, "minio-preflight", &identity)
            .await?;
        if outcome.exit_code != Some(0)
            || !outcome
                .stdout
                .lines()
                .any(|l| l.trim() == "REMOTE_HOSTS_RELAY_TARGET_OK")
        {
            return Err(failure(
                "target identity or required curl/SHA-256 tooling was not verified",
            ));
        }
        let (size, sha) = if let Some(meta) = upload_meta {
            meta
        } else {
            let outcome = self
                .execute_pty_transfer_stage(
                    handle,
                    request.operation_id,
                    "minio-source-metadata",
                    &interactive_pty_download_metadata_command(&request.spec),
                )
                .await?;
            require_exec_transfer_success(
                &outcome,
                "inspect relay source",
                &request.spec.remote_path,
                "",
            )?;
            let meta = crate::parse_transfer_marker(&outcome.stdout, "REMOTE_HOSTS_DOWNLOAD_META")?;
            crate::ensure_size_within_limit(meta.0, request.spec.max_size_bytes)?;
            if meta.0 < profile.threshold_bytes {
                return Ok(None);
            }
            ensure_expected_sha256(&request.spec, &meta.1)?;
            meta
        };
        let client = store.client(profile).await?;
        let key = format!(
            "remote-hosts/{}/{}/{sha}/payload",
            profile.name, request.operation_id
        );
        emit_sftp_progress(request, "minio_preflight", 0, Some(size), 0, 0);
        let mut result = match request.spec.direction {
            SftpDirection::Upload => {
                self.minio_upload(handle, &client, request, &key, size, &sha)
                    .await
            }
            SftpDirection::Download => {
                self.minio_download(handle, &client, request, &key, size, &sha)
                    .await
            }
        };
        if result.is_ok() {
            emit_sftp_progress(request, "minio_cleanup", size, Some(size), 0, 0);
            let cleanup = if deadline.saturating_duration_since(tokio::time::Instant::now())
                > std::time::Duration::from_secs(8)
            {
                tokio::time::timeout(std::time::Duration::from_secs(2), client.delete(&key))
                    .await
                    .unwrap_or_else(|_| Err(failure("cleanup receipt pending")))
            } else {
                Err(failure("cleanup deferred at operation deadline"))
            };
            if cleanup.is_err() {
                if let Ok(value) = &mut result {
                    value.warnings.push("minio_cleanup_pending".into());
                }
                tracing::warn!(operation_id=%request.operation_id,profile=%profile.name,
                    "MinIO placement verified; exact relay-object cleanup pending, retention remains the safety net");
                emit_sftp_progress(request, "minio_cleanup_pending", size, Some(size), 0, 0);
            }
        }
        // On missing receipts preserve the exact temporary object for diagnosis; never guess that
        // a still-running remote curl has stopped. Bucket lifecycle expires orphaned objects.
        result.map(|mut value| {
            value.transfer_method = Some("minio_relay".into());
            Some(value)
        })
    }

    async fn minio_upload(
        &self,
        handle: &InteractivePtyTransferHandle,
        client: &RelayClient,
        request: &SftpRequest,
        key: &str,
        size: u64,
        sha: &str,
    ) -> Result<SftpResult, TransportError> {
        let temporary = resumable_remote_temporary_path(&request.spec.remote_path, sha);
        // Fail parent/type/overwrite checks before uploading a large object. This existing staging
        // initializer is idempotent and verifies any reusable partial bytes.
        let preparation = self
            .prepare_interactive_pty_upload(handle, request, size, sha, &temporary)
            .await?;
        let resume_bytes = match preparation {
            crate::InteractivePtyUploadPreparation::Complete(result) => return Ok(result),
            crate::InteractivePtyUploadPreparation::Pending { resume_bytes, .. } => resume_bytes,
        };
        if resume_bytes < size {
            client.put_file(key, request, size, sha).await?;
        }
        emit_sftp_progress(
            request,
            "minio_target_downloading",
            resume_bytes,
            Some(size),
            resume_bytes,
            0,
        );
        if resume_bytes < size {
            let url = client.signed_url("GET", key)?;
            let script = download_script(&url, &temporary, size, request.spec.timeout_seconds);
            let outcome = self
                .execute_pty_transfer_stage_with_timeout(
                    handle,
                    request.operation_id,
                    "minio-target-download",
                    &script,
                    std::time::Duration::from_secs(request.spec.timeout_seconds),
                )
                .await?;
            if outcome.exit_code != Some(0) {
                return Err(failure(
                    "target HTTP download failed; retain operation identity and inspect target route",
                ));
            }
        }
        emit_sftp_progress(request, "minio_target_verifying", size, Some(size), 0, 0);
        let script = russh_exec_upload_finalize_command(&request.spec, &temporary, size, sha);
        let outcome = self
            .execute_pty_transfer_stage(handle, request.operation_id, "minio-target-place", &script)
            .await?;
        require_exec_transfer_success(
            &outcome,
            "REMOTE_HOSTS_UPLOAD_COMPLETE",
            &request.spec.remote_path,
            &temporary,
        )?;
        let (remote_size, remote_sha) =
            crate::parse_transfer_marker(&outcome.stdout, "REMOTE_HOSTS_TRANSFER_OK")?;
        if remote_size != size || remote_sha != sha {
            return Err(failure("target placement SHA-256 mismatch"));
        }
        Ok(make_result(request, size, sha))
    }

    async fn minio_download(
        &self,
        handle: &InteractivePtyTransferHandle,
        client: &RelayClient,
        request: &SftpRequest,
        key: &str,
        size: u64,
        sha: &str,
    ) -> Result<SftpResult, TransportError> {
        let destination = Path::new(&request.spec.local_path);
        crate::ensure_local_destination(destination, request.spec.overwrite).await?;
        let temporary = crate::local_temporary_path(destination, request.operation_id)?;
        crate::cleanup_local_temporary_file(&temporary).await?;
        let url = client.signed_url("PUT", key)?;
        let script = upload_script(
            &url,
            &request.spec.remote_path,
            size,
            sha,
            request.spec.timeout_seconds,
        );
        emit_sftp_progress(request, "minio_target_uploading", 0, Some(size), 0, 0);
        let outcome = self
            .execute_pty_transfer_stage_with_timeout(
                handle,
                request.operation_id,
                "minio-target-upload",
                &script,
                std::time::Duration::from_secs(request.spec.timeout_seconds),
            )
            .await?;
        if outcome.exit_code != Some(0)
            || !outcome
                .stdout
                .lines()
                .any(|l| l.trim() == "REMOTE_HOSTS_RELAY_UPLOAD_OK")
        {
            return Err(failure("target upload or source stability check failed"));
        }
        let transfer = async {
            client.download(key, &temporary, request, size, sha).await?;
            if let Some(mode) = request.spec.mode {
                crate::set_local_mode(&temporary, mode).await?;
            }
            crate::place_local_file(&temporary, destination, request.spec.overwrite).await?;
            Ok(make_result(request, size, sha))
        }
        .await;
        if transfer.is_err() {
            let _ = tokio::fs::remove_file(&temporary).await;
        }
        transfer
    }
}

fn make_result(request: &SftpRequest, size: u64, sha: &str) -> SftpResult {
    SftpResult {
        direction: request.spec.direction,
        bytes_transferred: size,
        sha256: sha.into(),
        local_path: request.spec.local_path.clone(),
        remote_path: request.spec.remote_path.clone(),
        overwrite: request.spec.overwrite,
        transfer_method: Some("minio_relay".into()),
        warnings: Vec::new(),
    }
}

fn identity_script(hostname: &str) -> String {
    format!(
        "set -eu\n[ \"$(hostname)\" = {} ] || exit 81\ncommand -v curl >/dev/null || exit 82\ncommand -v sha256sum >/dev/null || command -v shasum >/dev/null || exit 83\nprintf 'REMOTE_HOSTS_RELAY_TARGET_OK\\n'\n",
        shell_quote(hostname)
    )
}

fn curl_config(url: &str) -> String {
    // URLs are constructed by the signer; reject controls at the policy boundary as well.
    format!(
        "url = \"{}\"\n",
        url.replace('\\', "\\\\").replace('"', "\\\"")
    )
}

fn download_script(url: &str, temporary: &str, size: u64, timeout: u64) -> String {
    format!(
        "set -eu\numask 077\ndst={}\n[ -f \"$dst\" ] && [ ! -L \"$dst\" ] || exit 84\ncurl --silent --show-error --fail --noproxy '*' --connect-timeout 10 --max-time {timeout} --speed-time 60 --speed-limit 1024 --max-filesize {size} --continue-at - --output \"$dst\" --config - <<'REMOTE_HOSTS_CURL_CONFIG' 2>/dev/null\n{}REMOTE_HOSTS_CURL_CONFIG\n",
        shell_quote(temporary),
        curl_config(url)
    )
}

fn upload_script(url: &str, source: &str, size: u64, sha: &str, timeout: u64) -> String {
    let check = format!(
        "[ -f \"$src\" ] && [ ! -L \"$src\" ] || exit 84\n[ \"$(wc -c < \"$src\" | tr -d '[:space:]')\" = {size} ] || exit 85\nif command -v sha256sum >/dev/null; then actual=$(sha256sum \"$src\" | awk '{{print $1}}'); else actual=$(shasum -a 256 \"$src\" | awk '{{print $1}}'); fi\n[ \"$actual\" = {} ] || exit 86\n",
        shell_quote(sha)
    );
    format!(
        "set -eu\nsrc={}\n{check}curl --silent --show-error --fail --noproxy '*' --connect-timeout 10 --max-time {timeout} --speed-time 60 --speed-limit 1024 --upload-file \"$src\" --output /dev/null --config - <<'REMOTE_HOSTS_CURL_CONFIG' 2>/dev/null\n{}REMOTE_HOSTS_CURL_CONFIG\n{check}printf 'REMOTE_HOSTS_RELAY_UPLOAD_OK\\n'\n",
        shell_quote(source),
        curl_config(url)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scripts_pin_identity_and_keep_signed_urls_out_of_curl_arguments()
    -> Result<(), Box<dyn std::error::Error>> {
        assert!(identity_script("target-01").contains("$(hostname)"));
        let script = download_script(
            "https://storage.test/b/o?X-Amz-Signature=fixture",
            "/tmp/file ' with spaces",
            32,
            60,
        );
        let command = script
            .lines()
            .find(|l| l.starts_with("curl "))
            .ok_or("curl command")?;
        assert!(!command.contains("X-Amz"));
        assert!(command.contains("--config -"));
        assert!(!command.contains("--location"));
        assert!(script.contains("umask 077"));
        assert!(
            upload_script(
                "https://storage.test/b/o",
                "/tmp/source",
                32,
                &"a".repeat(64),
                60
            )
            .matches("$actual")
            .count()
                == 2
        );
        Ok(())
    }
    async fn run_private_script(
        script: &str,
    ) -> Result<std::process::Output, Box<dyn std::error::Error>> {
        use tokio::io::AsyncWriteExt;
        let mut child = tokio::process::Command::new("sh")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()?;
        child
            .stdin
            .take()
            .ok_or("stdin")?
            .write_all(script.as_bytes())
            .await?;
        Ok(child.wait_with_output().await?)
    }

    #[tokio::test]
    async fn identity_and_source_hash_fail_before_network_or_placement()
    -> Result<(), Box<dyn std::error::Error>> {
        let output =
            run_private_script(&identity_script("remote-hosts-deliberately-wrong-hostname"))
                .await?;
        assert_eq!(output.status.code(), Some(81));
        let directory = tempfile::tempdir()?;
        let source = directory.path().join("source ' quoted");
        tokio::fs::write(&source, b"abc").await?;
        let output = run_private_script(&upload_script(
            "http://127.0.0.1:1/never-contact",
            source.to_str().ok_or("source path")?,
            3,
            &"0".repeat(64),
            2,
        ))
        .await?;
        assert_eq!(output.status.code(), Some(86));
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires explicitly authorized private relay and vault paths in REMOTE_HOSTS_RELAY_TEST_* env"]
    #[allow(clippy::too_many_lines)] // A single isolated live lifecycle with guaranteed owned-object cleanup.
    async fn live_private_multipart_and_curl_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        use remote_hosts_core::{FileTransferSpec, SftpOverwritePolicy};
        use remote_hosts_domain::OperationId;
        use sha2::{Digest, Sha256};
        let config = std::env::var("REMOTE_HOSTS_RELAY_TEST_CONFIG")?;
        let database = std::env::var("REMOTE_HOSTS_RELAY_TEST_DATABASE")?;
        let master_path = std::env::var("REMOTE_HOSTS_RELAY_TEST_MASTER_FILE")?;
        let master =
            secrecy::SecretString::from(std::fs::read_to_string(master_path)?.trim().to_owned());
        let pool = remote_hosts_db::connect_sqlite(&database).await?;
        let store = MinioRelayStore::load(
            Path::new(&config),
            remote_hosts_db::Repositories::new(pool),
            master,
        )?
        .ok_or("missing config")?;
        let profile = store.config.profiles.first().ok_or("missing profile")?;
        let client = store.client(profile).await?;
        let directory = tempfile::tempdir()?;
        let source = directory.path().join("source ' multipart.bin");
        let local_download = directory.path().join("native-download.bin");
        let target_download = directory.path().join("target ' download.bin");
        let final_path = directory.path().join("placed.bin");
        let bytes = vec![0x5a; 33 * 1024 * 1024 + 17];
        let size = bytes.len() as u64;
        let sha = format!("{:x}", Sha256::digest(&bytes));
        tokio::fs::write(&source, &bytes).await?;
        let request = SftpRequest {
            operation_id: OperationId::new(),
            host_id: profile.host_id,
            access_path_id: profile.access_path_id,
            progress_tx: None,
            spec: FileTransferSpec {
                direction: SftpDirection::Upload,
                local_path: source.to_string_lossy().into_owned(),
                remote_path: final_path.to_string_lossy().into_owned(),
                overwrite: SftpOverwritePolicy::Deny,
                mode: Some(0o600),
                max_size_bytes: size,
                expected_sha256: Some(sha.clone()),
                timeout_seconds: 120,
            },
        };
        let key = format!(
            "remote-hosts/{}/check-{}/{sha}/payload",
            profile.name, request.operation_id
        );
        let result = async {
            client.put_file(&key, &request, size, &sha).await?;
            client
                .download(&key, &local_download, &request, size, &sha)
                .await?;
            assert_eq!(tokio::fs::read(&local_download).await?, bytes);
            // Exercise the exact target curl GET with an existing partial file and quoted path.
            tokio::fs::write(&target_download, &bytes[..123_456]).await?;
            let script = download_script(
                &client.signed_url("GET", &key)?,
                target_download.to_str().ok_or("path")?,
                size,
                120,
            );
            assert!(
                run_private_script(&script).await?.status.success(),
                "target GET status"
            );
            let finalize = russh_exec_upload_finalize_command(
                &request.spec,
                target_download.to_str().ok_or("path")?,
                size,
                &sha,
            );
            assert!(
                run_private_script(&finalize).await?.status.success(),
                "atomic final placement"
            );
            assert_eq!(tokio::fs::read(&final_path).await?, bytes);
            // The opposite direction uses the exact target curl PUT, then verifies native GET.
            client.delete(&key).await?;
            let script = upload_script(
                &client.signed_url("PUT", &key)?,
                source.to_str().ok_or("path")?,
                size,
                &sha,
                120,
            );
            assert!(
                run_private_script(&script).await?.status.success(),
                "target PUT status"
            );
            tokio::fs::remove_file(&local_download).await?;
            client
                .download(&key, &local_download, &request, size, &sha)
                .await?;
            assert_eq!(tokio::fs::read(&local_download).await?, bytes);
            // Wrong expected hash must never place the temporary bytes.
            let mismatch = client
                .download(&key, &target_download, &request, size, &"0".repeat(64))
                .await;
            assert!(mismatch.is_err());
            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await;
        let cleanup = client.delete(&key).await;
        result?;
        cleanup?;
        Ok(())
    }
}
