//! Local-only authenticated Unix socket transport; no TCP listener and no password input.
use crate::{
    engine::{Engine, unix_time},
    executor::SystemBackend,
    filesystem::{Store, private_read, trusted_dir},
    protocol::*,
};
use anyhow::{Context, Result, ensure};
use std::{
    fs::{self, OpenOptions},
    os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::Path,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
};

pub fn default_socket() -> &'static str {
    if cfg!(target_os = "macos") {
        "/Library/Application Support/RemoteHostsAdmin/helper.sock"
    } else {
        "/run/remote-hosts-admin/helper.sock"
    }
}
async fn read_frame(stream: &mut UnixStream) -> Result<Vec<u8>> {
    let mut data = vec![];
    let mut reader = BufReader::new(stream.take((MAX_FRAME + 1) as u64));
    tokio::time::timeout(Duration::from_secs(5), reader.read_until(b'\n', &mut data))
        .await
        .context("request_frame_timeout")??;
    ensure!(
        !data.is_empty() && data.len() <= MAX_FRAME && data.last() == Some(&b'\n'),
        "invalid_or_oversized_frame"
    );
    Ok(data)
}
async fn write_frame(stream: &mut UnixStream, value: &serde_json::Value) -> Result<()> {
    let mut data = serde_json::to_vec(value)?;
    ensure!(data.len() < MAX_FRAME, "response_frame_too_large");
    data.push(b'\n');
    tokio::time::timeout(Duration::from_secs(5), stream.write_all(&data))
        .await
        .context("response_write_timeout")??;
    Ok(())
}
fn load_policy(path: &Path) -> Result<Policy> {
    trusted_dir(path.parent().context("policy_parent_missing")?, 0)?;
    let p: Policy = serde_json::from_slice(&private_read(path, 0)?)?;
    p.validate()?;
    ensure!(
        p.platform == std::env::consts::OS,
        "policy_platform_mismatch"
    );
    Ok(p)
}
pub async fn serve(policy_path: &Path, state_dir: &Path, socket_path: &Path) -> Result<()> {
    ensure!(
        nix::unistd::geteuid().is_root(),
        "helper_requires_separately_authorized_root_installation"
    );
    let policy = load_policy(policy_path)?;
    trusted_dir(state_dir, 0)?;
    let socket_dir = socket_path.parent().context("socket_parent_missing")?;
    trusted_dir(socket_dir, 0)?;
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(nix::libc::O_NOFOLLOW)
        .open(state_dir.join("helper.lock"))?;
    fs2::FileExt::try_lock_exclusive(&lock).context("helper_already_running")?;
    if let Ok(m) = fs::symlink_metadata(socket_path) {
        ensure!(
            m.file_type().is_socket() && m.uid() == 0,
            "refuse_replace_non_root_socket"
        );
        fs::remove_file(socket_path)?;
    }
    let listener = UnixListener::bind(socket_path)?;
    nix::unistd::chown(
        socket_path,
        Some(nix::unistd::Uid::from_raw(0)),
        Some(nix::unistd::Gid::from_raw(policy.allowed_gid)),
    )?;
    fs::set_permissions(socket_path, fs::Permissions::from_mode(0o660))?;
    let engine = Engine {
        store: Store {
            dir: state_dir.to_owned(),
            owner: 0,
        },
        backend: SystemBackend,
    };
    // Deliberately serialized: one maintenance action at a time. A client disconnect does
    // not cancel a mutation; its durable receipt is queried after reconnection.
    loop {
        let (mut stream, _) = listener.accept().await?;
        let result = async {
            let uid = stream.peer_cred()?.uid();
            let p = load_policy(policy_path)?;
            p.authorize(uid)?;
            let frame = read_frame(&mut stream).await?;
            let request: Request = serde_json::from_slice(&frame)?;
            engine.handle(&p, uid, request, unix_time()).await
        }
        .await;
        let value = match result {
            Ok(v) => serde_json::json!({"ok":true,"result":v}),
            Err(e) => serde_json::json!({"ok":false,"error":format!("{e:#}")}),
        };
        // All mutation state is already fsynced. Failure to deliver a response is not a retry.
        let _ = write_frame(&mut stream, &value).await;
    }
}
pub async fn request(socket: &Path, request: &Request) -> Result<serde_json::Value> {
    request.validate()?;
    let mut stream = UnixStream::connect(socket)
        .await
        .context("helper_unavailable_or_not_installed")?;
    ensure!(
        stream.peer_cred()?.uid() == 0,
        "refuse_non_root_helper_peer"
    );
    write_frame(&mut stream, &serde_json::to_value(request)?).await?;
    // Mutations may outlive a client. The caller must use the same request_id and receipt.
    let mut data = vec![];
    let mut reader = BufReader::new((&mut stream).take((MAX_FRAME + 1) as u64));
    tokio::time::timeout(
        Duration::from_secs(300),
        reader.read_until(b'\n', &mut data),
    )
    .await
    .context("helper_reply_timeout_query_receipt_do_not_replay")??;
    ensure!(
        !data.is_empty() && data.len() <= MAX_FRAME && data.last() == Some(&b'\n'),
        "invalid_helper_response"
    );
    Ok(serde_json::from_slice(&data)?)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn root_install_is_required() {
        if nix::unistd::geteuid().is_root() {
            return;
        }
        let d = tempfile::tempdir().unwrap();
        assert!(
            serve(&d.path().join("policy"), d.path(), &d.path().join("socket"))
                .await
                .unwrap_err()
                .to_string()
                .contains("separately_authorized")
        );
    }
    #[tokio::test]
    async fn normal_frame_roundtrip() {
        let (mut a, mut b) = UnixStream::pair().unwrap();
        write_frame(&mut a, &serde_json::json!({"op":"status"}))
            .await
            .unwrap();
        let data = read_frame(&mut b).await.unwrap();
        assert!(serde_json::from_slice::<Request>(&data).is_ok());
    }
    #[tokio::test]
    async fn oversized_frame_rejected() {
        let (mut a, mut b) = UnixStream::pair().unwrap();
        let write = tokio::spawn(async move {
            let _ = a.write_all(&vec![b'x'; MAX_FRAME + 1]).await;
        });
        assert!(read_frame(&mut b).await.is_err());
        write.await.unwrap();
    }
    #[tokio::test]
    async fn unterminated_frame_rejected() {
        let (mut a, mut b) = UnixStream::pair().unwrap();
        a.write_all(b"{}").await.unwrap();
        a.shutdown().await.unwrap();
        assert!(read_frame(&mut b).await.is_err());
    }
    #[tokio::test]
    async fn spoofed_non_root_server_rejected() {
        if nix::unistd::geteuid().is_root() {
            return;
        }
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("fake.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let task = tokio::spawn(async move {
            let _ = listener.accept().await;
        });
        assert!(
            request(&path, &Request::Status)
                .await
                .unwrap_err()
                .to_string()
                .contains("non_root_helper")
        );
        task.await.unwrap();
    }
}
