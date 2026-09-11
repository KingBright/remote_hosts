//! Range recovery within a live operation. Never append a full 200 to a partial file.
//! Process restart recovery requires a separate durable publication journal.
use crate::progress::Progress;
use anyhow::{Context, Result, bail, ensure};
use reqwest::{Client, StatusCode, Url, header};
use sha2::{Digest, Sha256};
use std::time::Duration;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

pub(crate) const IDLE: Duration = Duration::from_secs(60);
const CHECKPOINT: u64 = 4 * 1024 * 1024;
#[derive(Clone, Copy)]
pub(crate) struct Policy {
    pub idle: Duration,
    pub retries: u32,
    pub backoff: Duration,
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            idle: IDLE,
            retries: 6,
            backoff: Duration::from_millis(250),
        }
    }
}
fn range(value: &str) -> Result<(u64, u64, u64)> {
    let (span, total) = value
        .strip_prefix("bytes ")
        .context("invalid Content-Range unit")?
        .split_once('/')
        .context("invalid Content-Range")?;
    let (start, end) = span.split_once('-').context("invalid Content-Range span")?;
    let (start, end, total) = (
        start.parse::<u64>()?,
        end.parse::<u64>()?,
        total.parse::<u64>()?,
    );
    ensure!(start <= end && end < total, "invalid Content-Range bounds");
    Ok((start, end, total))
}
fn strong_etag(headers: &header::HeaderMap) -> Option<String> {
    headers
        .get(header::ETAG)
        .and_then(|v| v.to_str().ok())
        .filter(|s| s.starts_with('"') && s.ends_with('"') && s.len() <= 1024)
        .map(str::to_owned)
}
async fn retry(p: &Progress, offset: u64, attempts: &mut u32, policy: Policy) -> Result<()> {
    *attempts += 1;
    ensure!(
        *attempts <= policy.retries,
        "transfer_retry_exhausted: destination unchanged; partial transfer was not published"
    );
    p.retry(offset);
    tokio::time::sleep(
        policy
            .backoff
            .saturating_mul(1u32 << (*attempts - 1).min(5)),
    )
    .await;
    Ok(())
}
/// `bytes_done` counts bytes written to staging, not socket-buffer reads.
pub(crate) async fn fetch(
    http: &Client,
    url: &Url,
    output: &mut tokio::fs::File,
    max: usize,
    expected_sha: Option<&str>,
    p: &Progress,
    policy: Policy,
) -> Result<(usize, String)> {
    let mut offset = 0u64;
    let mut total = None;
    let mut etag: Option<String> = None;
    let mut digest = Sha256::new();
    let mut attempts = 0;
    let mut synced = 0u64;
    loop {
        if offset > 0 && total == Some(offset) {
            break;
        }
        if offset > 0 && etag.is_none() && expected_sha.is_none() {
            // Without a strong identity, two responses must not be combined.
            output.set_len(0).await?;
            output.seek(std::io::SeekFrom::Start(0)).await?;
            offset = 0;
            total = None;
            synced = 0;
            digest = Sha256::new();
            p.advance(0, None);
        }
        p.phase("connecting");
        let mut request = http
            .get(url.clone())
            .header(header::ACCEPT_ENCODING, "identity");
        if offset > 0 {
            request = request.header(header::RANGE, format!("bytes={offset}-"));
            if let Some(etag) = &etag {
                request = request.header(header::IF_RANGE, etag);
            }
        }
        let response = tokio::time::timeout(policy.idle, request.send()).await;
        let mut response = match response {
            Ok(Ok(r)) => r,
            _ => {
                retry(p, offset, &mut attempts, policy).await?;
                continue;
            }
        };
        if response.status().is_server_error()
            || matches!(
                response.status(),
                StatusCode::TOO_MANY_REQUESTS | StatusCode::REQUEST_TIMEOUT
            )
        {
            drop(response);
            retry(p, offset, &mut attempts, policy).await?;
            continue;
        }
        ensure!(
            response
                .headers()
                .get(header::CONTENT_ENCODING)
                .is_none_or(|v| v == "identity"),
            "encoded response cannot be safely range-resumed"
        );
        let returned_etag = strong_etag(response.headers());
        let end = match response.status() {
            StatusCode::PARTIAL_CONTENT => {
                let value = response
                    .headers()
                    .get(header::CONTENT_RANGE)
                    .and_then(|v| v.to_str().ok())
                    .context("partial response missing Content-Range")?;
                let (start, last, length) = range(value)?;
                ensure!(
                    start == offset && length <= max as u64,
                    "range_offset_or_size_mismatch: destination unchanged"
                );
                ensure!(
                    total.is_none_or(|n| n == length),
                    "source_changed: length changed during resume"
                );
                if let (Some(old), Some(new)) = (&etag, &returned_etag) {
                    ensure!(old == new, "source_changed: ETag changed during resume");
                }
                ensure!(
                    response
                        .content_length()
                        .is_none_or(|n| n == last - start + 1),
                    "partial response length mismatch"
                );
                total = Some(length);
                Some(last + 1)
            }
            StatusCode::OK => {
                if offset > 0 {
                    // Range ignored or If-Range failed: restart from this full body.
                    output.set_len(0).await?;
                    output.seek(std::io::SeekFrom::Start(0)).await?;
                    offset = 0;
                    synced = 0;
                    digest = Sha256::new();
                }
                total = response.content_length();
                ensure!(
                    total.is_none_or(|n| n <= max as u64),
                    "file exceeds max_bytes"
                );
                total
            }
            _ => bail!(
                "file source rejected download (HTTP {}); redirects are not followed",
                response.status().as_u16()
            ),
        };
        if returned_etag.is_some() || offset == 0 {
            etag = returned_etag;
        }
        ensure!(
            !(response.status() == StatusCode::PARTIAL_CONTENT
                && end.zip(total).is_some_and(|(e, t)| e < t)
                && etag.is_none()
                && expected_sha.is_none()),
            "partial response has no stable content identity"
        );
        p.phase("transferring");
        p.advance(offset, total);
        let mut interrupted = false;
        loop {
            let next = tokio::time::timeout(policy.idle, response.chunk()).await;
            match next {
                Ok(Ok(Some(bytes))) => {
                    let next = offset
                        .checked_add(bytes.len() as u64)
                        .context("file size overflow")?;
                    ensure!(
                        next <= max as u64 && end.is_none_or(|n| next <= n),
                        "file exceeds declared range or max_bytes"
                    );
                    output.write_all(&bytes).await?;
                    digest.update(&bytes);
                    offset = next;
                    if offset - synced >= CHECKPOINT {
                        output.sync_data().await?;
                        synced = offset;
                    }
                    p.advance(offset, total);
                }
                Ok(Ok(None)) => break,
                _ => {
                    interrupted = true;
                    break;
                }
            }
        }
        if interrupted || end.is_some_and(|n| n != offset) {
            output.sync_data().await?;
            retry(p, offset, &mut attempts, policy).await?;
            continue;
        }
        if total.is_some_and(|n| offset < n) {
            continue;
        }
        break;
    }
    p.phase("verifying");
    let sha = format!("{:x}", digest.finalize());
    ensure!(
        expected_sha.is_none_or(|expected| expected == sha),
        "sha256_mismatch: destination unchanged"
    );
    output.sync_all().await?;
    p.advance(offset, Some(offset));
    Ok((offset as usize, sha))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Router,
        body::{Body, Bytes},
        response::Response,
        routing::get,
    };
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    async fn exercise(mode: &str) -> (Result<(usize, String)>, Vec<u8>, Vec<String>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(Mutex::new(vec![]));
        let c = calls.clone();
        let s = seen.clone();
        let mode = mode.to_owned();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!("http://{}/file", listener.local_addr().unwrap())).unwrap();
        let router = Router::new().route(
            "/file",
            get(move |headers: header::HeaderMap| {
                let c = c.clone();
                let s = s.clone();
                let mode = mode.clone();
                async move {
                    s.lock().unwrap().push(
                        headers
                            .get(header::RANGE)
                            .and_then(|x| x.to_str().ok())
                            .unwrap_or("")
                            .to_owned(),
                    );
                    let n = c.fetch_add(1, Ordering::Relaxed);
                    if n == 0 {
                        let stream = futures_util::stream::unfold(0, |step| async move {
                            match step {
                                0 => Some((Ok::<_, std::io::Error>(Bytes::from_static(b"abc")), 1)),
                                1 => {
                                    tokio::time::sleep(Duration::from_millis(80)).await;
                                    Some((Err(std::io::Error::other("synthetic disconnect")), 2))
                                }
                                _ => None,
                            }
                        });
                        return Response::builder()
                            .header(header::ETAG, "\"stable\"")
                            .header(header::CONTENT_LENGTH, "6")
                            .body(Body::from_stream(stream))
                            .unwrap();
                    }
                    if mode == "ignore" {
                        return Response::builder()
                            .header(header::ETAG, "\"new\"")
                            .body(Body::from("uvwxyz"))
                            .unwrap();
                    }
                    let content_range = if mode == "bad-range" {
                        "bytes 2-5/6"
                    } else {
                        "bytes 3-5/6"
                    };
                    let etag = if mode == "changed" {
                        "\"changed\""
                    } else {
                        "\"stable\""
                    };
                    Response::builder()
                        .status(206)
                        .header(header::CONTENT_RANGE, content_range)
                        .header(header::ETAG, etag)
                        .body(Body::from("def"))
                        .unwrap()
                }
            }),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("part");
        let mut file = tokio::fs::File::create(&path).await.unwrap();
        let p = Progress::new(&uuid::Uuid::new_v4().to_string());
        let result = fetch(
            &Client::new(),
            &url,
            &mut file,
            64,
            None,
            &p,
            Policy {
                idle: Duration::from_secs(2),
                retries: 2,
                backoff: Duration::ZERO,
            },
        )
        .await;
        drop(file);
        server.abort();
        let bytes = std::fs::read(path).unwrap();
        let seen = seen.lock().unwrap().clone();
        (result, bytes, seen)
    }
    #[tokio::test]
    async fn resumes_from_received_offset_without_duplicate_bytes() {
        let (r, bytes, seen) = exercise("resume").await;
        assert_eq!(r.unwrap(), (6, crate::hash(b"abcdef")));
        assert_eq!(bytes, b"abcdef");
        assert_eq!(seen, vec!["", "bytes=3-"]);
    }
    #[tokio::test]
    async fn ignored_range_restarts_instead_of_appending() {
        let (r, bytes, _) = exercise("ignore").await;
        assert_eq!(r.unwrap().0, 6);
        assert_eq!(bytes, b"uvwxyz");
    }
    #[tokio::test]
    async fn invalid_range_or_changed_etag_does_not_append() {
        for mode in ["bad-range", "changed"] {
            let (r, bytes, _) = exercise(mode).await;
            assert!(r.is_err());
            assert_eq!(bytes, b"abc");
        }
    }
    #[tokio::test]
    async fn active_stream_may_exceed_idle_interval_but_stalled_stream_fails() {
        for stall in [false, true] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url =
                Url::parse(&format!("http://{}/file", listener.local_addr().unwrap())).unwrap();
            let router = Router::new().route(
                "/file",
                get(move || async move {
                    let stream = futures_util::stream::unfold(0, move |n| async move {
                        if n >= 8 {
                            return None;
                        }
                        tokio::time::sleep(Duration::from_millis(if stall { 400 } else { 50 }))
                            .await;
                        Some((Ok::<_, std::io::Error>(Bytes::from_static(b"x")), n + 1))
                    });
                    Response::builder()
                        .header(header::CONTENT_LENGTH, "8")
                        .body(Body::from_stream(stream))
                        .unwrap()
                }),
            );
            let server = tokio::spawn(async move {
                axum::serve(listener, router).await.unwrap();
            });
            let d = tempfile::tempdir().unwrap();
            let mut output = tokio::fs::File::create(d.path().join("part"))
                .await
                .unwrap();
            let p = Progress::new(&uuid::Uuid::new_v4().to_string());
            let started = std::time::Instant::now();
            let result = fetch(
                &Client::new(),
                &url,
                &mut output,
                64,
                None,
                &p,
                Policy {
                    idle: Duration::from_millis(200),
                    retries: 0,
                    backoff: Duration::ZERO,
                },
            )
            .await;
            server.abort();
            if stall {
                assert!(result.is_err());
            } else {
                assert_eq!(result.unwrap().0, 8);
                assert!(started.elapsed() >= Duration::from_millis(350));
            }
        }
    }
    #[test]
    fn validates_ranges_and_rejects_unknown_lengths() {
        assert_eq!(range("bytes 3-5/6").unwrap(), (3, 5, 6));
        for bad in ["bytes 6-5/6", "bytes 0-6/6", "bytes 0-2/*", "items 0-2/3"] {
            assert!(range(bad).is_err());
        }
    }
}
