//! Upload idle detection uses body demand AND authenticated receiver progress.
//! Waiting for response headers is not a valid total upload deadline.
use anyhow::{Context, Result, bail};
use reqwest::{RequestBuilder, Response};
use serde_json::Value;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

pub(crate) async fn send(
    request: RequestBuilder,
    progress_request: RequestBuilder,
    consumed: Arc<AtomicU64>,
    idle: Duration,
) -> Result<Response> {
    let interval = (idle / 4)
        .min(Duration::from_secs(2))
        .max(Duration::from_millis(1));
    let sending = request.send();
    let monitor = async {
        let mut last = Instant::now();
        let mut previous_local = 0;
        let mut previous_peer = None;
        loop {
            tokio::time::sleep(interval).await;
            let local = consumed.load(Ordering::Relaxed);
            if local != previous_local {
                previous_local = local;
                last = Instant::now();
            }
            // This request is authorized by the same device and operation, and
            // exposes only counters. Fast uploads finish before the first poll.
            let response = progress_request
                .try_clone()
                .context("progress request is not cloneable")?
                .timeout(interval)
                .send()
                .await;
            if let Ok(response) = response
                && response.status().is_success()
                && let Ok(value) = response.json::<Value>().await
            {
                let peer = (
                    value["snapshot"]["bytes_done"].as_u64(),
                    value["snapshot"]["phase"].as_str().unwrap_or("").to_owned(),
                );
                if peer.0.is_some() && previous_peer.as_ref() != Some(&peer) {
                    previous_peer = Some(peer);
                    last = Instant::now();
                }
            }
            if last.elapsed() >= idle {
                bail!("upload_idle_timeout: neither body demand nor receiver progress advanced");
            }
        }
    };
    tokio::select! {
        response=sending=>response.map_err(|_|anyhow::anyhow!("gateway upload transport interrupted")),
        result=monitor=>result,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Json, Router,
        body::Bytes,
        extract::Request,
        routing::{get, post},
    };
    use futures_util::StreamExt;
    use serde_json::json;
    #[tokio::test]
    async fn slow_sender_or_slow_receiver_can_exceed_idle_interval() {
        for slow_sender in [true, false] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let origin = format!("http://{}", listener.local_addr().unwrap());
            let received = Arc::new(AtomicU64::new(0));
            let r = received.clone();
            let p = received.clone();
            let router=Router::new().route("/send",post(move |request:Request| {let r=r.clone();async move {
                let mut body=request.into_body().into_data_stream();
                while let Some(Ok(bytes))=body.next().await {
                    for _ in bytes { if !slow_sender {tokio::time::sleep(Duration::from_millis(70)).await;} r.fetch_add(1,Ordering::Relaxed); }
                }
                Json(json!({"ok":true}))
            }})).route("/progress",get(move || {let p=p.clone();async move {Json(json!({"snapshot":{"bytes_done":p.load(Ordering::Relaxed),"phase":"transferring"}}))}}));
            let server = tokio::spawn(async move {
                axum::serve(listener, router).await.unwrap();
            });
            let consumed = Arc::new(AtomicU64::new(0));
            let c = consumed.clone();
            let stream = futures_util::stream::unfold(0, move |n| {
                let c = c.clone();
                async move {
                    if n == 10 {
                        return None;
                    }
                    if slow_sender {
                        tokio::time::sleep(Duration::from_millis(70)).await;
                    }
                    c.fetch_add(1, Ordering::Relaxed);
                    Some((Ok::<_, std::io::Error>(Bytes::from_static(b"x")), n + 1))
                }
            });
            let http = reqwest::Client::new();
            let started = Instant::now();
            let response = send(
                http.post(format!("{origin}/send"))
                    .body(reqwest::Body::wrap_stream(stream)),
                http.get(format!("{origin}/progress")),
                consumed,
                Duration::from_millis(400),
            )
            .await;
            server.abort();
            assert!(response.unwrap().status().is_success());
            assert!(started.elapsed() > Duration::from_millis(600));
            assert_eq!(received.load(Ordering::Relaxed), 10);
        }
    }
    #[tokio::test]
    async fn no_sender_or_receiver_progress_times_out() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let router = Router::new()
            .route(
                "/send",
                post(|| async {
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    Json(json!({}))
                }),
            )
            .route(
                "/progress",
                get(|| async { Json(json!({"snapshot":{"bytes_done":0,"phase":"transferring"}})) }),
            );
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let http = reqwest::Client::new();
        let result = send(
            http.post(format!("{origin}/send")).body("x"),
            http.get(format!("{origin}/progress")),
            Arc::new(AtomicU64::new(0)),
            Duration::from_millis(200),
        )
        .await;
        server.abort();
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("upload_idle_timeout")
        );
    }
}
