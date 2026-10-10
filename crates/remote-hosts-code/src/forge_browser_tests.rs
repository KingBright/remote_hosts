use super::*;
use crate::{GatewayConfig, hash, now};
use axum::{
    Json,
    body::{Body, to_bytes},
    http::Request as HttpRequest,
};
use serde_json::json;
use std::sync::{Arc, Mutex};
use tower::ServiceExt;
const ID: &str = "53b96024-e939-5394-90da-45d5fd26a160";
const FOREIGN: &str = "44cb8f5f-81d3-5637-b5fc-4b036747fec6";
struct Fixture {
    g: Gateway,
    _dir: tempfile::TempDir,
    calls: Arc<Mutex<Vec<String>>>,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
async fn fixture() -> Fixture {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let seen = calls.clone();
    let upstream=Router::new().fallback(any(move |r:Request| {let seen=seen.clone();async move {
        assert!(!r.headers().contains_key(header::COOKIE));assert!(!r.headers().contains_key(header::AUTHORIZATION));
        seen.lock().unwrap().push(r.uri().path().to_owned());
        if r.uri().path()=="/_owner_read/projects.json" {Json(json!({"schema_version":"forge.owner_browser.catalog.v1","projects":[{"id":ID,"name":"Pixel Forge"},{"id":FOREIGN,"name":"Foreign hidden"}]}))}
        else {Json(json!({"schema_version":"forge.owner_browser.project.v1","project":{"id":ID,"name":"Pixel Forge"},"resources":[{"id":"tracking-issue1","title":"<script>bad</script>长项目问题标题","kind":"issue","version":9,"revision_id":"rev9","document":{"state":"open","next_step":"Real per-frame controls","observations":[{"state":"unknown","summary":"55% INVALID METRIC","source":{"kind":"agent_report","version":"a99f"}}]}},{"id":"tracking-exp1","title":"Route correctness","kind":"experiment","version":8,"revision_id":"rev8","document":{"hypothesis":"Village wilderness village","runs":[{"outcome":"positive","summary":"Route only; no performance baseline","evidence_refs":["results/route-only/"]}]}},{"id":"fact1","title":"Progress","kind":"fact","version":13,"revision_id":"rev13","document":{"summary":"Real movement controls pending"}}]}))}
    }}));
    let server = tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });
    let dir = tempfile::tempdir().unwrap();
    let app_addr = match addr {
        std::net::SocketAddr::V4(a) => a,
        _ => unreachable!(),
    };
    let g = Gateway::new(GatewayConfig {
        forge_browser: Some(ForgeBrowserConfig {
            app_addr,
            projects: vec![ID.into()],
        }),
        public_url: "https://owner.test".into(),
        bind: "127.0.0.1:0".into(),
        state_dir: dir.path().into(),
        owner: "fixture-owner".into(),
        password_hash: "fixture-hash-not-a-production-secret".into(),
        devices: vec![],
        redirect_uris: vec![],
        allowed_origins: vec![],
    })
    .await
    .unwrap();
    Fixture {
        g,
        _dir: dir,
        calls,
        server,
    }
}
async fn fetch(f: &Fixture, method: &str, path: &str, cookie: Option<&str>) -> (u16, String) {
    let mut b = HttpRequest::builder()
        .method(method)
        .uri(path)
        .header(header::HOST, "owner.test");
    if let Some(cookie) = cookie {
        b = b
            .header(header::COOKIE, cookie)
            .header(header::AUTHORIZATION, "Bearer do-not-forward");
    }
    let r =
        f.g.router()
            .unwrap()
            .oneshot(b.body(Body::empty()).unwrap())
            .await
            .unwrap();
    let code = r.status().as_u16();
    let body = String::from_utf8(
        to_bytes(r.into_body(), 2 * 1024 * 1024)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    (code, body)
}
async fn login(f: &Fixture, expires: i64) -> String {
    let token = "a".repeat(64);
    f.g.store
        .put("status_session", &hash(&token), &true, expires)
        .await
        .unwrap();
    format!("rh_status={token}")
}
#[tokio::test]
async fn forge_browser_requires_current_owner_session_before_upstream_reads() {
    let f = fixture().await;
    assert_eq!(fetch(&f, "GET", "/status/forge/", None).await.0, 401);
    let c = login(&f, now() - 1).await;
    assert_eq!(fetch(&f, "GET", "/status/forge/", Some(&c)).await.0, 401);
    assert!(f.calls.lock().unwrap().is_empty());
    let c = login(&f, now() + 30).await;
    assert_eq!(fetch(&f, "GET", "/status/forge/", Some(&c)).await.0, 200);
    sqlx::query("DELETE FROM kv WHERE kind='status_session'")
        .execute(&f.g.store.pool)
        .await
        .unwrap();
    assert_eq!(fetch(&f, "GET", "/status/forge/", Some(&c)).await.0, 401);
}
#[tokio::test]
async fn forge_browser_rejects_writes_traversal_foreign_projects_and_arbitrary_targets() {
    let f = fixture().await;
    let c = login(&f, now() + 30).await;
    for method in ["POST", "PUT", "PATCH", "DELETE", "OPTIONS"] {
        assert_eq!(fetch(&f, method, "/status/forge/", Some(&c)).await.0, 405);
    }
    for path in [
        format!("/status/forge/projects/{FOREIGN}"),
        "/status/forge/../admin".into(),
        "/status/forge/%2e%2e/admin".into(),
        "/status/forge/projects%2fother".into(),
        "/status/forge/?url=http://example.org".into(),
        "/status/forge/?actor=other".into(),
        format!("/status/forge/projects/{ID}/issues/.."),
        format!("/status/forge/projects/{ID}/issues/tracking-exp1"),
        "/status/forge//projects/other".into(),
    ] {
        assert_eq!(fetch(&f, "GET", &path, Some(&c)).await.0, 404, "{path}");
    }
    // The valid but wrong-kind detail performs a fixed read, never an arbitrary target.
    assert_eq!(
        *f.calls.lock().unwrap(),
        vec![format!("/_owner_read/project/{ID}.json")]
    );
}
#[tokio::test]
async fn forge_browser_deep_links_evidence_and_html_escaping_are_scoped() {
    let f = fixture().await;
    let c = login(&f, now() + 30).await;
    let (code, catalog) = fetch(&f, "GET", "/status/forge/", Some(&c)).await;
    assert_eq!(code, 200);
    assert!(!catalog.contains("Foreign hidden"));
    for section in [
        "overview",
        "issues",
        "experiments",
        "knowledge",
        "evidence",
        "issues/tracking-issue1",
        "experiments/tracking-exp1",
    ] {
        let (code, b) = fetch(
            &f,
            "GET",
            &format!("/status/forge/projects/{ID}/{section}"),
            Some(&c),
        )
        .await;
        assert_eq!(code, 200);
        assert!(b.contains("项目导航"));
        assert!(!b.contains("<script>bad"));
        assert!(!b.contains("do-not-forward"));
        assert!(!b.contains("fixture-hash"));
        assert!(!b.contains("<form"));
        if section.starts_with("issues/") {
            assert!(
                b.contains("版本 9") && b.contains("55% INVALID METRIC") && b.contains("unknown")
            );
        }
    }
    let (code, body) = fetch(&f, "HEAD", "/status/forge/", Some(&c)).await;
    assert_eq!(code, 200);
    assert!(body.is_empty());
    assert_eq!(fetch(&f, "GET", "/status", None).await.0, 200);
    assert!(
        fetch(&f, "GET", "/status", Some(&c))
            .await
            .1
            .contains("href='/status/forge/'")
    );
}
#[tokio::test]
async fn forge_browser_is_disabled_by_default_and_rejects_nonloopback_configuration() {
    let mut f = fixture().await;
    let mut cfg = (*f.g.config).clone();
    cfg.forge_browser = None;
    f.g.config = Arc::new(cfg);
    assert_eq!(fetch(&f, "GET", "/status/forge/", None).await.0, 404);
    assert!(f.calls.lock().unwrap().is_empty());
    for addr in ["8.8.8.8:80", "127.0.0.2:60594", "127.0.0.1:0"] {
        assert!(
            ForgeBrowserConfig {
                app_addr: addr.parse().unwrap(),
                projects: vec![ID.into()]
            }
            .validate()
            .is_err()
        );
    }
    assert!(
        ForgeBrowserConfig {
            app_addr: "127.0.0.1:60594".parse().unwrap(),
            projects: vec![ID.into(), ID.into()]
        }
        .validate()
        .is_err()
    );
}

#[tokio::test]
#[ignore = "explicit owned local browser fixture only"]
async fn forge_browser_live_fixture() {
    let mut f = fixture().await;
    let app: serde_json::Value = serde_json::from_slice(
        &std::fs::read(std::env::var("FORGE_OWNER_APP_FIXTURE_MANIFEST").unwrap()).unwrap(),
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut cfg = (*f.g.config).clone();
    cfg.public_url = format!("https://{addr}");
    cfg.forge_browser = Some(ForgeBrowserConfig {
        app_addr: app["app_addr"].as_str().unwrap().parse().unwrap(),
        projects: vec![app["project_id"].as_str().unwrap().to_owned()],
    });
    cfg.forge_browser.as_ref().unwrap().validate().unwrap();
    f.g.config = Arc::new(cfg);
    login(&f, now() + 240).await;
    f.g.store
        .put("status_session", &hash("b".repeat(64)), &true, now() - 1)
        .await
        .unwrap();
    let manifest =
        std::path::PathBuf::from(std::env::var("FORGE_OWNER_GATEWAY_FIXTURE_MANIFEST").unwrap());
    std::fs::write(&manifest,serde_json::json!({"base_url":format!("http://{addr}"),"pid":std::process::id(),"fixture_only":true}).to_string()).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&manifest, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    axum::serve(listener, f.g.router().unwrap())
        .with_graceful_shutdown(async move {
            for _ in 0..2400 {
                if manifest.with_extension("stop").exists() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn forge_browser_does_not_follow_redirects_or_expose_upstream_error_bodies() {
    let mut f = fixture().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let upstream = Router::new().fallback(any(|| async {
        (
            StatusCode::FOUND,
            [(header::LOCATION, "http://192.0.2.1/secret")],
            "upstream-private-token",
        )
    }));
    let task = tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });
    let mut cfg = (*f.g.config).clone();
    cfg.forge_browser.as_mut().unwrap().app_addr = match addr {
        std::net::SocketAddr::V4(a) => a,
        _ => unreachable!(),
    };
    f.g.config = Arc::new(cfg);
    let c = login(&f, now() + 30).await;
    let (code, b) = fetch(&f, "GET", "/status/forge/", Some(&c)).await;
    assert_eq!(code, 503);
    assert!(!b.contains("upstream-private-token"));
    assert!(!b.contains("192.0.2.1"));
    task.abort();
    let _ = task.await;
}
