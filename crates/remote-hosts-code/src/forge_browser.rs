//! Owner-session-only project reads. Fixed loopback projection, never a generic proxy.
use crate::gateway::{Gateway, status_html_escape as esc};
use anyhow::{Result, ensure};
use axum::{
    Router,
    extract::{Request, State},
    http::{StatusCode, header},
    response::{Html, IntoResponse, Response},
    routing::any,
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{net::SocketAddrV4, time::Duration};
const BASE: &str = "/status/forge";
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForgeBrowserConfig {
    pub app_addr: SocketAddrV4,
    pub projects: Vec<String>,
}
fn project_id(s: &str) -> bool {
    s.len() == 36
        && s.bytes().enumerate().all(|(i, b)| {
            if matches!(i, 8 | 13 | 18 | 23) {
                b == b'-'
            } else {
                b.is_ascii_hexdigit() && !b.is_ascii_uppercase()
            }
        })
}
fn resource_id(s: &str) -> bool {
    !s.is_empty() && s.len() <= 128 && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
}
impl ForgeBrowserConfig {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.app_addr.ip().octets() == [127, 0, 0, 1] && self.app_addr.port() != 0,
            "Forge browser requires a fixed IPv4 loopback target"
        );
        ensure!(
            !self.projects.is_empty()
                && self.projects.len() <= 64
                && self.projects.iter().all(|p| project_id(p)),
            "Forge browser requires existing canonical project IDs"
        );
        ensure!(
            self.projects
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                == self.projects.len(),
            "duplicate Forge project ID"
        );
        Ok(())
    }
}
pub(crate) fn routes(g: Gateway) -> Router {
    Router::new()
        .route(BASE, any(view))
        .route("/status/forge/", any(view))
        .route("/status/forge/{*path}", any(view))
        .with_state(g)
}
#[derive(Debug)]
struct Selection<'a> {
    project: Option<&'a str>,
    section: &'a str,
    resource: Option<&'a str>,
}
fn selection<'a>(path: &'a str, query: Option<&str>) -> Option<Selection<'a>> {
    if query.is_some() || path.contains(['%', '\\']) || path.contains("..") || path.contains("//") {
        return None;
    }
    let tail = path.strip_prefix(BASE)?.trim_start_matches('/');
    if tail.is_empty() {
        return Some(Selection {
            project: None,
            section: "projects",
            resource: None,
        });
    }
    let p: Vec<_> = tail.split('/').collect();
    if !(2..=4).contains(&p.len()) || p[0] != "projects" || !project_id(p[1]) {
        return None;
    }
    let section = p.get(2).copied().unwrap_or("overview");
    if !matches!(
        section,
        "overview" | "issues" | "experiments" | "knowledge" | "evidence"
    ) {
        return None;
    }
    let resource = p.get(3).copied();
    if resource.is_some_and(|r| !resource_id(r))
        || resource.is_some() && !matches!(section, "issues" | "experiments")
    {
        return None;
    }
    Some(Selection {
        project: Some(p[1]),
        section,
        resource,
    })
}
async fn read(c: &ForgeBrowserConfig, path: &str) -> Result<Value> {
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(5))
        .build()?;
    // Never forward owner cookies, Authorization, browser headers or query strings.
    let r = client
        .get(format!("http://{}{}", c.app_addr, path))
        .send()
        .await?;
    ensure!(r.status().is_success(), "Forge read unavailable");
    let mut stream = r.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        ensure!(
            bytes.len() + chunk.len() <= 1024 * 1024,
            "Forge read exceeds limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&bytes)?)
}
fn shell(title: &str, body: &str) -> String {
    format!(
        "<!doctype html><html lang=zh-CN><head><meta charset=utf-8><meta name=viewport content='width=device-width,initial-scale=1'><title>{} · Forge Collab</title><style>*,*:before,*:after{{box-sizing:border-box}}body{{margin:0;background:#f5f7fb;color:#17243a;font:16px/1.65 system-ui}}main{{max-width:1040px;margin:auto;padding:24px 16px}}h1{{font-size:26px}}a{{color:#245ec7;overflow-wrap:anywhere}}nav{{display:flex;flex-wrap:wrap;gap:8px;margin:16px 0}}nav a{{display:block;padding:8px 12px;border:1px solid #ced6e3;border-radius:8px;background:white}}.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,280px),1fr));gap:12px}}article,section{{background:white;border:1px solid #dce2ec;border-radius:12px;padding:16px;margin:12px 0;min-width:0}}p,h1,h2,h3,li,pre{{overflow-wrap:anywhere;word-break:break-word}}pre{{white-space:pre-wrap;font:14px/1.7 ui-monospace,monospace;max-width:100%}}.meta{{color:#526078;font-size:14px}}.badge{{border-radius:6px;background:#edf2fb;padding:3px 8px}}@media(max-width:400px){{main{{padding:16px 12px}}h1{{font-size:23px}}nav a{{flex:1 1 auto;text-align:center}}article,section{{padding:12px}}}}</style></head><body><main><a href='/status'>设备状态</a><h1>{}</h1><p class=meta>项目只读查看 · 历史与当前结论均保留</p>{}</main></body></html>",
        esc(title),
        esc(title),
        body
    )
}
fn nav(id: Option<&str>) -> String {
    let mut b = format!("<nav aria-label='项目导航'><a href='{BASE}/'>全部项目</a>");
    if let Some(id) = id {
        for (path, label) in [
            ("overview", "项目概览"),
            ("issues", "问题"),
            ("experiments", "实验"),
            ("knowledge", "项目进展"),
            ("evidence", "证据"),
        ] {
            b.push_str(&format!(
                "<a href='{BASE}/projects/{id}/{path}'>{label}</a>"
            ));
        }
    }
    b.push_str("</nav>");
    b
}
fn field(v: &Value, key: &str, label: &str) -> String {
    match v.get(key).filter(|v| !v.is_null()) {
        Some(v) => {
            let text = v
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| serde_json::to_string_pretty(v).unwrap_or_default());
            format!(
                "<p><strong>{}</strong></p><pre>{}</pre>",
                esc(label),
                esc(&text)
            )
        }
        None => String::new(),
    }
}
fn detail(r: &Value) -> String {
    let d = &r["document"];
    let mut b = format!(
        "<article><h2>{}</h2><p class=meta>版本 {} · 修订 {}</p>",
        esc(r["title"].as_str().unwrap_or("")),
        esc(&r["version"].to_string()),
        esc(r["revision_id"].as_str().unwrap_or(""))
    );
    for (key, label) in [
        ("coverage", "验收状态"),
        ("policy", "验收要求"),
        ("state", "当前状态"),
        ("stage", "阶段"),
        ("next_step", "下一步"),
        ("blockers", "阻塞"),
        ("hypothesis", "假设"),
        ("method", "方法"),
        ("baseline", "基线"),
        ("candidate", "候选"),
        ("limitations", "限制"),
        ("summary", "进展"),
        ("software_version", "历史软件版本"),
        ("source_commit", "源码"),
        ("source_commit_ref", "记录中的源码参考"),
        ("observed_at", "记录时间"),
    ] {
        b.push_str(&field(d, key, label));
    }
    for key in ["observations", "runs"] {
        if let Some(events) = d[key].as_array() {
            for e in events {
                b.push_str("<section><h3>历史证据</h3>");
                for (key, label) in [
                    ("state", "结论"),
                    ("outcome", "实验结论"),
                    ("occurred_at", "记录时间"),
                    ("summary", "说明"),
                    ("source", "来源"),
                    ("limitations", "限制"),
                    ("evidence_refs", "证据参考"),
                ] {
                    b.push_str(&field(e, key, label));
                }
                b.push_str("</section>");
            }
        }
    }
    b.push_str("</article>");
    b
}
fn render_project(v: &Value, s: &Selection<'_>) -> Option<String> {
    let id = s.project?;
    if v["schema_version"] != "forge.owner_browser.project.v1" || v["project"]["id"] != id {
        return None;
    }
    let rows = v["resources"].as_array()?;
    if rows.len() > 128 {
        return None;
    }
    if rows.iter().any(|r| {
        !resource_id(r["id"].as_str().unwrap_or(""))
            || !matches!(r["kind"].as_str(), Some("issue" | "experiment" | "fact"))
    }) {
        return None;
    }
    let mut b = nav(Some(id));
    if let Some(rid) = s.resource {
        let kind = if s.section == "issues" {
            "issue"
        } else {
            "experiment"
        };
        let r = rows.iter().find(|r| r["id"] == rid && r["kind"] == kind)?;
        b.push_str(&detail(r));
    } else {
        for r in rows.iter().filter(|r| match s.section {
            "issues" => r["kind"] == "issue",
            "experiments" => r["kind"] == "experiment",
            "knowledge" => r["kind"] == "fact",
            "evidence" => r["kind"] != "fact",
            _ => true,
        }) {
            if matches!(s.section, "knowledge" | "evidence") {
                b.push_str(&detail(r));
                continue;
            }
            let kind = r["kind"].as_str()?;
            let path = match kind {
                "issue" => format!("issues/{}", r["id"].as_str()?),
                "experiment" => format!("experiments/{}", r["id"].as_str()?),
                _ => "knowledge".into(),
            };
            b.push_str(&format!("<article><h2><a href='{BASE}/projects/{id}/{path}'>{}</a></h2><p class=meta>版本 {} · {}</p>{}</article>",esc(r["title"].as_str().unwrap_or("")),esc(&r["version"].to_string()),esc(kind),field(&r["document"],"state","状态")));
        }
    }
    Some(shell(v["project"]["name"].as_str().unwrap_or("项目"), &b))
}
async fn view(State(g): State<Gateway>, request: Request) -> Response {
    let Some(c) = g.config.forge_browser.as_ref() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if !matches!(
        *request.method(),
        axum::http::Method::GET | axum::http::Method::HEAD
    ) {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    if !crate::gateway::status_session_valid(&g, request.headers()).await {
        return (
            StatusCode::UNAUTHORIZED,
            Html(shell(
                "登录后查看项目",
                "<p><a href='/status'>使用现有 owner 登录</a>，登录后返回此项目链接。</p>",
            )),
        )
            .into_response();
    }
    let Some(s) = selection(request.uri().path(), request.uri().query()) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if s.project
        .is_some_and(|p| !c.projects.iter().any(|id| id == p))
    {
        return StatusCode::NOT_FOUND.into_response();
    }
    let html = if let Some(id) = s.project {
        match read(c, &format!("/_owner_read/project/{id}.json")).await {
            Ok(v) => render_project(&v, &s),
            Err(_) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
        }
    } else {
        let v = match read(c, "/_owner_read/projects.json").await {
            Ok(v) => v,
            Err(_) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
        };
        if v["schema_version"] != "forge.owner_browser.catalog.v1" {
            return StatusCode::BAD_GATEWAY.into_response();
        }
        let Some(projects) = v["projects"].as_array().filter(|p| p.len() <= 64) else {
            return StatusCode::BAD_GATEWAY.into_response();
        };
        let mut b = nav(None);
        b.push_str("<div class=cards>");
        for p in projects {
            let Some(id) = p["id"]
                .as_str()
                .filter(|id| project_id(id) && c.projects.iter().any(|p| p == id))
            else {
                continue;
            };
            b.push_str(&format!("<article><h2><a href='{BASE}/projects/{id}'>{}</a></h2><a href='{BASE}/projects/{id}/issues'>问题</a> · <a href='{BASE}/projects/{id}/experiments'>实验</a> · <a href='{BASE}/projects/{id}/knowledge'>进展</a></article>",esc(p["name"].as_str().unwrap_or("项目"))));
        }
        b.push_str("</div>");
        Some(shell("项目", &b))
    };
    let Some(html) = html else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let mut response = Html(html).into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    response
}
#[cfg(test)]
#[path = "forge_browser_tests.rs"]
mod tests;
